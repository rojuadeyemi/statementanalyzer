from flask import (
    Flask,
    request,
    render_template,
    redirect,
    url_for,
    jsonify,
    abort,
    send_file,
)
import os
import uuid
import threading
import gc
import psutil

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

from analyzer.analyzer import Analyzer

app = Flask(__name__)

# -----------------------------
# CONFIG
# -----------------------------

app.config["MAX_CONTENT_LENGTH"] = 10 * 1024 * 1024

UPLOAD_FOLDER = os.path.join(app.root_path, "uploads")
REPORT_FOLDER = os.path.join(app.root_path, "reports")

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(REPORT_FOLDER, exist_ok=True)

# -----------------------------
# JOB STORAGE
# -----------------------------

jobs = {}

# Limit concurrent processing
executor = ThreadPoolExecutor(max_workers=2)

# -----------------------------
# MEMORY LOGGER
# -----------------------------

def memory_usage():
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024


# -----------------------------
# CLEANUP OLD JOBS
# -----------------------------

def cleanup_jobs():

    now = datetime.utcnow()

    expired = []

    for job_id, job in jobs.items():

        completed_at = job.get("completed_at")

        if (
            completed_at
            and now - completed_at > timedelta(minutes=10)
        ):
            expired.append(job_id)

    for job_id in expired:

        job = jobs.pop(job_id, None)

        if not job:
            continue

        for key in ["report_file", "json_file", "file_path"]:

            path = job.get(key)

            if path and os.path.exists(path):

                try:
                    os.remove(path)
                except:
                    pass


# -----------------------------
# ANALYSIS WORKER
# -----------------------------

def run_analysis(job_id, filepath, filename):

    try:

        print(f"Starting analysis: {memory_usage():.1f} MB")

        jobs[job_id]["status"] = "Reading statement..."

        statement = Analyzer(filepath)

        print(f"After Analyzer: {memory_usage():.1f} MB")

        jobs[job_id]["status"] = "Analyzing transactions..."

        summary = statement.risk_indicators.to_dict()

        # Save reports to disk
        excel_path = os.path.join(
            REPORT_FOLDER,
            f"{job_id}.xlsx"
        )

        json_path = os.path.join(
            REPORT_FOLDER,
            f"{job_id}.json"
        )

        statement.generate_excel_report(excel_path)
        #statement.generate_json_report(json_path)

        del statement
        gc.collect()

        print(f"After report generation: {memory_usage():.1f} MB")

        jobs[job_id].update(
            {
                "status": "Completed",
                "summary": summary,
                "report_file": excel_path,
                #"json_file": json_path,
                "filename": filename,
                "file_path": filepath,
                "completed_at": datetime.utcnow(),
            }
        )

    except Exception as e:

        jobs[job_id]["status"] = "Error"
        jobs[job_id]["result"] = str(e)

    finally:

        try:
            if os.path.exists(filepath):
                os.remove(filepath)
        except:
            pass

        try:
            del statement
        except:
            pass

        gc.collect()

        print(f"Finished: {memory_usage():.1f} MB")


# -----------------------------
# ROUTES
# -----------------------------

@app.route("/")
def main():

    cleanup_jobs()

    return render_template("index.html")


@app.route("/extract", methods=["POST"])
def extract():

    if "file" not in request.files:
        return redirect(url_for("main"))

    file = request.files["file"]

    if file.filename == "":
        return redirect(url_for("main"))

    filename = file.filename

    filepath = os.path.join(
        UPLOAD_FOLDER,
        f"{uuid.uuid4()}_{filename}"
    )

    file.save(filepath)

    job_id = str(uuid.uuid4())

    jobs[job_id] = {
        "status": "Queued...",
        "result": None,
    }

    executor.submit(
        run_analysis,
        job_id,
        filepath,
        filename,
    )

    return redirect(
        url_for(
            "processing",
            job_id=job_id
        )
    )


@app.route("/processing/<job_id>")
def processing(job_id):

    return render_template(
        "processing.html",
        job_id=job_id
    )


@app.route("/status/<job_id>")
def status(job_id):

    job = jobs.get(job_id)

    if not job:

        return jsonify(
            {"status": "Unknown job"}
        )

    return jsonify(
        {
            "status": job.get("status"),
            "result": job.get("result"),
        }
    )


@app.route("/result/<job_id>")
def result(job_id):

    job = jobs.get(job_id)

    if not job:
        abort(404)

    if job["status"] != "Completed":

        return redirect(
            url_for(
                "processing",
                job_id=job_id
            )
        )

    return render_template(
        "after.html",
        statement_summary=job["summary"],
        job_id=job_id,
    )


# -----------------------------
# DOWNLOADS
# -----------------------------

@app.route("/download/<job_id>")
def download(job_id):

    job = jobs.get(job_id)

    if not job or job["status"] != "Completed":
        abort(404)

    timestamp = datetime.now().strftime(
        "%Y%m%d%H%M%S"
    )

    filename = job.get(
        "filename",
        "report.xlsx"
    )

    download_name = (
        f"{os.path.splitext(filename)[0]}"
        f"_{timestamp}.xlsx"
    )

    return send_file(
        job["report_file"],
        as_attachment=True,
        download_name=download_name,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


"""@app.route("/download_json/<job_id>")
def download_json(job_id):

    job = jobs.get(job_id)

    if not job or job["status"] != "Completed":
        abort(404)

    timestamp = datetime.now().strftime(
        "%Y%m%d%H%M%S"
    )

    filename = job.get(
        "filename",
        "report"
    )

    download_name = (
        f"{os.path.splitext(filename)[0]}"
        f"_{timestamp}.json"
    )

    return send_file(
        job["json_file"],
        as_attachment=True,
        download_name=download_name,
        mimetype="application/json",
    )"""


# -----------------------------
# APP START
# -----------------------------

if __name__ == "__main__":
    app.run()
