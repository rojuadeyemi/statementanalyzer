from flask import Flask, request, render_template, redirect, url_for, jsonify, abort, send_file
import os
import threading
import uuid
import logging
from datetime import datetime
from analyzer.analyzer import Analyzer


# App setup
app = Flask(__name__)

app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024
app.config['UPLOAD_FOLDER'] = os.path.join(app.root_path, 'static', 'uploads')

os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# In-memory job store
jobs = {}
lock = threading.Lock()


# Background worker
def run_analysis(job_id, filepath, filename):
    logger.info(f"Job started: {job_id}")

    try:
        with lock:
            jobs[job_id]["status"] = "Reading statement..."

        statement = Analyzer(filepath)

        with lock:
            jobs[job_id]["status"] = "Analyzing transactions..."

        summary = statement.risk_indicators().to_dict()

        with lock:
            jobs[job_id]["status"] = "Completed"
            jobs[job_id]["summary"] = summary
            jobs[job_id]["file_path"] = filepath
            jobs[job_id]["filename"] = filename

        logger.info(f"Job completed: {job_id}")

    except Exception as e:
        logger.exception("Job failed")

        with lock:
            jobs[job_id]["status"] = "Error"
            jobs[job_id]["result"] = str(e)

# Home page
@app.route('/')
def main():
    return render_template('index.html')

# Upload + start job
@app.route('/extract', methods=['POST'])
def extract():

    if 'file' not in request.files:
        return redirect(url_for('main'))

    file = request.files['file']

    if file.filename == '':
        return redirect(url_for('main'))

    filepath = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
    file.save(filepath)

    job_id = str(uuid.uuid4())

    with lock:
        jobs[job_id] = {
            "status": "Queued",
            "result": None,
            "summary": None,
            "file_path": filepath,
            "filename": file.filename
        }

    thread = threading.Thread(
        target=run_analysis,
        args=(job_id, filepath, file.filename),
        daemon=True
    )
    thread.start()

    return redirect(url_for("processing", job_id=job_id))


# Processing page
@app.route("/processing/<job_id>")
def processing(job_id):
    return render_template("processing.html", job_id=job_id)

# Status API
@app.route("/status/<job_id>")
def status(job_id):
    with lock:
        job = jobs.get(job_id)

    if not job:
        return jsonify({"status": "Unknown job"})

    return jsonify({
        "status": job.get("status"),
        "result": job.get("result")
    })

# Result page
@app.route("/result/<job_id>")
def result(job_id):

    with lock:
        job = jobs.get(job_id)

    if not job:
        return abort(404)

    if job["status"] != "Completed":
        return redirect(url_for("processing", job_id=job_id))

    return render_template(
        "after.html",
        statement_summary=job["summary"],
        job_id=job_id
    )

# Download report
@app.route("/download/<job_id>")
def download(job_id):

    with lock:
        job = jobs.get(job_id)

    if not job or job.get("status") != "Completed":
        return abort(404)

    analyzer = Analyzer(job["file_path"])
    output = analyzer.generate_excel_report()

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    filename = job.get("filename", "report.xlsx")
    download_name = f"{os.path.splitext(filename)[0]}_{timestamp}.xlsx"

    return send_file(
        output,
        as_attachment=True,
        download_name=download_name,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )


# Run
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
