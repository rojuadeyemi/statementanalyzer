from flask import Flask, request, render_template, redirect, url_for, jsonify, abort, send_file
import os
import uuid
import time
import threading
from queue import Queue
from datetime import datetime
from analyzer.analyzer import Analyzer

# App setup
app = Flask(__name__)

app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024
app.config['UPLOAD_FOLDER'] = os.path.join(app.root_path, 'static', 'uploads')
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# Job storage (in-memory)
jobs = {}

# Job queue (IMPORTANT)
job_queue = Queue()

# Background worker (runs forever)
def worker():
    while True:
        job_id = job_queue.get()

        job = jobs.get(job_id)
        if not job:
            continue

        try:
            jobs[job_id]["status"] = "Processing..."

            analyzer = Analyzer(job["file_path"])

            # 1. compute summary
            summary = analyzer.risk_indicators().to_dict()

            # 2. generate report ONCE
            output = analyzer.generate_excel_report()

            jobs[job_id]["summary"] = summary
            jobs[job_id]["report_file"] = output  # store result once

            jobs[job_id]["status"] = "Completed"

        except Exception as e:
            jobs[job_id]["status"] = "Error"
            jobs[job_id]["result"] = str(e)

        job_queue.task_done()

# Start worker thread ON APP START (critical)
threading.Thread(target=worker, daemon=True).start()

# Home page
@app.route('/')
def main():
    return render_template('index.html')

# Upload endpoint
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

    jobs[job_id] = {
        "status": "Queued",
        "file_path": filepath,
        "filename": file.filename,
        "summary": None,
        "result": None
    }

    # Push job into queue (NOT threading)
    job_queue.put(job_id)

    return redirect(url_for("processing", job_id=job_id))

# Processing page
@app.route("/processing/<job_id>")
def processing(job_id):
    return render_template("processing.html", job_id=job_id)

# Status API (frontend polling)
@app.route("/status/<job_id>")
def status(job_id):

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

    job = jobs.get(job_id)

    if not job or job.get("status") != "Completed":
        return abort(404)

    output = job["report_file"]  # reused result

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    filename = job.get("filename", "report.xlsx")
    download_name = f"{os.path.splitext(filename)[0]}_{timestamp}.xlsx"

    return send_file(
        output,
        as_attachment=True,
        download_name=download_name,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
# Run (Render-safe)
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
