from flask import Flask, request, render_template, redirect, url_for, jsonify, abort, send_file
import os
import uuid
import json
from redis_conn import conn
from rq import Queue
from tasks import run_analysis
from analyzer.analyzer import Analyzer

app = Flask(__name__)

app.config['UPLOAD_FOLDER'] = "/tmp/uploads"
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# Create queue
q = Queue(connection=conn)

# Upload route
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

    # Initial status
    conn.set(job_id, json.dumps({"status": "Starting analysis..."}))

    # Enqueue job
    q.enqueue(run_analysis, job_id, filepath, file.filename)

    return redirect(url_for("processing", job_id=job_id))

# Status route
@app.route("/status/<job_id>")
def status(job_id):
    data = conn.get(job_id)

    if not data:
        return jsonify({"status": "Unknown job"})

    return jsonify(json.loads(data))

# Result page
@app.route("/result/<job_id>")
def result(job_id):
    data = conn.get(job_id)

    if not data:
        return abort(404)

    job = json.loads(data)

    if job["status"] != "Completed":
        return redirect(url_for("processing", job_id=job_id))

    return render_template(
        "after.html",
        statement_summary=job["summary"],
        job_id=job_id
    )

# Download
@app.route("/download/<job_id>")
def download(job_id):
    data = conn.get(job_id)

    if not data:
        return abort(404)

    job = json.loads(data)

    if job["status"] != "Completed":
        return abort(404)

    analyzer = Analyzer(job["file_path"])
    output = analyzer.generate_excel_report()

    filename = job.get("filename", "report.xlsx")

    from datetime import datetime
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    return send_file(
        output,
        as_attachment=True,
        download_name=f"{filename}_{timestamp}.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

# Pages
@app.route('/')
def main():
    return render_template('index.html')

@app.route("/processing/<job_id>")
def processing(job_id):
    return render_template("processing.html", job_id=job_id)
