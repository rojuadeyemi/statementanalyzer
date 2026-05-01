from flask import Flask, request, render_template, redirect, url_for, jsonify,abort,send_file
import os
import threading
import uuid
from analyzer.analyzer import Analyzer

# Initialize Flask app
app = Flask(__name__)

# Limit upload size (50MB)
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024

# Upload folder
app.config['UPLOAD_FOLDER'] = os.path.join(app.root_path, 'static', 'uploads')

# Ensure upload folder exists
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

@app.route("/download/<job_id>")
def download(job_id):
    job = jobs.get(job_id)

    if not job or job["status"] != "Completed":
        return abort(404)

    output = job["report_file"]

    from datetime import datetime
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    filename = job.get("filename", "report.xlsx")
    download_name = f"{os.path.splitext(filename)[0]}_{timestamp}.xlsx"

    return send_file(
        output,
        as_attachment=True,
        download_name=download_name,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

# Job storage
jobs = {}

# Background analysis function
def run_analysis(job_id, filepath, filename):

    try:
        jobs[job_id]["status"] = "Reading statement..."

        statement = Analyzer(filepath)

        jobs[job_id]["status"] = "Analyzing transactions..."

        statement_summary = statement.risk_indicators().to_dict()
        jobs[job_id]["report_file"] = statement.generate_excel_report()  # store result once
        
        jobs[job_id]["status"] = "Completed"
        jobs[job_id]["summary"] = statement_summary
        jobs[job_id]["file_path"] = filepath
        jobs[job_id]["filename"] = filename

    except Exception as e:
        jobs[job_id]["status"] = "Error"
        jobs[job_id]["result"] = str(e)

# Home page
@app.route('/')
def main():
    return render_template('index.html')

# Upload + start background job
@app.route('/extract', methods=['POST'])
def extract():

    if 'file' not in request.files:
        return redirect(url_for('main'))

    file = request.files['file']

    if file.filename == '':
        return redirect(url_for('main'))

    # Save file
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
    file.save(filepath)

    # Create job
    job_id = str(uuid.uuid4())

    jobs[job_id]["status"] = "Extracting Statement..."

    # Start background thread
    thread = threading.Thread(
        target=run_analysis,
        args=(job_id, filepath, file.filename)
    )

    thread.start()

    return redirect(url_for("processing", job_id=job_id))

# Processing page
@app.route("/processing/<job_id>")
def processing(job_id):
    return render_template("processing.html", job_id=job_id)

# Job status API
@app.route("/status/<job_id>")
def status(job_id):
    job = jobs.get(job_id)

    if not job:
        return jsonify({"status": "Unknown job"})

    safe_job = {
        "status": job.get("status"),
        "result": job.get("result")
    }

    return jsonify(safe_job)

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

# Run app
if __name__ == '__main__':
    app.run()
