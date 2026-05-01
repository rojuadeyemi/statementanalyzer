import json
from analyzer.analyzer import Analyzer
from redis_conn import conn

def run_analysis(job_id, filepath, filename):
    print("JOB STARTED:", job_id)

    try:
        conn.set(job_id, json.dumps({"status": "Reading statement..."}))

        statement = Analyzer(filepath)

        conn.set(job_id, json.dumps({"status": "Analyzing transactions..."}))

        summary = statement.risk_indicators().to_dict()

        conn.set(job_id, json.dumps({
            "status": "Completed",
            "summary": summary,
            "file_path": filepath,
            "filename": filename
        }))

        print("JOB COMPLETED")

    except Exception as e:
        print("JOB ERROR:", str(e))

        conn.set(job_id, json.dumps({
            "status": "Error",
            "result": str(e)
        }))
