from analyzer.analyzer import Analyzer
from redis_conn import conn
import json

def run_analysis(job_id, filepath, filename):
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

    except Exception as e:
        conn.set(job_id, json.dumps({
            "status": "Error",
            "result": str(e)
        }))
