from fastapi import FastAPI
import os
import pandas as pd
from datetime import datetime
from deltalake import write_deltalake
import uvicorn
DATASETS = [
    "vle", "studentVle", "students", "students_mental_health_survey",
    "studentRegistration", "studentAssessment", "student_course",
    "healthcare_dataset_updated", "courses", "assessments"
]
app = FastAPI()


@app.post("/ingest")
def ingest_csv(dataset: str):
    try:
        print(f"Received dataset parameter: {dataset}") 
        file_path = f"/data/structured/{dataset}.csv"
        if not os.path.exists(file_path):
            available = os.listdir('/data/structured')
            return {
                "status": "error", 
                "message": f"File not found at {file_path}. Available files: {available}"
            }
        df = pd.read_csv(file_path)
        os.makedirs("/data/delta", exist_ok=True)
        
        write_deltalake(
            f"/data/delta/{dataset}",
            df,
            mode="overwrite"
        )
        return {"status": "success"}
    except Exception as e:
        return {"status": "error", "message": str(e)}