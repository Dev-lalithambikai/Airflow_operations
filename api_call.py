from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
import os

app = FastAPI()

# Configuration: Update this to your actual Airflow DAGs path
AIRFLOW_DAGS_FOLDER = "/opt/airflow/dags" 

# --- Data Models ---
class NodeDef(BaseModel):
    id: str
    engine: str
    executor_build_id: str
    executor_order_id: str
    executor_sequence_id: str

class DagCreationPayload(BaseModel):
    nodes: List[NodeDef]

# --- Endpoints ---
@app.post("/api/v1/create-dag/{dag_id}")
def create_dynamic_dag(dag_id: str, payload: DagCreationPayload):
    try:
        # Sort nodes by sequence ID to ensure correct execution order
        sorted_nodes = sorted(payload.nodes, key=lambda x: int(x.executor_sequence_id))
        
        # 1. Base template for the DAG code
        dag_code = f"""
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def execute_engine_call(task_key, **kwargs):
    # Retrieve the configuration payload passed during the trigger
    dag_run_conf = kwargs['dag_run'].conf
    if not dag_run_conf:
        raise ValueError("No configuration payload found in dag_run.conf")
    
    # Extract specific task data
    task_config = dag_run_conf.get(task_key)
    if not task_config:
        raise ValueError(f"Configuration for {{task_key}} is missing.")
    
    target_url = task_config.get('url')
    request_payload = task_config.get('json', {{}})
    headers = request_payload.get('headers', {{}})
    timeout = request_payload.get('timeout', 120)
    
    print(f"Triggering {{task_key}} to {{target_url}}")
    
    # Execute the HTTP request to the external engine (NiFi/Python)
    response = requests.post(target_url, json=request_payload, headers=headers, timeout=timeout)
    response.raise_for_status()
    print(f"Engine responded successfully: {{response.status_code}}")

with DAG(
    dag_id='{dag_id}', 
    start_date=datetime(2024, 1, 1), 
    schedule_interval=None, 
    catchup=False
) as dag:
"""

        # 2. Inject task definitions based on the Hub's payload
        task_names = []
        for index, node in enumerate(sorted_nodes):
            task_key = f"Task{index + 1}"
            safe_task_id = node.id.replace("-", "_")
            task_names.append(safe_task_id)
            
            dag_code += f"""
    {safe_task_id} = PythonOperator(
        task_id='{node.id}',
        python_callable=execute_engine_call,
        op_kwargs={{'task_key': '{task_key}'}}
    )
"""

        # 3. Define the execution sequence (e.g., Node1 >> Node2 >> Node3)
        if task_names:
            dependencies = " >> ".join(task_names)
            dag_code += f"\n    {dependencies}\n"

        # 4. Write the DAG to the Airflow directory
        os.makedirs(AIRFLOW_DAGS_FOLDER, exist_ok=True)
        file_path = os.path.join(AIRFLOW_DAGS_FOLDER, f"{dag_id}.py")
        
        with open(file_path, "w") as f:
            f.write(dag_code)

        return {"status": "success", "message": f"DAG '{dag_id}' generated at {file_path}"}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
