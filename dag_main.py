from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
import httpx
import logging
import uuid
from datetime import datetime
from enum import Enum

# ─────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger("dag_generator")

# ─────────────────────────────────────────────
# App
# ─────────────────────────────────────────────
app = FastAPI(
    title="Airflow DAG Generator Service",
    description="Receives build payloads from Genesis Core and dynamically creates Airflow DAGs.",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────
AIRFLOW_BASE_URL = "http://airflow-webserver:8080/api/v1"
AIRFLOW_USERNAME = "admin"
AIRFLOW_PASSWORD = "admin"
GENESIS_CORE_CALLBACK_URL = "http://genesis-core:8081/api/dag/status"
DAG_FILES_PATH = "/opt/airflow/dags"

# ─────────────────────────────────────────────
# Models
# ─────────────────────────────────────────────

class NodeType(str, Enum):
    TASK = "task"
    SENSOR = "sensor"
    BRANCH = "branch"

class NodeConfig(BaseModel):
    node_id: str = Field(..., description="Unique ID of the node")
    node_name: str = Field(..., description="Display name of the node")
    node_type: NodeType = Field(default=NodeType.TASK)
    operator: str = Field(..., description="Airflow operator to use, e.g. PythonOperator")
    dependencies: List[str] = Field(default=[], description="List of upstream node_ids")
    config: Dict[str, Any] = Field(default={}, description="Operator-specific configuration")

class BuildPayload(BaseModel):
    workflow_id: str = Field(..., description="Unique ID of the workflow from Genesis")
    workflow_name: str = Field(..., description="Human-readable workflow name")
    build_event_id: str = Field(..., description="The triggering build event ID")
    schedule_interval: Optional[str] = Field(default=None, description="Cron or @daily etc.")
    start_date: Optional[str] = Field(default="2024-01-01", description="DAG start date (YYYY-MM-DD)")
    nodes: List[NodeConfig] = Field(..., description="List of nodes/tasks in the workflow")
    metadata: Optional[Dict[str, Any]] = Field(default={}, description="Extra metadata")

class DAGStatusResponse(BaseModel):
    dag_id: str
    workflow_id: str
    build_event_id: str
    status: str
    message: str
    created_at: str

class HealthResponse(BaseModel):
    status: str
    timestamp: str
    service: str

# ─────────────────────────────────────────────
# DAG File Generator
# ─────────────────────────────────────────────

def generate_dag_file(payload: BuildPayload) -> str:
    """
    Generates a Python DAG file string from the build payload.
    This file is written to the Airflow DAGs folder.
    """
    dag_id = f"genesis_{payload.workflow_id}"
    schedule = f'"{payload.schedule_interval}"' if payload.schedule_interval else "None"

    # Build task definitions
    task_lines = []
    for node in payload.nodes:
        task_lines.append(f"""
    {node.node_id} = {node.operator}(
        task_id="{node.node_id}",
        dag=dag,
        **{node.config}
    )""")

    # Build dependency definitions
    dep_lines = []
    for node in payload.nodes:
        for dep in node.dependencies:
            dep_lines.append(f"    {dep} >> {node.node_id}")

    task_block = "\n".join(task_lines)
    dep_block = "\n".join(dep_lines) if dep_lines else "    # No dependencies defined"

    dag_code = f'''"""
Auto-generated DAG by Genesis DAG Generator Service
Workflow ID : {payload.workflow_id}
Workflow Name: {payload.workflow_name}
Build Event : {payload.build_event_id}
Generated At: {datetime.utcnow().isoformat()}
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta

default_args = {{
    "owner": "genesis",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}}

with DAG(
    dag_id="{dag_id}",
    default_args=default_args,
    description="Generated for workflow: {payload.workflow_name}",
    schedule_interval={schedule},
    start_date=datetime.strptime("{payload.start_date}", "%Y-%m-%d"),
    catchup=False,
    tags=["genesis", "auto-generated"],
) as dag:

{task_block}

    # Task Dependencies
{dep_block}
'''
    return dag_id, dag_code


# ─────────────────────────────────────────────
# Callback to Genesis Core
# ─────────────────────────────────────────────

async def notify_genesis_core(dag_id: str, workflow_id: str, build_event_id: str, status: str, message: str):
    """
    After DAG creation, POST the result back to Genesis Core
    so it can update the PostgreSQL database with DAG ID and status.
    """
    payload = {
        "dag_id": dag_id,
        "workflow_id": workflow_id,
        "build_event_id": build_event_id,
        "status": status,
        "message": message,
        "updated_at": datetime.utcnow().isoformat()
    }
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.post(GENESIS_CORE_CALLBACK_URL, json=payload)
            response.raise_for_status()
            logger.info(f"Callback to Genesis Core succeeded for DAG: {dag_id}")
    except Exception as e:
        logger.error(f"Callback to Genesis Core FAILED for DAG {dag_id}: {e}")


# ─────────────────────────────────────────────
# Endpoints
# ─────────────────────────────────────────────

@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health_check():
    """Health check endpoint — used by Genesis Core to verify service is alive."""
    return HealthResponse(
        status="healthy",
        timestamp=datetime.utcnow().isoformat(),
        service="airflow-dag-generator"
    )


@app.post("/api/dag/build", response_model=DAGStatusResponse, tags=["DAG"])
async def build_dag(payload: BuildPayload, background_tasks: BackgroundTasks):
    """
    Main endpoint called by Genesis Core (Java) when a BUILD event is received.

    Flow:
    1. Receive JSON payload from Genesis Core
    2. Generate Python DAG file from payload
    3. Write DAG file to Airflow DAGs folder
    4. Trigger Airflow to register the new DAG
    5. Return DAG ID and status
    6. Asynchronously POST status back to Genesis Core (callback)
    """
    logger.info(f"Received BUILD request for workflow_id={payload.workflow_id}, event={payload.build_event_id}")

    try:
        # Step 1: Generate DAG file
        dag_id, dag_code = generate_dag_file(payload)
        logger.info(f"Generated DAG file for dag_id={dag_id}")

        # Step 2: Write DAG file to Airflow DAGs directory
        dag_file_path = f"{DAG_FILES_PATH}/{dag_id}.py"
        with open(dag_file_path, "w") as f:
            f.write(dag_code)
        logger.info(f"Written DAG file to {dag_file_path}")

        # Step 3: Trigger Airflow API to unpause/register the DAG
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.patch(
                f"{AIRFLOW_BASE_URL}/dags/{dag_id}",
                json={"is_paused": False},
                auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD)
            )
            if response.status_code not in [200, 404]:
                # 404 is okay — DAG might not be picked up yet by scheduler
                logger.warning(f"Airflow PATCH returned {response.status_code} for {dag_id}")

        result = DAGStatusResponse(
            dag_id=dag_id,
            workflow_id=payload.workflow_id,
            build_event_id=payload.build_event_id,
            status="CREATED",
            message=f"DAG '{dag_id}' successfully created and registered in Airflow.",
            created_at=datetime.utcnow().isoformat()
        )

        # Step 4: Async callback to Genesis Core to update DB
        background_tasks.add_task(
            notify_genesis_core,
            dag_id=dag_id,
            workflow_id=payload.workflow_id,
            build_event_id=payload.build_event_id,
            status="CREATED",
            message=result.message
        )

        logger.info(f"DAG build successful: {dag_id}")
        return result

    except Exception as e:
        logger.error(f"DAG build FAILED for workflow {payload.workflow_id}: {e}")
        # Notify Genesis Core of failure too
        background_tasks.add_task(
            notify_genesis_core,
            dag_id=f"genesis_{payload.workflow_id}",
            workflow_id=payload.workflow_id,
            build_event_id=payload.build_event_id,
            status="FAILED",
            message=str(e)
        )
        raise HTTPException(status_code=500, detail=f"DAG creation failed: {str(e)}")


@app.get("/api/dag/{dag_id}/status", tags=["DAG"])
async def get_dag_status(dag_id: str):
    """
    Check the current status of a DAG in Airflow.
    Can be used by Genesis Core to verify DAG exists.
    """
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.get(
                f"{AIRFLOW_BASE_URL}/dags/{dag_id}",
                auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD)
            )
            if response.status_code == 404:
                raise HTTPException(status_code=404, detail=f"DAG '{dag_id}' not found in Airflow")
            response.raise_for_status()
            return response.json()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/dag/{dag_id}", tags=["DAG"])
async def delete_dag(dag_id: str):
    """
    Delete a DAG from Airflow and remove its file.
    Used for cleanup or when a workflow is removed from Genesis.
    """
    import os
    errors = []

    # Remove DAG file
    dag_file = f"{DAG_FILES_PATH}/{dag_id}.py"
    try:
        if os.path.exists(dag_file):
            os.remove(dag_file)
            logger.info(f"Deleted DAG file: {dag_file}")
    except Exception as e:
        errors.append(f"File deletion failed: {e}")

    # Delete from Airflow via API
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.delete(
                f"{AIRFLOW_BASE_URL}/dags/{dag_id}",
                auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD)
            )
            if response.status_code not in [200, 204, 404]:
                errors.append(f"Airflow deletion returned {response.status_code}")
    except Exception as e:
        errors.append(f"Airflow API call failed: {e}")

    if errors:
        return {"dag_id": dag_id, "status": "PARTIAL_DELETE", "errors": errors}
    return {"dag_id": dag_id, "status": "DELETED", "message": "DAG successfully deleted"}
