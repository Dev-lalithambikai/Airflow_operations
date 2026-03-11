

import json
import logging
import requests  # Add this import
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import jsonschema
from airflow import DAG
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.operators.python import PythonOperator

# Constants
KAFKA_TOPIC_BUILD = "dag_build_events"
KAFKA_TOPIC_RUN = "dag_run_events"
EVENT_TYPE_BUILD_START = "BUILD_START"
EVENT_TYPE_RUN_START = "RUN_START"
VALID_HTTP_METHODS = ["GET", "POST", "PUT", "DELETE", "PATCH"]
_TASK_PROPERTIES = ["id", "engine", "executor_build_id", "executor_order_id", "executor_sequence_id", "url", "method", "headers", "json"]

logger = logging.getLogger(__name__)

# Schema for mapped nodes
NODES_SCHEMA = {
    "type": "array",
    "items": {
        "type": "object",
        "required": ["id", "engine", "executor_build_id", "executor_order_id", "executor_sequence_id", "url"],
        "properties": {
            "id": {"type": "string"},
            "engine": {"type": "string", "enum": ["NIFI", "PYTHON", "GENPROC", "DATASET"]},
            "executor_build_id": {"type": "string"},
            "executor_order_id": {"type": "string"},
            "executor_sequence_id": {"type": "string"},
            "url": {"type": "string"},
            "method": {"type": "string", "enum": VALID_HTTP_METHODS},
            "headers": {"type": "object"},
            "json": {"type": "object"}
        }
    }
}

@dag(
    dag_id="dynamic_nodes_central_dag",
    default_args={
        "owner": "data_engineer",
        "start_date": datetime(2026, 1, 1, tzinfo=timezone.utc),
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    schedule=None,
    catchup=False,
    tags=["dynamic", "nifi", "etl", "task-mapping"],
    params={},
)
def create_dynamic_dag():
    @task
    def validate_conf_and_map_nodes(**context):
        conf = context["dag_run"].conf
        correlation_id = conf.get("correlation_id", "unknown")
        
        # Extract Task1, Task2, ... from conf
        tasks = {k: v for k, v in conf.items() if k.startswith('Task')}
        if not tasks:
            raise AirflowException("No Task1/Task2/... found in conf")
        
        nodes = []
        for i, (task_name, task_data) in enumerate(sorted(tasks.items(), key=lambda x: int(x[0][4:])), 1):
            task_json = task_data.get("json", {})
            node = {
                "id": f"{task_name}-{conf.get('correlation_id', 'run')}",
                "engine": "NIFI" if "nifi" in task_data.get("url", "").lower() else "PYTHON",  # Auto-detect
                "executor_build_id": task_json.get("node_runId", task_name),
                "executor_order_id": str(i),
                "executor_sequence_id": str(i),
                "url": task_data["url"],
                "method": task_json.get("method", "POST").upper(),
                "headers": task_json.get("headers", {}),
                "json": task_json,
                "timeout": task_json.get("timeout", 120),
                "correlation_id": correlation_id
            }
            nodes.append(node)
        
        # Validate mapped nodes
        jsonschema.validate(instance=nodes, schema=NODES_SCHEMA)
        logger.info(f"Mapped {len(nodes)} tasks to nodes for correlation_id: {correlation_id}")
        return nodes

    @task
    def execute_node(node: Dict[str, Any], **context):
        try:
            url = node["url"]
            method = node["method"]
            headers = node["headers"]
            payload = node["json"]
            timeout = node.get("timeout", 120)
            correlation_id = node.get("correlation_id")

            if method not in VALID_HTTP_METHODS:
                raise ValueError(f"Invalid method '{method}' for {node['id']}")

            logger.info(f"Executing {node['id']} ({node['engine']}): {method} {url} (corr: {correlation_id})")
            
            response = requests.request(
                method=method,
                url=url,
                headers=headers,
                json=payload,
                timeout=timeout
            )
            response.raise_for_status()
            
            result = {
                "node_id": node["id"],
                "status": "success",
                "engine": node["engine"],
                "response_status": response.status_code,
                "response_body": response.json() if response.content else {},
                "correlation_id": correlation_id
            }
            logger.info(f"Success {node['id']}: {response.status_code}")
            return result

        except requests.exceptions.RequestException as e:
            logger.error(f"HTTP error for {node['id']}: {str(e)}")
            raise AirflowException(f"Request failed for {node['id']}: {str(e)}")
        except Exception as e:
            logger.error(f"Failed {node['id']}: {str(e)}")
            raise AirflowException(f"Execution failed for {node['id']}: {str(e)}")

    @task
    def finalize_results(results: List[Dict[str, Any]], **context):
        successes = sum(1 for r in results if r["status"] == "success")
        total = len(results)
        correlation_id = results[0]["correlation_id"] if results else "unknown"
        
        final_status = {
            "success_count": successes,
            "total_tasks": total,
            "correlation_id": correlation_id,
            "failures": [r for r in results if r["status"] != "success"]
        }
        
        logger.info(f"Finalized: {successes}/{total} (corr: {correlation_id})")
        # TODO: Publish to Kafka
        # producer.send(KAFKA_TOPIC_RUN, value=json.dumps(final_status).encode('utf-8'))
        return final_status

    @task
    def publish_final_status(final_result: Dict[str, Any], **context):
        logger.info(f"Published final status: {final_result}")
        # Cleanup if needed
        return "DAG_COMPLETED"

    # Workflow
    nodes = validate_conf_and_map_nodes()
    dynamic_tasks = execute_node.expand(node=nodes)
    
    # Chain by sequence_id (serial for now; group parallel if needed)
    for i in range(1, len(nodes) + 1):
        if i == 1:
            nodes >> dynamic_tasks.filter(lambda task: task["executor_sequence_id"] == str(i))
        else:
            dynamic_tasks.filter(lambda task: task["executor_sequence_id"] == str(i-1)) >> \
            dynamic_tasks.filter(lambda task: task["executor_sequence_id"] == str(i))
    
    finalize_results_task = finalize_results(dynamic_tasks)
    publish_final_status(finalize_results_task)

# Instantiate DAG
dag_obj = create_dynamic_dag()
