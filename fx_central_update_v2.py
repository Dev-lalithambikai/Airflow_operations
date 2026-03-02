"""
Airflow DAG for REST API workflow with Kafka integration (3 REST API calls).

This DAG:
1. Validates and prepares request payload in prepare_inputs (correlation_id, global, Task1..Task3)
2. Prepares normalized inputs (global defaults merged; url/method required; headers, json, timeout, verify_ssl optional)
3. Makes 3 REST API calls
4. Publishes Kafka messages at start and end
5. Processes and finalizes results
"""

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import jsonschema
from airflow.models.dagrun import DagRun
from jsonschema import ValidationError
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.sdk.definitions.context import get_current_context
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'trigger_rule': TriggerRule.ALL_SUCCESS,
}

logger = logging.getLogger(__name__)

KAFKA_EXECUTION_TIMEOUT = timedelta(seconds=120)
KAFKA_POLL_TIMEOUT = 60.0
DEFAULT_CORRELATION_ID = "UNKNOWN"
EVENT_TYPE_RUN_STARTED = "run.started.v1"
EVENT_TYPE_RUN_SUCCEEDED = "run.succeeded.v1"
EVENT_TYPE_RUN_FAILED = "run.failed.v1"
EVENT_STATUS_RUNNING = "RUNNING"
EVENT_STATUS_SUCCEEDED = "SUCCEEDED"
EVENT_STATUS_FAILED = "FAILED"
OUTCOME_SUCCESS = "success"
OUTCOME_FAILED = "failed"
TASK_ID_FINALIZE_RESULTS = "finalize_results"

DEFAULT_TIMEOUT_SECONDS = 30
DEFAULT_VERIFY_SSL = True
VALID_HTTP_METHODS = {'GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'HEAD', 'OPTIONS'}

_TASK_PROPERTIES = {
    'url': {'type': 'string', 'minLength': 1},
    'method': {'type': 'string'},
    'headers': {'type': 'object'},
    'json': {'type': ['object', 'array', 'string', 'number', 'boolean', 'null']},
    'timeout': {'type': 'number', 'minimum': 1},
    'verify_ssl': {'type': 'boolean'},
}

CONFIG_SCHEMA = {
    'type': 'object',
    'properties': {
        'correlation_id': {'type': 'string', 'minLength': 1},
        'global': {
            'type': 'object',
            'properties': {
                'headers': {'type': 'object'},
                'timeout': {'type': 'number', 'minimum': 1},
                'verify_ssl': {'type': 'boolean'},
            },
            'additionalProperties': True,
        },
        **{f'Task{n}': {
            'type': 'object',
            'required': ['url', 'method'],
            'properties': _TASK_PROPERTIES,
            'additionalProperties': True,
        } for n in range(1, 4)},
    },
    'required': ['correlation_id', 'Task1', 'Task2', 'Task3'],
    'additionalProperties': True,
}


def _merge_headers(
    correlation_id: str,
    global_headers: Optional[Dict[str, str]] = None,
    task_headers: Optional[Dict[str, str]] = None,
) -> Dict[str, str]:
    base = {'X-Correlation-Id': correlation_id, 'Content-Type': 'application/json'}
    if isinstance(global_headers, dict):
        base.update(global_headers)
    if isinstance(task_headers, dict):
        base.update(task_headers)
    return base


def _validate_http_method(method: str) -> None:
    m = (method or '').upper()
    if m not in VALID_HTTP_METHODS:
        raise ValueError(f"Invalid HTTP method '{method}'. Supported: ({', '.join(sorted(VALID_HTTP_METHODS))})")


def _validate_url(url: str) -> None:
    if not isinstance(url, str) or not url.strip():
        raise ValueError("URL must be a non-empty string")
    u = url.lower().strip()
    if not (u.startswith('http://') or u.startswith('https://')):
        raise ValueError("URL must start with http:// or https://")


def _extract_global_defaults(config: Dict[str, Any]) -> Dict[str, Any]:
    g = config.get('global') or {}
    timeout = g.get('timeout', DEFAULT_TIMEOUT_SECONDS)
    if not isinstance(timeout, (int, float)) or timeout < 1:
        timeout = DEFAULT_TIMEOUT_SECONDS
    verify_ssl = g.get('verify_ssl', DEFAULT_VERIFY_SSL)
    if not isinstance(verify_ssl, bool):
        verify_ssl = DEFAULT_VERIFY_SSL
    headers = g.get('headers') if isinstance(g.get('headers'), dict) else {}
    return {'headers': headers or {}, 'timeout': float(timeout), 'verify_ssl': verify_ssl}


def _normalize_task_config(
    task_config: Dict[str, Any],
    task_key: str,
    global_defaults: Dict[str, Any],
    correlation_id: str,
) -> Dict[str, Any]:
    if not isinstance(task_config, dict):
        raise AirflowException(f"{task_key} configuration must be a dictionary")
    if 'url' not in task_config:
        raise AirflowException(f"{task_key} missing required field: url")
    if 'method' not in task_config:
        raise AirflowException(f"{task_key} missing required field: method")
    _validate_url(task_config['url'])
    _validate_http_method(task_config['method'])
    timeout = task_config.get('timeout', global_defaults['timeout'])
    if not isinstance(timeout, (int, float)) or timeout < 1:
        timeout = global_defaults['timeout']
    verify_ssl = task_config.get('verify_ssl', global_defaults['verify_ssl'])
    if not isinstance(verify_ssl, bool):
        verify_ssl = global_defaults['verify_ssl']
    headers = _merge_headers(
        correlation_id,
        global_defaults['headers'],
        task_config.get('headers'),
    )
    return {
        'url': task_config['url'].strip(),
        'method': task_config['method'].upper(),
        'headers': headers,
        'json': task_config.get('json'),
        'timeout': float(timeout),
        'verify_ssl': verify_ssl,
    }


def validate_request_payload(**context) -> Dict[str, Any]:
    """
    Load request payload from conf/params and validate against CONFIG_SCHEMA.
    Single source of payload for prepare_inputs (no XCom).
    """
    dag_run = context.get('dag_run')
    request_payload = dag_run.conf if dag_run and dag_run.conf else {}
    if not request_payload:
        request_payload = context.get('params', {})

    logger.debug("Received request payload: %s", json.dumps(request_payload, indent=2))

    if not isinstance(request_payload, dict):
        raise AirflowException("Configuration must be a dictionary")
    if not request_payload:
        raise AirflowException("Configuration is empty. Provide correlation_id and Task1..Task3.")

    try:
        jsonschema.validate(instance=request_payload, schema=CONFIG_SCHEMA)
    except ValidationError as e:
        msg = str(e)
        if e.path:
            msg += f" (at: {' -> '.join(str(p) for p in e.path)})"
        logger.error("Configuration validation failed: %s", msg)
        raise AirflowException(f"Configuration validation failed: {msg}") from e
    logger.info("Request payload validation successful")
    return request_payload


def prepare_inputs(**context) -> Dict[str, Any]:
    """
    Load validated payload via validate_request_payload(**context), apply global defaults,
    and normalize Task1..Task3 for downstream tasks (same pattern as single_rest_call_dag).
    """
    try:
        validated_payload = validate_request_payload(**context)
        correlation_id = (validated_payload.get('correlation_id') or '').strip() or 'UNKNOWN'
        global_defaults = _extract_global_defaults(validated_payload)

        normalized = {}
        for n in range(1, 4):
            key = f'Task{n}'
            normalized[key] = _normalize_task_config(
                validated_payload[key],
                key,
                global_defaults,
                correlation_id,
            )

        logger.info("Preparing inputs for REST API calls and Kafka")
        logger.debug("Task1 config: %s", json.dumps(normalized['Task1'], indent=2))
        logger.debug("Task2 config: %s", json.dumps(normalized['Task2'], indent=2))
        logger.debug("Task3 config: %s", json.dumps(normalized['Task3'], indent=2))
        return normalized
    except AirflowException:
        raise
    except Exception as e:
        logger.error("Unexpected error in prepare_inputs: %s", str(e), exc_info=True)
        raise AirflowException(f"Failed to prepare inputs: {str(e)}") from e


def finalize_results(**context) -> Dict[str, Any]:
    """
    Finalize and aggregate results from Task1..Task3.

    This function is designed to run regardless of upstream task success:
    it collects whatever results are available and records missing ones
    in a failed_tasks list instead of failing itself.
    """
    try:
        ti = context.get("ti")
        if not ti:
            raise AirflowException("Missing 'ti' (TaskInstance) in context")

        task_ids = [
            "task1_rest_api_call",
            "task2_rest_api_call",
            "task3_rest_api_call",
        ]
        results: Dict[str, Any] = {}
        failed_tasks: List[str] = []

        for task_id in task_ids:
            result = ti.xcom_pull(task_ids=task_id)
            results[f"{task_id.replace('_rest_api_call', '')}_result"] = result
            if result is None:
                failed_tasks.append(task_id)

        logger.info("Finalizing results from REST API calls")
        for key, value in results.items():
            logger.debug("%s: %s", key, json.dumps(value, indent=2) if value is not None else "None")

        status = "completed" if not failed_tasks else "completed_with_failures"

        finalized_results: Dict[str, Any] = {
            "task1_result": results.get("task1_result"),
            "task2_result": results.get("task2_result"),
            "task3_result": results.get("task3_result"),
            "failed_tasks": failed_tasks,
            "timestamp": datetime.now().isoformat(),
            "status": status,
        }
        logger.debug("Finalized results: %s", json.dumps(finalized_results, indent=2))
        return finalized_results
    except AirflowException:
        raise
    except Exception as e:
        logger.error("Unexpected error in finalize_results: %s", str(e), exc_info=True)
        raise AirflowException(f"Failed to finalize results: {str(e)}") from e


def _now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


def _build_kafka_envelope(
    event_type: str,
    event_status: str,
    outcome: Optional[str] = None,
    summary: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Build Kafka message envelope with workflow metadata.
    Uses get_current_context() from Airflow (no context parameter).
    Same implementation as single_rest_call_dag.
    """
    try:
        context = get_current_context()
        if not isinstance(context, dict):
            raise ValueError("Context must be a dictionary")
        dag_run: Optional[DagRun] = context.get("dag_run")
        conf = (dag_run.conf or {}) if dag_run else {}
        if not isinstance(conf, dict):
            conf = {}

        payload: Dict[str, Any] = {
            "eventType": event_type,
            "run_control_id": conf.get("run_control_id") or "",
            "correlation_id": conf.get("correlation_id", "UNKNOWN"),
            "event_source": "AIRFLOW",
            "status": event_status,
            "trigger_payload": conf.get("trigger_payload") or "{}",
            "dagId": context["dag"].dag_id,
            "dagRunId": context["run_id"],
            "timestamp": _now_iso(),
        }
        if outcome is not None:
            payload["outcome"] = outcome
        if summary is not None:
            logger.info(f"Final summary of tasks : {summary}")
            payload["summary"] = summary

        return json.dumps(payload, default=str)
    except Exception as e:
        logger.error("Error building Kafka envelope: %s", str(e), exc_info=True)
        raise AirflowException(f"Failed to build Kafka message: {str(e)}") from e


def kafka_producer_start(**kwargs: Any):
    """Kafka workflow start message. Uses get_current_context() inside _build_kafka_envelope."""
    try:
        message_json = _build_kafka_envelope(
            event_type=EVENT_TYPE_RUN_STARTED,
            event_status=EVENT_STATUS_RUNNING,
        )
        return [(None, message_json.encode("utf-8"))]
    except Exception as e:
        logger.error("Error in kafka_producer_start: %s", str(e), exc_info=True)
        raise AirflowException(f"Failed to build Kafka start message: {str(e)}") from e


def kafka_producer_end(**kwargs: Any):
    """Kafka workflow end message. Uses get_current_context() for ti and envelope."""
    try:
        context = get_current_context()
        ti = context.get("ti") if isinstance(context, dict) else None
        if not ti:
            raise AirflowException("Missing 'ti' (TaskInstance) in context")

        final_summary = ti.xcom_pull(task_ids=TASK_ID_FINALIZE_RESULTS) or {}
        if not isinstance(final_summary, dict):
            final_summary = {}

        failed_tasks = final_summary.get("failed_tasks", [])
        if not isinstance(failed_tasks, list):
            failed_tasks = []

        if failed_tasks:
            event_type = EVENT_TYPE_RUN_FAILED
            event_status = EVENT_STATUS_FAILED
            outcome = OUTCOME_FAILED
        else:
            event_type = EVENT_TYPE_RUN_SUCCEEDED
            event_status = EVENT_STATUS_SUCCEEDED
            outcome = OUTCOME_SUCCESS

        message_json = _build_kafka_envelope(
            event_type=event_type,
            event_status=event_status,
            outcome=outcome,
            summary=final_summary,
        )
        return [(None, message_json.encode("utf-8"))]
    except AirflowException:
        raise
    except Exception as e:
        logger.error("Error in kafka_producer_end: %s", str(e), exc_info=True)
        raise AirflowException(f"Failed to build Kafka end message: {str(e)}") from e


def _execute_api_call(context, task_key: str, task_label: str) -> Dict[str, Any]:
    import requests
    ti = context.get('ti')
    if not ti:
        raise AirflowException("Missing 'ti' (TaskInstance) in context")
    payload = ti.xcom_pull(task_ids='prepare_inputs')
    if not payload:
        raise AirflowException("No payload from prepare_inputs; cannot run REST API call")
    if task_key not in payload:
        raise AirflowException(f"Payload missing {task_key}; cannot run REST API call")

    config = payload[task_key]
    url = config['url']
    method = config['method']
    headers = config.get('headers') or {}
    json_payload = config.get('json')
    verify_ssl = config.get('verify_ssl', DEFAULT_VERIFY_SSL)
    timeout = config.get('timeout', DEFAULT_TIMEOUT_SECONDS)
    logger.info("%s: Making %s request to %s", task_label, method, url)
    try:
        response = requests.request(
            method=method, url=url, headers=headers, json=json_payload,
            verify=verify_ssl, timeout=timeout
        )
        response.raise_for_status()
        result = {
            'status_code': response.status_code,
            'headers': dict(response.headers),
            'response': response.json() if response.content else None
        }
        logger.info("%s completed successfully: status_code=%s", task_label, result['status_code'])
        return result
    except requests.exceptions.HTTPError as e:
        logger.error("%s HTTP error: %s", task_label, str(e))
        raise AirflowException(f"{task_label} HTTP request failed: {str(e)}") from e
    except requests.exceptions.ConnectionError as e:
        logger.error("%s connection error: %s", task_label, str(e))
        raise AirflowException(f"{task_label} HTTP request failed: {str(e)}") from e
    except requests.exceptions.RequestException as e:
        logger.error("%s request failed: %s", task_label, str(e))
        raise AirflowException(f"{task_label} HTTP request failed: {str(e)}") from e
    except Exception as e:
        logger.error("Unexpected error in %s: %s", task_label, str(e), exc_info=True)
        raise AirflowException(f"{task_label} failed: {str(e)}") from e


def execute_task1_api_call(**context) -> Dict[str, Any]:
    return _execute_api_call(context, 'Task1', 'Task1')


def execute_task2_api_call(**context) -> Dict[str, Any]:
    return _execute_api_call(context, 'Task2', 'Task2')


def execute_task3_api_call(**context) -> Dict[str, Any]:
    return _execute_api_call(context, 'Task3', 'Task3')


dag = DAG(
    'fx_central_dag',
    default_args=default_args,
    description='DAG with 3 REST API calls and Kafka integration',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['rest_api', 'kafka', 'fx_central_dag'],
)

# Task 1: Publish Kafka "running" status first (before validation; builds message in producer)
publish_kafka_start_task = ProduceToTopicOperator(
    task_id='publish_kafka_start',
    topic='genesis.hub.run.events.v1',
    kafka_config_id='genesis_kafka_conn',
    producer_function=kafka_producer_start,
    execution_timeout=KAFKA_EXECUTION_TIMEOUT,
    poll_timeout=KAFKA_POLL_TIMEOUT,
    dag=dag,
)

# Prepare inputs (validate + normalize; same pattern as single_rest_call_dag)
prepare_inputs_task = PythonOperator(
    task_id='prepare_inputs',
    python_callable=prepare_inputs,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

task1_rest_api_call = PythonOperator(
    task_id='task1_rest_api_call',
    python_callable=execute_task1_api_call,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

task2_rest_api_call = PythonOperator(
    task_id='task2_rest_api_call',
    python_callable=execute_task2_api_call,
    dag=dag,
)

task3_rest_api_call = PythonOperator(
    task_id='task3_rest_api_call',
    python_callable=execute_task3_api_call,
    dag=dag,
)

finalize_results_task = PythonOperator(
    task_id='finalize_results',
    python_callable=finalize_results,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

publish_final_status_task = ProduceToTopicOperator(
    task_id='publish_final_status',
    topic='genesis.hub.run.events.v1',
    kafka_config_id='genesis_kafka_conn',
    producer_function=kafka_producer_end,
    trigger_rule=TriggerRule.ALL_DONE,
    execution_timeout=KAFKA_EXECUTION_TIMEOUT,
    poll_timeout=KAFKA_POLL_TIMEOUT,
    dag=dag,
)

# Kafka start -> prepare_inputs -> REST -> finalize -> Kafka end (like single_rest_call_dag)
publish_kafka_start_task >> prepare_inputs_task >> task1_rest_api_call >> task2_rest_api_call >> task3_rest_api_call >> finalize_results_task >> publish_final_status_task
