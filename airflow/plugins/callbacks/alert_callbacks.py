import logging
from datetime import datetime

logger = logging.getLogger(__name__)


def on_task_failure(context):
    """
    Callback khi task fail.
    Trong production: gửi Slack/email/PagerDuty alert.
    """
    dag_id = context["dag"].dag_id
    task_id = context["task"].task_id
    execution_date = context["execution_date"]
    exception = context.get("exception")
    run_id = context["run_id"]

    logger.error(
        f"\n{'='*50}\n"
        f"❌ TASK FAILED\n"
        f"  DAG:       {dag_id}\n"
        f"  Task:      {task_id}\n"
        f"  Run ID:    {run_id}\n"
        f"  Exec date: {execution_date}\n"
        f"  Error:     {exception}\n"
        f"{'='*50}"
    )

    # TODO production: gửi Slack webhook
    # send_slack_alert(dag_id, task_id, exception)


def on_task_retry(context):
    """Callback khi task retry"""
    dag_id = context["dag"].dag_id
    task_id = context["task"].task_id
    try_number = context["task_instance"].try_number
    max_tries = context["task_instance"].max_tries

    logger.warning(
        f"⚠️  RETRY [{try_number}/{max_tries}] "
        f"{dag_id}.{task_id}"
    )


def on_dag_success(context):
    """Callback khi toàn bộ DAG success"""
    dag_id = context["dag"].dag_id
    execution_date = context["execution_date"]

    logger.info(
        f"✅ DAG SUCCESS: {dag_id} | {execution_date}"
    )


def on_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis):
    """Callback khi SLA bị miss"""
    logger.warning(
        f"⏰ SLA MISS: DAG={dag.dag_id} | "
        f"Tasks={[t.task_id for t in task_list]}"
    )