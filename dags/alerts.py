"""Failure alerting for the stock pipeline DAG.

Airflow calls notify_failure when a task has failed for good, after its last retry. The message is
always logged. If ALERT_WEBHOOK_URL is set, it is also posted there as {"text": ...}, the format
Slack incoming webhooks accept. This module does not import Airflow, so it can be unit tested alone.
"""

import json
import logging
import os
import urllib.request

logger = logging.getLogger("stock_pipeline.alerts")


def failure_message(context):
    """Build a short, readable alert from the Airflow task context."""
    ti = context["task_instance"]
    return (
        f"stock_pipeline task failed: {ti.task_id}\n"
        f"run: {context['run_id']} (logical date {context['ds']})\n"
        f"attempt: {ti.try_number}\n"
        f"error: {context.get('exception')}\n"
        f"log: {ti.log_url}"
    )


def notify_failure(context):
    """on_failure_callback: log the failure and, if configured, send it to a webhook."""
    message = failure_message(context)
    logger.error(message)

    url = os.getenv("ALERT_WEBHOOK_URL")
    if not url:
        return
    request = urllib.request.Request(
        url,
        data=json.dumps({"text": message}).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        urllib.request.urlopen(request, timeout=10)
    except Exception as exc:  # noqa: BLE001 - a broken alert must not hide the original task failure
        logger.error("could not send failure alert: %s", exc)
