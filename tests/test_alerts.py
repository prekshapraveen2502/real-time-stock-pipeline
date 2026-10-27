"""Unit tests for the DAG failure alert, with Airflow's context and the network faked."""

import json

from dags import alerts


class FakeTaskInstance:
    task_id = "silver_clean"
    try_number = 4
    log_url = "http://localhost:8081/log?task_id=silver_clean"


CONTEXT = {
    "task_instance": FakeTaskInstance(),
    "run_id": "scheduled__2026-08-21T00:00:00+00:00",
    "ds": "2026-08-21",
    "exception": ImportError("numpy C-extensions failed"),
}


def test_failure_message_names_task_run_and_error():
    message = alerts.failure_message(CONTEXT)
    assert "silver_clean" in message
    assert "scheduled__2026-08-21T00:00:00+00:00" in message
    assert "attempt: 4" in message
    assert "numpy C-extensions failed" in message


def test_notify_failure_without_webhook_only_logs(monkeypatch):
    monkeypatch.delenv("ALERT_WEBHOOK_URL", raising=False)
    sent = []
    monkeypatch.setattr(alerts.urllib.request, "urlopen", lambda *a, **k: sent.append(a))
    alerts.notify_failure(CONTEXT)
    assert sent == []


def test_notify_failure_posts_to_webhook(monkeypatch):
    monkeypatch.setenv("ALERT_WEBHOOK_URL", "https://hooks.example.com/abc")
    sent = []
    monkeypatch.setattr(alerts.urllib.request, "urlopen", lambda request, timeout: sent.append(request))
    alerts.notify_failure(CONTEXT)
    assert len(sent) == 1
    assert sent[0].full_url == "https://hooks.example.com/abc"
    assert "silver_clean" in json.loads(sent[0].data)["text"]


def test_notify_failure_survives_a_broken_webhook(monkeypatch):
    monkeypatch.setenv("ALERT_WEBHOOK_URL", "https://hooks.example.com/abc")

    def boom(*args, **kwargs):
        raise OSError("network down")

    monkeypatch.setattr(alerts.urllib.request, "urlopen", boom)
    alerts.notify_failure(CONTEXT)   # must not raise
