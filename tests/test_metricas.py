"""Metricas hacia CloudWatch: no-op en local, publicacion en modo AWS."""

from __future__ import annotations

import logging

import boto3

from src import metrics


class _FakeCloudWatch:
    def __init__(self) -> None:
        self.calls = []

    def put_metric_data(self, **kwargs) -> None:
        self.calls.append(kwargs)


def test_en_local_solo_escribe_en_el_log(monkeypatch, caplog) -> None:
    monkeypatch.delenv("CLOUDWATCH_METRICS_ENABLED", raising=False)
    fake = _FakeCloudWatch()
    monkeypatch.setattr(boto3, "client", lambda *_args, **_kw: fake)

    with caplog.at_level(logging.INFO):
        metrics.emit_batch_metrics(
            source="order_events", batch_id="events_x",
            rows_read=200, rows_rejected=4, rows_duplicated=2,
        )

    assert fake.calls == []
    assert "RejectionRatePct" in caplog.text


def test_en_aws_publica_tasas_en_cloudwatch(monkeypatch) -> None:
    monkeypatch.setenv("CLOUDWATCH_METRICS_ENABLED", "true")
    fake = _FakeCloudWatch()
    monkeypatch.setattr(boto3, "client", lambda *_args, **_kw: fake)

    metrics.emit_batch_metrics(
        source="order_events", batch_id="events_x",
        rows_read=200, rows_rejected=4, rows_duplicated=2,
    )
    metrics.emit_quality_result(ingest_date="2026-10-22", passed=True)

    published = {
        item["MetricName"]: item["Value"]
        for call in fake.calls
        for item in call["MetricData"]
    }
    assert published["RejectionRatePct"] == 2.0
    assert published["DuplicateRatePct"] == 1.0
    assert published["BatchesPublished"] == 1.0
    assert published["QualityGateFailed"] == 0.0
