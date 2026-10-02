"""Tests — :mod:`statflows.core.reports`.

Frozen behaviour: dataclasses / mappings flattened to dotted finite floats
(booleans cast, strings and non-finite values dropped, forbidden characters
sanitised), HTTP counters, and run-level aggregation of per-query reports.
"""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from statflows.core.reports import (
    DownloadReport,
    FetchReport,
    HttpStats,
    QueryReport,
    RateLimitStats,
    _walk,
    flatten_metrics,
)

# ──────────────────────────────────────────────────────────────────────
# _walk / flatten_metrics
# ──────────────────────────────────────────────────────────────────────


@dataclass
class _Inner:
    count: int = 2
    ratio: float = 0.5


@dataclass
class _Outer:
    inner: _Inner
    flag: bool = True
    label: str = "text"


def test_walk_flattens_dataclasses_and_mappings() -> None:
    walked = _walk(_Outer(_Inner()), "run")
    assert walked == {
        "run.inner.count": 2,
        "run.inner.ratio": 0.5,
        "run.flag": True,
        "run.label": "text",
    }
    assert _walk({"a": {"b": 1}, 3: 2}, "") == {"a.b": 1, "3": 2}
    assert _walk(5, "leaf") == {"leaf": 5}


def test_flatten_metrics_keeps_only_finite_numbers() -> None:
    payload = {
        "ok": 3,
        "flag": True,
        "text": "x",
        "none": None,
        "nan": float("nan"),
        "inf": float("inf"),
        "seq": [1, 2],
        "bad key!": 1.5,
    }
    assert flatten_metrics(payload, prefix="p") == {
        "p.ok": 3.0,
        "p.flag": 1.0,
        "p.bad key_": 1.5,
    }


# ──────────────────────────────────────────────────────────────────────
# HttpStats
# ──────────────────────────────────────────────────────────────────────


def test_http_stats_record_failure_and_status_histogram() -> None:
    stats = HttpStats()
    stats.record(200, 0.5, 100)
    stats.record(200, 0.25, 50)
    stats.record_failure(500, 1.0)
    stats.record_failure(None, 0.1)

    assert stats.n_requests == 4
    assert stats.n_failures == 2
    assert stats.total_seconds == pytest.approx(1.85)
    assert stats.total_bytes == 150
    assert stats.status_counts == {"200": 2, "500": 1, "error": 1}


def test_http_stats_snapshot_is_detached_and_reset_is_in_place() -> None:
    stats = HttpStats()
    stats.record(200, 1.0, 10)
    snapshot = stats.snapshot()

    stats.reset()

    assert snapshot.n_requests == 1
    assert snapshot.status_counts == {"200": 1}
    assert (stats.n_requests, stats.total_bytes, stats.status_counts) == (0, 0, {})


# ──────────────────────────────────────────────────────────────────────
# QueryReport / DownloadReport
# ──────────────────────────────────────────────────────────────────────


def _query_report(**overrides) -> QueryReport:
    report = QueryReport(
        identity_key="k",
        dataflow="DF",
        fetch=FetchReport(n_requests=2, rows_fetched=10, n_duplicates=1),
        http=HttpStats(n_requests=3, n_failures=1, total_seconds=2.0, total_bytes=30),
        rate_limit=RateLimitStats(n_acquisitions=2, total_wait_seconds=1.5),
    )
    for name, value in overrides.items():
        setattr(report, name, value)
    return report


def test_query_report_to_metrics_flattens_nested_reports() -> None:
    metrics = _query_report(rows_written=7, empty=True).to_metrics(prefix="q")
    assert metrics["q.rows_written"] == 7.0
    assert metrics["q.empty"] == 1.0
    assert metrics["q.fetch.rows_fetched"] == 10.0
    assert metrics["q.http.total_bytes"] == 30.0
    # Les champs textuels ne sont pas des métriques
    assert "q.identity_key" not in metrics


def test_download_report_aggregates_sum_over_queries() -> None:
    report = DownloadReport(queries=[_query_report(), _query_report()])
    aggregates = report.aggregates()
    assert aggregates["n_requests"] == 4.0
    assert aggregates["rows_fetched"] == 20.0
    assert aggregates["n_duplicates"] == 2.0
    assert aggregates["n_http_failures"] == 2.0
    assert aggregates["rate_limit_wait_seconds"] == 3.0


def test_download_report_to_metrics_has_counters_and_aggregates_but_no_detail() -> None:
    report = DownloadReport(processed=2, rows_written=9, queries=[_query_report()])
    metrics = report.to_metrics()
    assert metrics["download.processed"] == 2.0
    assert metrics["download.rows_written"] == 9.0
    assert metrics["download.rows_fetched"] == 10.0
    assert not any(key.startswith("download.queries") for key in metrics)


def test_download_report_to_frame_has_one_row_per_query() -> None:
    frame = DownloadReport(queries=[_query_report(), _query_report()]).to_frame()
    assert len(frame) == 2
    assert "fetch.rows_fetched" in frame.columns
    assert frame["dataflow"].tolist() == ["DF", "DF"]


def test_empty_download_report_to_frame_is_empty() -> None:
    assert DownloadReport().to_frame().empty
