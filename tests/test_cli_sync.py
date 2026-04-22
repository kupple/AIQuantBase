from __future__ import annotations

import json
import sys

from aiquantbase import cli


def test_build_parser_parses_sync_run_task_arguments():
    args = cli.build_parser().parse_args(
        [
            "sync-run-task",
            "daily_kline",
            "--codes",
            "000001.SZ,600000.SH",
            "--begin-date",
            "20240101",
            "--end-date",
            "20240131",
            "--limit",
            "10",
            "--force",
            "--resume",
            "--log-level",
            "DEBUG",
        ]
    )

    assert args.command == "sync-run-task"
    assert args.name == "daily_kline"
    assert args.codes == "000001.SZ,600000.SH"
    assert args.begin_date == 20240101
    assert args.end_date == 20240131
    assert args.limit == 10
    assert args.force is True
    assert args.resume is True
    assert args.log_level == "DEBUG"


def test_main_sync_list_configs_outputs_json(monkeypatch, capsys):
    class StubIntegration:
        def list_configs(self):
            return ["run_sync.full.toml", "run_sync.example.toml"]

    monkeypatch.setattr(cli, "build_sync_service", lambda *_args, **_kwargs: StubIntegration())
    monkeypatch.setattr(sys, "argv", ["aiquantbase", "sync-list-configs"])

    cli.main()

    payload = json.loads(capsys.readouterr().out)
    assert payload == {"configs": ["run_sync.full.toml", "run_sync.example.toml"]}


def test_main_sync_run_task_dispatches_to_integration(monkeypatch, capsys):
    calls: list[dict] = []

    class StubIntegration:
        def run_task(self, **kwargs):
            calls.append(kwargs)
            return {"job_id": "job-123", "task_metadata": None}

    monkeypatch.setattr(cli, "build_sync_service", lambda *_args, **_kwargs: StubIntegration())
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "aiquantbase",
            "sync-run-task",
            "daily_kline",
            "--codes",
            "000001.SZ,600000.SH",
            "--begin-date",
            "20240101",
            "--limit",
            "5",
            "--resume",
        ],
    )

    cli.main()

    assert calls == [
        {
            "name": "daily_kline",
            "codes": ["000001.SZ", "600000.SH"],
            "begin_date": 20240101,
            "end_date": None,
            "limit": 5,
            "force": False,
            "resume": True,
            "log_level": None,
        }
    ]
    payload = json.loads(capsys.readouterr().out)
    assert payload["job_id"] == "job-123"
