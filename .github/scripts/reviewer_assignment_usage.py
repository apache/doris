#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.

"""Retain numeric Codex usage and timing without publishing raw model events."""

import argparse
import json
import os
from pathlib import Path

TOKEN_FIELDS = ("input_tokens", "cached_input_tokens", "output_tokens", "reasoning_output_tokens")


def usage_report(events, outcome, elapsed_seconds=None, exit_code=None, reused=False):
    status = "reused" if reused else "not_run" if outcome in {"", "skipped"} else outcome
    if exit_code == 124:
        status = "timed_out"
    report = {"schema_version": 1, "model": "gpt-6.1-sol", "reasoning_effort": "medium",
              "status": status, "elapsed_seconds": elapsed_seconds, "exit_code": exit_code,
              "completed_turns": 0, "malformed_event_lines": 0,
              "usage_complete": False, **{field: None for field in TOKEN_FIELDS}}
    if status in {"reused", "not_run"}:
        report.update(elapsed_seconds=0, usage_complete=True, uncached_input_tokens=0,
                      **{field: 0 for field in TOKEN_FIELDS})
        return report

    usages = []
    for line in events.splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            # A terminated process can leave a partial JSONL event at EOF.
            report["malformed_event_lines"] += 1
            continue
        if not isinstance(event, dict) or event.get("type") != "turn.completed":
            continue
        report["completed_turns"] += 1
        usage = event.get("usage")
        usages.append(usage if isinstance(usage, dict) else {})
    for field in TOKEN_FIELDS:
        values = [usage.get(field) for usage in usages]
        if values and all(type(value) is int and value >= 0 for value in values):
            report[field] = sum(values)
    total, cached = report["input_tokens"], report["cached_input_tokens"]
    if total is not None and cached is not None and cached > total:
        raise ValueError("Cached input tokens cannot exceed input tokens.")
    report["uncached_input_tokens"] = total - cached if total is not None and cached is not None else None
    report["usage_complete"] = (status == "success" and report["completed_turns"] > 0
                                and report["malformed_event_lines"] == 0
                                and all(report[field] is not None for field in TOKEN_FIELDS[:3]))
    return report


def markdown(report):
    rows = [("Model", report["model"]), ("Reasoning effort", report["reasoning_effort"]),
            ("Status", report["status"]), ("Codex elapsed seconds", report["elapsed_seconds"]),
            ("Exit code", report["exit_code"]), ("Completed turns", report["completed_turns"]),
            ("Input tokens (including cached)", report["input_tokens"]),
            ("Cached input tokens", report["cached_input_tokens"]),
            ("Uncached input tokens", report["uncached_input_tokens"]),
            ("Output tokens", report["output_tokens"]),
            ("Reasoning output tokens (when reported)", report["reasoning_output_tokens"])]
    lines = ["## Reviewer assignment usage", "", "| Metric | Value |", "|---|---:|"]
    lines.extend(f"| {name} | {value if value is not None else 'Unavailable'} |" for name, value in rows)
    lines += ["", "Cached input is part of input tokens; do not add it again.",
              "Reasoning output is reported separately when available; do not add it to output tokens.",
              "These are model token counts, not a price or a ChatGPT allowance estimate."]
    if not report["usage_complete"]:
        lines += ["", "Usage is incomplete: reported counts cover completed turns only. A failed or",
                  "interrupted request can consume tokens without a completion event; unavailable",
                  "counts are not zero."]
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--summary", type=Path, required=True)
    args = parser.parse_args()
    elapsed = os.environ.get("CODEX_ELAPSED_SECONDS", "")
    code = os.environ.get("CODEX_EXIT_CODE", "")
    report = usage_report(args.events.read_text() if args.events.exists() else "",
                          os.environ.get("CODEX_STEP_OUTCOME", ""),
                          int(elapsed) if elapsed else None, int(code) if code else None,
                          os.environ.get("REUSE_SELECTION") == "true")
    report.update(repository=os.environ["GITHUB_REPOSITORY"], run_id=os.environ["GITHUB_RUN_ID"],
                  run_attempt=os.environ["GITHUB_RUN_ATTEMPT"])
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    with args.summary.open("a") as summary:
        summary.write(markdown(report))


if __name__ == "__main__":
    main()
