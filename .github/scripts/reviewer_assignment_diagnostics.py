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

"""Publish routing decisions and filtered execution diagnostics, never raw sessions."""

import argparse
from collections import Counter
import json
import os
from pathlib import Path

from review_github_api import redact

FILES = ("context.json", "candidate-summary.json", "module-selection.json",
         "selected-candidates.json", "selection.json", "input-manifest.json", "model-run.json", "validation.json")
LIMIT = 4096


def read_text(path):
    return path.read_text(errors="replace") if path.exists() else ""


def document(path):
    if not path.exists():
        return {"status": "missing"}
    text = redact(read_text(path))
    try:
        return {"status": "available", "value": json.loads(text)}
    except json.JSONDecodeError as error:
        return {"status": "unparsed", "error": str(error), "text_excerpt": text[:LIMIT]}


def diagnostics_report(workspace, events, stderr):
    commands = {}
    tools = {}
    event_types, item_types = Counter(), Counter()
    report = {"schema_version": 1,
              "files": {name: document(workspace / name) for name in FILES},
              "commands": [], "tool_calls": [], "model_errors": [], "malformed_event_lines": 0,
              "stderr_tail": redact(stderr)[-LIMIT:]}
    for line in events.splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            # A timeout can interrupt the final JSONL write.
            report["malformed_event_lines"] += 1
            continue
        if not isinstance(event, dict):
            continue
        event_types[event.get("type", "missing")] += 1
        if event.get("type") in {"error", "turn.failed"}:
            report["model_errors"].append(redact(json.dumps(event.get("error", event.get("message"))))[:LIMIT])
        item = event.get("item", {})
        if item:
            item_types[item.get("type", "missing")] += 1
        if item.get("type") == "error":
            report["model_errors"].append(redact(item["message"])[:LIMIT])
        if item.get("type") == "mcp_tool_call":
            tool = {"server": item["server"], "tool": item["tool"], "status": item["status"]}
            if item.get("error"):
                tool["error"] = redact(json.dumps(item["error"]))[:LIMIT]
            tools[item["id"]] = tool
        if (event.get("type") not in {"item.started", "item.completed"}
                or item.get("type") != "command_execution"):
            continue
        command = {"command": redact(item["command"])[:LIMIT], "status": item["status"],
                   "exit_code": item.get("exit_code")}
        if event["type"] == "item.completed" and item.get("exit_code") != 0:
            command["output_tail"] = redact(item.get("aggregated_output"))[-LIMIT:]
        commands[item["id"]] = command
    report["commands"] = list(commands.values())
    report["tool_calls"] = list(tools.values())
    report["event_types"], report["item_types"] = dict(event_types), dict(item_types)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("workspace", "events", "stderr", "version", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    args = parser.parse_args()
    report = diagnostics_report(args.workspace, read_text(args.events), read_text(args.stderr))
    report.update(cli_version=redact(read_text(args.version)).strip()[:LIMIT],
                  reused_selection=os.environ.get("REUSE_SELECTION") == "true")
    data = json.dumps(report, indent=2) + "\n"
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(data)
    print(data, end="")


if __name__ == "__main__":
    main()
