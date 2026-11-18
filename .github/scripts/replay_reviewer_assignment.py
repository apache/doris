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

"""Replay the production reviewer selection path locally without GitHub mutations."""

import argparse
import json
from pathlib import Path
import shutil
import sys
import tempfile
import time

import reviewer_assignment as assignment
from reviewer_assignment_diagnostics import diagnostics_report, read_text
from reviewer_assignment_model import run_model
from reviewer_assignment_usage import usage_report


class ReadOnlyGitHub(assignment.GitHub):
    def request(self, method, *args, **kwargs):
        assignment.require(method == "GET", "Local replay cannot mutate GitHub.")
        return super().request(method, *args, **kwargs)


def replay(workspace, codex, snapshot=None, number=None, repository="apache/doris", timeout=480):
    assignment.require(not workspace.exists(), "Use a new workspace so replay cannot overwrite an earlier run.")
    workspace.parent.mkdir(parents=True, exist_ok=True)
    if snapshot is not None:
        workspace.mkdir()
        for name in ("context.json", "pr.json", "pr-details.json", "pr.diff"):
            shutil.copyfile(snapshot / name, workspace / name)
        shutil.copytree(assignment.TOOL, workspace / "tools/reviewer-routing")
        assignment.model_input(workspace, workspace / "tools/reviewer-routing")
    else:
        event = {"action": "labeled", "label": {"name": assignment.REQUEST_LABEL},
                 "pull_request": {"number": number}}
        reused = assignment.prepare(ReadOnlyGitHub(), event, repository, str(time.time_ns()), workspace)
        assignment.require(not reused, "A new local run cannot reuse a hosted checkpoint.")
    output = workspace / "runner"
    code = 1
    try:
        code = run_model(workspace, codex, output, timeout)
        if code:
            return code
        people = assignment.finalize_selection(workspace, workspace / "tools/reviewer-routing")
        print(json.dumps({"workspace": str(workspace), "reviewers": people,
                          "fallback": not people, "github_mutations": False}))
        return 0
    finally:
        events = read_text(output / "reviewer-assignment-codex.jsonl")
        report = diagnostics_report(workspace, events, read_text(output / "reviewer-assignment-codex.stderr"))
        report["github_mutations"] = False
        assignment.write_json(workspace / "diagnostics.json", report)
        run = json.loads((workspace / "model-run.json").read_text()) if (workspace / "model-run.json").exists() else {}
        usage = usage_report(events, "success" if code == 0 else "failure", run.get("elapsed_seconds"), code)
        assignment.write_json(workspace / "usage.json", usage)
        print("Replay diagnostics: " + str(workspace / "diagnostics.json"), file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--pr", type=int)
    source.add_argument("--snapshot", type=Path)
    parser.add_argument("--repository", default="apache/doris")
    parser.add_argument("--workspace", type=Path)
    parser.add_argument("--codex", type=Path, required=True)
    parser.add_argument("--timeout", type=float, default=480)
    args = parser.parse_args()
    if args.pr is not None:
        assignment.require(args.pr > 0, "PR number must be positive.")
    workspace = args.workspace
    if workspace is None:
        parent = Path(tempfile.mkdtemp(prefix="reviewer-assignment-replay-"))
        workspace = parent / "workspace"
    try:
        code = replay(workspace.resolve(), args.codex, args.snapshot, args.pr, args.repository, args.timeout)
    except (ValueError, OSError) as error:
        print(str(error), file=sys.stderr)
        code = 1
    raise SystemExit(code)


if __name__ == "__main__":
    main()
