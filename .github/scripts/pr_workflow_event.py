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

"""Bind untrusted PR event hints to an Actions run and authoritative GitHub data."""

import argparse
import json
import os
from pathlib import Path
import re

from review_github_api import decode_json, run_gh

SHA = re.compile(r"[0-9a-f]{40}")
ACTIONS = {"opened", "synchronize", "reopened", "ready_for_review", "edited", "closed"}
RELAY_PATH = ".github/workflows/pull-request-events.yml"


def require(condition, message):
    if not condition:
        raise ValueError(message)


def get(endpoint, *, pages=False):
    command = ["gh", "api", endpoint]
    if pages:
        command += ["--paginate", "--slurp"]
    result = run_gh(command, check=True, capture_output=True, text=True, timeout=90)
    data = decode_json(result, command)
    return [item for page in data for item in page] if pages else data


def resolve(event, relay, repository, api=get):
    run = event["workflow_run"]
    require(run["event"] == "pull_request" and run["conclusion"] == "success",
            "Only successful PR relay runs are accepted.")
    require(run["repository"]["full_name"] == repository, "Relay repository mismatch.")
    require(run["path"] == RELAY_PATH, "Unexpected relay workflow path.")
    number = relay["number"]
    require(type(number) is int and number > 0, "Invalid relayed PR number.")
    action = relay["action"]
    require(isinstance(action, str) and action in ACTIONS, "Unsupported relayed PR action.")
    head = relay["head_sha"]
    before = relay["before"]
    require(isinstance(head, str) and SHA.fullmatch(head), "Invalid relayed head SHA.")
    require(isinstance(before, str) and (not before or SHA.fullmatch(before)), "Invalid previous head SHA.")

    pr = api(f"repos/{repository}/pulls/{number}")
    require(pr["number"] == number and pr["base"]["repo"]["full_name"] == repository,
            "Relayed PR repository mismatch.")
    # The Actions API can omit pull_requests for fork runs. Resolve their
    # association through GitHub's commit/PR API, never through the artifact.
    associated = run["pull_requests"] or api(
        f"repos/{repository}/commits/{run['head_sha']}/pulls?per_page=100", pages=True)
    if not any(item["number"] == number for item in associated):
        # GitHub's commit API lists open/merged PRs, so a closed, unmerged fork
        # can be absent from both lists. Its source repository/branch still
        # identifies the PR, including a stale run whose head has since moved.
        source_repo = run["head_repository"]
        pr_repo = pr["head"]["repo"]
        require(pr["head"]["ref"] == run["head_branch"] and (
            (pr_repo is not None and source_repo is not None and pr_repo["id"] == source_repo["id"])
            or (pr_repo is None and pr["state"] == "closed" and pr["head"]["sha"] == run["head_sha"])),
            "The relayed PR does not belong to the triggering run.")
    require(head == run["head_sha"] or (
        action == "closed" and pr["merged"] and run["head_sha"] == pr["merge_commit_sha"]
        and head == pr["head"]["sha"]), "Relayed head does not match the triggering run.")

    # These SHAs only select commits to recompute from live PRs and status
    # sources. In particular, a forged 'before' cannot grant review approval.
    heads = list(dict.fromkeys([pr["head"]["sha"], head] +
                              ([before] if action == "synchronize" and before else [])))
    require(all(isinstance(sha, str) and SHA.fullmatch(sha) for sha in heads), "Invalid PR head SHA.")
    return {"pr_number": str(number), "head_shas": json.dumps(heads),
            "check_large_files": str(pr["state"] == "open" and
                                     action in {"opened", "synchronize", "reopened"}).lower()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--relay-file", type=Path, required=True)
    args = parser.parse_args()
    require(os.environ["GITHUB_EVENT_NAME"] == "workflow_run", "A workflow_run event is required.")
    values = resolve(json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text()),
                     json.loads(args.relay_file.read_text()), os.environ["GITHUB_REPOSITORY"])
    for name, value in values.items():
        print(f"{name}={value}")


if __name__ == "__main__":
    main()
