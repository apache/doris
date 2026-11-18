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

"""Prepare module context and draw reviewers from configured candidate pools."""

import argparse
import collections
import itertools
import json
from pathlib import Path
import random

from classify_paths import classify_pr
from routing_config import load_routing, require


def path_context(pr, mapping):
    assert isinstance(pr["number"], int) and pr["number"] > 0
    assert isinstance(pr["author"], str) and pr["author"]
    files = [item if isinstance(item, dict) else {"path": item} for item in pr["files"]]
    assert files, "The complete changed-file list is required"
    assert len({item["path"] for item in files}) == len(files), "Duplicate file paths"
    if "changed_files" in pr:
        assert pr["changed_files"] == len(files), "Changed-file list is incomplete"
    classified = classify_pr([item["path"] for item in files], mapping["config"])
    wanted = set(classified["routing_modules"])
    by_path = {item["path"]: item for item in files}
    weights = collections.Counter()
    for item in classified["evidence"]:
        if item["module"] in wanted and (not classified["primary_modules"] or item["kind"] == "production"):
            file = by_path[item["path"]]
            weights[item["module"]] += max(1, min(100, file.get("additions", 0) + file.get("deletions", 0)))
    return {"routing_modules": sorted(wanted), "test_modules": classified["test_modules"],
            "module_weights": dict(sorted(weights.items()))}


def eligible_pools(pr, mapping, module_ids):
    modules = {module["id"]: module for module in mapping["modules"]}
    blocked = {pr["author"].lower(), *[login.lower() for login in pr.get("unavailable_reviewers", [])]}
    return {mid: [p["login"] for p in modules[mid]["candidates"] if p["login"].lower() not in blocked][:5]
            for mid in module_ids}


def sample_reviewers(selection, pr, mapping):
    """Draw uniformly from valid pairs; names inside each five-person pool are peers."""
    require(isinstance(selection, dict) and set(selection) == {"modules"},
            "Codex must return an object containing only modules.")
    selected = selection["modules"]
    require(isinstance(selected, list) and len(selected) <= 2,
            "Codex must select at most two modules.")
    require(all(isinstance(mid, str) for mid in selected), "Module IDs must be strings.")
    require(len(set(selected)) == len(selected), "Duplicate module IDs are not allowed.")
    require(set(selected) <= mapping["config"]["modules"].keys(), "Unknown routing module.")
    pools = [pool for pool in eligible_pools(pr, mapping, selected).values() if pool]
    people = {login.lower(): login for pool in pools for login in pool}
    if not people:
        return []
    covered = [{login.lower() for login in pool} for pool in pools]
    pairs = [pair for pair in itertools.combinations(people, min(2, len(people)))
             if all(set(pair) & pool for pool in covered)]
    return [people[login] for login in random.SystemRandom().choice(pairs)]


def candidate_summary(pr, mapping, module_ids=None):
    # CI's two-assignee budget is independent of existing formal reviewers.
    routing = path_context(pr, mapping)
    modules = {module["id"]: module for module in mapping["modules"]}
    wanted = sorted(set(routing["routing_modules"] if module_ids is None else module_ids))
    pools = eligible_pools(pr, mapping, wanted)
    existing = {p.lower() for p in pr.get("existing_reviewers", [])}
    summaries = []
    for mid in wanted:
        module = modules[mid]
        people = [{"login": login, "existing_reviewer": login.lower() in existing} for login in pools[mid]]
        summaries.append({"id": mid, "label": module["label"],
                          "candidates": people})
    result = {"path_routing_modules": routing["routing_modules"], "test_modules": routing["test_modules"],
              "existing_reviewers": pr.get("existing_reviewers", []), "modules": summaries,
              "fallback_reviewer": mapping["config"]["fallback_reviewer"],
              "disabled_reviewers": mapping["config"]["disabled_reviewers"]}
    if module_ids is None:
        result["path_module_weights"] = routing["module_weights"]
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path, nargs="?")
    parser.add_argument("--modules", nargs="+")
    parser.add_argument("--list-modules", action="store_true")
    args = parser.parse_args()
    mapping = load_routing()
    if args.list_modules:
        result = {m["id"]: m["label"] for m in mapping["modules"]}
    else:
        if args.input is None:
            parser.error("input is required unless --list-modules is used")
        result = candidate_summary(json.loads(args.input.read_text()), mapping, args.modules)
    print(json.dumps(result, separators=(",", ":")))


if __name__ == "__main__":
    main()
