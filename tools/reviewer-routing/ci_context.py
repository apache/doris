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

"""Prepare compact candidate evidence for CI without sending model requests."""

import argparse
import collections
import json
from pathlib import Path

from classify_paths import classify_pr
from routing_config import load_routing


def plan(pr, mapping):
    assert isinstance(pr["number"], int) and pr["number"] > 0
    assert isinstance(pr["author"], str) and pr["author"]
    files = [item if isinstance(item, dict) else {"path": item} for item in pr["files"]]
    assert files, "The complete changed-file list is required"
    assert len({item["path"] for item in files}) == len(files), "Duplicate file paths"
    if "changed_files" in pr:
        assert pr["changed_files"] == len(files), "Changed-file list is incomplete"
    classified = classify_pr([item["path"] for item in files], mapping["config"])
    modules = {m["id"]: m for m in mapping["modules"]}
    wanted = set(classified["routing_modules"])
    blocked = {pr["author"].lower(), *[login.lower() for login in pr.get("unavailable_reviewers", [])]}
    disabled = {login.lower() for login in mapping["config"]["disabled_reviewers"]}
    blocked.update(disabled)
    by_path = {item["path"]: item for item in files}
    weights = collections.Counter()
    for item in classified["evidence"]:
        if item["module"] in wanted and (not classified["primary_modules"] or item["kind"] == "production"):
            file = by_path[item["path"]]
            weights[item["module"]] += max(1, min(100, file.get("additions", 0) + file.get("deletions", 0)))
    eligible = {mid: modules[mid]["candidates"] for mid in wanted}
    uncovered = set(wanted)
    choices = {}
    for mid in wanted:
        for s in eligible[mid][:5]:
            if s["login"].lower() not in blocked:
                choices.setdefault(s["login"], {})[mid] = s
    chosen = []
    for _ in range(2):
        ranked = []
        for login, evidence in choices.items():
            covers = uncovered & evidence.keys()
            benefit = sum(weights[mid] * evidence[mid]["preference_weight"] for mid in covers)
            if covers:
                ranked.append((benefit, len(covers), login, covers))
        if not ranked:
            break
        _, _, login, covers = sorted(ranked, key=lambda x: (-x[0], -x[1], x[2].lower()))[0]
        chosen.append({"login": login, "covers": sorted(covers)})
        uncovered -= covers
        del choices[login]
    return {"routing_modules": sorted(wanted), "test_modules": classified["test_modules"],
            "reviewers": chosen, "uncovered_modules": sorted(uncovered)}


def candidate_summary(pr, mapping, module_ids=None, all_candidates=False):
    # CI's two-assignee budget is independent of existing formal reviewers.
    routing = plan(pr, mapping)
    modules = {module["id"]: module for module in mapping["modules"]}
    wanted = sorted(set(routing["routing_modules"] if module_ids is None else module_ids))
    blocked = {pr["author"].lower(), *[p.lower() for p in pr.get("unavailable_reviewers", [])]}
    existing = {p.lower() for p in pr.get("existing_reviewers", [])}
    summaries = []
    for mid in wanted:
        module = modules[mid]
        eligible = module["candidates"]
        visible = eligible if all_candidates else eligible[:5]
        people = []
        for person in visible:
            if person["login"].lower() in blocked:
                continue
            people.append({"login": person["login"],
                           "preference_weight": round(person["preference_weight"], 6),
                           "existing_reviewer": person["login"].lower() in existing})
        summaries.append({"id": mid, "label": module["label"],
                          "configured_reviewers": module["reviewers"],
                          "eligible_candidate_count": sum(p["login"].lower() not in blocked for p in eligible),
                          "candidates": people})
    result = {"path_routing_modules": routing["routing_modules"], "test_modules": routing["test_modules"],
              "existing_reviewers": pr.get("existing_reviewers", []), "modules": summaries,
              "fallback_reviewer": mapping["config"]["fallback_reviewer"],
              "disabled_reviewers": mapping["config"]["disabled_reviewers"]}
    if module_ids is None:
        result.update(path_suggestion=routing["reviewers"],
                      uncovered_path_modules=routing["uncovered_modules"])
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path, nargs="?")
    parser.add_argument("--modules", nargs="+")
    parser.add_argument("--all-candidates", action="store_true")
    parser.add_argument("--list-modules", action="store_true")
    args = parser.parse_args()
    mapping = load_routing()
    if args.list_modules:
        result = {m["id"]: m["label"] for m in mapping["modules"]}
    else:
        if args.input is None:
            parser.error("input is required unless --list-modules is used")
        result = candidate_summary(json.loads(args.input.read_text()), mapping, args.modules, args.all_candidates)
    print(json.dumps(result, separators=(",", ":")))


if __name__ == "__main__":
    main()
