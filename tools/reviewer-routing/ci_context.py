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
import datetime as dt
import json
from pathlib import Path

from reviewer_candidates import eligible_candidates, preference_weight
from select_reviewers import plan

ROOT = Path(__file__).resolve().parent


def candidate_summary(pr, mapping, now, module_ids=None, all_candidates=False):
    # CI's two-assignee budget is independent of existing formal reviewers.
    routing = plan({**pr, "existing_reviewers": []}, mapping, now, limit=2)
    modules = {module["id"]: module for module in mapping["modules"]}
    wanted = sorted(set(routing["routing_modules"] if module_ids is None else module_ids))
    blocked = {pr["author"].lower(), *[p.lower() for p in pr.get("unavailable_reviewers", [])]}
    existing = {p.lower() for p in pr.get("existing_reviewers", [])}
    summaries = []
    for mid in wanted:
        module = modules[mid]
        eligible = eligible_candidates(module, now)
        visible = eligible if all_candidates or "reviewer_override" in module else eligible[:5]
        people = []
        for person in visible:
            if person["login"].lower() in blocked:
                continue
            people.append({key: person[key] for key in (
                "login", "confidence", "selection_source", "review_prs", "approval_prs", "merge_prs", "last_review")})
            people[-1].update(
                preference_weight=round(preference_weight(person, eligible[0]), 6),
                existing_reviewer=person["login"].lower() in existing,
                review_examples=person["review_examples"][:2])
        summaries.append({"id": mid, "label": module["label"],
                          "reviewer_override": module.get("reviewer_override"),
                          "eligible_candidate_count": sum(p["login"].lower() not in blocked for p in eligible),
                          "candidates": people})
    result = {"as_of": mapping["as_of"], "refresh_recommended": routing["refresh_recommended"],
              "path_routing_modules": routing["routing_modules"], "test_modules": routing["test_modules"],
              "existing_reviewers": pr.get("existing_reviewers", []), "modules": summaries}
    if module_ids is None:
        result.update(path_suggestion=[{"login": p["login"], "covers": p["covers"]}
                                       for p in routing["reviewers"]],
                      uncovered_path_modules=routing["uncovered_modules"])
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path, nargs="?")
    parser.add_argument("--modules", nargs="+")
    parser.add_argument("--all-candidates", action="store_true")
    parser.add_argument("--list-modules", action="store_true")
    args = parser.parse_args()
    mapping = json.loads((ROOT / "module-reviewers.json").read_text())
    if args.list_modules:
        result = {m["id"]: m["label"] for m in mapping["modules"]}
    else:
        if args.input is None:
            parser.error("input is required unless --list-modules is used")
        result = candidate_summary(json.loads(args.input.read_text()), mapping,
                                   dt.datetime.now(dt.timezone.utc), args.modules, args.all_candidates)
    print(json.dumps(result, separators=(",", ":")))


if __name__ == "__main__":
    main()
