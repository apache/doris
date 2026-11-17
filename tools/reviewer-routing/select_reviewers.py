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

"""Plan reviewer requests locally; this program never calls GitHub or sends requests.

Input JSON: number, author (login), files (paths or {path, additions, deletions}),
existing_reviewers (requested or previously reviewed logins), unavailable_reviewers.
"""
import argparse
import collections
import datetime as dt
import json
from pathlib import Path

from taxonomy.classify_paths import classify_pr

ROOT = Path(__file__).resolve().parent


def plan(pr, mapping, now, limit=3):
    assert limit in {1, 2, 3}, "Use a total review target between one and three"
    assert isinstance(pr["number"], int) and pr["number"] > 0
    assert isinstance(pr["author"], str) and pr["author"]
    files = [item if isinstance(item, dict) else {"path": item} for item in pr["files"]]
    assert files, "The complete changed-file list is required"
    assert len({item["path"] for item in files}) == len(files), "Duplicate file paths"
    if "changed_files" in pr:
        assert pr["changed_files"] == len(files), "Changed-file list is incomplete"
    classified = classify_pr([item["path"] for item in files])
    modules = {m["id"]: m for m in mapping["modules"]}
    wanted = set(classified["routing_modules"])
    asof = dt.datetime.fromisoformat(mapping["as_of"].replace("Z", "+00:00"))
    age_days = (now - asof).total_seconds() / 86400
    assert age_days >= 0, "The mapping snapshot is in the future"
    assert age_days <= 90, "Mapping older than 90 days: refresh before requesting reviewers"
    blocked = {pr["author"].lower(), *[login.lower() for login in pr.get("unavailable_reviewers", [])]}
    existing = {login.lower() for login in pr.get("existing_reviewers", [])
                if login.lower() != pr["author"].lower() and not login.lower().endswith("[bot]")}
    blocked.update(existing)
    by_path = {item["path"]: item for item in files}
    weights = collections.Counter()
    for item in classified["evidence"]:
        if item["module"] in wanted and (not classified["primary_modules"] or item["kind"] == "production"):
            file = by_path[item["path"]]
            weights[item["module"]] += max(1, min(100, file.get("additions", 0) + file.get("deletions", 0)))
    eligible = {mid: [s for s in modules[mid]["candidates"] if s["auto_eligible"]
                     and (now - dt.datetime.fromisoformat(s["last_review"].replace("Z", "+00:00"))).total_seconds() <= 180 * 86400]
                for mid in wanted}
    covered_existing = {mid for mid in wanted if any(s["login"].lower() in existing for s in eligible[mid])}
    uncovered = wanted - covered_existing
    choices = {}
    for mid in wanted:
        for s in eligible[mid][:5]:
            if s["login"].lower() not in blocked:
                choices.setdefault(s["login"], {})[mid] = s
    chosen = []
    for _ in range(max(0, limit - len(existing))):
        ranked = []
        for login, evidence in choices.items():
            covers = uncovered & evidence.keys()
            benefit = sum(weights[mid] * evidence[mid]["score"] / eligible[mid][0]["score"] for mid in covers)
            if covers:
                ranked.append((benefit, len(covers), login, covers))
        if not ranked:
            break
        _, _, login, covers = sorted(ranked, key=lambda x: (-x[0], -x[1], x[2].lower()))[0]
        chosen.append({"login": login, "covers": sorted(covers), "evidence": {
            mid: {key: choices[login][mid][key] for key in ["review_prs", "approval_prs", "merge_prs", "last_review", "confidence", "review_examples"]}
            for mid in sorted(covers)}})
        uncovered -= covers
        del choices[login]
    return {"repository": mapping["repository"], "pr_number": pr["number"], "dry_run": True,
            "as_of": mapping["as_of"], "refresh_recommended": age_days > 30,
            "routing_modules": sorted(wanted), "test_modules": classified["test_modules"],
            "existing_reviewers": sorted(existing), "covered_by_existing": sorted(covered_existing),
            "reviewers": chosen, "uncovered_modules": sorted(uncovered),
            "needs_manual_attention": bool(uncovered),
            "note": "Read the diff to verify semantics and live reviewer eligibility before executing the rule file. No request was sent."}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path)
    parser.add_argument("--limit", type=int, choices=[1, 2, 3], default=3)
    args = parser.parse_args()
    pr = json.loads(args.input.read_text())
    mapping = json.loads((ROOT / "module-reviewers.json").read_text())
    print(json.dumps(plan(pr, mapping, dt.datetime.now(dt.timezone.utc), args.limit), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
