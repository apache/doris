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

"""Prepare trusted Codex inputs and apply a validated reviewer-assignment plan."""

import argparse
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
from urllib.parse import quote

from review_github_api import decode_json, run_gh
from reviewer_assignment_diff import git_changes
from reviewer_assignment_model import response_modules, write_input

ROOT = Path(__file__).resolve().parents[2]
TOOL = ROOT / "tools/reviewer-routing"
sys.path.append(str(TOOL))
from routing_config import load_routing
from ci_context import candidate_summary, eligible_pools, sample_reviewers

REQUEST_LABEL = "need-reviewers"
DONE_LABEL = "reviewers-assigned"
BOT = "github-actions[bot]"
MARKER = re.compile(r"<!-- reviewer-assignment:(\{[^\n]+\}) -->")


def require(condition, message):
    if not condition:
        raise ValueError(message)


class GitHubError(RuntimeError):
    def __init__(self, status, message):
        super().__init__(message)
        self.status = status


class GitHub:
    def request(self, method, endpoint, payload=None, *, pages=False, raw=False):
        command = ["gh", "api", "--method", method, endpoint]
        if pages:
            command += ["--paginate", "--slurp"]
        if raw:
            command += ["-H", "Accept: application/vnd.github.v3.diff"]
        if payload is not None:
            command += ["--input", "-"]
        try:
            result = run_gh(command, input=json.dumps(payload) if payload is not None else None,
                            check=True, capture_output=True, text=True, timeout=90)
        except subprocess.CalledProcessError as error:
            status = re.search(r"HTTP=(\d+)", error.stderr or "")
            raise GitHubError(int(status[1]) if status else None,
                              f"GitHub {method} {endpoint} failed: {error.stderr}") from error
        if raw or not result.stdout.strip():
            return result.stdout
        data = decode_json(result, command)
        return [item for page in data for item in page] if pages else data


def candidates(tool, mapping=None):
    mapping = load_routing(tool) if mapping is None else mapping
    allowed = {}
    for module in mapping["modules"]:
        for person in module["candidates"]:
            allowed[person["login"].lower()] = person["login"]
    return allowed


def validate_selection(selection, author, allowed):
    require(isinstance(selection, dict) and set(selection) == {"reviewers"},
            "The assignment plan must contain only reviewers.")
    people = selection["reviewers"]
    require(isinstance(people, list) and len(people) <= 2,
            "The plan must select at most two reviewers; an empty result requests fallback triage.")
    require(all(isinstance(person, str) for person in people), "Reviewer IDs must be strings.")
    lowered = [person.lower() for person in people]
    require(len(set(lowered)) == len(lowered), "Duplicate reviewer IDs are not allowed.")
    require(author.lower() not in lowered, "The PR author cannot be selected.")
    require(all(person in allowed for person in lowered),
            "Every selected reviewer must be active in reviewer-routing.json.")
    return [allowed[person] for person in lowered]


def marker_data(context, people):
    data = {key: context[key] for key in ("repository", "pr_number", "run_id", "head_sha")} | {
        "reviewers": people}
    if not people:
        data["fallback_reviewer"] = context["fallback_reviewer"]
    return data


def marker(context, people):
    return "<!-- reviewer-assignment:" + json.dumps(marker_data(context, people),
                                                    sort_keys=True, separators=(",", ":")) + " -->"


def checkpoint(api, context):
    endpoint = f"repos/{context['repository']}/issues/{context['pr_number']}/comments?per_page=100"
    found = []
    for comment in api.request("GET", endpoint, pages=True):
        if comment.get("user", {}).get("login") != BOT:
            continue
        match = MARKER.search(comment.get("body") or "")
        if match:
            saved = json.loads(match[1])
            if (saved.get("repository") == context["repository"]
                    and saved.get("pr_number") == context["pr_number"]
                    and saved.get("run_id") == context["run_id"]):
                require(saved.get("head_sha") == context["head_sha"],
                        "This run already selected reviewers for an older PR head; reapply the label.")
                if not saved.get("reviewers"):
                    require(saved.get("fallback_reviewer") == context["fallback_reviewer"],
                            "This run's fallback reviewer changed; reapply the label.")
                found.append((comment, saved))
    require(len(found) <= 1, "Multiple assignment checkpoints exist for this workflow run.")
    return found[0] if found else None


def verify_pr(pr, context):
    require(pr["number"] == context["pr_number"] and pr["state"] == "open",
            "The requested PR is no longer open.")
    require(pr["base"]["repo"]["full_name"] == context["repository"], "PR repository mismatch.")
    require(pr["head"]["sha"] == context["head_sha"], "PR head changed; reapply need-reviewers.")
    require(pr["base"]["sha"] == context["base_sha"], "PR base changed; reapply need-reviewers.")
    require(pr["user"]["login"].lower() == context["author"].lower(), "PR author changed.")


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def model_input(workspace, tool=TOOL, mapping=None):
    mapping = load_routing(tool) if mapping is None else mapping
    pr = json.loads((workspace / "pr.json").read_text())
    details = json.loads((workspace / "pr-details.json").read_text())
    summary = candidate_summary(pr, mapping)
    write_json(workspace / "candidate-summary.json", summary)
    catalog = candidate_summary(pr, mapping, list(mapping["config"]["modules"]))["modules"]
    return write_input(workspace, (tool / "CI_GUIDE.md").read_text(), pr, details,
                       (workspace / "pr.diff").read_text(), summary, catalog)


def request_event(event, event_name):
    if event_name == "workflow_dispatch":
        number = event["inputs"]["pr_number"]
        require(isinstance(number, str) and number.isdigit() and int(number) > 0,
                "pr_number must be a positive integer.")
        return {"action": "labeled", "label": {"name": REQUEST_LABEL},
                "pull_request": {"number": int(number)}}
    require(event_name == "pull_request_target", "Unsupported assignment event.")
    return event


def prepare(api, event, repository, run_id, workspace, tool=TOOL):
    require(event.get("action") == "labeled" and event.get("label", {}).get("name") == REQUEST_LABEL,
            "This helper only handles need-reviewers labeled events.")
    require(event.get("pull_request"), "A pull request event is required.")
    require(str(run_id).isdigit(), "A numeric workflow run ID is required.")
    number = event["pull_request"]["number"]
    require(isinstance(number, int) and number > 0, "Invalid PR number.")
    pr = api.request("GET", f"repos/{repository}/pulls/{number}")
    context = {"repository": repository, "pr_number": number, "run_id": str(run_id),
               "head_sha": pr["head"]["sha"], "base_sha": pr["base"]["sha"],
               "author": pr["user"]["login"]}
    verify_pr(pr, context)
    mapping = load_routing(tool)
    context["fallback_reviewer"] = mapping["config"]["fallback_reviewer"]
    allowed = candidates(tool, mapping)
    previous = checkpoint(api, context)
    workspace.mkdir(parents=True, exist_ok=True)
    write_json(workspace / "context.json", context)
    if previous:
        people = validate_selection({"reviewers": previous[1]["reviewers"]}, context["author"], allowed)
        write_json(workspace / "selection.json", {"reviewers": people})
        return True
    for name in ("selection.json", "module-selection.json", "selected-candidates.json", "validation.json"):
        (workspace / name).unlink(missing_ok=True)

    endpoint = f"repos/{repository}/pulls/{number}"
    files = api.request("GET", endpoint + "/files?per_page=100", pages=True)
    complete_files = len(files) == pr["changed_files"] and len({f["filename"] for f in files}) == len(files)
    context["diff_source"] = "github"
    if complete_files:
        try:
            diff = api.request("GET", endpoint, raw=True)
        except GitHubError as error:
            if error.status != 406:
                raise
            complete_files = False  # The diff API rejects changes beyond its line limit.
    if not complete_files:
        files, diff, context["merge_base_sha"] = git_changes(api, repository, pr, files, workspace.parent)
        context["diff_source"] = "git"
    reviews = api.request("GET", endpoint + "/reviews?per_page=100", pages=True)
    verify_pr(api.request("GET", endpoint), context)
    write_json(workspace / "context.json", context)
    shutil.copytree(tool, workspace / "tools/reviewer-routing", dirs_exist_ok=True)
    existing = {p["login"] for p in pr.get("requested_reviewers", []) if p["type"] != "Bot"}
    existing.update(r["user"]["login"] for r in reviews if r.get("user")
                    and r["user"]["type"] != "Bot"
                    and r["state"] in {"APPROVED", "COMMENTED", "CHANGES_REQUESTED"})
    pr_input = {"number": number, "author": context["author"],
               "changed_files": len(files), "files": [
                   {"path": f["filename"], "additions": f["additions"], "deletions": f["deletions"]}
                   for f in files], "existing_reviewers": sorted(existing), "unavailable_reviewers": []}
    write_json(workspace / "pr.json", pr_input)
    write_json(workspace / "candidate-summary.json", candidate_summary(pr_input, mapping))
    write_json(workspace / "pr-details.json", {
        "title": pr["title"], "body": pr.get("body"), "draft": pr["draft"],
        "assignees": [p["login"] for p in pr.get("assignees", [])],
        "requested_teams": [p["slug"] for p in pr.get("requested_teams", [])]})
    (workspace / "pr.diff").write_text(diff)
    model_input(workspace, tool, mapping)
    return False


def finalize_selection(workspace, tool=TOOL):
    context = json.loads((workspace / "context.json").read_text())
    mapping = load_routing(tool)
    saved = workspace / "selection.json"
    if saved.exists():
        selection = json.loads(saved.read_text())
    else:
        try:
            modules = json.loads((workspace / "module-selection.json").read_text())
            pr = json.loads((workspace / "pr.json").read_text())
            selection = {"reviewers": sample_reviewers(response_modules(modules), pr, mapping)}
            write_json(workspace / "selected-candidates.json", candidate_summary(pr, mapping, modules["modules"]))
            require(all(eligible_pools(pr, mapping, modules["modules"]).values()),
                    "The classifier selected a module without eligible candidates.")
            matched = [m["id"] for m in candidate_summary(pr, mapping)["modules"] if m["candidates"]]
            require(selection["reviewers"] or not matched,
                    f"Empty reviewer selection despite eligible path modules: {', '.join(matched)}")
            write_json(workspace / "validation.json", {"status": "accepted", "modules": modules["modules"],
                       "reason": modules["reason"], "eligible_path_modules": matched})
        except ValueError as error:
            write_json(workspace / "validation.json", {"status": "rejected", "error": str(error)})
            raise
    people = validate_selection(selection, context["author"], candidates(tool, mapping))
    write_json(saved, {"reviewers": people})
    return people


def apply(api, context, selection, tool=TOOL):
    prefix = f"repos/{context['repository']}"
    issue = prefix + f"/issues/{context['pr_number']}"
    pr = api.request("GET", prefix + f"/pulls/{context['pr_number']}")
    verify_pr(pr, context)
    mapping = load_routing(tool)
    selected = validate_selection(selection, context["author"],
                                  candidates(tool, mapping))
    # Empty means manual triage, not a claim that the fallback has module expertise.
    fallback = mapping["config"]["fallback_reviewer"]
    if not selected:
        require(context["fallback_reviewer"] == fallback, "The fallback reviewer changed; reapply need-reviewers.")
    people = selected or [fallback]
    for person in people:
        user = api.request("GET", "users/" + quote(person, safe=""))
        require(user["type"] == "User" and user["login"].lower() == person.lower(),
                f"Selected account {person} is not the expected human user.")
        api.request("GET", prefix + "/assignees/" + quote(person, safe=""))

    previous = checkpoint(api, context)
    if previous:
        require(previous[1]["reviewers"] == selected, "The run's selected reviewers cannot change on retry.")
    try:
        api.request("GET", prefix + "/labels/" + DONE_LABEL)
    except GitHubError as error:
        if error.status != 404:
            raise
        api.request("POST", prefix + "/labels", {
            "name": DONE_LABEL, "color": "0e8a16", "description": "Reviewer candidates assigned by automation"})

    # Freeze the pair before the first assignment mutation. The same comment is
    # completed after readback, so retrying a partial run cannot select extra people.
    saved_marker = marker(context, selected)
    if previous:
        comment = previous[0]
    else:
        comment = api.request("POST", issue + "/comments", {
            "body": "Reviewer assignment is in progress.\n\n" + saved_marker})
    existing = {p["login"].lower() for p in pr.get("assignees", [])}
    missing = [person for person in people if person.lower() not in existing]
    if missing:
        api.request("POST", issue + "/assignees", {"assignees": missing})
    assigned = api.request("GET", issue)
    require({person.lower() for person in people} <= {p["login"].lower() for p in assigned["assignees"]},
            "GitHub did not assign every selected reviewer; keeping need-reviewers for recovery.")
    if selected:
        message = ", ".join("@" + person for person in people) + ", please review this PR."
    else:
        message = f"No suitable reviewer was found. @{fallback}, please help assign reviewers for this PR."
    body = message + "\n\n" + saved_marker
    if comment["body"] != body:
        comment = api.request("PATCH", prefix + f"/issues/comments/{comment['id']}", {"body": body})
    require(comment["body"] == body, "Assignment comment readback did not match the expected body.")
    api.request("POST", issue + "/labels", {"labels": [DONE_LABEL]})
    current = api.request("GET", issue)
    if REQUEST_LABEL in {label["name"] for label in current["labels"]}:
        try:
            api.request("DELETE", issue + "/labels/" + REQUEST_LABEL)
        except GitHubError as error:
            # An operator can remove the same label between readback and DELETE.
            if error.status != 404:
                raise
    current = api.request("GET", issue)
    labels = {label["name"] for label in current["labels"]}
    require(DONE_LABEL in labels and REQUEST_LABEL not in labels, "Assignment labels were not finalized.")
    return {"reviewers": people, "fallback": not selected, "comment_url": comment["html_url"], "assigned": True}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["prepare", "validate", "apply"])
    parser.add_argument("--workspace", type=Path, required=True)
    args = parser.parse_args()
    event_name = os.environ.get("GITHUB_EVENT_NAME", "pull_request_target")
    event = request_event(json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text()), event_name)
    require(args.mode != "apply" or event_name == "pull_request_target", "Manual runs cannot mutate a PR.")
    if args.mode == "prepare":
        reused = prepare(GitHub(), event,
                         os.environ["GITHUB_REPOSITORY"], os.environ["GITHUB_RUN_ID"], args.workspace)
        with Path(os.environ["GITHUB_OUTPUT"]).open("a") as output:
            output.write(f"reuse_selection={'true' if reused else 'false'}\n")
    else:
        context = json.loads((args.workspace / "context.json").read_text())
        require(context["repository"] == os.environ["GITHUB_REPOSITORY"]
                and context["run_id"] == os.environ["GITHUB_RUN_ID"]
                and context["pr_number"] == event["pull_request"]["number"],
                "Assignment artifact identity mismatch.")
        if args.mode == "validate":
            people = finalize_selection(args.workspace)
            print(json.dumps({"reviewers": people}))
        else:
            selection = json.loads((args.workspace / "selection.json").read_text())
            print(json.dumps(apply(GitHub(), context, selection)))


if __name__ == "__main__":
    main()
