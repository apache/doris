#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Submit an authorized PR snapshot and synchronize its verified result with the merge gate."""
import hashlib
import http.client
import json
import os
import re
import time
from pathlib import Path
from urllib.parse import urlencode, urlsplit

from review_github_api import decode_json, run_gh

FC = "https://doris-c-trigger-mtjzjrppvu.cn-hongkong.fcapp.run/reviews"
AUDIENCE = "urn:apache:doris:ci:review"


class TransientHTTPError(Exception):
    pass


def json_request(url, token, body=None):
    parts = urlsplit(url)
    if parts.scheme != "https":
        raise ValueError("Expected an HTTPS endpoint")
    conn = http.client.HTTPSConnection(parts.netloc, timeout=25)
    try:
        path = parts.path + ("?" + parts.query if parts.query else "")
        conn.request("GET" if body is None else "POST", path,
                     body=json.dumps(body).encode() if body is not None else None,
                     headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"})
        response = conn.getresponse()
        if response.status == 429 or 500 <= response.status < 600:
            raise TransientHTTPError(f"Request to {parts.hostname} returned HTTP {response.status}")
        if response.status not in (200, 202):
            raise RuntimeError(f"Request to {parts.hostname} returned HTTP {response.status}")
        return response.status, json.load(response)
    finally:
        conn.close()


def fc(path, body, expected):
    url = os.environ["ACTIONS_ID_TOKEN_REQUEST_URL"]
    url += ("&" if "?" in url else "?") + urlencode({"audience": AUDIENCE})
    # Each poll can outlive the previous GitHub OIDC token.
    for attempt in range(3):
        try:
            _, identity = json_request(url, os.environ["ACTIONS_ID_TOKEN_REQUEST_TOKEN"])
            status, result = json_request(FC + path, identity["value"], body)
            if status not in expected:
                raise ValueError(f"Unexpected FC HTTP {status}")
            return result
        except (OSError, http.client.HTTPException, TransientHTTPError):
            if attempt == 2:
                raise
            time.sleep(3 * (attempt + 1))


def gh(path, *args):
    command = ["gh", "api", path, *args]
    response = run_gh(command, check=True, capture_output=True, text=True, timeout=60)
    return decode_json(response, command)


def requested_focus():
    comment = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text())["comment"]["body"]
    match = re.fullmatch(r"/review(?:[ \t\n\r](.*))?", comment, re.IGNORECASE | re.DOTALL | re.ASCII)
    if not match:
        raise ValueError("Expected /review with an optional focus")
    return (match[1] or "").lstrip()


def focus_suffix(focus):
    return "/" + hashlib.sha256(focus.encode()).hexdigest() if focus else ""


def verdict(result, pr_url, run_url, pr):
    p0, p1 = result["p0"], result["p1"]
    converged, rounds = result["converged"], result["rounds"]
    review_id = result["review_id"]
    if (any(type(n) is not int or n < 0 for n in (p0, p1))
            or type(converged) is not bool or type(rounds) is not int or not 1 <= rounds <= 3
            or (not converged and rounds != 3) or type(review_id) is not int or review_id <= 0):
        raise ValueError("Invalid review result")
    link = f"{pr_url}#pullrequestreview-{review_id}"
    state = "success" if converged and p0 == p1 == 0 else "failure"
    if result["review_url"] != link or result["state"] != state:
        raise ValueError("Inconsistent verified review result")
    summary = f"Review completed for PR #{pr}: P0={p0}, P1={p1}."
    if not converged:
        summary = f"Review did not converge after {rounds} rounds for PR #{pr}: P0={p0}, P1={p1}."
    return state, summary, link if state == "failure" and converged else run_url


def publish_receipt(repo, run, pr, head, base, result, bot):
    """Called by the cloud controller after exact GitHub review and inline readback."""
    if not re.fullmatch(r"[1-9][0-9]*", run) or not bot:
        raise ValueError("Expected a workflow run ID and a publishing App login")
    pr_url = f"https://github.com/{repo}/pull/{pr}"
    state, summary, _ = verdict(result, pr_url, f"https://github.com/{repo}/actions/runs/{run}", pr)
    fields = dict(context=f"code-review/cloud/{run}/pr-{pr}/base-{base}", state=state,
                  description=summary, target_url=result["review_url"])
    root = f"repos/{repo}"
    # A lost POST response or controller restart must keep this result's original order.
    pages = gh(f"{root}/commits/{head}/statuses?per_page=100", "--paginate", "--slurp")
    for page in pages:
        for row in page:
            if row["context"] == fields["context"] and row["creator"]["login"] == bot:
                if any(row[key] != value for key, value in fields.items()):
                    raise ValueError("Conflicting cloud receipt for this run")
                return
    live = gh(f"{root}/pulls/{pr}")
    if live["state"] != "open" or (live["head"]["sha"], live["base"]["sha"]) != (head, base):
        raise ValueError("Cloud review snapshot changed before recording the verdict")
    args = []
    for key, value in fields.items():
        args.extend(("-f", f"{key}={value}"))
    gh(f"{root}/statuses/{head}", "-X", "POST", *args)


def review(pr, head, base):
    repo = os.environ["GITHUB_REPOSITORY"]
    run = os.environ["GITHUB_RUN_ID"]
    request_id = "github:review:" + run
    repo_url = f"https://github.com/{repo}"
    run_url = f"{repo_url}/actions/runs/{run}"
    pr_url = f"{repo_url}/pull/{pr}"
    source = f"code-review/source/automated/pr-{pr}/base-{base}"

    def current():
        live = gh(f"repos/{repo}/pulls/{pr}")
        return live["state"] == "open" and live["head"]["sha"] == head and live["base"]["sha"] == base

    focus = requested_focus()
    if not current():
        print("PR changed before dispatch")
        return
    snapshot = dict(pr_number=pr, head_sha=head, base_sha=base)
    with Path(os.environ["GITHUB_OUTPUT"]).open("a") as outputs:
        print(f"aggregate_head={head}", file=outputs)
    for context in ("code-review", source):
        gh(f"repos/{repo}/statuses/{head}", "-X", "POST", "-f", f"context={context}",
           "-f", "state=pending", "-f", f"description=Automated review is running for PR #{pr}.",
           "-f", f"target_url={run_url}")
    if len(focus.encode()) > 8192:
        raise ValueError("The cloud review focus exceeds 8192 UTF-8 bytes")
    payload = dict(snapshot, review_focus=focus) if focus else snapshot
    receipt = fc("", payload, (202,))
    if receipt.get("request_id") != request_id:
        raise ValueError("Receipt belongs to another run")
    print(f"Submitted {request_id} for PR #{pr}", flush=True)
    while True:
        row = fc("/status", {"request_id": request_id}, (200, 202))
        if row.get("request_id") != request_id:
            raise ValueError("Status belongs to another run")
        state = row["state"]
        if state != "awaiting_status":
            if (any(row.get(k) != v for k, v in snapshot.items())
                    or type(row.get("preview_only")) is not bool):
                raise ValueError("Status does not match the authorized snapshot")
            if row["preview_only"] or state == "previewed":
                raise RuntimeError("The default review requires a publishing cloud worker")
            if state == "completed":
                if row["result"].get("gate_receipt") is not True:
                    raise RuntimeError("The cloud publisher did not record a gate receipt")
                verdict(row["result"], pr_url, run_url, pr)
                return
            if state == "superseded":
                print("PR changed during review")
                return
            if state not in {"queued", "running", "publishing", "uncertain"}:
                raise RuntimeError(f"Review ended with state {state}")
        time.sleep(30)


if __name__ == "__main__":
    import sys

    if sys.argv[1:] == ["--focus-suffix"]:
        print("focus_suffix=" + focus_suffix(requested_focus()))
    else:
        review(int(os.environ["PR_NUMBER"]), os.environ["HEAD_SHA"], os.environ["BASE_SHA"])
