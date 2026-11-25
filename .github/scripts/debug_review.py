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
import http.client
import json
import os
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

    def write_status(context, state, description, target):
        gh(f"repos/{repo}/statuses/{head}", "-X", "POST", "-f", f"context={context}",
           "-f", f"state={state}", "-f", f"description={description}", "-f", f"target_url={target}")

    if not current():
        print("PR changed before dispatch")
        return
    snapshot = dict(pr_number=pr, head_sha=head, base_sha=base)
    receipt = fc("", snapshot, (202,))
    if receipt.get("request_id") != request_id:
        raise ValueError("Receipt belongs to another run")
    print(f"Submitted {request_id} for PR #{pr}", flush=True)
    pending = False
    while True:
        row = fc("/status", {"request_id": request_id}, (200, 202))
        if row.get("request_id") != request_id:
            raise ValueError("Status belongs to another run")
        state = row["state"]
        if state != "awaiting_status":
            if (any(row.get(k) != v for k, v in snapshot.items())
                    or type(row.get("preview_only")) is not bool):
                raise ValueError("Status does not match the authorized snapshot")
            if state == "previewed":
                print("ECS completed a preview")
                return
            if state == "superseded":
                print("PR changed during review")
                return
            if not row["preview_only"] and not pending:
                if not current():
                    return
                description = f"Automated review is running for PR #{pr}."
                with Path(os.environ["GITHUB_OUTPUT"]).open("a") as outputs:
                    print(f"aggregate_head={head}", file=outputs)
                write_status("code-review", "pending", description, run_url)
                write_status(source, "pending", description, run_url)
                pending = True
            if state == "completed" and not row["preview_only"]:
                value = verdict(row["result"], pr_url, run_url, pr)
                if current():
                    write_status(source, *value)
                    print(f"Recorded {value[0]} for {row['result']['review_url']}")
                return
            if state not in {"queued", "running", "publishing", "uncertain"}:
                raise RuntimeError(f"Review ended with state {state}")
        time.sleep(30)


if __name__ == "__main__":
    review(int(os.environ["PR_NUMBER"]), os.environ["HEAD_SHA"], os.environ["BASE_SHA"])
