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

"""Record who dismissed a specific AI review. Eligibility is decided by the runner."""

import os
import re
from pathlib import Path
from urllib.parse import quote

from review_github_api import decode_json, run_gh


def api(path, *args):
    command = ["gh", "api", path, *args]
    result = run_gh(command, capture_output=True, text=True, check=True, timeout=90)
    return decode_json(result, command)


def record_dismissal(repo, server, title, run_actor_id):
    # The signal runs in a PR context. Treat its title only as a lookup hint.
    match = re.fullmatch(r"Dismiss review ([1-9][0-9]*) on PR ([1-9][0-9]*)", title)
    if not match:
        return None
    review_id, number = match.groups()
    root = f"repos/{repo}"
    pr = api(f"{root}/pulls/{number}")
    if pr["state"] != "open":
        return None
    head = pr["head"]["sha"]
    review = api(f"{root}/pulls/{number}/reviews/{review_id}")
    if (review["state"] != "DISMISSED" or review["commit_id"] != head
            or review["user"]["login"] not in ("github-actions[bot]", os.getenv("DORIS_REVIEW_APP_BOT", ""))):
        return None

    pages = api(f"{root}/issues/{number}/timeline?per_page=100", "--paginate", "--slurp")
    for page in pages:
        for event in page:
            if event["event"] != "review_dismissed":
                continue
            dismissal = event["dismissed_review"]
            if (str(dismissal["review_id"]) != review_id or dismissal["state"] != "changes_requested"
                    or dismissal.get("dismissal_commit_id")):
                continue
            actor = event["actor"]
            if not actor or actor["type"] != "User" or actor["id"] != run_actor_id:
                continue
            login = actor["login"]
            permission = api(f"{root}/collaborators/{quote(login, safe='')}/permission")
            if permission["permission"] not in ("write", "admin") or permission["user"]["id"] != actor["id"]:
                continue

            api(f"{root}/statuses/{head}", "-X", "POST", "-f", "state=success",
                "-f", f"context=code-review/dismissal/pr-{number}/review-{review_id}",
                "-f", f"description=Review dismissed by {login}.",
                "-f", f"target_url={server}/{repo}/pull/{number}#pullrequestreview-{review_id}")
            return head
    return None


if __name__ == "__main__":
    head = record_dismissal(os.environ["GITHUB_REPOSITORY"], os.environ["GITHUB_SERVER_URL"],
                            os.environ["RUN_TITLE"], int(os.environ["RUN_ACTOR_ID"]))
    if head:
        with Path(os.environ["GITHUB_OUTPUT"]).open("a") as output:
            print(f"head_sha={head}", file=output)
