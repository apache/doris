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

"""Reconstruct API-limited PR changes from pinned Git snapshots without a checkout."""

from pathlib import Path
import re
import subprocess
import tempfile


def require(condition, message):
    if not condition:
        raise ValueError(message)


def parse_numstat(output):
    files = []
    records = iter(output.split("\0")[:-1])
    for record in records:
        additions, deletions, filename = record.split("\t", 2)
        previous = None
        if not filename:  # With -z, renames have separate old/new path records.
            previous, filename = next(records), next(records)
        item = {"filename": filename, "additions": 0 if additions == "-" else int(additions),
                "deletions": 0 if deletions == "-" else int(deletions)}
        if previous is not None:
            item["previous_filename"] = previous
        files.append(item)
    return files


def git_changes(api, repository, pr, api_files, parent, remote=None):
    base, head = pr["base"]["sha"], pr["head"]["sha"]
    require(all(re.fullmatch(r"[0-9a-f]{40}", sha) for sha in (base, head)), "Invalid PR commit SHA.")
    comparison = api.request("GET", f"repos/{repository}/compare/{base}...{head}?per_page=1")
    merge_base = comparison["merge_base_commit"]["sha"]
    require(re.fullmatch(r"[0-9a-f]{40}", merge_base), "Invalid PR merge-base SHA.")
    with tempfile.TemporaryDirectory(prefix="reviewer-assignment-git-", dir=parent) as temporary:
        git_dir = Path(temporary)

        def git(*args):
            result = subprocess.run(["git", "-c", "core.hooksPath=/dev/null", "--git-dir", str(git_dir), *args],
                                    check=True, capture_output=True, timeout=240)
            return result.stdout.decode("utf-8")

        git("init", "--bare", "--quiet")
        # The production URL is the trusted base repository, never a PR-supplied clone URL.
        git("remote", "add", "origin", remote or f"https://github.com/{repository}.git")
        git("config", "remote.origin.promisor", "true")
        git("config", "remote.origin.partialclonefilter", "blob:none")
        git("fetch", "--quiet", "--no-tags", "--depth=1", "--filter=blob:none", "origin",
            f"{base}:refs/routing/base", f"{head}:refs/routing/head", f"{merge_base}:refs/routing/merge-base")
        for name, sha in (("base", base), ("head", head), ("merge-base", merge_base)):
            require(git("rev-parse", f"refs/routing/{name}").strip() == sha, "Fetched PR snapshot mismatch.")
        options = ("--no-ext-diff", "--no-textconv", "--find-renames=50%")
        files = parse_numstat(git("diff", *options, "--numstat", "-z", merge_base, head, "--"))
        diff = git("diff", *options, "--no-color", "--src-prefix=a/", "--dst-prefix=b/", merge_base, head, "--")

    by_path = {f["filename"]: (f["additions"], f["deletions"]) for f in files}
    require(len(files) == len(by_path) == pr["changed_files"], "Reconstructed PR file count mismatch.")
    require(sum(f["additions"] for f in files) == pr["additions"]
            and sum(f["deletions"] for f in files) == pr["deletions"], "Reconstructed PR line counts mismatch.")
    require(all(by_path.get(f["filename"]) == (f["additions"], f["deletions"]) for f in api_files),
            "Reconstructed PR changes disagree with the available API files.")
    return files, diff, merge_base
