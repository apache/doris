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
"""
Batch cherry-pick admission ("Mergify-like batch") orchestrator.

Goal: instead of running one full TeamCity pipeline per cherry-pick PR to a
release branch, stack N approved PRs onto ONE integration branch, run the
pipeline ONCE, and if it is green, admit + merge all N PRs. This amortizes CI
cost across the whole batch.

Design (agreed):
  - One long-lived tracking Issue per release branch, tagged with a
    `batch-queue/<branch>` label. Operators drive the flow by commenting there.
  - The integration branch is exposed as a throwaway "vehicle PR" purely so the
    existing TeamCity trigger (which builds `pull/<N>` refs) and status
    reporting work unchanged. The vehicle PR is never merged.
  - On green, success statuses are broadcast onto each member PR's head SHA
    (reusing the same commit-status contexts the pipelines report), then the
    member PRs are merged individually (Model A) so each keeps its own
    "Merged" provenance.
  - Exactly one batch may be in flight per branch (merge-queue serialization).
  - Failure is handled by the operator (no auto-bisection): `rerun batch`
    (dodge flaky / pick up a fixed member), fix the red sub-build directly in
    TeamCity (the poller auto-detects green), or `cancel batch`.

Commands (subcommand = arg 1):
  run    ISSUE_NUMBER   -- start a NEW batch (rejected if one is in flight)
  rerun  ISSUE_NUMBER   -- re-assemble the SAME PR set at current heads + re-run
  cancel ISSUE_NUMBER   -- abandon the active batch, release the lock
  poll                  -- cron: detect completion of in-flight batches and act

Everything TeamCity-related is delegated to regression-test/pipeline/common/
batch-utils.sh (which reuses teamcity-utils.sh).

--- WIRING / ENV (see the workflow yaml) -------------------------------------
  GITHUB_TOKEN     required. Needs: pull-requests:write, contents:write,
                   statuses:write, issues:write. NOTE: to actually MERGE into a
                   protected release branch the token/identity must be allowed
                   to do so by branch protection -- if bot merges are
                   restricted, supply a PAT/App token via MERGE_TOKEN.
  MERGE_TOKEN      optional. Token used only for the final member-PR merges;
                   defaults to GITHUB_TOKEN.
  REPO_NAME        default apache/doris
  COMMENT_BODY     the triggering comment (run/rerun/cancel modes) -- used to
                   parse an explicit "#101 #102" PR list.
  BRANCHES         poll mode: comma list of release branches to scan,
                   e.g. "branch-4.0,branch-4.1"
  MAX_BATCH        sweep cap (default 10)
  REQUIRE_APPROVED default "true" -- only pick PRs carrying the `approved` label
  MERGE_METHOD     merge | squash | rebase (default squash)
  BATCH_MAX_AGE_H  auto-cancel a stuck/abandoned batch after N hours (default 12)
------------------------------------------------------------------------------
"""

import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone

from github import Github  # PyGithub

# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
REPO_NAME = os.getenv("REPO_NAME", "apache/doris")
GITHUB_TOKEN = os.environ["GITHUB_TOKEN"]
MERGE_TOKEN = os.getenv("MERGE_TOKEN", GITHUB_TOKEN)
MAX_BATCH = int(os.getenv("MAX_BATCH", "10"))
REQUIRE_APPROVED = os.getenv("REQUIRE_APPROVED", "true").lower() == "true"
MERGE_METHOD = os.getenv("MERGE_METHOD", "squash")
BATCH_MAX_AGE_H = float(os.getenv("BATCH_MAX_AGE_H", "12"))

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
BATCH_UTILS = os.path.join(
    REPO_ROOT, "regression-test", "pipeline", "common", "batch-utils.sh"
)

QUEUE_LABEL_PREFIX = "batch-queue/"          # tracking issue label -> target branch
STATE_MARKER = "<!-- DORIS-BATCH-STATE -->"  # identifies the machine-state comment
STATE_JSON_OPEN = "<!-- STATE-JSON"
STATE_JSON_CLOSE = "STATE-JSON -->"

# batch lifecycle states; ACTIVE_STATES hold the per-branch lock.
ACTIVE_STATES = {"assembling", "testing", "failed", "merging"}
TERMINAL_STATES = {"done", "canceled"}

gh = Github(GITHUB_TOKEN)
repo = gh.get_repo(REPO_NAME)
gh_merge = Github(MERGE_TOKEN)
repo_merge = gh_merge.get_repo(REPO_NAME)


def log(msg):
    print(f"[batch] {msg}", flush=True)


def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# --------------------------------------------------------------------------- #
# git / shell helpers
# --------------------------------------------------------------------------- #
def sh(cmd, check=True, cwd=REPO_ROOT, capture=False):
    log("$ " + (cmd if isinstance(cmd, str) else " ".join(cmd)))
    res = subprocess.run(
        cmd, shell=isinstance(cmd, str), cwd=cwd, check=False,
        text=True, capture_output=capture,
    )
    if capture:
        if res.stdout:
            print(res.stdout, end="")
    if check and res.returncode != 0:
        raise RuntimeError(f"command failed ({res.returncode}): {cmd}\n{res.stderr or ''}")
    return res


def bash_func(snippet, check=True, capture=True):
    """Run a snippet that sources batch-utils.sh (reuse teamcity-utils.sh)."""
    return sh(["bash", "-c", f"source {BATCH_UTILS}; {snippet}"],
              check=check, capture=capture)


def required_contexts(target_branch):
    out = bash_func(f'list_required_contexts "{target_branch}"').stdout
    return [ln.strip() for ln in out.splitlines() if ln.strip()]


def trigger_batch_ci(vehicle_pr, target_branch, vehicle_sha):
    # env (GITHUB_TOKEN etc.) is inherited by the child bash.
    bash_func(
        f'trigger_batch_ci "{vehicle_pr}" "{target_branch}" "{vehicle_sha}"',
        check=True, capture=False,
    )


# --------------------------------------------------------------------------- #
# tracking issue <-> target branch, and batch state (stored in a marker comment)
# --------------------------------------------------------------------------- #
def target_branch_of_issue(issue):
    for lbl in issue.labels:
        if lbl.name.startswith(QUEUE_LABEL_PREFIX):
            return lbl.name[len(QUEUE_LABEL_PREFIX):]
    return None


def find_state_comment(issue):
    """Return (comment, state_dict) for this issue's batch record, or (None, None)."""
    for c in issue.get_comments():
        if STATE_MARKER in c.body and STATE_JSON_OPEN in c.body:
            try:
                raw = c.body.split(STATE_JSON_OPEN, 1)[1].split(STATE_JSON_CLOSE, 1)[0]
                return c, json.loads(raw)
            except Exception as e:  # noqa: BLE001
                log(f"WARN: unparseable state comment {c.id}: {e}")
    return None, None


def render_state(st):
    prs = " ".join(f"#{p['num']}" for p in st.get("prs", [])) or "-"
    dropped = st.get("dropped", [])
    drop_line = ""
    if dropped:
        drop_line = "\n| Dropped | " + \
            ", ".join(f"#{d['num']} ({d['reason']})" for d in dropped) + " |"
    vehicle = f"#{st['vehicle_pr']}" if st.get("vehicle_pr") else "-"
    isha = (st.get("integration_sha") or "")[:12]
    return (
        f"{STATE_MARKER}\n"
        f"### 🚂 Batch `{st['batch_id']}` — **{st['state']}**\n\n"
        f"| | |\n|---|---|\n"
        f"| Target | `{st['target_branch']}` |\n"
        f"| Vehicle PR | {vehicle} |\n"
        f"| Integration | `{st.get('integration_branch','-')}` @ `{isha}` |\n"
        f"| Members ({len(st.get('prs', []))}) | {prs} |{drop_line}\n"
        f"| Updated | {st['updated_at']} |\n\n"
        f"<sub>machine state (do not edit):</sub>\n"
        f"{STATE_JSON_OPEN}\n{json.dumps(st, indent=2)}\n{STATE_JSON_CLOSE}\n"
    )


def write_state(issue, comment, st):
    st["updated_at"] = now_iso()
    body = render_state(st)
    if comment is None:
        comment = issue.create_comment(body)
    else:
        comment.edit(body)
    return comment


def announce(issue, text):
    issue.create_comment(text)


# --------------------------------------------------------------------------- #
# PR selection
# --------------------------------------------------------------------------- #
def conflict_label_for(target_branch):
    # branch-4.0 -> dev/4.0.x-conflict (see .github/workflows/auto-cherry-pick.yml)
    m = re.match(r"branch-(\d+\.\d+)", target_branch)
    return f"dev/{m.group(1)}.x-conflict" if m else None


def is_eligible(pr, target_branch):
    if pr.draft:
        return False, "draft"
    labels = {l.name for l in pr.labels}
    cflabel = conflict_label_for(target_branch)
    if cflabel and cflabel in labels:
        return False, "conflict-label"
    if REQUIRE_APPROVED and "approved" not in labels:
        return False, "not-approved"
    # pr.mergeable is computed async and may be None; only reject a *known* conflict.
    if pr.mergeable is False:
        return False, "not-mergeable"
    return True, ""


def select_prs(target_branch, explicit_nums):
    """Return a list of PR objects to attempt, ascending by number (~= master
    merge order for auto-picks). Explicit list = operator vouches; still filtered."""
    picked = []
    if explicit_nums:
        for n in explicit_nums:
            pr = repo.get_pull(n)
            if pr.base.ref != target_branch:
                log(f"skip #{n}: base {pr.base.ref} != {target_branch}")
                continue
            ok, why = is_eligible(pr, target_branch)
            if not ok:
                log(f"skip #{n}: {why}")
                continue
            picked.append(pr)
    else:
        for pr in repo.get_pulls(state="open", base=target_branch,
                                 sort="created", direction="asc"):
            ok, _ = is_eligible(pr, target_branch)
            if ok:
                picked.append(pr)
            if len(picked) >= MAX_BATCH:
                break
    picked.sort(key=lambda p: p.number)
    return picked


# --------------------------------------------------------------------------- #
# assembly: build the integration branch by cherry-picking each member's commits
# --------------------------------------------------------------------------- #
def git_setup():
    sh('git config user.email "dev@doris.apache.org"')
    sh('git config user.name "doris-batch-bot"')


def assemble(target_branch, prs, integration_branch):
    """Fresh integration branch off current <target_branch> tip; cherry-pick each
    member PR's own commits in order. Returns (members, dropped) where members is
    [{num, head}] actually included and dropped is [{num, reason}]."""
    sh(f"git fetch origin {target_branch}")
    sh(f"git checkout -B {integration_branch} origin/{target_branch}")
    members, dropped = [], []
    for pr in prs:
        num = pr.number
        try:
            sh(f"git fetch origin pull/{num}/head")
            head = sh("git rev-parse FETCH_HEAD", capture=True).stdout.strip()
            mb = sh(f"git merge-base origin/{target_branch} {head}",
                    capture=True).stdout.strip()
            # range mb..head = the commits this PR adds on top of the branch
            rng = sh(f"git rev-list --count {mb}..{head}", capture=True).stdout.strip()
            if rng == "0":
                dropped.append({"num": num, "reason": "no new commits vs base"})
                continue
            sh(f"git cherry-pick {mb}..{head}")
            members.append({"num": num, "head": head})
            log(f"picked #{num} ({rng} commit(s))")
        except RuntimeError:
            log(f"conflict picking #{num}; aborting and dropping it")
            sh("git cherry-pick --abort", check=False)
            dropped.append({"num": num, "reason": "cherry-pick conflict"})
    return members, dropped


def push_integration(integration_branch):
    sh(f"git push -f origin {integration_branch}")


def ensure_vehicle_pr(st, issue):
    """Create the vehicle PR if absent (run), else return the existing one (rerun)."""
    if st.get("vehicle_pr"):
        return repo.get_pull(st["vehicle_pr"])
    title = f"[test](batch) DO NOT MERGE — batch {st['batch_id']} → {st['target_branch']}"
    body = (
        f"Batch integration vehicle. **Do not merge or comment `run` here.**\n\n"
        f"Tracking issue: #{issue.number}\n"
        f"Members: " + " ".join(f"#{p['num']}" for p in st["prs"]) + "\n\n"
        f"Validated changes are admitted to the member PRs on green."
    )
    pr = repo.create_pull(title=title, body=body,
                          head=st["integration_branch"], base=st["target_branch"])
    return pr


# --------------------------------------------------------------------------- #
# status detection on the vehicle SHA
# --------------------------------------------------------------------------- #
def status_map(sha):
    """context -> state (success/failure/error/pending) for the latest status."""
    commit = repo.get_commit(sha)
    combined = commit.get_combined_status()
    return {s.context: s.state for s in combined.statuses}


def classify(target_branch, sha):
    """Return ('green'|'failed'|'pending', pending_ctxs, failed_ctxs)."""
    req = required_contexts(target_branch)
    have = status_map(sha)
    pending, failed = [], []
    for ctx in req:
        state = have.get(ctx)
        if state == "success":
            continue
        if state in ("failure", "error"):
            failed.append(ctx)
        else:  # None (not reported yet) or "pending"
            pending.append(ctx)
    if failed:
        return "failed", pending, failed
    if pending:
        return "pending", pending, failed
    return "green", [], []


# --------------------------------------------------------------------------- #
# admission + merge (reconcile against live state)
# --------------------------------------------------------------------------- #
def broadcast_success(sha, contexts, target_url):
    commit = repo.get_commit(sha)
    for ctx in contexts:
        commit.create_status(
            state="success", context=ctx, target_url=target_url,
            description="Admitted via batch build",
        )


def reconcile_and_merge(st, issue):
    """On green: for each member, reconcile against live GitHub state, broadcast
    admission statuses, and merge. Returns (merged, skipped[list of (num,reason)])."""
    target = st["target_branch"]
    contexts = required_contexts(target)
    vehicle_url = ""
    if st.get("vehicle_pr"):
        vehicle_url = f"https://github.com/{REPO_NAME}/pull/{st['vehicle_pr']}"
    merged, skipped = [], []
    for m in st["prs"]:
        num, tested_head = m["num"], m["head"]
        pr = repo.get_pull(num)
        # (a) already merged/closed externally -> nothing to do
        if pr.merged or pr.state == "closed":
            skipped.append((num, "already merged/closed"))
            continue
        # (b) member changed after we assembled/tested it -> untested content
        if pr.head.sha != tested_head:
            skipped.append((num, "head changed after batch; excluded"))
            continue
        # (c) admit: broadcast success onto the member's head SHA
        try:
            broadcast_success(pr.head.sha, contexts, vehicle_url)
        except Exception as e:  # noqa: BLE001
            skipped.append((num, f"could not set admission status: {e}"))
            log(f"broadcast #{num} failed: {e}")
            continue
        # (d) merge; base may have moved -> conflict/blocked -> skip and report
        try:
            repo_merge.get_pull(num).merge(merge_method=MERGE_METHOD)
            merged.append(num)
            log(f"merged #{num}")
        except Exception as e:  # noqa: BLE001
            skipped.append((num, f"merge blocked: {e}"))
            log(f"merge #{num} failed: {e}")
    return merged, skipped


def cleanup(st):
    """Close the vehicle PR and delete the integration branch."""
    try:
        if st.get("vehicle_pr"):
            vp = repo.get_pull(st["vehicle_pr"])
            if vp.state == "open":
                vp.edit(state="closed")
    except Exception as e:  # noqa: BLE001
        log(f"WARN: closing vehicle PR failed: {e}")
    try:
        ref = repo.get_git_ref(f"heads/{st['integration_branch']}")
        ref.delete()
    except Exception as e:  # noqa: BLE001
        log(f"WARN: deleting integration branch failed: {e}")


def finish_green(st, comment, issue):
    st["state"] = "merging"
    write_state(issue, comment, st)
    merged, skipped = reconcile_and_merge(st, issue)
    lines = ["✅ **Batch green — admission complete.**", ""]
    lines.append(f"Merged ({len(merged)}): " +
                 (" ".join(f"#{n}" for n in merged) or "-"))
    if skipped:
        lines.append("Skipped:")
        lines += [f"  - #{n}: {why}" for n, why in skipped]
        lines.append("_Skipped members were not merged — re-queue them in a new batch._")
    announce(issue, "\n".join(lines))
    cleanup(st)
    st["state"] = "done"
    st["merged"] = merged
    st["skipped"] = skipped
    write_state(issue, comment, st)


# --------------------------------------------------------------------------- #
# commands
# --------------------------------------------------------------------------- #
def parse_pr_list(comment_body):
    # explicit "#101 #102" (or "101 102") after the command word
    return [int(x) for x in re.findall(r"#?(\d{2,})", comment_body or "")]


def cmd_run(issue):
    target = target_branch_of_issue(issue)
    if not target:
        log("issue is not a batch-queue tracking issue; ignoring")
        return
    comment, st = find_state_comment(issue)
    if st and st["state"] in ACTIVE_STATES:
        announce(issue, f"⚠️ A batch is already in flight (`{st['batch_id']}`, "
                        f"state **{st['state']}**). Wait for it to finish or "
                        f"comment `cancel batch`.")
        return

    explicit = parse_pr_list(os.getenv("COMMENT_BODY", ""))
    prs = select_prs(target, explicit)
    if not prs:
        announce(issue, "No eligible PRs found for a batch.")
        return

    batch_id = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    integration_branch = f"batch-{target}-{batch_id}"
    st = {
        "schema": "doris-batch/v1",
        "batch_id": batch_id,
        "target_branch": target,
        "tracking_issue": issue.number,
        "integration_branch": integration_branch,
        "integration_sha": None,
        "vehicle_pr": None,
        "state": "assembling",
        "prs": [],
        "dropped": [],
        "updated_at": now_iso(),
    }
    comment = write_state(issue, comment, st)

    git_setup()
    members, dropped = assemble(target, prs, integration_branch)
    if not members:
        announce(issue, "All candidate PRs conflicted during assembly; nothing to test.")
        st["state"] = "canceled"
        st["dropped"] = dropped
        write_state(issue, comment, st)
        return
    push_integration(integration_branch)
    st["prs"] = members
    st["dropped"] = dropped
    st["integration_sha"] = sh("git rev-parse HEAD", capture=True).stdout.strip()

    vehicle = ensure_vehicle_pr(st, issue)
    st["vehicle_pr"] = vehicle.number
    st["integration_sha"] = vehicle.head.sha  # authoritative head TeamCity will build
    st["state"] = "testing"
    write_state(issue, comment, st)

    trigger_batch_ci(st["vehicle_pr"], target, st["integration_sha"])

    msg = [f"🚂 **Batch `{batch_id}` started** on `{target}` — vehicle #{st['vehicle_pr']}.",
           "", f"Members ({len(members)}): " + " ".join(f"#{m['num']}" for m in members)]
    if dropped:
        msg.append("Dropped: " + ", ".join(f"#{d['num']} ({d['reason']})" for d in dropped))
    msg.append("\nCI running; results will be posted here.")
    announce(issue, "\n".join(msg))


def cmd_rerun(issue):
    comment, st = find_state_comment(issue)
    if not st or st["state"] not in ACTIVE_STATES:
        announce(issue, "No active batch to rerun.")
        return
    target = st["target_branch"]

    # reconcile the member set against live state: drop already-merged/closed,
    # refresh remaining members to their CURRENT head (picks up any fix pushed).
    live = []
    for m in st["prs"]:
        pr = repo.get_pull(m["num"])
        if pr.merged or pr.state == "closed":
            log(f"#{m['num']} already merged/closed; dropping from rerun")
            continue
        live.append(pr)
    if not live:
        announce(issue, "All members are already merged/closed; nothing to rerun. "
                        "Comment `cancel batch` to release the lock.")
        return

    st["state"] = "assembling"
    write_state(issue, comment, st)
    git_setup()
    members, dropped = assemble(target, live, st["integration_branch"])
    if not members:
        announce(issue, "All members conflicted on re-assembly; nothing to test.")
        st["state"] = "failed"
        write_state(issue, comment, st)
        return
    push_integration(st["integration_branch"])
    st["prs"] = members
    st["dropped"] = dropped

    vehicle = repo.get_pull(st["vehicle_pr"])  # head auto-updated by the force-push
    st["integration_sha"] = vehicle.head.sha
    st["state"] = "testing"
    write_state(issue, comment, st)

    trigger_batch_ci(st["vehicle_pr"], target, st["integration_sha"])
    announce(issue, f"🔁 **Batch `{st['batch_id']}` re-assembled and re-running** "
                    f"({len(members)} members).")


def cmd_cancel(issue):
    comment, st = find_state_comment(issue)
    if not st or st["state"] in TERMINAL_STATES:
        announce(issue, "No active batch to cancel.")
        return
    cleanup(st)
    st["state"] = "canceled"
    write_state(issue, comment, st)
    announce(issue, f"🛑 Batch `{st['batch_id']}` canceled; lock released.")


def poll_issue(issue):
    comment, st = find_state_comment(issue)
    if not st or st["state"] not in ("testing", "failed"):
        return
    target = st["target_branch"]
    sha = st["integration_sha"]

    # auto-cancel stuck/abandoned batches
    age_h = (datetime.now(timezone.utc) -
             datetime.strptime(st["updated_at"], "%Y-%m-%dT%H:%M:%SZ")
             .replace(tzinfo=timezone.utc)).total_seconds() / 3600.0
    if age_h > BATCH_MAX_AGE_H:
        cleanup(st)
        st["state"] = "canceled"
        write_state(issue, comment, st)
        announce(issue, f"⌛ Batch `{st['batch_id']}` exceeded {BATCH_MAX_AGE_H}h; "
                        f"auto-canceled. Start a fresh `run batch`.")
        return

    kind, pending, failed = classify(target, sha)
    if kind == "green":
        # covers both a normal pass and the operator fixing a red sub-build in
        # TeamCity directly (state was 'failed', now all contexts are success).
        finish_green(st, comment, issue)
    elif kind == "failed" and st["state"] == "testing":
        st["state"] = "failed"
        write_state(issue, comment, st)
        announce(issue,
                 "❌ **Batch CI failed.**\n\n"
                 f"Failed: {', '.join(failed)}\n"
                 f"Still pending: {', '.join(pending) or '-'}\n\n"
                 "Options: `rerun batch` (re-assemble same set at current heads / "
                 "dodge flaky), fix the failed build directly in TeamCity (I will "
                 "auto-detect green), or `cancel batch`.")
    # kind == 'pending', or already-'failed' still red -> keep waiting


def cmd_poll():
    branches = [b.strip() for b in os.getenv("BRANCHES", "").split(",") if b.strip()]
    for br in branches:
        for issue in repo.get_issues(state="open", labels=[f"{QUEUE_LABEL_PREFIX}{br}"]):
            try:
                poll_issue(issue)
            except Exception as e:  # noqa: BLE001
                log(f"poll error on issue #{issue.number}: {e}")


# --------------------------------------------------------------------------- #
def main():
    if len(sys.argv) < 2:
        sys.exit("usage: batch-cherry-pick.py {run|rerun|cancel} ISSUE | poll")
    cmd = sys.argv[1]
    if cmd == "poll":
        cmd_poll()
        return
    issue = repo.get_issue(int(sys.argv[2]))
    {"run": cmd_run, "rerun": cmd_rerun, "cancel": cmd_cancel}[cmd](issue)


if __name__ == "__main__":
    main()
