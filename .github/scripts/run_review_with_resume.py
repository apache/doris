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

"""Resume the same review after capacity failures, never restart the workflow."""

import argparse
import ctypes
import json
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

RETRY_DELAYS = (30, 60, 120)
CAPACITY_MESSAGE = "Selected model is at capacity. Please try a different model."
PROCESS_EXIT_GRACE_SECONDS = 5


class ChildReaper:
    """Own orphaned commands in the standalone Linux helper, not the whole runner."""

    def __init__(self):
        if sys.platform != "linux":
            raise OSError("Review process supervision requires Linux")
        # Fail before starting Codex if the runner cannot provide safe cleanup.
        for module, name in (
            (os, "pidfd_open"),
            (os, "P_PIDFD"),
            (signal, "pidfd_send_signal"),
        ):
            if not hasattr(module, name):
                raise OSError(f"Review process supervision requires {name}")
        fd = os.pidfd_open(os.getpid())
        try:
            signal.pidfd_send_signal(fd, 0)
            try:
                os.waitid(os.P_PIDFD, fd, os.WEXITED | os.WNOHANG | os.WNOWAIT)
            except ChildProcessError:
                pass  # Expected: this process is not its own child.
        finally:
            os.close(fd)
        self.children = Path(f"/proc/self/task/{os.getpid()}/children")
        self.children.read_text()
        prctl = ctypes.CDLL(None, use_errno=True).prctl
        prctl.argtypes = [ctypes.c_int] + [ctypes.c_ulong] * 4
        prctl.restype = ctypes.c_int
        # PR_SET_CHILD_SUBREAPER: descendants that outlive Codex are reparented
        # to this helper, even if shell/PTY commands created new sessions.
        if prctl(36, 1, 0, 0, 0) != 0:
            error = ctypes.get_errno()
            raise OSError(error, os.strerror(error))

    def reap(self):
        # Called only after Popen.wait() reaps Codex. This dedicated helper has
        # no other concurrent subprocesses; gh/help commands run between attempts.
        deadline = time.monotonic() + PROCESS_EXIT_GRACE_SECONDS
        while children := self.children.read_text().split():
            if time.monotonic() >= deadline:
                raise OSError("Codex descendant cleanup did not finish; not resuming")
            for child in children:
                try:
                    fd = os.pidfd_open(int(child))
                except ProcessLookupError:
                    continue
                try:
                    # Kernel-verified parenthood plus a stable pidfd prevents
                    # signalling an unrelated process if a PID was recycled.
                    os.waitid(os.P_PIDFD, fd, os.WEXITED | os.WNOHANG | os.WNOWAIT)
                    signal.pidfd_send_signal(fd, signal.SIGKILL)
                    os.waitid(os.P_PIDFD, fd, os.WEXITED | os.WNOHANG)
                except (ChildProcessError, ProcessLookupError):
                    pass
                finally:
                    os.close(fd)
            # Killing one orphan can reparent its children to us on the next pass.
            time.sleep(0.01)


def read_events(path):
    with path.open() as handle:
        events = [json.loads(line) for line in handle if line.strip()]
    if any(not isinstance(event, dict) for event in events):
        raise ValueError("Codex JSONL contains a non-object event")
    return events


def failure(events, status, stderr_path):
    for event_type in ("turn.failed", "error"):
        for event in reversed(events):
            if event.get("type") == event_type:
                error = event.get("error") or event
                return error.get("message") or f"Codex exited with status {status}"
    lines = stderr_path.read_text(errors="replace").splitlines()
    return next(
        (line for line in reversed(lines) if line.strip()),
        f"Codex exited with status {status}",
    )


def session_id(events):
    ids = [
        event.get("thread_id")
        for event in events
        if event.get("type") == "thread.started"
    ]
    if len(ids) != 1 or not isinstance(ids[0], str):
        raise ValueError("Expected exactly one main thread.started event")
    # Names and --last can fall back to a new thread in codex exec. A UUID uses
    # thread/resume directly and fails if the persisted thread cannot be loaded.
    if str(uuid.UUID(ids[0])) != ids[0]:
        raise ValueError("Main session ID is not a canonical UUID")
    return ids[0]


def require_rollout(codex_home, thread_id, cwd):
    for path in (codex_home / "sessions").rglob(f"*{thread_id}.jsonl"):
        with path.open() as handle:
            meta = json.loads(handle.readline())
        payload = meta.get("payload") or {}
        if (
            meta.get("type") == "session_meta"
            and payload.get("id") == thread_id
            and payload.get("cwd")
            and Path(payload["cwd"]).resolve() == cwd.resolve()
        ):
            return
    raise ValueError("Main session rollout is missing; refusing to start a new review")


def stop_process(process):
    try:
        # Codex handles SIGINT through its graceful turn-interrupt/shutdown path.
        process.send_signal(signal.SIGINT)
        process.wait(timeout=PROCESS_EXIT_GRACE_SECONDS)
    except subprocess.TimeoutExpired:
        pass
    finally:
        process.kill()
        process.wait()


def run_attempt(command, events_path, stderr_path, timeout, reaper=None):
    with events_path.open("w") as stdout, stderr_path.open("w") as stderr:
        process = None
        copier = None
        try:
            process = subprocess.Popen(
                command,
                stdout=stdout,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="replace",
                start_new_session=True,
            )

            def copy_stderr():
                for line in process.stderr:
                    stderr.write(line)
                    stderr.flush()
                    print(line, end="", file=sys.stderr, flush=True)

            copier = threading.Thread(target=copy_stderr, daemon=True)
            # Do not interrupt Thread.start() between the native thread being
            # created and its ident being published. The copier inherits this
            # mask; pending cancellation reaches the main thread on restoration,
            # inside the cleanup-protected region. Codex was spawned unmasked.
            previous_mask = signal.pthread_sigmask(
                signal.SIG_BLOCK, {signal.SIGINT, signal.SIGTERM}
            )
            try:
                copier.start()
            finally:
                signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)
            return process.wait(timeout=timeout)
        finally:
            try:
                try:
                    if process is not None:
                        stop_process(process)
                finally:
                    # Popen itself may be interrupted before returning a handle.
                    if reaper is not None:
                        reaper.reap()
            finally:
                if copier is not None and copier.ident is not None:
                    copier.join(timeout=5)
                    if copier.is_alive():
                        raise OSError(
                            "Codex stderr remained open after descendant cleanup"
                        )
                if process is not None:
                    process.stderr.close()


def append_events(source, target):
    # Interrupted writes can leave a partial JSON line. Retain raw bytes in the
    # attempt file, but keep the aggregate parseable by jq and the trace uploader.
    with source.open() as src, target.open("a") as dst:
        for line in src:
            if not line.strip():
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(event, dict):
                dst.write(json.dumps(event) + "\n")


def check_resume_target(args, started_at, remaining):
    def api(path, paginated=False):
        command = ["gh", "api", path]
        if paginated:
            command += ["--paginate", "--slurp"]
        result = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            timeout=min(30, remaining()),
        )
        return json.loads(result.stdout)

    pr = api(f"repos/{args.repository}/pulls/{args.pr_number}")
    if (
        pr["state"] != "open"
        or pr["head"]["sha"] != args.head_sha
        or pr["base"]["sha"] != args.base_sha
    ):
        raise ValueError(
            "PR base/head or open state changed; refusing to resume stale context"
        )
    pages = api(
        f"repos/{args.repository}/pulls/{args.pr_number}/reviews", paginated=True
    )
    for page in pages:
        for review in page:
            if (
                review.get("user", {}).get("login") == "github-actions[bot]"
                and review.get("commit_id") == args.head_sha
                and (review.get("submitted_at") or "") >= started_at
            ):
                # A capacity error can arrive after the final GitHub write.
                # Keep the failure visible for manual verification instead of
                # replaying a possibly already-completed external side effect.
                raise ValueError(
                    "A bot review was already submitted during this run; "
                    "refusing automatic resume to avoid duplicate submission"
                )


def resume_prompt(goal_prompt):
    return (
        goal_prompt
        + "\n\n"
        + (
            "Recovery after a temporary model-capacity error in this SAME review session. "
            "Continue unfinished work using the existing conversation and shared subagent review ledger; "
            "do not restart the review, reset the ledger, or reset the three-round limit. "
            "The previous process exited: do not assume child agents or shell processes are still running. "
            "Recover unfinished child-agent work using their recorded IDs and ledger sections where possible. "
            "Keep completed findings and replace only work that could not be recovered. "
            "Before any GitHub write, fetch the current PR reviews and inline comments again; "
            "the initial snapshots may be stale. Verify already-submitted work and never submit it twice. "
            "Mark the goal complete only after the original review completion criteria are satisfied."
        )
    )


def run_review(args, reaper=None):
    context = args.context_dir
    aggregate = context / "codex-events.jsonl"
    stderr_log = context / "codex-stderr.log"
    final_message = context / "codex-final-message.txt"
    attempts = context / "codex-attempts"
    attempts.mkdir()
    aggregate.touch()
    stderr_log.touch()
    goal_prompt = (context / "codex_goal_prompt.txt").read_text()
    deadline = time.monotonic() + args.budget_seconds
    started_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    thread_id = None

    def remaining():
        seconds = deadline - time.monotonic()
        if seconds <= 0:
            raise TimeoutError("Review's shared time budget was exhausted")
        return seconds

    def fail(message):
        print(message, file=sys.stderr, flush=True)
        with aggregate.open("a") as handle:
            handle.write(
                json.dumps(
                    {
                        "type": "turn.failed",
                        "source": "review-resume-runner",
                        "error": {"message": message},
                    }
                )
                + "\n"
            )
        return 1

    try:
        for attempt in range(len(RETRY_DELAYS) + 1):
            events_path = attempts / f"{attempt + 1}.jsonl"
            stderr_path = attempts / f"{attempt + 1}.stderr.log"
            output_path = attempts / f"{attempt + 1}.final.txt"
            final_message.write_text("")
            command = [
                "codex",
                "exec",
                "--goal",
                "--cd",
                str(args.cwd),
                "--model",
                args.model,
                "--config",
                f"model_reasoning_effort={args.effort}",
                "--sandbox",
                "danger-full-access",
                "--color",
                "never",
                "--json",
                "--output-last-message",
                str(output_path),
            ]
            if thread_id:
                command += ["resume", thread_id, resume_prompt(goal_prompt)]
            else:
                command += [goal_prompt]
            print(
                f"Starting Codex review attempt {attempt + 1}/4 "
                f"(session={thread_id or 'new'}, remaining={remaining():.0f}s)",
                file=sys.stderr,
                flush=True,
            )
            try:
                status = run_attempt(
                    command, events_path, stderr_path, remaining(), reaper=reaper
                )
            finally:
                # Preserve failed attempts and their child-thread IDs for Litefuse.
                if events_path.exists():
                    append_events(events_path, aggregate)
                if stderr_path.exists():
                    with stderr_path.open("rb") as src, stderr_log.open("ab") as dst:
                        shutil.copyfileobj(src, dst)
                        dst.write(b"\n")
                if output_path.exists():
                    shutil.copyfile(output_path, final_message)

            events = read_events(events_path)
            if status < 0 or status in (124, 130, 137, 143):
                return fail(
                    f"Codex was interrupted or timed out (status {status}); not resuming"
                )
            message = failure(events, status, stderr_path)
            if status != 0 and (
                message != CAPACITY_MESSAGE or attempt == len(RETRY_DELAYS)
            ):
                return fail(message)
            current_id = session_id(events)
            if thread_id and current_id != thread_id:
                return fail(
                    "Codex resumed a different session; refusing further attempts"
                )
            thread_id = current_id
            if status == 0:
                return 0
            require_rollout(Path(os.environ["CODEX_HOME"]), thread_id, args.cwd)
            # Check parser support without authenticating or starting a model request.
            subprocess.run(
                ["codex", "exec", "--goal", "resume", "--help"],
                check=True,
                capture_output=True,
                timeout=min(10, remaining()),
            )
            delay = RETRY_DELAYS[attempt]
            if remaining() <= delay:
                return fail("Insufficient shared review budget for capacity backoff")
            print(
                f"Capacity unavailable; waiting {delay}s before resuming {thread_id}",
                file=sys.stderr,
                flush=True,
            )
            with aggregate.open("a") as handle:
                handle.write(
                    json.dumps(
                        {
                            "type": "review.capacity_retry",
                            "thread_id": thread_id,
                            "next_attempt": attempt + 2,
                            "delay_seconds": delay,
                        }
                    )
                    + "\n"
                )
            time.sleep(delay)
            check_resume_target(args, started_at, remaining)
    except KeyboardInterrupt:
        fail("Review cancelled; not resuming")
        return 130
    except subprocess.TimeoutExpired:
        return fail(
            "Review recovery stopped: shared time budget or helper timeout exhausted"
        )
    except (ValueError, OSError, subprocess.SubprocessError) as exc:
        return fail(f"Review recovery stopped: {exc}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--context-dir", type=Path, required=True)
    parser.add_argument("--cwd", type=Path, required=True)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--pr-number", required=True)
    parser.add_argument("--head-sha", required=True)
    parser.add_argument("--base-sha", required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--effort", required=True)
    # Leave one minute for the existing GitHub verification within the 90-minute step.
    parser.add_argument("--budget-seconds", type=int, default=89 * 60)
    args = parser.parse_args()

    def cancelled(_signum, _frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, cancelled)
    return run_review(args, reaper=ChildReaper())


if __name__ == "__main__":
    sys.exit(main())
