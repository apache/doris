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

import io
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from contextlib import redirect_stderr
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import emit_litefuse_otel_io as exporter
import run_review_with_resume as runner

THREAD = "0199a213-81c0-7800-8aa1-bbab2a035a53"
CHILD = "0199a213-81c0-7800-8aa1-bbab2a035a54"
OTHER = "0199a213-81c0-7800-8aa1-bbab2a035a55"


def thread_event(thread_id=THREAD):
    return {"type": "thread.started", "thread_id": thread_id}


def failed(message=runner.CAPACITY_MESSAGE):
    return {"type": "turn.failed", "error": {"message": message}}


def completed():
    return {"type": "turn.completed", "usage": {"input_tokens": 20, "output_tokens": 3}}


class ResumeReviewTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        self.context = root / "context"
        self.context.mkdir()
        self.cwd = root / "checkout"
        self.cwd.mkdir()
        self.codex_home = root / "codex-home"
        self.sessions = self.codex_home / "sessions" / "2026" / "09" / "07"
        self.sessions.mkdir(parents=True)
        self.args = SimpleNamespace(
            context_dir=self.context,
            cwd=self.cwd,
            repository="apache/doris",
            pr_number="123",
            head_sha="a" * 40,
            base_sha="b" * 40,
            model="gpt-5.6-sol",
            effort="xhigh",
            budget_seconds=1000,
        )
        (self.context / "codex_goal_prompt.txt").write_text(
            "Read review_prompt.txt; complete the review."
        )
        self.ledger = self.context / "subagent_review_findings.md"
        self.ledger.write_text("Completed risk scan and round 1; keep these findings.")
        self.write_rollout()
        self.clock = 0
        self.commands = []
        self.timeouts = []
        self.sleeps = []
        self.enterContext(
            mock.patch.dict(os.environ, {"CODEX_HOME": str(self.codex_home)})
        )
        self.enterContext(redirect_stderr(io.StringIO()))
        self.enterContext(
            mock.patch.object(runner.time, "monotonic", side_effect=lambda: self.clock)
        )
        self.sleep = self.enterContext(
            mock.patch.object(runner.time, "sleep", side_effect=self.advance)
        )
        self.target_check = self.enterContext(
            mock.patch.object(runner, "check_resume_target")
        )
        self.help = self.enterContext(mock.patch.object(runner.subprocess, "run"))

    def write_rollout(self, *, thread_id=THREAD, cwd=None):
        (self.sessions / f"rollout-2026-09-07-{thread_id}.jsonl").write_text(
            json.dumps(
                {
                    "type": "session_meta",
                    "payload": {"id": thread_id, "cwd": str(cwd or self.cwd)},
                }
            )
            + "\n"
        )

    def advance(self, delay):
        self.sleeps.append(delay)
        self.clock += delay

    def execute(self, attempts):
        pending = iter(attempts)

        def fake_attempt(command, events_path, stderr_path, timeout, reaper=None):
            self.commands.append(command)
            self.timeouts.append(timeout)
            spec = next(pending)
            self.clock += spec.get("elapsed", 0)
            events_path.write_text(
                spec.get("raw", "".join(json.dumps(e) + "\n" for e in spec["events"]))
            )
            stderr_path.write_text(spec.get("stderr", ""))
            if "output" in spec:
                Path(command[command.index("--output-last-message") + 1]).write_text(
                    spec["output"]
                )
            if "raises" in spec:
                raise spec["raises"]
            return spec.get("status", 1)

        with mock.patch.object(runner, "run_attempt", side_effect=fake_attempt):
            return runner.run_review(self.args)

    def events(self):
        return runner.read_events(self.context / "codex-events.jsonl")

    def last_error(self):
        return self.events()[-1]["error"]["message"]

    def test_success_does_not_retry(self):
        self.assertEqual(
            0, self.execute([{"events": [thread_event(), completed()], "status": 0}])
        )
        self.help.assert_not_called()
        self.target_check.assert_not_called()
        self.assertEqual([], self.sleeps)

    def test_capacity_resumes_exact_session_with_same_settings_and_ledger(self):
        before = self.ledger.read_text()
        self.assertEqual(
            0,
            self.execute(
                [
                    {
                        "events": [thread_event(), failed()],
                        "elapsed": 5,
                        "output": "partial",
                    },
                    {
                        "events": [thread_event(), completed()],
                        "status": 0,
                        "output": "done",
                    },
                ]
            ),
        )
        self.assertEqual([30], self.sleeps)
        self.assertEqual([1000, 965], self.timeouts)
        command = self.commands[1]
        self.assertEqual(["resume", THREAD], command[-3:-1])
        self.assertNotIn("--last", command)
        for option in ("--goal", "--cd", "--model", "--config", "--sandbox", "--json"):
            self.assertIn(option, command)
        self.assertEqual("gpt-5.6-sol", command[command.index("--model") + 1])
        self.assertIn("do not restart the review", command[-1])
        self.assertIn("initial snapshots may be stale", command[-1])
        self.assertIn("do not assume child agents", command[-1])
        self.assertEqual(before, self.ledger.read_text())
        self.assertEqual("done", (self.context / "codex-final-message.txt").read_text())
        self.target_check.assert_called_once()

    def test_retry_count_is_bounded(self):
        self.assertEqual(1, self.execute([{"events": [thread_event(), failed()]}] * 4))
        self.assertEqual([30, 60, 120], self.sleeps)
        self.assertEqual(4, len(self.commands))
        self.assertEqual(runner.CAPACITY_MESSAGE, self.last_error())

    def test_no_retry_for_auth_usage_or_generic_errors_even_before_session_starts(self):
        for message in (
            "You've hit your usage limit. Try again at 9:00 PM.",
            "refresh_token_reused",
            "HTTP 500",
            "permission denied",
        ):
            with self.subTest(message=message), tempfile.TemporaryDirectory() as tmp:
                self.args.context_dir = Path(tmp)
                (self.args.context_dir / "codex_goal_prompt.txt").write_text("review")
                self.assertEqual(1, self.execute([{"events": [failed(message)]}]))
                self.assertEqual(
                    message,
                    runner.read_events(self.args.context_dir / "codex-events.jsonl")[
                        -1
                    ]["error"]["message"],
                )
        self.assertEqual([], self.sleeps)

    def test_stderr_auth_error_is_preserved(self):
        message = "You've hit your usage limit. Try again at 9:00 PM."
        self.assertEqual(1, self.execute([{"events": [], "stderr": message}]))
        self.assertEqual(message, self.last_error())

    def test_capacity_text_inside_tool_output_does_not_trigger_retry(self):
        event = {
            "type": "item.completed",
            "item": {
                "type": "command_execution",
                "aggregated_output": runner.CAPACITY_MESSAGE,
            },
        }
        self.assertEqual(
            1,
            self.execute([{"events": [thread_event(), event, failed("auth failed")]}]),
        )
        self.assertEqual([], self.sleeps)

    def test_missing_session_id_fails_closed(self):
        self.assertEqual(1, self.execute([{"events": [failed()]}]))
        self.assertIn("thread.started", self.last_error())
        self.assertEqual([], self.sleeps)

    def test_thread_name_is_not_accepted_as_resume_id(self):
        self.assertEqual(
            1, self.execute([{"events": [thread_event("latest-review"), failed()]}])
        )
        self.assertEqual([], self.sleeps)

    def test_multiple_bootstrap_ids_fail_closed(self):
        self.assertEqual(
            1,
            self.execute([{"events": [thread_event(), thread_event(CHILD), failed()]}]),
        )
        self.assertEqual([], self.sleeps)

    def test_missing_or_wrong_workspace_rollout_fails_closed(self):
        self.write_rollout(cwd=self.context)
        self.assertEqual(1, self.execute([{"events": [thread_event(), failed()]}]))
        self.assertIn("rollout is missing", self.last_error())
        self.assertEqual([], self.sleeps)

    def test_resuming_another_session_is_rejected(self):
        self.assertEqual(
            1,
            self.execute(
                [
                    {"events": [thread_event(), failed()]},
                    {"events": [thread_event(OTHER), completed()], "status": 0},
                ]
            ),
        )
        self.assertIn("different session", self.last_error())

    def test_unsupported_goal_resume_fails_without_new_request(self):
        self.help.side_effect = subprocess.CalledProcessError(
            2, ["codex", "exec", "--help"]
        )
        self.assertEqual(1, self.execute([{"events": [thread_event(), failed()]}]))
        self.assertEqual(1, len(self.commands))
        self.assertEqual([], self.sleeps)

    def test_budget_is_not_reset_between_attempts(self):
        self.args.budget_seconds = 100
        self.assertEqual(
            1,
            self.execute(
                [
                    {"events": [thread_event(), failed()], "elapsed": 10},
                    {"events": [thread_event(), failed()], "elapsed": 10},
                ]
            ),
        )
        self.assertEqual([100, 60], self.timeouts)
        self.assertEqual([30], self.sleeps)
        self.assertIn("Insufficient shared review budget", self.last_error())

    def test_timeout_preserves_raw_partial_json_but_aggregate_stays_parseable(self):
        raw = json.dumps(thread_event()) + '\n{"type":"item.'
        self.assertEqual(
            1,
            self.execute(
                [
                    {
                        "events": [],
                        "raw": raw,
                        "raises": subprocess.TimeoutExpired(["codex"], 1),
                    }
                ]
            ),
        )
        self.assertIn("timeout exhausted", self.last_error())
        self.assertEqual(raw, (self.context / "codex-attempts/1.jsonl").read_text())
        self.assertEqual([], self.sleeps)

    def test_non_object_json_is_rejected(self):
        self.assertEqual(1, self.execute([{"events": [], "raw": "[]\n"}]))
        self.assertIn("non-object event", self.last_error())

    def test_cancellation_while_waiting_never_resumes(self):
        self.sleep.side_effect = KeyboardInterrupt
        self.assertEqual(130, self.execute([{"events": [thread_event(), failed()]}]))
        self.assertEqual(1, len(self.commands))
        self.target_check.assert_not_called()

    def test_interrupted_process_is_not_retried_even_with_capacity_event(self):
        self.assertEqual(
            1, self.execute([{"events": [thread_event(), failed()], "status": -15}])
        )
        self.assertEqual([], self.sleeps)

    def test_existing_review_or_stale_pr_stops_resume_and_keeps_failure(self):
        self.target_check.side_effect = ValueError("review already submitted")
        self.assertEqual(1, self.execute([{"events": [thread_event(), failed()]}]))
        self.assertEqual(1, len(self.commands))
        self.assertIn("review already submitted", self.last_error())

    def test_failed_attempt_output_is_not_reused_as_final_response(self):
        self.assertEqual(
            1,
            self.execute(
                [
                    {"events": [thread_event(), failed()], "output": "old partial"},
                    {"events": [thread_event(), failed("auth failed")]},
                ]
            ),
        )
        self.assertEqual("", (self.context / "codex-final-message.txt").read_text())
        self.assertEqual(
            "old partial", (self.context / "codex-attempts/1.final.txt").read_text()
        )

    def test_litefuse_retains_failed_attempt_items_and_child_ids_after_recovery(self):
        first_item = {
            "type": "item.completed",
            "item": {
                "type": "collab_tool_call",
                "id": "item_0",
                "tool": "spawn_agent",
                "receiver_thread_ids": [CHILD],
            },
        }
        last_item = {
            "type": "item.completed",
            "item": {"type": "agent_message", "id": "item_0", "text": "done"},
        }
        self.assertEqual(
            0,
            self.execute(
                [
                    {"events": [thread_event(), first_item, failed()]},
                    {"events": [thread_event(), last_item, completed()], "status": 0},
                ]
            ),
        )
        events = exporter.load_jsonl(str(self.context / "codex-events.jsonl"))
        self.assertEqual({CHILD}, exporter.receiver_thread_ids(events))
        self.assertEqual("completed", exporter.latest_turn_result(events)[0])
        args = SimpleNamespace(
            **vars(self.args),
            reasoning_effort="xhigh",
            run_id="123",
            workflow="review",
            trace_name="review",
            session_id="123",
            environment="test",
            max_json_chars=4000,
            max_context_json_chars=4000,
        )
        _, payload, _ = exporter.build_ingestion_payload(args, "review", "done", events)
        items = [
            event["body"]
            for event in payload["batch"]
            if event["body"].get("metadata", {}).get("item_id") == "item_0"
        ]
        self.assertEqual(2, len(items))
        self.assertNotEqual(items[0]["id"], items[1]["id"])
        self.assertNotEqual(
            items[0]["metadata"]["event_line"], items[1]["metadata"]["event_line"]
        )


class ResumeTargetTest(unittest.TestCase):
    def setUp(self):
        self.args = SimpleNamespace(
            repository="apache/doris",
            pr_number="123",
            head_sha="a" * 40,
            base_sha="b" * 40,
        )
        self.pr = {
            "state": "open",
            "head": {"sha": self.args.head_sha},
            "base": {"sha": self.args.base_sha},
        }
        self.review = {
            "user": {"login": "github-actions[bot]"},
            "commit_id": self.args.head_sha,
            "submitted_at": "2026-09-07T11:30:00Z",
        }

    def check(self, pages):
        def api(command, **kwargs):
            data = pages if "--paginate" in command else self.pr
            return SimpleNamespace(stdout=json.dumps(data))

        with mock.patch.object(runner.subprocess, "run", side_effect=api):
            runner.check_resume_target(self.args, "2026-09-07T11:00:00Z", lambda: 500)

    def test_published_review_on_later_page_prevents_duplicate_resume(self):
        with self.assertRaisesRegex(ValueError, "already submitted"):
            self.check([[], [self.review]])

    def test_other_heads_humans_and_old_reviews_do_not_block(self):
        old = {**self.review, "submitted_at": "2026-09-07T10:00:00Z"}
        human = {**self.review, "user": {"login": "reviewer"}}
        other = {**self.review, "commit_id": "c" * 40}
        self.check([[old, human, other]])

    def test_changed_pr_prevents_resume(self):
        self.pr["base"]["sha"] = "c" * 40
        with self.assertRaisesRegex(ValueError, "changed"):
            self.check([[]])

    def test_unavailable_api_does_not_blindly_resume(self):
        with (
            mock.patch.object(
                runner.subprocess,
                "run",
                side_effect=subprocess.CalledProcessError(1, ["gh"]),
            ),
            self.assertRaises(subprocess.CalledProcessError),
        ):
            runner.check_resume_target(self.args, "2026-09-07T11:00:00Z", lambda: 500)


class ChildReaperTest(unittest.TestCase):
    def setUp(self):
        self.reaper = runner.ChildReaper.__new__(runner.ChildReaper)
        self.reaper.children = mock.Mock()
        self.reaper.children.read_text.side_effect = ["10", "20", ""]
        self.open = self.enterContext(mock.patch.object(os, "pidfd_open", create=True))
        self.open.side_effect = [110, 120]
        self.close = self.enterContext(mock.patch.object(os, "close"))
        self.wait = self.enterContext(mock.patch.object(os, "waitid", create=True))
        self.enterContext(mock.patch.object(os, "P_PIDFD", 3, create=True))
        self.send = self.enterContext(
            mock.patch.object(signal, "pidfd_send_signal", create=True)
        )
        self.enterContext(mock.patch.object(runner.time, "sleep"))

    def test_unsupported_platform_fails_before_starting_codex(self):
        with (
            mock.patch.object(runner.sys, "platform", "darwin"),
            self.assertRaisesRegex(OSError, "requires Linux"),
        ):
            runner.ChildReaper()
        self.open.assert_not_called()

    def test_subreaper_setup_failure_is_not_silenced(self):
        with (
            mock.patch.object(runner.sys, "platform", "linux"),
            mock.patch.object(Path, "read_text", return_value=""),
            mock.patch.object(runner.ctypes, "CDLL") as libc,
            mock.patch.object(runner.ctypes, "get_errno", return_value=1),
        ):
            libc.return_value.prctl.return_value = -1
            with self.assertRaises(OSError):
                runner.ChildReaper()
            libc.return_value.prctl.assert_called_once_with(36, 1, 0, 0, 0)
        self.close.assert_called_once_with(110)

    def test_reaps_newly_adopted_descendants_with_stable_handles(self):
        self.reaper.reap()
        self.assertEqual([mock.call(10), mock.call(20)], self.open.call_args_list)
        self.assertEqual(
            [mock.call(110, signal.SIGKILL), mock.call(120, signal.SIGKILL)],
            self.send.call_args_list,
        )
        self.assertEqual([mock.call(110), mock.call(120)], self.close.call_args_list)
        self.assertEqual(
            mock.call(3, 110, os.WEXITED | os.WNOHANG | os.WNOWAIT),
            self.wait.call_args_list[0],
        )

    def test_non_child_pid_is_never_signalled(self):
        self.reaper.children.read_text.side_effect = ["10", ""]
        self.wait.side_effect = ChildProcessError()
        self.reaper.reap()
        self.send.assert_not_called()
        self.close.assert_called_once_with(110)

    def test_already_exited_child_is_safe(self):
        self.reaper.children.read_text.side_effect = ["10", ""]
        self.open.side_effect = ProcessLookupError()
        self.reaper.reap()
        self.send.assert_not_called()
        self.close.assert_not_called()

    def test_cleanup_deadline_fails_closed(self):
        with (
            mock.patch.object(runner.time, "monotonic", side_effect=[0, 6]),
            self.assertRaisesRegex(OSError, "cleanup did not finish"),
        ):
            self.reaper.reap()
        self.send.assert_not_called()


class ProcessLifecycleTest(unittest.TestCase):
    def test_copier_construction_failure_cleans_process(self):
        self.check_startup_cleanup("construct")

    def test_copier_start_failure_cleans_process(self):
        self.check_startup_cleanup("start")

    def test_cancellation_before_copier_starts_cleans_process(self):
        self.check_startup_cleanup("cancel_before")

    def test_cancellation_after_copier_starts_cleans_process(self):
        self.check_startup_cleanup("cancel_after")

    def check_startup_cleanup(self, stage):
        processes = []
        copiers = []
        original_mask = signal.pthread_sigmask(signal.SIG_BLOCK, set())
        real_popen = subprocess.Popen
        real_thread = runner.threading.Thread
        real_start = real_thread.start
        reaper = mock.Mock()

        def create_process(*args, **kwargs):
            process = real_popen(*args, **kwargs)
            processes.append(process)
            return process

        def create_thread(*args, **kwargs):
            if stage == "construct":
                raise RuntimeError("copier construction failed")
            thread = real_thread(*args, **kwargs)
            copiers.append(thread)
            return thread

        def start_thread(thread):
            if stage == "start":
                raise RuntimeError("copier start failed")
            if stage == "cancel_before":
                os.kill(os.getpid(), signal.SIGTERM)
            real_start(thread)
            if stage == "cancel_after":
                os.kill(os.getpid(), signal.SIGTERM)

        def cancelled(_signum, _frame):
            raise KeyboardInterrupt

        original_handler = signal.signal(signal.SIGTERM, cancelled)
        try:
            with (
                tempfile.TemporaryDirectory() as tmp,
                redirect_stderr(io.StringIO()),
                mock.patch.object(
                    runner.subprocess, "Popen", side_effect=create_process
                ),
                mock.patch.object(
                    runner.threading, "Thread", side_effect=create_thread
                ),
                mock.patch.object(real_thread, "start", start_thread),
            ):
                expected = (
                    KeyboardInterrupt if stage.startswith("cancel") else RuntimeError
                )
                with self.assertRaises(expected) as error:
                    runner.run_attempt(
                        [sys.executable, "-c", "import time; time.sleep(20)"],
                        Path(tmp) / "events",
                        Path(tmp) / "stderr",
                        5,
                        reaper=reaper,
                    )
                if expected is RuntimeError:
                    self.assertIn("copier", str(error.exception))
                self.assertIsNotNone(
                    processes[0].poll(), "Codex survived startup failure"
                )
                self.assertTrue(processes[0].stderr.closed)
                reaper.reap.assert_called_once_with()
                self.assertTrue(all(not thread.is_alive() for thread in copiers))
                self.assertEqual(
                    original_mask, signal.pthread_sigmask(signal.SIG_BLOCK, set())
                )
        finally:
            signal.signal(signal.SIGTERM, original_handler)
            signal.pthread_sigmask(signal.SIG_SETMASK, original_mask)
            for process in processes:
                if process.poll() is None:
                    process.kill()
                process.wait(timeout=5)
            for thread in copiers:
                if thread.ident is not None:
                    thread.join(timeout=5)
            for process in processes:
                process.stderr.close()

    def test_interrupted_popen_still_calls_reaper(self):
        reaper = mock.Mock()
        with (
            tempfile.TemporaryDirectory() as tmp,
            mock.patch.object(
                runner.subprocess, "Popen", side_effect=KeyboardInterrupt
            ),
            self.assertRaises(KeyboardInterrupt),
        ):
            runner.run_attempt(
                ["codex"], Path(tmp) / "events", Path(tmp) / "stderr", 5, reaper=reaper
            )
        reaper.reap.assert_called_once_with()

    def test_real_process_capacity_then_resume_preserves_state(self):
        with tempfile.TemporaryDirectory() as tmp, redirect_stderr(io.StringIO()):
            root = Path(tmp)
            (root / "codex_goal_prompt.txt").write_text("test review")
            ledger = root / "subagent_review_findings.md"
            ledger.write_text("completed round 1")
            fake_codex = root / "codex"
            fake_codex.write_text(
                f"#!{sys.executable}\n"
                + r"""
import json, os, sys
from pathlib import Path
if "--help" in sys.argv:
    print("exec --goal resume SESSION_ID PROMPT")
    sys.exit(0)
root = Path(os.environ["FAKE_REVIEW_ROOT"])
thread_id = os.environ["FAKE_THREAD_ID"]
print(json.dumps({"type": "thread.started", "thread_id": thread_id}), flush=True)
if "resume" in sys.argv:
    assert sys.argv[sys.argv.index("resume") + 1] == thread_id
    assert (root / "subagent_review_findings.md").read_text() == "completed round 1"
    assert (root / "request-count").read_text() == "1"
    (root / "request-count").write_text("2")
    Path(sys.argv[sys.argv.index("--output-last-message") + 1]).write_text("review complete")
    print(json.dumps({"type": "turn.completed", "usage": {"input_tokens": 123}}))
else:
    sessions = Path(os.environ["CODEX_HOME"]) / "sessions"
    sessions.mkdir(parents=True)
    (sessions / f"rollout-{thread_id}.jsonl").write_text(json.dumps({
        "type": "session_meta", "payload": {"id": thread_id, "cwd": str(root)}
    }) + "\n")
    (root / "request-count").write_text("1")
    print(json.dumps({"type": "turn.failed", "error": {
        "message": "Selected model is at capacity. Please try a different model."
    }}))
    sys.exit(1)
"""
            )
            fake_codex.chmod(0o700)
            args = SimpleNamespace(
                context_dir=root,
                cwd=root,
                repository="apache/doris",
                pr_number="123",
                head_sha="a" * 40,
                base_sha="b" * 40,
                model="gpt-5.6-sol",
                effort="xhigh",
                budget_seconds=30,
            )
            with (
                mock.patch.dict(
                    os.environ,
                    {
                        "PATH": str(root) + os.pathsep + os.environ["PATH"],
                        "CODEX_HOME": str(root / "isolated-home"),
                        "FAKE_REVIEW_ROOT": str(root),
                        "FAKE_THREAD_ID": THREAD,
                    },
                ),
                mock.patch.object(runner, "RETRY_DELAYS", (0, 0, 0)),
                mock.patch.object(runner, "check_resume_target"),
            ):
                self.assertEqual(0, runner.run_review(args))
            self.assertEqual("2", (root / "request-count").read_text())
            self.assertEqual(
                "review complete", (root / "codex-final-message.txt").read_text()
            )
            self.assertEqual(2, len(list((root / "codex-attempts").glob("*.jsonl"))))

    def test_actual_process_captures_stdout_and_stderr(self):
        with tempfile.TemporaryDirectory() as tmp, redirect_stderr(io.StringIO()):
            root = Path(tmp)
            status = runner.run_attempt(
                [
                    sys.executable,
                    "-c",
                    "import sys; print('output'); print('error', file=sys.stderr)",
                ],
                root / "events",
                root / "stderr",
                5,
            )
            self.assertEqual(0, status)
            self.assertEqual("output\n", (root / "events").read_text())
            self.assertEqual("error\n", (root / "stderr").read_text())

    def test_timeout_terminates_codex_process(self):
        with tempfile.TemporaryDirectory() as tmp, redirect_stderr(io.StringIO()):
            root = Path(tmp)
            program = "import os,time; print(os.getpid(), flush=True); time.sleep(60)"
            with self.assertRaises(subprocess.TimeoutExpired):
                runner.run_attempt(
                    [sys.executable, "-c", program],
                    root / "events",
                    root / "stderr",
                    0.2,
                )
            pid = int((root / "events").read_text())
            with self.assertRaises(ProcessLookupError):
                os.kill(pid, 0)

    @unittest.skipUnless(sys.platform == "linux", "CLI supervision requires Linux")
    def test_cli_sigterm_cleans_up_child_and_does_not_restart(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "codex_goal_prompt.txt").write_text(
                "test only; do not call a service"
            )
            fake_codex = root / "codex"
            fake_codex.write_text(
                f"#!{sys.executable}\n"
                + "import json,os,time\nfrom pathlib import Path\n"
                + f"print(json.dumps({thread_event()!r}), flush=True)\n"
                + "Path(os.environ['FAKE_CHILD_PID']).write_text(str(os.getpid()))\n"
                + "time.sleep(60)\n"
            )
            fake_codex.chmod(0o700)
            pid_file = root / "child.pid"
            command = [
                sys.executable,
                str(Path(runner.__file__).resolve()),
                "--context-dir",
                str(root),
                "--cwd",
                str(root),
                "--repository",
                "apache/doris",
                "--pr-number",
                "123",
                "--head-sha",
                "a" * 40,
                "--base-sha",
                "b" * 40,
                "--model",
                "gpt-5.6-sol",
                "--effort",
                "xhigh",
                "--budget-seconds",
                "30",
            ]
            process = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env={
                    **os.environ,
                    "PATH": str(root) + os.pathsep + os.environ["PATH"],
                    "CODEX_HOME": str(root / "isolated-home"),
                    "FAKE_CHILD_PID": str(pid_file),
                },
            )
            try:
                deadline = time.monotonic() + 5
                while not pid_file.exists() and time.monotonic() < deadline:
                    time.sleep(0.01)
                self.assertTrue(pid_file.exists(), "fake Codex did not start")
                process.send_signal(signal.SIGTERM)
                process.communicate(timeout=10)
                self.assertEqual(130, process.returncode)
                self.assertEqual(
                    1, len(list((root / "codex-attempts").glob("*.jsonl")))
                )
                events = runner.read_events(root / "codex-events.jsonl")
                self.assertEqual(
                    "Review cancelled; not resuming", events[-1]["error"]["message"]
                )
                with self.assertRaises(ProcessLookupError):
                    os.kill(int(pid_file.read_text()), 0)
            finally:
                if process.poll() is None:
                    process.kill()
                process.communicate(timeout=10)

    @unittest.skipUnless(sys.platform == "linux", "requires Linux subreaper/pidfd")
    def test_cli_reaps_detached_descendants_without_touching_other_processes(self):
        for stop in ("cancel", "timeout", "exit"):
            for graceful in (True, False):
                with self.subTest(stop=stop, graceful=graceful):
                    self.check_detached_descendants(stop, graceful)

    def check_detached_descendants(self, stop, graceful):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "codex_goal_prompt.txt").write_text("local process test only")
            fake_codex = root / "codex"
            fake_codex.write_text(
                f"#!{sys.executable}\n"
                + r"""
import json, os, signal, subprocess, sys, time
from pathlib import Path
root = Path(os.environ["TREE_ROOT"])
role = sys.argv[1]
if role == "worker":
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
elif role == "shell":
    worker = subprocess.Popen([sys.executable, __file__, "worker"], start_new_session=True)
    print(worker.pid, flush=True)
else:
    shell = subprocess.Popen(
        [sys.executable, __file__, "shell"], start_new_session=True,
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True,
    )
    worker_pid = int(shell.stdout.readline())
    def interrupted(_signum, _frame):
        (root / "interrupted").touch()
        shell.terminate()
        shell.wait()
        sys.exit(0)
    signal.signal(signal.SIGINT, interrupted if os.environ["TREE_GRACEFUL"] == "1" else signal.SIG_IGN)
    (root / "pids.json").write_text(json.dumps([shell.pid, worker_pid]))
    print(json.dumps({"type": "thread.started", "thread_id": os.environ["TREE_THREAD"]}), flush=True)
    if os.environ["TREE_STOP"] == "exit":
        print(json.dumps({"type": "turn.failed", "error": {"message": "test failure"}}), flush=True)
        sys.exit(1)
deadline = time.monotonic() + 20
while time.monotonic() < deadline:
    time.sleep(1)
"""
            )
            fake_codex.chmod(0o700)
            command = [
                sys.executable,
                "-c",
                (
                    "import sys; sys.path.insert(0, sys.argv.pop(1)); "
                    "import run_review_with_resume as r; "
                    "r.PROCESS_EXIT_GRACE_SECONDS = 0.5; sys.exit(r.main())"
                ),
                str(Path(runner.__file__).parent),
                "--context-dir",
                str(root),
                "--cwd",
                str(root),
                "--repository",
                "apache/doris",
                "--pr-number",
                "123",
                "--head-sha",
                "a" * 40,
                "--base-sha",
                "b" * 40,
                "--model",
                "test",
                "--effort",
                "xhigh",
                "--budget-seconds",
                "2" if stop == "timeout" else "30",
            ]
            with subprocess.Popen(
                [sys.executable, "-c", "import time; time.sleep(30)"],
                start_new_session=True,
            ) as unrelated:
                process = subprocess.Popen(
                    command,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    env={
                        **os.environ,
                        "PATH": str(root) + os.pathsep + os.environ["PATH"],
                        "TREE_ROOT": str(root),
                        "TREE_THREAD": THREAD,
                        "TREE_STOP": stop,
                        "TREE_GRACEFUL": str(int(graceful)),
                        "CODEX_HOME": str(root / "isolated-home"),
                    },
                )
                try:
                    deadline = time.monotonic() + 5
                    while (
                        not (root / "pids.json").exists()
                        and time.monotonic() < deadline
                    ):
                        time.sleep(0.01)
                    self.assertTrue(
                        (root / "pids.json").exists(), "fake Codex did not start"
                    )
                    if stop == "cancel":
                        process.send_signal(signal.SIGTERM)
                    _, stderr = process.communicate(timeout=10)
                    self.assertEqual(
                        130 if stop == "cancel" else 1, process.returncode, stderr
                    )
                    for pid in json.loads((root / "pids.json").read_text()):
                        with self.assertRaises(ProcessLookupError):
                            os.kill(pid, 0)
                    self.assertIsNone(
                        unrelated.poll(), "cleanup killed an unrelated process"
                    )
                    self.assertEqual(
                        1, len(list((root / "codex-attempts").glob("*.jsonl")))
                    )
                    if stop != "exit":
                        self.assertEqual(graceful, (root / "interrupted").exists())
                finally:
                    if process.poll() is None:
                        process.kill()
                    process.communicate(timeout=5)
                    unrelated.kill()


if __name__ == "__main__":
    unittest.main()
