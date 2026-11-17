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

import http.server
import io
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import unittest
from contextlib import redirect_stderr
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from pathlib import Path
from unittest import mock
from urllib.parse import parse_qs, urlsplit

import review_github_api as api


def trace(status, *, method="GET", retry_after=None):
    header = "" if retry_after is None else f"< Retry-After: {retry_after}\n"
    return ("* Request at now\n> " + method + " /repos/a/b/pulls HTTP/2.0\n"
            + f"< HTTP/2.0 {status} Result\n< X-GitHub-Request-Id: retry-test\n"
            + header + "\nresponse\n* Request took 1ms\n")


class ReadRetryTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.enterContext(mock.patch.dict(os.environ, {"REVIEW_GITHUB_DIAGNOSTICS_DIR": self.tmp.name}))
        self.enterContext(redirect_stderr(io.StringIO()))
        self.now = 0
        self.sleeps = []
        self.enterContext(mock.patch.object(api.time, "monotonic", side_effect=lambda: self.now))
        self.enterContext(mock.patch.object(api.time, "sleep", side_effect=self.sleep))
        self.command = ["gh", "api", "repos/a/b/pulls"]

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds

    def failure(self, status=503, *, method="GET", retry_after=None, stdout="partial"):
        return subprocess.CalledProcessError(1, self.command, stdout,
                                             trace(status, method=method, retry_after=retry_after))

    def success(self, stdout='[{"id":2}]'):
        return subprocess.CompletedProcess(self.command, 0, stdout, trace(200))

    def run_get(self, command=None, **kwargs):
        return api.run_gh(command or self.command, check=True, capture_output=True, text=True, **kwargs)

    def records(self):
        return [json.loads(p.read_text()) for p in Path(self.tmp.name).glob("*.json")]

    def test_known_get_forms_only(self):
        for args in (["repos/a/b/pulls"], ["--paginate", "--slurp", "repos/a/b/pulls"],
                     ["repos/a/b/pulls", "-H", "Accept: application/json"],
                     ["-X", "GET", "repos/a/b/pulls"], ["--method=GET", "repos/a/b/pulls"],
                     ["-XGET", "repos/a/b/pulls"]):
            with self.subTest(args=args):
                self.assertTrue(api.is_read_only_get(["gh", "api", *args]))
        for args in (["graphql"], ["https://api.github.com/graphql"],
                     ["repos/a/b/pulls", "-f", "title=x"], ["repos/a/b/pulls", "-ftitle=x"],
                     ["repos/a/b/pulls", "--field=title=x"], ["repos/a/b/pulls", "-Ftitle=x"],
                     ["repos/a/b/pulls", "--input", "-"], ["repos/a/b/pulls", "--input=-"],
                     ["repos/a/b/pulls", "-X", "POST"], ["repos/a/b/pulls", "-XPATCH"],
                     ["repos/a/b/pulls", "--method=DELETE"],
                     ["repos/a/b/pulls", "-XGET", "-XPOST"],
                     ["repos/a/b/pulls", "--unknown"], ["repos/a/b/pulls", "-H"], []):
            with self.subTest(args=args):
                self.assertFalse(api.is_read_only_get(["gh", "api", *args]))
        self.assertFalse(api.is_read_only_get(["gh", "pr", "comment", "1"]))

    def test_transient_statuses_retry_and_return_only_complete_success(self):
        for status in (502, 503, 504):
            with self.subTest(status=status):
                with mock.patch.object(api.subprocess, "run", side_effect=[self.failure(status), self.success()]) as run:
                    result = self.run_get()
                self.assertEqual(result.stdout, '[{"id":2}]')
                self.assertEqual(run.call_count, 2)
                self.assertEqual(result.stderr, "")
        self.assertEqual(self.sleeps, [2, 2, 2])
        self.assertEqual(len(self.records()), 3)
        self.assertTrue(all(r["stdout_excerpt"] == "partial" for r in self.records()))

    def test_retry_after_seconds_are_respected(self):
        with mock.patch.object(api.subprocess, "run", side_effect=[self.failure(retry_after="7"), self.success()]):
            self.run_get()
        self.assertEqual(self.sleeps, [7])

    def test_retry_after_dates_and_invalid_values(self):
        now = datetime(2026, 10, 23, tzinfo=timezone.utc)
        with mock.patch.object(api, "datetime") as clock:
            clock.now.return_value = now
            for value, expected in ((format_datetime(now + timedelta(seconds=9)), 9),
                                    (format_datetime(now - timedelta(seconds=9)), 2),
                                    ("invalid", None), ("-1", None), ("9" * 5000, None)):
                with self.subTest(value=value[:40]):
                    self.assertEqual(api.retry_delay({"headers": {"retry-after": value}}, 1), expected)

    def test_long_or_invalid_retry_after_does_not_retry_early(self):
        for value in ("1000", "bad-date"):
            with self.subTest(value=value), mock.patch.object(api.subprocess, "run", side_effect=self.failure(retry_after=value)) as run:
                with self.assertRaises(subprocess.CalledProcessError):
                    self.run_get()
                self.assertEqual(run.call_count, 1)
        self.assertEqual(self.sleeps, [])

    def test_caller_budget_includes_attempts_and_sleep(self):
        def first(*args, **kwargs):
            self.now += 3
            raise self.failure()
        with mock.patch.object(api.subprocess, "run", side_effect=[self.failure(), self.success()]) as run:
            self.run_get(timeout=10)
        self.assertEqual([c.kwargs["timeout"] for c in run.call_args_list], [10, 8])
        self.assertEqual(self.sleeps, [2])
        self.now = 0
        with mock.patch.object(api.subprocess, "run", side_effect=first) as run:
            with self.assertRaises(subprocess.CalledProcessError):
                self.run_get(timeout=5)
        self.assertEqual(run.call_count, 1)  # No room for the server/backoff wait.

    def test_default_budget_caps_each_attempt_and_whole_operation(self):
        def fail(*args, **kwargs):
            self.now += 29
            raise self.failure()
        with mock.patch.object(api.subprocess, "run", side_effect=fail) as run:
            with self.assertRaises(subprocess.CalledProcessError):
                self.run_get()
        self.assertEqual([c.kwargs["timeout"] for c in run.call_args_list], [30, 30, 26])
        self.assertEqual(self.sleeps, [2, 4])

    def test_exhaustion_keeps_all_diagnostics_but_no_partial_stdout(self):
        with mock.patch.object(api.subprocess, "run", side_effect=[self.failure() for _ in range(3)]) as run:
            with self.assertRaises(subprocess.CalledProcessError) as caught:
                self.run_get()
        self.assertEqual(run.call_count, 3)
        self.assertEqual(caught.exception.stdout, "")
        self.assertEqual(caught.exception.returncode, 1)
        self.assertIn("retry-test", caught.exception.stderr)
        self.assertEqual(len(self.records()), 3)

    def test_non_transient_statuses_are_not_retried(self):
        for status in (400, 401, 403, 404, 422, 429, 500):
            with self.subTest(status=status), mock.patch.object(api.subprocess, "run", side_effect=self.failure(status)) as run:
                with self.assertRaises(subprocess.CalledProcessError):
                    self.run_get()
                self.assertEqual(run.call_count, 1)
        self.assertEqual(self.sleeps, [])

    def test_writes_never_retry_even_if_redirect_ends_in_get(self):
        commands = [["gh", "pr", "comment", "1", "--body", "x"],
                    [*self.command, "-X", "POST"], [*self.command, "-f", "state=pending"],
                    [*self.command, "--input", "-"], ["gh", "api", "graphql"]]
        for command in commands:
            with self.subTest(command=command), mock.patch.object(api.subprocess, "run", side_effect=self.failure()) as run:
                with self.assertRaises(subprocess.CalledProcessError) as caught:
                    self.run_get(command)
                self.assertEqual(run.call_count, 1)
                self.assertEqual(caught.exception.stdout, "partial")
                self.assertNotIn("timeout", run.call_args.kwargs)
        self.assertEqual(self.sleeps, [])

    def test_input_and_uncaptured_calls_do_not_retry(self):
        for options in ({"input": "body"}, {"capture_output": False}):
            kwargs = dict(check=True, capture_output=True, text=True)
            kwargs.update(options)
            with self.subTest(options=options), mock.patch.object(api.subprocess, "run", side_effect=self.failure()) as run:
                with self.assertRaises(subprocess.CalledProcessError):
                    api.run_gh(self.command, **kwargs)
                self.assertEqual(run.call_count, 1)

    def test_timeouts_transport_errors_and_signals_do_not_retry(self):
        signal = self.failure()
        signal.returncode = -15
        for error in (subprocess.TimeoutExpired(self.command, 3, output="partial", stderr=trace(503)),
                      OSError("transport"), signal,
                      subprocess.CalledProcessError(1, self.command, "", "connection reset")):
            with self.subTest(error=type(error)), mock.patch.object(api.subprocess, "run", side_effect=error) as run:
                with self.assertRaises(type(error)):
                    self.run_get()
                self.assertEqual(run.call_count, 1)
        self.assertEqual(self.sleeps, [])

    def test_sleep_overrun_does_not_start_another_request(self):
        with mock.patch.object(api.time, "sleep", side_effect=lambda _: setattr(self, "now", 90)), \
             mock.patch.object(api.subprocess, "run", side_effect=self.failure()) as run:
            with self.assertRaises(subprocess.CalledProcessError):
                self.run_get()
        self.assertEqual(run.call_count, 1)

    def test_json_decode_failure_is_not_retried(self):
        with mock.patch.object(api.subprocess, "run", return_value=self.success("{")) as run:
            result = self.run_get()
            with self.assertRaises(json.JSONDecodeError):
                api.decode_json(result, self.command)
        self.assertEqual(run.call_count, 1)

    def test_cli_timeout_returns_124(self):
        with mock.patch.object(api, "run_gh", side_effect=subprocess.TimeoutExpired(self.command, 30, stderr="timed out")):
            self.assertEqual(api.main(), 124)


@unittest.skipUnless(os.environ.get("RUN_GH_HTTP_TESTS") == "1" and shutil.which("gh"), "opt-in real gh loopback test")
class GetRetryHTTPTest(unittest.TestCase):
    def test_real_get_recovery_pagination_exhaustion_and_write_isolation(self):
        requests = []
        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_GET(self):
                parsed = urlsplit(self.path)
                page = parse_qs(parsed.query).get("page", [None])[0]
                route = parsed.path + (f"?page={page}" if page is not None else "")
                requests.append(("GET", route))
                count = requests.count(("GET", route))
                failed = route == "/always" or (count == 1 and route in ("/once", "/pages?page=2"))
                status = 504 if failed and "page=2" in self.path else 503 if failed else 200
                data = b"Service Unavailable" if failed else b'[{"id":2}]' if "page=2" in self.path else b'[{"id":1}]'
                self.send_response(status)
                self.send_header("Content-Type", "text/plain" if failed else "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.send_header("Retry-After", "0")
                if route == "/pages?page=1":
                    self.send_header("Link", f'<http://127.0.0.1:{self.server.server_port}/pages?page=2>; rel="next"')
                self.end_headers()
                self.wfile.write(data)

            def do_POST(self):
                requests.append(("POST", self.path))
                self.send_response(503)
                self.send_header("Content-Length", "0")
                self.end_headers()

        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as tmp:
                env = {"PATH": os.environ["PATH"], "GH_TOKEN": "fake-only", "GH_ENTERPRISE_TOKEN": "fake-only",
                       "GH_CONFIG_DIR": tmp, "GH_NO_UPDATE_NOTIFIER": "1", "GH_PROMPT_DISABLED": "1",
                       "REVIEW_GITHUB_DIAGNOSTICS_DIR": tmp}
                base = f"http://127.0.0.1:{server.server_port}"
                helper = Path(__file__).with_name("review_github_api.py")
                def call(path, *args):
                    return subprocess.run([sys.executable, str(helper), "api", base + path, *args],
                                          env=env, capture_output=True, text=True, timeout=20)
                result = call("/once")
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(json.loads(result.stdout), [{"id": 1}])
                self.assertEqual(requests, [("GET", "/once")] * 2)
                requests.clear()
                result = call("/pages?page=1", "--paginate", "--slurp")
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(json.loads(result.stdout), [[{"id": 1}], [{"id": 2}]])
                self.assertEqual(requests, [("GET", "/pages?page=1"), ("GET", "/pages?page=2")] * 2)
                requests.clear()
                result = call("/always")
                self.assertEqual(result.returncode, 1)
                self.assertEqual(result.stdout, "")
                self.assertEqual(requests, [("GET", "/always")] * 3)
                requests.clear()
                result = call("/write", "-X", "POST", "-f", "state=pending")
                self.assertEqual(result.returncode, 1)
                self.assertEqual(requests, [("POST", "/write")])
                records = [json.loads(p.read_text()) for p in Path(tmp).glob("*.json")]
                self.assertEqual(len(records), 6)
                self.assertNotIn("fake-only", json.dumps(records))
        finally:
            server.shutdown()
            server.server_close()
            thread.join()


if __name__ == "__main__":
    unittest.main()
