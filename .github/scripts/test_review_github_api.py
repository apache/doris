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
import re
import shutil
import subprocess
import sys
import threading
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path
from unittest import mock

import review_github_api as diagnostic


def trace(status=502, body='{"message":', path="/repos/apache/doris/statuses/abc"):
    return (
        "* Request at 2026-10-21\n"
        "* Request to https://api.github.com" + path + "\n"
        "> POST " + path + " HTTP/2.0\n"
        "> Authorization: token request-secret\n"
        "> Cookie: request-cookie\n\n"
        '{"private_request":"DO_NOT_PERSIST_REQUEST_PAYLOAD"}\n\n'
        f"< HTTP/2.0 {status} Example\n"
        "< Content-Type: application/json\n"
        "< X-Github-Request-Id: REQUEST-123\n"
        "< X-Ratelimit-Remaining: 0\n"
        "< Retry-After: 10\n"
        "< Set-Cookie: response-cookie\n"
        "< Authorization: Bearer response-secret\n\n"
        + body + "\n* Request took 1ms\n"
    )


class GitHubDiagnosticsTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.directory = Path(temp.name)
        self.env = self.enterContext(mock.patch.dict(os.environ, {
            "REVIEW_GITHUB_DIAGNOSTICS_DIR": temp.name,
            "GH_TOKEN": "env-github-secret",
            "LANGFUSE_SECRET_KEY": "env-telemetry-secret",
        }))
        self.logs = self.enterContext(redirect_stderr(io.StringIO()))
        self.command = ["gh", "api", "repos/apache/doris/statuses/abc",
                        "-X", "POST", "-f", "sensitive=COMMAND_SECRET"]

    def records(self):
        return [json.loads(p.read_text()) for p in sorted(self.directory.glob("*.json"))]

    def test_malformed_http_error_retains_status_headers_body_and_exit(self):
        error = subprocess.CalledProcessError(1, self.command, "", trace() + "unexpected end of JSON input\n")
        with mock.patch.object(diagnostic.subprocess, "run", side_effect=error) as run:
            with self.assertRaises(subprocess.CalledProcessError) as result:
                diagnostic.run_gh(self.command, capture_output=True, text=True, check=True, timeout=10)
        self.assertIs(error, result.exception)
        self.assertEqual(1, run.call_count)  # Never replay a write for diagnostics.
        self.assertEqual("api", run.call_args.kwargs["env"]["GH_DEBUG"])
        record, = self.records()
        self.assertEqual(502, record["http"]["status"])
        self.assertEqual('{"message":', record["http"]["body_excerpt"])
        self.assertEqual("REQUEST-123", record["http"]["headers"]["x-github-request-id"])
        self.assertEqual("10", record["http"]["headers"]["retry-after"])
        self.assertEqual(1, record["returncode"])
        self.assertIn("unexpected end of JSON input", error.stderr)
        data = json.dumps(record)
        for secret in ("COMMAND_SECRET", "request-secret", "request-cookie",
                       "response-cookie", "response-secret", "DO_NOT_PERSIST_REQUEST_PAYLOAD"):
            self.assertNotIn(secret, data)
        self.assertEqual(0o600, next(self.directory.glob("*.json")).stat().st_mode & 0o777)

    def test_success_stdout_and_pagination_are_unchanged_and_not_archived(self):
        output = '[[{"id":1}],[{"id":2}]]\n'
        response = subprocess.CompletedProcess(self.command, 0, output, trace(200, '{"id":1}')+trace(200, '{"id":2}'))
        with mock.patch.object(diagnostic.subprocess, "run", return_value=response):
            actual = diagnostic.run_gh(self.command, check=True, capture_output=True, text=True)
        self.assertEqual(output, actual.stdout)
        self.assertEqual("", actual.stderr)
        self.assertEqual(2, actual.github_response["request_count"])
        self.assertEqual([], self.records())
        self.assertEqual("", self.logs.getvalue())

    def test_later_page_failure_has_last_request_not_previous_body(self):
        stderr = trace(200, '{"previous":"large successful body"}') + trace(503, "")
        error = subprocess.CalledProcessError(1, self.command, "partial stdout", stderr+"unexpected end of JSON input")
        with mock.patch.object(diagnostic.subprocess, "run", side_effect=error):
            with self.assertRaises(subprocess.CalledProcessError):
                diagnostic.run_gh(self.command, check=True)
        http = self.records()[0]["http"]
        self.assertEqual((503, 2, ""), (http["status"], http["request_count"], http["body_excerpt"]))

    def test_timeout_keeps_partial_response_and_does_not_retry(self):
        error = subprocess.TimeoutExpired(self.command, 10, output=b"", stderr=trace(502).encode())
        with mock.patch.object(diagnostic.subprocess, "run", side_effect=error) as run:
            with self.assertRaises(subprocess.TimeoutExpired):
                diagnostic.run_gh(self.command, check=True, timeout=10)
        record, = self.records()
        self.assertEqual(10, record["timeout_seconds"])
        self.assertEqual(502, record["http"]["status"])
        self.assertEqual(1, run.call_count)

    def test_transport_failure_without_response_does_not_claim_http_status(self):
        stderr = trace().split("< HTTP/",1)[0] + "* Request took 1ms\nPost: connection reset by peer\n"
        error = subprocess.CalledProcessError(1, self.command, "", stderr)
        with mock.patch.object(diagnostic.subprocess, "run", side_effect=error):
            with self.assertRaises(subprocess.CalledProcessError):
                diagnostic.run_gh(self.command, check=True)
        record, = self.records()
        self.assertIsNone(record["http"]["status"])
        self.assertEqual("", record["http"]["body_excerpt"])
        self.assertIn("connection reset", record["stderr"])

    def test_json_decode_failure_keeps_success_status_and_raw_output_excerpt(self):
        response = subprocess.CompletedProcess(self.command, 0, "{", trace(201, "{"))
        with mock.patch.object(diagnostic.subprocess, "run", return_value=response):
            result = diagnostic.run_gh(self.command, check=True)
        with self.assertRaises(json.JSONDecodeError):
            diagnostic.decode_json(result, self.command)
        record, = self.records()
        self.assertEqual(201, record["http"]["status"])
        self.assertEqual("JSONDecodeError", record["error_type"])
        self.assertEqual("{", record["stdout_excerpt"])

    def test_credential_echoes_are_redacted_in_artifacts_and_summary(self):
        body = '{"access_token":"not-in-env","password":"another-secret","message":"env-github-secret env-telemetry-secret ghp_ExampleToken"}'
        error = subprocess.CalledProcessError(1, self.command, body, trace(403,body)+"env-github-secret")
        with mock.patch.object(diagnostic.subprocess, "run", side_effect=error):
            with self.assertRaises(subprocess.CalledProcessError):
                diagnostic.run_gh(self.command, check=True)
        data = json.dumps(self.records()) + self.logs.getvalue()
        for secret in ("not-in-env","another-secret","env-github-secret","env-telemetry-secret","ghp_ExampleToken"):
            self.assertNotIn(secret, data)

    def test_excerpt_is_bounded_and_paths_drop_sensitive_queries(self):
        http, _ = diagnostic.split_trace(trace(500, "x"*100000, "/repos/a/b?token=secret"))
        self.assertEqual("/repos/a/b", http["path"])
        self.assertLessEqual(len(http["body_excerpt"]), diagnostic.LIMIT + 20)

    def test_later_failure_cannot_overwrite_earlier_evidence(self):
        for status in (502,403):
            error = subprocess.CalledProcessError(1, self.command, "", trace(status))
            with mock.patch.object(diagnostic.subprocess, "run", side_effect=error):
                with self.assertRaises(subprocess.CalledProcessError):
                    diagnostic.run_gh(self.command, check=True)
        self.assertEqual([502,403], [r["http"]["status"] for r in self.records()])

    def test_artifact_write_failure_does_not_replace_original_error(self):
        error = subprocess.CalledProcessError(7, self.command, "", trace())
        with mock.patch.object(diagnostic.subprocess, "run", side_effect=error), \
             mock.patch.object(Path, "mkdir", side_effect=PermissionError):
            with self.assertRaises(subprocess.CalledProcessError) as result:
                diagnostic.run_gh(self.command, check=True)
        self.assertEqual(7, result.exception.returncode)
        self.assertIn("Could not persist", self.logs.getvalue())

    def test_unknown_trace_format_is_not_dumped_as_request_data(self):
        http, ordinary = diagnostic.split_trace("* Request at now\nSECRET REQUEST CONTENT\n")
        self.assertEqual("", http["body_excerpt"])
        self.assertEqual("", ordinary)


    def test_redaction_precedes_body_truncation(self):
        prefix = "x" * (diagnostic.LIMIT - 4)
        http, _ = diagnostic.split_trace(trace(500, prefix + "env-github-secret"))
        self.assertNotIn("env-", http["body_excerpt"])

    def test_json_escaped_credentials_are_fully_redacted(self):
        raw = json.dumps({"password": 'a"b,secret', "id_token": "ID_SECRET", "api_key": "KEY_SECRET"})
        output = diagnostic.redact(raw)
        for secret in ("secret", "ID_SECRET", "KEY_SECRET"):
            self.assertNotIn(secret, output)
        json.loads(output)

    def test_cli_signal_exit_uses_shell_convention(self):
        error = subprocess.CalledProcessError(-15, self.command, "", "terminated")
        with mock.patch.object(diagnostic, "run_gh", side_effect=error):
            self.assertEqual(143, diagnostic.main())


@unittest.skipUnless(os.environ.get("RUN_GH_HTTP_TESTS") == "1" and shutil.which("gh"),
                     "opt-in real gh loopback test")
class GitHubHTTPLoopbackTest(unittest.TestCase):
    def test_real_cli_failures_keep_http_evidence_without_reposting(self):
        requests = []
        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_POST(self):
                requests.append(json.loads(self.rfile.read(int(self.headers["Content-Length"]))))
                status = int(self.path.rsplit("/", 1)[1])
                body = b'{"id":1}' if status == 201 else b''
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("X-GitHub-Request-Id", "loopback-id")
                self.send_header("Retry-After", "12")
                self.end_headers()
                self.wfile.write(body)

        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as tmp:
                env = {"PATH": os.environ["PATH"], "GH_TOKEN": "fake-only",
                       "GH_ENTERPRISE_TOKEN": "fake-only", "GH_CONFIG_DIR": tmp,
                       "GH_NO_UPDATE_NOTIFIER": "1", "GH_PROMPT_DISABLED": "1",
                       "REVIEW_GITHUB_DIAGNOSTICS_DIR": tmp}
                helper = Path(__file__).with_name("review_github_api.py")
                for status in (201,422,502,503):
                    result = subprocess.run([
                        sys.executable, str(helper), "api",
                        f"http://127.0.0.1:{server.server_port}/statuses/{status}",
                        "-X", "POST", "-f", "state=pending",
                    ], env=env, capture_output=True, text=True, timeout=10)
                    self.assertEqual(0 if status == 201 else 1, result.returncode)
                    if status == 201:
                        self.assertEqual('{"id":1}', result.stdout)
                        self.assertEqual("", result.stderr)
                    else:
                        self.assertEqual("", result.stdout)
                        self.assertIn("unexpected end of JSON input", result.stderr)
                        self.assertIn(f"HTTP={status}", result.stderr)
                records = [json.loads(p.read_text()) for p in sorted(Path(tmp).glob("*.json"))]
                self.assertEqual([422,502,503], [r["http"]["status"] for r in records])
                self.assertTrue(all(r["http"]["headers"]["x-github-request-id"] == "loopback-id" for r in records))
                self.assertEqual([{"state":"pending"}]*4, requests)
                self.assertNotIn("fake-only", json.dumps(records))
        finally:
            server.shutdown()
            server.server_close()
            thread.join()


class WorkflowDiagnosticsWiringTest(unittest.TestCase):
    def test_every_job_installs_before_api_and_uploads_after_cleanup(self):
        workflows = Path(__file__).resolve().parents[1] / "workflows"
        for filename, jobs in (
            ("code-review-runner.yml", ["code-review"]),
            ("code-review-aggregate-status.yml", ["aggregate"]),
            ("code-review-sync-result.yml", ["accept-skill-review", "skip-on-comment"]),
        ):
            text = (workflows / filename).read_text()
            for job in jobs:
                block = text.split("\n  " + job + ":", 1)[1]
                block = re.split(r"\n  [\w-]+:", block, maxsplit=1)[0]
                self.assertLess(block.index("Install GitHub API diagnostics"),
                                block.index('python3 "$REVIEW_GH_HELPER"'))
                upload = block.split("      - name: Preserve GitHub API failure diagnostics",1)[1]
                self.assertIn("if: always()", upload)
                self.assertIn("continue-on-error: true", upload)
                self.assertIn("review-github-diagnostics/*.json", upload)
                self.assertNotIn("      - name:", upload)
                self.assertNotIn("gh api ", block)
        aggregate = (workflows / "code-review-aggregate-status.yml").read_text()
        # Two reusable aggregate jobs can run for current and previous heads.
        self.assertIn("github-api-errors-aggregate-${{ inputs.head_sha }}-", aggregate)


if __name__ == "__main__":
    unittest.main()
