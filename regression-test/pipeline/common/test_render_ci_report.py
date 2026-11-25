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

import contextlib
import gzip
from html import unescape
import io
import json
from email.message import Message
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase, main
from unittest.mock import patch
import subprocess

import render_ci_report as ci


TEST_ORIGIN = "https://ci.example.invalid"


class ReportTest(TestCase):
    def setUp(self):
        temp = TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        env = patch.dict(ci.os.environ, {"TEAMCITY_CURRENT_URL": TEST_ORIGIN})
        env.start()
        self.addCleanup(env.stop)
        self.root = Path(temp.name)
        self.key = "builds/agent-test/3817/index.html"
        self.output = self.root / self.key

    def generate(self, *args):
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            ci.main([*args, "--build-id", "3817", "--output-dir", str(self.root)])

    def test_offline_for_each_pipeline(self):
        with patch.object(ci.subprocess, "run", side_effect=AssertionError("unexpected network client")):
            for pipeline, label in ci.PIPELINES.items():
                self.generate(pipeline, "--status", "SUCCESS")
                page = self.output.read_text()
                self.assertIn(f"<h1>{label}</h1>", page)
                self.assertIn(">Passed</strong>", page)
                if pipeline != "compile":
                    self.assertIn("Case counts were not recorded", page)
                self.assertNotIn("0 failed", page)

    def test_counts_and_revision(self):
        self.generate("p0", "--status", "failure", "--unmuted-failed", "80", "--passed", "0",
                      "--pr", "70580", "--sha", "a"*40, "--tested-sha", "b"*40)
        page = self.output.read_text()
        for field in ("Unmuted failures</dt><dd>80", "Passed</dt><dd>0", "a"*40, "b"*40, "#70580"):
            self.assertIn(field, page)
        self.assertNotIn("Ignored", page)

    def test_invalid_or_untrusted_values(self):
        for args in (("--sha", "<svg>"), ("--status", "running"), ("--pr", "0"), ("--failed", "-2"),
                     ("--failed-step", "binary"), ("--tested-sha", "https://example.invalid")):
            with self.subTest(args=args), self.assertRaises(SystemExit):
                self.generate("compile", "--status", "success", *args)
        self.assertFalse(self.output.exists())

    def test_failed_step_log_excludes_scripts_and_passing_steps(self):
        log = self.root / "build.log"
        log.write_text("Build 'Example' #1\n"
                       "embedded_script_should_not_appear\n"
                       "[10:59:59] : Step 1/3: 1. 环境预检 (Command Line)\n"
                       "[11:00:00] : [Step 1/3] AssertionError private-user@example.com\n"
                       "[11:00:01] : [Step 1/3] Process exited with code 0\n"
                       "[11:00:01]E: Step 2/3: 2. 包装 (Command Line)\n"
                       "[11:00:02] : [Step 2/3] OSError: [Errno 7] Argument list too long: 'java'\n"
                       "[11:00:03]E: [Step 2/3] Process exited with code 1\n"
                       "[11:00:04] : [Step 3/3] Bearer test_secret_123 https://10.0.0.5/private\n")
        self.generate("compile", "--status", "failure", "--failure-log", str(log))
        html = self.output.read_text()
        self.assertIn("OSError: [Errno 7] Argument list too long: 'java'", unescape(html))
        self.assertNotIn("embedded_script_should_not_appear", html)
        for private in ("private-user@", "test_secret_123", "10.0.0.5", "A test assertion failed"):
            self.assertNotIn(private, html)

    def test_plain_failure_log_preserves_text_and_masks_authorization(self):
        log = self.root / "compile.log"
        log.write_text("starting\nERROR: Authorization=very-private-message\n")
        self.generate("compile", "--status", "failure", "--failure-log", str(log))
        self.assertIn("starting\nERROR: Authorization=[redacted]\n", self.output.read_text())
        self.assertNotIn("very-private-message", self.output.read_text())

    def test_junit_keeps_name_type_message_trace_and_escapes_markup(self):
        xml = self.root / "junit.xml"
        xml.write_text('<testsuite><testcase name="ok"/><testcase classname="org.test.PlannerTest" '
                       'name="testFilter[case1]"><failure type="AssertionError" message="wrong row">'
                       'expected: &lt;3&gt; but was: &lt;2&gt;\n  at testFilter:18\napi_token=very-private</failure>'
                       '<system-out>unrelated-output</system-out></testcase>'
                       '<testcase name="&lt;script&gt;"><error>new exception</error></testcase></testsuite>')
        self.generate("feut", "--status", "failure", "--junit", str(xml))
        page = self.output.read_text()
        for field in ("testFilter[case1]", "org.test.PlannerTest", "new exception", "&lt;script&gt;",
                      "3 test executions · 2 failed · 1 passed · 0 skipped",
                      "AssertionError\nwrong row\nexpected: &lt;3&gt; but was: &lt;2&gt;\n  at testFilter:18"):
            self.assertIn(field, page)
        for secret in ("very-private", "unrelated-output", "<script>"):
            self.assertNotIn(secret, page)

    def test_junit_rejects_invalid_input_before_upload(self):
        xml = self.root / "junit.xml"
        for invalid in ("<testsuite>", '<!DOCTYPE x [<!ENTITY t "private">]><testsuite/>', "<html/>"):
            xml.write_text(invalid)
            with self.assertRaises(SystemExit), patch.object(ci, "upload", side_effect=AssertionError):
                self.generate("p0", "--status", "failure", "--junit", str(xml), "--upload")
        self.assertFalse(self.output.exists())

    def test_junit_shards_and_summary_only(self):
        one, two = self.root / "1.xml", self.root / "2.xml"
        one.write_text('<testsuite><testcase/><testcase><skipped/></testcase></testsuite>')
        two.write_text('<testsuites><testsuite tests="5" failures="2" errors="1" skipped="1"/></testsuites>')
        self.generate("p0", "--status", "failure", "--unmuted-failed", "80", "--junit", str(one), "--junit", str(two))
        page = self.output.read_text()
        self.assertIn("Unmuted failures</dt><dd>80", page)
        self.assertIn("7 test executions · 3 failed · 2 passed · 2 skipped", page)
        self.assertIn("without individual case details", page)
        with self.assertRaises(SystemExit):
            self.generate("p0", "--status", "failure", "--junit", str(one), "--junit", str(one))

    def test_compile_error_and_timeline(self):
        log = self.root / "build.log"
        log.write_text("Build 'Community' #1\n"
                       "[08:00:00] : Step 1/2: 1. 环境预检 (Command Line) (5s)\n"
                       "[08:00:05] : [Step 1/2] Process exited with code 0\n"
                       "[08:00:05]E: Step 2/2: 2. Docker 编译 binary (Command Line) (2s)\n"
                       "[08:00:06] : [Step 2/2] /private/be/a.cpp:42:3: error: use of undeclared identifier 'secret_code'\n"
                       "[08:00:07]E: [Step 2/2] Process exited with code 1\n")
        self.generate("compile", "--status", "failure", "--failure-log", str(log),
                      "--started-at", "2026-11-24T00:00:00Z", "--finished-at", "2026-11-24T08:00:07+08:00")
        page = self.output.read_text()
        self.assertIn("2. Docker 编译 binary</th><td>Failed</td><td>2s", page)
        self.assertIn("error: use of undeclared identifier", page)
        self.assertIn("build log L5", page)
        self.assertIn("0m 07s", page)
        self.assertIn("error: use of undeclared identifier 'secret_code'", unescape(page))

    def test_partial_junit_preserves_declared_totals_and_shows_one_row(self):
        xml = self.root / "partial.xml"
        xml.write_text('<testsuites tests="100" failures="5" errors="0" skipped="2"><testsuite>'
                       '<testcase><failure type="AssertionError">expected: &lt;5&gt; but was: &lt;4&gt;</failure></testcase>'
                       '</testsuite></testsuites>')
        self.generate("p0", "--status", "failure", "--junit", str(xml))
        page = self.output.read_text()
        self.assertIn("100 test executions · 5 failed · 93 passed · 2 skipped", page)
        self.assertIn("1 of 5 failures", page)
        self.assertIn("expected: <5> but was: <4>", unescape(page))

    def test_native_problems_and_occurrences_are_verbatim_and_bound_to_build(self):
        path = self.root / "build.json"
        error = "unrecognized error <expression>\n  expected:  x\nactual: y"
        problem = "Process exited with code 1 (Step: 编译)"
        record = {"id":3817, "state":"finished", "buildTypeId":"Doris_P0", "status":"FAILURE",
                  "statusText":"Tests failed: 1, muted: 1", "ignoredField":"private_env_dump",
                  "problemOccurrences":{"problemOccurrence":[{"details":problem}]},
                  "testOccurrences":{"testCounters":{"failed":1,"muted":1},
                   "testOccurrence":[{"name":"suite.Case[A]","status":"FAILURE","details":error},
                                     {"name":"suite.Case[A]","status":"FAILURE","muted":True,"details":"different trace"}]}}
        path.write_text(json.dumps(record))
        self.generate("p0", "--status", "failure", "--teamcity", str(path))
        page = self.output.read_text()
        self.assertIn(error, unescape(page))
        self.assertIn(problem, page)
        self.assertEqual(page.count("suite.Case[A]"), 2)
        self.assertIn("different trace", page)
        self.assertIn("<small>Muted</small>", page)
        self.assertNotIn("private_env_dump", page)
        self.assertNotIn("<expression>", page)
        for field,value in (("id",24), ("buildTypeId","Doris_FEUT"), ("state","running"),("status","SUCCESS"),
                            ("testOccurrences",{"nextHref":"/another-page"})):
            path.write_text(json.dumps(dict(record, **{field:value})))
            with self.assertRaises(SystemExit):
                self.generate("p0", "--status", "failure", "--teamcity", str(path))

    def test_redaction_preserves_sql_layout(self):
        text = """  "s3.access_key" = "private-value",\n  's3.secret_key'='private-secret'\n  at 10.0.0.4"""
        masked = ci.redact(text)
        self.assertEqual(masked, text.replace("private-value","[redacted]").replace("private-secret","[redacted]").replace("10.0.0.4","[private address]"))
        self.assertEqual(ci.redact(masked), masked)

    def test_upload_contract(self):
        def put(argv, **options):
            self.assertEqual(argv[0:3], ["ossutil", "api", "put-object"])
            for flag, value in (("--mode","EcsRamRole"), ("--bucket","doris-pipelines"),
                                ("--endpoint","https://oss-cn-hongkong-internal.aliyuncs.com"),
                                ("--key", self.key), ("--content-type","text/html; charset=utf-8"),
                                ("--content-disposition","inline")):
                self.assertEqual(argv[argv.index(flag)+1], value)
            self.assertIn("--forbid-overwrite", argv)
            body_file = Path(argv[argv.index("--body")+1].removeprefix("file://"))
            self.assertIn(b"<h1>Compile</h1>", body_file.read_bytes())
            return subprocess.CompletedProcess(argv, 0, b"", b"")
        with patch.object(ci.subprocess, "run", side_effect=put) as client:
            self.generate("compile", "--status", "failure", "--failed-step", "checkout", "--upload")
            self.assertEqual(client.call_count, 1)

    def test_retry_requires_identical_bytes(self):
        self.generate("p0", "--status", "success")
        url = f"https://{ci.BUCKET}.{ci.ENDPOINT}/{self.key}"
        for same in (True, False):
            response = io.BytesIO(self.output.read_bytes() if same else b"other report")
            response.geturl = lambda: url
            response.status = 200
            response.headers = Message()
            response.headers["Content-Type"] = "text/html; charset=utf-8"
            result = subprocess.CompletedProcess([], 1, b"", b"409 FileAlreadyExists")
            with patch.object(ci.subprocess, "run", return_value=result), patch.object(ci, "urlopen", return_value=response):
                if same:
                    self.generate("p0", "--status", "success", "--upload")
                else:
                    with self.assertRaises(SystemExit):
                        self.generate("p0", "--status", "success", "--upload")

    def test_status_is_not_inferred_from_counts(self):
        for status in ("failure", "error", "canceled"):
            self.generate("p0", "--status", status, "--failed", "0")
            self.assertIn(f">{ci.RESULTS[status]}</strong>", self.output.read_text())

    def receipt(self, *objects, build="3817", pipeline="Doris_P0"):
        path = self.root / "oss-artifacts.json"
        path.write_text(json.dumps(dict(buildId=build, pipeline=pipeline, state="EXPORTED", objects=objects)))
        return str(path)

    def obj(self, group, name, **attrs):
        expires = 2000000000
        return dict(group=group, name=name, expires=expires,
                    url=f"{TEST_ORIGIN}/_ci/diagnostics/d/{'a'*32}?expires={expires}&signature={'a'*64}", **attrs)

    def test_downloads_and_core_frames(self):
        log = self.root / "full.log"
        log.write_text("line one\naccess_key=classified\n-----BEGIN PRIVATE KEY-----\n"
                       "private_payload\n-----END PRIVATE KEY-----\nlast line\n")
        backtrace = self.root / "bt.txt"
        backtrace.write_text("Program terminated with signal SIGSEGV\n#0  f<Mutex> at be.cpp:42\n"
                             "#1  main () at main.cpp:8\nlocals and unrelated output\n")
        args = ["p0", "--status", "failure", "--public-build-log", str(log), "--core-backtrace", str(backtrace),
                "--artifact-index", self.receipt(self.obj("cluster", "cluster-logs.safe-v1.tar.gz"),
                                                 self.obj("large-file", "core.doris_be.123.gz"),
                                                 self.obj("large-file", "doris_be.profraw"),
                                                 self.obj("core", "doris_be"))]
        uploaded = []
        def put(argv, **kwargs):
            key = argv[argv.index("--key")+1]
            uploaded.append((key,argv[argv.index("--content-type")+1],argv[argv.index("--content-disposition")+1]))
            return subprocess.CompletedProcess(argv, 0, b"", b"")
        with patch.object(ci.subprocess, "run", side_effect=put):
            self.generate(*args, "--upload")
        page = self.output.read_text()
        self.assertEqual(len(uploaded), 2)
        attachment = self.output.parent / Path(uploaded[0][0]).name
        self.assertEqual(uploaded[0][1:], ("application/gzip", "attachment"))
        self.assertEqual(uploaded[1][0], self.key)
        self.assertEqual(gzip.decompress(attachment.read_bytes()).decode(),
                         "line one\naccess_key=[redacted]\n[redacted]\nlast line\n")
        self.assertIn(f'href="{attachment.name}"', page)
        first = attachment.read_bytes()
        self.generate(*args)
        self.assertEqual(self.output.read_text(), page)
        self.assertEqual(attachment.read_bytes(), first)
        for expected in ("cluster-logs.safe-v1.tar.gz", "core.doris_be.123.gz", "Core backtrace",
                         "signal SIGSEGV", "#0  f&lt;Mutex&gt; at be.cpp:42", "Valid until", "data-expires="):
            self.assertIn(expected, page)
        for excluded in ("locals and unrelated", "doris_be.profraw", ">doris_be</a>"):
            self.assertNotIn(excluded, page)

    def test_native_core_pairs_and_archive_members(self):
        core = self.root / "core-diagnostics.json"
        core.write_text(json.dumps(dict(build="3817", core_pairs=[], errors=[])))
        self.generate("p0", "--status", "failure", "--core-diagnostics", str(core))
        self.assertIn("No core recorded", self.output.read_text())
        core.write_text(json.dumps(dict(buildId="3817", pipeline="Doris_P0", pairs=[{"backtrace":"#0  foo\n#1  bar"}])))
        archive = self.obj("cluster", "cluster-logs.tar.gz", archive=True, sources=["log/be/core.1473"])
        self.generate("p0", "--status", "failure", "--artifact-index", self.receipt(archive),
                      "--core-diagnostics", str(core))
        page = self.output.read_text()
        self.assertEqual(page.count(">cluster-logs.tar.gz</a>"), 2)
        self.assertIn("#0  foo\n#1  bar", page)
        for name in ("core-diagnostics.json", "core.1473.backtrace.txt", "core.cpp", "unrelated.profraw"):
            archive["sources"] = ["be/" + name]
            self.assertEqual(ci.download_kinds(archive), ["cluster"])
        core.write_text(json.dumps(dict(buildId=4000, pairs=[])))
        with self.assertRaises(SystemExit):
            self.generate("p0", "--status", "failure", "--core-diagnostics", str(core))

    def test_untrusted_or_mismatched_download_receipts_stop_publication(self):
        original = self.obj("large-file", "core.1234")
        url = original["url"]
        invalid = [url.replace("https://", "http://"), url.replace("ci.example.invalid", "user@ci.example.invalid"),
                   url.replace("/_ci/", "/else/"), url + "#fragment", url + "&next=else", url.replace("expires=", "bogus="),
                   url.replace("2000000000", "2000000001")]
        for modified in invalid:
            with self.subTest(url=modified), self.assertRaises(SystemExit), patch.object(ci, "upload", side_effect=AssertionError):
                self.generate("p0", "--status", "failure", "--artifact-index", self.receipt(dict(original,url=modified)), "--upload")
        for build, pipeline in (("4000", "Doris_P0"), ("3817", "Doris_FEUT")):
            with self.assertRaises(SystemExit):
                self.generate("p0", "--status", "failure", "--artifact-index", self.receipt(original,build=build,pipeline=pipeline))
        self.assertFalse(self.output.exists())

    def test_attachment_upload_failure_does_not_publish_html(self):
        path = self.root / "full.log"
        path.write_text("test\n")
        with patch.object(ci, "upload", side_effect=ValueError("upload failed")) as client, self.assertRaises(SystemExit):
            self.generate("p0", "--status", "failure", "--public-build-log", str(path), "--upload")
        self.assertEqual(client.call_count, 1)
        self.assertTrue(client.call_args.args[1].endswith(".gz"))
        self.assertFalse(self.output.exists())

    def test_binary_core_never_read_as_text(self):
        path = self.root / "core"
        path.write_bytes(b"\x7fELF\x00private bytes")
        with self.assertRaises(SystemExit):
            self.generate("p0", "--status", "failure", "--core-backtrace", str(path))

    def test_local_diagnostics_keep_bytes_and_require_matching_file_types(self):
        archive = self.root / "cluster-logs.tar.gz"
        archive.write_bytes(gzip.compress(b"archive test payload"))
        core = self.root / "doris_be.core"
        core.write_bytes(b"\x7fELF\x02\x01\x01" + bytes(9) + b"\x04\x00" + bytes(160))
        with patch.object(ci.subprocess, "run", return_value=subprocess.CompletedProcess([], 0)) as client:
            self.generate("p0", "--status", "failure", "--public-doris-log", str(archive), "--public-core", str(core), "--upload")
        page = self.output.read_text()
        for source, prefix, mime, call in zip((archive, core), ("doris-logs-", "coredump-"),
                                              ("application/gzip", "application/octet-stream"), client.call_args_list):
            argv = call.args[0]
            dest = self.output.with_name(Path(argv[argv.index("--key") + 1]).name)
            self.assertTrue(dest.name.startswith(prefix))
            self.assertIn(f'href="{dest.name}"', page)
            self.assertEqual(dest.read_bytes(), source.read_bytes())
            self.assertEqual(argv[argv.index("--content-type")+1], mime)
        self.assertEqual(client.call_count, 3)
        self.assertIn("No debugger backtrace available", page)
        for option, wrong in (("--public-core", archive), ("--public-doris-log", core)):
            with self.assertRaises(SystemExit), patch.object(ci, "upload", side_effect=AssertionError):
                self.generate("p0", "--status", "failure", option, str(wrong), "--upload")

    def test_configured_origin_and_build_copy(self):
        path = self.root / "copy.json"
        path.write_text(json.dumps(dict(id=3817, buildTypeId="Compile_Copy", state="finished", status="SUCCESS")))
        args = ["compile", "--status", "success", "--teamcity", str(path)]
        with self.assertRaises(SystemExit):
            self.generate(*args)
        self.generate(*args, "--build-type-id", "Compile_Copy")
        self.assertIn(TEST_ORIGIN + "/downloadBuildLog.html?", self.output.read_text())
        with patch.dict(ci.os.environ, {"TEAMCITY_CURRENT_URL": ""}):
            self.generate(*args, "--build-type-id", "Compile_Copy")
            self.assertNotIn("/downloadBuildLog", self.output.read_text())
            with self.assertRaises(SystemExit):
                self.generate("p0", "--status", "failure", "--artifact-index",
                              self.receipt(self.obj("large-file", "core.123")))
        with patch.dict(ci.os.environ, {"TEAMCITY_CURRENT_URL": "https://ci.example.invalid/path"}):
            with self.assertRaises(SystemExit):
                self.generate(*args, "--build-type-id", "Compile_Copy")

    def test_truncated_or_cross_build_occurrences(self):
        path = self.root / "build.json"
        for occurrences in ({"count": 2, "testOccurrence": [{"name":"one"}]},
                            {"testOccurrence": [{"build":{"id":4000}}]}):
            path.write_text(json.dumps(dict(id=3817, state="finished", buildTypeId="Doris_P0", status="FAILURE",
                                            testOccurrences=occurrences)))
            with self.assertRaises(SystemExit):
                self.generate("p0", "--status", "failure", "--teamcity", str(path))


if __name__ == "__main__":
    main()
