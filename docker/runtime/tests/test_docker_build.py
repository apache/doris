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

"""Exercise release verification and build handoff without network or Docker."""

import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import tarfile
import tempfile
import unittest

RUNTIME = Path(__file__).resolve().parents[1]
ROOT = RUNTIME.parents[1]

FAKE_DOCKER = """#!/usr/bin/env python3
import json
import os
from pathlib import Path
import sys

args = sys.argv[1:]
entry = {"args": args}
if args[0] == "build" and "DORIS_VERSION=" in " ".join(args):
    context = Path(args[-1])
    entry["files"] = sorted(str(p.relative_to(context)) for p in context.rglob("*") if p.is_file())
with open(os.environ["DOCKER_CALLS"], "a") as log:
    log.write(json.dumps(entry) + "\\n")
if args[:2] == ["image", "inspect"]:
    print('[{"Id": "sha256:fixture"}]')
"""

FAKE_CURL = """#!/usr/bin/env python3
import os
from pathlib import Path
import sys

args = sys.argv[1:]
Path(args[args.index("-o") + 1]).write_text(os.environ["CHECKSUM_DOCUMENT"])
"""


class BuildFixture:
    def setUp(self):
        (ROOT / "output").mkdir(exist_ok=True)
        self.tmp = tempfile.TemporaryDirectory(dir=ROOT / "output", prefix="runtime-build-test-")
        self.work = Path(self.tmp.name)
        self.bin = self.work / "bin"
        self.bin.mkdir()
        for name, content in [
            ("docker", FAKE_DOCKER),
            ("curl", FAKE_CURL),
            ("wget", "#!/usr/bin/env python3\nraise SystemExit('Unexpected unverified downloader')\n"),
        ]:
            path = self.bin / name
            path.write_text(content)
            path.chmod(0o755)
        self.calls = self.work / "docker-calls.jsonl"

    def tearDown(self):
        self.tmp.cleanup()

    def read_calls(self):
        return [json.loads(line) for line in self.calls.read_text().splitlines()]


class RuntimeBuildTest(BuildFixture, unittest.TestCase):
    def archive(self, version="4.1.5", arch="arm64", include_ms=True):
        path = self.work / "release.tar.gz"
        with tarfile.open(path, "w:gz") as archive:
            for role in (["fe", "be", "ms"] if include_ms else ["fe", "be"]):
                data = role.encode()
                info = tarfile.TarInfo(f"apache-doris-{version}-bin-{arch}/{role}/payload")
                info.size = len(data)
                archive.addfile(info, io.BytesIO(data))
        return path, hashlib.sha512(path.read_bytes()).hexdigest()

    def build(self, archive, digest, version="4.1.5", **overrides):
        env = dict(os.environ)
        env.update(
            PATH=f"{self.bin}:{env['PATH']}",
            DOCKER_CALLS=str(self.calls),
            DORIS_ARCHIVE=str(archive),
            DORIS_ARCHIVE_SHA512=digest,
            DORIS_ARCH="arm64",
            DORIS_BASE_IMAGE="runtime-test:base",
            DORIS_IMAGE_PREFIX="runtime-test",
            DORIS_IMAGE_WORK_DIR=str(self.work / "build"),
        )
        env.update(overrides)
        return subprocess.run(
            ["bash", str(RUNTIME / "docker-build.sh"), "-v", version],
            cwd=self.work, env=env, text=True, capture_output=True, check=False, timeout=30,
        )

    def test_verified_archive_builds_every_role_from_the_new_base(self):
        archive, digest = self.archive()
        result = self.build(archive, digest.upper())
        self.assertEqual(result.returncode, 0, result.stderr)
        builds = [call for call in self.read_calls() if call["args"][0] == "build"]
        self.assertEqual(len(builds), 4)
        self.assertIn("--pull", builds[0]["args"])
        self.assertIn("JAVA_VERSION=17", builds[0]["args"])
        for role, call in zip(["fe", "be", "ms"], builds[1:]):
            self.assertIn("BASE_IMAGE=runtime-test:base", call["args"])
            self.assertNotIn("--pull", call["args"])
            self.assertIn("--no-cache", call["args"])
            self.assertIn(f"resource/arm64/apache-doris-4.1.5-bin-arm64/{role}/payload", call["files"])
            record = self.work / "build" / f"{role}-4.1.5-arm64.image.json"
            self.assertEqual(json.loads(record.read_text())[0]["Id"], "sha256:fixture")
        self.assertFalse(list((self.work / "build").glob("staging.*")))

    def test_checksum_mismatch_stops_before_extracting_or_building(self):
        archive, _ = self.archive()
        result = self.build(archive, "0" * 128)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("SHA512 verification failed", result.stderr)
        self.assertFalse(self.calls.exists())
        self.assertFalse(list((self.work / "build").glob("staging.*")))

    def test_legacy_release_uses_java8_and_does_not_require_ms(self):
        archive, digest = self.archive("2.1.9", "x64-noavx2", include_ms=False)
        result = self.build(archive, digest, version="2.1.9", DORIS_ARCH="amd64", DORIS_NO_AVX2="1")
        self.assertEqual(result.returncode, 0, result.stderr)
        builds = [call for call in self.read_calls() if call["args"][0] == "build"]
        self.assertEqual(len(builds), 3)
        self.assertIn("JAVA_VERSION=8", builds[0]["args"])
        self.assertIn("linux/amd64", builds[0]["args"])
        self.assertIn("resource/amd64/apache-doris-2.1.9-bin-x64-noavx2/be/payload", builds[2]["files"])

    def test_checksum_documents_accept_gnu_and_bsd_formats(self):
        archive, digest = self.archive()
        for document in [f"{digest}  archive.tar.gz\n", f"SHA512 (archive.tar.gz) = {digest}\n"]:
            with self.subTest(document=document[:20]):
                result = self.build(archive, "", CHECKSUM_DOCUMENT=document)
                self.assertEqual(result.returncode, 0, result.stderr)

    def test_ambiguous_or_malformed_checksum_document_is_rejected(self):
        archive, digest = self.archive()
        for document in ["not a checksum", f"{digest}\n{digest}\n"]:
            with self.subTest(document=document[:20]):
                result = self.build(archive, "", CHECKSUM_DOCUMENT=document)
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(self.calls.exists())

    def test_invalid_version_is_rejected_before_building(self):
        archive, digest = self.archive()
        result = self.build(archive, digest, version="../4.1.5")
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.calls.exists())


class AllInOneBuildTest(BuildFixture, unittest.TestCase):
    def build(self, source, **overrides):
        env = dict(os.environ)
        env.update(PATH=f"{self.bin}:{env['PATH']}", DOCKER_CALLS=str(self.calls))
        env.update(overrides)
        command = ["bash", str(RUNTIME / "all-in-one/4.1/build.sh"),
                   "-v", "4.1.5", "--platform", "linux/arm64", "--source", source]
        if source != "image":
            payload = self.work / "payload"
            for role in ["fe", "be", "ms"]:
                (payload / role).mkdir(parents=True, exist_ok=True)
            if source == "local":
                command += ["--local-output", str(payload.relative_to(ROOT))]
            else:
                command += ["--tarball-dir", str(payload)]
        return subprocess.run(command, cwd=self.work, env=env, text=True,
                              capture_output=True, check=False, timeout=30)

    def test_image_mode_forwards_pinned_inputs_for_both_flavors(self):
        inputs = {key: f"registry.example/{key.lower()}@sha256:" + "a" * 64
                  for key in ["BASE_IMAGE", "FE_IMAGE", "BE_IMAGE", "MS_IMAGE"]}
        result = self.build("image", **inputs)
        self.assertEqual(result.returncode, 0, result.stderr)
        builds = [call["args"] for call in self.read_calls()
                  if call["args"][:2] == ["buildx", "build"]]
        self.assertEqual(len(builds), 2)
        for args in builds:
            self.assertIn("--pull", args)
            self.assertIn("ARTIFACT_SOURCE=image", args)
            for key, value in inputs.items():
                self.assertIn(f"{key}={value}", args)
        self.assertIn("FLAVOR=base", builds[0])
        self.assertIn("FLAVOR=full", builds[1])

    def test_directory_modes_keep_named_contexts(self):
        for source in ["local", "tarball"]:
            with self.subTest(source=source):
                self.calls.unlink(missing_ok=True)
                result = self.build(source)
                self.assertEqual(result.returncode, 0, result.stderr)
                builds = [call["args"] for call in self.read_calls()
                          if call["args"][:2] == ["buildx", "build"]]
                self.assertEqual(len(builds), 2)
                for args in builds:
                    self.assertIn("ARTIFACT_SOURCE=dir", args)
                    for role in ["fe", "be", "ms"]:
                        self.assertIn(f"doris-{role}={self.work}/payload/{role}", args)


if __name__ == "__main__":
    unittest.main()
