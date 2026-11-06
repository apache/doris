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

"""Test the build-time redaction transform with the repository's Thrift compiler.

Check generated-code shape, idempotence and preservation of unrelated diagnostics.
The Java and C++ contract tests separately exercise runtime protocol round trips
and diagnostic output from the transformed code.
"""

import os
import pathlib
import re
import subprocess
import tempfile
import unittest

import redact_file_encryption as redact

ROOT = pathlib.Path(__file__).resolve().parents[2]


class RedactFileEncryptionTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temporary = tempfile.TemporaryDirectory()
        cls.output = pathlib.Path(cls.temporary.name)
        thirdparty = pathlib.Path(os.environ.get("DORIS_THIRDPARTY", ROOT / "thirdparty"))
        for language in ("java", "cpp"):
            destination = cls.output / language
            destination.mkdir()
            subprocess.run([str(thirdparty / "installed/bin/thrift"), "-r", "--gen", language,
                            "-I", str(ROOT / "gensrc/thrift"), "-I", str(ROOT / "gensrc/build/thrift"),
                            "-out", str(destination), str(ROOT / "gensrc/thrift/PlanNodes.thrift")],
                           check=True, capture_output=True)
        cls.java = (cls.output / "java/org/apache/doris/thrift/TFileEncryptionInfo.java").read_text()
        cls.cpp = (cls.output / "cpp/PlanNodes_types.cpp").read_text()

    @classmethod
    def tearDownClass(cls):
        cls.temporary.cleanup()

    def test_java_transform_is_idempotent(self):
        transformed = redact.redact_java(self.java)
        # CLI strips its single file-level marker before transforming again.
        again = redact.redact_java(transformed.replace("\n// " + redact.MARKER + "\n", ""))
        self.assertEqual(transformed, again)
        self.assertIn("public transient", transformed)
        self.assertNotIn("TBaseHelper.toString(this.key", transformed)

    def test_cpp_transform_is_idempotent(self):
        transformed = redact.redact_cpp(self.cpp)
        again = redact.redact_cpp(transformed.replace("\n// " + redact.MARKER + "\n", ""))
        self.assertEqual(transformed, again)
        diagnostic = re.search(r"void TFileEncryptionInfo::printTo\(std::ostream& out\) const \{.*?\n\}",
                               transformed, re.S).group()
        self.assertNotIn("to_string(key)", diagnostic)
        self.assertIn("TCompactProtocolT<TMemoryBuffer>", transformed)

    def test_generator_drift_fails_closed(self):
        with self.assertRaises(ValueError):
            redact.redact_java(self.java.replace("toString()", "renamed()"))
        with self.assertRaises(ValueError):
            redact.redact_java(self.java.replace("aad_prefix;", "renamed;"))
        with self.assertRaises(ValueError):
            redact.redact_cpp(self.cpp.replace("::write(", "::renamed("))

    def test_other_plan_descriptors_keep_their_diagnostics(self):
        transformed = redact.redact_cpp(self.cpp)
        signature = r"void TFileRangeDesc::printTo\(std::ostream& out\) const \{.*?\n\}"
        original = re.search(signature, self.cpp, re.S)
        self.assertIsNotNone(original)
        self.assertEqual(original.group(), re.search(signature, transformed, re.S).group())


if __name__ == "__main__":
    unittest.main()
