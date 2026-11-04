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

"""Run with Python 3; uses the repository's actual Thrift compiler."""

import os
import pathlib
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
            subprocess.run([str(thirdparty / "installed/bin/thrift"), "--gen", language,
                            "-out", str(destination), str(ROOT / "gensrc/thrift/FileEncryption.thrift")],
                           check=True, capture_output=True)
        cls.java = (cls.output / "java/org/apache/doris/thrift/TFileEncryptionInfo.java").read_text()
        cls.cpp = (cls.output / "cpp/FileEncryption_types.cpp").read_text()

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
        self.assertNotIn("to_string(key)", transformed)
        self.assertIn("TCompactProtocolT<TMemoryBuffer>", transformed)

    def test_generator_drift_fails_closed(self):
        with self.assertRaises(ValueError):
            redact.redact_java(self.java.replace("toString()", "renamed()"))
        with self.assertRaises(ValueError):
            redact.redact_java(self.java.replace("aad_prefix;", "renamed;"))
        with self.assertRaises(ValueError):
            redact.redact_cpp(self.cpp.replace("::write(", "::renamed("))


if __name__ == "__main__":
    unittest.main()
