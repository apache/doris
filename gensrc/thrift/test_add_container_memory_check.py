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

import pathlib
import subprocess
import sys
import tempfile
import unittest


SCRIPT_DIR = pathlib.Path(__file__).resolve().parent
CHECKER = SCRIPT_DIR / "add_container_memory_check.py"


class AddContainerMemoryCheckTest(unittest.TestCase):
    def test_instruments_cpp_and_generated_vector_fields_idempotently(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            source = pathlib.Path(temp_dir) / "Sample_types.cpp"
            header = source.with_suffix(".h")
            source.write_text(
                '#include "Sample_types.h"\n'
                "uint32_t Sample::read(TProtocol* iprot) {\n"
                "  this->page_locations.resize(_size0);\n"
                "}\n"
            )
            header.write_text(
                '#include "thrift/TBase.h"\n'
                "class Sample {\n"
                " public:\n"
                "  std::vector<int64_t>  page_locations;\n"
                "  void __set_page_locations(const std::vector<int64_t> & val);\n"
                "};\n"
            )

            subprocess.run([sys.executable, str(CHECKER), str(source)], check=True)
            first_source = source.read_text()
            first_header = header.read_text()
            subprocess.run([sys.executable, str(CHECKER), str(source)], check=True)

            self.assertEqual(source.read_text(), first_source)
            self.assertEqual(header.read_text(), first_header)
            self.assertIn("reserve_thrift_container_memory", first_source)
            self.assertIn("ThriftMemoryTrackedVector<int64_t>  page_locations", first_header)
            self.assertIn(
                "__set_page_locations(const std::vector<int64_t> & val)", first_header
            )

    def test_checker_is_prerequisite_of_every_generated_reader(self):
        makefile = (SCRIPT_DIR / "Makefile").read_text()
        pattern_rule = (
            "${BUILD_DIR}/gen_cpp/%_types.cpp: ${CURDIR}/%.thrift "
            "${CONTAINER_MEMORY_CHECK} | ${BUILD_DIR}/gen_cpp"
        )
        self.assertIn(pattern_rule, makefile)
        self.assertNotIn("gen_cpp/parquet_types.cpp: ${CONTAINER_MEMORY_CHECK}", makefile)


if __name__ == "__main__":
    unittest.main()
