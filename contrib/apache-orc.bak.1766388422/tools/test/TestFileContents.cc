/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "orc/OrcFile.hh"

#include "Adaptor.hh"
#include "ToolTest.hh"

#include "wrap/gmock.h"
#include "wrap/gtest-wrapper.h"

TEST(TestFileContents, testRaw) {
  const std::string pgm = findProgram("tools/src/orc-contents");
  const std::string file = findExample("TestOrcFile.test1.orc");
  const std::string expected =
      "{\"boolean1\": false, \"byte1\": 1, \"short1\": 1024, \"int1\": 65536, "
      "\"long1\": 9223372036854775807, \"float1\": 1, \"double1\": -15,"
      " \"bytes1\": [0, 1, 2, 3, 4], \"string1\": \"hi\", \"middle\": "
      "{\"list\": [{\"int1\": 1, \"string1\": \"bye\"}, {\"int1\": 2, "
      "\"string1\": \"sigh\"}]}, \"list\": [{\"int1\": 3, \"string1\": "
      "\"good\"}, {\"int1\": 4, \"string1\": \"bad\"}], \"map\": []}\n"
      "{\"boolean1\": true, \"byte1\": 100, \"short1\": 2048, \"int1\": 65536,"
      " \"long1\": 9223372036854775807, \"float1\": 2, \"double1\": -5, "
      "\"bytes1\": [], \"string1\": \"bye\", \"middle\": {\"list\": "
      "[{\"int1\": 1, \"string1\": \"bye\"}, {\"int1\": 2, \"string1\":"
      " \"sigh\"}]}, \"list\": [{\"int1\": 100000000, \"string1\": \"cat\"},"
      " {\"int1\": -100000, \"string1\": \"in\"}, {\"int1\": 1234, "
      "\"string1\": \"hat\"}], \"map\": [{\"key\": \"chani\", \"value\": "
      "{\"int1\": 5, \"string1\": \"chani\"}}, {\"key\": \"mauddib\", "
      "\"value\": {\"int1\": 1, \"string1\": \"mauddib\"}}]}\n";

  std::string output;
  std::string error;

  EXPECT_EQ(0, runProgram({pgm, file}, output, error));
  EXPECT_EQ(expected, output);
  EXPECT_EQ("", error);
}

TEST(TestFileContents, testSelectedColumns) {
  const std::string pgm = findProgram("tools/src/orc-contents");
  const std::string file = findExample("TestOrcFile.test1.orc");
  const std::string columnFields = "1,3,5,7";
  const std::string columnTypeIds = "2,4,6,8";
  const std::string columnNames = "byte1,int1,float1,bytes1";
  const std::string expected =
      "{\"byte1\": 1, \"int1\": 65536, \"float1\": 1, \"bytes1\": [0, 1, 2, 3, 4]}\n"
      "{\"byte1\": 100, \"int1\": 65536, \"float1\": 2, \"bytes1\": []}\n";

  std::string output;
  std::string error;

  EXPECT_EQ(0, runProgram({pgm, "--columns=" + columnFields, file}, output, error));
  EXPECT_EQ(expected, output);
  EXPECT_EQ("", error);
  EXPECT_EQ(0, runProgram({pgm, "--columns", columnFields, file}, output, error));
  EXPECT_EQ(expected, output);
  EXPECT_EQ("", error);
  EXPECT_EQ(0, runProgram({pgm, "-c", columnFields, file}, output, error));
  EXPECT_EQ(expected, output);
  EXPECT_EQ("", error);

  EXPECT_EQ(0, runProgram({pgm, "--columnTypeIds=" + columnTypeIds, file}, output, error));
  EXPECT_EQ(expected, output);
  EXPECT_EQ("", error);
  EXPECT_EQ(0, runProgram({pgm, "--columnTypeIds", columnTypeIds, file}, output, error));
  EXPECT_EQ(expected, output);
  EXPECT_EQ("", error);
  EXPECT_EQ(0, runProgram({pgm, "-t", columnTypeIds, file}, output, error));
  EXPECT_EQ(expected, output);
  EXPECT_EQ("", error);

  EXPECT_EQ(0, runProgram({pgm, "--columnNames=" + columnNames, file}, output, error));
  EXPECT_EQ(expected, output);
  EXPECT_EQ("", error);
  EXPECT_EQ(0, runProgram({pgm, "--columnNames", columnNames, file}, output, error));
  EXPECT_EQ(expected, output);
  EXPECT_EQ("", error);
  EXPECT_EQ(0, runProgram({pgm, "-n", columnNames, file}, output, error));
  EXPECT_EQ(expected, output);
  EXPECT_EQ("", error);
}

TEST(TestFileContents, testNestedColumns) {
  const std::string pgm = findProgram("tools/src/orc-contents");
  const std::string file = findExample("complextypes_iceberg.orc");
  const std::string columnTypeIds = "1,15,16";
  const std::string columnNames = "id,nested_struct.a,nested_struct.b";
  const std::string expected = "{\"id\": 8, \"nested_struct\": {\"a\": -1, \"b\": [-1]}}\n";

  std::string output;
  std::string error;

  EXPECT_EQ(0, runProgram({pgm, "--columnTypeIds=" + columnTypeIds, file}, output, error));
  EXPECT_EQ(expected, output);
  EXPECT_EQ("", error);
  EXPECT_EQ(0, runProgram({pgm, "--columnTypeIds", columnTypeIds, file}, output, error));
  EXPECT_EQ(expected, output);
  EXPECT_EQ("", error);
  EXPECT_EQ(0, runProgram({pgm, "-t", columnTypeIds, file}, output, error));
  EXPECT_EQ(expected, output);
  EXPECT_EQ("", error);

  EXPECT_EQ(0, runProgram({pgm, "--columnNames=" + columnNames, file}, output, error));
  EXPECT_EQ(expected, output);
  EXPECT_EQ("", error);
  EXPECT_EQ(0, runProgram({pgm, "--columnNames", columnNames, file}, output, error));
  EXPECT_EQ(expected, output);
  EXPECT_EQ("", error);
  EXPECT_EQ(0, runProgram({pgm, "-n", columnNames, file}, output, error));
  EXPECT_EQ(expected, output);
  EXPECT_EQ("", error);
}

TEST(TestFileContents, testInvalidName) {
  const std::string pgm = findProgram("tools/src/orc-contents");
  const std::string file = findExample("TestOrcFile.test1.orc");
  const std::string error_msg =
      "Invalid column selected abc. Valid names are boolean1, byte1, bytes1, double1, "
      "float1, int1, list, list.int1, list.string1, long1, map, map.int1, map.string1, "
      "middle, middle.list, middle.list.int1, middle.list.string1, short1, string1";

  std::string output;
  std::string error;
  EXPECT_EQ(1, runProgram({pgm, "-n", "byte1,abc", file}, output, error));
  EXPECT_EQ("", output);
  EXPECT_NE(std::string::npos, error.find(error_msg));
}

TEST(TestFileContents, testDecimal64V2) {
  const std::string pgm = findProgram("tools/src/orc-contents");
  const std::string file = findExample("decimal64_v2.orc");
  const std::string expected =
      "{\"a\": 17292380420, \"b\": 24, \"c\": 36164.16, \"d\": 0.03, \"e\": 0.01}\n"
      "{\"a\": 17292380421, \"b\": 38, \"c\": 63351.70, \"d\": 0.08, \"e\": 0.01}\n"
      "{\"a\": 17292380421, \"b\": 28, \"c\": 42673.96, \"d\": 0.09, \"e\": 0.06}\n"
      "{\"a\": 17292380421, \"b\": 40, \"c\": 76677.60, \"d\": 0.05, \"e\": 0.04}\n"
      "{\"a\": 17292380421, \"b\": 2, \"c\": 2096.48, \"d\": 0.07, \"e\": 0.07}\n"
      "{\"a\": 17292380421, \"b\": 42, \"c\": 45284.82, \"d\": 0.07, \"e\": 0.05}\n"
      "{\"a\": 17292380421, \"b\": 10, \"c\": 18572.90, \"d\": 0.01, \"e\": 0.08}\n"
      "{\"a\": 17292380422, \"b\": 12, \"c\": 14836.80, \"d\": 0.09, \"e\": 0.06}\n"
      "{\"a\": 17292380422, \"b\": 41, \"c\": 82152.52, \"d\": 0.07, \"e\": 0.02}\n"
      "{\"a\": 17292380422, \"b\": 38, \"c\": 47240.84, \"d\": 0.10, \"e\": 0.00}\n";
  const std::string error_msg =
      "Warning: ORC file " + file + " was written in an unknown format version UNSTABLE-PRE-2.0\n";

  std::string output;
  std::string error;

  EXPECT_EQ(0, runProgram({pgm, file}, output, error)) << error;
  EXPECT_EQ(expected, output);
  EXPECT_EQ(error_msg, error);
}
