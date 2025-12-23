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

TEST(TestFileMetadata, testRaw) {
  const std::string pgm = findProgram("tools/src/orc-metadata");
  const std::string file = findExample("orc_split_elim.orc");
  const std::string expected = "Raw file tail: " + file +
                               "\n"
                               "postscript {\n"
                               "  footerLength: 288\n"
                               "  compression: NONE\n"
                               "  version: 0\n"
                               "  version: 12\n"
                               "  metadataLength: 526\n"
                               "  magic: \"ORC\"\n"
                               "}\n"
                               "footer {\n"
                               "  headerLength: 3\n"
                               "  contentLength: 245568\n"
                               "  stripes {\n"
                               "    offset: 3\n"
                               "    indexLength: 137\n"
                               "    dataLength: 45282\n"
                               "    footerLength: 149\n"
                               "    numberOfRows: 5000\n"
                               "  }\n"
                               "  stripes {\n"
                               "    offset: 45571\n"
                               "    indexLength: 137\n"
                               "    dataLength: 45282\n"
                               "    footerLength: 149\n"
                               "    numberOfRows: 5000\n"
                               "  }\n"
                               "  stripes {\n"
                               "    offset: 91139\n"
                               "    indexLength: 137\n"
                               "    dataLength: 45282\n"
                               "    footerLength: 149\n"
                               "    numberOfRows: 5000\n"
                               "  }\n"
                               "  stripes {\n"
                               "    offset: 136707\n"
                               "    indexLength: 138\n"
                               "    dataLength: 45283\n"
                               "    footerLength: 149\n"
                               "    numberOfRows: 5000\n"
                               "  }\n"
                               "  stripes {\n"
                               "    offset: 200000\n"
                               "    indexLength: 137\n"
                               "    dataLength: 45282\n"
                               "    footerLength: 149\n"
                               "    numberOfRows: 5000\n"
                               "  }\n"
                               "  types {\n"
                               "    kind: STRUCT\n"
                               "    subtypes: 1\n"
                               "    subtypes: 2\n"
                               "    subtypes: 3\n"
                               "    subtypes: 4\n"
                               "    subtypes: 5\n"
                               "    fieldNames: \"userid\"\n"
                               "    fieldNames: \"string1\"\n"
                               "    fieldNames: \"subtype\"\n"
                               "    fieldNames: \"decimal1\"\n"
                               "    fieldNames: \"ts\"\n"
                               "  }\n"
                               "  types {\n"
                               "    kind: LONG\n"
                               "  }\n"
                               "  types {\n"
                               "    kind: STRING\n"
                               "  }\n"
                               "  types {\n"
                               "    kind: DOUBLE\n"
                               "  }\n"
                               "  types {\n"
                               "    kind: DECIMAL\n"
                               "  }\n"
                               "  types {\n"
                               "    kind: TIMESTAMP\n"
                               "  }\n"
                               "  numberOfRows: 25000\n"
                               "  statistics {\n"
                               "    numberOfValues: 25000\n"
                               "  }\n"
                               "  statistics {\n"
                               "    numberOfValues: 25000\n"
                               "    intStatistics {\n"
                               "      minimum: 2\n"
                               "      maximum: 100\n"
                               "      sum: 2499619\n"
                               "    }\n"
                               "  }\n"
                               "  statistics {\n"
                               "    numberOfValues: 25000\n"
                               "    stringStatistics {\n"
                               "      minimum: \"bar\"\n"
                               "      maximum: \"zebra\"\n"
                               "      sum: 124990\n"
                               "    }\n"
                               "  }\n"
                               "  statistics {\n"
                               "    numberOfValues: 25000\n"
                               "    doubleStatistics {\n"
                               "      minimum: 0.8\n"
                               "      maximum: 80\n"
                               "      sum: 200051.40000000002\n"
                               "    }\n"
                               "  }\n"
                               "  statistics {\n"
                               "    numberOfValues: 25000\n"
                               "    decimalStatistics {\n"
                               "      minimum: \"0\"\n"
                               "      maximum: \"5.5\"\n"
                               "      sum: \"16.6\"\n"
                               "    }\n"
                               "  }\n"
                               "  statistics {\n"
                               "    numberOfValues: 25000\n"
                               "  }\n"
                               "  rowIndexStride: 10000\n"
                               "}\n"
                               "fileLength: 246402\n"
                               "postscriptLength: 19\n";
  std::string output;
  std::string error;

  EXPECT_EQ(0, runProgram({pgm, std::string("-r"), file}, output, error));
  EXPECT_EQ(expected, output);
  EXPECT_EQ("", error);

  EXPECT_EQ(0, runProgram({pgm, std::string("--raw"), file}, output, error));
  EXPECT_EQ(expected, output);
  EXPECT_EQ("", error);
}

TEST(TestFileMetadata, testJson) {
  const std::string pgm = findProgram("tools/src/orc-metadata");
  const std::string file = findExample("orc_split_elim.orc");
  const std::string expected =
      "{ \"name\": \"" + file +
      "\",\n"
      "  \"type\": "
      "\"struct<userid:bigint,string1:string,subtype:double,decimal1:decimal(0,0),ts:timestamp>\","
      "\n"
      "  \"attributes\": {},\n"
      "  \"rows\": 25000,\n"
      "  \"stripe count\": 5,\n"
      "  \"format\": \"0.12\", \"writer version\": \"original\", \"software version\": \"ORC "
      "Java\",\n"
      "  \"compression\": \"none\",\n"
      "  \"file length\": 246402,\n"
      "  \"content\": 245568, \"stripe stats\": 526, \"footer\": 288, \"postscript\": 19,\n"
      "  \"row index stride\": 10000,\n"
      "  \"user metadata\": {\n"
      "  },\n"
      "  \"stripes\": [\n"
      "    { \"stripe\": 0, \"rows\": 5000,\n"
      "      \"offset\": 3, \"length\": 45568,\n"
      "      \"index\": 137, \"data\": 45282, \"footer\": 149\n"
      "    },\n"
      "    { \"stripe\": 1, \"rows\": 5000,\n"
      "      \"offset\": 45571, \"length\": 45568,\n"
      "      \"index\": 137, \"data\": 45282, \"footer\": 149\n"
      "    },\n"
      "    { \"stripe\": 2, \"rows\": 5000,\n"
      "      \"offset\": 91139, \"length\": 45568,\n"
      "      \"index\": 137, \"data\": 45282, \"footer\": 149\n"
      "    },\n"
      "    { \"stripe\": 3, \"rows\": 5000,\n"
      "      \"offset\": 136707, \"length\": 45570,\n"
      "      \"index\": 138, \"data\": 45283, \"footer\": 149\n"
      "    },\n"
      "    { \"stripe\": 4, \"rows\": 5000,\n"
      "      \"offset\": 200000, \"length\": 45568,\n"
      "      \"index\": 137, \"data\": 45282, \"footer\": 149\n"
      "    }\n"
      "  ]\n"
      "}\n";

  std::string output;
  std::string error;

  EXPECT_EQ(0, runProgram({pgm, file}, output, error));
  EXPECT_EQ(expected, output);
  EXPECT_EQ("", error);
}

TEST(TestFileMetadata, testNoFormat) {
  const std::string pgm = findProgram("tools/src/orc-metadata");
  const std::string file = findExample("orc_no_format.orc");
  const std::string expected =
      "{ \"name\": \"" + file +
      "\",\n"
      "  \"type\": "
      "\"struct<_col0:array<string>,_col1:map<int,string>,_col2:struct<name:string,score:int>>\",\n"
      "  \"attributes\": {},\n"
      "  \"rows\": 5,\n"
      "  \"stripe count\": 1,\n"
      "  \"format\": \"0.11\", \"writer version\": \"original\", \"software version\": \"ORC "
      "Java\",\n"
      "  \"compression\": \"zlib\", \"compression block\": 262144,\n"
      "  \"file length\": 745,\n"
      "  \"content\": 525, \"stripe stats\": 0, \"footer\": 210, \"postscript\": 9,\n"
      "  \"row index stride\": 10000,\n"
      "  \"user metadata\": {\n"
      "  },\n"
      "  \"stripes\": [\n"
      "    { \"stripe\": 0, \"rows\": 5,\n"
      "      \"offset\": 3, \"length\": 522,\n"
      "      \"index\": 224, \"data\": 187, \"footer\": 111\n"
      "    }\n"
      "  ]\n"
      "}\n";

  std::string output;
  std::string error;
  std::cout << expected;
  EXPECT_EQ(0, runProgram({pgm, file}, output, error));
  EXPECT_EQ(expected, output);
  EXPECT_EQ("", error);
}

TEST(TestFileMetadata, testV2Format) {
  const std::string pgm = findProgram("tools/src/orc-metadata");
  const std::string file = findExample("decimal64_v2.orc");
  const std::string expected_out =
      "{ \"name\": \"" + file +
      "\",\n"
      "  \"type\": "
      "\"struct<a:bigint,b:decimal(12,0),c:decimal(20,2),d:decimal(12,2),e:decimal(2,2)>\",\n"
      "  \"attributes\": {},\n"
      "  \"rows\": 10,\n"
      "  \"stripe count\": 1,\n"
      "  \"format\": \"UNSTABLE-PRE-2.0\", \"writer version\": \"ORC-135\", \"software version\": "
      "\"ORC Java\",\n"
      "  \"compression\": \"zlib\", \"compression block\": 262144,\n"
      "  \"file length\": 738,\n"
      "  \"content\": 377, \"stripe stats\": 130, \"footer\": 204, \"postscript\": 26,\n"
      "  \"row index stride\": 10000,\n"
      "  \"user metadata\": {\n"
      "  },\n"
      "  \"stripes\": [\n"
      "    { \"stripe\": 0, \"rows\": 10,\n"
      "      \"offset\": 3, \"length\": 374,\n"
      "      \"index\": 192, \"data\": 112, \"footer\": 70\n"
      "    }\n"
      "  ]\n"
      "}\n";
  const std::string expected_err =
      "Warning: ORC file " + file + " was written in an unknown format version UNSTABLE-PRE-2.0\n";

  std::string output;
  std::string error;
  EXPECT_EQ(0, runProgram({pgm, file}, output, error)) << error;
  EXPECT_EQ(expected_out, output);
  EXPECT_EQ(expected_err, error);
}

TEST(TestFileMetadata, testAttributes) {
  const std::string pgm = findProgram("tools/src/orc-metadata");
  const std::string file = findExample("complextypes_iceberg.orc");
  const std::string expected =
      "{ \"name\": \"" + file +
      "\",\n"
      "  \"type\": "
      "\"struct<id:bigint,int_array:array<int>,int_array_array:array<array<int>>,int_map:map<"
      "string,int>,int_map_array:array<map<string,int>>,nested_struct:struct<a:int,b:array<int>,c:"
      "struct<d:array<array<struct<e:int,f:string>>>>,g:map<string,struct<h:struct<i:array<double>>"
      ">>>>\",\n"
      "  \"attributes\": {\n"
      "    \"id\": {\"iceberg.id\": \"1\", \"iceberg.long-type\": \"LONG\", \"iceberg.required\": "
      "\"false\"},\n"
      "    \"int_array\": {\"iceberg.id\": \"2\", \"iceberg.required\": \"false\"},\n"
      "    \"int_array._elem\": {\"iceberg.id\": \"7\", \"iceberg.required\": \"false\"},\n"
      "    \"int_array_array\": {\"iceberg.id\": \"3\", \"iceberg.required\": \"false\"},\n"
      "    \"int_array_array._elem\": {\"iceberg.id\": \"8\", \"iceberg.required\": \"false\"},\n"
      "    \"int_array_array._elem._elem\": {\"iceberg.id\": \"9\", \"iceberg.required\": "
      "\"false\"},\n"
      "    \"int_map\": {\"iceberg.id\": \"4\", \"iceberg.required\": \"false\"},\n"
      "    \"int_map._key\": {\"iceberg.id\": \"10\", \"iceberg.required\": \"true\"},\n"
      "    \"int_map._value\": {\"iceberg.id\": \"11\", \"iceberg.required\": \"false\"},\n"
      "    \"int_map_array\": {\"iceberg.id\": \"5\", \"iceberg.required\": \"false\"},\n"
      "    \"int_map_array._elem\": {\"iceberg.id\": \"12\", \"iceberg.required\": \"false\"},\n"
      "    \"int_map_array._elem._key\": {\"iceberg.id\": \"13\", \"iceberg.required\": "
      "\"true\"},\n"
      "    \"int_map_array._elem._value\": {\"iceberg.id\": \"14\", \"iceberg.required\": "
      "\"false\"},\n"
      "    \"nested_struct\": {\"iceberg.id\": \"6\", \"iceberg.required\": \"false\"},\n"
      "    \"nested_struct.a\": {\"iceberg.id\": \"15\", \"iceberg.required\": \"false\"},\n"
      "    \"nested_struct.b\": {\"iceberg.id\": \"16\", \"iceberg.required\": \"false\"},\n"
      "    \"nested_struct.b._elem\": {\"iceberg.id\": \"19\", \"iceberg.required\": \"false\"},\n"
      "    \"nested_struct.c\": {\"iceberg.id\": \"17\", \"iceberg.required\": \"false\"},\n"
      "    \"nested_struct.c.d\": {\"iceberg.id\": \"20\", \"iceberg.required\": \"false\"},\n"
      "    \"nested_struct.c.d._elem\": {\"iceberg.id\": \"21\", \"iceberg.required\": "
      "\"false\"},\n"
      "    \"nested_struct.c.d._elem._elem\": {\"iceberg.id\": \"22\", \"iceberg.required\": "
      "\"false\"},\n"
      "    \"nested_struct.c.d._elem._elem.e\": {\"iceberg.id\": \"23\", \"iceberg.required\": "
      "\"false\"},\n"
      "    \"nested_struct.c.d._elem._elem.f\": {\"iceberg.id\": \"24\", \"iceberg.required\": "
      "\"false\"},\n"
      "    \"nested_struct.g\": {\"iceberg.id\": \"18\", \"iceberg.required\": \"false\"},\n"
      "    \"nested_struct.g._key\": {\"iceberg.id\": \"25\", \"iceberg.required\": \"true\"},\n"
      "    \"nested_struct.g._value\": {\"iceberg.id\": \"26\", \"iceberg.required\": \"false\"},\n"
      "    \"nested_struct.g._value.h\": {\"iceberg.id\": \"27\", \"iceberg.required\": "
      "\"false\"},\n"
      "    \"nested_struct.g._value.h.i\": {\"iceberg.id\": \"28\", \"iceberg.required\": "
      "\"false\"},\n"
      "    \"nested_struct.g._value.h.i._elem\": {\"iceberg.id\": \"29\", \"iceberg.required\": "
      "\"false\"}},\n"
      "  \"rows\": 1,\n"
      "  \"stripe count\": 1,\n"
      "  \"format\": \"0.12\", \"writer version\": \"ORC-14\", \"software version\": \"ORC "
      "Java\",\n"
      "  \"compression\": \"zlib\", \"compression block\": 131072,\n"
      "  \"file length\": 1734,\n"
      "  \"content\": 1006, \"stripe stats\": 167, \"footer\": 535, \"postscript\": 25,\n"
      "  \"row index stride\": 10000,\n"
      "  \"user metadata\": {\n"
      "  },\n"
      "  \"stripes\": [\n"
      "    { \"stripe\": 0, \"rows\": 1,\n"
      "      \"offset\": 3, \"length\": 1003,\n"
      "      \"index\": 679, \"data\": 150, \"footer\": 174\n"
      "    }\n"
      "  ]\n"
      "}\n";

  std::string output;
  std::string error;
  std::cout << expected;
  EXPECT_EQ(0, runProgram({pgm, file}, output, error));
  EXPECT_EQ(expected, output);
  EXPECT_EQ("", error);
}
