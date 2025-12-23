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

TEST(TestFileStatistics, testNormal) {
  const std::string pgm = findProgram("tools/src/orc-statistics");
  const std::string file = findExample("orc_index_int_string.orc");
  const std::string expected = "File " + file +
                               " has 3 columns\n"
                               "*** Column 0 ***\n"
                               "Column has 6000 values and has null value: yes\n"
                               "\n"
                               "*** Column 1 ***\n"
                               "Data type: Integer\n"
                               "Values: 6000\n"
                               "Has null: yes\n"
                               "Minimum: 1\n"
                               "Maximum: 6000\n"
                               "Sum: 18003000\n"
                               "\n"
                               "*** Column 2 ***\n"
                               "Data type: String\n"
                               "Values: 6000\n"
                               "Has null: yes\n"
                               "Minimum: 1000\n"
                               "Maximum: 9a\n"
                               "Total length: 23892\n"
                               "\n"
                               "File " +
                               file +
                               " has 1 stripes\n"
                               "*** Stripe 0 ***\n"
                               "\n"
                               "--- Column 0 ---\n"
                               "Column has 6000 values and has null value: yes\n"
                               "\n"
                               "--- Column 1 ---\n"
                               "Data type: Integer\n"
                               "Values: 6000\n"
                               "Has null: yes\n"
                               "Minimum: 1\n"
                               "Maximum: 6000\n"
                               "Sum: 18003000\n"
                               "\n"
                               "--- Column 2 ---\n"
                               "Data type: String\n"
                               "Values: 6000\n"
                               "Has null: yes\n"
                               "Minimum: 1000\n"
                               "Maximum: 9a\n"
                               "Total length: 23892\n\n";

  std::string output;
  std::string error;

  EXPECT_EQ(0, runProgram({pgm, file}, output, error));
  EXPECT_EQ(expected, output);
  EXPECT_EQ("", error);
}

TEST(TestFileStatistics, testOptions) {
  const std::string pgm = findProgram("tools/src/orc-statistics");
  const std::string file = findExample("orc_index_int_string.orc");
  const std::string expected = "File " + file +
                               " has 3 columns\n"
                               "*** Column 0 ***\n"
                               "Column has 6000 values and has null value: yes\n"
                               "\n"
                               "*** Column 1 ***\n"
                               "Data type: Integer\n"
                               "Values: 6000\n"
                               "Has null: yes\n"
                               "Minimum: 1\n"
                               "Maximum: 6000\n"
                               "Sum: 18003000\n"
                               "\n"
                               "*** Column 2 ***\n"
                               "Data type: String\n"
                               "Values: 6000\n"
                               "Has null: yes\n"
                               "Minimum: 1000\n"
                               "Maximum: 9a\n"
                               "Total length: 23892\n"
                               "\n"
                               "File " +
                               file +
                               " has 1 stripes\n"
                               "*** Stripe 0 ***\n"
                               "\n"
                               "--- Column 0 ---\n"
                               "Column has 6000 values and has null value: yes\n"
                               "\n"
                               "--- RowIndex 0 ---\n"
                               "Column has 2000 values and has null value: yes\n"
                               "\n"
                               "--- RowIndex 1 ---\n"
                               "Column has 2000 values and has null value: yes\n"
                               "\n"
                               "--- RowIndex 2 ---\n"
                               "Column has 2000 values and has null value: yes\n"
                               "\n"
                               "--- Column 1 ---\n"
                               "Data type: Integer\n"
                               "Values: 6000\n"
                               "Has null: yes\n"
                               "Minimum: 1\n"
                               "Maximum: 6000\n"
                               "Sum: 18003000\n"
                               "\n"
                               "--- RowIndex 0 ---\n"
                               "Data type: Integer\n"
                               "Values: 2000\n"
                               "Has null: yes\n"
                               "Minimum: 1\n"
                               "Maximum: 2000\n"
                               "Sum: 2001000\n"
                               "\n"
                               "--- RowIndex 1 ---\n"
                               "Data type: Integer\n"
                               "Values: 2000\n"
                               "Has null: yes\n"
                               "Minimum: 2001\n"
                               "Maximum: 4000\n"
                               "Sum: 6001000\n"
                               "\n"
                               "--- RowIndex 2 ---\n"
                               "Data type: Integer\n"
                               "Values: 2000\n"
                               "Has null: yes\n"
                               "Minimum: 4001\n"
                               "Maximum: 6000\n"
                               "Sum: 10001000\n"
                               "\n"
                               "--- Column 2 ---\n"
                               "Data type: String\n"
                               "Values: 6000\n"
                               "Has null: yes\n"
                               "Minimum: 1000\n"
                               "Maximum: 9a\n"
                               "Total length: 23892\n"
                               "\n"
                               "--- RowIndex 0 ---\n"
                               "Data type: String\n"
                               "Values: 2000\n"
                               "Has null: yes\n"
                               "Minimum: 1000\n"
                               "Maximum: 9a\n"
                               "Total length: 7892\n"
                               "\n"
                               "--- RowIndex 1 ---\n"
                               "Data type: String\n"
                               "Values: 2000\n"
                               "Has null: yes\n"
                               "Minimum: 2001\n"
                               "Maximum: 4000\n"
                               "Total length: 8000\n"
                               "\n"
                               "--- RowIndex 2 ---\n"
                               "Data type: String\n"
                               "Values: 2000\n"
                               "Has null: yes\n"
                               "Minimum: 4001\n"
                               "Maximum: 6000\n"
                               "Total length: 8000\n\n";

  std::string output;
  std::string error;
  std::vector<std::string> args;
  args.push_back(pgm);
  args.push_back("-i");
  args.push_back(file);
  EXPECT_EQ(0, runProgram(args, output, error));
  EXPECT_EQ(expected, output);
  EXPECT_EQ("", error);
}
