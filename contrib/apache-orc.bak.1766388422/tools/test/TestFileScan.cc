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

#include "orc/ColumnPrinter.hh"
#include "orc/OrcFile.hh"

#include "Adaptor.hh"
#include "ToolTest.hh"

#include "wrap/gmock.h"
#include "wrap/gtest-wrapper.h"

TEST(TestFileScan, testNominal) {
  const std::string pgm = findProgram("tools/src/orc-scan");
  const std::string file = findExample("TestOrcFile.testSeek.orc");
  std::string output;
  std::string error;
  EXPECT_EQ(0, runProgram({pgm, file}, output, error));
  EXPECT_EQ("Rows: 32768\nBatches: 33\n", output);
  EXPECT_EQ("", error);

  EXPECT_EQ(0, runProgram({pgm, std::string("-c"), std::string("1,2,3,9"), file}, output, error));
  EXPECT_EQ("Rows: 32768\nBatches: 33\n", output);
  EXPECT_EQ("", error);

  EXPECT_EQ(0, runProgram({pgm, std::string("-b"), std::string("256"), file}, output, error));
  EXPECT_EQ("Rows: 32768\nBatches: 131\n", output);
  EXPECT_EQ("", error);

  EXPECT_EQ(0, runProgram({pgm, std::string("-b"), std::string("256"), std::string("-c"),
                           std::string("1,2,3"), file},
                          output, error));
  EXPECT_EQ("Rows: 32768\nBatches: 131\n", output);
  EXPECT_EQ("", error);

  EXPECT_EQ(0, runProgram({pgm, std::string("-b256"), file}, output, error));
  EXPECT_EQ("Rows: 32768\nBatches: 131\n", output);
  EXPECT_EQ("", error);

  EXPECT_EQ(0, runProgram({pgm, std::string("--batch"), std::string("256"), file}, output, error));
  EXPECT_EQ("Rows: 32768\nBatches: 131\n", output);
  EXPECT_EQ("", error);

  EXPECT_EQ(0, runProgram({pgm, std::string("--batch=256"), file}, output, error));
  EXPECT_EQ("Rows: 32768\nBatches: 131\n", output);
  EXPECT_EQ("", error);

  EXPECT_EQ(0, runProgram({pgm, std::string("--batch=256"), std::string("--columns=1,2,3"), file},
                          output, error));
  EXPECT_EQ("Rows: 32768\nBatches: 131\n", output);
  EXPECT_EQ("", error);
}

/**
 * This function locates the goal substring in the input and removes
 * everything before it.
 * stripPrefix("abcdef", "cd") -> "cdef"
 * stripPrefix("abcdef", "xx") -> "abcdef"
 */
std::string stripPrefix(const std::string& input, const std::string& goal) {
  size_t loc = input.find(goal);
  if (loc == std::string::npos) {
    return input;
  } else {
    return input.substr(loc);
  }
}

std::string removeChars(const std::string& input, const std::string& chars) {
  std::string result;
  size_t prev = 0;
  size_t pos = input.find_first_of(chars);
  while (pos != std::string::npos) {
    if (pos > prev) {
      result += input.substr(prev, pos - prev);
    }
    prev = pos + 1;
    pos = input.find_first_of(chars, prev);
  }
  pos = input.length();
  if (pos > prev) {
    result += input.substr(prev, pos - prev);
  }
  return result;
}

TEST(TestFileScan, testRemoveChars) {
  EXPECT_EQ("abcdef", removeChars("abcdef", "xyz"));
  EXPECT_EQ("bbbddd", removeChars("aaabbbcccddd", "ca"));
  EXPECT_EQ("aaaccc", removeChars("aaabbbcccddd", "bd"));
  EXPECT_EQ("abcde", removeChars("axbxcxdxe", "x"));
}

TEST(TestFileScan, testBadCommand) {
  const std::string pgm = findProgram("tools/src/orc-scan");
  const std::string file = findExample("TestOrcFile.testSeek.orc");
  std::string output;
  std::string error;
  EXPECT_EQ(1, runProgram({pgm, file, std::string("-b")}, output, error));
  EXPECT_EQ("", output);
  EXPECT_EQ(
      "orc-scan: option requires an argument -- b\n"
      "Usage: orc-scan [options] <filename>...\n"
      "Options:\n"
      "\t-h --help\n"
      "\t-c --columns\t\tComma separated list of top-level column fields\n"
      "\t-t --columnTypeIds\tComma separated list of column type ids\n"
      "\t-n --columnNames\tComma separated list of column names\n"
      "\t-b --batch\t\tBatch size for reading\n"
      "\t-m --metrics\t\tShow metrics for reading\n"
      "Scans and displays the row count of the ORC files.\n",
      removeChars(stripPrefix(error, "orc-scan: "), "'`"));

  EXPECT_EQ(1, runProgram({pgm, file, std::string("-b"), std::string("20x")}, output, error));
  EXPECT_EQ("", output);
  EXPECT_EQ(
      "The --batch parameter requires an integer option.\n"
      "Usage: orc-scan [options] <filename>...\n"
      "Options:\n"
      "\t-h --help\n"
      "\t-c --columns\t\tComma separated list of top-level column fields\n"
      "\t-t --columnTypeIds\tComma separated list of column type ids\n"
      "\t-n --columnNames\tComma separated list of column names\n"
      "\t-b --batch\t\tBatch size for reading\n"
      "\t-m --metrics\t\tShow metrics for reading\n"
      "Scans and displays the row count of the ORC files.\n",
      error);

  EXPECT_EQ(1, runProgram({pgm, file, std::string("-b"), std::string("x30")}, output, error));
  EXPECT_EQ("", output);
  EXPECT_EQ(
      "The --batch parameter requires an integer option.\n"
      "Usage: orc-scan [options] <filename>...\n"
      "Options:\n"
      "\t-h --help\n"
      "\t-c --columns\t\tComma separated list of top-level column fields\n"
      "\t-t --columnTypeIds\tComma separated list of column type ids\n"
      "\t-n --columnNames\tComma separated list of column names\n"
      "\t-b --batch\t\tBatch size for reading\n"
      "\t-m --metrics\t\tShow metrics for reading\n"
      "Scans and displays the row count of the ORC files.\n",
      error);

  EXPECT_EQ(1, runProgram({pgm, file, std::string("--batch")}, output, error));
  EXPECT_EQ("", output);
  EXPECT_EQ(
      "orc-scan: option --batch requires an argument\n"
      "Usage: orc-scan [options] <filename>...\n"
      "Options:\n"
      "\t-h --help\n"
      "\t-c --columns\t\tComma separated list of top-level column fields\n"
      "\t-t --columnTypeIds\tComma separated list of column type ids\n"
      "\t-n --columnNames\tComma separated list of column names\n"
      "\t-b --batch\t\tBatch size for reading\n"
      "\t-m --metrics\t\tShow metrics for reading\n"
      "Scans and displays the row count of the ORC files.\n",
      removeChars(stripPrefix(error, "orc-scan: "), "'`"));

  EXPECT_EQ(1, runProgram({pgm, file, std::string("--batch"), std::string("20x")}, output, error));
  EXPECT_EQ("", output);
  EXPECT_EQ(
      "The --batch parameter requires an integer option.\n"
      "Usage: orc-scan [options] <filename>...\n"
      "Options:\n"
      "\t-h --help\n"
      "\t-c --columns\t\tComma separated list of top-level column fields\n"
      "\t-t --columnTypeIds\tComma separated list of column type ids\n"
      "\t-n --columnNames\tComma separated list of column names\n"
      "\t-b --batch\t\tBatch size for reading\n"
      "\t-m --metrics\t\tShow metrics for reading\n"
      "Scans and displays the row count of the ORC files.\n",
      error);

  EXPECT_EQ(1, runProgram({pgm, file, std::string("--batch"), std::string("x30")}, output, error));
  EXPECT_EQ("", output);
  EXPECT_EQ(
      "The --batch parameter requires an integer option.\n"
      "Usage: orc-scan [options] <filename>...\n"
      "Options:\n"
      "\t-h --help\n"
      "\t-c --columns\t\tComma separated list of top-level column fields\n"
      "\t-t --columnTypeIds\tComma separated list of column type ids\n"
      "\t-n --columnNames\tComma separated list of column names\n"
      "\t-b --batch\t\tBatch size for reading\n"
      "\t-m --metrics\t\tShow metrics for reading\n"
      "Scans and displays the row count of the ORC files.\n",
      error);
}

void checkForError(const std::string& filename, const std::string& error_msg) {
  const std::string pgm = findProgram("tools/src/orc-scan");
  std::string output;
  std::string error;
  EXPECT_EQ(1, runProgram({pgm, filename}, output, error));
  EXPECT_EQ("", output);
  EXPECT_NE(std::string::npos, error.find(error_msg)) << error;
}

TEST(TestFileScan, testErrorHandling) {
  checkForError(findExample("corrupt/stripe_footer_bad_column_encodings.orc"),
                "bad number of ColumnEncodings in StripeFooter: expected=6, actual=0");
  checkForError(findExample("corrupt/negative_dict_entry_lengths.orc"),
                "Negative dictionary entry length");
  checkForError(findExample("corrupt/missing_length_stream_in_string_dict.orc"),
                "LENGTH stream not found in StringDictionaryColumn");
  checkForError(findExample("corrupt/missing_blob_stream_in_string_dict.orc"),
                "DICTIONARY_DATA stream not found in StringDictionaryColumn");
}
