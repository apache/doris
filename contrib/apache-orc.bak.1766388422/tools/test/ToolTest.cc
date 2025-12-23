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

#include "ToolTest.hh"
#include "orc/OrcFile.hh"
#include "orc/orc-config.hh"

#include "wrap/gtest-wrapper.h"
#include "wrap/orc-proto-wrapper.hh"

#include <cerrno>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

namespace {
  const char* exampleDirectory = 0;
  const char* buildDirectory = 0;
}  // namespace

GTEST_API_ int main(int argc, char** argv) {
  GOOGLE_PROTOBUF_VERIFY_VERSION;
  std::cout << "ORC version: " << ORC_VERSION << "\n";
  if (argc >= 2) {
    exampleDirectory = argv[1];
  } else {
    exampleDirectory = "../examples";
  }
  if (argc >= 3) {
    buildDirectory = argv[2];
  } else {
    buildDirectory = ".";
  }
  std::cout << "example dir = " << exampleDirectory << "\n";
  if (buildDirectory) {
    std::cout << "build dir = " << buildDirectory << "\n";
  }
  testing::InitGoogleTest(&argc, argv);
  int result = RUN_ALL_TESTS();
  google::protobuf::ShutdownProtobufLibrary();
  return result;
}

/**
 * Run the given program and set the stdout and stderr parameters to
 * the output on each of the streams. The return code of the program is
 * returned as the result.
 */
int runProgram(const std::vector<std::string>& args, std::string& out, std::string& err) {
  std::ostringstream command;
  std::copy(args.begin(), args.end(), std::ostream_iterator<std::string>(command, " "));

  testing::internal::CaptureStdout();
  testing::internal::CaptureStderr();

  int status = system(command.str().c_str());

  out = testing::internal::GetCapturedStdout();
  err = testing::internal::GetCapturedStderr();

  return WEXITSTATUS(status);
}

/**
 * Get the name of the given example file.
 * @param name the simple name of the example file
 */
std::string findExample(const std::string& name) {
  std::string result = exampleDirectory;
  result += "/";
  result += name;
  return result;
}

/**
 * Get the name of the given executable.
 * @param name the simple name of the executable
 */
std::string findProgram(const std::string& name) {
  std::string result = buildDirectory;
  result += "/";
  result += name;
  return result;
}
