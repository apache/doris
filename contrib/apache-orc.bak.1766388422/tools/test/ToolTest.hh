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

#include <string>
#include <vector>

/**
 * Run the given program and set the stdout and stderr parameters to
 * the output on each of the streams. The return code of the program is
 * returned as the result.
 */
int runProgram(const std::vector<std::string>& command, std::string& out, std::string& err);

/**
 * Get the name of the given example file.
 * @param name the simple name of the example file
 */
std::string findExample(const std::string& name);

/**
 * Get the name of the given executable.
 * @param name the simple name of the executable
 */
std::string findProgram(const std::string& name);
