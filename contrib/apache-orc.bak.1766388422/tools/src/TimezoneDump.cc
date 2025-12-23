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

#include "orc/Exceptions.hh"

#include "Timezone.hh"

#include <iostream>
#include <memory>
#include <string>

void printFile(const std::string& name) {
  std::cout << "Timezone " << name << ":\n";
  const orc::Timezone& tz = orc::getTimezoneByName(name);
  tz.print(std::cout);
}

int main(int argc, char* argv[]) {
  if (argc < 2) {
    std::cout << "Usage: timezone-dump<filename>\n";
  }
  for (int o = 1; o < argc; ++o) {
    printFile(argv[o]);
  }
  return 0;
}
