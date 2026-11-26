// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "testutil/adbc_sqlite_driver.h"

#include <sys/stat.h>

#include <cstdlib>
#include <string>

namespace doris {

namespace {

// run-be-ut.sh exports both, defaulting DORIS_THIRDPARTY to ${DORIS_HOME}/thirdparty.
std::string thirdparty_dir() {
    if (const char* tp = std::getenv("DORIS_THIRDPARTY"); tp != nullptr && *tp != '\0') {
        return tp;
    }
    if (const char* home = std::getenv("DORIS_HOME"); home != nullptr && *home != '\0') {
        return std::string(home) + "/thirdparty";
    }
    return "thirdparty";
}

} // namespace

std::string adbc_sqlite_driver_path() {
    return thirdparty_dir() + "/installed/lib64/libadbc_driver_sqlite.so";
}

bool adbc_sqlite_driver_available() {
    struct stat st {};
    return ::stat(adbc_sqlite_driver_path().c_str(), &st) == 0;
}

} // namespace doris
