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

#pragma once

#include <fmt/format.h>
#include <glog/logging.h>

namespace doris {

class BeExecVersionManager {
public:
    BeExecVersionManager() = delete;

    static bool check_be_exec_version(int be_exec_version) {
        if (be_exec_version > max_be_exec_version || be_exec_version < min_be_exec_version) {
            LOG(WARNING) << fmt::format(
                    "Received be_exec_version is not supported, be_exec_version={}, "
                    "min_be_exec_version={}, max_be_exec_version={}, maybe due to FE version not "
                    "match "
                    "with BE.",
                    be_exec_version, min_be_exec_version, max_be_exec_version);
            return false;
        }
        return true;
    }

    static int get_newest_version() { return max_be_exec_version; }

private:
    static const int max_be_exec_version;
    static const int min_be_exec_version;
};

// When we have some breaking change for execute engine, we should update be_exec_version.
// 0: not contain be_exec_version.
// 1: start from doris 1.2
//    a. remove ColumnString terminating zero.
//    b. runtime filter use new hash method.
inline const int BeExecVersionManager::max_be_exec_version = 1;
inline const int BeExecVersionManager::min_be_exec_version = 0;

} // namespace doris
