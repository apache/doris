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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/util/path-builder.h
// and modified by Doris

#ifndef DORIS_BE_SRC_COMMON_UTIL_PATH_BUILDER_H
#define DORIS_BE_SRC_COMMON_UTIL_PATH_BUILDER_H

#include <string>

namespace doris {

// Utility class to construct full paths relative to the doris_home path.
class PathBuilder {
public:
    // Sets full_path to <DORIS_HOME>/path
    static void get_full_path(const std::string& path, std::string* full_path);

    // Sets full_path to <DORIS_HOME>/<build><debug OR release>/path
    static void get_full_build_path(const std::string& path, std::string* full_path);

private:
    // Cache of env['DORIS_HOME']
    static const char* _s_doris_home;

    // Load _s_doris_home if it is not already loaded
    static void load_doris_home();
};

} // namespace doris

#endif
