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
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/util/path-builder.cc
// and modified by Doris

#include "util/path_builder.h"

#include <stdlib.h>

#include <sstream>

namespace doris {

const char* PathBuilder::_s_doris_home;

void PathBuilder::load_doris_home() {
    if (_s_doris_home != nullptr) {
        return;
    }

    _s_doris_home = getenv("DORIS_HOME");
}

void PathBuilder::get_full_path(const std::string& path, std::string* full_path) {
    load_doris_home();
    std::stringstream s;
    s << _s_doris_home << "/" << path;
    *full_path = s.str();
}

void PathBuilder::get_full_build_path(const std::string& path, std::string* full_path) {
    load_doris_home();
    std::stringstream s;
#ifdef NDEBUG
    s << _s_doris_home << "/be/build/release/" << path;
#else
    s << _s_doris_home << "/be/build/debug/" << path;
#endif
    *full_path = s.str();
}

} // namespace doris
