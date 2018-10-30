// Modifications copyright (C) 2017, Baidu.com, Inc.
// Copyright 2017 The Apache Software Foundation

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

#include "util/path_builder.h"

#include <sstream>
#include <stdlib.h>

namespace palo {

const char* PathBuilder::_s_palo_home;

void PathBuilder::load_palo_home() {
    if (_s_palo_home != NULL) {
        return;
    }

    _s_palo_home = getenv("DORIS_HOME");
}

void PathBuilder::get_full_path(const std::string& path, std::string* full_path) {
    load_palo_home();
    std::stringstream s;
    s << _s_palo_home << "/" << path;
    *full_path = s.str();
}

void PathBuilder::get_full_build_path(const std::string& path, std::string* full_path) {
    load_palo_home();
    std::stringstream s;
#ifdef NDEBUG
    s << _s_palo_home << "/be/build/release/" << path;
#else
    s << _s_palo_home << "/be/build/debug/" << path;
#endif
    *full_path = s.str();
}

}
