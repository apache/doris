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

#include "common/version_internal.h"

#include <gen_cpp/version.h>

namespace doris {
namespace version {

const char* doris_build_version_prefix() {
    return DORIS_BUILD_VERSION_PREFIX;
}
int doris_build_version_major() {
    return DORIS_BUILD_VERSION_MAJOR;
}
int doris_build_version_minor() {
    return DORIS_BUILD_VERSION_MINOR;
}
int doris_build_version_patch() {
    return DORIS_BUILD_VERSION_PATCH;
}
int doris_build_version_hotfix() {
    return DORIS_BUILD_VERSION_HOTFIX;
}
const char* doris_build_version_rc_version() {
    return DORIS_BUILD_VERSION_RC_VERSION;
}

const char* doris_build_version() {
    return DORIS_BUILD_VERSION;
}
const char* doris_build_hash() {
    return DORIS_BUILD_HASH;
}
const char* doris_build_short_hash() {
    return DORIS_BUILD_SHORT_HASH;
}
const char* doris_build_time() {
    return DORIS_BUILD_TIME;
}
const char* doris_build_info() {
    return DORIS_BUILD_INFO;
}

} // namespace version

} // namespace doris
