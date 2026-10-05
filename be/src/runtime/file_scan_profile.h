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

#include "runtime/runtime_profile.h"

namespace doris::file_scan_profile {

inline constexpr const char* SCANNER = "FileScannerV2";
inline constexpr const char* TABLE_READER = "TableReader";
inline constexpr const char* FILE_READER = "FileReader";
inline constexpr const char* IO = "IO";

struct Hierarchy {
    RuntimeProfile::Counter* scanner = nullptr;
    RuntimeProfile::Counter* table_reader = nullptr;
    RuntimeProfile::Counter* file_reader = nullptr;
    RuntimeProfile::Counter* io = nullptr;
};

inline Hierarchy ensure_hierarchy(RuntimeProfile* profile) {
    if (profile == nullptr) {
        return {};
    }
    // RuntimeProfile stores one flat counter namespace and a separate parent map. Create every
    // layer in order so later format and IO profiles cannot accidentally become scanner siblings.
    auto* scanner = ADD_TIMER_WITH_LEVEL(profile, SCANNER, 1);
    auto* table_reader = ADD_CHILD_TIMER_WITH_LEVEL(profile, TABLE_READER, SCANNER, 1);
    auto* file_reader = ADD_CHILD_TIMER_WITH_LEVEL(profile, FILE_READER, TABLE_READER, 1);
    auto* io = ADD_CHILD_TIMER_WITH_LEVEL(profile, IO, FILE_READER, 1);
    return {scanner, table_reader, file_reader, io};
}

inline const char* parent_or_root(RuntimeProfile* profile, const char* preferred_parent) {
    return profile != nullptr && profile->get_counter(preferred_parent) != nullptr
                   ? preferred_parent
                   : RuntimeProfile::ROOT_COUNTER.c_str();
}

} // namespace doris::file_scan_profile
