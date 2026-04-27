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

#include <gen_cpp/Types_types.h>

#include <string>

#include "common/status.h"
#include "paimon/status.h"

namespace paimon {

struct ParsedUri {
    std::string scheme;
    std::string authority;
};

// Visible for tests: maps a URI scheme to the Doris file type used by paimon-cpp.
doris::TFileType::type map_scheme_to_file_type(const std::string& scheme);

// Visible for tests.
ParsedUri parse_uri(const std::string& path);
std::string replace_scheme(const std::string& path, const std::string& scheme);
std::string normalize_local_path(const std::string& path);
std::string normalize_path_for_type(const std::string& path, const ParsedUri& uri,
                                    doris::TFileType::type type);
std::string build_fs_cache_key(doris::TFileType::type type, const ParsedUri& uri,
                               const std::string& default_fs_name);
Status to_paimon_status(const doris::Status& status);

} // namespace paimon

namespace doris {

// Force-link helper so the paimon-cpp file system factory registration is kept.
void register_paimon_doris_file_system();

} // namespace doris
