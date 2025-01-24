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

#include "common/kerberos/kerberos_config.h"

#include <filesystem>

#include "common/config.h"

namespace doris::kerberos {

KerberosConfig::KerberosConfig()
        : _refresh_interval_second(300), _min_time_before_refresh_second(600) {}

void KerberosConfig::set_cache_file_path(const std::string& path) {
    const std::string prefix = doris::config::kerberos_ccache_path;
    std::filesystem::path full_path = std::filesystem::path(prefix) / path;
    full_path = std::filesystem::weakly_canonical(full_path);
    _cache_file_path = full_path.string();
}

} // namespace doris::kerberos
