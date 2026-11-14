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

#include "cpp/obj-storage/ca_bundle_cache.h"

#include <sstream>
#include <utility>
#include <vector>

#include "cpp/aws_common.h"

namespace doris {

std::string CaBundleCache::path(const std::string& configured_paths) {
    std::lock_guard lock(_lock);
    if (_path.empty()) {
        std::istringstream input(configured_paths);
        std::vector<std::string> candidates;
        for (std::string candidate; std::getline(input, candidate, ';');) {
            candidates.push_back(std::move(candidate));
        }
        _path = get_valid_ca_cert_path(candidates);
    }
    return _path;
}

} // namespace doris
