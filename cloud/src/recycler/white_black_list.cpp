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

#include "recycler/white_black_list.h"

namespace doris::cloud {

void WhiteBlackList::reset(const std::vector<std::string>& whitelist,
                           const std::vector<std::string>& blacklist) {
    blacklist_.clear();
    whitelist_.clear();
    if (!whitelist.empty()) {
        for (const auto& str : whitelist) {
            whitelist_.insert(str);
        }
    } else {
        for (const auto& str : blacklist) {
            blacklist_.insert(str);
        }
    }
}

bool WhiteBlackList::filter_out(const std::string& instance_id) const {
    if (whitelist_.empty()) {
        return blacklist_.count(instance_id);
    }
    return !whitelist_.count(instance_id);
}

} // namespace doris::cloud
