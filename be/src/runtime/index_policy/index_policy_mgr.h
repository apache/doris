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

#include <gen_cpp/AgentService_types.h>

#include <shared_mutex>

#include "olap/rowset/segment_v2/inverted_index/analyzer/custom_analyzer.h"

namespace doris {

using Policys = std::unordered_map<int64_t, TIndexPolicy>;

class IndexPolicyMgr {
public:
    IndexPolicyMgr() = default;
    ~IndexPolicyMgr() = default;

    void apply_policy_changes(const std::vector<TIndexPolicy>& policies_to_update,
                              const std::vector<int64_t>& policies_to_delete);

    const Policys& get_index_policys();
    segment_v2::inverted_index::CustomAnalyzerPtr get_policy_by_name(const std::string& name);

private:
    constexpr static auto PROP_TOKENIZER = "tokenizer";
    constexpr static auto PROP_TOKEN_FILTER = "token_filter";
    constexpr static auto PROP_TYPE = "type";

    std::shared_mutex _mutex;

    Policys _policys;
    std::unordered_map<std::string, int64_t> _name_to_id;
};

} // namespace doris