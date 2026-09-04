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

#include <cstdint>
#include <functional>
#include <map>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>

#include "storage/index/inverted/analyzer/custom_analyzer.h"
#include "storage/index/inverted/normalizer/custom_normalizer.h"

namespace doris {

using Policys = std::unordered_map<int64_t, TIndexPolicy>;
using AnalyzerPtr = std::shared_ptr<lucene::analysis::Analyzer>;
using AnalyzerProviderPtr = segment_v2::inverted_index::AnalyzerProviderPtr;

class IndexPolicyMgr {
public:
    IndexPolicyMgr() = default;
    ~IndexPolicyMgr() = default;

    void apply_policy_changes(const std::vector<TIndexPolicy>& policies_to_update,
                              const std::vector<int64_t>& policies_to_delete);

    Policys get_index_policys();
    AnalyzerPtr get_policy_by_name(const std::string& name);
    AnalyzerPtr get_analyzer_by_name(const std::string& name,
                                     segment_v2::inverted_index::AnalysisPurpose purpose);
    AnalyzerProviderPtr get_analyzer_provider_by_name(
            const std::string& name,
            const std::map<std::string, std::string>& outer_char_filter_map = {});
    AnalyzerProviderPtr get_analyzer_provider_by_base_fingerprint(
            std::string_view base_analyzer_fingerprint,
            const std::map<std::string, std::string>& outer_char_filter_map = {});

private:
    segment_v2::inverted_index::CustomAnalyzerConfigPtr build_analyzer_config_from_policy(
            const TIndexPolicy& index_policy_analyzer);
    AnalyzerProviderPtr build_analyzer_provider_from_config(
            segment_v2::inverted_index::CustomAnalyzerConfigPtr config,
            const std::map<std::string, std::string>& outer_char_filter_map);
    AnalyzerPtr build_analyzer_from_policy(const TIndexPolicy& index_policy_analyzer);
    AnalyzerPtr build_normalizer_from_policy(const TIndexPolicy& index_policy_normalizer);

    void process_filter_configs(
            const TIndexPolicy& index_policy_analyzer, const std::string& prop_name,
            const std::string& error_prefix,
            std::function<void(const std::string&, const segment_v2::inverted_index::Settings&)>
                    add_config_func);

    bool is_builtin_normalizer(const std::string& name);
    AnalyzerPtr build_builtin_normalizer(const std::string& name);

    // Normalize policy name to lowercase for case-insensitive lookup
    static std::string normalize_name(const std::string& name);

    constexpr static auto PROP_TOKENIZER = "tokenizer";
    constexpr static auto PROP_CHAR_FILTER = "char_filter";
    constexpr static auto PROP_TOKEN_FILTER = "token_filter";
    constexpr static auto PROP_TYPE = "type";

    static const std::unordered_set<std::string> BUILTIN_NORMALIZERS;

    std::shared_mutex _mutex;

    Policys _policys;
    std::unordered_map<std::string, int64_t> _name_to_id;
};

} // namespace doris