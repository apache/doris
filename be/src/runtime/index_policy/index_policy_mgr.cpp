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

#include "index_policy_mgr.h"

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <utility>

namespace doris {

void IndexPolicyMgr::apply_policy_changes(const std::vector<TIndexPolicy>& policys_to_update,
                                          const std::vector<int64_t>& policys_to_delete) {
    LOG(INFO) << "Starting policy changes - "
              << "Updates: " << policys_to_update.size() << " policies, "
              << "Deletions: " << policys_to_delete.size() << " policies";

    std::unique_lock lock(_mutex);
    int32_t success_deletes = 0;
    int32_t success_updates = 0;

    for (auto id : policys_to_delete) {
        if (auto it = _policys.find(id); it != _policys.end()) {
            LOG(INFO) << "Deleting policy - "
                      << "ID: " << id << ", "
                      << "Name: " << it->second.name;

            _name_to_id.erase(it->second.name);
            _policys.erase(it);
            success_deletes++;
        } else {
            LOG(WARNING) << "Delete failed - Policy ID not found: " << id;
        }
    }

    for (const auto& policy : policys_to_update) {
        if (_policys.contains(policy.id)) {
            LOG(ERROR) << "Reject update - Duplicate policy ID: " << policy.id
                       << " | Existing name: " << _policys[policy.id].name
                       << " | New name: " << policy.name;
            continue;
        }

        if (_name_to_id.contains(policy.name)) {
            LOG(ERROR) << "Reject update - Duplicate policy name: " << policy.name
                       << " | Existing ID: " << _name_to_id[policy.name]
                       << " | New ID: " << policy.id;
            continue;
        }

        _policys.emplace(policy.id, policy);
        _name_to_id.emplace(policy.name, policy.id);
        success_updates++;

        LOG(INFO) << "Successfully applied policy - "
                  << "ID: " << policy.id << ", "
                  << "Name: " << policy.name << ", "
                  << "Type: " << policy.type;
    }

    LOG(INFO) << "Policy changes completed - "
              << "Deleted: " << success_deletes << "/" << policys_to_delete.size() << ", "
              << "Updated: " << success_updates << "/" << policys_to_update.size() << ", "
              << "Total policies: " << _policys.size();
}

const Policys& IndexPolicyMgr::get_index_policys() {
    std::shared_lock<std::shared_mutex> r_lock(_mutex);
    return _policys;
}

// TODO: Potential high-concurrency bottleneck
segment_v2::inverted_index::CustomAnalyzerPtr IndexPolicyMgr::get_policy_by_name(
        const std::string& name) {
    std::shared_lock lock(_mutex);

    // Check if policy exists
    auto name_it = _name_to_id.find(name);
    if (name_it == _name_to_id.end()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Policy not found with name: " + name);
    }

    // Get policy by ID
    auto policy_it = _policys.find(name_it->second);
    if (policy_it == _policys.end()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Policy not found with name: " + name);
    }

    const auto& index_policy_analyzer = policy_it->second;
    segment_v2::inverted_index::CustomAnalyzerConfig::Builder builder;

    // Process tokenizer
    auto tokenizer_it = index_policy_analyzer.properties.find(PROP_TOKENIZER);
    if (tokenizer_it == index_policy_analyzer.properties.end() || tokenizer_it->second.empty()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Invalid tokenizer configuration in policy: " + name);
    }
    const auto& tokenzier_name = tokenizer_it->second;
    if (_name_to_id.contains(tokenzier_name)) {
        const auto& tokenizer_policy = _policys[_name_to_id[tokenzier_name]];
        auto type_it = tokenizer_policy.properties.find(PROP_TYPE);
        if (type_it == tokenizer_policy.properties.end()) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Invalid tokenizer configuration in policy: " + tokenzier_name);
        }

        segment_v2::inverted_index::Settings settings;
        for (const auto& prop : tokenizer_policy.properties) {
            if (prop.first != PROP_TYPE) {
                settings.set(prop.first, prop.second);
            }
        }
        builder.with_tokenizer_config(type_it->second, settings);
    } else {
        builder.with_tokenizer_config(tokenzier_name, {});
    }

    // Process token filters
    auto token_filter_it = index_policy_analyzer.properties.find(PROP_TOKEN_FILTER);
    if (token_filter_it != index_policy_analyzer.properties.end()) {
        std::vector<std::string> token_filter_strs;
        boost::split(token_filter_strs, token_filter_it->second, boost::is_any_of(","));

        for (auto& filter_name : token_filter_strs) {
            boost::trim(filter_name);
            if (filter_name.empty()) {
                continue;
            }

            if (_name_to_id.contains(filter_name)) {
                // Nested token filter policy
                const auto& filter_policy = _policys[_name_to_id[filter_name]];
                auto type_it = filter_policy.properties.find(PROP_TYPE);
                if (type_it == filter_policy.properties.end()) {
                    throw Exception(ErrorCode::INVALID_ARGUMENT,
                                    "Invalid token filter configuration in policy: " + filter_name);
                }

                segment_v2::inverted_index::Settings settings;
                for (const auto& prop : filter_policy.properties) {
                    if (prop.first != PROP_TYPE) {
                        settings.set(prop.first, prop.second);
                    }
                }
                builder.add_token_filter_config(type_it->second, settings);
            } else {
                // Simple token filter
                builder.add_token_filter_config(filter_name, {});
            }
        }
    }

    auto custom_analyzer_config = builder.build();
    return segment_v2::inverted_index::CustomAnalyzer::build_custom_analyzer(
            custom_analyzer_config);
}

} // namespace doris