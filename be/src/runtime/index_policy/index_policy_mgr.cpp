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
#include <unordered_set>
#include <utility>

namespace doris {

const std::unordered_set<std::string> IndexPolicyMgr::BUILTIN_NORMALIZERS = {"lowercase"};

std::string IndexPolicyMgr::normalize_name(const std::string& name) {
    std::string result = name;
    boost::algorithm::trim(result);
    boost::algorithm::to_lower(result);
    return result;
}

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

            // Use normalized name for deletion
            _name_to_id.erase(normalize_name(it->second.name));
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

        // Use normalized name for case-insensitive lookup
        std::string normalized_name = normalize_name(policy.name);
        if (_name_to_id.contains(normalized_name)) {
            LOG(ERROR) << "Reject update - Duplicate policy name: " << policy.name
                       << " | Existing ID: " << _name_to_id[normalized_name]
                       << " | New ID: " << policy.id;
            continue;
        }

        _policys.emplace(policy.id, policy);
        // Store with normalized key for case-insensitive lookup
        _name_to_id.emplace(normalized_name, policy.id);
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

Policys IndexPolicyMgr::get_index_policys() {
    std::shared_lock<std::shared_mutex> r_lock(_mutex);
    return _policys; // Return copy to ensure thread safety after lock release
}

// NOTE: This function holds a shared_lock while calling build_analyzer_from_policy/
// build_normalizer_from_policy, which also access _name_to_id and _policys.
// This is safe because std::shared_mutex allows the same thread to hold multiple
// shared_locks (read locks are reentrant). The lock is held throughout to ensure
// consistency when resolving nested policy references (e.g., tokenizer policies).
AnalyzerPtr IndexPolicyMgr::get_policy_by_name(const std::string& name) {
    std::shared_lock lock(_mutex);

    // Use normalized name for case-insensitive lookup
    std::string normalized_name = normalize_name(name);
    auto name_it = _name_to_id.find(normalized_name);
    if (name_it == _name_to_id.end()) {
        if (is_builtin_normalizer(normalized_name)) {
            return build_builtin_normalizer(name);
        }
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Policy not found with name: " + name);
    }

    auto policy_it = _policys.find(name_it->second);
    if (policy_it == _policys.end()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Policy not found with id: " + name);
    }

    const auto& index_policy = policy_it->second;
    if (index_policy.type == TIndexPolicyType::ANALYZER) {
        return build_analyzer_from_policy(index_policy);
    } else if (index_policy.type == TIndexPolicyType::NORMALIZER) {
        return build_normalizer_from_policy(index_policy);
    }

    throw Exception(ErrorCode::INVALID_ARGUMENT, "Policy not found with type: " + name);
}

AnalyzerPtr IndexPolicyMgr::build_analyzer_from_policy(const TIndexPolicy& index_policy_analyzer) {
    segment_v2::inverted_index::CustomAnalyzerConfig::Builder builder;

    auto tokenizer_it = index_policy_analyzer.properties.find(PROP_TOKENIZER);
    if (tokenizer_it == index_policy_analyzer.properties.end() || tokenizer_it->second.empty()) {
        throw Exception(
                ErrorCode::INVALID_ARGUMENT,
                "Invalid tokenizer configuration in policy: analyzer must have a tokenizer");
    }

    const auto& tokenizer_name = tokenizer_it->second;
    // Use normalized name for case-insensitive lookup
    std::string normalized_tokenizer_name = normalize_name(tokenizer_name);
    if (_name_to_id.contains(normalized_tokenizer_name)) {
        const auto& tokenizer_policy = _policys[_name_to_id[normalized_tokenizer_name]];
        auto type_it = tokenizer_policy.properties.find(PROP_TYPE);
        if (type_it == tokenizer_policy.properties.end()) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Invalid tokenizer configuration in policy: " + tokenizer_name);
        }

        segment_v2::inverted_index::Settings settings;
        for (const auto& prop : tokenizer_policy.properties) {
            if (prop.first != PROP_TYPE) {
                settings.set(prop.first, prop.second);
            }
        }
        builder.with_tokenizer_config(type_it->second, settings);
    } else {
        builder.with_tokenizer_config(tokenizer_name, {});
    }

    process_filter_configs(index_policy_analyzer, PROP_CHAR_FILTER, "char filter",
                           [&builder](const std::string& name,
                                      const segment_v2::inverted_index::Settings& settings) {
                               builder.add_char_filter_config(name, settings);
                           });

    process_filter_configs(index_policy_analyzer, PROP_TOKEN_FILTER, "token filter",
                           [&builder](const std::string& name,
                                      const segment_v2::inverted_index::Settings& settings) {
                               builder.add_token_filter_config(name, settings);
                           });

    auto custom_analyzer_config = builder.build();
    return segment_v2::inverted_index::CustomAnalyzer::build_custom_analyzer(
            custom_analyzer_config);
}

AnalyzerPtr IndexPolicyMgr::build_normalizer_from_policy(
        const TIndexPolicy& index_policy_normalizer) {
    segment_v2::inverted_index::CustomNormalizerConfig::Builder builder;

    process_filter_configs(index_policy_normalizer, PROP_CHAR_FILTER, "char filter",
                           [&builder](const std::string& name,
                                      const segment_v2::inverted_index::Settings& settings) {
                               builder.add_char_filter_config(name, settings);
                           });

    process_filter_configs(index_policy_normalizer, PROP_TOKEN_FILTER, "token filter",
                           [&builder](const std::string& name,
                                      const segment_v2::inverted_index::Settings& settings) {
                               builder.add_token_filter_config(name, settings);
                           });

    auto custom_normalizer_config = builder.build();
    return segment_v2::inverted_index::CustomNormalizer::build_custom_normalizer(
            custom_normalizer_config);
}

void IndexPolicyMgr::process_filter_configs(
        const TIndexPolicy& index_policy_analyzer, const std::string& prop_name,
        const std::string& error_prefix,
        std::function<void(const std::string&, const segment_v2::inverted_index::Settings&)>
                add_config_func) {
    auto filter_it = index_policy_analyzer.properties.find(prop_name);
    if (filter_it == index_policy_analyzer.properties.end()) {
        return;
    }

    std::vector<std::string> filter_strs;
    boost::split(filter_strs, filter_it->second, boost::is_any_of(","));

    for (auto& filter_name : filter_strs) {
        boost::trim(filter_name);
        if (filter_name.empty()) {
            continue;
        }

        // Use normalized name for case-insensitive lookup
        std::string normalized_filter_name = normalize_name(filter_name);
        if (_name_to_id.contains(normalized_filter_name)) {
            // Nested filter policy
            const auto& filter_policy = _policys[_name_to_id[normalized_filter_name]];
            auto type_it = filter_policy.properties.find(PROP_TYPE);
            if (type_it == filter_policy.properties.end()) {
                throw Exception(
                        ErrorCode::INVALID_ARGUMENT,
                        "Invalid " + error_prefix + " configuration in policy: " + filter_name);
            }

            segment_v2::inverted_index::Settings settings;
            for (const auto& prop : filter_policy.properties) {
                if (prop.first != PROP_TYPE) {
                    settings.set(prop.first, prop.second);
                }
            }
            add_config_func(type_it->second, settings);
        } else {
            // Simple filter
            add_config_func(filter_name, {});
        }
    }
}

bool IndexPolicyMgr::is_builtin_normalizer(const std::string& name) {
    return BUILTIN_NORMALIZERS.contains(name);
}

AnalyzerPtr IndexPolicyMgr::build_builtin_normalizer(const std::string& name) {
    using namespace segment_v2::inverted_index;

    if (name == "lowercase") {
        CustomNormalizerConfig::Builder builder;
        builder.add_token_filter_config("lowercase", Settings {});
        auto config = builder.build();
        return CustomNormalizer::build_custom_normalizer(config);
    }

    throw Exception(ErrorCode::INVALID_ARGUMENT, "Unknown builtin normalizer: " + name);
}

} // namespace doris