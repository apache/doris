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

#include "runtime/index_policy/index_policy_mgr.h"

#include <algorithm>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <unordered_set>
#include <utility>

namespace doris {
namespace {

class PurposeInsensitiveAnalyzerProvider final
        : public segment_v2::inverted_index::AnalyzerProvider {
public:
    explicit PurposeInsensitiveAnalyzerProvider(AnalyzerPtr analyzer)
            : _analyzer(std::move(analyzer)) {}

    AnalyzerPtr get_analyzer(
            segment_v2::inverted_index::AnalysisPurpose /*purpose*/) const override {
        return _analyzer;
    }

private:
    const AnalyzerPtr _analyzer;
};

} // namespace

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
            _name_to_id.erase(normalize_name(it->second.name));
            _policys.erase(it);
            ++success_deletes;
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
        std::string normalized_name = normalize_name(policy.name);
        if (_name_to_id.contains(normalized_name)) {
            LOG(ERROR) << "Reject update - Duplicate policy name: " << policy.name
                       << " | Existing ID: " << _name_to_id[normalized_name]
                       << " | New ID: " << policy.id;
            continue;
        }

        _policys.emplace(policy.id, policy);
        _name_to_id.emplace(normalized_name, policy.id);
        ++success_updates;
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

AnalyzerPtr IndexPolicyMgr::get_analyzer_by_name(
        const std::string& name, segment_v2::inverted_index::AnalysisPurpose purpose) {
    std::shared_lock lock(_mutex);
    const std::string normalized_name = normalize_name(name);
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
    if (policy_it->second.type == TIndexPolicyType::ANALYZER) {
        return build_analyzer_provider_from_config(
                       build_analyzer_config_from_policy(policy_it->second), {})
                ->get_analyzer(purpose);
    }
    if (policy_it->second.type == TIndexPolicyType::NORMALIZER) {
        return build_normalizer_from_policy(policy_it->second);
    }
    throw Exception(ErrorCode::INVALID_ARGUMENT, "Analyzer policy not found: " + name);
}

AnalyzerProviderPtr IndexPolicyMgr::get_analyzer_provider_by_name(
        const std::string& name, const std::map<std::string, std::string>& outer_char_filter_map) {
    std::shared_lock lock(_mutex);
    const std::string normalized_name = normalize_name(name);
    auto name_it = _name_to_id.find(normalized_name);
    if (name_it == _name_to_id.end()) {
        if (is_builtin_normalizer(normalized_name)) {
            return std::make_shared<PurposeInsensitiveAnalyzerProvider>(
                    build_builtin_normalizer(name));
        }
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Policy not found with name: " + name);
    }
    auto policy_it = _policys.find(name_it->second);
    if (policy_it == _policys.end()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Policy not found with id: " + name);
    }
    if (policy_it->second.type == TIndexPolicyType::ANALYZER) {
        return build_analyzer_provider_from_config(
                build_analyzer_config_from_policy(policy_it->second), outer_char_filter_map);
    }
    if (policy_it->second.type == TIndexPolicyType::NORMALIZER) {
        return std::make_shared<PurposeInsensitiveAnalyzerProvider>(
                build_normalizer_from_policy(policy_it->second));
    }
    throw Exception(ErrorCode::INVALID_ARGUMENT, "Analyzer policy not found: " + name);
}

AnalyzerProviderPtr IndexPolicyMgr::get_analyzer_provider_by_base_fingerprint(
        std::string_view base_analyzer_fingerprint,
        const std::map<std::string, std::string>& outer_char_filter_map) {
    std::shared_lock lock(_mutex);
    for (const auto& [_, policy] : _policys) {
        if (policy.type != TIndexPolicyType::ANALYZER) {
            continue;
        }
        auto config = build_analyzer_config_from_policy(policy);
        if (segment_v2::inverted_index::CustomAnalyzerProvider::calculate_base_analyzer_fingerprint(
                    config, outer_char_filter_map) != base_analyzer_fingerprint) {
            continue;
        }
        return build_analyzer_provider_from_config(std::move(config), outer_char_filter_map);
    }
    return nullptr;
}

segment_v2::inverted_index::CustomAnalyzerConfigPtr
IndexPolicyMgr::build_analyzer_config_from_policy(const TIndexPolicy& index_policy_analyzer) {
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

    return builder.build();
}

AnalyzerProviderPtr IndexPolicyMgr::build_analyzer_provider_from_config(
        segment_v2::inverted_index::CustomAnalyzerConfigPtr config,
        const std::map<std::string, std::string>& outer_char_filter_map) {
    // One shape for every policy: the provider sources its CommonGrams word list from the
    // BE-local default, so there is no per-policy word set to look up and no "not yet prepared"
    // state to represent.
    return std::make_shared<segment_v2::inverted_index::CustomAnalyzerProvider>(
            std::move(config), outer_char_filter_map);
}

AnalyzerPtr IndexPolicyMgr::build_analyzer_from_policy(const TIndexPolicy& index_policy_analyzer) {
    return build_analyzer_provider_from_config(
                   build_analyzer_config_from_policy(index_policy_analyzer), {})
            ->get_analyzer(segment_v2::inverted_index::AnalysisPurpose::kIndex);
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
            const int64_t filter_policy_id = _name_to_id.at(normalized_filter_name);
            const auto& filter_policy = _policys.at(filter_policy_id);
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