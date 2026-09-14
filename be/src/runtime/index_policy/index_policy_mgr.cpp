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

class SingleAnalyzerProvider final : public segment_v2::inverted_index::AnalyzerProvider {
public:
    explicit SingleAnalyzerProvider(AnalyzerPtr analyzer) : _analyzer(std::move(analyzer)) {}

    AnalyzerPtr get_analyzer() const override { return _analyzer; }

private:
    const AnalyzerPtr _analyzer;
};

} // namespace

const std::unordered_set<std::string> IndexPolicyMgr::BUILTIN_NORMALIZERS = {"lowercase"};

std::string IndexPolicyMgr::trim_name(const std::string& name) {
    std::string result = name;
    boost::algorithm::trim(result);
    return result;
}

std::string IndexPolicyMgr::normalize_name(const std::string& name) {
    std::string result = trim_name(name);
    boost::algorithm::to_lower(result);
    return result;
}

const TIndexPolicy* IndexPolicyMgr::find_policy_by_name_locked(const std::string& name) const {
    const std::string exact_name = trim_name(name);
    if (auto exact_it = _exact_name_to_id.find(exact_name); exact_it != _exact_name_to_id.end()) {
        if (auto policy_it = _policys.find(exact_it->second); policy_it != _policys.end()) {
            return &policy_it->second;
        }
    }

    const std::string normalized_name = normalize_name(name);
    if (auto normalized_it = _name_to_id.find(normalized_name);
        normalized_it != _name_to_id.end()) {
        if (auto policy_it = _policys.find(normalized_it->second); policy_it != _policys.end()) {
            return &policy_it->second;
        }
    }
    return nullptr;
}

void IndexPolicyMgr::register_policy_name_locked(const TIndexPolicy& policy) {
    const std::string exact_name = trim_name(policy.name);
    if (auto exact_it = _exact_name_to_id.find(exact_name);
        exact_it == _exact_name_to_id.end() || policy.id > exact_it->second) {
        _exact_name_to_id[exact_name] = policy.id;
    }

    const std::string normalized_name = normalize_name(policy.name);
    if (auto normalized_it = _name_to_id.find(normalized_name);
        normalized_it != _name_to_id.end()) {
        LOG(WARNING) << "Policies have the same normalized name: " << policy.name
                     << " | Existing authoritative ID: " << normalized_it->second
                     << " | New ID: " << policy.id << " | The higher ID is authoritative";
    }
    if (!_name_to_id.contains(normalized_name) || policy.id > _name_to_id.at(normalized_name)) {
        _name_to_id[normalized_name] = policy.id;
    }
}

void IndexPolicyMgr::unregister_policy_name_locked(const TIndexPolicy& policy) {
    const std::string exact_name = trim_name(policy.name);
    const std::string normalized_name = normalize_name(policy.name);
    if (_exact_name_to_id.contains(exact_name) && _exact_name_to_id.at(exact_name) == policy.id) {
        _exact_name_to_id.erase(exact_name);
    }
    if (_name_to_id.contains(normalized_name) && _name_to_id.at(normalized_name) == policy.id) {
        _name_to_id.erase(normalized_name);
    }
    for (const auto& [remaining_id, remaining] : _policys) {
        if (trim_name(remaining.name) == exact_name ||
            normalize_name(remaining.name) == normalized_name) {
            register_policy_name_locked(remaining);
        }
    }
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
            const TIndexPolicy policy = it->second;
            _policys.erase(it);
            unregister_policy_name_locked(policy);
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
        _policys.emplace(policy.id, policy);
        register_policy_name_locked(policy);
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

// Hold the lock throughout nested policy resolution so an analyzer observes a consistent
// policy-name mapping and policy set.
AnalyzerPtr IndexPolicyMgr::get_policy_by_name(const std::string& name) {
    std::shared_lock lock(_mutex);

    std::string normalized_name = normalize_name(name);
    const auto* index_policy = find_policy_by_name_locked(name);
    if (index_policy == nullptr) {
        if (is_builtin_normalizer(normalized_name)) {
            return build_builtin_normalizer(name);
        }
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Policy not found with name: " + name);
    }

    if (index_policy->type == TIndexPolicyType::ANALYZER) {
        return build_analyzer_from_policy(*index_policy);
    } else if (index_policy->type == TIndexPolicyType::NORMALIZER) {
        return build_normalizer_from_policy(*index_policy);
    }

    throw Exception(ErrorCode::INVALID_ARGUMENT, "Policy not found with type: " + name);
}

AnalyzerPtr IndexPolicyMgr::get_analyzer_by_name(const std::string& name) {
    std::shared_lock lock(_mutex);
    const std::string normalized_name = normalize_name(name);
    const auto* index_policy = find_policy_by_name_locked(name);
    if (index_policy == nullptr) {
        if (is_builtin_normalizer(normalized_name)) {
            return build_builtin_normalizer(name);
        }
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Policy not found with name: " + name);
    }
    if (index_policy->type == TIndexPolicyType::ANALYZER) {
        return build_analyzer_provider_from_config(build_analyzer_config_from_policy(*index_policy),
                                                   {})
                ->get_analyzer();
    }
    if (index_policy->type == TIndexPolicyType::NORMALIZER) {
        return build_normalizer_from_policy(*index_policy);
    }
    throw Exception(ErrorCode::INVALID_ARGUMENT, "Analyzer policy not found: " + name);
}

AnalyzerProviderPtr IndexPolicyMgr::get_analyzer_provider_by_name(
        const std::string& name, const std::map<std::string, std::string>& outer_char_filter_map) {
    std::shared_lock lock(_mutex);
    const std::string normalized_name = normalize_name(name);
    const auto* index_policy = find_policy_by_name_locked(name);
    if (index_policy == nullptr) {
        if (is_builtin_normalizer(normalized_name)) {
            return std::make_shared<SingleAnalyzerProvider>(build_builtin_normalizer(name));
        }
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Policy not found with name: " + name);
    }
    if (index_policy->type == TIndexPolicyType::ANALYZER) {
        return build_analyzer_provider_from_config(build_analyzer_config_from_policy(*index_policy),
                                                   outer_char_filter_map);
    }
    if (index_policy->type == TIndexPolicyType::NORMALIZER) {
        return std::make_shared<SingleAnalyzerProvider>(
                build_normalizer_from_policy(*index_policy));
    }
    throw Exception(ErrorCode::INVALID_ARGUMENT, "Analyzer policy not found: " + name);
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
    std::string normalized_tokenizer_name = normalize_name(tokenizer_name);
    if (const auto* tokenizer_policy = find_policy_by_name_locked(tokenizer_name);
        tokenizer_policy != nullptr) {
        auto type_it = tokenizer_policy->properties.find(PROP_TYPE);
        if (type_it == tokenizer_policy->properties.end()) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Invalid tokenizer configuration in policy: " + tokenizer_name);
        }

        segment_v2::inverted_index::Settings settings;
        for (const auto& prop : tokenizer_policy->properties) {
            if (prop.first != PROP_TYPE) {
                settings.set(prop.first, prop.second);
            }
        }
        builder.with_tokenizer_config(type_it->second, settings);
    } else {
        builder.with_tokenizer_config(normalized_tokenizer_name, {});
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
    return std::make_shared<segment_v2::inverted_index::CustomAnalyzerProvider>(
            std::move(config), outer_char_filter_map);
}

AnalyzerPtr IndexPolicyMgr::build_analyzer_from_policy(const TIndexPolicy& index_policy_analyzer) {
    return build_analyzer_provider_from_config(
                   build_analyzer_config_from_policy(index_policy_analyzer), {})
            ->get_analyzer();
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

        std::string normalized_filter_name = normalize_name(filter_name);
        if (const auto* filter_policy = find_policy_by_name_locked(filter_name);
            filter_policy != nullptr) {
            // Nested filter policy
            auto type_it = filter_policy->properties.find(PROP_TYPE);
            if (type_it == filter_policy->properties.end()) {
                throw Exception(
                        ErrorCode::INVALID_ARGUMENT,
                        "Invalid " + error_prefix + " configuration in policy: " + filter_name);
            }

            segment_v2::inverted_index::Settings settings;
            for (const auto& prop : filter_policy->properties) {
                if (prop.first != PROP_TYPE) {
                    settings.set(prop.first, prop.second);
                }
            }
            add_config_func(type_it->second, settings);
        } else {
            // Simple filter
            add_config_func(normalized_filter_name, {});
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
