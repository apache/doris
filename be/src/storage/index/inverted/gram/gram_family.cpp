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

#include "storage/index/inverted/gram/gram_family.h"

#include "runtime/index_policy/index_policy_mgr.h"
#include "storage/index/inverted/analyzer/analyzer.h"
#include "storage/index/inverted/analyzer/analyzer_provider.h"
#include "storage/index/inverted/inverted_index_parser.h"

// This file (the sole exception under gram/) depends on the concrete runtime/index_policy and
// analyzer types, because the chain "resolve the analyzer name -> obtain the provider -> read its
// gram scheme" is runtime policy resolution and cannot be done without those dependencies
// (R8: every other file under gram/ stays free of any runtime dependency).

namespace doris::segment_v2::gram {

bool may_be_gram_index(const std::map<std::string, std::string>& index_properties) {
    const std::string name = get_analyzer_name_from_properties(index_properties);
    // The same built-in tests the analyzer and normalizer lookups use, so they agree on which
    // names are built in.
    return !name.empty() && !inverted_index::InvertedIndexAnalyzer::is_builtin_analyzer(name) &&
           !IndexPolicyMgr::is_builtin_normalizer(name);
}

std::optional<GramScheme> resolve_gram_scheme(
        const std::map<std::string, std::string>& index_properties, IndexPolicyMgr* mgr) {
    // Built-in names are never registered as policies, so they must not reach the policy
    // manager, whose lookup would throw "Policy not found".
    if (mgr == nullptr || !may_be_gram_index(index_properties)) {
        return std::nullopt;
    }
    // get_analyzer_provider_by_name throws when the policy is missing (it never returns
    // nullptr); see the contract in the header.
    return mgr->get_analyzer_provider_by_name(get_analyzer_name_from_properties(index_properties))
            ->gram_scheme();
}

} // namespace doris::segment_v2::gram
