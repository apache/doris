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

std::optional<GramScheme> resolve_gram_scheme(
        const std::map<std::string, std::string>& index_properties, IndexPolicyMgr* mgr) {
    const std::string name = get_analyzer_name_from_properties(index_properties);
    // Built-in analyzer names (standard/english/unicode/chinese/icu/...) must short-circuit
    // before the policy manager is touched: they are never registered as policies, and
    // get_analyzer_provider_by_name would throw "Policy not found" straight away. The test
    // reuses InvertedIndexAnalyzer::is_builtin_analyzer -- the same predicate
    // create_analyzer_provider uses, so the two sides cannot drift on "what counts as a built-in
    // name".
    if (name.empty() || inverted_index::InvertedIndexAnalyzer::is_builtin_analyzer(name) ||
        mgr == nullptr) {
        return std::nullopt;
    }
    // get_analyzer_provider_by_name throws when the policy is missing (it never returns
    // nullptr); see the contract in the header.
    return mgr->get_analyzer_provider_by_name(name)->gram_scheme();
}

} // namespace doris::segment_v2::gram
