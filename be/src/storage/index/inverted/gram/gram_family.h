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

#include <map>
#include <optional>
#include <string>

#include "storage/index/inverted/gram/gram_scheme.h"

namespace doris {
class IndexPolicyMgr;
}

namespace doris::segment_v2::gram {

// Resolve index properties -> analyzer/normalizer name -> policy -> gram scheme, so that a
// caller that only has the index properties and cannot reach an analyzer provider (the query
// side) can decide whether an index belongs to the "gram family" (an ngram tokenizer carrying a
// mode property) and obtain its GramScheme parameters.
//
// nullopt is returned when: the analyzer name is empty, the name is a built-in analyzer
// (standard/english/... -- these names never reach the policy manager and are handled by
// InvertedIndexAnalyzer::create_analyzer_provider itself), the policy manager is not ready, or
// the policy exists but is not in the gram family.
//
// An exception is thrown when the name is neither a built-in analyzer nor found in the policy
// manager: IndexPolicyMgr::get_analyzer_provider_by_name then throws "Policy not found". This
// function does not swallow it -- that is a genuine configuration error, and the caller (the
// query side in phase C) decides whether to fail or to fall back to a full scan.
//
// Note: the write side (the SNII writer) does not come through here. It creates the analyzer
// provider itself and simply asks that provider for gram_scheme(), which saves one policy
// resolution and guarantees that "the analyzer actually used for tokenization" and "the analyzer
// judged to be gram family" are always the same object (Ruling R21).
std::optional<GramScheme> resolve_gram_scheme(
        const std::map<std::string, std::string>& index_properties, IndexPolicyMgr* mgr);

} // namespace doris::segment_v2::gram
