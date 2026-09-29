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

// Whether the index properties name an analyzer or normalizer policy rather than a built-in
// analyzer, a built-in normalizer or nothing; only such an index can be written as a gram index.
// It never resolves the policy.
bool may_be_gram_index(const std::map<std::string, std::string>& index_properties);

// Resolve a custom analyzer's gram scheme. Built-in and non-gram analyzers return no scheme.
// Unknown custom policies retain their normal lookup error.
std::optional<GramScheme> resolve_gram_scheme(
        const std::map<std::string, std::string>& index_properties, IndexPolicyMgr* mgr);

} // namespace doris::segment_v2::gram
