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

#include <cstddef>
#include <map>
#include <string>
#include <vector>

namespace doris::segment_v2::inverted_index::golden {

enum class GoldenKind {
    ANALYZE,   // the tokens an index writes for a value
    NORMALIZE, // the term a prefix or wildcard query normalizes its text to
};

// A policy as FE sends it to BE, which a configuration needs registered in IndexPolicyMgr.
struct GoldenPolicy {
    std::string name;
    TIndexPolicyType::type type;
    std::map<std::string, std::string> properties;
};

// One analyzer configuration, described the way users describe it: the index properties, and the
// analyzer, normalizer, tokenizer and filter policies they name. Nothing here names the analyzer
// API, so the matrix stays as it is while the implementation behind it changes.
struct GoldenConfig {
    std::string name; // part of the keys in the golden files; never renamed
    GoldenKind kind = GoldenKind::ANALYZE;
    std::map<std::string, std::string> properties;
    std::vector<GoldenPolicy> policies;
};

// The configuration matrix of the golden tests.
std::vector<GoldenConfig> golden_configs();

// The properties and policies of a configuration, for failure messages.
std::string describe(const GoldenConfig& config);

// A covering array of strength two over `factors` two-valued factors: for every two factors, each
// of the four value combinations occurs in some row. Row 0 is all false; true means a factor is
// set to the value that is not its default.
std::vector<std::vector<bool>> pairwise_rows(size_t factors);

} // namespace doris::segment_v2::inverted_index::golden
