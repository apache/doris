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

#include <concepts>
#include <cstdint>
#include <memory>

namespace doris::index_query {

// Query statistics and encoded-norm interpretation belong to the supplied context.
// The score type preserves each adapter's arithmetic precision.
template <std::floating_point Score>
class ScoringContext {
public:
    virtual ~ScoringContext() = default;
    virtual Score score(Score frequency, int64_t encoded_norm) = 0;
    // Returns a conservative upper bound for postings scored by this context.
    virtual Score max_score() = 0;
};

template <std::floating_point Score>
using ScoringContextPtr = std::shared_ptr<ScoringContext<Score>>;

} // namespace doris::index_query
