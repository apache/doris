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

#include <array>
#include <cstddef>
#include <cstdint>

#include "storage/changelog_types.h"

namespace doris::changelog {

enum class MinDeltaResultType { SKIP, INSERT, DELETE, UPDATE_BEFORE_AFTER };

// MIN_DELTA uses changelog op codes as indices into a 2D lookup table, so we guard the op layout here.
static_assert(static_cast<int64_t>(ChangelogOp::APPEND) == 0 &&
                      static_cast<int64_t>(ChangelogOp::UPDATE) == 1 &&
                      static_cast<int64_t>(ChangelogOp::DELETE) == 2,
              "changelog op layout changed; update min-delta transition matrix");

inline bool is_valid_changelog_op(ChangelogOp op) {
    const auto value = static_cast<int64_t>(op);
    return value >= static_cast<int64_t>(doris::ChangelogOp::APPEND) &&
           value <= static_cast<int64_t>(doris::ChangelogOp::DELETE);
}

inline MinDeltaResultType calculate_min_delta_result(ChangelogOp first_op, ChangelogOp last_op) {
    using ResultType = MinDeltaResultType;

    // Transition matrix: row=first_op, col=last_op, value=min-delta result type.
    // Column order is fixed as [APPEND, UPDATE, DELETE].
    //
    // Semantic examples:
    // 1) APPEND -> DELETE = SKIP:
    //    Insert then delete within the same window yields no visible change.
    // 2) UPDATE -> DELETE = DELETE:
    //    Update then delete; downstream only needs the pre-delete snapshot.
    // 3) DELETE -> APPEND/UPDATE = UPDATE_BEFORE_AFTER:
    //    A DELETE as the first op means the key already existed before this window, so deleting
    //    then re-adding it is an update of the pre-existing row (BEFORE = the deleted row's old
    //    value), not a fresh insert.
    static constexpr std::array<std::array<ResultType, 3>, 3> kTransitionMatrix = {{
            // first_op = APPEND
            {ResultType::INSERT, ResultType::INSERT, ResultType::SKIP},
            // first_op = UPDATE
            {ResultType::UPDATE_BEFORE_AFTER, ResultType::UPDATE_BEFORE_AFTER, ResultType::DELETE},
            // first_op = DELETE
            {ResultType::UPDATE_BEFORE_AFTER, ResultType::UPDATE_BEFORE_AFTER, ResultType::DELETE},
    }};

    // Fallback for unknown/invalid op codes: avoid out-of-bounds and keep changes conservatively.
    if (!is_valid_changelog_op(first_op) || !is_valid_changelog_op(last_op)) {
        return ResultType::UPDATE_BEFORE_AFTER;
    }

    return kTransitionMatrix[static_cast<size_t>(first_op)][static_cast<size_t>(last_op)];
}

/**
 * MIN_DELTA result helper:
 * Given the first/last changelog op within the same key group, returns the min-delta change type.
 */
class AggregateFunctionMinDelta {
public:
    using ResultType = MinDeltaResultType;

    static ResultType calculate_result(ChangelogOp first_op, ChangelogOp last_op) {
        return calculate_min_delta_result(first_op, last_op);
    }
};

} // namespace doris::changelog
