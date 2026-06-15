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
#include <string>
#include <string_view>

#include "core/block/block.h"
#include "storage/binlog.h"

namespace doris::binlog {

constexpr int64_t ROW_BINLOG_UNKNOWN = 3;

constexpr int64_t STREAM_CHANGE_INSERT = 0;
constexpr int64_t STREAM_CHANGE_DELETE = 1;
constexpr int64_t STREAM_CHANGE_UPDATE_BEFORE = 2;
constexpr int64_t STREAM_CHANGE_UPDATE_AFTER = 3;

// Build the __BEFORE__ column name for a base column.
inline std::string build_before_column_name(std::string_view name) {
    std::string before_name = "__BEFORE__";
    before_name.append(name.data(), name.size());
    before_name.append("__");
    return before_name;
}

// Resolve __BEFORE__ column index for a base column when present.
inline int resolve_before_column_index(const doris::Block& block, int idx, int binlog_op_pos) {
    if (idx == binlog_op_pos) {
        return idx;
    }

    const auto& col_with_name = block.get_by_position(idx);
    std::string before_name = build_before_column_name(col_with_name.name);
    int tmp_idx = block.get_position_by_name(before_name);
    return tmp_idx < 0 ? idx : tmp_idx;
}

enum class MinDeltaResultType { SKIP, INSERT, DELETE, UPDATE_BEFORE_AFTER };

// MIN_DELTA uses row binlog op codes as indices into a 2D lookup table, so we guard the op layout here.
static_assert(doris::ROW_BINLOG_APPEND == 0 && doris::ROW_BINLOG_UPDATE == 1 &&
                      doris::ROW_BINLOG_DELETE == 2,
              "row binlog op layout changed; update min-delta transition matrix");

inline bool is_valid_row_binlog_op(int64_t op) {
    return op >= doris::ROW_BINLOG_APPEND && op <= doris::ROW_BINLOG_DELETE;
}

inline MinDeltaResultType calculate_min_delta_result(int64_t first_op, int64_t last_op) {
    using ResultType = MinDeltaResultType;

    // Transition matrix: row=first_op, col=last_op, value=min-delta result type.
    // Column order is fixed as [APPEND, UPDATE, DELETE].
    //
    // Semantic examples:
    // 1) APPEND -> DELETE = SKIP:
    //    Insert then delete within the same window yields no visible change.
    // 2) UPDATE -> DELETE = DELETE:
    //    Update then delete; downstream only needs the pre-delete snapshot.
    // 3) DELETE -> APPEND = INSERT:
    //    Delete then append (rebuild) is equivalent to inserting a new value.
    static constexpr std::array<std::array<ResultType, 3>, 3> kTransitionMatrix = {{
            // first_op = APPEND
            {ResultType::INSERT, ResultType::INSERT, ResultType::SKIP},
            // first_op = UPDATE
            {ResultType::UPDATE_BEFORE_AFTER, ResultType::UPDATE_BEFORE_AFTER, ResultType::DELETE},
            // first_op = DELETE
            {ResultType::INSERT, ResultType::INSERT, ResultType::DELETE},
    }};

    // Fallback for unknown/invalid op codes: avoid out-of-bounds and keep changes conservatively.
    if (!is_valid_row_binlog_op(first_op) || !is_valid_row_binlog_op(last_op)) {
        return ResultType::UPDATE_BEFORE_AFTER;
    }

    return kTransitionMatrix[static_cast<size_t>(first_op)][static_cast<size_t>(last_op)];
}

/**
 * MIN_DELTA result helper:
 * Given the first/last row binlog op within the same key group, returns the min-delta change type.
 */
class AggregateFunctionMinDelta {
public:
    using ResultType = MinDeltaResultType;

    static ResultType calculate_result(int64_t first_op, int64_t last_op) {
        return calculate_min_delta_result(first_op, last_op);
    }
};

} // namespace doris::binlog