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
#include <utility>
#include <vector>

#include "storage/binlog.h"
#include "storage/tablet/tablet_schema.h"

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

using RowBinlogValueColumnPair = std::pair<uint32_t, uint32_t>;

inline bool row_binlog_value_columns_have_same_type(const TabletColumn& lhs,
                                                    const TabletColumn& rhs) {
    if (lhs.type() != rhs.type() || lhs.is_nullable() != rhs.is_nullable() ||
        lhs.length() != rhs.length() || lhs.precision() != rhs.precision() ||
        lhs.frac() != rhs.frac() || lhs.get_subtype_count() != rhs.get_subtype_count()) {
        return false;
    }
    for (uint32_t i = 0; i < lhs.get_subtype_count(); ++i) {
        if (!row_binlog_value_columns_have_same_type(lhs.get_sub_column(i),
                                                     rhs.get_sub_column(i))) {
            return false;
        }
    }
    return true;
}

// IColumn::compare_at is unavailable for opaque aggregate-state families. FLOAT/DOUBLE are
// excluded as well because their ordering comparison treats +0 and -0 as equal even though their
// stored row states differ. Complex columns inherit the capability of all nested children.
inline bool supports_min_delta_value_comparison(const TabletColumn& column) {
    switch (column.type()) {
    case FieldType::OLAP_FIELD_TYPE_FLOAT:
    case FieldType::OLAP_FIELD_TYPE_DOUBLE:
    case FieldType::OLAP_FIELD_TYPE_DISCRETE_DOUBLE:
    case FieldType::OLAP_FIELD_TYPE_HLL:
    case FieldType::OLAP_FIELD_TYPE_BITMAP:
    case FieldType::OLAP_FIELD_TYPE_QUANTILE_STATE:
    case FieldType::OLAP_FIELD_TYPE_VARIANT:
    case FieldType::OLAP_FIELD_TYPE_AGG_STATE:
    case FieldType::OLAP_FIELD_TYPE_UNKNOWN:
    case FieldType::OLAP_FIELD_TYPE_NONE:
        return false;
    case FieldType::OLAP_FIELD_TYPE_ARRAY:
    case FieldType::OLAP_FIELD_TYPE_MAP:
    case FieldType::OLAP_FIELD_TYPE_STRUCT:
        if (column.get_subtype_count() == 0) {
            return false;
        }
        for (const auto& child : column.get_sub_columns()) {
            if (!supports_min_delta_value_comparison(*child)) {
                return false;
            }
        }
        return true;
    default:
        return true;
    }
}

// Row-binlog schemas place all AFTER values before all BEFORE values. Metadata can be either a
// prefix (legacy layout) or a suffix, so remove it by its schema ids, then split the remaining
// non-key columns in half. Resolving pairs by physical ordinal avoids ambiguity when a user column
// name itself looks like a generated __BEFORE__ name.
inline bool get_row_binlog_value_column_pairs(const TabletSchema& schema,
                                              std::vector<RowBinlogValueColumnPair>* pairs) {
    pairs->clear();
    std::vector<uint32_t> value_column_ids;
    value_column_ids.reserve(schema.num_columns() - schema.num_key_columns());
    for (uint32_t cid = 0; cid < schema.num_columns(); ++cid) {
        if (static_cast<int32_t>(cid) == schema.binlog_tso_col_idx() ||
            static_cast<int32_t>(cid) == schema.binlog_lsn_col_idx() ||
            static_cast<int32_t>(cid) == schema.binlog_op_col_idx() ||
            schema.column(cid).is_key()) {
            continue;
        }
        value_column_ids.push_back(cid);
    }

    if (value_column_ids.empty() || value_column_ids.size() % 2 != 0) {
        return false;
    }
    const size_t value_column_count = value_column_ids.size() / 2;
    pairs->reserve(value_column_count);
    for (size_t i = 0; i < value_column_count; ++i) {
        const uint32_t after_cid = value_column_ids[i];
        const uint32_t before_cid = value_column_ids[i + value_column_count];
        const auto& after = schema.column(after_cid);
        const auto& before = schema.column(before_cid);
        if (before.name() != build_before_column_name(after.name()) ||
            !row_binlog_value_columns_have_same_type(after, before)) {
            pairs->clear();
            return false;
        }
        pairs->emplace_back(after_cid, before_cid);
    }
    return true;
}

inline bool supports_complete_min_delta_value_comparison(
        const TabletSchema& schema, const std::vector<RowBinlogValueColumnPair>& pairs) {
    if (pairs.empty()) {
        return false;
    }
    for (const auto& [after_cid, before_cid] : pairs) {
        if (!supports_min_delta_value_comparison(schema.column(after_cid)) ||
            !supports_min_delta_value_comparison(schema.column(before_cid))) {
            return false;
        }
    }
    return true;
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
