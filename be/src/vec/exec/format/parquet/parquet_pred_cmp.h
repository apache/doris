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

#include <cstring>
#include <vector>

#include "exec/olap_common.h"

namespace doris::vectorized {

#define _PLAIN_DECODE(T, value, min_bytes, max_bytes, out_value, out_min, out_max) \
    const T out_min = reinterpret_cast<const T*>(min_bytes)[0];                    \
    const T out_max = reinterpret_cast<const T*>(max_bytes)[0];                    \
    T out_value = *((T*)value);

#define _PLAIN_DECODE_SINGLE(T, value, bytes, conjunct_value, out) \
    const T out = reinterpret_cast<const T*>(bytes)[0];            \
    T conjunct_value = *((T*)value);

#define _FILTER_GROUP_BY_EQ_PRED(conjunct_value, min, max) \
    if (conjunct_value < min || conjunct_value > max) {    \
        return true;                                       \
    }

#define _FILTER_GROUP_BY_GT_PRED(conjunct_value, max) \
    if (max <= conjunct_value) {                      \
        return true;                                  \
    }

#define _FILTER_GROUP_BY_GE_PRED(conjunct_value, max) \
    if (max < conjunct_value) {                       \
        return true;                                  \
    }

#define _FILTER_GROUP_BY_LT_PRED(conjunct_value, min) \
    if (min >= conjunct_value) {                      \
        return true;                                  \
    }

#define _FILTER_GROUP_BY_LE_PRED(conjunct_value, min) \
    if (min > conjunct_value) {                       \
        return true;                                  \
    }

#define _FILTER_GROUP_BY_IN(T, in_pred_values, min_bytes, max_bytes)       \
    std::vector<T> in_values;                                              \
    for (auto val : in_pred_values) {                                      \
        T value = reinterpret_cast<T*>(val)[0];                            \
        in_values.emplace_back(value);                                     \
    }                                                                      \
    if (in_values.empty()) {                                               \
        return false;                                                      \
    }                                                                      \
    auto result = std::minmax_element(in_values.begin(), in_values.end()); \
    T in_min = *result.first;                                              \
    T in_max = *result.second;                                             \
    const T group_min = reinterpret_cast<const T*>(min_bytes)[0];          \
    const T group_max = reinterpret_cast<const T*>(max_bytes)[0];          \
    if (in_max < group_min || in_min > group_max) {                        \
        return true;                                                       \
    }

static bool _eval_in_val(PrimitiveType conjunct_type, std::vector<void*> in_pred_values,
                         const char* min_bytes, const char* max_bytes) {
    switch (conjunct_type) {
    case TYPE_TINYINT: {
        _FILTER_GROUP_BY_IN(int8_t, in_pred_values, min_bytes, max_bytes)
        break;
    }
    case TYPE_SMALLINT: {
        _FILTER_GROUP_BY_IN(int16_t, in_pred_values, min_bytes, max_bytes)
        break;
    }
    case TYPE_INT: {
        _FILTER_GROUP_BY_IN(int32_t, in_pred_values, min_bytes, max_bytes)
        break;
    }
    case TYPE_BIGINT: {
        _FILTER_GROUP_BY_IN(int64_t, in_pred_values, min_bytes, max_bytes)
        break;
    }
    case TYPE_STRING:
    case TYPE_VARCHAR:
    case TYPE_CHAR: {
        std::vector<const char*> in_values;
        for (auto val : in_pred_values) {
            const char* value = ((std::string*)val)->data();
            in_values.emplace_back(value);
        }
        if (in_values.empty()) {
            return false;
        }
        auto result = std::minmax_element(in_values.begin(), in_values.end());
        const char* in_min = *result.first;
        const char* in_max = *result.second;
        if (strcmp(in_max, min_bytes) < 0 || strcmp(in_min, max_bytes) > 0) {
            return true;
        }
        break;
    }
    default:
        return false;
    }
    return false;
}

static bool _eval_eq(PrimitiveType conjunct_type, void* value, const char* min_bytes,
                     const char* max_bytes) {
    switch (conjunct_type) {
    case TYPE_TINYINT: {
        _PLAIN_DECODE(int16_t, value, min_bytes, max_bytes, conjunct_value, min, max)
        _FILTER_GROUP_BY_EQ_PRED(conjunct_value, min, max)
        break;
    }
    case TYPE_SMALLINT: {
        _PLAIN_DECODE(int16_t, value, min_bytes, max_bytes, conjunct_value, min, max)
        _FILTER_GROUP_BY_EQ_PRED(conjunct_value, min, max)
        break;
    }
    case TYPE_INT: {
        _PLAIN_DECODE(int32_t, value, min_bytes, max_bytes, conjunct_value, min, max)
        _FILTER_GROUP_BY_EQ_PRED(conjunct_value, min, max)
        break;
    }
    case TYPE_BIGINT: {
        _PLAIN_DECODE(int64_t, value, min_bytes, max_bytes, conjunct_value, min, max)
        _FILTER_GROUP_BY_EQ_PRED(conjunct_value, min, max)
        break;
    }
    case TYPE_DOUBLE: {
        _PLAIN_DECODE(double, value, min_bytes, max_bytes, conjunct_value, min, max)
        _FILTER_GROUP_BY_EQ_PRED(conjunct_value, min, max)
        break;
    }
    case TYPE_FLOAT: {
        _PLAIN_DECODE(float, value, min_bytes, max_bytes, conjunct_value, min, max)
        _FILTER_GROUP_BY_EQ_PRED(conjunct_value, min, max)
        break;
    }
    case TYPE_STRING:
    case TYPE_VARCHAR:
    case TYPE_CHAR: {
        const char* conjunct_value = ((std::string*)value)->data();
        if (strcmp(conjunct_value, min_bytes) < 0 || strcmp(conjunct_value, max_bytes) > 0) {
            return true;
        }
        break;
    }
    default:
        return false;
    }
    return false;
}

static bool _eval_gt(PrimitiveType conjunct_type, void* value, const char* max_bytes) {
    switch (conjunct_type) {
    case TYPE_TINYINT: {
        _PLAIN_DECODE_SINGLE(int8_t, value, max_bytes, conjunct_value, max)
        _FILTER_GROUP_BY_GT_PRED(conjunct_value, max)
        break;
    }
    case TYPE_SMALLINT: {
        _PLAIN_DECODE_SINGLE(int16_t, value, max_bytes, conjunct_value, max)
        _FILTER_GROUP_BY_GT_PRED(conjunct_value, max)
        break;
    }
    case TYPE_INT: {
        _PLAIN_DECODE_SINGLE(int32_t, value, max_bytes, conjunct_value, max)
        _FILTER_GROUP_BY_GT_PRED(conjunct_value, max)
        break;
    }
    case TYPE_BIGINT: {
        _PLAIN_DECODE_SINGLE(int64_t, value, max_bytes, conjunct_value, max)
        _FILTER_GROUP_BY_GT_PRED(conjunct_value, max)
        break;
    }
    case TYPE_DOUBLE: {
        _PLAIN_DECODE_SINGLE(double, value, max_bytes, conjunct_value, max)
        _FILTER_GROUP_BY_GT_PRED(conjunct_value, max)
        break;
    }
    case TYPE_FLOAT: {
        _PLAIN_DECODE_SINGLE(float, value, max_bytes, conjunct_value, max)
        _FILTER_GROUP_BY_GT_PRED(conjunct_value, max)
        break;
    }
    case TYPE_STRING:
    case TYPE_VARCHAR:
    case TYPE_CHAR: {
        const char* conjunct_value = ((std::string*)value)->data();
        if (strcmp(max_bytes, conjunct_value) <= 0) {
            return true;
        }
        break;
    }
    default:
        return false;
    }
    return false;
}

static bool _eval_ge(PrimitiveType conjunct_type, void* value, const char* max_bytes) {
    switch (conjunct_type) {
    case TYPE_TINYINT: {
        _PLAIN_DECODE_SINGLE(int8_t, value, max_bytes, conjunct_value, max)
        _FILTER_GROUP_BY_GE_PRED(conjunct_value, max)
        break;
    }
    case TYPE_SMALLINT: {
        _PLAIN_DECODE_SINGLE(int16_t, value, max_bytes, conjunct_value, max)
        _FILTER_GROUP_BY_GE_PRED(conjunct_value, max)
        break;
    }
    case TYPE_INT: {
        _PLAIN_DECODE_SINGLE(int32_t, value, max_bytes, conjunct_value, max)
        _FILTER_GROUP_BY_GE_PRED(conjunct_value, max)
        break;
    }
    case TYPE_BIGINT: {
        _PLAIN_DECODE_SINGLE(int64_t, value, max_bytes, conjunct_value, max)
        _FILTER_GROUP_BY_GE_PRED(conjunct_value, max)
        break;
    }
    case TYPE_DOUBLE: {
        _PLAIN_DECODE_SINGLE(double, value, max_bytes, conjunct_value, max)
        _FILTER_GROUP_BY_GE_PRED(conjunct_value, max)
        break;
    }
    case TYPE_FLOAT: {
        _PLAIN_DECODE_SINGLE(float, value, max_bytes, conjunct_value, max)
        _FILTER_GROUP_BY_GE_PRED(conjunct_value, max)
        break;
    }
    case TYPE_STRING:
    case TYPE_VARCHAR:
    case TYPE_CHAR: {
        const char* conjunct_value = ((std::string*)value)->data();
        if (strcmp(max_bytes, conjunct_value) < 0) {
            return true;
        }
        break;
    }
    default:
        return false;
    }
    return false;
}

static bool _eval_lt(PrimitiveType conjunct_type, void* value, const char* min_bytes) {
    switch (conjunct_type) {
    case TYPE_TINYINT: {
        _PLAIN_DECODE_SINGLE(int8_t, value, min_bytes, conjunct_value, min)
        _FILTER_GROUP_BY_LT_PRED(conjunct_value, min)
        break;
    }
    case TYPE_SMALLINT: {
        _PLAIN_DECODE_SINGLE(int16_t, value, min_bytes, conjunct_value, min)
        _FILTER_GROUP_BY_LT_PRED(conjunct_value, min)
        break;
    }
    case TYPE_INT: {
        _PLAIN_DECODE_SINGLE(int32_t, value, min_bytes, conjunct_value, min)
        _FILTER_GROUP_BY_LT_PRED(conjunct_value, min)
        break;
    }
    case TYPE_BIGINT: {
        _PLAIN_DECODE_SINGLE(int64_t, value, min_bytes, conjunct_value, min)
        _FILTER_GROUP_BY_LT_PRED(conjunct_value, min)
        break;
    }
    case TYPE_DOUBLE: {
        _PLAIN_DECODE_SINGLE(double, value, min_bytes, conjunct_value, min)
        _FILTER_GROUP_BY_LT_PRED(conjunct_value, min)
        break;
    }
    case TYPE_FLOAT: {
        _PLAIN_DECODE_SINGLE(float, value, min_bytes, conjunct_value, min)
        _FILTER_GROUP_BY_LT_PRED(conjunct_value, min)
        break;
    }
    case TYPE_STRING:
    case TYPE_VARCHAR:
    case TYPE_CHAR: {
        const char* conjunct_value = ((std::string*)value)->data();
        if (strcmp(min_bytes, conjunct_value) >= 0) {
            return true;
        }
        break;
    }
    default:
        return false;
    }
    return false;
}

static bool _eval_le(PrimitiveType conjunct_type, void* value, const char* min_bytes) {
    switch (conjunct_type) {
    case TYPE_TINYINT: {
        _PLAIN_DECODE_SINGLE(int8_t, value, min_bytes, conjunct_value, min)
        _FILTER_GROUP_BY_LE_PRED(conjunct_value, min)
        break;
    }
    case TYPE_SMALLINT: {
        _PLAIN_DECODE_SINGLE(int16_t, value, min_bytes, conjunct_value, min)
        _FILTER_GROUP_BY_LE_PRED(conjunct_value, min)
        break;
    }
    case TYPE_INT: {
        _PLAIN_DECODE_SINGLE(int32_t, value, min_bytes, conjunct_value, min)
        _FILTER_GROUP_BY_LE_PRED(conjunct_value, min)
        break;
    }
    case TYPE_BIGINT: {
        _PLAIN_DECODE_SINGLE(int64_t, value, min_bytes, conjunct_value, min)
        _FILTER_GROUP_BY_LE_PRED(conjunct_value, min)
        break;
    }
    case TYPE_DOUBLE: {
        _PLAIN_DECODE_SINGLE(double, value, min_bytes, conjunct_value, min)
        _FILTER_GROUP_BY_LE_PRED(conjunct_value, min)
        break;
    }
    case TYPE_FLOAT: {
        _PLAIN_DECODE_SINGLE(float, value, min_bytes, conjunct_value, min)
        _FILTER_GROUP_BY_LE_PRED(conjunct_value, min)
        break;
    }
    case TYPE_STRING:
    case TYPE_VARCHAR:
    case TYPE_CHAR: {
        const char* conjunct_value = ((std::string*)value)->data();
        if (strcmp(min_bytes, conjunct_value) > 0) {
            return true;
        }
        break;
    }
    default:
        return false;
    }
    return false;
}

struct ScanPredicate {
    ScanPredicate() = default;
    ~ScanPredicate() = default;
    std::string _col_name;
    TExprOpcode::type _op;
    std::vector<void*> _values;
    bool _null_op = false;
    bool _is_null = false;
    int _scale;
};

template <PrimitiveType primitive_type>
static void to_filter(const ColumnValueRange<primitive_type>& col_val_range,
                      std::vector<ScanPredicate>& filters) {
    using CppType = typename PrimitiveTypeTraits<primitive_type>::CppType;
    const auto& high_value = col_val_range.get_range_max_value();
    const auto& low_value = col_val_range.get_range_min_value();
    const auto& high_op = col_val_range.get_range_high_op();
    const auto& low_op = col_val_range.get_range_low_op();

    // todo: process equals
    if (col_val_range.is_fixed_value_range()) {
        // 1. convert to in filter condition
        ScanPredicate condition;
        condition._col_name = col_val_range.column_name();
        condition._op = TExprOpcode::FILTER_NEW_IN;
        condition._scale = col_val_range.scale();
        if (col_val_range.get_fixed_value_set().empty()) {
            return;
        }
        for (const auto& value : col_val_range.get_fixed_value_set()) {
            condition._values.push_back(const_cast<CppType*>(&value));
        }
        filters.push_back(condition);
    } else if (low_value < high_value) {
        // 2. convert to min max filter condition
        ScanPredicate null_pred;
        if (col_val_range.is_high_value_maximum() && high_op == SQLFilterOp::FILTER_LESS_OR_EQUAL &&
            col_val_range.is_low_value_mininum() && low_op == SQLFilterOp::FILTER_LARGER_OR_EQUAL &&
            !col_val_range.contain_null()) {
            null_pred._col_name = col_val_range.column_name();
            null_pred._null_op = true;
            null_pred._is_null = false;
            filters.push_back(null_pred);
            return;
        }
        ScanPredicate low;
        if (!col_val_range.is_low_value_mininum() ||
            SQLFilterOp::FILTER_LARGER_OR_EQUAL != low_op) {
            low._col_name = col_val_range.column_name();
            low._op = (low_op == SQLFilterOp::FILTER_LARGER_OR_EQUAL ? TExprOpcode::GE
                                                                     : TExprOpcode::GT);
            low._values.push_back(const_cast<CppType*>(&low_value));
            low._scale = col_val_range.scale();
            filters.push_back(low);
        }

        ScanPredicate high;
        if (!col_val_range.is_high_value_maximum() ||
            SQLFilterOp::FILTER_LESS_OR_EQUAL != high_op) {
            high._col_name = col_val_range.column_name();
            high._op = (high_op == SQLFilterOp::FILTER_LESS_OR_EQUAL ? TExprOpcode::LE
                                                                     : TExprOpcode::LT);
            high._values.push_back(const_cast<CppType*>(&high_value));
            high._scale = col_val_range.scale();
            filters.push_back(high);
        }
    } else {
        // 3. convert to is null and is not null filter condition
        ScanPredicate null_pred;
        if (col_val_range.is_low_value_maximum() && col_val_range.is_high_value_mininum() &&
            col_val_range.contain_null()) {
            null_pred._col_name = col_val_range.column_name();
            null_pred._null_op = true;
            null_pred._is_null = true;
            filters.push_back(null_pred);
        }
    }
}

static void _eval_predicate(ScanPredicate filter, PrimitiveType col_type, const char* min_bytes,
                            const char* max_bytes, bool& need_filter) {
    if (filter._values.empty()) {
        return;
    }
    if (filter._op == TExprOpcode::FILTER_NEW_IN) {
        need_filter = _eval_in_val(col_type, filter._values, min_bytes, max_bytes);
        return;
    }
    // preserve TExprOpcode::FILTER_NEW_NOT_IN
    auto& value = filter._values[0];
    switch (filter._op) {
    case TExprOpcode::EQ:
        need_filter = _eval_eq(col_type, value, min_bytes, max_bytes);
        break;
    case TExprOpcode::NE:
        break;
    case TExprOpcode::GT:
        need_filter = _eval_gt(col_type, value, max_bytes);
        break;
    case TExprOpcode::GE:
        need_filter = _eval_ge(col_type, value, max_bytes);
        break;
    case TExprOpcode::LT:
        need_filter = _eval_lt(col_type, value, min_bytes);
        break;
    case TExprOpcode::LE:
        need_filter = _eval_le(col_type, value, min_bytes);
        break;
    default:
        break;
    }
}

static bool determine_filter_min_max(ColumnValueRangeType& col_val_range,
                                     const std::string& encoded_min,
                                     const std::string& encoded_max) {
    const char* min_bytes = encoded_min.data();
    const char* max_bytes = encoded_max.data();
    bool need_filter = false;
    std::vector<ScanPredicate> filters;
    PrimitiveType col_type;
    std::visit(
            [&](auto&& range) {
                col_type = range.type();
                to_filter(range, filters);
            },
            col_val_range);

    for (int i = 0; i < filters.size(); i++) {
        ScanPredicate filter = filters[i];
        _eval_predicate(filter, col_type, min_bytes, max_bytes, need_filter);
        if (need_filter) {
            break;
        }
    }
    return need_filter;
}

} // namespace doris::vectorized
