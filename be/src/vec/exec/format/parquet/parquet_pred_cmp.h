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

struct ColumnMinMaxParams {
    PrimitiveType conjunct_type;
    tparquet::Type::type parquet_type;
    void* value;
    // Use for decimal type
    int32_t parquet_precision;
    int32_t parquet_scale;
    int32_t parquet_type_length;
    // Use for in predicate
    std::vector<void*> in_pred_values;
    const char* min_bytes;
    const char* max_bytes;
};

template <typename T>
static void _align_decimal_v2_scale(T* conjunct_value, int32_t value_scale, T* parquet_value,
                                    int32_t parquet_scale) {
    if (value_scale > parquet_scale) {
        *parquet_value = *parquet_value * common::exp10_i32(value_scale - parquet_scale);
    } else if (value_scale < parquet_scale) {
        *conjunct_value = *conjunct_value * common::exp10_i32(parquet_scale - value_scale);
    }
}

template <typename T>
static void _decode_decimal_v2_to_primary(const ColumnMinMaxParams& params,
                                          const char* raw_parquet_val, T* out_value,
                                          T* parquet_val) {
    *parquet_val = reinterpret_cast<const T*>(raw_parquet_val)[0];
    DecimalV2Value conjunct_value = *((DecimalV2Value*)params.value);
    *out_value = conjunct_value.value();
    _align_decimal_v2_scale(out_value, conjunct_value.scale(), parquet_val, params.parquet_scale);
}

//  todo: support decimal128 after the test passes
//static Int128 _decode_value_to_int128(const ColumnMinMaxParams& params,
//                                      const char* raw_parquet_val) {
//    const uint8_t* buf = reinterpret_cast<const uint8_t*>(raw_parquet_val);
//    int32_t length = params.parquet_type_length;
//    Int128 value = buf[0] & 0x80 ? -1 : 0;
//    memcpy(reinterpret_cast<uint8_t*>(&value) + sizeof(value) - length, buf, length);
//    return BigEndian::ToHost128(value);
//}

static bool _eval_in_val(const ColumnMinMaxParams& params) {
    switch (params.conjunct_type) {
    case TYPE_TINYINT: {
        _FILTER_GROUP_BY_IN(int8_t, params.in_pred_values, params.min_bytes, params.max_bytes)
        break;
    }
    case TYPE_SMALLINT: {
        _FILTER_GROUP_BY_IN(int16_t, params.in_pred_values, params.min_bytes, params.max_bytes)
        break;
    }
    case TYPE_DECIMAL32:
    case TYPE_INT: {
        _FILTER_GROUP_BY_IN(int32_t, params.in_pred_values, params.min_bytes, params.max_bytes)
        break;
    }
    case TYPE_DECIMAL64:
    case TYPE_BIGINT: {
        _FILTER_GROUP_BY_IN(int64_t, params.in_pred_values, params.min_bytes, params.max_bytes)
        break;
    }
    case TYPE_DECIMALV2: {
        break;
    }
    case TYPE_STRING:
    case TYPE_VARCHAR:
    case TYPE_CHAR: {
        std::vector<const char*> in_values;
        for (auto val : params.in_pred_values) {
            std::string value = ((StringValue*)val)->to_string();
            in_values.emplace_back(value.data());
        }
        if (in_values.empty()) {
            return false;
        }
        auto result = std::minmax_element(in_values.begin(), in_values.end());
        const char* in_min = *result.first;
        const char* in_max = *result.second;
        if (strcmp(in_max, params.min_bytes) < 0 || strcmp(in_min, params.max_bytes) > 0) {
            return true;
        }
        break;
    }
    default:
        return false;
    }
    return false;
}

static bool _eval_eq(const ColumnMinMaxParams& params) {
    switch (params.conjunct_type) {
    case TYPE_TINYINT: {
        _PLAIN_DECODE(int16_t, params.value, params.min_bytes, params.max_bytes, conjunct_value,
                      min, max)
        _FILTER_GROUP_BY_EQ_PRED(conjunct_value, min, max)
        break;
    }
    case TYPE_SMALLINT: {
        _PLAIN_DECODE(int16_t, params.value, params.min_bytes, params.max_bytes, conjunct_value,
                      min, max)
        _FILTER_GROUP_BY_EQ_PRED(conjunct_value, min, max)
        break;
    }
    case TYPE_DECIMAL32:
    case TYPE_INT: {
        _PLAIN_DECODE(int32_t, params.value, params.min_bytes, params.max_bytes, conjunct_value,
                      min, max)
        _FILTER_GROUP_BY_EQ_PRED(conjunct_value, min, max)
        break;
    }
    case TYPE_DECIMAL64:
    case TYPE_BIGINT: {
        _PLAIN_DECODE(int64_t, params.value, params.min_bytes, params.max_bytes, conjunct_value,
                      min, max)
        _FILTER_GROUP_BY_EQ_PRED(conjunct_value, min, max)
        break;
    }
    case TYPE_DECIMALV2: {
        if (params.parquet_type == tparquet::Type::INT32) {
            int32_t min_value = reinterpret_cast<const int32_t*>(params.min_bytes)[0];
            int32_t max_value = reinterpret_cast<const int32_t*>(params.max_bytes)[0];
            DecimalV2Value conjunct_value = *((DecimalV2Value*)params.value);
            int32_t conjunct_int_value = conjunct_value.value();
            _align_decimal_v2_scale(&conjunct_int_value, conjunct_value.scale(), &min_value,
                                    params.parquet_scale);
            _align_decimal_v2_scale(&conjunct_int_value, conjunct_value.scale(), &max_value,
                                    params.parquet_scale);
            _FILTER_GROUP_BY_EQ_PRED(conjunct_int_value, min_value, max_value)
        } else if (params.parquet_type == tparquet::Type::INT64) {
            int64_t min_value = reinterpret_cast<const int64_t*>(params.min_bytes)[0];
            int64_t max_value = reinterpret_cast<const int64_t*>(params.max_bytes)[0];
            DecimalV2Value conjunct_value = *((DecimalV2Value*)params.value);
            int64_t conjunct_int_value = conjunct_value.value();
            _align_decimal_v2_scale(&conjunct_int_value, conjunct_value.scale(), &min_value,
                                    params.parquet_scale);
            _align_decimal_v2_scale(&conjunct_int_value, conjunct_value.scale(), &max_value,
                                    params.parquet_scale);
            _FILTER_GROUP_BY_EQ_PRED(conjunct_int_value, min_value, max_value)
        }
        break;
        //  When precision exceeds 18, decimal will use tparquet::Type::FIXED_LEN_BYTE_ARRAY to encode
        //  todo: support decimal128 after the test passes
        //        else if (params.parquet_type == tparquet::Type::FIXED_LEN_BYTE_ARRAY) {
        //            DecimalV2Value conjunct_value = *((DecimalV2Value*)params.value);
        //            Int128 conjunct_int_value = conjunct_value.value();
        //            Int128 max = _decode_value_to_int128(params, params.max_bytes);
        //            _align_decimal_v2_scale(&conjunct_int_value, conjunct_value.scale(), &max,
        //                                    params.parquet_scale);
        //            Int128 min = _decode_value_to_int128(params, params.min_bytes);
        //            _align_decimal_v2_scale(&conjunct_int_value, conjunct_value.scale(), &min,
        //                                    params.parquet_scale);
        //            _FILTER_GROUP_BY_EQ_PRED(conjunct_int_value, min, max)
        //        }
    }
    case TYPE_STRING:
    case TYPE_VARCHAR:
    case TYPE_CHAR: {
        std::string conjunct_value = ((StringValue*)params.value)->to_string();
        if (strcmp(conjunct_value.data(), params.min_bytes) < 0 ||
            strcmp(conjunct_value.data(), params.max_bytes) > 0) {
            return true;
        }
        break;
    }
    default:
        return false;
    }
    return false;
}

template <typename T>
static bool _filter_group_by_gt_or_ge(T conjunct_value, T max, bool is_ge) {
    if (!is_ge) {
        if (max <= conjunct_value) {
            return true;
        }
    } else {
        if (max < conjunct_value) {
            return true;
        }
    }
    return false;
}

static bool _eval_gt(const ColumnMinMaxParams& params, bool is_eq) {
    switch (params.conjunct_type) {
    case TYPE_TINYINT: {
        _PLAIN_DECODE_SINGLE(int8_t, params.value, params.max_bytes, conjunct_value, max)
        return _filter_group_by_gt_or_ge(conjunct_value, max, is_eq);
    }
    case TYPE_SMALLINT: {
        _PLAIN_DECODE_SINGLE(int16_t, params.value, params.max_bytes, conjunct_value, max)
        return _filter_group_by_gt_or_ge(conjunct_value, max, is_eq);
    }
    case TYPE_DECIMAL32:
    case TYPE_INT: {
        _PLAIN_DECODE_SINGLE(int32_t, params.value, params.max_bytes, conjunct_value, max)
        return _filter_group_by_gt_or_ge(conjunct_value, max, is_eq);
    }
    case TYPE_DECIMAL64:
    case TYPE_BIGINT: {
        _PLAIN_DECODE_SINGLE(int64_t, params.value, params.max_bytes, conjunct_value, max)
        return _filter_group_by_gt_or_ge(conjunct_value, max, is_eq);
    }
    case TYPE_DECIMALV2: {
        if (params.parquet_type == tparquet::Type::INT32) {
            int32_t conjunct_int_value = 0;
            int32_t parquet_value = 0;
            _decode_decimal_v2_to_primary(params, params.max_bytes, &conjunct_int_value,
                                          &parquet_value);
            return _filter_group_by_gt_or_ge(conjunct_int_value, parquet_value, is_eq);
        } else if (params.parquet_type == tparquet::Type::INT64) {
            int64_t conjunct_int_value = 0;
            int64_t parquet_value = 0;
            _decode_decimal_v2_to_primary(params, params.max_bytes, &conjunct_int_value,
                                          &parquet_value);
            return _filter_group_by_gt_or_ge(conjunct_int_value, parquet_value, is_eq);
        }
        break;
        //  When precision exceeds 18, decimal will use tparquet::Type::FIXED_LEN_BYTE_ARRAY to encode
        //  todo: support decimal128 after the test passes
        //        else if (params.parquet_type == tparquet::Type::FIXED_LEN_BYTE_ARRAY) {
        //            DecimalV2Value conjunct_value = *((DecimalV2Value*)params.value);
        //            Int128 conjunct_int_value = conjunct_value.value();
        //            Int128 max = _decode_value_to_int128(params, params.max_bytes);
        //            _align_decimal_v2_scale(&conjunct_int_value, conjunct_value.scale(), &max,
        //                                    params.parquet_scale);
        //            return _filter_group_by_gt_or_ge(conjunct_int_value, max, is_eq);
        //        }
    }
    case TYPE_STRING:
    case TYPE_VARCHAR:
    case TYPE_CHAR: {
        std::string conjunct_value = ((StringValue*)params.value)->to_string();
        if (!is_eq && strcmp(params.max_bytes, conjunct_value.data()) <= 0) {
            return true;
        } else if (strcmp(params.max_bytes, conjunct_value.data()) < 0) {
            return true;
        }
        break;
    }
    default:
        return false;
    }
    return false;
}

template <typename T>
static bool _filter_group_by_lt_or_le(T conjunct_value, T min, bool is_le) {
    if (!is_le) {
        if (min >= conjunct_value) {
            return true;
        }
    } else {
        if (min > conjunct_value) {
            return true;
        }
    }
    return false;
}

static bool _eval_lt(const ColumnMinMaxParams& params, bool is_eq) {
    switch (params.conjunct_type) {
    case TYPE_TINYINT: {
        _PLAIN_DECODE_SINGLE(int8_t, params.value, params.min_bytes, conjunct_value, min)
        return _filter_group_by_lt_or_le(conjunct_value, min, is_eq);
    }
    case TYPE_SMALLINT: {
        _PLAIN_DECODE_SINGLE(int16_t, params.value, params.min_bytes, conjunct_value, min)
        return _filter_group_by_lt_or_le(conjunct_value, min, is_eq);
    }
    case TYPE_DECIMAL32:
    case TYPE_INT: {
        _PLAIN_DECODE_SINGLE(int32_t, params.value, params.min_bytes, conjunct_value, min)
        return _filter_group_by_lt_or_le(conjunct_value, min, is_eq);
    }
    case TYPE_DECIMAL64:
    case TYPE_BIGINT: {
        _PLAIN_DECODE_SINGLE(int64_t, params.value, params.min_bytes, conjunct_value, min)
        return _filter_group_by_lt_or_le(conjunct_value, min, is_eq);
    }
    case TYPE_STRING:
    case TYPE_VARCHAR:
    case TYPE_CHAR: {
        std::string conjunct_value = ((StringValue*)params.value)->to_string();
        if (!is_eq && strcmp(params.min_bytes, conjunct_value.data()) >= 0) {
            return true;
        } else if (strcmp(params.min_bytes, conjunct_value.data()) > 0) {
            return true;
        }
        break;
    }
    case TYPE_DECIMALV2: {
        if (params.parquet_type == tparquet::Type::INT32) {
            int32_t conjunct_int_value = 0;
            int32_t parquet_value = 0;
            _decode_decimal_v2_to_primary(params, params.min_bytes, &conjunct_int_value,
                                          &parquet_value);
            return _filter_group_by_lt_or_le(conjunct_int_value, parquet_value, is_eq);
        } else if (params.parquet_type == tparquet::Type::INT64) {
            int64_t conjunct_int_value = 0;
            int64_t parquet_value = 0;
            _decode_decimal_v2_to_primary(params, params.min_bytes, &conjunct_int_value,
                                          &parquet_value);
            return _filter_group_by_lt_or_le(conjunct_int_value, parquet_value, is_eq);
        }
        break;
        //  When precision exceeds 18, decimal will use tparquet::Type::FIXED_LEN_BYTE_ARRAY to encode
        //  todo: support decimal128 after the test passes
        //        else if (params.parquet_type == tparquet::Type::FIXED_LEN_BYTE_ARRAY) {
        //            DecimalV2Value conjunct_value = *((DecimalV2Value*)params.value);
        //            Int128 conjunct_int_value = conjunct_value.value();
        //            Int128 min = _decode_value_to_int128(params, params.min_bytes);
        //            _align_decimal_v2_scale(&conjunct_int_value, conjunct_value.scale(), &min,
        //                                    params.parquet_scale);
        //            return _filter_group_by_lt_or_le(conjunct_int_value, min, is_eq);
        //        }
    }
    case TYPE_DATE: {
        //        doris::DateTimeValue* min_date = (doris::DateTimeValue*)params.value;
        //        LOG(INFO) << min_date->debug_string();
        return false;
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

static void _eval_predicate(const ScanPredicate& filter, ColumnMinMaxParams* params,
                            bool* need_filter) {
    if (filter._values.empty()) {
        return;
    }
    if (filter._op == TExprOpcode::FILTER_NEW_IN) {
        if (filter._values.size() == 1) {
            params->value = filter._values[0];
            *need_filter = _eval_eq(*params);
            return;
        }
        params->in_pred_values = filter._values;
        *need_filter = _eval_in_val(*params);
        return;
    }
    // preserve TExprOpcode::FILTER_NEW_NOT_IN
    params->value = filter._values[0];
    switch (filter._op) {
    case TExprOpcode::EQ:
        *need_filter = _eval_eq(*params);
        break;
    case TExprOpcode::NE:
        break;
    case TExprOpcode::GT:
        *need_filter = _eval_gt(*params, false);
        break;
    case TExprOpcode::GE:
        *need_filter = _eval_gt(*params, true);
        break;
    case TExprOpcode::LT:
        *need_filter = _eval_lt(*params, false);
        break;
    case TExprOpcode::LE:
        *need_filter = _eval_lt(*params, true);
        break;
    default:
        break;
    }
}

static bool determine_filter_min_max(const ColumnValueRangeType& col_val_range,
                                     const FieldSchema* col_schema, const std::string& encoded_min,
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
    if (filters.empty()) {
        return false;
    }

    ColumnMinMaxParams params;
    params.conjunct_type = col_type;
    params.parquet_type = col_schema->physical_type;
    params.parquet_precision = col_schema->parquet_schema.precision;
    params.parquet_scale = col_schema->parquet_schema.scale;
    params.parquet_type_length = col_schema->parquet_schema.type_length;
    params.min_bytes = min_bytes;
    params.max_bytes = max_bytes;

    for (int i = 0; i < filters.size(); i++) {
        _eval_predicate(filters[i], &params, &need_filter);
        if (need_filter) {
            break;
        }
    }
    return need_filter;
}

} // namespace doris::vectorized
