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

#include <exprs/expr_context.h>
#include <exprs/in_predicate.h>

#include <cstring>
#include <vector>

#include "vparquet_group_reader.h"

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

bool _eval_in_val(PrimitiveType conjunct_type, std::vector<void*> in_pred_values,
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
    case TYPE_CHAR:
    case TYPE_DATE:
    case TYPE_DATETIME: {
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

void ParquetReader::_eval_in_predicate(ExprContext* ctx, const char* min_bytes,
                                       const char* max_bytes, bool& need_filter) {
    Expr* conjunct = ctx->root();
    std::vector<void*> in_pred_values;
    const InPredicate* pred = static_cast<const InPredicate*>(conjunct);
    HybridSetBase::IteratorBase* iter = pred->hybrid_set()->begin();
    // TODO: process expr: in(func(123),123)
    while (iter->has_next()) {
        if (nullptr == iter->get_value()) {
            return;
        }
        in_pred_values.emplace_back(const_cast<void*>(iter->get_value()));
        iter->next();
    }
    auto conjunct_type = conjunct->get_child(1)->type().type;
    switch (conjunct->op()) {
    case TExprOpcode::FILTER_IN:
        need_filter = _eval_in_val(conjunct_type, in_pred_values, min_bytes, max_bytes);
        break;
        //  case TExprOpcode::FILTER_NOT_IN:
    default:
        need_filter = false;
    }
}

bool _eval_eq(PrimitiveType conjunct_type, void* value, const char* min_bytes,
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
    case TYPE_CHAR:
    case TYPE_DATE:
    case TYPE_DATETIME: {
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

bool _eval_gt(PrimitiveType conjunct_type, void* value, const char* max_bytes) {
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
    case TYPE_CHAR:
    case TYPE_DATE:
    case TYPE_DATETIME: {
        //            case TYPE_TIME:
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

bool _eval_ge(PrimitiveType conjunct_type, void* value, const char* max_bytes) {
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
    case TYPE_CHAR:
    case TYPE_DATE:
    case TYPE_DATETIME: {
        //            case TYPE_TIME:
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

bool _eval_lt(PrimitiveType conjunct_type, void* value, const char* min_bytes) {
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
    case TYPE_CHAR:
    case TYPE_DATE:
    case TYPE_DATETIME: {
        //            case TYPE_TIME:
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

bool _eval_le(PrimitiveType conjunct_type, void* value, const char* min_bytes) {
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
    case TYPE_CHAR:
    case TYPE_DATE:
    case TYPE_DATETIME: {
        //            case TYPE_TIME:
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

void ParquetReader::_eval_binary_predicate(ExprContext* ctx, const char* min_bytes,
                                           const char* max_bytes, bool& need_filter) {
    Expr* conjunct = ctx->root();
    Expr* expr = conjunct->get_child(1);
    if (expr == nullptr) {
        return;
    }
    // supported conjunct example: slot_ref < 123, slot_ref > func(123), ..
    auto conjunct_type = expr->type().type;
    void* conjunct_value = ctx->get_value(expr, nullptr);
    switch (conjunct->op()) {
    case TExprOpcode::EQ:
        need_filter = _eval_eq(conjunct_type, conjunct_value, min_bytes, max_bytes);
        break;
    case TExprOpcode::NE:
        break;
    case TExprOpcode::GT:
        need_filter = _eval_gt(conjunct_type, conjunct_value, max_bytes);
        break;
    case TExprOpcode::GE:
        need_filter = _eval_ge(conjunct_type, conjunct_value, max_bytes);
        break;
    case TExprOpcode::LT:
        need_filter = _eval_lt(conjunct_type, conjunct_value, min_bytes);
        break;
    case TExprOpcode::LE:
        need_filter = _eval_le(conjunct_type, conjunct_value, min_bytes);
        break;
    default:
        break;
    }
}

bool ParquetReader::_determine_filter_min_max(const std::vector<ExprContext*>& conjuncts,
                                              const std::string& encoded_min,
                                              const std::string& encoded_max) {
    const char* min_bytes = encoded_min.data();
    const char* max_bytes = encoded_max.data();
    bool need_filter = false;
    for (int i = 0; i < conjuncts.size(); i++) {
        Expr* conjunct = conjuncts[i]->root();
        if (TExprNodeType::BINARY_PRED == conjunct->node_type()) {
            _eval_binary_predicate(conjuncts[i], min_bytes, max_bytes, need_filter);
        } else if (TExprNodeType::IN_PRED == conjunct->node_type()) {
            _eval_in_predicate(conjuncts[i], min_bytes, max_bytes, need_filter);
        }
    }
    return need_filter;
}

} // namespace doris::vectorized
