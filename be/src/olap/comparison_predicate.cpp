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

#include "olap/comparison_predicate.h"
#include "olap/field.h"
#include "runtime/string_value.hpp"
#include "runtime/vectorized_row_batch.h"

namespace doris {

#define COMPARISON_PRED_CONSTRUCTOR(CLASS) \
    template<class type> \
    CLASS<type>::CLASS(int column_id, const type& value) \
        : _column_id(column_id), \
          _value(value) \
        {} \

COMPARISON_PRED_CONSTRUCTOR(EqualPredicate)
COMPARISON_PRED_CONSTRUCTOR(NotEqualPredicate)
COMPARISON_PRED_CONSTRUCTOR(LessPredicate)
COMPARISON_PRED_CONSTRUCTOR(LessEqualPredicate)
COMPARISON_PRED_CONSTRUCTOR(GreaterPredicate)
COMPARISON_PRED_CONSTRUCTOR(GreaterEqualPredicate)

#define COMPARISON_PRED_CONSTRUCTOR_STRING(CLASS) \
    template<> \
    CLASS<StringValue>::CLASS(int column_id, const StringValue& value) \
        : _column_id(column_id) \
        { \
            _value.len = value.len; \
            _value.ptr = value.ptr; \
        } \

COMPARISON_PRED_CONSTRUCTOR_STRING(EqualPredicate)
COMPARISON_PRED_CONSTRUCTOR_STRING(NotEqualPredicate)
COMPARISON_PRED_CONSTRUCTOR_STRING(LessPredicate)
COMPARISON_PRED_CONSTRUCTOR_STRING(LessEqualPredicate)
COMPARISON_PRED_CONSTRUCTOR_STRING(GreaterPredicate)
COMPARISON_PRED_CONSTRUCTOR_STRING(GreaterEqualPredicate)

#define COMPARISON_PRED_EVALUATE(CLASS, OP) \
    template<class type> \
    void CLASS<type>::evaluate(VectorizedRowBatch* batch) const { \
        uint16_t n = batch->size(); \
        if (n == 0) { \
            return; \
        } \
        uint16_t* sel = batch->selected(); \
        const type* col_vector = reinterpret_cast<const type*>(batch->column(_column_id)->col_data()); \
        uint16_t new_size = 0; \
        if (batch->column(_column_id)->no_nulls()) { \
            if (batch->selected_in_use()) { \
                for (uint16_t j = 0; j !=n; ++j) { \
                    uint16_t i = sel[j]; \
                    sel[new_size] = i; \
                    new_size += (col_vector[i] OP _value); \
                } \
                batch->set_size(new_size); \
            } else { \
                for (uint16_t i = 0; i !=n; ++i) { \
                    sel[new_size] = i; \
                    new_size += (col_vector[i] OP _value); \
                } \
                if (new_size < n) { \
                    batch->set_size(new_size); \
                    batch->set_selected_in_use(true); \
                } \
            } \
        } else { \
            bool* is_null = batch->column(_column_id)->is_null(); \
            if (batch->selected_in_use()) { \
                for (uint16_t j = 0; j !=n; ++j) { \
                    uint16_t i = sel[j]; \
                    sel[new_size] = i; \
                    new_size += (!is_null[i] && (col_vector[i] OP _value)); \
                } \
                batch->set_size(new_size); \
            } else { \
                for (uint16_t i = 0; i !=n; ++i) { \
                    sel[new_size] = i; \
                    new_size += (!is_null[i] && (col_vector[i] OP _value)); \
                } \
                if (new_size < n) { \
                    batch->set_size(new_size); \
                    batch->set_selected_in_use(true); \
                } \
            } \
        } \
    } \


COMPARISON_PRED_EVALUATE(EqualPredicate, ==)
COMPARISON_PRED_EVALUATE(NotEqualPredicate, !=)
COMPARISON_PRED_EVALUATE(LessPredicate, <)
COMPARISON_PRED_EVALUATE(LessEqualPredicate, <=)
COMPARISON_PRED_EVALUATE(GreaterPredicate, >)
COMPARISON_PRED_EVALUATE(GreaterEqualPredicate, >=)

#define COMPARISON_PRED_CONSTRUCTOR_DECLARATION(CLASS) \
    template CLASS<int8_t>::CLASS(int column_id, const int8_t& value); \
    template CLASS<int16_t>::CLASS(int column_id, const int16_t& value); \
    template CLASS<int32_t>::CLASS(int column_id, const int32_t& value); \
    template CLASS<int64_t>::CLASS(int column_id, const int64_t& value); \
    template CLASS<int128_t>::CLASS(int column_id, const int128_t& value); \
    template CLASS<float>::CLASS(int column_id, const float& value); \
    template CLASS<double>::CLASS(int column_id, const double& value); \
    template CLASS<decimal12_t>::CLASS(int column_id, const decimal12_t& value); \
    template CLASS<StringValue>::CLASS(int column_id, const StringValue& value); \
    template CLASS<uint24_t>::CLASS(int column_id, const uint24_t& value); \
    template CLASS<uint64_t>::CLASS(int column_id, const uint64_t& value); \

COMPARISON_PRED_CONSTRUCTOR_DECLARATION(EqualPredicate)
COMPARISON_PRED_CONSTRUCTOR_DECLARATION(NotEqualPredicate)
COMPARISON_PRED_CONSTRUCTOR_DECLARATION(LessPredicate)
COMPARISON_PRED_CONSTRUCTOR_DECLARATION(LessEqualPredicate)
COMPARISON_PRED_CONSTRUCTOR_DECLARATION(GreaterPredicate)
COMPARISON_PRED_CONSTRUCTOR_DECLARATION(GreaterEqualPredicate)

#define COMPARISON_PRED_EVALUATE_DECLARATION(CLASS) \
    template void CLASS<int8_t>::evaluate(VectorizedRowBatch* batch) const; \
    template void CLASS<int16_t>::evaluate(VectorizedRowBatch* batch) const; \
    template void CLASS<int32_t>::evaluate(VectorizedRowBatch* batch) const; \
    template void CLASS<int64_t>::evaluate(VectorizedRowBatch* batch) const; \
    template void CLASS<int128_t>::evaluate(VectorizedRowBatch* batch) const; \
    template void CLASS<float>::evaluate(VectorizedRowBatch* batch) const; \
    template void CLASS<double>::evaluate(VectorizedRowBatch* batch) const; \
    template void CLASS<decimal12_t>::evaluate(VectorizedRowBatch* batch) const; \
    template void CLASS<StringValue>::evaluate(VectorizedRowBatch* batch) const; \
    template void CLASS<uint24_t>::evaluate(VectorizedRowBatch* batch) const; \
    template void CLASS<uint64_t>::evaluate(VectorizedRowBatch* batch) const; \

COMPARISON_PRED_EVALUATE_DECLARATION(EqualPredicate)
COMPARISON_PRED_EVALUATE_DECLARATION(NotEqualPredicate)
COMPARISON_PRED_EVALUATE_DECLARATION(LessPredicate)
COMPARISON_PRED_EVALUATE_DECLARATION(LessEqualPredicate)
COMPARISON_PRED_EVALUATE_DECLARATION(GreaterPredicate)
COMPARISON_PRED_EVALUATE_DECLARATION(GreaterEqualPredicate)

} //namespace doris
