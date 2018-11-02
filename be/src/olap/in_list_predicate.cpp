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

#include "olap/in_list_predicate.h"
#include "olap/field.h"
#include "runtime/string_value.hpp"
#include "runtime/vectorized_row_batch.h"

namespace doris {

#define IN_LIST_PRED_CONSTRUCTOR(CLASS) \
template<class type> \
CLASS<type>::CLASS(int column_id, std::set<type>&& values) \
    : _column_id(column_id), \
      _values(std::move(values)) {} \

IN_LIST_PRED_CONSTRUCTOR(InListPredicate)
IN_LIST_PRED_CONSTRUCTOR(NotInListPredicate)

#define IN_LIST_PRED_EVALUATE(CLASS, OP) \
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
            for (uint16_t j = 0; j != n; ++j) { \
                uint16_t i = sel[j]; \
                sel[new_size] = i; \
                new_size += (_values.find(col_vector[i]) OP _values.end()); \
            } \
            batch->set_size(new_size); \
        } else { \
            for (uint16_t i = 0; i != n; ++i) { \
                sel[new_size] = i; \
                new_size += (_values.find(col_vector[i]) OP _values.end()); \
            } \
            if (new_size < n) { \
                batch->set_size(new_size); \
                batch->set_selected_in_use(true); \
            } \
        } \
    } else { \
        bool* is_null = batch->column(_column_id)->is_null(); \
        if (batch->selected_in_use()) { \
            for (uint16_t j = 0; j != n; ++j) { \
                uint16_t i = sel[j]; \
                sel[new_size] = i; \
                new_size += (!is_null[i] && _values.find(col_vector[i]) OP _values.end()); \
            } \
            batch->set_size(new_size); \
        } else { \
            for (int i = 0; i != n; ++i) { \
                sel[new_size] = i; \
                new_size += (!is_null[i] && _values.find(col_vector[i]) OP _values.end()); \
            } \
            if (new_size < n) { \
                batch->set_size(new_size); \
                batch->set_selected_in_use(true); \
            } \
        } \
    } \
} \

IN_LIST_PRED_EVALUATE(InListPredicate, !=)
IN_LIST_PRED_EVALUATE(NotInListPredicate, ==)

#define IN_LIST_PRED_CONSTRUCTOR_DECLARATION(CLASS) \
    template CLASS<int8_t>::CLASS(int column_id, std::set<int8_t>&& values); \
    template CLASS<int16_t>::CLASS(int column_id, std::set<int16_t>&& values); \
    template CLASS<int32_t>::CLASS(int column_id, std::set<int32_t>&& values); \
    template CLASS<int64_t>::CLASS(int column_id, std::set<int64_t>&& values); \
    template CLASS<int128_t>::CLASS(int column_id, std::set<int128_t>&& values); \
    template CLASS<float>::CLASS(int column_id, std::set<float>&& values); \
    template CLASS<double>::CLASS(int column_id, std::set<double>&& values); \
    template CLASS<decimal12_t>::CLASS(int column_id, std::set<decimal12_t>&& values); \
    template CLASS<StringValue>::CLASS(int column_id, std::set<StringValue>&& values); \
    template CLASS<uint24_t>::CLASS(int column_id, std::set<uint24_t>&& values); \
    template CLASS<uint64_t>::CLASS(int column_id, std::set<uint64_t>&& values); \

IN_LIST_PRED_CONSTRUCTOR_DECLARATION(InListPredicate)
IN_LIST_PRED_CONSTRUCTOR_DECLARATION(NotInListPredicate)

#define IN_LIST_PRED_EVALUATE_DECLARATION(CLASS) \
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

IN_LIST_PRED_EVALUATE_DECLARATION(InListPredicate)
IN_LIST_PRED_EVALUATE_DECLARATION(NotInListPredicate)
} //namespace doris
