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

#include "common/logging.h"
#include "olap/schema.h"
#include "runtime/string_value.hpp"
#include "runtime/vectorized_row_batch.h"
#include "vec/columns/column_dictionary.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/predicate_column.h"

namespace doris {

#define COMPARISON_PRED_CONSTRUCTOR(CLASS)                             \
    template <class T>                                                 \
    CLASS<T>::CLASS(uint32_t column_id, const T& value, bool opposite) \
            : ColumnPredicate(column_id, opposite), _value(value) {}

COMPARISON_PRED_CONSTRUCTOR(EqualPredicate)
COMPARISON_PRED_CONSTRUCTOR(NotEqualPredicate)
COMPARISON_PRED_CONSTRUCTOR(LessPredicate)
COMPARISON_PRED_CONSTRUCTOR(LessEqualPredicate)
COMPARISON_PRED_CONSTRUCTOR(GreaterPredicate)
COMPARISON_PRED_CONSTRUCTOR(GreaterEqualPredicate)

#define COMPARISON_PRED_CONSTRUCTOR_STRING(CLASS)                                          \
    template <>                                                                            \
    CLASS<StringValue>::CLASS(uint32_t column_id, const StringValue& value, bool opposite) \
            : ColumnPredicate(column_id, opposite) {                                       \
        _value.len = value.len;                                                            \
        _value.ptr = value.ptr;                                                            \
    }

COMPARISON_PRED_CONSTRUCTOR_STRING(EqualPredicate)
COMPARISON_PRED_CONSTRUCTOR_STRING(NotEqualPredicate)
COMPARISON_PRED_CONSTRUCTOR_STRING(LessPredicate)
COMPARISON_PRED_CONSTRUCTOR_STRING(LessEqualPredicate)
COMPARISON_PRED_CONSTRUCTOR_STRING(GreaterPredicate)
COMPARISON_PRED_CONSTRUCTOR_STRING(GreaterEqualPredicate)

#define COMPARISON_PRED_EVALUATE(CLASS, OP)                                                      \
    template <class T>                                                                           \
    void CLASS<T>::evaluate(VectorizedRowBatch* batch) const {                                   \
        uint16_t n = batch->size();                                                              \
        if (n == 0) {                                                                            \
            return;                                                                              \
        }                                                                                        \
        uint16_t* sel = batch->selected();                                                       \
        const T* col_vector = reinterpret_cast<const T*>(batch->column(_column_id)->col_data()); \
        uint16_t new_size = 0;                                                                   \
        if (batch->column(_column_id)->no_nulls()) {                                             \
            if (batch->selected_in_use()) {                                                      \
                for (uint16_t j = 0; j != n; ++j) {                                              \
                    uint16_t i = sel[j];                                                         \
                    sel[new_size] = i;                                                           \
                    new_size += (col_vector[i] OP _value);                                       \
                }                                                                                \
                batch->set_size(new_size);                                                       \
            } else {                                                                             \
                for (uint16_t i = 0; i != n; ++i) {                                              \
                    sel[new_size] = i;                                                           \
                    new_size += (col_vector[i] OP _value);                                       \
                }                                                                                \
                if (new_size < n) {                                                              \
                    batch->set_size(new_size);                                                   \
                    batch->set_selected_in_use(true);                                            \
                }                                                                                \
            }                                                                                    \
        } else {                                                                                 \
            bool* is_null = batch->column(_column_id)->is_null();                                \
            if (batch->selected_in_use()) {                                                      \
                for (uint16_t j = 0; j != n; ++j) {                                              \
                    uint16_t i = sel[j];                                                         \
                    sel[new_size] = i;                                                           \
                    new_size += (!is_null[i] && (col_vector[i] OP _value));                      \
                }                                                                                \
                batch->set_size(new_size);                                                       \
            } else {                                                                             \
                for (uint16_t i = 0; i != n; ++i) {                                              \
                    sel[new_size] = i;                                                           \
                    new_size += (!is_null[i] && (col_vector[i] OP _value));                      \
                }                                                                                \
                if (new_size < n) {                                                              \
                    batch->set_size(new_size);                                                   \
                    batch->set_selected_in_use(true);                                            \
                }                                                                                \
            }                                                                                    \
        }                                                                                        \
    }

COMPARISON_PRED_EVALUATE(EqualPredicate, ==)
COMPARISON_PRED_EVALUATE(NotEqualPredicate, !=)
COMPARISON_PRED_EVALUATE(LessPredicate, <)
COMPARISON_PRED_EVALUATE(LessEqualPredicate, <=)
COMPARISON_PRED_EVALUATE(GreaterPredicate, >)
COMPARISON_PRED_EVALUATE(GreaterEqualPredicate, >=)

#define COMPARISON_PRED_COLUMN_BLOCK_EVALUATE(CLASS, OP)                                       \
    template <class T>                                                                         \
    void CLASS<T>::evaluate(ColumnBlock* block, uint16_t* sel, uint16_t* size) const {         \
        uint16_t new_size = 0;                                                                 \
        if (block->is_nullable()) {                                                            \
            for (uint16_t i = 0; i < *size; ++i) {                                             \
                uint16_t idx = sel[i];                                                         \
                sel[new_size] = idx;                                                           \
                const T* cell_value = reinterpret_cast<const T*>(block->cell(idx).cell_ptr()); \
                auto result = (!block->cell(idx).is_null() && (*cell_value OP _value));        \
                new_size += _opposite ? !result : result;                                      \
            }                                                                                  \
        } else {                                                                               \
            for (uint16_t i = 0; i < *size; ++i) {                                             \
                uint16_t idx = sel[i];                                                         \
                sel[new_size] = idx;                                                           \
                const T* cell_value = reinterpret_cast<const T*>(block->cell(idx).cell_ptr()); \
                auto result = (*cell_value OP _value);                                         \
                new_size += _opposite ? !result : result;                                      \
            }                                                                                  \
        }                                                                                      \
        *size = new_size;                                                                      \
    }

COMPARISON_PRED_COLUMN_BLOCK_EVALUATE(EqualPredicate, ==)
COMPARISON_PRED_COLUMN_BLOCK_EVALUATE(NotEqualPredicate, !=)
COMPARISON_PRED_COLUMN_BLOCK_EVALUATE(LessPredicate, <)
COMPARISON_PRED_COLUMN_BLOCK_EVALUATE(LessEqualPredicate, <=)
COMPARISON_PRED_COLUMN_BLOCK_EVALUATE(GreaterPredicate, >)
COMPARISON_PRED_COLUMN_BLOCK_EVALUATE(GreaterEqualPredicate, >=)

// todo(zeno) define interface in IColumn to simplify code
#define COMPARISON_PRED_COLUMN_EVALUATE(CLASS, OP, IS_RANGE)                                       \
    template <class T>                                                                             \
    void CLASS<T>::evaluate(vectorized::IColumn& column, uint16_t* sel, uint16_t* size) const {    \
        uint16_t new_size = 0;                                                                     \
        if (column.is_nullable()) {                                                                \
            auto* nullable_col =                                                                   \
                    vectorized::check_and_get_column<vectorized::ColumnNullable>(column);          \
            auto& null_bitmap = reinterpret_cast<const vectorized::ColumnUInt8&>(                  \
                                        nullable_col->get_null_map_column())                       \
                                        .get_data();                                               \
            auto& nested_col = nullable_col->get_nested_column();                                  \
            if (nested_col.is_column_dictionary()) {                                               \
                if constexpr (std::is_same_v<T, StringValue>) {                                    \
                    auto* nested_col_ptr = vectorized::check_and_get_column<                       \
                            vectorized::ColumnDictionary<vectorized::Int32>>(nested_col);          \
                    auto& data_array = nested_col_ptr->get_data();                                 \
                    auto dict_code =                                                               \
                            IS_RANGE ? nested_col_ptr->find_code_by_bound(_value, 0 OP 1, 1 OP 1)  \
                                     : nested_col_ptr->find_code(_value);                          \
                    for (uint16_t i = 0; i < *size; i++) {                                         \
                        uint16_t idx = sel[i];                                                     \
                        sel[new_size] = idx;                                                       \
                        const auto& cell_value = data_array[idx];                                  \
                        bool ret = !null_bitmap[idx] && (cell_value OP dict_code);                 \
                        new_size += _opposite ? !ret : ret;                                        \
                    }                                                                              \
                }                                                                                  \
            } else {                                                                               \
                auto* nested_col_ptr =                                                             \
                        vectorized::check_and_get_column<vectorized::PredicateColumnType<T>>(      \
                                nested_col);                                                       \
                auto& data_array = nested_col_ptr->get_data();                                     \
                for (uint16_t i = 0; i < *size; i++) {                                             \
                    uint16_t idx = sel[i];                                                         \
                    sel[new_size] = idx;                                                           \
                    const auto& cell_value = reinterpret_cast<const T&>(data_array[idx]);          \
                    bool ret = !null_bitmap[idx] && (cell_value OP _value);                        \
                    new_size += _opposite ? !ret : ret;                                            \
                }                                                                                  \
            }                                                                                      \
        } else if (column.is_column_dictionary()) {                                                \
            if constexpr (std::is_same_v<T, StringValue>) {                                        \
                auto& dict_col =                                                                   \
                        reinterpret_cast<vectorized::ColumnDictionary<vectorized::Int32>&>(        \
                                column);                                                           \
                auto& data_array = dict_col.get_data();                                            \
                auto dict_code = IS_RANGE ? dict_col.find_code_by_bound(_value, 0 OP 1, 1 OP 1)    \
                                          : dict_col.find_code(_value);                            \
                for (uint16_t i = 0; i < *size; ++i) {                                             \
                    uint16_t idx = sel[i];                                                         \
                    sel[new_size] = idx;                                                           \
                    const auto& cell_value = data_array[idx];                                      \
                    bool ret = cell_value OP dict_code;                                            \
                    new_size += _opposite ? !ret : ret;                                            \
                }                                                                                  \
            }                                                                                      \
        } else {                                                                                   \
            auto& pred_column_ref = reinterpret_cast<vectorized::PredicateColumnType<T>&>(column); \
            auto& data_array = pred_column_ref.get_data();                                         \
            for (uint16_t i = 0; i < *size; i++) {                                                 \
                uint16_t idx = sel[i];                                                             \
                sel[new_size] = idx;                                                               \
                const auto& cell_value = reinterpret_cast<const T&>(data_array[idx]);              \
                auto ret = cell_value OP _value;                                                   \
                new_size += _opposite ? !ret : ret;                                                \
            }                                                                                      \
        }                                                                                          \
        *size = new_size;                                                                          \
    }

COMPARISON_PRED_COLUMN_EVALUATE(EqualPredicate, ==, false)
COMPARISON_PRED_COLUMN_EVALUATE(NotEqualPredicate, !=, false)
COMPARISON_PRED_COLUMN_EVALUATE(LessPredicate, <, true)
COMPARISON_PRED_COLUMN_EVALUATE(LessEqualPredicate, <=, true)
COMPARISON_PRED_COLUMN_EVALUATE(GreaterPredicate, >, true)
COMPARISON_PRED_COLUMN_EVALUATE(GreaterEqualPredicate, >=, true)

#define COMPARISON_PRED_COLUMN_EVALUATE_VEC(CLASS, OP)                                           \
    template <class T>                                                                           \
    void CLASS<T>::evaluate_vec(vectorized::IColumn& column, uint16_t size, bool* flags) const { \
        if (column.is_nullable()) {                                                              \
            auto* nullable_column =                                                              \
                    vectorized::check_and_get_column<vectorized::ColumnNullable>(column);        \
            auto& data_array = reinterpret_cast<const vectorized::PredicateColumnType<T>&>(      \
                                       nullable_column->get_nested_column())                     \
                                       .get_data();                                              \
            auto& null_bitmap = reinterpret_cast<const vectorized::ColumnVector<uint8_t>&>(      \
                                        *(nullable_column->get_null_map_column_ptr()))           \
                                        .get_data();                                             \
            for (uint16_t i = 0; i < size; i++) {                                                \
                flags[i] = (data_array[i] OP _value) && (!null_bitmap[i]);                       \
            }                                                                                    \
        } else {                                                                                 \
            auto& predicate_column =                                                             \
                    reinterpret_cast<vectorized::PredicateColumnType<T>&>(column);               \
            auto& data_array = predicate_column.get_data();                                      \
            for (uint16_t i = 0; i < size; i++) {                                                \
                flags[i] = data_array[i] OP _value;                                              \
            }                                                                                    \
        }                                                                                        \
        if (_opposite) {                                                                         \
            for (uint16_t i = 0; i < size; i++) {                                                \
                flags[i] = !flags[i];                                                            \
            }                                                                                    \
        }                                                                                        \
    }

COMPARISON_PRED_COLUMN_EVALUATE_VEC(EqualPredicate, ==)
COMPARISON_PRED_COLUMN_EVALUATE_VEC(NotEqualPredicate, !=)
COMPARISON_PRED_COLUMN_EVALUATE_VEC(LessPredicate, <)
COMPARISON_PRED_COLUMN_EVALUATE_VEC(LessEqualPredicate, <=)
COMPARISON_PRED_COLUMN_EVALUATE_VEC(GreaterPredicate, >)
COMPARISON_PRED_COLUMN_EVALUATE_VEC(GreaterEqualPredicate, >=)

#define COMPARISON_PRED_COLUMN_BLOCK_EVALUATE_OR(CLASS, OP)                                    \
    template <class T>                                                                         \
    void CLASS<T>::evaluate_or(ColumnBlock* block, uint16_t* sel, uint16_t size, bool* flags)  \
            const {                                                                            \
        if (block->is_nullable()) {                                                            \
            for (uint16_t i = 0; i < size; ++i) {                                              \
                if (flags[i]) continue;                                                        \
                uint16_t idx = sel[i];                                                         \
                const T* cell_value = reinterpret_cast<const T*>(block->cell(idx).cell_ptr()); \
                auto result = (!block->cell(idx).is_null() && (*cell_value OP _value));        \
                flags[i] |= _opposite ? !result : result;                                      \
            }                                                                                  \
        } else {                                                                               \
            for (uint16_t i = 0; i < size; ++i) {                                              \
                if (flags[i]) continue;                                                        \
                uint16_t idx = sel[i];                                                         \
                const T* cell_value = reinterpret_cast<const T*>(block->cell(idx).cell_ptr()); \
                auto result = (*cell_value OP _value);                                         \
                flags[i] |= _opposite ? !result : result;                                      \
            }                                                                                  \
        }                                                                                      \
    }

COMPARISON_PRED_COLUMN_BLOCK_EVALUATE_OR(EqualPredicate, ==)
COMPARISON_PRED_COLUMN_BLOCK_EVALUATE_OR(NotEqualPredicate, !=)
COMPARISON_PRED_COLUMN_BLOCK_EVALUATE_OR(LessPredicate, <)
COMPARISON_PRED_COLUMN_BLOCK_EVALUATE_OR(LessEqualPredicate, <=)
COMPARISON_PRED_COLUMN_BLOCK_EVALUATE_OR(GreaterPredicate, >)
COMPARISON_PRED_COLUMN_BLOCK_EVALUATE_OR(GreaterEqualPredicate, >=)

#define COMPARISON_PRED_COLUMN_EVALUATE_OR(CLASS, OP)                                       \
    template <class T>                                                                      \
    void CLASS<T>::evaluate_or(vectorized::IColumn& column, uint16_t* sel, uint16_t size,   \
                               bool* flags) const {                                         \
        if (column.is_nullable()) {                                                         \
            auto* nullable_column =                                                         \
                    vectorized::check_and_get_column<vectorized::ColumnNullable>(column);   \
            auto& data_array = reinterpret_cast<const vectorized::PredicateColumnType<T>&>( \
                                       nullable_column->get_nested_column())                \
                                       .get_data();                                         \
            auto& null_bitmap = reinterpret_cast<const vectorized::ColumnVector<uint8_t>&>( \
                                        *(nullable_column->get_null_map_column_ptr()))      \
                                        .get_data();                                        \
            for (uint16_t i = 0; i < size; i++) {                                           \
                if (flags[i]) continue;                                                     \
                uint16_t idx = sel[i];                                                      \
                bool ret = !null_bitmap[idx] && (data_array[idx] OP _value);                \
                flags[i] |= _opposite ? !ret : ret;                                         \
            }                                                                               \
        } else {                                                                            \
            auto& predicate_column =                                                        \
                    reinterpret_cast<vectorized::PredicateColumnType<T>&>(column);          \
            auto& data_array = predicate_column.get_data();                                 \
            for (uint16_t i = 0; i < size; ++i) {                                           \
                if (flags[i]) continue;                                                     \
                uint16_t idx = sel[i];                                                      \
                bool ret = data_array[idx] OP _value;                                       \
                flags[i] |= _opposite ? !ret : ret;                                         \
            }                                                                               \
        }                                                                                   \
    }

COMPARISON_PRED_COLUMN_EVALUATE_OR(EqualPredicate, ==)
COMPARISON_PRED_COLUMN_EVALUATE_OR(NotEqualPredicate, !=)
COMPARISON_PRED_COLUMN_EVALUATE_OR(LessPredicate, <)
COMPARISON_PRED_COLUMN_EVALUATE_OR(LessEqualPredicate, <=)
COMPARISON_PRED_COLUMN_EVALUATE_OR(GreaterPredicate, >)
COMPARISON_PRED_COLUMN_EVALUATE_OR(GreaterEqualPredicate, >=)

#define COMPARISON_PRED_COLUMN_BLOCK_EVALUATE_AND(CLASS, OP)                                   \
    template <class T>                                                                         \
    void CLASS<T>::evaluate_and(ColumnBlock* block, uint16_t* sel, uint16_t size, bool* flags) \
            const {                                                                            \
        if (block->is_nullable()) {                                                            \
            for (uint16_t i = 0; i < size; ++i) {                                              \
                if (!flags[i]) continue;                                                       \
                uint16_t idx = sel[i];                                                         \
                const T* cell_value = reinterpret_cast<const T*>(block->cell(idx).cell_ptr()); \
                auto result = (!block->cell(idx).is_null() && (*cell_value OP _value));        \
                flags[i] &= _opposite ? !result : result;                                      \
            }                                                                                  \
        } else {                                                                               \
            for (uint16_t i = 0; i < size; ++i) {                                              \
                if (!flags[i]) continue;                                                       \
                uint16_t idx = sel[i];                                                         \
                const T* cell_value = reinterpret_cast<const T*>(block->cell(idx).cell_ptr()); \
                auto result = (*cell_value OP _value);                                         \
                flags[i] &= _opposite ? !result : result;                                      \
            }                                                                                  \
        }                                                                                      \
    }

COMPARISON_PRED_COLUMN_BLOCK_EVALUATE_AND(EqualPredicate, ==)
COMPARISON_PRED_COLUMN_BLOCK_EVALUATE_AND(NotEqualPredicate, !=)
COMPARISON_PRED_COLUMN_BLOCK_EVALUATE_AND(LessPredicate, <)
COMPARISON_PRED_COLUMN_BLOCK_EVALUATE_AND(LessEqualPredicate, <=)
COMPARISON_PRED_COLUMN_BLOCK_EVALUATE_AND(GreaterPredicate, >)
COMPARISON_PRED_COLUMN_BLOCK_EVALUATE_AND(GreaterEqualPredicate, >=)

#define COMPARISON_PRED_COLUMN_EVALUATE_AND(CLASS, OP)                                      \
    template <class T>                                                                      \
    void CLASS<T>::evaluate_and(vectorized::IColumn& column, uint16_t* sel, uint16_t size,  \
                                bool* flags) const {                                        \
        if (column.is_nullable()) {                                                         \
            auto* nullable_column =                                                         \
                    vectorized::check_and_get_column<vectorized::ColumnNullable>(column);   \
            auto& data_array = reinterpret_cast<const vectorized::PredicateColumnType<T>&>( \
                                       nullable_column->get_nested_column())                \
                                       .get_data();                                         \
            auto& null_bitmap = reinterpret_cast<const vectorized::ColumnVector<uint8_t>&>( \
                                        *(nullable_column->get_null_map_column_ptr()))      \
                                        .get_data();                                        \
            for (uint16_t i = 0; i < size; i++) {                                           \
                if (!flags[i]) continue;                                                    \
                uint16_t idx = sel[i];                                                      \
                bool ret = !null_bitmap[idx] && (data_array[idx] OP _value);                \
                flags[i] &= _opposite ? !ret : ret;                                         \
            }                                                                               \
        } else {                                                                            \
            auto& predicate_column =                                                        \
                    reinterpret_cast<vectorized::PredicateColumnType<T>&>(column);          \
            auto& data_array = predicate_column.get_data();                                 \
            for (uint16_t i = 0; i < size; ++i) {                                           \
                if (!flags[i]) continue;                                                    \
                uint16_t idx = sel[i];                                                      \
                bool ret = data_array[idx] OP _value;                                       \
                flags[i] &= _opposite ? !ret : ret;                                         \
            }                                                                               \
        }                                                                                   \
    }

COMPARISON_PRED_COLUMN_EVALUATE_AND(EqualPredicate, ==)
COMPARISON_PRED_COLUMN_EVALUATE_AND(NotEqualPredicate, !=)
COMPARISON_PRED_COLUMN_EVALUATE_AND(LessPredicate, <)
COMPARISON_PRED_COLUMN_EVALUATE_AND(LessEqualPredicate, <=)
COMPARISON_PRED_COLUMN_EVALUATE_AND(GreaterPredicate, >)
COMPARISON_PRED_COLUMN_EVALUATE_AND(GreaterEqualPredicate, >=)

#define BITMAP_COMPARE_EqualPredicate(s, exact_match, seeked_ordinal, iterator, bitmap, roaring) \
    do {                                                                                         \
        if (!s.is_not_found()) {                                                                 \
            if (!s.ok()) {                                                                       \
                return s;                                                                        \
            }                                                                                    \
            if (exact_match) {                                                                   \
                RETURN_IF_ERROR(iterator->read_bitmap(seeked_ordinal, &roaring));                \
            }                                                                                    \
        }                                                                                        \
    } while (0)

#define BITMAP_COMPARE_NotEqualPredicate(s, exact_match, seeked_ordinal, iterator, bitmap, \
                                         roaring)                                          \
    do {                                                                                   \
        if (s.is_not_found()) {                                                            \
            return Status::OK();                                                           \
        }                                                                                  \
        if (!s.ok()) {                                                                     \
            return s;                                                                      \
        }                                                                                  \
        if (!exact_match) {                                                                \
            return Status::OK();                                                           \
        }                                                                                  \
        RETURN_IF_ERROR(iterator->read_bitmap(seeked_ordinal, &roaring));                  \
        *bitmap -= roaring;                                                                \
        return Status::OK();                                                               \
    } while (0)

#define BITMAP_COMPARE_LessPredicate(s, exact_match, seeked_ordinal, iterator, bitmap, roaring) \
    do {                                                                                        \
        if (s.is_not_found()) {                                                                 \
            return Status::OK();                                                                \
        }                                                                                       \
        if (!s.ok()) {                                                                          \
            return s;                                                                           \
        }                                                                                       \
        RETURN_IF_ERROR(iterator->read_union_bitmap(0, seeked_ordinal, &roaring));              \
    } while (0)

#define BITMAP_COMPARE_LessEqualPredicate(s, exact_match, seeked_ordinal, iterator, bitmap, \
                                          roaring)                                          \
    do {                                                                                    \
        if (s.is_not_found()) {                                                             \
            return Status::OK();                                                            \
        }                                                                                   \
        if (!s.ok()) {                                                                      \
            return s;                                                                       \
        }                                                                                   \
        if (exact_match) {                                                                  \
            seeked_ordinal++;                                                               \
        }                                                                                   \
        RETURN_IF_ERROR(iterator->read_union_bitmap(0, seeked_ordinal, &roaring));          \
    } while (0)

#define BITMAP_COMPARE_GreaterPredicate(s, exact_match, seeked_ordinal, iterator, bitmap, roaring) \
    do {                                                                                           \
        if (!s.is_not_found()) {                                                                   \
            if (!s.ok()) {                                                                         \
                return s;                                                                          \
            }                                                                                      \
            if (exact_match) {                                                                     \
                seeked_ordinal++;                                                                  \
            }                                                                                      \
            RETURN_IF_ERROR(iterator->read_union_bitmap(seeked_ordinal, ordinal_limit, &roaring)); \
        }                                                                                          \
    } while (0)

#define BITMAP_COMPARE_GreaterEqualPredicate(s, exact_match, seeked_ordinal, iterator, bitmap,     \
                                             roaring)                                              \
    do {                                                                                           \
        if (!s.is_not_found()) {                                                                   \
            if (!s.ok()) {                                                                         \
                return s;                                                                          \
            }                                                                                      \
            RETURN_IF_ERROR(iterator->read_union_bitmap(seeked_ordinal, ordinal_limit, &roaring)); \
        }                                                                                          \
    } while (0)

#define BITMAP_COMPARE(CLASS, s, exact_match, seeked_ordinal, iterator, bitmap, roaring) \
    BITMAP_COMPARE_##CLASS(s, exact_match, seeked_ordinal, iterator, bitmap, roaring)

#define COMPARISON_PRED_BITMAP_EVALUATE(CLASS, OP)                                        \
    template <class T>                                                                    \
    Status CLASS<T>::evaluate(const Schema& schema,                                       \
                              const std::vector<BitmapIndexIterator*>& iterators,         \
                              uint32_t num_rows, roaring::Roaring* bitmap) const {        \
        BitmapIndexIterator* iterator = iterators[_column_id];                            \
        if (iterator == nullptr) {                                                        \
            return Status::OK();                                                          \
        }                                                                                 \
        rowid_t ordinal_limit = iterator->bitmap_nums();                                  \
        if (iterator->has_null_bitmap()) {                                                \
            ordinal_limit--;                                                              \
            roaring::Roaring null_bitmap;                                                 \
            RETURN_IF_ERROR(iterator->read_null_bitmap(&null_bitmap));                    \
            *bitmap -= null_bitmap;                                                       \
        }                                                                                 \
        roaring::Roaring roaring;                                                         \
        bool exact_match;                                                                 \
        Status s = iterator->seek_dictionary(&_value, &exact_match);                      \
        rowid_t seeked_ordinal = iterator->current_ordinal();                             \
        BITMAP_COMPARE(CLASS, s, exact_match, seeked_ordinal, iterator, bitmap, roaring); \
        *bitmap &= roaring;                                                               \
        return Status::OK();                                                              \
    }

COMPARISON_PRED_BITMAP_EVALUATE(EqualPredicate, ==)
COMPARISON_PRED_BITMAP_EVALUATE(NotEqualPredicate, !=)
COMPARISON_PRED_BITMAP_EVALUATE(LessPredicate, <)
COMPARISON_PRED_BITMAP_EVALUATE(LessEqualPredicate, <=)
COMPARISON_PRED_BITMAP_EVALUATE(GreaterPredicate, >)
COMPARISON_PRED_BITMAP_EVALUATE(GreaterEqualPredicate, >=)

#define COMPARISON_PRED_CONSTRUCTOR_DECLARATION(CLASS)                                         \
    template CLASS<int8_t>::CLASS(uint32_t column_id, const int8_t& value, bool opposite);     \
    template CLASS<int16_t>::CLASS(uint32_t column_id, const int16_t& value, bool opposite);   \
    template CLASS<int32_t>::CLASS(uint32_t column_id, const int32_t& value, bool opposite);   \
    template CLASS<int64_t>::CLASS(uint32_t column_id, const int64_t& value, bool opposite);   \
    template CLASS<int128_t>::CLASS(uint32_t column_id, const int128_t& value, bool opposite); \
    template CLASS<float>::CLASS(uint32_t column_id, const float& value, bool opposite);       \
    template CLASS<double>::CLASS(uint32_t column_id, const double& value, bool opposite);     \
    template CLASS<decimal12_t>::CLASS(uint32_t column_id, const decimal12_t& value,           \
                                       bool opposite);                                         \
    template CLASS<uint24_t>::CLASS(uint32_t column_id, const uint24_t& value, bool opposite); \
    template CLASS<uint64_t>::CLASS(uint32_t column_id, const uint64_t& value, bool opposite); \
    template CLASS<bool>::CLASS(uint32_t column_id, const bool& value, bool opposite);

COMPARISON_PRED_CONSTRUCTOR_DECLARATION(EqualPredicate)
COMPARISON_PRED_CONSTRUCTOR_DECLARATION(NotEqualPredicate)
COMPARISON_PRED_CONSTRUCTOR_DECLARATION(LessPredicate)
COMPARISON_PRED_CONSTRUCTOR_DECLARATION(LessEqualPredicate)
COMPARISON_PRED_CONSTRUCTOR_DECLARATION(GreaterPredicate)
COMPARISON_PRED_CONSTRUCTOR_DECLARATION(GreaterEqualPredicate)

#define COMPARISON_PRED_EVALUATE_DECLARATION(CLASS)                              \
    template void CLASS<int8_t>::evaluate(VectorizedRowBatch* batch) const;      \
    template void CLASS<int16_t>::evaluate(VectorizedRowBatch* batch) const;     \
    template void CLASS<int32_t>::evaluate(VectorizedRowBatch* batch) const;     \
    template void CLASS<int64_t>::evaluate(VectorizedRowBatch* batch) const;     \
    template void CLASS<int128_t>::evaluate(VectorizedRowBatch* batch) const;    \
    template void CLASS<float>::evaluate(VectorizedRowBatch* batch) const;       \
    template void CLASS<double>::evaluate(VectorizedRowBatch* batch) const;      \
    template void CLASS<decimal12_t>::evaluate(VectorizedRowBatch* batch) const; \
    template void CLASS<StringValue>::evaluate(VectorizedRowBatch* batch) const; \
    template void CLASS<uint24_t>::evaluate(VectorizedRowBatch* batch) const;    \
    template void CLASS<uint64_t>::evaluate(VectorizedRowBatch* batch) const;    \
    template void CLASS<bool>::evaluate(VectorizedRowBatch* batch) const;

COMPARISON_PRED_EVALUATE_DECLARATION(EqualPredicate)
COMPARISON_PRED_EVALUATE_DECLARATION(NotEqualPredicate)
COMPARISON_PRED_EVALUATE_DECLARATION(LessPredicate)
COMPARISON_PRED_EVALUATE_DECLARATION(LessEqualPredicate)
COMPARISON_PRED_EVALUATE_DECLARATION(GreaterPredicate)
COMPARISON_PRED_EVALUATE_DECLARATION(GreaterEqualPredicate)

#define COMPARISON_PRED_COLUMN_BLOCK_EVALUATE_DECLARATION(CLASS)                                   \
    template void CLASS<int8_t>::evaluate(ColumnBlock* block, uint16_t* sel, uint16_t* size)       \
            const;                                                                                 \
    template void CLASS<int16_t>::evaluate(ColumnBlock* block, uint16_t* sel, uint16_t* size)      \
            const;                                                                                 \
    template void CLASS<int32_t>::evaluate(ColumnBlock* block, uint16_t* sel, uint16_t* size)      \
            const;                                                                                 \
    template void CLASS<int64_t>::evaluate(ColumnBlock* block, uint16_t* sel, uint16_t* size)      \
            const;                                                                                 \
    template void CLASS<int128_t>::evaluate(ColumnBlock* block, uint16_t* sel, uint16_t* size)     \
            const;                                                                                 \
    template void CLASS<float>::evaluate(ColumnBlock* block, uint16_t* sel, uint16_t* size) const; \
    template void CLASS<double>::evaluate(ColumnBlock* block, uint16_t* sel, uint16_t* size)       \
            const;                                                                                 \
    template void CLASS<decimal12_t>::evaluate(ColumnBlock* block, uint16_t* sel, uint16_t* size)  \
            const;                                                                                 \
    template void CLASS<StringValue>::evaluate(ColumnBlock* block, uint16_t* sel, uint16_t* size)  \
            const;                                                                                 \
    template void CLASS<uint24_t>::evaluate(ColumnBlock* block, uint16_t* sel, uint16_t* size)     \
            const;                                                                                 \
    template void CLASS<uint64_t>::evaluate(ColumnBlock* block, uint16_t* sel, uint16_t* size)     \
            const;                                                                                 \
    template void CLASS<bool>::evaluate(ColumnBlock* block, uint16_t* sel, uint16_t* size) const;

COMPARISON_PRED_COLUMN_BLOCK_EVALUATE_DECLARATION(EqualPredicate)
COMPARISON_PRED_COLUMN_BLOCK_EVALUATE_DECLARATION(NotEqualPredicate)
COMPARISON_PRED_COLUMN_BLOCK_EVALUATE_DECLARATION(LessPredicate)
COMPARISON_PRED_COLUMN_BLOCK_EVALUATE_DECLARATION(LessEqualPredicate)
COMPARISON_PRED_COLUMN_BLOCK_EVALUATE_DECLARATION(GreaterPredicate)
COMPARISON_PRED_COLUMN_BLOCK_EVALUATE_DECLARATION(GreaterEqualPredicate)

#define COMPARISON_PRED_BITMAP_EVALUATE_DECLARATION(CLASS)                                        \
    template Status CLASS<int8_t>::evaluate(const Schema& schema,                                 \
                                            const std::vector<BitmapIndexIterator*>& iterators,   \
                                            uint32_t num_rows, roaring::Roaring* bitmap) const;   \
    template Status CLASS<int16_t>::evaluate(const Schema& schema,                                \
                                             const std::vector<BitmapIndexIterator*>& iterators,  \
                                             uint32_t num_rows, roaring::Roaring* bitmap) const;  \
    template Status CLASS<int32_t>::evaluate(const Schema& schema,                                \
                                             const std::vector<BitmapIndexIterator*>& iterators,  \
                                             uint32_t num_rows, roaring::Roaring* bitmap) const;  \
    template Status CLASS<int64_t>::evaluate(const Schema& schema,                                \
                                             const std::vector<BitmapIndexIterator*>& iterators,  \
                                             uint32_t num_rows, roaring::Roaring* bitmap) const;  \
    template Status CLASS<int128_t>::evaluate(const Schema& schema,                               \
                                              const std::vector<BitmapIndexIterator*>& iterators, \
                                              uint32_t num_rows, roaring::Roaring* bitmap) const; \
    template Status CLASS<float>::evaluate(const Schema& schema,                                  \
                                           const std::vector<BitmapIndexIterator*>& iterators,    \
                                           uint32_t num_rows, roaring::Roaring* bitmap) const;    \
    template Status CLASS<double>::evaluate(const Schema& schema,                                 \
                                            const std::vector<BitmapIndexIterator*>& iterators,   \
                                            uint32_t num_rows, roaring::Roaring* bitmap) const;   \
    template Status CLASS<decimal12_t>::evaluate(                                                 \
            const Schema& schema, const std::vector<BitmapIndexIterator*>& iterators,             \
            uint32_t num_rows, roaring::Roaring* bitmap) const;                                   \
    template Status CLASS<StringValue>::evaluate(                                                 \
            const Schema& schema, const std::vector<BitmapIndexIterator*>& iterators,             \
            uint32_t num_rows, roaring::Roaring* bitmap) const;                                   \
    template Status CLASS<uint24_t>::evaluate(const Schema& schema,                               \
                                              const std::vector<BitmapIndexIterator*>& iterators, \
                                              uint32_t num_rows, roaring::Roaring* bitmap) const; \
    template Status CLASS<uint64_t>::evaluate(const Schema& schema,                               \
                                              const std::vector<BitmapIndexIterator*>& iterators, \
                                              uint32_t num_rows, roaring::Roaring* bitmap) const; \
    template Status CLASS<bool>::evaluate(const Schema& schema,                                   \
                                          const std::vector<BitmapIndexIterator*>& iterators,     \
                                          uint32_t num_rows, roaring::Roaring* bitmap) const;

COMPARISON_PRED_BITMAP_EVALUATE_DECLARATION(EqualPredicate)
COMPARISON_PRED_BITMAP_EVALUATE_DECLARATION(NotEqualPredicate)
COMPARISON_PRED_BITMAP_EVALUATE_DECLARATION(LessPredicate)
COMPARISON_PRED_BITMAP_EVALUATE_DECLARATION(LessEqualPredicate)
COMPARISON_PRED_BITMAP_EVALUATE_DECLARATION(GreaterPredicate)
COMPARISON_PRED_BITMAP_EVALUATE_DECLARATION(GreaterEqualPredicate)

#define COMPARISON_PRED_COLUMN_EVALUATE_DECLARATION(CLASS)                                 \
    template void CLASS<int8_t>::evaluate(vectorized::IColumn& column, uint16_t* sel,      \
                                          uint16_t* size) const;                           \
    template void CLASS<int16_t>::evaluate(vectorized::IColumn& column, uint16_t* sel,     \
                                           uint16_t* size) const;                          \
    template void CLASS<int32_t>::evaluate(vectorized::IColumn& column, uint16_t* sel,     \
                                           uint16_t* size) const;                          \
    template void CLASS<int64_t>::evaluate(vectorized::IColumn& column, uint16_t* sel,     \
                                           uint16_t* size) const;                          \
    template void CLASS<int128_t>::evaluate(vectorized::IColumn& column, uint16_t* sel,    \
                                            uint16_t* size) const;                         \
    template void CLASS<float>::evaluate(vectorized::IColumn& column, uint16_t* sel,       \
                                         uint16_t* size) const;                            \
    template void CLASS<double>::evaluate(vectorized::IColumn& column, uint16_t* sel,      \
                                          uint16_t* size) const;                           \
    template void CLASS<decimal12_t>::evaluate(vectorized::IColumn& column, uint16_t* sel, \
                                               uint16_t* size) const;                      \
    template void CLASS<StringValue>::evaluate(vectorized::IColumn& column, uint16_t* sel, \
                                               uint16_t* size) const;                      \
    template void CLASS<uint24_t>::evaluate(vectorized::IColumn& column, uint16_t* sel,    \
                                            uint16_t* size) const;                         \
    template void CLASS<uint64_t>::evaluate(vectorized::IColumn& column, uint16_t* sel,    \
                                            uint16_t* size) const;                         \
    template void CLASS<bool>::evaluate(vectorized::IColumn& column, uint16_t* sel,        \
                                        uint16_t* size) const;

COMPARISON_PRED_COLUMN_EVALUATE_DECLARATION(EqualPredicate)
COMPARISON_PRED_COLUMN_EVALUATE_DECLARATION(NotEqualPredicate)
COMPARISON_PRED_COLUMN_EVALUATE_DECLARATION(LessPredicate)
COMPARISON_PRED_COLUMN_EVALUATE_DECLARATION(LessEqualPredicate)
COMPARISON_PRED_COLUMN_EVALUATE_DECLARATION(GreaterPredicate)
COMPARISON_PRED_COLUMN_EVALUATE_DECLARATION(GreaterEqualPredicate)

#define COMPARISON_PRED_COLUMN_EVALUATE_VEC_DECLARATION(CLASS)                                 \
    template void CLASS<int8_t>::evaluate_vec(vectorized::IColumn& column, uint16_t size,      \
                                              bool* flags) const;                              \
    template void CLASS<int16_t>::evaluate_vec(vectorized::IColumn& column, uint16_t size,     \
                                               bool* flags) const;                             \
    template void CLASS<int32_t>::evaluate_vec(vectorized::IColumn& column, uint16_t size,     \
                                               bool* flags) const;                             \
    template void CLASS<int64_t>::evaluate_vec(vectorized::IColumn& column, uint16_t size,     \
                                               bool* flags) const;                             \
    template void CLASS<int128_t>::evaluate_vec(vectorized::IColumn& column, uint16_t size,    \
                                                bool* flags) const;                            \
    template void CLASS<float>::evaluate_vec(vectorized::IColumn& column, uint16_t size,       \
                                             bool* flags) const;                               \
    template void CLASS<double>::evaluate_vec(vectorized::IColumn& column, uint16_t size,      \
                                              bool* flags) const;                              \
    template void CLASS<decimal12_t>::evaluate_vec(vectorized::IColumn& column, uint16_t size, \
                                                   bool* flags) const;                         \
    template void CLASS<StringValue>::evaluate_vec(vectorized::IColumn& column, uint16_t size, \
                                                   bool* flags) const;                         \
    template void CLASS<uint24_t>::evaluate_vec(vectorized::IColumn& column, uint16_t size,    \
                                                bool* flags) const;                            \
    template void CLASS<uint64_t>::evaluate_vec(vectorized::IColumn& column, uint16_t size,    \
                                                bool* flags) const;                            \
    template void CLASS<bool>::evaluate_vec(vectorized::IColumn& column, uint16_t size,        \
                                            bool* flags) const;

COMPARISON_PRED_COLUMN_EVALUATE_VEC_DECLARATION(EqualPredicate)
COMPARISON_PRED_COLUMN_EVALUATE_VEC_DECLARATION(NotEqualPredicate)
COMPARISON_PRED_COLUMN_EVALUATE_VEC_DECLARATION(LessPredicate)
COMPARISON_PRED_COLUMN_EVALUATE_VEC_DECLARATION(LessEqualPredicate)
COMPARISON_PRED_COLUMN_EVALUATE_VEC_DECLARATION(GreaterPredicate)
COMPARISON_PRED_COLUMN_EVALUATE_VEC_DECLARATION(GreaterEqualPredicate)

} //namespace doris