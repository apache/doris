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
#include "vec/columns/column_dictionary.h"
#include "vec/columns/predicate_column.h"
#include "vec/columns/column_nullable.h"

namespace doris {

#define IN_LIST_PRED_CONSTRUCTOR(CLASS)                                                        \
    template <class T>                                                                         \
    CLASS<T>::CLASS(uint32_t column_id, phmap::flat_hash_set<T>&& values, bool opposite)       \
            : ColumnPredicate(column_id, opposite), _values(std::move(values)) {}

IN_LIST_PRED_CONSTRUCTOR(InListPredicate)
IN_LIST_PRED_CONSTRUCTOR(NotInListPredicate)

#define IN_LIST_PRED_EVALUATE(CLASS, OP)                                                       \
    template <class T>                                                                         \
    void CLASS<T>::evaluate(VectorizedRowBatch* batch) const {                                 \
        uint16_t n = batch->size();                                                            \
        if (n == 0) {                                                                          \
            return;                                                                            \
        }                                                                                      \
        uint16_t* sel = batch->selected();                                                     \
        const T* col_vector =                                                                  \
                reinterpret_cast<const T*>(batch->column(_column_id)->col_data());             \
        uint16_t new_size = 0;                                                                 \
        if (batch->column(_column_id)->no_nulls()) {                                           \
            if (batch->selected_in_use()) {                                                    \
                for (uint16_t j = 0; j != n; ++j) {                                            \
                    uint16_t i = sel[j];                                                       \
                    sel[new_size] = i;                                                         \
                    new_size += (_values.find(col_vector[i]) OP _values.end());                \
                }                                                                              \
                batch->set_size(new_size);                                                     \
            } else {                                                                           \
                for (uint16_t i = 0; i != n; ++i) {                                            \
                    sel[new_size] = i;                                                         \
                    new_size += (_values.find(col_vector[i]) OP _values.end());                \
                }                                                                              \
                if (new_size < n) {                                                            \
                    batch->set_size(new_size);                                                 \
                    batch->set_selected_in_use(true);                                          \
                }                                                                              \
            }                                                                                  \
        } else {                                                                               \
            bool* is_null = batch->column(_column_id)->is_null();                              \
            if (batch->selected_in_use()) {                                                    \
                for (uint16_t j = 0; j != n; ++j) {                                            \
                    uint16_t i = sel[j];                                                       \
                    sel[new_size] = i;                                                         \
                    new_size += (!is_null[i] && _values.find(col_vector[i]) OP _values.end()); \
                }                                                                              \
                batch->set_size(new_size);                                                     \
            } else {                                                                           \
                for (int i = 0; i != n; ++i) {                                                 \
                    sel[new_size] = i;                                                         \
                    new_size += (!is_null[i] && _values.find(col_vector[i]) OP _values.end()); \
                }                                                                              \
                if (new_size < n) {                                                            \
                    batch->set_size(new_size);                                                 \
                    batch->set_selected_in_use(true);                                          \
                }                                                                              \
            }                                                                                  \
        }                                                                                      \
    }

IN_LIST_PRED_EVALUATE(InListPredicate, !=)
IN_LIST_PRED_EVALUATE(NotInListPredicate, ==)

#define IN_LIST_PRED_COLUMN_BLOCK_EVALUATE(CLASS, OP)                                     \
    template <class T>                                                                    \
    void CLASS<T>::evaluate(ColumnBlock* block, uint16_t* sel, uint16_t* size) const {    \
        uint16_t new_size = 0;                                                            \
        if (block->is_nullable()) {                                                       \
            for (uint16_t i = 0; i < *size; ++i) {                                        \
                uint16_t idx = sel[i];                                                    \
                sel[new_size] = idx;                                                      \
                const T* cell_value =                                                     \
                        reinterpret_cast<const T*>(block->cell(idx).cell_ptr());          \
                auto result = (!block->cell(idx).is_null() && _values.find(*cell_value)   \
                                                                      OP _values.end());  \
                new_size += _opposite ? !result : result;                                 \
            }                                                                             \
        } else {                                                                          \
            for (uint16_t i = 0; i < *size; ++i) {                                        \
                uint16_t idx = sel[i];                                                    \
                sel[new_size] = idx;                                                      \
                const T* cell_value =                                                     \
                        reinterpret_cast<const T*>(block->cell(idx).cell_ptr());          \
                auto result = (_values.find(*cell_value) OP _values.end());               \
                new_size += _opposite ? !result : result;                                 \
            }                                                                             \
        }                                                                                 \
        *size = new_size;                                                                 \
    }

IN_LIST_PRED_COLUMN_BLOCK_EVALUATE(InListPredicate, !=)
IN_LIST_PRED_COLUMN_BLOCK_EVALUATE(NotInListPredicate, ==)

// todo(zeno) define interface in IColumn to simplify code
#define IN_LIST_PRED_COLUMN_EVALUATE(CLASS, OP)                                                    \
    template <class T>                                                                             \
    void CLASS<T>::evaluate(vectorized::IColumn& column, uint16_t* sel, uint16_t* size) const {    \
        uint16_t new_size = 0;                                                                     \
        if (column.is_nullable()) {                                                                \
            auto* nullable_col =                                                                   \
                    vectorized::check_and_get_column<vectorized::ColumnNullable>(column);          \
            auto& null_bitmap = reinterpret_cast<const vectorized::ColumnUInt8&>(                  \
                                        nullable_col->get_null_map_column()).get_data();           \
            auto& nested_col = nullable_col->get_nested_column();                                  \
            if (nested_col.is_column_dictionary()) {                                               \
                if constexpr (std::is_same_v<T, StringValue>) {                                    \
                    auto* nested_col_ptr = vectorized::check_and_get_column<                       \
                            vectorized::ColumnDictionary<vectorized::Int32>>(nested_col);          \
                    auto& data_array = nested_col_ptr->get_data();                                 \
                    for (uint16_t i = 0; i < *size; i++) {                                         \
                        uint16_t idx = sel[i];                                                     \
                        sel[new_size] = idx;                                                       \
                        const auto& cell_value = data_array[idx];                                  \
                        bool ret = !null_bitmap[idx]                                               \
                                   && (_dict_codes.find(cell_value) OP _dict_codes.end());         \
                        new_size += _opposite ? !ret : ret;                                        \
                    }                                                                              \
                }                                                                                  \
            } else {                                                                               \
                auto* nested_col_ptr = vectorized::check_and_get_column<                           \
                        vectorized::PredicateColumnType<T>>(nested_col);                           \
                auto& data_array = nested_col_ptr->get_data();                                     \
                for (uint16_t i = 0; i < *size; i++) {                                             \
                    uint16_t idx = sel[i];                                                         \
                    sel[new_size] = idx;                                                           \
                    const auto& cell_value = reinterpret_cast<const T&>(data_array[idx]);          \
                    bool ret = !null_bitmap[idx] && (_values.find(cell_value) OP _values.end());   \
                    new_size += _opposite ? !ret : ret;                                            \
                }                                                                                  \
            }                                                                                      \
            *size = new_size;                                                                      \
        } else if (column.is_column_dictionary()) {                                                \
            if constexpr (std::is_same_v<T, StringValue>) {                                        \
                auto& dict_col =                                                                   \
                        reinterpret_cast<vectorized::ColumnDictionary<vectorized::Int32>&>(        \
                                column);                                                           \
                auto& data_array = dict_col.get_data();                                            \
                for (uint16_t i = 0; i < *size; i++) {                                             \
                    uint16_t idx = sel[i];                                                         \
                    sel[new_size] = idx;                                                           \
                    const auto& cell_value = data_array[idx];                                      \
                    auto result = (_dict_codes.find(cell_value) OP _dict_codes.end());             \
                    new_size += _opposite ? !result : result;                                      \
                }                                                                                  \
            }                                                                                      \
        } else {                                                                                   \
            auto& number_column = reinterpret_cast<vectorized::PredicateColumnType<T>&>(column);   \
            auto& data_array = number_column.get_data();                                           \
            for (uint16_t i = 0; i < *size; i++) {                                                 \
                uint16_t idx = sel[i];                                                             \
                sel[new_size] = idx;                                                               \
                const auto& cell_value = reinterpret_cast<const T&>(data_array[idx]);              \
                auto result = (_values.find(cell_value) OP _values.end());                         \
                new_size += _opposite ? !result : result;                                          \
            }                                                                                      \
        }                                                                                          \
        *size = new_size;                                                                          \
    }

IN_LIST_PRED_COLUMN_EVALUATE(InListPredicate, !=)
IN_LIST_PRED_COLUMN_EVALUATE(NotInListPredicate, ==)

#define IN_LIST_PRED_COLUMN_BLOCK_EVALUATE_OR(CLASS, OP)                                         \
    template <class T>                                                                           \
    void CLASS<T>::evaluate_or(ColumnBlock* block, uint16_t* sel, uint16_t size, bool* flags)    \
            const {                                                                              \
        if (block->is_nullable()) {                                                              \
            for (uint16_t i = 0; i < size; ++i) {                                                \
                if (flags[i]) continue;                                                          \
                uint16_t idx = sel[i];                                                           \
                const T* cell_value =                                                            \
                        reinterpret_cast<const T*>(block->cell(idx).cell_ptr());                 \
                auto result = (!block->cell(idx).is_null() && _values.find(*cell_value)          \
                                                                      OP _values.end());         \
                flags[i] |= _opposite ? !result : result;                                        \
            }                                                                                    \
        } else {                                                                                 \
            for (uint16_t i = 0; i < size; ++i) {                                                \
                if (flags[i]) continue;                                                          \
                uint16_t idx = sel[i];                                                           \
                const T* cell_value =                                                            \
                        reinterpret_cast<const T*>(block->cell(idx).cell_ptr());                 \
                auto result = (_values.find(*cell_value) OP _values.end());                      \
                flags[i] |= _opposite ? !result : result;                                        \
            }                                                                                    \
        }                                                                                        \
    }

IN_LIST_PRED_COLUMN_BLOCK_EVALUATE_OR(InListPredicate, !=)
IN_LIST_PRED_COLUMN_BLOCK_EVALUATE_OR(NotInListPredicate, ==)

#define IN_LIST_PRED_COLUMN_BLOCK_EVALUATE_AND(CLASS, OP)                                         \
    template <class T>                                                                            \
    void CLASS<T>::evaluate_and(ColumnBlock* block, uint16_t* sel, uint16_t size, bool* flags)    \
            const {                                                                               \
        if (block->is_nullable()) {                                                               \
            for (uint16_t i = 0; i < size; ++i) {                                                 \
                if (!flags[i]) continue;                                                          \
                uint16_t idx = sel[i];                                                            \
                const T* cell_value =                                                             \
                        reinterpret_cast<const T*>(block->cell(idx).cell_ptr());                  \
                auto result = (!block->cell(idx).is_null() && _values.find(*cell_value)           \
                                                                      OP _values.end());          \
                flags[i] &= _opposite ? !result : result;                                         \
            }                                                                                     \
        } else {                                                                                  \
            for (uint16_t i = 0; i < size; ++i) {                                                 \
                if (!flags[i]) continue;                                                          \
                uint16_t idx = sel[i];                                                            \
                const T* cell_value =                                                             \
                        reinterpret_cast<const T*>(block->cell(idx).cell_ptr());                  \
                auto result = (_values.find(*cell_value) OP _values.end());                       \
                flags[i] &= _opposite ? !result : result;                                         \
            }                                                                                     \
        }                                                                                         \
    }

IN_LIST_PRED_COLUMN_BLOCK_EVALUATE_AND(InListPredicate, !=)
IN_LIST_PRED_COLUMN_BLOCK_EVALUATE_AND(NotInListPredicate, ==)

#define IN_LIST_PRED_BITMAP_EVALUATE(CLASS, OP)                                       \
    template <class T>                                                                \
    Status CLASS<T>::evaluate(const Schema& schema,                                   \
                                 const std::vector<BitmapIndexIterator*>& iterators,  \
                                 uint32_t num_rows, roaring::Roaring* result) const { \
        BitmapIndexIterator* iterator = iterators[_column_id];                        \
        if (iterator == nullptr) {                                                    \
            return Status::OK();                                                      \
        }                                                                             \
        if (iterator->has_null_bitmap()) {                                            \
            roaring::Roaring null_bitmap;                                             \
            RETURN_IF_ERROR(iterator->read_null_bitmap(&null_bitmap));                \
            *result -= null_bitmap;                                                   \
        }                                                                             \
        roaring::Roaring indices;                                                     \
        for (auto value : _values) {                                                  \
            bool exact_match;                                                         \
            Status s = iterator->seek_dictionary(&value, &exact_match);               \
            rowid_t seeked_ordinal = iterator->current_ordinal();                     \
            if (!s.is_not_found()) {                                                  \
                if (!s.ok()) {                                                        \
                    return s;                                                         \
                }                                                                     \
                if (exact_match) {                                                    \
                    roaring::Roaring index;                                           \
                    RETURN_IF_ERROR(iterator->read_bitmap(seeked_ordinal, &index));   \
                    indices |= index;                                                 \
                }                                                                     \
            }                                                                         \
        }                                                                             \
        *result OP indices;                                                           \
        return Status::OK();                                                          \
    }

IN_LIST_PRED_BITMAP_EVALUATE(InListPredicate, &=)
IN_LIST_PRED_BITMAP_EVALUATE(NotInListPredicate, -=)

#define IN_LIST_PRED_SET_DICT_CODE(CLASS)                                                      \
    template <class T>                                                                         \
    void CLASS<T>::set_dict_code_if_necessary(vectorized::IColumn& column) {                   \
        if (_dict_code_inited) {                                                               \
            return;                                                                            \
        }                                                                                      \
        if constexpr (std::is_same_v<T, StringValue>) {                                        \
            auto* col_ptr = column.get_ptr().get();                                            \
            if (column.is_nullable()) {                                                        \
                auto nullable_col =                                                            \
                        reinterpret_cast<vectorized::ColumnNullable*>(col_ptr);                \
                col_ptr = nullable_col->get_nested_column_ptr().get();                         \
            }                                                                                  \
            if (col_ptr->is_column_dictionary()) {                                             \
                auto& dict_col =                                                               \
                        reinterpret_cast<vectorized::ColumnDictionary<vectorized::Int32>&>(    \
                                *col_ptr);                                                     \
                auto code_set = dict_col.find_codes(_values);                                  \
                _dict_codes = std::move(code_set);                                             \
                _dict_code_inited = true;                                                      \
            }                                                                                  \
        }                                                                                      \
    }

IN_LIST_PRED_SET_DICT_CODE(InListPredicate)
IN_LIST_PRED_SET_DICT_CODE(NotInListPredicate)

#define IN_LIST_PRED_CONSTRUCTOR_DECLARATION(CLASS)                                                \
    template CLASS<int8_t>::CLASS(uint32_t column_id, phmap::flat_hash_set<int8_t>&& values,       \
                                  bool opposite);                                                  \
    template CLASS<int16_t>::CLASS(uint32_t column_id, phmap::flat_hash_set<int16_t>&& values,     \
                                   bool opposite);                                                 \
    template CLASS<int32_t>::CLASS(uint32_t column_id, phmap::flat_hash_set<int32_t>&& values,     \
                                   bool opposite);                                                 \
    template CLASS<int64_t>::CLASS(uint32_t column_id, phmap::flat_hash_set<int64_t>&& values,     \
                                   bool opposite);                                                 \
    template CLASS<int128_t>::CLASS(uint32_t column_id, phmap::flat_hash_set<int128_t>&& values,   \
                                    bool opposite);                                                \
    template CLASS<float>::CLASS(uint32_t column_id, phmap::flat_hash_set<float>&& values,         \
                                 bool opposite);                                                   \
    template CLASS<double>::CLASS(uint32_t column_id, phmap::flat_hash_set<double>&& values,       \
                                  bool opposite);                                                  \
    template CLASS<decimal12_t>::CLASS(uint32_t column_id,                                         \
                                       phmap::flat_hash_set<decimal12_t>&& values, bool opposite); \
    template CLASS<StringValue>::CLASS(uint32_t column_id,                                         \
                                       phmap::flat_hash_set<StringValue>&& values, bool opposite); \
    template CLASS<uint24_t>::CLASS(uint32_t column_id, phmap::flat_hash_set<uint24_t>&& values,   \
                                    bool opposite);                                                \
    template CLASS<uint64_t>::CLASS(uint32_t column_id, phmap::flat_hash_set<uint64_t>&& values,   \
                                    bool opposite);

IN_LIST_PRED_CONSTRUCTOR_DECLARATION(InListPredicate)
IN_LIST_PRED_CONSTRUCTOR_DECLARATION(NotInListPredicate)

#define IN_LIST_PRED_EVALUATE_DECLARATION(CLASS)                                 \
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
    template void CLASS<uint64_t>::evaluate(VectorizedRowBatch* batch) const;

IN_LIST_PRED_EVALUATE_DECLARATION(InListPredicate)
IN_LIST_PRED_EVALUATE_DECLARATION(NotInListPredicate)

#define IN_LIST_PRED_COLUMN_BLOCK_EVALUATE_DECLARATION(CLASS)                                      \
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
            const;

IN_LIST_PRED_COLUMN_BLOCK_EVALUATE_DECLARATION(InListPredicate)
IN_LIST_PRED_COLUMN_BLOCK_EVALUATE_DECLARATION(NotInListPredicate)

#define IN_LIST_PRED_BITMAP_EVALUATE_DECLARATION(CLASS)                                           \
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
                                              uint32_t num_rows, roaring::Roaring* bitmap) const;

IN_LIST_PRED_BITMAP_EVALUATE_DECLARATION(InListPredicate)
IN_LIST_PRED_BITMAP_EVALUATE_DECLARATION(NotInListPredicate)

template void InListPredicate<StringValue>::set_dict_code_if_necessary(vectorized::IColumn& column);
template void NotInListPredicate<StringValue>::set_dict_code_if_necessary(
        vectorized::IColumn& column);

} //namespace doris
