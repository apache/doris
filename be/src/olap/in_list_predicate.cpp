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
#include "vec/columns/predicate_column.h"
#include "vec/columns/column_nullable.h"

namespace doris {

#define IN_LIST_PRED_CONSTRUCTOR(CLASS)                                                        \
    template <class type>                                                                      \
    CLASS<type>::CLASS(uint32_t column_id, phmap::flat_hash_set<type>&& values, bool opposite) \
            : ColumnPredicate(column_id, opposite), _values(std::move(values)) {}

IN_LIST_PRED_CONSTRUCTOR(InListPredicate)
IN_LIST_PRED_CONSTRUCTOR(NotInListPredicate)

#define IN_LIST_PRED_EVALUATE(CLASS, OP)                                                       \
    template <class type>                                                                      \
    void CLASS<type>::evaluate(VectorizedRowBatch* batch) const {                              \
        uint16_t n = batch->size();                                                            \
        if (n == 0) {                                                                          \
            return;                                                                            \
        }                                                                                      \
        uint16_t* sel = batch->selected();                                                     \
        const type* col_vector =                                                               \
                reinterpret_cast<const type*>(batch->column(_column_id)->col_data());          \
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
    template <class type>                                                                 \
    void CLASS<type>::evaluate(ColumnBlock* block, uint16_t* sel, uint16_t* size) const { \
        uint16_t new_size = 0;                                                            \
        if (block->is_nullable()) {                                                       \
            for (uint16_t i = 0; i < *size; ++i) {                                        \
                uint16_t idx = sel[i];                                                    \
                sel[new_size] = idx;                                                      \
                const type* cell_value =                                                  \
                        reinterpret_cast<const type*>(block->cell(idx).cell_ptr());       \
                auto result = (!block->cell(idx).is_null() && _values.find(*cell_value)   \
                                                                      OP _values.end());  \
                new_size += _opposite ? !result : result;                                 \
            }                                                                             \
        } else {                                                                          \
            for (uint16_t i = 0; i < *size; ++i) {                                        \
                uint16_t idx = sel[i];                                                    \
                sel[new_size] = idx;                                                      \
                const type* cell_value =                                                  \
                        reinterpret_cast<const type*>(block->cell(idx).cell_ptr());       \
                auto result = (_values.find(*cell_value) OP _values.end());               \
                new_size += _opposite ? !result : result;                                 \
            }                                                                             \
        }                                                                                 \
        *size = new_size;                                                                 \
    }

IN_LIST_PRED_COLUMN_BLOCK_EVALUATE(InListPredicate, !=)
IN_LIST_PRED_COLUMN_BLOCK_EVALUATE(NotInListPredicate, ==)

// todo(zeno) need to support ColumnDictionary
#define IN_LIST_PRED_COLUMN_EVALUATE(CLASS, OP)                                                    \
    template <class type>                                                                          \
    void CLASS<type>::evaluate(vectorized::IColumn& column, uint16_t* sel, uint16_t* size) const { \
        uint16_t new_size = 0;                                                                     \
        if (column.is_nullable()) {                                                                \
            auto* nullable_column =                                                                \
                vectorized::check_and_get_column<vectorized::ColumnNullable>(column);              \
            auto& null_bitmap = reinterpret_cast<const vectorized::ColumnVector<uint8_t>&>(*(      \
                nullable_column->get_null_map_column_ptr())).get_data();                           \
            auto* nest_column_vector = vectorized::check_and_get_column                            \
                <vectorized::PredicateColumnType<type>>(nullable_column->get_nested_column());     \
            auto& data_array = nest_column_vector->get_data();                                     \
            for (uint16_t i = 0; i < *size; i++) {                                                 \
                uint16_t idx = sel[i];                                                             \
                sel[new_size] = idx;                                                               \
                const type& cell_value = reinterpret_cast<const type&>(data_array[idx]);           \
                bool ret = !null_bitmap[idx] && (_values.find(cell_value) OP _values.end());       \
                new_size += _opposite ? !ret : ret;                                                \
            }                                                                                      \
            *size = new_size;                                                                      \
        } else {                                                                                   \
            auto& number_column = reinterpret_cast<vectorized::PredicateColumnType<type>&>(column);\
            auto& data_array = number_column.get_data();                                           \
            for (uint16_t i = 0; i < *size; i++) {                                                 \
                uint16_t idx = sel[i];                                                             \
                sel[new_size] = idx;                                                               \
                const type& cell_value = reinterpret_cast<const type&>(data_array[idx]);           \
                auto result = (_values.find(cell_value) OP _values.end());                         \
                new_size += _opposite ? !result : result;                                          \
            }                                                                                      \
        }                                                                                          \
        *size = new_size;                                                                          \
    }

IN_LIST_PRED_COLUMN_EVALUATE(InListPredicate, !=)
IN_LIST_PRED_COLUMN_EVALUATE(NotInListPredicate, ==)

#define IN_LIST_PRED_COLUMN_BLOCK_EVALUATE_OR(CLASS, OP)                                         \
    template <class type>                                                                        \
    void CLASS<type>::evaluate_or(ColumnBlock* block, uint16_t* sel, uint16_t size, bool* flags) \
            const {                                                                              \
        if (block->is_nullable()) {                                                              \
            for (uint16_t i = 0; i < size; ++i) {                                                \
                if (flags[i]) continue;                                                          \
                uint16_t idx = sel[i];                                                           \
                const type* cell_value =                                                         \
                        reinterpret_cast<const type*>(block->cell(idx).cell_ptr());              \
                auto result = (!block->cell(idx).is_null() && _values.find(*cell_value)          \
                                                                      OP _values.end());         \
                flags[i] |= _opposite ? !result : result;                                        \
            }                                                                                    \
        } else {                                                                                 \
            for (uint16_t i = 0; i < size; ++i) {                                                \
                if (flags[i]) continue;                                                          \
                uint16_t idx = sel[i];                                                           \
                const type* cell_value =                                                         \
                        reinterpret_cast<const type*>(block->cell(idx).cell_ptr());              \
                auto result = (_values.find(*cell_value) OP _values.end());                      \
                flags[i] |= _opposite ? !result : result;                                        \
            }                                                                                    \
        }                                                                                        \
    }

IN_LIST_PRED_COLUMN_BLOCK_EVALUATE_OR(InListPredicate, !=)
IN_LIST_PRED_COLUMN_BLOCK_EVALUATE_OR(NotInListPredicate, ==)

#define IN_LIST_PRED_COLUMN_BLOCK_EVALUATE_AND(CLASS, OP)                                         \
    template <class type>                                                                         \
    void CLASS<type>::evaluate_and(ColumnBlock* block, uint16_t* sel, uint16_t size, bool* flags) \
            const {                                                                               \
        if (block->is_nullable()) {                                                               \
            for (uint16_t i = 0; i < size; ++i) {                                                 \
                if (!flags[i]) continue;                                                          \
                uint16_t idx = sel[i];                                                            \
                const type* cell_value =                                                          \
                        reinterpret_cast<const type*>(block->cell(idx).cell_ptr());               \
                auto result = (!block->cell(idx).is_null() && _values.find(*cell_value)           \
                                                                      OP _values.end());          \
                flags[i] &= _opposite ? !result : result;                                         \
            }                                                                                     \
        } else {                                                                                  \
            for (uint16_t i = 0; i < size; ++i) {                                                 \
                if (!flags[i]) continue;                                                          \
                uint16_t idx = sel[i];                                                            \
                const type* cell_value =                                                          \
                        reinterpret_cast<const type*>(block->cell(idx).cell_ptr());               \
                auto result = (_values.find(*cell_value) OP _values.end());                       \
                flags[i] &= _opposite ? !result : result;                                         \
            }                                                                                     \
        }                                                                                         \
    }

IN_LIST_PRED_COLUMN_BLOCK_EVALUATE_AND(InListPredicate, !=)
IN_LIST_PRED_COLUMN_BLOCK_EVALUATE_AND(NotInListPredicate, ==)

#define IN_LIST_PRED_BITMAP_EVALUATE(CLASS, OP)                                       \
    template <class type>                                                             \
    Status CLASS<type>::evaluate(const Schema& schema,                                \
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

} //namespace doris
