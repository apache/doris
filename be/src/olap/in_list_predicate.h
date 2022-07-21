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

#include <parallel_hashmap/phmap.h>
#include <stdint.h>

#include <roaring/roaring.hh>
#include <type_traits>

#include "decimal12.h"
#include "olap/column_predicate.h"
#include "runtime/string_value.h"
#include "uint24.h"
#include "vec/columns/column_dictionary.h"
#include "vec/core/types.h"

namespace std {
// for string value
template <>
struct hash<doris::StringValue> {
    uint64_t operator()(const doris::StringValue& rhs) const { return hash_value(rhs); }
};

template <>
struct equal_to<doris::StringValue> {
    bool operator()(const doris::StringValue& lhs, const doris::StringValue& rhs) const {
        return lhs == rhs;
    }
};
// for decimal12_t
template <>
struct hash<doris::decimal12_t> {
    int64_t operator()(const doris::decimal12_t& rhs) const {
        return hash<int64_t>()(rhs.integer) ^ hash<int32_t>()(rhs.fraction);
    }
};

template <>
struct equal_to<doris::decimal12_t> {
    bool operator()(const doris::decimal12_t& lhs, const doris::decimal12_t& rhs) const {
        return lhs == rhs;
    }
};
// for uint24_t
template <>
struct hash<doris::uint24_t> {
    size_t operator()(const doris::uint24_t& rhs) const {
        uint32_t val(rhs);
        return hash<int>()(val);
    }
};

template <>
struct equal_to<doris::uint24_t> {
    bool operator()(const doris::uint24_t& lhs, const doris::uint24_t& rhs) const {
        return lhs == rhs;
    }
};
} // namespace std

namespace doris {

template <class T, PredicateType PT>
class InListPredicateBase : public ColumnPredicate {
public:
    InListPredicateBase(uint32_t column_id, phmap::flat_hash_set<T>&& values,
                        bool is_opposite = false)
            : ColumnPredicate(column_id, is_opposite), _values(std::move(values)) {}

    PredicateType type() const override { return PT; }

    void evaluate(ColumnBlock* block, uint16_t* sel, uint16_t* size) const override {
        if (block->is_nullable()) {
            _base_evaluate<true>(block, sel, size);
        } else {
            _base_evaluate<false>(block, sel, size);
        }
    }

    void evaluate_or(ColumnBlock* block, uint16_t* sel, uint16_t size, bool* flags) const override {
        if (block->is_nullable()) {
            _base_evaluate<true, false>(block, sel, size, flags);
        } else {
            _base_evaluate<false, false>(block, sel, size, flags);
        }
    }

    void evaluate_and(ColumnBlock* block, uint16_t* sel, uint16_t size,
                      bool* flags) const override {
        if (block->is_nullable()) {
            _base_evaluate<true, true>(block, sel, size, flags);
        } else {
            _base_evaluate<false, true>(block, sel, size, flags);
        }
    }

    Status evaluate(const Schema& schema, const std::vector<BitmapIndexIterator*>& iterators,
                    uint32_t num_rows, roaring::Roaring* result) const override {
        BitmapIndexIterator* iterator = iterators[_column_id];
        if (iterator == nullptr) {
            return Status::OK();
        }
        if (iterator->has_null_bitmap()) {
            roaring::Roaring null_bitmap;
            RETURN_IF_ERROR(iterator->read_null_bitmap(&null_bitmap));
            *result -= null_bitmap;
        }
        roaring::Roaring indices;
        for (auto value : _values) {
            bool exact_match;
            Status s = iterator->seek_dictionary(&value, &exact_match);
            rowid_t seeked_ordinal = iterator->current_ordinal();
            if (!s.is_not_found()) {
                if (!s.ok()) {
                    return s;
                }
                if (exact_match) {
                    roaring::Roaring index;
                    RETURN_IF_ERROR(iterator->read_bitmap(seeked_ordinal, &index));
                    indices |= index;
                }
            }
        }

        if constexpr (PT == PredicateType::IN_LIST) {
            *result &= indices;
        } else {
            *result -= indices;
        }

        return Status::OK();
    }

    uint16_t evaluate(const vectorized::IColumn& column, uint16_t* sel,
                      uint16_t size) const override {
        if (column.is_nullable()) {
            auto* nullable_col =
                    vectorized::check_and_get_column<vectorized::ColumnNullable>(column);
            auto& null_bitmap = reinterpret_cast<const vectorized::ColumnUInt8&>(
                                        nullable_col->get_null_map_column())
                                        .get_data();
            auto& nested_col = nullable_col->get_nested_column();

            if (_opposite) {
                return _base_evaluate<true, true>(&nested_col, &null_bitmap, sel, size);
            } else {
                return _base_evaluate<true, false>(&nested_col, &null_bitmap, sel, size);
            }
        } else {
            if (_opposite) {
                return _base_evaluate<false, true>(&column, nullptr, sel, size);
            } else {
                return _base_evaluate<false, false>(&column, nullptr, sel, size);
            }
        }
    }

    // todo(wb) support evaluate_and,evaluate_or
    void evaluate_and(const vectorized::IColumn& column, const uint16_t* sel, uint16_t size,
                      bool* flags) const override {
        LOG(FATAL) << "IColumn not support in_list_predicate.evaluate_and now.";
    }
    void evaluate_or(const vectorized::IColumn& column, const uint16_t* sel, uint16_t size,
                     bool* flags) const override {
        LOG(FATAL) << "IColumn not support in_list_predicate.evaluate_or now.";
    }

private:
    template <typename LeftT, typename RightT>
    bool _operator(const LeftT& lhs, const RightT& rhs) const {
        if constexpr (PT == PredicateType::IN_LIST) {
            return lhs != rhs;
        }
        return lhs == rhs;
    }

    template <bool is_nullable>
    void _base_evaluate(const ColumnBlock* block, uint16_t* sel, uint16_t* size) const {
        uint16_t new_size = 0;
        for (uint16_t i = 0; i < *size; ++i) {
            uint16_t idx = sel[i];
            sel[new_size] = idx;
            const T* cell_value = reinterpret_cast<const T*>(block->cell(idx).cell_ptr());
            if constexpr (is_nullable) {
                new_size += _opposite ^ (!block->cell(idx).is_null() &&
                                         _operator(_values.find(*cell_value), _values.end()));
            } else {
                new_size += _opposite ^ _operator(_values.find(*cell_value), _values.end());
            }
        }
        *size = new_size;
    }

    template <bool is_nullable, bool is_and>
    void _base_evaluate(const ColumnBlock* block, const uint16_t* sel, uint16_t size,
                        bool* flags) const {
        for (uint16_t i = 0; i < size; ++i) {
            if (!flags[i]) {
                continue;
            }

            uint16_t idx = sel[i];
            const T* cell_value = reinterpret_cast<const T*>(block->cell(idx).cell_ptr());
            auto result = true;
            if constexpr (is_nullable) {
                result &= !block->cell(idx).is_null();
            }
            result &= _operator(_values.find(*cell_value), _values.end());

            if constexpr (is_and) {
                flags[i] &= _opposite ^ result;
            } else {
                flags[i] |= _opposite ^ result;
            }
        }
    }

    template <bool is_nullable, bool is_opposite>
    uint16_t _base_evaluate(const vectorized::IColumn* column,
                            const vectorized::PaddedPODArray<vectorized::UInt8>* null_map,
                            uint16_t* sel, uint16_t size) const {
        uint16_t new_size = 0;

        if constexpr (std::is_same_v<T, uint24_t>) {
            auto* nested_col_ptr =
                    vectorized::check_and_get_column<vectorized::PredicateColumnType<uint32_t>>(
                            column);
            auto& data_array = nested_col_ptr->get_data();

            uint24_t tmp_uint24_value;
            for (uint16_t i = 0; i < size; i++) {
                uint16_t idx = sel[i];
                if constexpr (is_nullable) {
                    if ((*null_map)[idx]) {
                        if constexpr (is_opposite) {
                            sel[new_size++] = idx;
                        }
                        continue;
                    }
                }

                memcpy((char*)(&tmp_uint24_value), (char*)(&(data_array[idx])), sizeof(uint24_t));
                if constexpr (!is_opposite) {
                    if (_operator(_values.find(tmp_uint24_value), _values.end())) {
                        sel[new_size++] = idx;
                    }
                } else {
                    if (!_operator(_values.find(tmp_uint24_value), _values.end())) {
                        sel[new_size++] = idx;
                    }
                }
            }

        } else if (column->is_column_dictionary()) {
            if constexpr (std::is_same_v<T, StringValue>) {
                auto* nested_col_ptr = vectorized::check_and_get_column<
                        vectorized::ColumnDictionary<vectorized::Int32>>(column);
                auto& data_array = nested_col_ptr->get_data();
                nested_col_ptr->find_codes(_values, _value_in_dict_flags);

                for (uint16_t i = 0; i < size; i++) {
                    uint16_t idx = sel[i];
                    if constexpr (is_nullable) {
                        if ((*null_map)[idx]) {
                            if constexpr (is_opposite) {
                                sel[new_size++] = idx;
                            }
                            continue;
                        }
                    }

                    if constexpr (is_opposite != (PT == PredicateType::IN_LIST)) {
                        if (_value_in_dict_flags[data_array[idx]]) {
                            sel[new_size++] = idx;
                        }
                    } else {
                        if (!_value_in_dict_flags[data_array[idx]]) {
                            sel[new_size++] = idx;
                        }
                    }
                }
            } else {
                LOG(FATAL) << "column_dictionary must use StringValue predicate.";
            }
        } else {
            auto* nested_col_ptr =
                    vectorized::check_and_get_column<vectorized::PredicateColumnType<T>>(column);
            auto& data_array = nested_col_ptr->get_data();

            for (uint16_t i = 0; i < size; i++) {
                uint16_t idx = sel[i];
                if constexpr (is_nullable) {
                    if ((*null_map)[idx]) {
                        if constexpr (is_opposite) {
                            sel[new_size++] = idx;
                        }
                        continue;
                    }
                }

                if constexpr (!is_opposite) {
                    if (_operator(_values.find(reinterpret_cast<const T&>(data_array[idx])),
                                  _values.end())) {
                        sel[new_size++] = idx;
                    }
                } else {
                    if (!_operator(_values.find(reinterpret_cast<const T&>(data_array[idx])),
                                   _values.end())) {
                        sel[new_size++] = idx;
                    }
                }
            }
        }

        return new_size;
    }

    phmap::flat_hash_set<T> _values;
    mutable std::vector<vectorized::UInt8> _value_in_dict_flags;
};

template <class T>
using InListPredicate = InListPredicateBase<T, PredicateType::IN_LIST>;

template <class T>
using NotInListPredicate = InListPredicateBase<T, PredicateType::NOT_IN_LIST>;

} //namespace doris
