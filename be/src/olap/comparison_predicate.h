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

#include <cstdint>

#include "olap/column_predicate.h"
#include "vec/columns/column_dictionary.h"

namespace doris {

template <class T, PredicateType PT>
class ComparisonPredicateBase : public ColumnPredicate {
public:
    ComparisonPredicateBase(uint32_t column_id, const T& value, bool opposite = false)
            : ColumnPredicate(column_id, opposite), _value(value) {
        if constexpr (std::is_same_v<T, uint24_t>) {
            _value_real = 0;
            memory_copy(&_value_real, _value.get_data(), sizeof(T));
        } else {
            _value_real = _value;
        }
    }

    PredicateType type() const override { return PT; }

    void evaluate(ColumnBlock* block, uint16_t* sel, uint16_t* size) const override {
        uint16_t new_size = 0;
        if (block->is_nullable()) {
            for (uint16_t i = 0; i < *size; ++i) {
                uint16_t idx = sel[i];
                sel[new_size] = idx;
                const T* cell_value = reinterpret_cast<const T*>(block->cell(idx).cell_ptr());
                auto result = (!block->cell(idx).is_null() && _operator(*cell_value, _value));
                new_size += _opposite ? !result : result;
            }
        } else {
            for (uint16_t i = 0; i < *size; ++i) {
                uint16_t idx = sel[i];
                sel[new_size] = idx;
                const T* cell_value = reinterpret_cast<const T*>(block->cell(idx).cell_ptr());
                auto result = _operator(*cell_value, _value);
                new_size += _opposite ? !result : result;
            }
        }
        *size = new_size;
    }

    void evaluate_or(ColumnBlock* block, uint16_t* sel, uint16_t size, bool* flags) const override {
        if (block->is_nullable()) {
            for (uint16_t i = 0; i < size; ++i) {
                if (flags[i]) {
                    continue;
                }
                uint16_t idx = sel[i];
                const T* cell_value = reinterpret_cast<const T*>(block->cell(idx).cell_ptr());
                auto result = (!block->cell(idx).is_null() && _operator(*cell_value, _value));
                flags[i] = flags[i] | (_opposite ? !result : result);
            }
        } else {
            for (uint16_t i = 0; i < size; ++i) {
                if (flags[i]) {
                    continue;
                }
                uint16_t idx = sel[i];
                const T* cell_value = reinterpret_cast<const T*>(block->cell(idx).cell_ptr());
                auto result = _operator(*cell_value, _value);
                flags[i] = flags[i] | (_opposite ? !result : result);
            }
        }
    }

    void evaluate_and(ColumnBlock* block, uint16_t* sel, uint16_t size,
                      bool* flags) const override {
        if (block->is_nullable()) {
            for (uint16_t i = 0; i < size; ++i) {
                if (!flags[i]) {
                    continue;
                }
                uint16_t idx = sel[i];
                const T* cell_value = reinterpret_cast<const T*>(block->cell(idx).cell_ptr());
                auto result = (!block->cell(idx).is_null() && _operator(*cell_value, _value));
                flags[i] = flags[i] & (_opposite ? !result : result);
            }
        } else {
            for (uint16_t i = 0; i < size; ++i) {
                if (flags[i]) {
                    continue;
                }
                uint16_t idx = sel[i];
                const T* cell_value = reinterpret_cast<const T*>(block->cell(idx).cell_ptr());
                auto result = _operator(*cell_value, _value);
                flags[i] = flags[i] & (_opposite ? !result : result);
            }
        }
    }

    Status evaluate(const Schema& schema, const std::vector<BitmapIndexIterator*>& iterators,
                    uint32_t num_rows, roaring::Roaring* bitmap) const override {
        BitmapIndexIterator* iterator = iterators[_column_id];
        if (iterator == nullptr) {
            return Status::OK();
        }

        rowid_t ordinal_limit = iterator->bitmap_nums();
        if (iterator->has_null_bitmap()) {
            ordinal_limit--;
            roaring::Roaring null_bitmap;
            RETURN_IF_ERROR(iterator->read_null_bitmap(&null_bitmap));
            *bitmap -= null_bitmap;
        }

        roaring::Roaring roaring;
        bool exact_match;
        Status status = iterator->seek_dictionary(&_value, &exact_match);
        rowid_t seeked_ordinal = iterator->current_ordinal();

        return _bitmap_compare(status, exact_match, ordinal_limit, seeked_ordinal, iterator,
                               bitmap);
    }

    uint16_t evaluate(const vectorized::IColumn& column, uint16_t* sel,
                      uint16_t size) const override {
        if (column.is_nullable()) {
            auto* nullable_column_ptr =
                    vectorized::check_and_get_column<vectorized::ColumnNullable>(column);
            auto& nested_column = nullable_column_ptr->get_nested_column();
            auto& null_map = reinterpret_cast<const vectorized::ColumnUInt8&>(
                                     nullable_column_ptr->get_null_map_column())
                                     .get_data();

            return _base_evaluate<true>(&nested_column, null_map.data(), sel, size);
        } else {
            return _base_evaluate<false>(&column, nullptr, sel, size);
        }
    }

    void evaluate_and(const vectorized::IColumn& column, const uint16_t* sel, uint16_t size,
                      bool* flags) const override {
        _evaluate_bit<true>(column, sel, size, flags);
    }

    void evaluate_or(const vectorized::IColumn& column, const uint16_t* sel, uint16_t size,
                     bool* flags) const override {
        _evaluate_bit<false>(column, sel, size, flags);
    }

    void evaluate_vec(const vectorized::IColumn& column, uint16_t size,
                      bool* flags) const override {
        if (column.is_nullable()) {
            auto* nullable_column_ptr =
                    vectorized::check_and_get_column<vectorized::ColumnNullable>(column);
            auto& nested_column = nullable_column_ptr->get_nested_column();
            auto& null_map = reinterpret_cast<const vectorized::ColumnUInt8&>(
                                     nullable_column_ptr->get_null_map_column())
                                     .get_data();

            if (nested_column.is_column_dictionary()) {
                if constexpr (std::is_same_v<T, StringValue>) {
                    auto* dict_column_ptr =
                            vectorized::check_and_get_column<vectorized::ColumnDictI32>(
                                    nested_column);
                    auto dict_code = _is_range() ? dict_column_ptr->find_code_by_bound(
                                                           _value, _is_greater(), _is_eq())
                                                 : dict_column_ptr->find_code(_value);
                    auto* data_array = dict_column_ptr->get_data().data();

                    _base_loop_vec<true>(size, flags, null_map.data(), data_array, dict_code);
                } else {
                    LOG(FATAL) << "column_dictionary must use StringValue predicate.";
                }
            } else {
                auto* data_array = reinterpret_cast<const vectorized::PredicateColumnType<TReal>&>(
                                           nested_column)
                                           .get_data()
                                           .data();

                _base_loop_vec<true>(size, flags, null_map.data(), data_array, _value_real);
            }
        } else {
            if (column.is_column_dictionary()) {
                if constexpr (std::is_same_v<T, StringValue>) {
                    auto* dict_column_ptr =
                            vectorized::check_and_get_column<vectorized::ColumnDictI32>(column);
                    auto dict_code = _is_range() ? dict_column_ptr->find_code_by_bound(
                                                           _value, _is_greater(), _is_eq())
                                                 : dict_column_ptr->find_code(_value);
                    auto* data_array = dict_column_ptr->get_data().data();

                    _base_loop_vec<false>(size, flags, nullptr, data_array, dict_code);
                } else {
                    LOG(FATAL) << "column_dictionary must use StringValue predicate.";
                }
            } else {
                auto* data_array =
                        vectorized::check_and_get_column<vectorized::PredicateColumnType<TReal>>(
                                column)
                                ->get_data()
                                .data();

                _base_loop_vec<false>(size, flags, nullptr, data_array, _value_real);
            }
        }

        if (_opposite) {
            for (uint16_t i = 0; i < size; i++) {
                flags[i] = !flags[i];
            }
        }
    }

private:
    using TReal = std::conditional_t<std::is_same_v<T, uint24_t>, uint32_t, T>;

    template <typename LeftT, typename RightT>
    bool _operator(const LeftT& lhs, const RightT& rhs) const {
        if constexpr (PT == PredicateType::EQ) {
            return lhs == rhs;
        } else if constexpr (PT == PredicateType::NE) {
            return lhs != rhs;
        } else if constexpr (PT == PredicateType::LT) {
            return lhs < rhs;
        } else if constexpr (PT == PredicateType::LE) {
            return lhs <= rhs;
        } else if constexpr (PT == PredicateType::GT) {
            return lhs > rhs;
        } else if constexpr (PT == PredicateType::GE) {
            return lhs >= rhs;
        }
    }

    constexpr bool _is_range() const { return PredicateTypeTraits::is_range(PT); }

    constexpr bool _is_greater() const { return _operator(1, 0); }

    constexpr bool _is_eq() const { return _operator(1, 1); }

    Status _bitmap_compare(Status status, bool exact_match, rowid_t ordinal_limit,
                           rowid_t& seeked_ordinal, BitmapIndexIterator* iterator,
                           roaring::Roaring* bitmap) const {
        roaring::Roaring roaring;

        if (status.is_not_found()) {
            if constexpr (PT == PredicateType::EQ || PT == PredicateType::GT ||
                          PT == PredicateType::GE) {
                *bitmap &= roaring; // set bitmap to empty
            }
            return Status::OK();
        }

        if (!status.ok()) {
            return status;
        }

        if constexpr (PT == PredicateType::EQ || PT == PredicateType::NE) {
            if (exact_match) {
                RETURN_IF_ERROR(iterator->read_bitmap(seeked_ordinal, &roaring));
            }
        } else if constexpr (PredicateTypeTraits::is_range(PT)) {
            rowid_t from = 0;
            rowid_t to = ordinal_limit;
            if constexpr (PT == PredicateType::LT) {
                to = seeked_ordinal;
            } else if constexpr (PT == PredicateType::LE) {
                to = seeked_ordinal + exact_match;
            } else if constexpr (PT == PredicateType::GT) {
                from = seeked_ordinal + exact_match;
            } else if constexpr (PT == PredicateType::GE) {
                from = seeked_ordinal;
            }

            RETURN_IF_ERROR(iterator->read_union_bitmap(from, to, &roaring));
        }

        if constexpr (PT == PredicateType::NE) {
            *bitmap -= roaring;
        } else {
            *bitmap &= roaring;
        }

        return Status::OK();
    }

    template <bool is_and>
    void _evaluate_bit(const vectorized::IColumn& column, const uint16_t* sel, uint16_t size,
                       bool* flags) const {
        if (column.is_nullable()) {
            auto* nullable_column_ptr =
                    vectorized::check_and_get_column<vectorized::ColumnNullable>(column);
            auto& nested_column = nullable_column_ptr->get_nested_column();
            auto& null_map = reinterpret_cast<const vectorized::ColumnUInt8&>(
                                     nullable_column_ptr->get_null_map_column())
                                     .get_data();

            _base_evaluate_bit<true, is_and>(&nested_column, null_map.data(), sel, size, flags);
        } else {
            _base_evaluate_bit<false, is_and>(&column, nullptr, sel, size, flags);
        }
    }

    template <bool is_nullable, typename TArray, typename TValue>
    void _base_loop_vec(uint16_t size, bool* __restrict flags, const uint8_t* __restrict null_map,
                        const TArray* __restrict data_array, const TValue& value) const {
        for (uint16_t i = 0; i < size; i++) {
            if constexpr (is_nullable) {
                flags[i] = !null_map[i] && _operator(data_array[i], value);
            } else {
                flags[i] = _operator(data_array[i], value);
            }
        }
    }

    template <bool is_nullable, bool is_and, typename TArray, typename TValue>
    void _base_loop_bit(const uint16_t* sel, uint16_t size, bool* flags,
                        const uint8_t* __restrict null_map, const TArray* __restrict data_array,
                        const TValue& value) const {
        for (uint16_t i = 0; i < size; i++) {
            if (is_and ^ flags[i]) {
                continue;
            }
            if constexpr (is_nullable) {
                if (_opposite ^ is_and ^
                    (!null_map[sel[i]] && _operator(data_array[sel[i]], value))) {
                    flags[i] = !is_and;
                }
            } else {
                if (_opposite ^ is_and ^ _operator(data_array[sel[i]], value)) {
                    flags[i] = !is_and;
                }
            }
        }
    }

    template <bool is_nullable, bool is_and>
    void _base_evaluate_bit(const vectorized::IColumn* column, const uint8_t* null_map,
                            const uint16_t* sel, uint16_t size, bool* flags) const {
        if (column->is_column_dictionary()) {
            if constexpr (std::is_same_v<T, StringValue>) {
                auto* dict_column_ptr =
                        vectorized::check_and_get_column<vectorized::ColumnDictI32>(column);
                auto* data_array = dict_column_ptr->get_data().data();
                auto dict_code = _is_range() ? dict_column_ptr->find_code_by_bound(
                                                       _value, _operator(1, 0), _operator(1, 1))
                                             : dict_column_ptr->find_code(_value);
                _base_loop_bit<is_nullable, is_and>(sel, size, flags, null_map, data_array,
                                                    dict_code);
            } else {
                LOG(FATAL) << "column_dictionary must use StringValue predicate.";
            }
        } else {
            auto* data_array =
                    vectorized::check_and_get_column<vectorized::PredicateColumnType<TReal>>(column)
                            ->get_data()
                            .data();

            _base_loop_bit<is_nullable, is_and>(sel, size, flags, null_map, data_array,
                                                _value_real);
        }
    }

    template <bool is_nullable, typename TArray, typename TValue>
    uint16_t _base_loop(uint16_t* sel, uint16_t size, const uint8_t* __restrict null_map,
                        const TArray* __restrict data_array, const TValue& value) const {
        uint16_t new_size = 0;
        for (uint16_t i = 0; i < size; ++i) {
            uint16_t idx = sel[i];
            if constexpr (is_nullable) {
                if (_opposite ^ (!null_map[i] && _operator(data_array[idx], value))) {
                    sel[new_size++] = idx;
                }
            } else {
                if (_opposite ^ _operator(data_array[idx], value)) {
                    sel[new_size++] = idx;
                }
            }
        }
        return new_size;
    }

    template <bool is_nullable>
    uint16_t _base_evaluate(const vectorized::IColumn* column, const uint8_t* null_map,
                            uint16_t* sel, uint16_t size) const {
        if (column->is_column_dictionary()) {
            if constexpr (std::is_same_v<T, StringValue>) {
                auto* dict_column_ptr =
                        vectorized::check_and_get_column<vectorized::ColumnDictI32>(column);
                auto* data_array = dict_column_ptr->get_data().data();
                auto dict_code = _is_range() ? dict_column_ptr->find_code_by_bound(
                                                       _value, _is_greater(), _is_eq())
                                             : dict_column_ptr->find_code(_value);

                return _base_loop<is_nullable>(sel, size, null_map, data_array, dict_code);
            } else {
                LOG(FATAL) << "column_dictionary must use StringValue predicate.";
                return 0;
            }
        } else {
            auto* data_array =
                    vectorized::check_and_get_column<vectorized::PredicateColumnType<TReal>>(column)
                            ->get_data()
                            .data();

            return _base_loop<is_nullable>(sel, size, null_map, data_array, _value_real);
        }
    }

    T _value;
    TReal _value_real;
};

template <class T>
using EqualPredicate = ComparisonPredicateBase<T, PredicateType::EQ>;
template <class T>
using NotEqualPredicate = ComparisonPredicateBase<T, PredicateType::NE>;
template <class T>
using LessPredicate = ComparisonPredicateBase<T, PredicateType::LT>;
template <class T>
using LessEqualPredicate = ComparisonPredicateBase<T, PredicateType::LE>;
template <class T>
using GreaterPredicate = ComparisonPredicateBase<T, PredicateType::GT>;
template <class T>
using GreaterEqualPredicate = ComparisonPredicateBase<T, PredicateType::GE>;

} //namespace doris
