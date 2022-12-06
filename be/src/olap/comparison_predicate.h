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
#include "olap/rowset/segment_v2/bloom_filter.h"
#include "olap/wrapper_field.h"
#include "vec/columns/column_dictionary.h"

namespace doris {

template <PrimitiveType Type, PredicateType PT>
class ComparisonPredicateBase : public ColumnPredicate {
public:
    using T = typename PredicatePrimitiveTypeTraits<Type>::PredicateFieldType;
    ComparisonPredicateBase(uint32_t column_id, const T& value, bool opposite = false)
            : ColumnPredicate(column_id, opposite), _value(value) {}

    PredicateType type() const override { return PT; }

    void evaluate(ColumnBlock* block, uint16_t* sel, uint16_t* size) const override {
        uint16_t new_size = 0;
        if (block->is_nullable()) {
            for (uint16_t i = 0; i < *size; ++i) {
                uint16_t idx = sel[i];
                sel[new_size] = idx;
                if constexpr (Type == TYPE_DATE) {
                    T tmp_uint32_value = 0;
                    memcpy((char*)(&tmp_uint32_value), block->cell(idx).cell_ptr(),
                           sizeof(uint24_t));
                    auto result =
                            (!block->cell(idx).is_null() && _operator(tmp_uint32_value, _value));
                    new_size += _opposite ? !result : result;
                } else {
                    const T* cell_value = reinterpret_cast<const T*>(block->cell(idx).cell_ptr());
                    auto result = (!block->cell(idx).is_null() && _operator(*cell_value, _value));
                    new_size += _opposite ? !result : result;
                }
            }
        } else {
            for (uint16_t i = 0; i < *size; ++i) {
                uint16_t idx = sel[i];
                sel[new_size] = idx;
                if constexpr (Type == TYPE_DATE) {
                    T tmp_uint32_value = 0;
                    memcpy((char*)(&tmp_uint32_value), block->cell(idx).cell_ptr(),
                           sizeof(uint24_t));
                    auto result = _operator(tmp_uint32_value, _value);
                    new_size += _opposite ? !result : result;
                } else {
                    const T* cell_value = reinterpret_cast<const T*>(block->cell(idx).cell_ptr());
                    auto result = _operator(*cell_value, _value);
                    new_size += _opposite ? !result : result;
                }
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
                if constexpr (Type == TYPE_DATE) {
                    T tmp_uint32_value = 0;
                    memcpy((char*)(&tmp_uint32_value), block->cell(idx).cell_ptr(),
                           sizeof(uint24_t));
                    auto result =
                            (!block->cell(idx).is_null() && _operator(tmp_uint32_value, _value));
                    flags[i] = flags[i] | (_opposite ? !result : result);
                } else {
                    const T* cell_value = reinterpret_cast<const T*>(block->cell(idx).cell_ptr());
                    auto result = (!block->cell(idx).is_null() && _operator(*cell_value, _value));
                    flags[i] = flags[i] | (_opposite ? !result : result);
                }
            }
        } else {
            for (uint16_t i = 0; i < size; ++i) {
                if (flags[i]) {
                    continue;
                }
                uint16_t idx = sel[i];
                if constexpr (Type == TYPE_DATE) {
                    T tmp_uint32_value = 0;
                    memcpy((char*)(&tmp_uint32_value), block->cell(idx).cell_ptr(),
                           sizeof(uint24_t));
                    auto result = _operator(tmp_uint32_value, _value);
                    flags[i] = flags[i] | (_opposite ? !result : result);
                } else {
                    const T* cell_value = reinterpret_cast<const T*>(block->cell(idx).cell_ptr());
                    auto result = _operator(*cell_value, _value);
                    flags[i] = flags[i] | (_opposite ? !result : result);
                }
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
                if constexpr (Type == TYPE_DATE) {
                    T tmp_uint32_value = 0;
                    memcpy((char*)(&tmp_uint32_value), block->cell(idx).cell_ptr(),
                           sizeof(uint24_t));
                    auto result =
                            (!block->cell(idx).is_null() && _operator(tmp_uint32_value, _value));
                    flags[i] = flags[i] & (_opposite ? !result : result);
                } else {
                    const T* cell_value = reinterpret_cast<const T*>(block->cell(idx).cell_ptr());
                    auto result = (!block->cell(idx).is_null() && _operator(*cell_value, _value));
                    flags[i] = flags[i] & (_opposite ? !result : result);
                }
            }
        } else {
            for (uint16_t i = 0; i < size; ++i) {
                if (!flags[i]) {
                    continue;
                }
                uint16_t idx = sel[i];
                if constexpr (Type == TYPE_DATE) {
                    T tmp_uint32_value = 0;
                    memcpy((char*)(&tmp_uint32_value), block->cell(idx).cell_ptr(),
                           sizeof(uint24_t));
                    auto result = _operator(tmp_uint32_value, _value);
                    flags[i] = flags[i] & (_opposite ? !result : result);
                } else {
                    const T* cell_value = reinterpret_cast<const T*>(block->cell(idx).cell_ptr());
                    auto result = _operator(*cell_value, _value);
                    flags[i] = flags[i] & (_opposite ? !result : result);
                }
            }
        }
    }

    Status evaluate(BitmapIndexIterator* iterator, uint32_t num_rows,
                    roaring::Roaring* bitmap) const override {
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

#define COMPARE_TO_MIN_OR_MAX(ELE)                                                        \
    if constexpr (Type == TYPE_DATE) {                                                    \
        T tmp_uint32_value = 0;                                                           \
        memcpy((char*)(&tmp_uint32_value), statistic.ELE->cell_ptr(), sizeof(uint24_t));  \
        return _operator(tmp_uint32_value, _value);                                       \
    } else {                                                                              \
        return _operator(*reinterpret_cast<const T*>(statistic.ELE->cell_ptr()), _value); \
    }

    bool evaluate_and(const std::pair<WrapperField*, WrapperField*>& statistic) const override {
        if (statistic.first->is_null()) {
            return true;
        }
        if constexpr (PT == PredicateType::EQ) {
            if constexpr (Type == TYPE_DATE) {
                T tmp_min_uint32_value = 0;
                memcpy((char*)(&tmp_min_uint32_value), statistic.first->cell_ptr(),
                       sizeof(uint24_t));
                T tmp_max_uint32_value = 0;
                memcpy((char*)(&tmp_max_uint32_value), statistic.second->cell_ptr(),
                       sizeof(uint24_t));
                return _operator(tmp_min_uint32_value <= _value && tmp_max_uint32_value >= _value,
                                 true);
            } else {
                return _operator(
                        *reinterpret_cast<const T*>(statistic.first->cell_ptr()) <= _value &&
                                *reinterpret_cast<const T*>(statistic.second->cell_ptr()) >= _value,
                        true);
            }
        } else if constexpr (PT == PredicateType::NE) {
            if constexpr (Type == TYPE_DATE) {
                T tmp_min_uint32_value = 0;
                memcpy((char*)(&tmp_min_uint32_value), statistic.first->cell_ptr(),
                       sizeof(uint24_t));
                T tmp_max_uint32_value = 0;
                memcpy((char*)(&tmp_max_uint32_value), statistic.second->cell_ptr(),
                       sizeof(uint24_t));
                return _operator(tmp_min_uint32_value == _value && tmp_max_uint32_value == _value,
                                 true);
            } else {
                return _operator(
                        *reinterpret_cast<const T*>(statistic.first->cell_ptr()) == _value &&
                                *reinterpret_cast<const T*>(statistic.second->cell_ptr()) == _value,
                        true);
            }
        } else if constexpr (PT == PredicateType::LT || PT == PredicateType::LE) {
            COMPARE_TO_MIN_OR_MAX(first)
        } else {
            static_assert(PT == PredicateType::GT || PT == PredicateType::GE);
            COMPARE_TO_MIN_OR_MAX(second)
        }
    }

    bool evaluate_del(const std::pair<WrapperField*, WrapperField*>& statistic) const override {
        if (statistic.first->is_null() || statistic.second->is_null()) {
            return false;
        }
        if constexpr (PT == PredicateType::EQ) {
            if constexpr (Type == TYPE_DATE) {
                T tmp_min_uint32_value = 0;
                memcpy((char*)(&tmp_min_uint32_value), statistic.first->cell_ptr(),
                       sizeof(uint24_t));
                T tmp_max_uint32_value = 0;
                memcpy((char*)(&tmp_max_uint32_value), statistic.second->cell_ptr(),
                       sizeof(uint24_t));
                return _operator(tmp_min_uint32_value == _value && tmp_max_uint32_value == _value,
                                 true);
            } else {
                return *reinterpret_cast<const T*>(statistic.first->cell_ptr()) == _value &&
                       *reinterpret_cast<const T*>(statistic.second->cell_ptr()) == _value;
            }
        } else if constexpr (PT == PredicateType::NE) {
            if constexpr (Type == TYPE_DATE) {
                T tmp_min_uint32_value = 0;
                memcpy((char*)(&tmp_min_uint32_value), statistic.first->cell_ptr(),
                       sizeof(uint24_t));
                T tmp_max_uint32_value = 0;
                memcpy((char*)(&tmp_max_uint32_value), statistic.second->cell_ptr(),
                       sizeof(uint24_t));
                return tmp_min_uint32_value > _value || tmp_max_uint32_value < _value;
            } else {
                return *reinterpret_cast<const T*>(statistic.first->cell_ptr()) > _value ||
                       *reinterpret_cast<const T*>(statistic.second->cell_ptr()) < _value;
            }
        } else if constexpr (PT == PredicateType::LT || PT == PredicateType::LE) {
            COMPARE_TO_MIN_OR_MAX(second)
        } else {
            static_assert(PT == PredicateType::GT || PT == PredicateType::GE);
            COMPARE_TO_MIN_OR_MAX(first)
        }
    }
#undef COMPARE_TO_MIN_OR_MAX

    bool evaluate_and(const segment_v2::BloomFilter* bf) const override {
        if constexpr (PT == PredicateType::EQ) {
            if constexpr (std::is_same_v<T, StringValue>) {
                return bf->test_bytes(_value.ptr, _value.len);
            } else if constexpr (Type == TYPE_DATE) {
                return bf->test_bytes(const_cast<char*>(reinterpret_cast<const char*>(&_value)),
                                      sizeof(uint24_t));
            } else {
                return bf->test_bytes(const_cast<char*>(reinterpret_cast<const char*>(&_value)),
                                      sizeof(_value));
            }
        } else {
            LOG(FATAL) << "Bloom filter is not supported by predicate type.";
            return true;
        }
    }

    bool can_do_bloom_filter() const override { return PT == PredicateType::EQ; }

    void evaluate_or(const vectorized::IColumn& column, const uint16_t* sel, uint16_t size,
                     bool* flags) const override {
        _evaluate_bit<false>(column, sel, size, flags);
    }

    template <bool is_and>
    __attribute__((flatten)) void _evaluate_vec_internal(const vectorized::IColumn& column,
                                                         uint16_t size, bool* flags) const {
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

                    _base_loop_vec<true, is_and>(size, flags, null_map.data(), data_array,
                                                 dict_code);
                } else {
                    LOG(FATAL) << "column_dictionary must use StringValue predicate.";
                }
            } else {
                auto* data_array = reinterpret_cast<const vectorized::PredicateColumnType<Type>&>(
                                           nested_column)
                                           .get_data()
                                           .data();

                _base_loop_vec<true, is_and>(size, flags, null_map.data(), data_array, _value);
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

                    _base_loop_vec<false, is_and>(size, flags, nullptr, data_array, dict_code);
                } else {
                    LOG(FATAL) << "column_dictionary must use StringValue predicate.";
                }
            } else {
                auto* data_array =
                        vectorized::check_and_get_column<vectorized::PredicateColumnType<Type>>(
                                column)
                                ->get_data()
                                .data();

                _base_loop_vec<false, is_and>(size, flags, nullptr, data_array, _value);
            }
        }

        if (_opposite) {
            for (uint16_t i = 0; i < size; i++) {
                flags[i] = !flags[i];
            }
        }
    }

    void evaluate_vec(const vectorized::IColumn& column, uint16_t size,
                      bool* flags) const override {
        _evaluate_vec_internal<false>(column, size, flags);
    }

    void evaluate_and_vec(const vectorized::IColumn& column, uint16_t size,
                          bool* flags) const override {
        _evaluate_vec_internal<true>(column, size, flags);
    }

private:
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

    template <bool is_nullable, bool is_and, typename TArray, typename TValue>
    __attribute__((flatten)) void _base_loop_vec(uint16_t size, bool* __restrict bflags,
                                                 const uint8_t* __restrict null_map,
                                                 const TArray* __restrict data_array,
                                                 const TValue& value) const {
        //uint8_t helps compiler to generate vectorized code
        uint8_t* flags = reinterpret_cast<uint8_t*>(bflags);
        if constexpr (is_and) {
            for (uint16_t i = 0; i < size; i++) {
                if constexpr (is_nullable) {
                    flags[i] &= (uint8_t)(!null_map[i] && _operator(data_array[i], value));
                } else {
                    flags[i] &= (uint8_t)_operator(data_array[i], value);
                }
            }
        } else {
            for (uint16_t i = 0; i < size; i++) {
                if constexpr (is_nullable) {
                    flags[i] = !null_map[i] && _operator(data_array[i], value);
                } else {
                    flags[i] = _operator(data_array[i], value);
                }
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
                    vectorized::check_and_get_column<vectorized::PredicateColumnType<Type>>(column)
                            ->get_data()
                            .data();

            _base_loop_bit<is_nullable, is_and>(sel, size, flags, null_map, data_array, _value);
        }
    }

    template <bool is_nullable, typename TArray, typename TValue>
    uint16_t _base_loop(uint16_t* sel, uint16_t size, const uint8_t* __restrict null_map,
                        const TArray* __restrict data_array, const TValue& value) const {
        uint16_t new_size = 0;
        for (uint16_t i = 0; i < size; ++i) {
            uint16_t idx = sel[i];
            if constexpr (is_nullable) {
                if (_opposite ^ (!null_map[idx] && _operator(data_array[idx], value))) {
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
                    vectorized::check_and_get_column<vectorized::PredicateColumnType<EvalType>>(
                            column)
                            ->get_data()
                            .data();

            return _base_loop<is_nullable>(sel, size, null_map, data_array, _value);
        }
    }

    std::string _debug_string() const override {
        std::string info =
                "ComparisonPredicateBase(" + type_to_string(Type) + ", " + type_to_string(PT) + ")";
        return info;
    }

    T _value;
    static constexpr PrimitiveType EvalType = (Type == TYPE_CHAR ? TYPE_STRING : Type);
};

} //namespace doris
