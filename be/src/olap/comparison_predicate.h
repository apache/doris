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
#include "olap/rowset/segment_v2/inverted_index_cache.h" // IWYU pragma: keep
#include "olap/rowset/segment_v2/inverted_index_reader.h"
#include "olap/wrapper_field.h"
#include "vec/columns/column_dictionary.h"

namespace doris {

template <PrimitiveType Type, PredicateType PT>
class ComparisonPredicateBase : public ColumnPredicate {
public:
    using T = typename PredicatePrimitiveTypeTraits<Type>::PredicateFieldType;
    ComparisonPredicateBase(uint32_t column_id, const T& value, bool opposite = false)
            : ColumnPredicate(column_id, opposite),
              _cached_code(_InvalidateCodeValue),
              _value(value) {}

    void clone(ColumnPredicate** to) const override {
        auto* cloned = new ComparisonPredicateBase(_column_id, _value, _opposite);
        cloned->predicate_params()->value = _predicate_params->value;
        cloned->_cache_code_enabled = true;
        cloned->predicate_params()->marked_by_runtime_filter =
                _predicate_params->marked_by_runtime_filter;
        *to = cloned;
    }

    bool need_to_clone() const override { return true; }

    PredicateType type() const override { return PT; }

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

    Status evaluate(const Schema& schema, InvertedIndexIterator* iterator, uint32_t num_rows,
                    roaring::Roaring* bitmap) const override {
        if (iterator == nullptr) {
            return Status::OK();
        }
        auto column_desc = schema.column(_column_id);
        std::string column_name = column_desc->name();

        InvertedIndexQueryType query_type;
        switch (PT) {
        case PredicateType::EQ:
            query_type = InvertedIndexQueryType::EQUAL_QUERY;
            break;
        case PredicateType::NE:
            query_type = InvertedIndexQueryType::EQUAL_QUERY;
            break;
        case PredicateType::LT:
            query_type = InvertedIndexQueryType::LESS_THAN_QUERY;
            break;
        case PredicateType::LE:
            query_type = InvertedIndexQueryType::LESS_EQUAL_QUERY;
            break;
        case PredicateType::GT:
            query_type = InvertedIndexQueryType::GREATER_THAN_QUERY;
            break;
        case PredicateType::GE:
            query_type = InvertedIndexQueryType::GREATER_EQUAL_QUERY;
            break;
        default:
            return Status::InvalidArgument("invalid comparison predicate type {}", PT);
        }

        roaring::Roaring roaring;
        RETURN_IF_ERROR(iterator->read_from_inverted_index(column_name, &_value, query_type,
                                                           num_rows, &roaring));

        // mask out null_bitmap, since NULL cmp VALUE will produce NULL
        //  and be treated as false in WHERE
        // keep it after query, since query will try to read null_bitmap and put it to cache
        InvertedIndexQueryCacheHandle null_bitmap_cache_handle;
        RETURN_IF_ERROR(iterator->read_null_bitmap(&null_bitmap_cache_handle));
        std::shared_ptr<roaring::Roaring> null_bitmap = null_bitmap_cache_handle.get_bitmap();
        if (null_bitmap) {
            *bitmap -= *null_bitmap;
        }

        if constexpr (PT == PredicateType::NE) {
            *bitmap -= roaring;
        } else {
            *bitmap &= roaring;
        }

        return Status::OK();
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

    using WarpperFieldType = std::conditional_t<Type == TYPE_DATE, uint24_t, T>;

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
                        _get_zone_map_value<T>(statistic.first->cell_ptr()) <= _value &&
                                _get_zone_map_value<T>(statistic.second->cell_ptr()) >= _value,
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
                        _get_zone_map_value<T>(statistic.first->cell_ptr()) == _value &&
                                _get_zone_map_value<T>(statistic.second->cell_ptr()) == _value,
                        true);
            }
        } else if constexpr (PT == PredicateType::LT || PT == PredicateType::LE) {
            COMPARE_TO_MIN_OR_MAX(first)
        } else {
            static_assert(PT == PredicateType::GT || PT == PredicateType::GE);
            COMPARE_TO_MIN_OR_MAX(second)
        }
    }

    bool is_always_true(const std::pair<WrapperField*, WrapperField*>& statistic) const override {
        if (statistic.first->is_null() || statistic.second->is_null()) {
            return false;
        }

        DCHECK_LE(sizeof(T), statistic.first->size());

        T tmp_min_value {};
        T tmp_max_value {};
        memcpy((char*)(&tmp_min_value), statistic.first->cell_ptr(), sizeof(WarpperFieldType));
        memcpy((char*)(&tmp_max_value), statistic.second->cell_ptr(), sizeof(WarpperFieldType));

        if constexpr (PT == PredicateType::LT) {
            return _value > tmp_max_value;
        } else if constexpr (PT == PredicateType::LE) {
            return _value >= tmp_max_value;
        } else if constexpr (PT == PredicateType::GT) {
            return _value < tmp_min_value;
        } else if constexpr (PT == PredicateType::GE) {
            return _value <= tmp_min_value;
        }

        return false;
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
                return _get_zone_map_value<T>(statistic.first->cell_ptr()) == _value &&
                       _get_zone_map_value<T>(statistic.second->cell_ptr()) == _value;
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
                return _get_zone_map_value<T>(statistic.first->cell_ptr()) > _value ||
                       _get_zone_map_value<T>(statistic.second->cell_ptr()) < _value;
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
            // EQ predicate can not use ngram bf, just return true to accept
            if (bf->is_ngram_bf()) return true;
            if constexpr (std::is_same_v<T, StringRef>) {
                return bf->test_bytes(_value.data, _value.size);
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

    bool evaluate_and(const StringRef* dict_words, const size_t count) const override {
        if constexpr (std::is_same_v<T, StringRef>) {
            for (size_t i = 0; i != count; ++i) {
                if (_operator(dict_words[i], _value) ^ _opposite) {
                    return true;
                }
            }
            return false;
        }

        return true;
    }

    bool can_do_bloom_filter(bool ngram) const override {
        return PT == PredicateType::EQ && !ngram;
    }

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
                if constexpr (std::is_same_v<T, StringRef>) {
                    auto* dict_column_ptr =
                            vectorized::check_and_get_column<vectorized::ColumnDictI32>(
                                    nested_column);

                    auto dict_code = _find_code_from_dictionary_column(*dict_column_ptr);
                    do {
                        if constexpr (PT == PredicateType::EQ) {
                            if (dict_code == -2) {
                                memset(flags, 0, size);
                                break;
                            }
                        }
                        auto* data_array = dict_column_ptr->get_data().data();

                        _base_loop_vec<true, is_and>(size, flags, null_map.data(), data_array,
                                                     dict_code);
                    } while (false);
                } else {
                    LOG(FATAL) << "column_dictionary must use StringRef predicate.";
                }
            } else {
                auto* data_array = reinterpret_cast<const vectorized::PredicateColumnType<
                        PredicateEvaluateType<Type>>&>(nested_column)
                                           .get_data()
                                           .data();

                _base_loop_vec<true, is_and>(size, flags, null_map.data(), data_array, _value);
            }
        } else {
            if (column.is_column_dictionary()) {
                if constexpr (std::is_same_v<T, StringRef>) {
                    auto* dict_column_ptr =
                            vectorized::check_and_get_column<vectorized::ColumnDictI32>(column);
                    auto dict_code = _find_code_from_dictionary_column(*dict_column_ptr);
                    do {
                        if constexpr (PT == PredicateType::EQ) {
                            if (dict_code == -2) {
                                memset(flags, 0, size);
                                break;
                            }
                        }
                        auto* data_array = dict_column_ptr->get_data().data();

                        _base_loop_vec<false, is_and>(size, flags, nullptr, data_array, dict_code);
                    } while (false);
                } else {
                    LOG(FATAL) << "column_dictionary must use StringRef predicate.";
                }
            } else {
                auto* data_array =
                        vectorized::check_and_get_column<
                                vectorized::PredicateColumnType<PredicateEvaluateType<Type>>>(
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

        if (status.is<ErrorCode::ENTRY_NOT_FOUND>()) {
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
            if constexpr (std::is_same_v<T, StringRef>) {
                auto* dict_column_ptr =
                        vectorized::check_and_get_column<vectorized::ColumnDictI32>(column);
                auto* data_array = dict_column_ptr->get_data().data();
                auto dict_code = _find_code_from_dictionary_column(*dict_column_ptr);
                _base_loop_bit<is_nullable, is_and>(sel, size, flags, null_map, data_array,
                                                    dict_code);
            } else {
                LOG(FATAL) << "column_dictionary must use StringRef predicate.";
            }
        } else {
            auto* data_array =
                    vectorized::check_and_get_column<
                            vectorized::PredicateColumnType<PredicateEvaluateType<Type>>>(column)
                            ->get_data()
                            .data();

            _base_loop_bit<is_nullable, is_and>(sel, size, flags, null_map, data_array, _value);
        }
    }

    template <bool is_nullable>
    uint16_t _base_evaluate(const vectorized::IColumn* column, const uint8_t* null_map,
                            uint16_t* sel, uint16_t size) const {
        if (column->is_column_dictionary()) {
            if constexpr (std::is_same_v<T, StringRef>) {
                auto* dict_column_ptr =
                        vectorized::check_and_get_column<vectorized::ColumnDictI32>(column);
                auto& pred_col = dict_column_ptr->get_data();
                auto pred_col_data = pred_col.data();
                auto dict_code = _find_code_from_dictionary_column(*dict_column_ptr);

                if constexpr (PT == PredicateType::EQ) {
                    if (dict_code == -2) {
                        return _opposite ? size : 0;
                    }
                }
                uint16_t new_size = 0;
#define EVALUATE_WITH_NULL_IMPL(IDX) \
    _opposite ^ (!null_map[IDX] && _operator(pred_col_data[IDX], dict_code))
#define EVALUATE_WITHOUT_NULL_IMPL(IDX) _opposite ^ _operator(pred_col_data[IDX], dict_code)
                EVALUATE_BY_SELECTOR(EVALUATE_WITH_NULL_IMPL, EVALUATE_WITHOUT_NULL_IMPL)
#undef EVALUATE_WITH_NULL_IMPL
#undef EVALUATE_WITHOUT_NULL_IMPL

                return new_size;
            } else {
                LOG(FATAL) << "column_dictionary must use StringRef predicate.";
                return 0;
            }
        } else {
            auto& pred_col =
                    vectorized::check_and_get_column<
                            vectorized::PredicateColumnType<PredicateEvaluateType<Type>>>(column)
                            ->get_data();
            auto pred_col_data = pred_col.data();
            uint16_t new_size = 0;
#define EVALUATE_WITH_NULL_IMPL(IDX) \
    _opposite ^ (!null_map[IDX] && _operator(pred_col_data[IDX], _value))
#define EVALUATE_WITHOUT_NULL_IMPL(IDX) _opposite ^ _operator(pred_col_data[IDX], _value)
            EVALUATE_BY_SELECTOR(EVALUATE_WITH_NULL_IMPL, EVALUATE_WITHOUT_NULL_IMPL)
#undef EVALUATE_WITH_NULL_IMPL
#undef EVALUATE_WITHOUT_NULL_IMPL
            return new_size;
        }
    }

    __attribute__((flatten)) int32_t _find_code_from_dictionary_column(
            const vectorized::ColumnDictI32& column) const {
        /// if _cache_code_enabled is false, always find the code from dict.
        if (UNLIKELY(!_cache_code_enabled || _cached_code == _InvalidateCodeValue)) {
            int32_t code = _is_range() ? column.find_code_by_bound(_value, _is_greater(), _is_eq())
                                       : column.find_code(_value);

            // Protect the invalid code logic, to avoid data error.
            if (code == _InvalidateCodeValue) {
                LOG(FATAL) << "column dictionary should not return the code " << code
                           << ", because it is assumed as an invalid code in comparison predicate";
            }
            // Sometimes the dict is not initialized when run comparison predicate here, for example,
            // the full page is null, then the reader will skip read, so that the dictionary is not
            // inited. The cached code is wrong during this case, because the following page maybe not
            // null, and the dict should have items in the future.
            //
            // Cached code may have problems, so that add a config here, if not opened, then
            // we will return the code and not cache it.
            if (column.is_dict_empty() || !config::enable_low_cardinality_cache_code) {
                return code;
            }
            // If the dict is not empty, then the dict is inited and we could cache the value.
            _cached_code = code;
        }
        return _cached_code;
    }

    std::string _debug_string() const override {
        std::string info =
                "ComparisonPredicateBase(" + type_to_string(Type) + ", " + type_to_string(PT) + ")";
        return info;
    }

    static constexpr int32_t _InvalidateCodeValue = std::numeric_limits<int32_t>::max();
    mutable int32_t _cached_code;
    bool _cache_code_enabled = false;
    T _value;
};

} //namespace doris
