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
#include <roaring/roaring.hh>

#include "decimal12.h"
#include "exprs/hybrid_set.h"
#include "olap/column_predicate.h"
#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/bloom_filter.h"
#include "olap/rowset/segment_v2/inverted_index_cache.h" // IWYU pragma: keep
#include "olap/rowset/segment_v2/inverted_index_reader.h"
#include "olap/wrapper_field.h"
#include "runtime/define_primitive_type.h"
#include "runtime/primitive_type.h"
#include "runtime/type_limit.h"
#include "uint24.h"
#include "vec/columns/column_dictionary.h"
#include "vec/common/string_ref.h"
#include "vec/core/types.h"

template <>
struct std::equal_to<doris::StringRef> {
    bool operator()(const doris::StringRef& lhs, const doris::StringRef& rhs) const {
        return lhs == rhs;
    }
};

// for uint24_t
template <>
struct std::hash<doris::uint24_t> {
    size_t operator()(const doris::uint24_t& rhs) const {
        uint32_t val(rhs);
        return hash<int>()(val);
    }
};

template <>
struct std::equal_to<doris::uint24_t> {
    bool operator()(const doris::uint24_t& lhs, const doris::uint24_t& rhs) const {
        return lhs == rhs;
    }
};

namespace doris {

/**
 * Use HybridSetType can avoid virtual function call in the loop.
 * @tparam Type
 * @tparam PT
 * @tparam HybridSetType
 */
template <PrimitiveType Type, PredicateType PT, typename HybridSetType>
class InListPredicateBase : public ColumnPredicate {
public:
    using T = std::conditional_t<Type == PrimitiveType::TYPE_DECIMALV2, DecimalV2Value,
                                 typename PredicatePrimitiveTypeTraits<Type>::PredicateFieldType>;
    template <typename ConditionType, typename ConvertFunc>
    InListPredicateBase(uint32_t column_id, const ConditionType& conditions,
                        const ConvertFunc& convert, bool is_opposite = false,
                        const TabletColumn* col = nullptr, vectorized::Arena* arena = nullptr)
            : ColumnPredicate(column_id, is_opposite),
              _min_value(type_limit<T>::max()),
              _max_value(type_limit<T>::min()) {
        _values = std::make_shared<HybridSetType>();
        for (const auto& condition : conditions) {
            T tmp;
            if constexpr (Type == TYPE_STRING || Type == TYPE_CHAR) {
                tmp = convert(*col, condition, arena);
            } else if constexpr (Type == TYPE_DECIMAL32 || Type == TYPE_DECIMAL64 ||
                                 Type == TYPE_DECIMAL128I) {
                tmp = convert(*col, condition);
            } else {
                tmp = convert(condition);
            }
            _values->insert(&tmp);
            _update_min_max(tmp);
        }
    }

    InListPredicateBase(uint32_t column_id, const std::shared_ptr<HybridSetBase>& hybrid_set,
                        size_t char_length = 0)
            : ColumnPredicate(column_id, false),
              _min_value(type_limit<T>::max()),
              _max_value(type_limit<T>::min()) {
        CHECK(hybrid_set != nullptr);

        if constexpr (is_string_type(Type) || Type == TYPE_DECIMALV2 || is_date_type(Type)) {
            _values = std::make_shared<HybridSetType>();
            if constexpr (is_string_type(Type)) {
                HybridSetBase::IteratorBase* iter = hybrid_set->begin();
                while (iter->has_next()) {
                    const StringRef* value = (const StringRef*)(iter->get_value());
                    if constexpr (Type == TYPE_CHAR) {
                        _temp_datas.push_back("");
                        _temp_datas.back().resize(std::max(char_length, value->size));
                        memcpy(_temp_datas.back().data(), value->data, value->size);
                        const string& str = _temp_datas.back();
                        _values->insert((void*)str.data(), str.length());
                    } else {
                        _values->insert((void*)value->data, value->size);
                    }
                    iter->next();
                }
            } else if constexpr (Type == TYPE_DECIMALV2) {
                HybridSetBase::IteratorBase* iter = hybrid_set->begin();
                while (iter->has_next()) {
                    const DecimalV2Value* value = (const DecimalV2Value*)(iter->get_value());
                    _values->insert(value);
                    iter->next();
                }
            } else if constexpr (Type == TYPE_DATE) {
                HybridSetBase::IteratorBase* iter = hybrid_set->begin();
                while (iter->has_next()) {
                    const vectorized::VecDateTimeValue* value =
                            (const vectorized::VecDateTimeValue*)(iter->get_value());
                    uint64_t date = value->to_olap_date();
                    _values->insert(&date);
                    iter->next();
                }
            } else if constexpr (Type == TYPE_DATETIME) {
                HybridSetBase::IteratorBase* iter = hybrid_set->begin();
                while (iter->has_next()) {
                    const vectorized::VecDateTimeValue* value =
                            (const vectorized::VecDateTimeValue*)(iter->get_value());
                    uint64_t date_time = value->to_olap_datetime();
                    _values->insert(&date_time);
                    iter->next();
                }
            } else {
                HybridSetBase::IteratorBase* iter = hybrid_set->begin();
                while (iter->has_next()) {
                    const void* value = iter->get_value();
                    _values->insert(value);
                    iter->next();
                }
                CHECK(Type == TYPE_DATETIMEV2 || Type == TYPE_DATEV2);
            }
        } else {
            // shared from the caller, so it needs to be shared ptr
            _values = hybrid_set;
        }
        HybridSetBase::IteratorBase* iter = _values->begin();
        while (iter->has_next()) {
            const T* value = (const T*)(iter->get_value());
            _update_min_max(*value);
            iter->next();
        }
    }

    ~InListPredicateBase() override = default;

    PredicateType type() const override { return PT; }

    Status evaluate(BitmapIndexIterator* iterator, uint32_t num_rows,
                    roaring::Roaring* result) const override {
        if (iterator == nullptr) {
            return Status::OK();
        }
        if (iterator->has_null_bitmap()) {
            roaring::Roaring null_bitmap;
            RETURN_IF_ERROR(iterator->read_null_bitmap(&null_bitmap));
            *result -= null_bitmap;
        }
        roaring::Roaring indices;
        HybridSetBase::IteratorBase* iter = _values->begin();
        while (iter->has_next()) {
            const void* value = iter->get_value();
            bool exact_match;
            Status s = iterator->seek_dictionary(value, &exact_match);
            rowid_t seeked_ordinal = iterator->current_ordinal();
            if (!s.is<ErrorCode::NOT_FOUND>()) {
                if (!s.ok()) {
                    return s;
                }
                if (exact_match) {
                    roaring::Roaring index;
                    RETURN_IF_ERROR(iterator->read_bitmap(seeked_ordinal, &index));
                    indices |= index;
                }
            }
            iter->next();
        }

        if constexpr (PT == PredicateType::IN_LIST) {
            *result &= indices;
        } else {
            *result -= indices;
        }

        return Status::OK();
    }

    Status evaluate(const Schema& schema, InvertedIndexIterator* iterator, uint32_t num_rows,
                    roaring::Roaring* result) const override {
        if (iterator == nullptr) {
            return Status::OK();
        }
        auto column_desc = schema.column(_column_id);
        std::string column_name = column_desc->name();
        roaring::Roaring indices;
        HybridSetBase::IteratorBase* iter = _values->begin();
        while (iter->has_next()) {
            const void* value = iter->get_value();
            InvertedIndexQueryType query_type = InvertedIndexQueryType::EQUAL_QUERY;
            roaring::Roaring index;
            RETURN_IF_ERROR(iterator->read_from_inverted_index(column_name, value, query_type,
                                                               num_rows, &index));
            indices |= index;
            iter->next();
        }

        // mask out null_bitmap, since NULL cmp VALUE will produce NULL
        //  and be treated as false in WHERE
        // keep it after query, since query will try to read null_bitmap and put it to cache
        InvertedIndexQueryCacheHandle null_bitmap_cache_handle;
        RETURN_IF_ERROR(iterator->read_null_bitmap(&null_bitmap_cache_handle));
        std::shared_ptr<roaring::Roaring> null_bitmap = null_bitmap_cache_handle.get_bitmap();
        if (null_bitmap) {
            *result -= *null_bitmap;
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
        int64_t new_size = 0;

        if (column.is_nullable()) {
            auto* nullable_col =
                    vectorized::check_and_get_column<vectorized::ColumnNullable>(column);
            auto& null_map = reinterpret_cast<const vectorized::ColumnUInt8&>(
                                     nullable_col->get_null_map_column())
                                     .get_data();
            auto& nested_col = nullable_col->get_nested_column();

            if (_opposite) {
                new_size = _base_evaluate<true, true>(&nested_col, &null_map, sel, size);
            } else {
                new_size = _base_evaluate<true, false>(&nested_col, &null_map, sel, size);
            }
        } else {
            if (_opposite) {
                new_size = _base_evaluate<false, true>(&column, nullptr, sel, size);
            } else {
                new_size = _base_evaluate<false, false>(&column, nullptr, sel, size);
            }
        }
        _evaluated_rows += size;
        _passed_rows += new_size;
        return new_size;
    }
    int get_filter_id() const override { return _values->get_filter_id(); }
    bool is_filter() const override { return true; }

    template <bool is_and>
    void _evaluate_bit(const vectorized::IColumn& column, const uint16_t* sel, uint16_t size,
                       bool* flags) const {
        if (column.is_nullable()) {
            auto* nullable_col =
                    vectorized::check_and_get_column<vectorized::ColumnNullable>(column);
            auto& null_bitmap = reinterpret_cast<const vectorized::ColumnUInt8&>(
                                        nullable_col->get_null_map_column())
                                        .get_data();
            auto& nested_col = nullable_col->get_nested_column();

            if (_opposite) {
                return _base_evaluate_bit<true, true, is_and>(&nested_col, &null_bitmap, sel, size,
                                                              flags);
            } else {
                return _base_evaluate_bit<true, false, is_and>(&nested_col, &null_bitmap, sel, size,
                                                               flags);
            }
        } else {
            if (_opposite) {
                return _base_evaluate_bit<false, true, is_and>(&column, nullptr, sel, size, flags);
            } else {
                return _base_evaluate_bit<false, false, is_and>(&column, nullptr, sel, size, flags);
            }
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

    template <PrimitiveType primitive_type, typename ResultType>
    ResultType _get_zone_map_value2(void* data_ptr) const {
        ResultType res;
        // DecimalV2's storage value is different from predicate or compute value type
        // need convert it to DecimalV2Value
        if constexpr (primitive_type == PrimitiveType::TYPE_DECIMALV2) {
            decimal12_t decimal_12_t_value;
            memcpy((char*)(&decimal_12_t_value), data_ptr, sizeof(decimal12_t));
            res->from_olap_decimal(decimal_12_t_value.integer, decimal_12_t_value.fraction);
        } else {
            // TODO add datev1 convert here
            memcpy(&res, data_ptr, sizeof(ResultType));
        }
        return res;
    }

    bool evaluate_and(const std::pair<WrapperField*, WrapperField*>& statistic) const override {
        if (statistic.first->is_null()) {
            return true;
        }
        if constexpr (PT == PredicateType::IN_LIST) {
            if constexpr (Type == TYPE_DATE) {
                T tmp_min_uint32_value = 0;
                memcpy((char*)(&tmp_min_uint32_value), statistic.first->cell_ptr(),
                       sizeof(uint24_t));
                T tmp_max_uint32_value = 0;
                memcpy((char*)(&tmp_max_uint32_value), statistic.second->cell_ptr(),
                       sizeof(uint24_t));
                return tmp_min_uint32_value <= _max_value && tmp_max_uint32_value >= _min_value;
            } else {
                return _get_zone_map_value2<Type, T>(statistic.first->cell_ptr()) <= _max_value &&
                       _get_zone_map_value2<Type, T>(statistic.second->cell_ptr()) >= _min_value;
            }
        } else {
            return true;
        }
    }

    bool evaluate_and(const StringRef* dict_words, const size_t count) const override {
        for (size_t i = 0; i != count; ++i) {
            const auto found = _values->find(dict_words[i].data, dict_words[i].size) ^ _opposite;
            if (found == (PT == PredicateType::IN_LIST)) {
                return true;
            }
        }

        return false;
    }

    bool evaluate_del(const std::pair<WrapperField*, WrapperField*>& statistic) const override {
        if (statistic.first->is_null() || statistic.second->is_null()) {
            return false;
        }
        if constexpr (PT == PredicateType::NOT_IN_LIST) {
            if constexpr (Type == TYPE_DATE) {
                T tmp_min_uint32_value = 0;
                memcpy((char*)(&tmp_min_uint32_value), statistic.first->cell_ptr(),
                       sizeof(uint24_t));
                T tmp_max_uint32_value = 0;
                memcpy((char*)(&tmp_max_uint32_value), statistic.second->cell_ptr(),
                       sizeof(uint24_t));
                return tmp_min_uint32_value > _max_value || tmp_max_uint32_value < _min_value;
            } else {
                return _get_zone_map_value2<Type, T>(statistic.first->cell_ptr()) > _max_value ||
                       _get_zone_map_value2<Type, T>(statistic.second->cell_ptr()) < _min_value;
            }
        } else {
            return false;
        }
    }

    bool evaluate_and(const segment_v2::BloomFilter* bf) const override {
        if constexpr (PT == PredicateType::IN_LIST) {
            // IN predicate can not use ngram bf, just return true to accept
            if (bf->is_ngram_bf()) return true;
            HybridSetBase::IteratorBase* iter = _values->begin();
            while (iter->has_next()) {
                if constexpr (std::is_same_v<T, StringRef>) {
                    const StringRef* value = (const StringRef*)iter->get_value();
                    if (bf->test_bytes(value->data, value->size)) {
                        return true;
                    }
                } else if constexpr (Type == TYPE_DATE) {
                    const void* value = iter->get_value();
                    if (bf->test_bytes(reinterpret_cast<const char*>(value), sizeof(uint24_t))) {
                        return true;
                    }
                } else if constexpr (Type == PrimitiveType::TYPE_DECIMALV2) {
                    // DecimalV2 using decimal12_t in bloom filter in storage layer, should convert value to decimal12_t
                    const T* value = (const T*)(iter->get_value());
                    decimal12_t decimal12_t_val(value.int_value(), value.frac_value());
                    if (bf->test_bytes(
                                const_cast<char*>(reinterpret_cast<const char*>(&decimal12_t_val)),
                                sizeof(decimal12_t))) {
                        return true;
                    }
                } else {
                    const T* value = (const T*)(iter->get_value());
                    if (bf->test_bytes(reinterpret_cast<const char*>(value), sizeof(*value))) {
                        return true;
                    }
                }
                iter->next();
            }
            return false;
        } else {
            LOG(FATAL) << "Bloom filter is not supported by predicate type.";
            return true;
        }
    }

    bool can_do_bloom_filter(bool ngram) const override {
        return PT == PredicateType::IN_LIST && !ngram;
    }

private:
    template <typename LeftT, typename RightT>
    bool _operator(const LeftT& lhs, const RightT& rhs) const {
        if constexpr (PT == PredicateType::IN_LIST) {
            return lhs != rhs;
        } else {
            return lhs == rhs;
        }
    }

    template <bool is_nullable, bool is_opposite>
    uint16_t _base_evaluate(const vectorized::IColumn* column,
                            const vectorized::PaddedPODArray<vectorized::UInt8>* null_map,
                            uint16_t* sel, uint16_t size) const {
        uint16_t new_size = 0;

        if (column->is_column_dictionary()) {
            if constexpr (std::is_same_v<T, StringRef>) {
                auto* nested_col_ptr = vectorized::check_and_get_column<
                        vectorized::ColumnDictionary<vectorized::Int32>>(column);
                auto& data_array = nested_col_ptr->get_data();
                auto segid = column->get_rowset_segment_id();
                DCHECK((segid.first.hi | segid.first.mi | segid.first.lo) != 0);
                auto& value_in_dict_flags = _segment_id_to_value_in_dict_flags[segid];
                if (value_in_dict_flags.empty()) {
                    nested_col_ptr->find_codes(_values.get(), value_in_dict_flags);
                }

                CHECK(value_in_dict_flags.size() == nested_col_ptr->dict_size())
                        << "value_in_dict_flags.size()!=nested_col_ptr->dict_size(), "
                        << value_in_dict_flags.size() << " vs " << nested_col_ptr->dict_size()
                        << " rowsetid=" << segid.first << " segmentid=" << segid.second
                        << "dict_info" << nested_col_ptr->dict_debug_string();

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
                        if (value_in_dict_flags[data_array[idx]]) {
                            sel[new_size++] = idx;
                        }
                    } else {
                        if (!value_in_dict_flags[data_array[idx]]) {
                            sel[new_size++] = idx;
                        }
                    }
                }
            } else {
                LOG(FATAL) << "column_dictionary must use StringRef predicate.";
            }
        } else {
            auto& pred_col =
                    vectorized::check_and_get_column<
                            vectorized::PredicateColumnType<PredicateEvaluateType<Type>>>(column)
                            ->get_data();
            auto pred_col_data = pred_col.data();

#define EVALUATE_WITH_NULL_IMPL(IDX) \
    is_opposite ^                    \
            (!(*null_map)[IDX] &&    \
             _operator(_values->find(reinterpret_cast<const T*>(&pred_col_data[IDX])), false))
#define EVALUATE_WITHOUT_NULL_IMPL(IDX) \
    is_opposite ^ _operator(_values->find(reinterpret_cast<const T*>(&pred_col_data[IDX])), false)
            EVALUATE_BY_SELECTOR(EVALUATE_WITH_NULL_IMPL, EVALUATE_WITHOUT_NULL_IMPL)
#undef EVALUATE_WITH_NULL_IMPL
#undef EVALUATE_WITHOUT_NULL_IMPL
        }
        return new_size;
    }

    template <bool is_nullable, bool is_opposite, bool is_and>
    void _base_evaluate_bit(const vectorized::IColumn* column,
                            const vectorized::PaddedPODArray<vectorized::UInt8>* null_map,
                            const uint16_t* sel, uint16_t size, bool* flags) const {
        if (column->is_column_dictionary()) {
            if constexpr (std::is_same_v<T, StringRef>) {
                auto* nested_col_ptr = vectorized::check_and_get_column<
                        vectorized::ColumnDictionary<vectorized::Int32>>(column);
                auto& data_array = nested_col_ptr->get_data();
                auto& value_in_dict_flags =
                        _segment_id_to_value_in_dict_flags[column->get_rowset_segment_id()];
                if (value_in_dict_flags.empty()) {
                    nested_col_ptr->find_codes(_values.get(), value_in_dict_flags);
                }

                for (uint16_t i = 0; i < size; i++) {
                    if (is_and ^ flags[i]) {
                        continue;
                    }

                    uint16_t idx = sel[i];
                    if constexpr (is_nullable) {
                        if ((*null_map)[idx]) {
                            if (is_and ^ is_opposite) {
                                flags[i] = !is_and;
                            }
                            continue;
                        }
                    }

                    if constexpr (is_opposite != (PT == PredicateType::IN_LIST)) {
                        if (is_and ^ value_in_dict_flags[data_array[idx]]) {
                            flags[i] = !is_and;
                        }
                    } else {
                        if (is_and ^ !value_in_dict_flags[data_array[idx]]) {
                            flags[i] = !is_and;
                        }
                    }
                }
            } else {
                LOG(FATAL) << "column_dictionary must use StringRef predicate.";
            }
        } else {
            auto* nested_col_ptr = vectorized::check_and_get_column<
                    vectorized::PredicateColumnType<PredicateEvaluateType<Type>>>(column);
            auto& data_array = nested_col_ptr->get_data();

            for (uint16_t i = 0; i < size; i++) {
                if (is_and ^ flags[i]) {
                    continue;
                }
                uint16_t idx = sel[i];
                if constexpr (is_nullable) {
                    if ((*null_map)[idx]) {
                        if (is_and ^ is_opposite) {
                            flags[i] = !is_and;
                        }
                        continue;
                    }
                }

                if constexpr (!is_opposite) {
                    if (is_and ^
                        _operator(_values->find(reinterpret_cast<const T*>(&data_array[idx])),
                                  false)) {
                        flags[i] = !is_and;
                    }
                } else {
                    if (is_and ^
                        !_operator(_values->find(reinterpret_cast<const T*>(&data_array[idx])),
                                   false)) {
                        flags[i] = !is_and;
                    }
                }
            }
        }
    }

    std::string _debug_string() const override {
        std::string info =
                "InListPredicateBase(" + type_to_string(Type) + ", " + type_to_string(PT) + ")";
        return info;
    }

    void _update_min_max(const T& value) {
        if (value > _max_value) {
            _max_value = value;
        }
        if (value < _min_value) {
            _min_value = value;
        }
    }

    std::shared_ptr<HybridSetBase> _values;
    mutable std::map<std::pair<RowsetId, uint32_t>, std::vector<vectorized::UInt8>>
            _segment_id_to_value_in_dict_flags;
    T _min_value;
    T _max_value;

    // temp string for char type column
    std::list<std::string> _temp_datas;
};

template <PrimitiveType Type, PredicateType PT, typename ConditionType, typename ConvertFunc,
          size_t N = 0>
ColumnPredicate* _create_in_list_predicate(uint32_t column_id, const ConditionType& conditions,
                                           const ConvertFunc& convert, bool is_opposite = false,
                                           const TabletColumn* col = nullptr,
                                           vectorized::Arena* arena = nullptr) {
    using T = typename PredicatePrimitiveTypeTraits<Type>::PredicateFieldType;
    if constexpr (N >= 1 && N <= FIXED_CONTAINER_MAX_SIZE) {
        using Set = std::conditional_t<
                std::is_same_v<T, StringRef>, StringSet<FixedContainer<std::string, N>>,
                HybridSet<Type, FixedContainer<T, N>,
                          vectorized::PredicateColumnType<PredicateEvaluateType<Type>>>>;
        return new InListPredicateBase<Type, PT, Set>(column_id, conditions, convert, is_opposite,
                                                      col, arena);
    } else {
        using Set = std::conditional_t<
                std::is_same_v<T, StringRef>, StringSet<DynamicContainer<std::string>>,
                HybridSet<Type, DynamicContainer<T>,
                          vectorized::PredicateColumnType<PredicateEvaluateType<Type>>>>;
        return new InListPredicateBase<Type, PT, Set>(column_id, conditions, convert, is_opposite,
                                                      col, arena);
    }
}

template <PrimitiveType Type, PredicateType PT, typename ConditionType, typename ConvertFunc>
ColumnPredicate* create_in_list_predicate(uint32_t column_id, const ConditionType& conditions,
                                          const ConvertFunc& convert, bool is_opposite = false,
                                          const TabletColumn* col = nullptr,
                                          vectorized::Arena* arena = nullptr) {
    if (conditions.size() == 1) {
        return _create_in_list_predicate<Type, PT, ConditionType, ConvertFunc, 1>(
                column_id, conditions, convert, is_opposite, col, arena);
    } else if (conditions.size() == 2) {
        return _create_in_list_predicate<Type, PT, ConditionType, ConvertFunc, 2>(
                column_id, conditions, convert, is_opposite, col, arena);
    } else if (conditions.size() == 3) {
        return _create_in_list_predicate<Type, PT, ConditionType, ConvertFunc, 3>(
                column_id, conditions, convert, is_opposite, col, arena);
    } else if (conditions.size() == 4) {
        return _create_in_list_predicate<Type, PT, ConditionType, ConvertFunc, 4>(
                column_id, conditions, convert, is_opposite, col, arena);
    } else if (conditions.size() == 5) {
        return _create_in_list_predicate<Type, PT, ConditionType, ConvertFunc, 5>(
                column_id, conditions, convert, is_opposite, col, arena);
    } else if (conditions.size() == 6) {
        return _create_in_list_predicate<Type, PT, ConditionType, ConvertFunc, 6>(
                column_id, conditions, convert, is_opposite, col, arena);
    } else if (conditions.size() == 7) {
        return _create_in_list_predicate<Type, PT, ConditionType, ConvertFunc, 7>(
                column_id, conditions, convert, is_opposite, col, arena);
    } else if (conditions.size() == FIXED_CONTAINER_MAX_SIZE) {
        return _create_in_list_predicate<Type, PT, ConditionType, ConvertFunc,
                                         FIXED_CONTAINER_MAX_SIZE>(column_id, conditions, convert,
                                                                   is_opposite, col, arena);
    } else {
        return _create_in_list_predicate<Type, PT, ConditionType, ConvertFunc>(
                column_id, conditions, convert, is_opposite, col, arena);
    }
}

template <PrimitiveType Type, PredicateType PT, size_t N = 0>
ColumnPredicate* _create_in_list_predicate(uint32_t column_id,
                                           const std::shared_ptr<HybridSetBase>& hybrid_set,
                                           size_t char_length = 0) {
    using T = std::conditional_t<Type == PrimitiveType::TYPE_DECIMALV2, DecimalV2Value,
                                 typename PredicatePrimitiveTypeTraits<Type>::PredicateFieldType>;
    if constexpr (N >= 1 && N <= FIXED_CONTAINER_MAX_SIZE) {
        using Set = std::conditional_t<
                std::is_same_v<T, StringRef>, StringSet<FixedContainer<std::string, N>>,
                HybridSet<Type, FixedContainer<T, N>,
                          vectorized::PredicateColumnType<PredicateEvaluateType<Type>>>>;
        return new InListPredicateBase<Type, PT, Set>(column_id, hybrid_set, char_length);
    } else {
        using Set = std::conditional_t<
                std::is_same_v<T, StringRef>, StringSet<DynamicContainer<std::string>>,
                HybridSet<Type, DynamicContainer<T>,
                          vectorized::PredicateColumnType<PredicateEvaluateType<Type>>>>;
        return new InListPredicateBase<Type, PT, Set>(column_id, hybrid_set, char_length);
    }
}

template <PrimitiveType Type, PredicateType PT>
ColumnPredicate* create_in_list_predicate(uint32_t column_id,
                                          const std::shared_ptr<HybridSetBase>& hybrid_set,
                                          size_t char_length = 0) {
    if (hybrid_set->size() == 1) {
        return _create_in_list_predicate<Type, PT, 1>(column_id, hybrid_set, char_length);
    } else if (hybrid_set->size() == 2) {
        return _create_in_list_predicate<Type, PT, 2>(column_id, hybrid_set, char_length);
    } else if (hybrid_set->size() == 3) {
        return _create_in_list_predicate<Type, PT, 3>(column_id, hybrid_set, char_length);
    } else if (hybrid_set->size() == 4) {
        return _create_in_list_predicate<Type, PT, 4>(column_id, hybrid_set, char_length);
    } else if (hybrid_set->size() == 5) {
        return _create_in_list_predicate<Type, PT, 5>(column_id, hybrid_set, char_length);
    } else if (hybrid_set->size() == 6) {
        return _create_in_list_predicate<Type, PT, 6>(column_id, hybrid_set, char_length);
    } else if (hybrid_set->size() == 7) {
        return _create_in_list_predicate<Type, PT, 7>(column_id, hybrid_set, char_length);
    } else if (hybrid_set->size() == FIXED_CONTAINER_MAX_SIZE) {
        return _create_in_list_predicate<Type, PT, FIXED_CONTAINER_MAX_SIZE>(column_id, hybrid_set,
                                                                             char_length);
    } else {
        return _create_in_list_predicate<Type, PT>(column_id, hybrid_set, char_length);
    }
}

} //namespace doris
