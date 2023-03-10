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

#include <cstdint>
#include <roaring/roaring.hh>

#include "decimal12.h"
#include "exprs/hybrid_set.h"
#include "olap/column_predicate.h"
#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/bloom_filter.h"
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
// for decimal12_t
template <>
struct std::hash<doris::decimal12_t> {
    int64_t operator()(const doris::decimal12_t& rhs) const {
        return hash<int64_t>()(rhs.integer) ^ hash<int32_t>()(rhs.fraction);
    }
};

template <>
struct std::equal_to<doris::decimal12_t> {
    bool operator()(const doris::decimal12_t& lhs, const doris::decimal12_t& rhs) const {
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

template <PrimitiveType Type, PredicateType PT>
class InListPredicateBase : public ColumnPredicate {
public:
    using T = typename PredicatePrimitiveTypeTraits<Type>::PredicateFieldType;
    template <typename ConditionType, typename ConvertFunc>
    InListPredicateBase(uint32_t column_id, const ConditionType& conditions,
                        const ConvertFunc& convert, bool is_opposite = false,
                        const TabletColumn* col = nullptr, MemPool* pool = nullptr)
            : ColumnPredicate(column_id, is_opposite),
              _values(new phmap::flat_hash_set<T>()),
              _min_value(type_limit<T>::max()),
              _max_value(type_limit<T>::min()) {
        for (const auto& condition : conditions) {
            T tmp;
            if constexpr (Type == TYPE_STRING || Type == TYPE_CHAR) {
                tmp = convert(*col, condition, pool);
            } else if constexpr (Type == TYPE_DECIMAL32 || Type == TYPE_DECIMAL64 ||
                                 Type == TYPE_DECIMAL128I) {
                tmp = convert(*col, condition);
            } else {
                tmp = convert(condition);
            }
            _values->insert(tmp);
            _update_min_max(tmp);
        }
    }

    InListPredicateBase(uint32_t column_id, const std::shared_ptr<HybridSetBase>& hybrid_set,
                        size_t char_length = 0)
            : ColumnPredicate(column_id, false),
              _min_value(type_limit<T>::max()),
              _max_value(type_limit<T>::min()) {
        using HybridSetType = std::conditional_t<is_string_type(Type), StringSet, HybridSet<Type>>;

        CHECK(hybrid_set != nullptr);

        if constexpr (is_string_type(Type) || Type == TYPE_DECIMALV2 || is_date_type(Type)) {
            _values = new phmap::flat_hash_set<T>();
            auto values = ((HybridSetType*)hybrid_set.get())->get_inner_set();

            if constexpr (is_string_type(Type)) {
                // values' type is "phmap::flat_hash_set<std::string>"
                for (const std::string& value : *values) {
                    StringRef sv = value;
                    if constexpr (Type == TYPE_CHAR) {
                        _temp_datas.push_back("");
                        _temp_datas.back().resize(std::max(char_length, value.size()));
                        memcpy(_temp_datas.back().data(), value.data(), value.size());
                        sv = _temp_datas.back();
                    }
                    _values->insert(sv);
                }
            } else if constexpr (Type == TYPE_DECIMALV2) {
                for (auto& value : *values) {
                    _values->insert({value.int_value(), value.frac_value()});
                }
            } else if constexpr (Type == TYPE_DATE) {
                for (auto& value : *values) {
                    _values->insert(value.to_olap_date());
                }
            } else if constexpr (Type == TYPE_DATETIME) {
                for (auto& value : *values) {
                    _values->insert(value.to_olap_datetime());
                }
            } else {
                CHECK(Type == TYPE_DATETIMEV2 || Type == TYPE_DATEV2);
                for (auto& value : *values) {
                    _values->insert(T(value));
                }
            }
        } else {
            _values = ((HybridSetType*)hybrid_set.get())->get_inner_set();
        }

        for (auto& value : *_values) {
            _update_min_max(value);
        }
    }

    ~InListPredicateBase() override {
        if constexpr (is_string_type(Type) || Type == TYPE_DECIMALV2 || is_date_type(Type)) {
            delete _values;
        }
    }

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
        for (auto value : *_values) {
            bool exact_match;
            Status s = iterator->seek_dictionary(&value, &exact_match);
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

        InvertedIndexQuery<Type> query(column_desc->type_info());

        for (auto& value : *_values) {
            //NOTE: we DO NOT use predicate_params()->value here, because there are multiple values in in_list_predicate.
            //we use value.to_string() to convert StringRef to string, although it is not efficient, because we need to cast it again to std::string in add_range.
            //also don't need to worry about StringRef->std::string->StringRef, because StringRef is just a pointer to the original string.
            if constexpr (std::is_same_v<decltype(value), const StringRef&>) {
                auto value_str = ((StringRef)value).to_string_view();
                RETURN_IF_ERROR(query.add_value_str(InvertedIndexQueryOp::EQUAL_QUERY, value_str,
                                                    predicate_params()->precision,
                                                    predicate_params()->scale));
            } else {
                RETURN_IF_ERROR(
                        query.add_value(InvertedIndexQueryOp::EQUAL_QUERY, std::move(value)));
            }

            //InvertedIndexQueryOp query_type = InvertedIndexQueryOp::EQUAL_QUERY;
            //roaring::Roaring index;
            //RETURN_IF_ERROR(iterator->read_from_inverted_index(column_name, &value, query_type,
            //                                                   num_rows, &index));
            //indices |= index;
        }
        std::unique_ptr<InvertedIndexQueryType> q(new InvertedIndexQueryType(query));
        RETURN_IF_ERROR(
                iterator->read_from_inverted_index(column_name, q.get(), num_rows, &indices));
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
                return _get_zone_map_value<T>(statistic.first->cell_ptr()) <= _max_value &&
                       _get_zone_map_value<T>(statistic.second->cell_ptr()) >= _min_value;
            }
        } else {
            return true;
        }
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
                return _get_zone_map_value<T>(statistic.first->cell_ptr()) > _max_value ||
                       _get_zone_map_value<T>(statistic.second->cell_ptr()) < _min_value;
            }
        } else {
            return false;
        }
    }

    bool evaluate_and(const segment_v2::BloomFilter* bf) const override {
        if constexpr (PT == PredicateType::IN_LIST) {
            for (auto value : *_values) {
                if constexpr (std::is_same_v<T, StringRef>) {
                    if (bf->test_bytes(value.data, value.size)) {
                        return true;
                    }
                } else if constexpr (Type == TYPE_DATE) {
                    if (bf->test_bytes(reinterpret_cast<char*>(&value), sizeof(uint24_t))) {
                        return true;
                    }
                } else {
                    if (bf->test_bytes(reinterpret_cast<char*>(&value), sizeof(value))) {
                        return true;
                    }
                }
            }
            return false;
        } else {
            LOG(FATAL) << "Bloom filter is not supported by predicate type.";
            return true;
        }
    }

    bool can_do_bloom_filter() const override { return PT == PredicateType::IN_LIST; }

private:
    template <typename LeftT, typename RightT>
    bool _operator(const LeftT& lhs, const RightT& rhs) const {
        if constexpr (Type == TYPE_BOOLEAN) {
            DCHECK(_values->size() == 2);
            return PT == PredicateType::IN_LIST;
        } else if constexpr (PT == PredicateType::IN_LIST) {
            return lhs != rhs;
        }
        return lhs == rhs;
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
                    nested_col_ptr->find_codes(*_values, value_in_dict_flags);
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
            auto* nested_col_ptr = vectorized::check_and_get_column<
                    vectorized::PredicateColumnType<PredicateEvaluateType<Type>>>(column);
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
                    if (_operator(_values->find(reinterpret_cast<const T&>(data_array[idx])),
                                  _values->end())) {
                        sel[new_size++] = idx;
                    }
                } else {
                    if (!_operator(_values->find(reinterpret_cast<const T&>(data_array[idx])),
                                   _values->end())) {
                        sel[new_size++] = idx;
                    }
                }
            }
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
                    nested_col_ptr->find_codes(*_values, value_in_dict_flags);
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
                        _operator(_values->find(reinterpret_cast<const T&>(data_array[idx])),
                                  _values->end())) {
                        flags[i] = !is_and;
                    }
                } else {
                    if (is_and ^
                        !_operator(_values->find(reinterpret_cast<const T&>(data_array[idx])),
                                   _values->end())) {
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

    phmap::flat_hash_set<T>* _values;
    mutable std::map<std::pair<RowsetId, uint32_t>, std::vector<vectorized::UInt8>>
            _segment_id_to_value_in_dict_flags;
    T _min_value;
    T _max_value;

    // temp string for char type column
    std::list<std::string> _temp_datas;
};

} //namespace doris
