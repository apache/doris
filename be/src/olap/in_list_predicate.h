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

#include "common/exception.h"
#include "decimal12.h"
#include "exprs/hybrid_set.h"
#include "olap/column_predicate.h"
#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/bloom_filter.h"
#include "olap/rowset/segment_v2/inverted_index_cache.h" // IWYU pragma: keep
#include "olap/rowset/segment_v2/inverted_index_reader.h"
#include "runtime/define_primitive_type.h"
#include "runtime/primitive_type.h"
#include "runtime/type_limit.h"
#include "uint24.h"
#include "vec/columns/column_dictionary.h"
#include "vec/common/string_ref.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"

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
#include "common/compile_check_begin.h"
/**
 * Use HybridSetType can avoid virtual function call in the loop.
 * @tparam Type
 * @tparam PT
 * @tparam HybridSetType
 */
template <PrimitiveType Type, PredicateType PT, int N>
class InListPredicateBase final : public ColumnPredicate {
public:
    ENABLE_FACTORY_CREATOR(InListPredicateBase);
    using T = typename PrimitiveTypeTraits<Type>::CppType;
    using HybridSetType = std::conditional_t<
            N >= 1 && N <= FIXED_CONTAINER_MAX_SIZE,
            std::conditional_t<
                    is_string_type(Type), StringSet<FixedContainer<std::string, N>>,
                    HybridSet<Type, FixedContainer<T, N>,
                              vectorized::PredicateColumnType<PredicateEvaluateType<Type>>>>,
            std::conditional_t<
                    is_string_type(Type), StringSet<DynamicContainer<std::string>>,
                    HybridSet<Type, DynamicContainer<T>,
                              vectorized::PredicateColumnType<PredicateEvaluateType<Type>>>>>;
    InListPredicateBase(uint32_t column_id, std::string col_name,
                        const std::shared_ptr<HybridSetBase>& hybrid_set, bool is_opposite,
                        size_t char_length = 0)
            : ColumnPredicate(column_id, col_name, Type, is_opposite),
              _min_value(type_limit<T>::max()),
              _max_value(type_limit<T>::min()) {
        CHECK(hybrid_set != nullptr);

        if constexpr (is_string_type(Type) || Type == TYPE_DECIMALV2 || is_date_type(Type)) {
            _values = std::make_shared<HybridSetType>(false);
            if constexpr (is_string_type(Type)) {
                HybridSetBase::IteratorBase* iter = hybrid_set->begin();
                while (iter->has_next()) {
                    const auto* value = (const StringRef*)(iter->get_value());
                    if constexpr (Type == TYPE_CHAR) {
                        _temp_datas.emplace_back("");
                        _temp_datas.back().resize(std::max(char_length, value->size));
                        memcpy(_temp_datas.back().data(), value->data, value->size);
                        const std::string& str = _temp_datas.back();
                        _values->insert((void*)str.data(), str.length());
                    } else {
                        _values->insert((void*)value->data, value->size);
                    }
                    iter->next();
                }
            } else {
                HybridSetBase::IteratorBase* iter = hybrid_set->begin();
                while (iter->has_next()) {
                    const void* value = iter->get_value();
                    _values->insert(value);
                    iter->next();
                }
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
    InListPredicateBase(const InListPredicateBase<Type, PT, N>& other, uint32_t col_id)
            : ColumnPredicate(other, col_id) {
        _values = other._values;
        _min_value = other._min_value;
        _max_value = other._max_value;
        _temp_datas = other._temp_datas;
        DCHECK(_segment_id_to_value_in_dict_flags.empty());
    }
    InListPredicateBase(const InListPredicateBase<Type, PT, N>& other) = delete;
    std::shared_ptr<ColumnPredicate> clone(uint32_t col_id) const override {
        return InListPredicateBase<Type, PT, N>::create_shared(*this, col_id);
    }

    ~InListPredicateBase() override = default;
    std::string debug_string() const override {
        fmt::memory_buffer debug_string_buffer;
        fmt::format_to(debug_string_buffer, "InListPredicateBase({})",
                       ColumnPredicate::debug_string());
        return fmt::to_string(debug_string_buffer);
    }

    PredicateType type() const override { return PT; }

    bool could_be_erased() const override {
        if ((PT == PredicateType::NOT_IN_LIST && !_opposite) ||
            (PT == PredicateType::IN_LIST && _opposite)) {
            return false;
        }
        return true;
    }
    Status evaluate(const vectorized::IndexFieldNameAndTypePair& name_with_type,
                    IndexIterator* iterator, uint32_t num_rows,
                    roaring::Roaring* result) const override {
        if (iterator == nullptr) {
            return Status::Error<ErrorCode::INVERTED_INDEX_EVALUATE_SKIPPED>(
                    "Inverted index evaluate skipped, no inverted index reader can not support "
                    "in_list");
        }
        // only string type and bkd inverted index reader can be used for in
        if (iterator->get_reader(segment_v2::InvertedIndexReaderType::STRING_TYPE) == nullptr &&
            iterator->get_reader(segment_v2::InvertedIndexReaderType::BKD) == nullptr) {
            //NOT support in list when parser is FULLTEXT for expr inverted index evaluate.
            return Status::Error<ErrorCode::INVERTED_INDEX_EVALUATE_SKIPPED>(
                    "Inverted index evaluate skipped, no inverted index reader can not support "
                    "in_list");
        }
        roaring::Roaring indices;
        HybridSetBase::IteratorBase* iter = _values->begin();
        while (iter->has_next()) {
            const void* ptr = iter->get_value();
            //            auto&& value = PrimitiveTypeConvertor<Type>::to_storage_field_type(
            //                    *reinterpret_cast<const T*>(ptr));
            std::unique_ptr<InvertedIndexQueryParamFactory> query_param = nullptr;
            RETURN_IF_ERROR(InvertedIndexQueryParamFactory::create_query_value<Type>((const T*)ptr,
                                                                                     query_param));
            InvertedIndexQueryType query_type = InvertedIndexQueryType::EQUAL_QUERY;
            InvertedIndexParam param;
            param.column_name = name_with_type.first;
            param.column_type = name_with_type.second;
            param.query_value = query_param->get_value();
            param.query_type = query_type;
            param.num_rows = num_rows;
            param.roaring = std::make_shared<roaring::Roaring>();
            RETURN_IF_ERROR(iterator->read_from_index(&param));
            indices |= *param.roaring;
            iter->next();
        }

        // mask out null_bitmap, since NULL cmp VALUE will produce NULL
        //  and be treated as false in WHERE
        // keep it after query, since query will try to read null_bitmap and put it to cache
        if (iterator->has_null()) {
            InvertedIndexQueryCacheHandle null_bitmap_cache_handle;
            RETURN_IF_ERROR(iterator->read_null_bitmap(&null_bitmap_cache_handle));
            std::shared_ptr<roaring::Roaring> null_bitmap = null_bitmap_cache_handle.get_bitmap();
            if (null_bitmap) {
                *result -= *null_bitmap;
            }
        }

        if constexpr (PT == PredicateType::IN_LIST) {
            *result &= indices;
        } else {
            *result -= indices;
        }
        return Status::OK();
    }

    template <bool is_and>
    void _evaluate_bit(const vectorized::IColumn& column, const uint16_t* sel, uint16_t size,
                       bool* flags) const {
        if (column.is_nullable()) {
            const auto* nullable_col =
                    vectorized::check_and_get_column<vectorized::ColumnNullable>(column);
            const auto& null_bitmap =
                    assert_cast<const vectorized::ColumnUInt8&>(nullable_col->get_null_map_column())
                            .get_data();
            const auto& nested_col = nullable_col->get_nested_column();

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

    bool evaluate_and(const segment_v2::ZoneMap& zone_map) const override {
        if (!zone_map.has_not_null) {
            return false;
        }
        if constexpr (PT == PredicateType::IN_LIST) {
            return Compare::less_equal(zone_map.min_value.template get<Type>(), _max_value) &&
                   Compare::greater_equal(zone_map.max_value.template get<Type>(), _min_value);
        } else {
            return true;
        }
    }

    bool camp_field(const vectorized::Field& min_field, const vectorized::Field& max_field) const {
        if constexpr (PT == PredicateType::IN_LIST) {
            return (Compare::less_equal(min_field.template get<Type>(), _max_value) &&
                    Compare::greater_equal(max_field.template get<Type>(), _min_value)) ||
                   (Compare::greater_equal(max_field.template get<Type>(), _min_value) &&
                    Compare::less_equal(min_field.template get<Type>(), _max_value));
        } else {
            return true;
        }
    }

    bool evaluate_and(vectorized::ParquetPredicate::ColumnStat* statistic) const override {
        bool result = true;
        if ((*statistic->get_stat_func)(statistic, column_id())) {
            vectorized::Field min_field;
            vectorized::Field max_field;
            if (statistic->is_all_null) {
                result = false;
            } else if (!vectorized::ParquetPredicate::parse_min_max_value(
                                statistic->col_schema, statistic->encoded_min_value,
                                statistic->encoded_max_value, *statistic->ctz, &min_field,
                                &max_field)
                                .ok()) [[unlikely]] {
                result = true;
            } else {
                result = camp_field(min_field, max_field);
            }
        }

        if constexpr (PT == PredicateType::IN_LIST) {
            if (result && statistic->get_bloom_filter_func != nullptr &&
                (*statistic->get_bloom_filter_func)(statistic, column_id())) {
                if (!statistic->bloom_filter) {
                    return result;
                }
                return evaluate_and(statistic->bloom_filter.get());
            }
        }
        return result;
    }

    bool evaluate_and(vectorized::ParquetPredicate::CachedPageIndexStat* statistic,
                      RowRanges* row_ranges) const override {
        vectorized::ParquetPredicate::PageIndexStat* stat = nullptr;
        if (!(statistic->get_stat_func)(&stat, column_id())) {
            row_ranges->add(statistic->row_group_range);
            return true;
        }

        for (int page_id = 0; page_id < stat->num_of_pages; page_id++) {
            if (stat->is_all_null[page_id]) {
                // all null page, not need read.
                continue;
            }

            vectorized::Field min_field;
            vectorized::Field max_field;
            if (!vectorized::ParquetPredicate::parse_min_max_value(
                         stat->col_schema, stat->encoded_min_value[page_id],
                         stat->encoded_max_value[page_id], *statistic->ctz, &min_field, &max_field)
                         .ok()) [[unlikely]] {
                row_ranges->add(stat->ranges[page_id]);
                continue;
            };

            if (camp_field(min_field, max_field)) {
                row_ranges->add(stat->ranges[page_id]);
            }
        };
        return row_ranges->count() > 0;
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

    bool evaluate_del(const segment_v2::ZoneMap& zone_map) const override {
        if (zone_map.has_null) {
            return false;
        }
        if constexpr (PT == PredicateType::NOT_IN_LIST) {
            return Compare::greater(zone_map.min_value.template get<Type>(), _max_value) ||
                   Compare::less(zone_map.max_value.template get<Type>(), _min_value);
        } else {
            return false;
        }
    }

    bool evaluate_and(const segment_v2::BloomFilter* bf) const override {
        if constexpr (PT == PredicateType::IN_LIST) {
            // IN predicate can not use ngram bf, just return true to accept
            if (bf->is_ngram_bf()) {
                return true;
            }
            HybridSetBase::IteratorBase* iter = _values->begin();
            while (iter->has_next()) {
                if constexpr (is_string_type(Type)) {
                    const auto* value = (const StringRef*)iter->get_value();
                    if (bf->test_bytes(value->data, value->size)) {
                        return true;
                    }
                } else if constexpr (Type == PrimitiveType::TYPE_DECIMALV2) {
                    // DecimalV2 using decimal12_t in bloom filter in storage layer,
                    // should convert value to decimal12_t
                    // Datev1/DatetimeV1 using VecDatetimeValue in bloom filter, NO need to convert.
                    const T* value = (const T*)(iter->get_value());
                    decimal12_t decimal12_t_val(value->int_value(), value->frac_value());
                    if (bf->test_bytes(reinterpret_cast<const char*>(&decimal12_t_val),
                                       sizeof(decimal12_t))) {
                        return true;
                    }
                } else if constexpr (Type == PrimitiveType::TYPE_DATE) {
                    const T* value = (const T*)(iter->get_value());
                    uint24_t date_value(uint32_t(value->to_olap_date()));
                    if (bf->test_bytes(reinterpret_cast<const char*>(&date_value),
                                       sizeof(uint24_t))) {
                        return true;
                    }
                    // DatetimeV1 using int64_t in bloom filter
                } else if constexpr (Type == PrimitiveType::TYPE_DATETIME) {
                    const T* value = (const T*)(iter->get_value());
                    int64_t datetime_value(value->to_olap_datetime());
                    if (bf->test_bytes(reinterpret_cast<const char*>(&datetime_value),
                                       sizeof(int64_t))) {
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

    double get_ignore_threshold() const override {
        return get_in_list_ignore_thredhold(_values->size());
    }

    bool evaluate_and(const vectorized::ParquetBlockSplitBloomFilter* bf) const override {
        if constexpr (PT == PredicateType::IN_LIST) {
            HybridSetBase::IteratorBase* iter = _values->begin();
            while (iter->has_next()) {
                const T* value = (const T*)(iter->get_value());

                auto test_bytes = [&]<typename V>(const V& val) {
                    return bf->test_bytes(const_cast<char*>(reinterpret_cast<const char*>(&val)),
                                          sizeof(V));
                };

                // Small integers (TINYINT, SMALLINT, INTEGER) -> hash as int32
                if constexpr (Type == PrimitiveType::TYPE_TINYINT ||
                              Type == PrimitiveType::TYPE_SMALLINT ||
                              Type == PrimitiveType::TYPE_INT) {
                    int32_t int32_value = static_cast<int32_t>(*value);
                    if (test_bytes(int32_value)) {
                        return true;
                    }
                } else if constexpr (Type == PrimitiveType::TYPE_BIGINT) {
                    // BIGINT -> hash as int64
                    if (test_bytes(*value)) {
                        return true;
                    }
                } else if constexpr (Type == PrimitiveType::TYPE_DOUBLE) {
                    // DOUBLE -> hash as double
                    if (test_bytes(*value)) {
                        return true;
                    }
                } else if constexpr (Type == PrimitiveType::TYPE_FLOAT) {
                    // FLOAT -> hash as float
                    if (test_bytes(*value)) {
                        return true;
                    }
                } else if constexpr (is_string_type(Type)) {
                    // VARCHAR/STRING -> hash bytes
                    if (bf->test_bytes(value->data(), value->size())) {
                        return true;
                    }
                } else {
                    // Unsupported types: return true (accept)
                    return true;
                }
                iter->next();
            }
            return false;
        } else {
            LOG(FATAL) << "Bloom filter is not supported by predicate type.";
            return true;
        }
    }

private:
    uint16_t _evaluate_inner(const vectorized::IColumn& column, uint16_t* sel,
                             uint16_t size) const override {
        int16_t new_size = 0;

        if (column.is_nullable()) {
            const auto* nullable_col =
                    vectorized::check_and_get_column<vectorized::ColumnNullable>(column);
            const auto& null_map =
                    assert_cast<const vectorized::ColumnUInt8&>(nullable_col->get_null_map_column())
                            .get_data();
            const auto& nested_col = nullable_col->get_nested_column();

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
        return new_size;
    }

    bool _operator(const bool& lhs, const bool& rhs) const {
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
            if constexpr (is_string_type(Type)) {
                const auto* nested_col_ptr =
                        vectorized::check_and_get_column<vectorized::ColumnDictI32>(column);
                const auto& data_array = nested_col_ptr->get_data();
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
                __builtin_unreachable();
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
            if constexpr (is_string_type(Type)) {
                const auto* nested_col_ptr =
                        vectorized::check_and_get_column<vectorized::ColumnDictI32>(column);
                const auto& data_array = nested_col_ptr->get_data();
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
                __builtin_unreachable();
            }
        } else {
            auto* nested_col_ptr = vectorized::check_and_get_column<
                    vectorized::PredicateColumnType<PredicateEvaluateType<Type>>>(column);
            if (nested_col_ptr == nullptr) {
                throw Exception(ErrorCode::INTERNAL_ERROR,
                                "InListPredicateBase: _base_evaluate_bit get invalid column type");
            }

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

    void _update_min_max(const T& value) {
        if (Compare::greater(value, _max_value)) {
            _max_value = value;
        }
        if (Compare::less(value, _min_value)) {
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
#include "common/compile_check_end.h"
} //namespace doris
