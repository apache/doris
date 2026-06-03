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

#include "format/new_parquet/parquet_statistics.h"

#include <parquet/api/reader.h>
#include <parquet/api/schema.h>
#include <parquet/bloom_filter.h>
#include <parquet/bloom_filter_reader.h>
#include <parquet/column_page.h>
#include <parquet/encoding.h>
#include <parquet/page_index.h>
#include <parquet/statistics.h>
#include <parquet/types.h>

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <exception>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/primitive_type.h"
#include "core/field.h"
#include "format/new_parquet/parquet_column_schema.h"
#include "runtime/runtime_profile.h"
#include "storage/index/zone_map/zone_map_index.h"
#include "storage/predicate/accept_null_predicate.h"
#include "storage/predicate/column_predicate.h"

namespace doris::parquet {

namespace {

PrimitiveType physical_filter_type(const ParquetColumnSchema& column_schema) {
    if (column_schema.type == nullptr) {
        return INVALID_TYPE;
    }
    switch (remove_nullable(column_schema.type)->get_primitive_type()) {
    case TYPE_BOOLEAN:
    case TYPE_INT:
    case TYPE_BIGINT:
    case TYPE_FLOAT:
    case TYPE_DOUBLE:
    case TYPE_STRING:
        return remove_nullable(column_schema.type)->get_primitive_type();
    default:
        return INVALID_TYPE;
    }
}

template <typename ParquetDType, PrimitiveType DorisType, typename ConvertFn>
bool set_typed_min_max(const std::shared_ptr<::parquet::Statistics>& statistics, ConvertFn convert,
                       ParquetColumnStatistics* column_statistics) {
    auto typed_statistics =
            std::static_pointer_cast<::parquet::TypedStatistics<ParquetDType>>(statistics);
    column_statistics->min_value = Field::create_field<DorisType>(convert(typed_statistics->min()));
    column_statistics->max_value = Field::create_field<DorisType>(convert(typed_statistics->max()));
    return true;
}

bool set_string_min_max(const std::shared_ptr<::parquet::Statistics>& statistics,
                        const ::parquet::ColumnDescriptor* descriptor,
                        ParquetColumnStatistics* column_statistics) {
    switch (statistics->physical_type()) {
    case ::parquet::Type::BYTE_ARRAY: {
        auto typed_statistics =
                std::static_pointer_cast<::parquet::TypedStatistics<::parquet::ByteArrayType>>(
                        statistics);
        column_statistics->min_value = Field::create_field<TYPE_STRING>(
                ::parquet::ByteArrayToString(typed_statistics->min()));
        column_statistics->max_value = Field::create_field<TYPE_STRING>(
                ::parquet::ByteArrayToString(typed_statistics->max()));
        return true;
    }
    case ::parquet::Type::FIXED_LEN_BYTE_ARRAY: {
        if (descriptor == nullptr || descriptor->type_length() <= 0) {
            return false;
        }
        auto typed_statistics =
                std::static_pointer_cast<::parquet::TypedStatistics<::parquet::FLBAType>>(
                        statistics);
        const int type_length = descriptor->type_length();
        column_statistics->min_value = Field::create_field<TYPE_STRING>(std::string(
                reinterpret_cast<const char*>(typed_statistics->min().ptr), type_length));
        column_statistics->max_value = Field::create_field<TYPE_STRING>(std::string(
                reinterpret_cast<const char*>(typed_statistics->max().ptr), type_length));
        return true;
    }
    default:
        return false;
    }
}

bool is_null_only_predicate(const ColumnPredicate& predicate) {
    return predicate.type() == PredicateType::IS_NULL ||
           predicate.type() == PredicateType::IS_NOT_NULL;
}

bool is_supported_dictionary_predicate(const ColumnPredicate& predicate) {
    switch (predicate.type()) {
    case PredicateType::EQ:
    case PredicateType::IN_LIST:
        return true;
    default:
        return false;
    }
}

bool is_bloom_filter_prunable_predicate(const ColumnPredicate& predicate) {
    if (dynamic_cast<const AcceptNullPredicate*>(&predicate) != nullptr ||
        is_null_only_predicate(predicate)) {
        return false;
    }
    return predicate.can_do_bloom_filter(false);
}

template <typename T>
T load_predicate_value(const char* data) {
    T value;
    memcpy(&value, data, sizeof(T));
    return value;
}

class ArrowParquetBloomFilterAdapter final : public segment_v2::BloomFilter {
public:
    ArrowParquetBloomFilterAdapter(const ParquetColumnSchema& column_schema,
                                   const ::parquet::BloomFilter& bloom_filter)
            : _column_schema(column_schema), _bloom_filter(bloom_filter) {}

    void add_bytes(const char* buf, size_t size) override { DORIS_CHECK(false); }

    bool test_bytes(const char* buf, size_t size) const override {
        if (buf == nullptr) {
            return true;
        }
        switch (physical_filter_type(_column_schema)) {
        case TYPE_BOOLEAN:
            return test_boolean(buf, size);
        case TYPE_INT:
            return test_int32(buf, size);
        case TYPE_BIGINT:
            return test_int64(buf, size);
        case TYPE_FLOAT:
            return test_float(buf, size);
        case TYPE_DOUBLE:
            return test_double(buf, size);
        case TYPE_STRING:
            return test_string(buf, size);
        default:
            return true;
        }
    }

    void set_has_null(bool has_null) override { DORIS_CHECK(!has_null); }
    bool has_null() const override { return false; }
    void add_hash(uint64_t hash) override { DORIS_CHECK(false); }
    bool test_hash(uint64_t hash) const override { return _bloom_filter.FindHash(hash); }

private:
    bool test_boolean(const char* buf, size_t size) const {
        if (size == sizeof(bool)) {
            const int32_t value = load_predicate_value<bool>(buf) ? 1 : 0;
            return _bloom_filter.FindHash(_bloom_filter.Hash(value));
        }
        if (size == sizeof(int32_t)) {
            const int32_t value = load_predicate_value<int32_t>(buf);
            return _bloom_filter.FindHash(_bloom_filter.Hash(value != 0 ? 1 : 0));
        }
        return true;
    }

    bool test_int32(const char* buf, size_t size) const {
        if (size == sizeof(int8_t)) {
            return find_int32(static_cast<int32_t>(load_predicate_value<int8_t>(buf)));
        }
        if (size == sizeof(int16_t)) {
            return find_int32(static_cast<int32_t>(load_predicate_value<int16_t>(buf)));
        }
        if (size == sizeof(int32_t)) {
            return find_int32(load_predicate_value<int32_t>(buf));
        }
        return true;
    }

    bool test_int64(const char* buf, size_t size) const {
        if (size != sizeof(int64_t)) {
            return true;
        }
        const int64_t value = load_predicate_value<int64_t>(buf);
        return _bloom_filter.FindHash(_bloom_filter.Hash(value));
    }

    bool test_float(const char* buf, size_t size) const {
        if (size != sizeof(float)) {
            return true;
        }
        const float value = load_predicate_value<float>(buf);
        return _bloom_filter.FindHash(_bloom_filter.Hash(value));
    }

    bool test_double(const char* buf, size_t size) const {
        if (size != sizeof(double)) {
            return true;
        }
        const double value = load_predicate_value<double>(buf);
        return _bloom_filter.FindHash(_bloom_filter.Hash(value));
    }

    bool test_string(const char* buf, size_t size) const {
        ::parquet::ByteArray value(static_cast<uint32_t>(size),
                                   reinterpret_cast<const uint8_t*>(buf));
        return _bloom_filter.FindHash(_bloom_filter.Hash(&value));
    }

    bool find_int32(int32_t value) const {
        return _bloom_filter.FindHash(_bloom_filter.Hash(value));
    }

    const ParquetColumnSchema& _column_schema;
    const ::parquet::BloomFilter& _bloom_filter;
};

struct RowGroupBloomFilterCache {
    ::parquet::BloomFilterReader* bloom_filter_reader = nullptr;
    std::map<int, std::unique_ptr<::parquet::BloomFilter>> column_bloom_filters;
    std::set<int> loaded_columns;

    ::parquet::BloomFilter* get(int row_group_idx, int leaf_column_id,
                                ParquetPruningStats* pruning_stats) {
        if (bloom_filter_reader == nullptr || leaf_column_id < 0) {
            return nullptr;
        }
        if (loaded_columns.find(leaf_column_id) == loaded_columns.end()) {
            loaded_columns.insert(leaf_column_id);
            try {
                std::shared_ptr<::parquet::RowGroupBloomFilterReader> row_group_reader;
                if (pruning_stats != nullptr) {
                    SCOPED_RAW_TIMER(&pruning_stats->bloom_filter_read_time);
                    row_group_reader = bloom_filter_reader->RowGroup(row_group_idx);
                    if (row_group_reader != nullptr) {
                        column_bloom_filters[leaf_column_id] =
                                row_group_reader->GetColumnBloomFilter(leaf_column_id);
                    }
                } else {
                    row_group_reader = bloom_filter_reader->RowGroup(row_group_idx);
                    if (row_group_reader != nullptr) {
                        column_bloom_filters[leaf_column_id] =
                                row_group_reader->GetColumnBloomFilter(leaf_column_id);
                    }
                }
            } catch (const ::parquet::ParquetException&) {
                return nullptr;
            } catch (const std::exception&) {
                return nullptr;
            }
        }
        auto it = column_bloom_filters.find(leaf_column_id);
        return it == column_bloom_filters.end() ? nullptr : it->second.get();
    }
};

ParquetRowGroupPruneReason BloomFilterPruneReason(
        int row_group_idx, const std::vector<std::unique_ptr<ParquetColumnSchema>>& schema,
        const reader::FileColumnPredicateFilter& column_filter,
        RowGroupBloomFilterCache* bloom_filter_cache, ParquetPruningStats* pruning_stats) {
    if (bloom_filter_cache == nullptr || column_filter.predicates.empty()) {
        return ParquetRowGroupPruneReason::NONE;
    }
    DCHECK_LT(column_filter.file_column_id, schema.size());
    const auto& column_schema = *schema[column_filter.file_column_id];
    if (column_schema.kind != ParquetColumnSchemaKind::PRIMITIVE ||
        column_schema.leaf_column_id < 0 ||
        !ParquetStatisticsUtils::BloomFilterSupported(column_schema)) {
        return ParquetRowGroupPruneReason::NONE;
    }
    for (const auto& column_predicate : column_filter.predicates) {
        if (column_predicate == nullptr || !is_bloom_filter_prunable_predicate(*column_predicate)) {
            return ParquetRowGroupPruneReason::NONE;
        }
    }
    auto* bloom_filter =
            bloom_filter_cache->get(row_group_idx, column_schema.leaf_column_id, pruning_stats);
    if (bloom_filter == nullptr) {
        return ParquetRowGroupPruneReason::NONE;
    }
    return ParquetStatisticsUtils::BloomFilterExcludes(column_schema, column_filter, *bloom_filter)
                   ? ParquetRowGroupPruneReason::BLOOM_FILTER
                   : ParquetRowGroupPruneReason::NONE;
}

bool is_dictionary_data_encoding(::parquet::Encoding::type encoding) {
    return encoding == ::parquet::Encoding::PLAIN_DICTIONARY ||
           encoding == ::parquet::Encoding::RLE_DICTIONARY;
}

bool is_level_encoding(::parquet::Encoding::type encoding) {
    return encoding == ::parquet::Encoding::RLE || encoding == ::parquet::Encoding::BIT_PACKED;
}

bool is_data_page_type(::parquet::PageType::type page_type) {
    return page_type == ::parquet::PageType::DATA_PAGE ||
           page_type == ::parquet::PageType::DATA_PAGE_V2;
}

bool is_dictionary_encoded_chunk(const ::parquet::ColumnChunkMetaData& column_metadata) {
    if (!column_metadata.has_dictionary_page()) {
        return false;
    }

    const auto& encoding_stats = column_metadata.encoding_stats();
    if (!encoding_stats.empty()) {
        bool has_dictionary_data_page = false;
        for (const auto& encoding_stat : encoding_stats) {
            if (!is_data_page_type(encoding_stat.page_type) || encoding_stat.count <= 0) {
                continue;
            }
            if (!is_dictionary_data_encoding(encoding_stat.encoding)) {
                return false;
            }
            has_dictionary_data_page = true;
        }
        return has_dictionary_data_page;
    }

    bool has_dictionary_encoding = false;
    for (const auto encoding : column_metadata.encodings()) {
        if (is_dictionary_data_encoding(encoding)) {
            has_dictionary_encoding = true;
            continue;
        }
        if (!is_level_encoding(encoding)) {
            return false;
        }
    }
    return has_dictionary_encoding;
}

bool supports_dictionary_pruning(const ParquetColumnSchema& column_schema,
                                 const ::parquet::ColumnChunkMetaData& column_metadata,
                                 const reader::FileColumnPredicateFilter& column_filter) {
    if (column_schema.kind != ParquetColumnSchemaKind::PRIMITIVE ||
        column_schema.descriptor == nullptr || column_schema.type == nullptr) {
        return false;
    }
    if (!column_schema.type_descriptor.is_string_like) {
        return false;
    }
    if (column_metadata.type() != ::parquet::Type::BYTE_ARRAY &&
        column_metadata.type() != ::parquet::Type::FIXED_LEN_BYTE_ARRAY) {
        return false;
    }
    for (const auto& column_predicate : column_filter.predicates) {
        if (column_predicate == nullptr || !is_supported_dictionary_predicate(*column_predicate)) {
            return false;
        }
    }
    return true;
}

struct OwnedDictionaryWords {
    std::vector<std::string> values;
    std::vector<StringRef> refs;

    void clear() {
        values.clear();
        refs.clear();
    }

    void build_refs() {
        refs.reserve(values.size());
        for (const auto& value : values) {
            refs.emplace_back(value.data(), value.size());
        }
    }
};

bool read_dictionary_words(::parquet::ParquetFileReader* file_reader, int row_group_idx,
                           int leaf_column_id, const ParquetColumnSchema& column_schema,
                           OwnedDictionaryWords* dict_words) {
    DORIS_CHECK(dict_words != nullptr);
    dict_words->clear();
    if (file_reader == nullptr || leaf_column_id < 0) {
        return false;
    }

    auto row_group_reader = file_reader->RowGroup(row_group_idx);
    if (row_group_reader == nullptr) {
        return false;
    }
    auto page_reader = row_group_reader->GetColumnPageReader(leaf_column_id);
    if (page_reader == nullptr) {
        return false;
    }

    std::shared_ptr<::parquet::Page> page;
    try {
        page = page_reader->NextPage();
    } catch (const ::parquet::ParquetException&) {
        return false;
    } catch (const std::exception&) {
        return false;
    }
    if (page == nullptr || page->type() != ::parquet::PageType::DICTIONARY_PAGE) {
        return false;
    }
    const auto* dictionary_page = static_cast<const ::parquet::DictionaryPage*>(page.get());
    if (dictionary_page->encoding() != ::parquet::Encoding::PLAIN &&
        dictionary_page->encoding() != ::parquet::Encoding::PLAIN_DICTIONARY) {
        return false;
    }
    const int32_t dictionary_length = dictionary_page->num_values();
    if (dictionary_length <= 0) {
        return false;
    }
    const auto* dictionary_data = dictionary_page->data();
    const int dictionary_size = dictionary_page->size();

    dict_words->values.reserve(static_cast<size_t>(dictionary_length));
    if (column_schema.descriptor->physical_type() == ::parquet::Type::BYTE_ARRAY) {
        auto decoder = ::parquet::MakeTypedDecoder<::parquet::ByteArrayType>(
                ::parquet::Encoding::PLAIN, column_schema.descriptor);
        decoder->SetData(dictionary_length, dictionary_data, dictionary_size);
        std::vector<::parquet::ByteArray> byte_array_values(static_cast<size_t>(dictionary_length));
        if (decoder->Decode(byte_array_values.data(), dictionary_length) != dictionary_length) {
            return false;
        }
        for (int32_t dict_idx = 0; dict_idx < dictionary_length; ++dict_idx) {
            dict_words->values.emplace_back(
                    reinterpret_cast<const char*>(byte_array_values[dict_idx].ptr),
                    byte_array_values[dict_idx].len);
        }
        dict_words->build_refs();
        return true;
    }
    if (column_schema.descriptor->physical_type() == ::parquet::Type::FIXED_LEN_BYTE_ARRAY) {
        const int type_length = column_schema.descriptor->type_length();
        if (type_length <= 0) {
            return false;
        }
        auto decoder = ::parquet::MakeTypedDecoder<::parquet::FLBAType>(::parquet::Encoding::PLAIN,
                                                                        column_schema.descriptor);
        decoder->SetData(dictionary_length, dictionary_data, dictionary_size);
        std::vector<::parquet::FixedLenByteArray> flba_values(
                static_cast<size_t>(dictionary_length));
        if (decoder->Decode(flba_values.data(), dictionary_length) != dictionary_length) {
            return false;
        }
        for (int32_t dict_idx = 0; dict_idx < dictionary_length; ++dict_idx) {
            dict_words->values.emplace_back(
                    reinterpret_cast<const char*>(flba_values[dict_idx].ptr), type_length);
        }
        dict_words->build_refs();
        return true;
    }
    return false;
}

segment_v2::ZoneMap to_column_predicate_statistics(const ParquetColumnStatistics& statistics) {
    segment_v2::ZoneMap predicate_statistics;
    predicate_statistics.min_value = statistics.min_value;
    predicate_statistics.max_value = statistics.max_value;
    predicate_statistics.has_null = statistics.has_null;
    predicate_statistics.has_not_null = statistics.has_not_null;
    return predicate_statistics;
}

} // namespace

ParquetColumnStatistics ParquetStatisticsUtils::TransformColumnStatistics(
        const ParquetColumnSchema& column_schema,
        const std::shared_ptr<::parquet::Statistics>& statistics) {
    ParquetColumnStatistics result;
    if (statistics == nullptr) {
        return result;
    }

    result.has_null = statistics->HasNullCount() && statistics->null_count() > 0;
    result.has_not_null = statistics->num_values() > 0 || statistics->HasMinMax();
    result.has_null_count = statistics->HasNullCount();
    if (!result.has_not_null || !statistics->HasMinMax()) {
        return result;
    }

    switch (statistics->physical_type()) {
    case ::parquet::Type::BOOLEAN:
        result.has_min_max = set_typed_min_max<::parquet::BooleanType, TYPE_BOOLEAN>(
                statistics, [](bool value) { return static_cast<UInt8>(value); }, &result);
        return result;
    case ::parquet::Type::INT32:
        result.has_min_max = set_typed_min_max<::parquet::Int32Type, TYPE_INT>(
                statistics, [](int32_t value) { return value; }, &result);
        return result;
    case ::parquet::Type::INT64:
        result.has_min_max = set_typed_min_max<::parquet::Int64Type, TYPE_BIGINT>(
                statistics, [](int64_t value) { return value; }, &result);
        return result;
    case ::parquet::Type::FLOAT:
        result.has_min_max = set_typed_min_max<::parquet::FloatType, TYPE_FLOAT>(
                statistics, [](float value) { return value; }, &result);
        return result;
    case ::parquet::Type::DOUBLE:
        result.has_min_max = set_typed_min_max<::parquet::DoubleType, TYPE_DOUBLE>(
                statistics, [](double value) { return value; }, &result);
        return result;
    case ::parquet::Type::BYTE_ARRAY:
    case ::parquet::Type::FIXED_LEN_BYTE_ARRAY:
        result.has_min_max = set_string_min_max(statistics, column_schema.descriptor, &result);
        return result;
    default:
        return result;
    }
}

bool ParquetStatisticsUtils::CheckStatistics(const reader::FileColumnPredicateFilter& column_filter,
                                             const ParquetColumnStatistics& statistics) {
    if (!statistics.has_any_statistics()) {
        return false;
    }

    for (const auto& column_predicate : column_filter.predicates) {
        if (is_null_only_predicate(*column_predicate)) {
            if (!statistics.has_null_count) {
                continue;
            }
        } else if (!statistics.has_any_statistics()) {
            continue;
        }
        if (!column_predicate->evaluate_and(to_column_predicate_statistics(statistics))) {
            return true;
        }
    }
    return false;
}

ParquetRowGroupPruneReason ParquetStatisticsUtils::RowGroupPruneReason(
        const ::parquet::RowGroupMetaData& row_group, ::parquet::ParquetFileReader* file_reader,
        int row_group_idx, const std::vector<std::unique_ptr<ParquetColumnSchema>>& schema,
        const reader::FileColumnPredicateFilter& column_filter) {
    if (column_filter.predicates.empty()) {
        return ParquetRowGroupPruneReason::NONE;
    }
    DCHECK_LT(column_filter.file_column_id, schema.size());
    const auto& column_schema = *schema[column_filter.file_column_id];
    if (column_schema.kind != ParquetColumnSchemaKind::PRIMITIVE ||
        column_schema.leaf_column_id < 0) {
        return ParquetRowGroupPruneReason::NONE;
    }
    DCHECK_LT(column_schema.leaf_column_id, row_group.num_columns());
    auto column_chunk = row_group.ColumnChunk(column_schema.leaf_column_id);
    if (column_chunk == nullptr) {
        return ParquetRowGroupPruneReason::NONE;
    }
    if (CheckStatistics(column_filter,
                        TransformColumnStatistics(column_schema, column_chunk->statistics()))) {
        return ParquetRowGroupPruneReason::STATISTICS;
    }
    if (!supports_dictionary_pruning(column_schema, *column_chunk, column_filter) ||
        !is_dictionary_encoded_chunk(*column_chunk)) {
        return ParquetRowGroupPruneReason::NONE;
    }
    OwnedDictionaryWords dict_words;
    if (!read_dictionary_words(file_reader, row_group_idx, column_schema.leaf_column_id,
                               column_schema, &dict_words)) {
        return ParquetRowGroupPruneReason::NONE;
    }
    for (const auto& column_predicate : column_filter.predicates) {
        if (!column_predicate->evaluate_and(dict_words.refs.data(), dict_words.refs.size())) {
            return ParquetRowGroupPruneReason::DICTIONARY;
        }
    }
    return ParquetRowGroupPruneReason::NONE;
}

bool ParquetStatisticsUtils::RowGroupExcludes(
        const ::parquet::RowGroupMetaData& row_group, ::parquet::ParquetFileReader* file_reader,
        int row_group_idx, const std::vector<std::unique_ptr<ParquetColumnSchema>>& schema,
        const reader::FileColumnPredicateFilter& column_filter) {
    return RowGroupPruneReason(row_group, file_reader, row_group_idx, schema, column_filter) !=
           ParquetRowGroupPruneReason::NONE;
}

Status ParquetStatisticsUtils::SelectRowGroups(
        const ::parquet::FileMetaData& metadata, ::parquet::ParquetFileReader* file_reader,
        const std::vector<std::unique_ptr<ParquetColumnSchema>>& file_schema,
        const reader::FileScanRequest& request, std::vector<int>* selected_row_groups,
        bool enable_bloom_filter, ParquetPruningStats* pruning_stats) {
    if (selected_row_groups == nullptr) {
        return Status::InvalidArgument("selected_row_groups is null");
    }
    selected_row_groups->clear();

    const int num_row_groups = metadata.num_row_groups();
    if (pruning_stats != nullptr) {
        pruning_stats->total_row_groups = num_row_groups;
    }
    selected_row_groups->reserve(num_row_groups);
    for (int row_group_idx = 0; row_group_idx < num_row_groups; ++row_group_idx) {
        auto row_group = metadata.RowGroup(row_group_idx);
        if (row_group == nullptr) {
            selected_row_groups->push_back(row_group_idx);
            continue;
        }
        bool drop = false;
        RowGroupBloomFilterCache bloom_filter_cache;
        if (enable_bloom_filter && file_reader != nullptr) {
            try {
                bloom_filter_cache.bloom_filter_reader = &file_reader->GetBloomFilterReader();
            } catch (const ::parquet::ParquetException&) {
                bloom_filter_cache.bloom_filter_reader = nullptr;
            } catch (const std::exception&) {
                bloom_filter_cache.bloom_filter_reader = nullptr;
            }
        }
        for (const auto& column_filter : request.column_predicate_filters) {
            const auto prune_reason = RowGroupPruneReason(*row_group, file_reader, row_group_idx,
                                                          file_schema, column_filter);
            auto effective_prune_reason = prune_reason;
            if (effective_prune_reason == ParquetRowGroupPruneReason::NONE && enable_bloom_filter) {
                effective_prune_reason =
                        BloomFilterPruneReason(row_group_idx, file_schema, column_filter,
                                               &bloom_filter_cache, pruning_stats);
            }
            if (effective_prune_reason == ParquetRowGroupPruneReason::NONE) {
                continue;
            }
            drop = true;
            if (pruning_stats != nullptr) {
                pruning_stats->filtered_group_rows += row_group->num_rows();
                if (effective_prune_reason == ParquetRowGroupPruneReason::STATISTICS) {
                    ++pruning_stats->filtered_row_groups_by_statistics;
                } else if (effective_prune_reason == ParquetRowGroupPruneReason::DICTIONARY) {
                    ++pruning_stats->filtered_row_groups_by_dictionary;
                } else if (effective_prune_reason == ParquetRowGroupPruneReason::BLOOM_FILTER) {
                    ++pruning_stats->filtered_row_groups_by_bloom_filter;
                }
                break;
            }
            break;
        }
        if (drop) {
            continue;
        }
        selected_row_groups->push_back(row_group_idx);
    }
    return Status::OK();
}

bool ParquetStatisticsUtils::BloomFilterSupported(const ParquetColumnSchema& column_schema) {
    switch (physical_filter_type(column_schema)) {
    case TYPE_BOOLEAN:
    case TYPE_INT:
    case TYPE_BIGINT:
    case TYPE_FLOAT:
    case TYPE_DOUBLE:
    case TYPE_STRING:
        return true;
    default:
        return false;
    }
}

bool ParquetStatisticsUtils::BloomFilterExcludes(
        const ParquetColumnSchema& column_schema,
        const reader::FileColumnPredicateFilter& column_filter,
        const ::parquet::BloomFilter& bloom_filter) {
    if (!BloomFilterSupported(column_schema)) {
        return false;
    }
    ArrowParquetBloomFilterAdapter adapter(column_schema, bloom_filter);
    for (const auto& column_predicate : column_filter.predicates) {
        if (column_predicate == nullptr || !is_bloom_filter_prunable_predicate(*column_predicate)) {
            return false;
        }
        if (!column_predicate->evaluate_and(&adapter)) {
            return true;
        }
    }
    return false;
}

Status select_row_groups_by_statistics(
        const ::parquet::FileMetaData& metadata, ::parquet::ParquetFileReader* file_reader,
        const std::vector<std::unique_ptr<ParquetColumnSchema>>& file_schema,
        const reader::FileScanRequest& request, std::vector<int>* selected_row_groups,
        bool enable_bloom_filter, ParquetPruningStats* pruning_stats) {
    return ParquetStatisticsUtils::SelectRowGroups(metadata, file_reader, file_schema, request,
                                                   selected_row_groups, enable_bloom_filter,
                                                   pruning_stats);
}

namespace {

template <typename ParquetDType, PrimitiveType DorisType, typename ConvertFn>
bool set_page_typed_min_max(const std::shared_ptr<::parquet::ColumnIndex>& column_index,
                            size_t page_idx, ConvertFn convert,
                            ParquetColumnStatistics* page_statistics) {
    const auto typed_index =
            std::static_pointer_cast<::parquet::TypedColumnIndex<ParquetDType>>(column_index);
    if (page_idx >= typed_index->min_values().size() ||
        page_idx >= typed_index->max_values().size()) {
        return false;
    }
    page_statistics->min_value =
            Field::create_field<DorisType>(convert(typed_index->min_values()[page_idx]));
    page_statistics->max_value =
            Field::create_field<DorisType>(convert(typed_index->max_values()[page_idx]));
    page_statistics->has_min_max = true;
    return true;
}

bool set_page_string_min_max(const std::shared_ptr<::parquet::ColumnIndex>& column_index,
                             const ParquetColumnSchema& column_schema, size_t page_idx,
                             ParquetColumnStatistics* page_statistics) {
    switch (column_schema.descriptor->physical_type()) {
    case ::parquet::Type::BYTE_ARRAY: {
        const auto typed_index =
                std::static_pointer_cast<::parquet::ByteArrayColumnIndex>(column_index);
        if (page_idx >= typed_index->min_values().size() ||
            page_idx >= typed_index->max_values().size()) {
            return false;
        }
        page_statistics->min_value = Field::create_field<TYPE_STRING>(
                ::parquet::ByteArrayToString(typed_index->min_values()[page_idx]));
        page_statistics->max_value = Field::create_field<TYPE_STRING>(
                ::parquet::ByteArrayToString(typed_index->max_values()[page_idx]));
        page_statistics->has_min_max = true;
        return true;
    }
    case ::parquet::Type::FIXED_LEN_BYTE_ARRAY: {
        const int type_length = column_schema.descriptor->type_length();
        if (type_length <= 0) {
            return false;
        }
        const auto typed_index = std::static_pointer_cast<::parquet::FLBAColumnIndex>(column_index);
        if (page_idx >= typed_index->min_values().size() ||
            page_idx >= typed_index->max_values().size()) {
            return false;
        }
        page_statistics->min_value = Field::create_field<TYPE_STRING>(
                std::string(reinterpret_cast<const char*>(typed_index->min_values()[page_idx].ptr),
                            type_length));
        page_statistics->max_value = Field::create_field<TYPE_STRING>(
                std::string(reinterpret_cast<const char*>(typed_index->max_values()[page_idx].ptr),
                            type_length));
        page_statistics->has_min_max = true;
        return true;
    }
    default:
        return false;
    }
}

bool set_page_min_max(const std::shared_ptr<::parquet::ColumnIndex>& column_index,
                      const ParquetColumnSchema& column_schema, size_t page_idx,
                      ParquetColumnStatistics* page_statistics) {
    switch (column_schema.descriptor->physical_type()) {
    case ::parquet::Type::BOOLEAN:
        return set_page_typed_min_max<::parquet::BooleanType, TYPE_BOOLEAN>(
                column_index, page_idx, [](bool value) { return static_cast<UInt8>(value); },
                page_statistics);
    case ::parquet::Type::INT32:
        return set_page_typed_min_max<::parquet::Int32Type, TYPE_INT>(
                column_index, page_idx, [](int32_t value) { return value; }, page_statistics);
    case ::parquet::Type::INT64:
        return set_page_typed_min_max<::parquet::Int64Type, TYPE_BIGINT>(
                column_index, page_idx, [](int64_t value) { return value; }, page_statistics);
    case ::parquet::Type::FLOAT:
        return set_page_typed_min_max<::parquet::FloatType, TYPE_FLOAT>(
                column_index, page_idx, [](float value) { return value; }, page_statistics);
    case ::parquet::Type::DOUBLE:
        return set_page_typed_min_max<::parquet::DoubleType, TYPE_DOUBLE>(
                column_index, page_idx, [](double value) { return value; }, page_statistics);
    case ::parquet::Type::BYTE_ARRAY:
    case ::parquet::Type::FIXED_LEN_BYTE_ARRAY:
        return set_page_string_min_max(column_index, column_schema, page_idx, page_statistics);
    default:
        return false;
    }
}

bool build_page_statistics(const std::shared_ptr<::parquet::ColumnIndex>& column_index,
                           const ParquetColumnSchema& column_schema, size_t page_idx,
                           ParquetColumnStatistics* page_statistics) {
    DORIS_CHECK(page_statistics != nullptr);
    *page_statistics = ParquetColumnStatistics {};

    const auto& null_pages = column_index->null_pages();
    if (!column_index->has_null_counts() || page_idx >= null_pages.size() ||
        page_idx >= column_index->null_counts().size()) {
        return false;
    }

    page_statistics->has_null_count = true;
    page_statistics->has_null = column_index->null_counts()[page_idx] > 0;
    page_statistics->has_not_null = !null_pages[page_idx];
    if (!page_statistics->has_not_null) {
        return true;
    }
    return set_page_min_max(column_index, column_schema, page_idx, page_statistics);
}

std::vector<RowRange> intersect_ranges(const std::vector<RowRange>& left,
                                       const std::vector<RowRange>& right) {
    std::vector<RowRange> result;
    size_t left_idx = 0;
    size_t right_idx = 0;
    while (left_idx < left.size() && right_idx < right.size()) {
        const int64_t left_start = left[left_idx].start;
        const int64_t left_end = left_start + left[left_idx].length;
        const int64_t right_start = right[right_idx].start;
        const int64_t right_end = right_start + right[right_idx].length;
        const int64_t start = std::max(left_start, right_start);
        const int64_t end = std::min(left_end, right_end);
        if (start < end) {
            result.push_back(RowRange {start, end - start});
        }
        if (left_end < right_end) {
            ++left_idx;
        } else {
            ++right_idx;
        }
    }
    return result;
}

int64_t count_range_rows(const std::vector<RowRange>& ranges) {
    int64_t rows = 0;
    for (const auto& range : ranges) {
        rows += range.length;
    }
    return rows;
}

void append_page_range(const ::parquet::OffsetIndex& offset_index, size_t page_idx,
                       int64_t row_group_rows, std::vector<RowRange>* ranges) {
    const auto& page_locations = offset_index.page_locations();
    const int64_t start = page_locations[page_idx].first_row_index;
    const int64_t end = page_idx + 1 == page_locations.size()
                                ? row_group_rows
                                : page_locations[page_idx + 1].first_row_index;
    DORIS_CHECK(start >= 0);
    DORIS_CHECK(end >= start);
    DORIS_CHECK(end <= row_group_rows);
    if (start == end) {
        return;
    }
    if (!ranges->empty()) {
        auto& previous = ranges->back();
        if (previous.start + previous.length == start) {
            previous.length += end - start;
            return;
        }
    }
    ranges->push_back(RowRange {start, end - start});
}

bool select_ranges_for_filter(const std::shared_ptr<::parquet::RowGroupPageIndexReader>& row_group,
                              const std::vector<std::unique_ptr<ParquetColumnSchema>>& file_schema,
                              const reader::FileColumnPredicateFilter& column_filter,
                              int64_t row_group_rows, std::vector<RowRange>* ranges) {
    if (column_filter.predicates.empty()) {
        return false;
    }
    DORIS_CHECK(column_filter.file_column_id >= 0);
    DORIS_CHECK(column_filter.file_column_id < static_cast<int>(file_schema.size()));
    const auto& column_schema = *file_schema[column_filter.file_column_id];
    if (column_schema.kind != ParquetColumnSchemaKind::PRIMITIVE ||
        column_schema.descriptor == nullptr || column_schema.leaf_column_id < 0) {
        return false;
    }

    std::shared_ptr<::parquet::ColumnIndex> column_index;
    std::shared_ptr<::parquet::OffsetIndex> offset_index;
    try {
        column_index = row_group->GetColumnIndex(column_schema.leaf_column_id);
        offset_index = row_group->GetOffsetIndex(column_schema.leaf_column_id);
    } catch (const ::parquet::ParquetException&) {
        return false;
    } catch (const std::exception&) {
        return false;
    }
    if (column_index == nullptr || offset_index == nullptr ||
        column_index->null_pages().size() != offset_index->page_locations().size()) {
        return false;
    }

    ranges->clear();
    const auto page_count = offset_index->page_locations().size();
    for (size_t page_idx = 0; page_idx < page_count; ++page_idx) {
        ParquetColumnStatistics page_statistics;
        if (!build_page_statistics(column_index, column_schema, page_idx, &page_statistics)) {
            ranges->clear();
            return false;
        }
        if (ParquetStatisticsUtils::CheckStatistics(column_filter, page_statistics)) {
            continue;
        }
        append_page_range(*offset_index, page_idx, row_group_rows, ranges);
    }
    return true;
}

} // namespace

Status select_row_group_ranges_by_page_index(
        ::parquet::ParquetFileReader* file_reader,
        const std::vector<std::unique_ptr<ParquetColumnSchema>>& file_schema,
        const reader::FileScanRequest& request, int row_group_idx, int64_t row_group_rows,
        std::vector<RowRange>* selected_ranges, ParquetPruningStats* pruning_stats) {
    DORIS_CHECK(selected_ranges != nullptr);
    selected_ranges->clear();
    if (row_group_rows <= 0) {
        return Status::OK();
    }
    selected_ranges->push_back(RowRange {0, row_group_rows});
    if (!config::enable_parquet_page_index || request.column_predicate_filters.empty() ||
        file_reader == nullptr) {
        return Status::OK();
    }

    std::shared_ptr<::parquet::PageIndexReader> page_index_reader;
    std::shared_ptr<::parquet::RowGroupPageIndexReader> row_group_index_reader;
    try {
        if (pruning_stats != nullptr) {
            ++pruning_stats->page_index_read_calls;
        }
        page_index_reader = file_reader->GetPageIndexReader();
        if (page_index_reader == nullptr) {
            return Status::OK();
        }
        row_group_index_reader = page_index_reader->RowGroup(row_group_idx);
    } catch (const ::parquet::ParquetException&) {
        return Status::OK();
    } catch (const std::exception&) {
        return Status::OK();
    }
    if (row_group_index_reader == nullptr) {
        return Status::OK();
    }

    for (const auto& column_filter : request.column_predicate_filters) {
        std::vector<RowRange> filter_ranges;
        if (!select_ranges_for_filter(row_group_index_reader, file_schema, column_filter,
                                      row_group_rows, &filter_ranges)) {
            continue;
        }
        *selected_ranges = intersect_ranges(*selected_ranges, filter_ranges);
        if (selected_ranges->empty()) {
            if (pruning_stats != nullptr) {
                pruning_stats->filtered_page_rows += row_group_rows;
                ++pruning_stats->filtered_row_groups_by_page_index;
            }
            return Status::OK();
        }
    }
    if (pruning_stats != nullptr) {
        const int64_t selected_rows = count_range_rows(*selected_ranges);
        DORIS_CHECK(selected_rows <= row_group_rows);
        pruning_stats->filtered_page_rows += row_group_rows - selected_rows;
    }
    return Status::OK();
}

} // namespace doris::parquet
