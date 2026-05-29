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
#include <parquet/column_page.h>
#include <parquet/encoding.h>
#include <parquet/statistics.h>
#include <parquet/types.h>

#include <cstddef>
#include <exception>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "core/data_type/data_type.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/primitive_type.h"
#include "core/field.h"
#include "format/new_parquet/parquet_column_schema.h"
#include "storage/index/zone_map/zone_map_index.h"
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

bool ParquetStatisticsUtils::RowGroupExcludes(
        const ::parquet::RowGroupMetaData& row_group, ::parquet::ParquetFileReader* file_reader,
        int row_group_idx, const std::vector<std::unique_ptr<ParquetColumnSchema>>& schema,
        const reader::FileColumnPredicateFilter& column_filter) {
    if (column_filter.predicates.empty()) {
        return false;
    }
    DCHECK_LT(column_filter.file_column_id, schema.size());
    const auto& column_schema = *schema[column_filter.file_column_id];
    if (column_schema.kind != ParquetColumnSchemaKind::PRIMITIVE ||
        column_schema.leaf_column_id < 0) {
        return false;
    }
    DCHECK_LT(column_schema.leaf_column_id, row_group.num_columns());
    auto column_chunk = row_group.ColumnChunk(column_schema.leaf_column_id);
    if (column_chunk == nullptr) {
        return false;
    }
    if (CheckStatistics(column_filter,
                        TransformColumnStatistics(column_schema, column_chunk->statistics()))) {
        return true;
    }
    if (!supports_dictionary_pruning(column_schema, *column_chunk, column_filter) ||
        !is_dictionary_encoded_chunk(*column_chunk)) {
        return false;
    }
    OwnedDictionaryWords dict_words;
    if (!read_dictionary_words(file_reader, row_group_idx, column_schema.leaf_column_id,
                               column_schema, &dict_words)) {
        return false;
    }
    for (const auto& column_predicate : column_filter.predicates) {
        if (!column_predicate->evaluate_and(dict_words.refs.data(), dict_words.refs.size())) {
            return true;
        }
    }
    return false;
}

Status ParquetStatisticsUtils::SelectRowGroups(
        const ::parquet::FileMetaData& metadata, ::parquet::ParquetFileReader* file_reader,
        const std::vector<std::unique_ptr<ParquetColumnSchema>>& file_schema,
        const reader::FileScanRequest& request, std::vector<int>* selected_row_groups) {
    if (selected_row_groups == nullptr) {
        return Status::InvalidArgument("selected_row_groups is null");
    }
    selected_row_groups->clear();

    const int num_row_groups = metadata.num_row_groups();
    selected_row_groups->reserve(num_row_groups);
    for (int row_group_idx = 0; row_group_idx < num_row_groups; ++row_group_idx) {
        auto row_group = metadata.RowGroup(row_group_idx);
        if (row_group == nullptr) {
            selected_row_groups->push_back(row_group_idx);
            continue;
        }
        bool drop = false;
        for (const auto& column_filter : request.column_predicate_filters) {
            if (RowGroupExcludes(*row_group, file_reader, row_group_idx, file_schema,
                                 column_filter)) {
                drop = true;
                break;
            }
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

Status select_row_groups_by_statistics(
        const ::parquet::FileMetaData& metadata, ::parquet::ParquetFileReader* file_reader,
        const std::vector<std::unique_ptr<ParquetColumnSchema>>& file_schema,
        const reader::FileScanRequest& request, std::vector<int>* selected_row_groups) {
    return ParquetStatisticsUtils::SelectRowGroups(metadata, file_reader, file_schema, request,
                                                   selected_row_groups);
}

} // namespace doris::parquet
