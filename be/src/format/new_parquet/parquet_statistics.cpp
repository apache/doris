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
#include <parquet/statistics.h>
#include <parquet/types.h>

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
        const ::parquet::RowGroupMetaData& row_group,
        const std::vector<std::unique_ptr<ParquetColumnSchema>>& schema,
        const reader::FileColumnPredicateFilter& column_filter) {
    if (column_filter.predicates.empty()) {
        return false;
    }
    DCHECK(column_filter.file_column_id >= 0 &&
           column_filter.file_column_id < row_group.num_columns());
    DCHECK_LT(column_filter.file_column_id, schema.size());
    auto column_chunk = row_group.ColumnChunk(column_filter.file_column_id);
    if (column_chunk == nullptr) {
        return false;
    }
    return CheckStatistics(column_filter,
                           TransformColumnStatistics(*schema[column_filter.file_column_id],
                                                     column_chunk->statistics()));
}

Status ParquetStatisticsUtils::SelectRowGroups(
        const ::parquet::FileMetaData& metadata,
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
            if (RowGroupExcludes(*row_group, file_schema, column_filter)) {
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
        const ::parquet::FileMetaData& metadata,
        const std::vector<std::unique_ptr<ParquetColumnSchema>>& file_schema,
        const reader::FileScanRequest& request, std::vector<int>* selected_row_groups) {
    return ParquetStatisticsUtils::SelectRowGroups(metadata, file_schema, request,
                                                   selected_row_groups);
}

} // namespace doris::parquet
