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

#include "format/new_parquet/parquet_page_index.h"

#include <parquet/api/reader.h>
#include <parquet/page_index.h>
#include <parquet/types.h>

#include <algorithm>
#include <exception>
#include <memory>
#include <string>
#include <vector>

#include "common/config.h"
#include "core/field.h"
#include "format/new_parquet/parquet_column_schema.h"
#include "format/new_parquet/parquet_statistics.h"

namespace doris::parquet {
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
        std::vector<RowRange>* selected_ranges) {
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
            return Status::OK();
        }
    }
    return Status::OK();
}

} // namespace doris::parquet
