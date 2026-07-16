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

#include "format_v2/parquet/reader/count_column_reader.h"

#include <algorithm>
#include <ranges>
#include <utility>

#include "common/config.h"
#include "format/parquet/schema_desc.h"
#include "format/parquet/vparquet_file_metadata.h"
#include "format_v2/parquet/parquet_column_schema.h"
#include "format_v2/parquet/reader/native/level_reader.h"
#include "runtime/runtime_profile.h"

namespace doris::format::parquet {
namespace {

Status find_count_leaf(const ParquetColumnSchema& schema,
                       const format::LocalColumnIndex* projection,
                       const ParquetColumnSchema** leaf) {
    switch (schema.kind) {
    case ParquetColumnSchemaKind::PRIMITIVE:
        if (format::is_partial_projection(projection)) {
            return Status::InvalidArgument("Parquet COUNT projection is invalid for column {}",
                                           schema.name);
        }
        *leaf = &schema;
        return Status::OK();
    case ParquetColumnSchemaKind::STRUCT: {
        DORIS_CHECK(!schema.children.empty());
        if (!format::is_partial_projection(projection)) {
            return find_count_leaf(*schema.children.front(), nullptr, leaf);
        }
        DORIS_CHECK(!projection->children.empty());
        const auto child_id = projection->children.front().local_id();
        const auto child = std::ranges::find_if(schema.children, [child_id](const auto& candidate) {
            return candidate->local_id == child_id;
        });
        if (child == schema.children.end()) {
            return Status::InvalidArgument(
                    "Parquet COUNT projection for column {} contains invalid child", schema.name);
        }
        return find_count_leaf(**child, &projection->children.front(), leaf);
    }
    case ParquetColumnSchemaKind::LIST: {
        DORIS_CHECK(schema.children.size() == 1);
        const auto& element = *schema.children.front();
        return find_count_leaf(element, format::find_child_projection(projection, element.local_id),
                               leaf);
    }
    case ParquetColumnSchemaKind::MAP:
        // The key leaf owns entry existence and top-level MAP shape. Choosing it also avoids
        // reading a potentially huge value BYTE_ARRAY for COUNT(map_col).
        DORIS_CHECK(!schema.children.empty());
        return find_count_leaf(*schema.children.front(), nullptr, leaf);
    }
    return Status::InternalError("Unknown Parquet schema kind for column {}", schema.name);
}

FieldSchema* find_physical_leaf(FieldSchema* field, int physical_column_index) {
    DORIS_CHECK(field != nullptr);
    if (field->children.empty()) {
        return field->physical_column_index == physical_column_index ? field : nullptr;
    }
    for (auto& child : field->children) {
        if (auto* result = find_physical_leaf(&child, physical_column_index); result != nullptr) {
            return result;
        }
    }
    return nullptr;
}

} // namespace

CountColumnReader::CountColumnReader(std::string name,
                                     std::unique_ptr<native::LevelReader> level_reader,
                                     ParquetColumnReaderProfile profile)
        : _level_reader(std::move(level_reader)), _profile(profile), _name(std::move(name)) {}

CountColumnReader::~CountColumnReader() {
    sync_profile();
}

Status CountColumnReader::create(io::FileReaderSPtr file, const FileMetaData* metadata,
                                 int row_group_id, const ParquetColumnSchema& root_schema,
                                 const format::LocalColumnIndex* projection, io::IOContext* io_ctx,
                                 bool enable_page_cache, ParquetColumnReaderProfile profile,
                                 std::unique_ptr<CountColumnReader>* reader) {
    DORIS_CHECK(file != nullptr);
    DORIS_CHECK(metadata != nullptr);
    DORIS_CHECK(reader != nullptr);
    const ParquetColumnSchema* leaf_schema = nullptr;
    RETURN_IF_ERROR(find_count_leaf(root_schema, projection, &leaf_schema));
    DORIS_CHECK(leaf_schema != nullptr);
    DORIS_CHECK(leaf_schema->leaf_column_id >= 0);

    const auto& thrift_metadata = metadata->to_thrift();
    if (row_group_id < 0 || row_group_id >= static_cast<int>(thrift_metadata.row_groups.size())) {
        return Status::InvalidArgument("Invalid Parquet COUNT row group {}", row_group_id);
    }
    const auto& row_group = thrift_metadata.row_groups[row_group_id];
    if (leaf_schema->leaf_column_id >= static_cast<int>(row_group.columns.size())) {
        return Status::Corruption("Invalid Parquet COUNT leaf {} for row group {}",
                                  leaf_schema->leaf_column_id, row_group_id);
    }

    DORIS_CHECK(root_schema.local_id >= 0);
    auto* root_field =
            const_cast<FieldSchema*>(metadata->schema().get_column(root_schema.local_id));
    DORIS_CHECK(root_field != nullptr);
    auto* leaf_field = find_physical_leaf(root_field, leaf_schema->leaf_column_id);
    if (leaf_field == nullptr) {
        return Status::Corruption("Cannot resolve Parquet COUNT physical leaf {} for column {}",
                                  leaf_schema->leaf_column_id, root_schema.name);
    }

    const size_t max_group_buffer = config::parquet_rowgroup_max_buffer_mb << 20;
    const size_t max_column_buffer = config::parquet_column_max_buffer_mb << 20;
    std::unique_ptr<native::LevelReader> level_reader;
    RETURN_IF_ERROR(native::LevelReader::create(
            std::move(file), row_group.columns[leaf_schema->leaf_column_id], leaf_field,
            row_group.num_rows, std::min(max_group_buffer, max_column_buffer), io_ctx,
            enable_page_cache, &level_reader));
    reader->reset(new CountColumnReader(leaf_schema->name, std::move(level_reader), profile));
    return Status::OK();
}

Status CountColumnReader::skip(int64_t rows) {
    DORIS_CHECK(rows >= 0);
    if (rows == 0) {
        return Status::OK();
    }
    {
        SCOPED_TIMER(_profile.level_only_skip_time);
        RETURN_IF_ERROR(_level_reader->skip_rows(static_cast<size_t>(rows)));
    }
    if (_profile.reader_skip_rows != nullptr) {
        COUNTER_UPDATE(_profile.reader_skip_rows, rows);
    }
    sync_profile();
    return Status::OK();
}

Status CountColumnReader::read_levels(int64_t rows, int64_t* rows_read) {
    DORIS_CHECK(rows >= 0);
    DORIS_CHECK(rows_read != nullptr);
    _definition_levels.clear();
    _repetition_levels.clear();
    _levels_written = 0;
    size_t native_rows_read = 0;
    {
        SCOPED_TIMER(_profile.level_only_read_time);
        RETURN_IF_ERROR(_level_reader->read_rows(static_cast<size_t>(rows), &_repetition_levels,
                                                 &_definition_levels, &native_rows_read));
    }
    *rows_read = static_cast<int64_t>(native_rows_read);
    if (*rows_read != rows || _definition_levels.size() != _repetition_levels.size()) {
        return Status::Corruption(
                "Parquet COUNT level reader returned {} rows and {}/{} levels for {}", *rows_read,
                _definition_levels.size(), _repetition_levels.size(), _name);
    }
    _levels_written = static_cast<int64_t>(_definition_levels.size());
    if (_levels_written < *rows_read) {
        return Status::Corruption("Parquet COUNT returned {} levels for {} rows in column {}",
                                  _levels_written, *rows_read, _name);
    }
    if (_profile.reader_read_rows != nullptr) {
        COUNTER_UPDATE(_profile.reader_read_rows, *rows_read);
    }
    sync_profile();
    return Status::OK();
}

void CountColumnReader::sync_profile() {
    if (_level_reader == nullptr) {
        return;
    }
    const auto stats = _level_reader->statistics();
    const auto& reported = _reported_native_stats;
#define UPDATE_NATIVE_PROFILE(counter, field)                           \
    if (_profile.counter != nullptr) {                                  \
        COUNTER_UPDATE(_profile.counter, stats.field - reported.field); \
    }
    UPDATE_NATIVE_PROFILE(decompress_time, decompress_time);
    UPDATE_NATIVE_PROFILE(decompress_count, decompress_cnt);
    UPDATE_NATIVE_PROFILE(decode_header_time, decode_header_time);
    UPDATE_NATIVE_PROFILE(decode_value_time, decode_value_time);
    UPDATE_NATIVE_PROFILE(decode_dictionary_time, decode_dict_time);
    UPDATE_NATIVE_PROFILE(decode_level_time, decode_level_time);
    UPDATE_NATIVE_PROFILE(skip_page_header_count, skip_page_header_num);
    UPDATE_NATIVE_PROFILE(parse_page_header_count, parse_page_header_num);
    UPDATE_NATIVE_PROFILE(read_page_header_time, read_page_header_time);
    UPDATE_NATIVE_PROFILE(page_read_count, page_read_counter);
    UPDATE_NATIVE_PROFILE(page_cache_write_count, page_cache_write_counter);
    UPDATE_NATIVE_PROFILE(page_cache_compressed_write_count, page_cache_compressed_write_counter);
    UPDATE_NATIVE_PROFILE(page_cache_decompressed_write_count,
                          page_cache_decompressed_write_counter);
    UPDATE_NATIVE_PROFILE(page_cache_hit_count, page_cache_hit_counter);
    UPDATE_NATIVE_PROFILE(page_cache_miss_count, page_cache_missing_counter);
    UPDATE_NATIVE_PROFILE(page_cache_compressed_hit_count, page_cache_compressed_hit_counter);
    UPDATE_NATIVE_PROFILE(page_cache_decompressed_hit_count, page_cache_decompressed_hit_counter);
#undef UPDATE_NATIVE_PROFILE
    _reported_native_stats = stats;
}

} // namespace doris::format::parquet
