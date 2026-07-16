// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "format_v2/parquet/reader/count_column_reader.h"

#include <arrow/memory_pool.h>
#include <parquet/api/reader.h>
#include <parquet/api/schema.h>
#include <parquet/column_reader.h>
#include <parquet/exception.h>
#include <parquet/level_conversion.h>

#include <algorithm>
#include <exception>
#include <ranges>
#include <utility>

#include "format_v2/parquet/parquet_column_schema.h"
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
        const auto child_id = projection->children.front().local_id();
        const auto child = std::ranges::find_if(
                schema.children,
                [child_id](const auto& candidate) { return candidate->local_id == child_id; });
        if (child == schema.children.end()) {
            return Status::InvalidArgument(
                    "Parquet COUNT projection for column {} contains invalid child", schema.name);
        }
        return find_count_leaf(**child, &projection->children.front(), leaf);
    }
    case ParquetColumnSchemaKind::LIST: {
        DORIS_CHECK(schema.children.size() == 1);
        const auto& element = *schema.children.front();
        return find_count_leaf(element,
                               format::find_child_projection(projection, element.local_id), leaf);
    }
    case ParquetColumnSchemaKind::MAP:
        // The key stream defines entry existence and top-level MAP shape. Never select the value
        // leaf: a COUNT(map_col) must not retain huge value strings merely to inspect nullability.
        DORIS_CHECK(!schema.children.empty());
        return find_count_leaf(*schema.children.front(), nullptr, leaf);
    }
    return Status::InternalError("Unknown Parquet schema kind for column {}", schema.name);
}

bool is_binary_physical_type(const ::parquet::ColumnDescriptor& descriptor) {
    return descriptor.physical_type() == ::parquet::Type::BYTE_ARRAY ||
           descriptor.physical_type() == ::parquet::Type::FIXED_LEN_BYTE_ARRAY;
}

} // namespace

CountColumnReader::CountColumnReader(
        const ParquetColumnSchema& leaf_schema,
        std::shared_ptr<::parquet::internal::RecordReader> record_reader,
        ParquetColumnReaderProfile profile)
        : _leaf_schema(leaf_schema),
          _record_reader(std::move(record_reader)),
          _profile(profile),
          _name(leaf_schema.name) {}

Status CountColumnReader::create(std::shared_ptr<::parquet::RowGroupReader> row_group,
                                 const ParquetColumnSchema& root_schema,
                                 const format::LocalColumnIndex* projection,
                                 ParquetColumnReaderProfile profile,
                                 std::unique_ptr<CountColumnReader>* reader) {
    DORIS_CHECK(row_group != nullptr);
    DORIS_CHECK(reader != nullptr);
    const ParquetColumnSchema* leaf = nullptr;
    RETURN_IF_ERROR(find_count_leaf(root_schema, projection, &leaf));
    DORIS_CHECK(leaf != nullptr);
    DORIS_CHECK(leaf->leaf_column_id >= 0);
    DORIS_CHECK(leaf->descriptor != nullptr);

    try {
        auto page_reader = row_group->GetColumnPageReader(leaf->leaf_column_id);
        DORIS_CHECK(page_reader != nullptr);
        const auto level_info =
                ::parquet::internal::LevelInfo::ComputeLevelInfo(leaf->descriptor);
        auto record_reader = ::parquet::internal::RecordReader::Make(
                leaf->descriptor, level_info, ::arrow::default_memory_pool(),
                /*read_dictionary=*/false,
                /*read_dense_for_nullable=*/false);
        DORIS_CHECK(record_reader != nullptr);
        record_reader->SetPageReader(std::move(page_reader));
        reader->reset(new CountColumnReader(*leaf, std::move(record_reader), profile));
        return Status::OK();
    } catch (const ::parquet::ParquetException& e) {
        return Status::Corruption("Failed to create Parquet COUNT reader for column {}: {}",
                                  leaf->name, e.what());
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to create Parquet COUNT reader for column {}: {}",
                                     leaf->name, e.what());
    }
}

Status CountColumnReader::skip(int64_t rows) {
    DORIS_CHECK(rows >= 0);
    if (rows == 0) {
        return Status::OK();
    }
    int64_t skipped_rows = 0;
    try {
        _record_reader->Reset();
        SCOPED_TIMER(_profile.arrow_skip_records_time);
        while (skipped_rows < rows) {
            const int64_t skipped = _record_reader->SkipRecords(rows - skipped_rows);
            if (skipped <= 0) {
                return Status::Corruption(
                        "Parquet COUNT reader skipped {} of {} rows for column {}", skipped_rows,
                        rows, _name);
            }
            skipped_rows += skipped;
        }
    } catch (const ::parquet::ParquetException& e) {
        return Status::Corruption("Failed to skip Parquet COUNT rows for column {}: {}", _name,
                                  e.what());
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to skip Parquet COUNT rows for column {}: {}", _name,
                                     e.what());
    }
    if (_profile.reader_skip_rows != nullptr) {
        COUNTER_UPDATE(_profile.reader_skip_rows, rows);
    }
    return Status::OK();
}

Status CountColumnReader::release_binary_builder() {
    DORIS_CHECK(_leaf_schema.descriptor != nullptr);
    if (!is_binary_physical_type(*_leaf_schema.descriptor)) {
        return Status::OK();
    }
    auto* binary_reader =
            dynamic_cast<::parquet::internal::BinaryRecordReader*>(_record_reader.get());
    if (binary_reader == nullptr) {
        return Status::InternalError(
                "Parquet COUNT binary reader is unavailable for column {}", _name);
    }
    // GetBuilderChunks transfers builder ownership. Keep the result local so binary payload pages
    // are released before the next batch and never overlap adaptive-batch allocations.
    auto discarded_chunks = binary_reader->GetBuilderChunks();
    discarded_chunks.clear();
    return Status::OK();
}

Status CountColumnReader::read_levels(int64_t rows, int64_t* rows_read) {
    DORIS_CHECK(rows >= 0);
    DORIS_CHECK(rows_read != nullptr);
    _definition_levels.clear();
    _repetition_levels.clear();
    _levels_written = 0;
    if (rows == 0) {
        *rows_read = 0;
        return Status::OK();
    }

    try {
        _record_reader->Reset();
        _record_reader->Reserve(rows);
        {
            SCOPED_TIMER(_profile.arrow_read_records_time);
            *rows_read = _record_reader->ReadRecords(rows);
        }
    } catch (const ::parquet::ParquetException& e) {
        return Status::Corruption("Failed to read Parquet COUNT levels for column {}: {}", _name,
                                  e.what());
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to read Parquet COUNT levels for column {}: {}", _name,
                                     e.what());
    }
    if (*rows_read < 0 || *rows_read > rows) {
        return Status::Corruption("Invalid Parquet COUNT row count {} for column {}", *rows_read,
                                  _name);
    }

    _levels_written = _record_reader->levels_position();
    if (_levels_written > _record_reader->levels_written()) {
        return Status::Corruption(
                "Invalid Parquet COUNT level position {} of {} for column {}", _levels_written,
                _record_reader->levels_written(), _name);
    }
    const auto* descriptor = _leaf_schema.descriptor;
    if (_levels_written == 0 && *rows_read > 0 && descriptor->max_definition_level() == 0 &&
        descriptor->max_repetition_level() == 0) {
        _levels_written = *rows_read;
    }
    if (_levels_written < *rows_read) {
        return Status::Corruption(
                "Parquet COUNT returned {} levels for {} rows in column {}", _levels_written,
                *rows_read, _name);
    }

    _definition_levels.resize(static_cast<size_t>(_levels_written));
    if (descriptor->max_definition_level() == 0) {
        std::fill(_definition_levels.begin(), _definition_levels.end(), 0);
    } else {
        const auto* levels = _record_reader->def_levels();
        DORIS_CHECK(levels != nullptr || _levels_written == 0);
        std::copy_n(levels, _levels_written, _definition_levels.begin());
    }
    _repetition_levels.resize(static_cast<size_t>(_levels_written));
    if (descriptor->max_repetition_level() == 0) {
        std::fill(_repetition_levels.begin(), _repetition_levels.end(), 0);
    } else {
        const auto* levels = _record_reader->rep_levels();
        DORIS_CHECK(levels != nullptr || _levels_written == 0);
        std::copy_n(levels, _levels_written, _repetition_levels.begin());
    }
    RETURN_IF_ERROR(release_binary_builder());
    if (_profile.reader_read_rows != nullptr) {
        COUNTER_UPDATE(_profile.reader_read_rows, *rows_read);
    }
    return Status::OK();
}

} // namespace doris::format::parquet
