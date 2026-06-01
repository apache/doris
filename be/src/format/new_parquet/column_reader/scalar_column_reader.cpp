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

#include "format/new_parquet/column_reader/scalar_column_reader.h"

#include <parquet/api/reader.h>

#include <exception>
#include <utility>

#include "core/column/column.h"

namespace doris::parquet {

ScalarColumnReader::ScalarColumnReader(
        int parquet_leaf_column_id, const ::parquet::ColumnDescriptor* descriptor,
        ParquetTypeDescriptor type_descriptor, DataTypePtr type, std::string name,
        std::shared_ptr<::parquet::internal::RecordReader> record_reader)
        : _file_column_id(parquet_leaf_column_id),
          _parquet_leaf_column_id(parquet_leaf_column_id),
          _descriptor(descriptor),
          _type_descriptor(std::move(type_descriptor)),
          _type(std::move(type)),
          _name(std::move(name)),
          _record_reader(std::move(record_reader)) {}

Status ScalarColumnReader::read(int64_t rows, MutableColumnPtr& column, int64_t* rows_read) {
    if (column.get() == nullptr || rows_read == nullptr) {
        return Status::InvalidArgument("Invalid parquet column read result pointer for column {}",
                                       _name);
    }
    if (_record_reader == nullptr) {
        return Status::InternalError("Parquet record reader is not initialized for column {}",
                                     _name);
    }
    ::parquet::internal::RecordReader* record_reader = nullptr;
    const auto context = leaf_context();
    RETURN_IF_ERROR(read_leaf_records(context, rows, &record_reader, rows_read));
    if (record_reader->values_written() != *rows_read) {
        return Status::Corruption(
                "Invalid parquet record read result for column {}: values={}, records={}", _name,
                record_reader->values_written(), *rows_read);
    }

    NullMap null_map;
    RETURN_IF_ERROR(build_leaf_null_map(context, *record_reader, *rows_read, &null_map));

    RETURN_IF_ERROR(append_leaf_values(context, *record_reader, *rows_read, &null_map, column));
    return Status::OK();
}

Status ScalarColumnReader::skip(int64_t rows) {
    if (rows <= 0) {
        return Status::OK();
    }

    if (_record_reader == nullptr) {
        return Status::InternalError("Parquet record reader is not initialized for column {}",
                                     _name);
    }
    int64_t skipped_rows = 0;
    try {
        _record_reader->Reset();
        while (skipped_rows < rows) {
            const int64_t skipped = _record_reader->SkipRecords(rows - skipped_rows);
            if (skipped <= 0) {
                return Status::Corruption(
                        "Failed to skip parquet records for column {}: skipped {} of {} rows",
                        _name, skipped_rows, rows);
            }
            skipped_rows += skipped;
        }
    } catch (const ::parquet::ParquetException& e) {
        return Status::Corruption("Failed to skip parquet records for column {}: {}", _name,
                                  e.what());
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to skip parquet records for column {}: {}", _name,
                                     e.what());
    }
    return Status::OK();
}

} // namespace doris::parquet
