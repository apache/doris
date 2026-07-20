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

#include "format_v2/parquet/reader/column_reader.h"

#include <utility>

#include "format_v2/parquet/parquet_column_schema.h"
#include "runtime/runtime_profile.h"

namespace doris::format::parquet {

ParquetColumnReader::ParquetColumnReader(const ParquetColumnSchema& schema, DataTypePtr type,
                                         ParquetColumnReaderProfile profile)
        : _profile(profile),
          _field_id(schema.local_id),
          _leaf_column_id(schema.leaf_column_id),
          _type(std::move(type)),
          _name(schema.name) {}

Status ParquetColumnReader::skip(int64_t rows) {
    return Status::NotSupported("Parquet column skip is not implemented, rows={}", rows);
}

void ParquetColumnReader::update_reader_read_rows(int64_t rows) const {
    if (_profile.reader_read_rows != nullptr) {
        COUNTER_UPDATE(_profile.reader_read_rows, rows);
    }
}

void ParquetColumnReader::update_reader_skip_rows(int64_t rows) const {
    if (_profile.reader_skip_rows != nullptr) {
        COUNTER_UPDATE(_profile.reader_skip_rows, rows);
    }
}

Status ParquetColumnReader::select(const SelectionVector& selection, uint16_t selected_rows,
                                   int64_t batch_rows, MutableColumnPtr& column) {
    DORIS_CHECK(column);
    RETURN_IF_ERROR(selection.verify(selected_rows, batch_rows));

    const auto ranges = selection_to_ranges(selection, selected_rows);
    int64_t cursor = 0;
    for (const auto& range : ranges) {
        DORIS_CHECK(range.start >= cursor);
        DORIS_CHECK(range.start + range.length <= batch_rows);
        RETURN_IF_ERROR(skip(range.start - cursor));

        int64_t range_rows_read = 0;
        RETURN_IF_ERROR(read(range.length, column, &range_rows_read));
        if (range_rows_read != range.length) {
            return Status::Corruption(
                    "Parquet selected read returned {} rows, expected {} rows for column {}",
                    range_rows_read, range.length, name());
        }
        cursor = range.start + range.length;
    }
    RETURN_IF_ERROR(skip(batch_rows - cursor));
    if (_profile.reader_select_rows != nullptr) {
        COUNTER_UPDATE(_profile.reader_select_rows, selected_rows);
    }
    return Status::OK();
}

Status ParquetColumnReader::select_with_dictionary_filter(const SelectionVector&, uint16_t, int64_t,
                                                          const IColumn::Filter&, MutableColumnPtr&,
                                                          IColumn::Filter*, bool*) {
    return Status::NotSupported("Parquet dictionary filter is not implemented for column {}",
                                name());
}

Status ParquetColumnReader::select_with_plain_filter(const SelectionVector&, uint16_t, int64_t,
                                                     const std::vector<PlainFixedPredicate>&,
                                                     IColumn::Filter* row_filter,
                                                     bool* used_filter) {
    DORIS_CHECK(row_filter != nullptr);
    DORIS_CHECK(used_filter != nullptr);
    row_filter->clear();
    *used_filter = false;
    return Status::OK();
}

} // namespace doris::format::parquet
