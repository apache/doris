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

#pragma once

#include <cstdint>
#include <string>

#include "common/status.h"
#include "core/column/column.h"
#include "core/data_type/data_type.h"
#include "format_v2/parquet/parquet_profile.h"
#include "format_v2/parquet/selection_vector.h"

namespace doris::format::parquet {
struct ParquetColumnSchema;

// Scan-time column contract for FileScannerV2.
//
// Physical Parquet columns are implemented by NativeColumnReader, which owns Doris' native page
// decoder and writes directly into the destination Doris column. Synthetic row-position/global-id
// readers implement the same cursor contract. Arrow RecordReader, ParquetLeafBatch, decoded value
// views, and the former nested build/consume protocol intentionally do not belong to this API.
class ParquetColumnReader {
public:
    virtual ~ParquetColumnReader() = default;

    virtual int file_column_id() const { return _field_id; }
    virtual int parquet_leaf_column_id() const { return _leaf_column_id; }
    virtual const DataTypePtr& type() const { return _type; }
    virtual const std::string& name() const { return _name; }
    const ParquetColumnReaderProfile& profile() const { return _profile; }

    // Consume rows from the row-group cursor and append them to column.
    virtual Status read(int64_t rows, MutableColumnPtr& column, int64_t* rows_read) = 0;

    // Consume rows without materializing values.
    virtual Status skip(int64_t rows);

    // Consume batch_rows and append only selection[0, selected_rows). The default implementation
    // coalesces adjacent indices into ranges; native readers override it with one decoder call.
    virtual Status select(const SelectionVector& selection, uint16_t selected_rows,
                          int64_t batch_rows, MutableColumnPtr& column);

    virtual Status select_with_dictionary_filter(const SelectionVector& selection,
                                                 uint16_t selected_rows, int64_t batch_rows,
                                                 const IColumn::Filter& dictionary_filter,
                                                 MutableColumnPtr& column,
                                                 IColumn::Filter* row_filter, bool* used_filter);

    // Native statistics are cumulative and can be recursively aggregated for complex columns.
    // Flush once at the scheduler batch boundary instead of snapshotting after each operation.
    virtual void flush_profile() {}
    virtual bool crossed_page_since_last_batch() { return false; }
    virtual Result<MutableColumnPtr> dictionary_values() {
        return ResultError(Status::NotSupported("Parquet dictionary values are not supported"));
    }

protected:
    ParquetColumnReader(const ParquetColumnSchema& schema, DataTypePtr type,
                        ParquetColumnReaderProfile profile = {});
    ParquetColumnReader() = default;

    void update_reader_read_rows(int64_t rows) const;
    void update_reader_skip_rows(int64_t rows) const;

    ParquetColumnReaderProfile _profile;
    const int _field_id = -1;
    const int _leaf_column_id = -1;
    const DataTypePtr _type;
    const std::string _name;
};

} // namespace doris::format::parquet
