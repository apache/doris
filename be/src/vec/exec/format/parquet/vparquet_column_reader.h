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
#include <common/status.h>
#include <gen_cpp/parquet_types.h>

#include "schema_desc.h"
#include "vparquet_column_chunk_reader.h"
#include "vparquet_reader.h"

namespace doris::vectorized {

struct RowRange;
class ParquetReadColumn;

class ParquetColumnMetadata {
public:
    ParquetColumnMetadata(int64_t chunk_start_offset, int64_t chunk_length,
                          tparquet::ColumnMetaData metadata)
            : _chunk_start_offset(chunk_start_offset),
              _chunk_length(chunk_length),
              _metadata(metadata) {};

    ~ParquetColumnMetadata() = default;
    int64_t start_offset() const { return _chunk_start_offset; };
    int64_t size() const { return _chunk_length; };
    tparquet::ColumnMetaData t_metadata() { return _metadata; };

private:
    int64_t _chunk_start_offset;
    int64_t _chunk_length;
    tparquet::ColumnMetaData _metadata;
};

class ParquetColumnReader {
public:
    ParquetColumnReader(const ParquetReadColumn& column, cctz::time_zone* ctz)
            : _column(column), _ctz(ctz) {};
    virtual ~ParquetColumnReader() {
        if (_stream_reader != nullptr) {
            delete _stream_reader;
            _stream_reader = nullptr;
        }
    };
    virtual Status read_column_data(ColumnPtr& doris_column, DataTypePtr& type, size_t batch_size,
                                    size_t* read_rows, bool* eof) = 0;
    static Status create(FileReader* file, FieldSchema* field, const ParquetReadColumn& column,
                         const tparquet::RowGroup& row_group, std::vector<RowRange>& row_ranges,
                         cctz::time_zone* ctz, std::unique_ptr<ParquetColumnReader>& reader);
    void init_column_metadata(const tparquet::ColumnChunk& chunk);
    virtual void close() = 0;

protected:
    void _skipped_pages();

protected:
    const ParquetReadColumn& _column;
    BufferedFileStreamReader* _stream_reader;
    std::unique_ptr<ParquetColumnMetadata> _metadata;
    std::vector<RowRange>* _row_ranges;
    cctz::time_zone* _ctz;
};

class ScalarColumnReader : public ParquetColumnReader {
public:
    ScalarColumnReader(const ParquetReadColumn& column, cctz::time_zone* ctz)
            : ParquetColumnReader(column, ctz) {};
    ~ScalarColumnReader() override { close(); };
    Status init(FileReader* file, FieldSchema* field, tparquet::ColumnChunk* chunk,
                std::vector<RowRange>& row_ranges);
    Status read_column_data(ColumnPtr& doris_column, DataTypePtr& type, size_t batch_size,
                            size_t* read_rows, bool* eof) override;
    void close() override;

private:
    std::unique_ptr<ColumnChunkReader> _chunk_reader;
};

//class ArrayColumnReader : public ParquetColumnReader {
//public:
//    ArrayColumnReader(const ParquetReadColumn& column) : ParquetColumnReader(column) {};
//    ~ArrayColumnReader() override = default;
//    Status init(FileReader* file, FieldSchema* field,
//                tparquet::ColumnChunk* chunk, const TypeDescriptor& col_type,
//                int64_t chunk_size);
//    Status read_column_data(ColumnPtr* data) override;
//    void close() override;
//private:
//    std::unique_ptr<ColumnChunkReader> _chunk_reader;
//};
}; // namespace doris::vectorized