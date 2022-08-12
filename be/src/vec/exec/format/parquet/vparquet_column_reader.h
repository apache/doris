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
#include "vparquet_reader.h"
//#include "vparquet_column_chunk_reader.h"

namespace doris::vectorized {

class ParquetReadColumn;

class ParquetColumnReader {
public:
    ParquetColumnReader(const ParquetReadColumn& column) : _column(column) {};
    virtual ~ParquetColumnReader() = 0;
    virtual Status read_column_data(const tparquet::RowGroup& row_group_meta, ColumnPtr* data) = 0;
    static Status create(const FileReader* file, int64_t chunk_size, const FieldSchema* field,
                         const ParquetReadColumn& column, const TypeDescriptor& col_type,
                         const tparquet::RowGroup& row_group, const ParquetColumnReader* reader);
    virtual void close() = 0;

protected:
    const ParquetReadColumn& _column;
    //    const ColumnChunkReader& _chunk_reader;
};

class ScalarColumnReader : public ParquetColumnReader {
public:
    ScalarColumnReader(const ParquetReadColumn& column) : ParquetColumnReader(column) {};
    ~ScalarColumnReader() override = default;
    Status init(const FileReader* file, const FieldSchema* field,
                const tparquet::ColumnChunk* chunk, const TypeDescriptor& col_type,
                int64_t chunk_size);
    Status read_column_data(const tparquet::RowGroup& row_group_meta, ColumnPtr* data) override;
    void close() override;
};
}; // namespace doris::vectorized