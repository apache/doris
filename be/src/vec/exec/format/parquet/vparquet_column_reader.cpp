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

#include "vparquet_column_reader.h"

#include <common/status.h>
#include <gen_cpp/parquet_types.h>

#include "schema_desc.h"
#include "vparquet_column_chunk_reader.h"

namespace doris::vectorized {

Status ScalarColumnReader::init(FileReader* file, FieldSchema* field, tparquet::ColumnChunk* chunk,
                                int64_t start_offset, int64_t chunk_size) {
    BufferedFileStreamReader stream_reader(file, start_offset, chunk_size);
    _chunk_reader.reset(new ColumnChunkReader(&stream_reader, chunk, field));
    _chunk_reader->init();
    _read_chunk_size = chunk_size;
    return Status();
}

Status ParquetColumnReader::create(FileReader* file, int64_t start_offset, int64_t chunk_size,
                                   FieldSchema* field, const ParquetReadColumn& column,
                                   const tparquet::RowGroup& row_group,
                                   const ParquetColumnReader* reader) {
    if (field->type.type == TYPE_MAP || field->type.type == TYPE_STRUCT) {
        return Status::Corruption("not supported type");
    }
    if (field->type.type == TYPE_ARRAY) {
        return Status::Corruption("not supported array type yet");
    } else {
        tparquet::ColumnChunk chunk = row_group.columns[field->physical_column_index];
        ScalarColumnReader* scalar_reader = new ScalarColumnReader(column);
        RETURN_IF_ERROR(scalar_reader->init(file, field, &chunk, start_offset, chunk_size));
        reader = scalar_reader;
    }
    return Status::OK();
}

Status ScalarColumnReader::read_column_data(ColumnPtr* column) {
    while (_chunk_reader->has_next_page()) {
        // seek to next page header
        _chunk_reader->next_page();
        // load data to decoder
        _chunk_reader->load_page_data();
        while (_chunk_reader->num_values() > 0) {
            size_t num_values = _chunk_reader->num_values() < _read_chunk_size
                                        ? _chunk_reader->num_values()
                                        : _read_chunk_size;
            _chunk_reader->decode_values(*column, num_values);
        }
    }
    return Status::OK();
}

void ScalarColumnReader::close() {}
}; // namespace doris::vectorized