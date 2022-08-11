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

Status ScalarColumnReader::init(const FileReader* file, const FieldSchema* field,
                                const tparquet::ColumnChunk* chunk, const TypeDescriptor& col_type,
                                int64_t chunk_size) {
    // todo1: init column chunk reader
    // BufferedFileStreamReader stream_reader(reader, 0, chunk_size);
    // _chunk_reader(&stream_reader, chunk, field);
    // _chunk_reader.init();
    return Status();
}

Status ParquetColumnReader::create(const FileReader* file, int64_t chunk_size,
                                   const FieldSchema* field, const ParquetReadColumn& column,
                                   const TypeDescriptor& col_type,
                                   const tparquet::RowGroup& row_group,
                                   const ParquetColumnReader* reader) {
    if (field->type.type == TYPE_MAP || field->type.type == TYPE_STRUCT) {
        return Status::Corruption("not supported type");
    }
    if (field->type.type == TYPE_ARRAY) {
        return Status::Corruption("not supported array type yet");
    } else {
        ScalarColumnReader* scalar_reader = new ScalarColumnReader(column);
        RETURN_IF_ERROR(scalar_reader->init(file, field,
                                            &row_group.columns[field->physical_column_index],
                                            col_type, chunk_size));
        reader = scalar_reader;
    }
    return Status::OK();
}

Status ScalarColumnReader::read_column_data(const tparquet::RowGroup& row_group_meta,
                                            ColumnPtr* data) {
    // todo2: read data with chunk reader to load page data
    // while (_chunk_reader.has_next) {
    // _chunk_reader.next_page();
    // _chunk_reader.load_page_data();
    // }
    return Status();
}

void ScalarColumnReader::close() {}
}; // namespace doris::vectorized