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
#include <vec/columns/columns_number.h>

#include "schema_desc.h"
#include "vparquet_column_chunk_reader.h"

namespace doris::vectorized {

Status ParquetColumnReader::create(FileReader* file, FieldSchema* field,
                                   const ParquetReadColumn& column,
                                   const tparquet::RowGroup& row_group,
                                   std::unique_ptr<ParquetColumnReader>& reader) {
    if (field->type.type == TYPE_MAP || field->type.type == TYPE_STRUCT) {
        return Status::Corruption("not supported type");
    }
    if (field->type.type == TYPE_ARRAY) {
        return Status::Corruption("not supported array type yet");
    } else {
        LOG(WARNING) << "field->physical_column_index: " << field->physical_column_index;
        tparquet::ColumnChunk chunk = row_group.columns[field->physical_column_index];
        ScalarColumnReader* scalar_reader = new ScalarColumnReader(column);
        scalar_reader->init_column_metadata(chunk);
        RETURN_IF_ERROR(scalar_reader->init(file, field, &chunk));
        reader.reset(scalar_reader);
    }
    return Status::OK();
}

void ParquetColumnReader::init_column_metadata(const tparquet::ColumnChunk& chunk) {
    auto chunk_meta = chunk.meta_data;
    int64_t chunk_start = chunk_meta.__isset.dictionary_page_offset
                                  ? chunk_meta.dictionary_page_offset
                                  : chunk_meta.data_page_offset;
    size_t chunk_len = chunk_meta.total_compressed_size;
    _metadata.reset(new ParquetColumnMetadata(chunk_start, chunk_len, chunk_meta));
}

Status ScalarColumnReader::init(FileReader* file, FieldSchema* field,
                                tparquet::ColumnChunk* chunk) {
    BufferedFileStreamReader stream_reader(file, _metadata->start_offset(), _metadata->size());
    _chunk_reader.reset(new ColumnChunkReader(&stream_reader, chunk, field));
    _chunk_reader->init();
    return Status::OK();
}

Status ScalarColumnReader::read_column_data(ColumnPtr& doris_column, const DataTypePtr& type,
                                            size_t batch_size, bool* eof) {
    while (_chunk_reader->has_next_page()) {
        // seek to next page header
        _chunk_reader->next_page();
        // load data to decoder
        _chunk_reader->load_page_data();
        while (_chunk_reader->num_values() > 0) {
            size_t read_values = _chunk_reader->num_values() < batch_size
                                         ? _chunk_reader->num_values()
                                         : batch_size;
            WhichDataType which_type(type);
            switch (_metadata->t_metadata().type) {
            case tparquet::Type::INT32: {
                _chunk_reader->decode_values(doris_column, read_values);
                return Status::OK();
            }
            case tparquet::Type::INT64: {
                // todo: test int64
                return Status::OK();
            }
            default:
                return Status::Corruption("unsupported parquet data type");
            }
        }
    }
    return Status::OK();
}

void ScalarColumnReader::close() {}
}; // namespace doris::vectorized