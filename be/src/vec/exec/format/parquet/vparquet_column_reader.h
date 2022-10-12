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

namespace doris::vectorized {

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
    struct Statistics {
        Statistics()
                : read_time(0),
                  read_calls(0),
                  read_bytes(0),
                  decompress_time(0),
                  decompress_cnt(0),
                  decode_header_time(0),
                  decode_value_time(0),
                  decode_dict_time(0),
                  decode_level_time(0),
                  decode_null_map_time(0) {}

        Statistics(BufferedStreamReader::Statistics& fs, ColumnChunkReader::Statistics& cs,
                   int64_t null_map_time)
                : read_time(fs.read_time),
                  read_calls(fs.read_calls),
                  read_bytes(fs.read_bytes),
                  decompress_time(cs.decompress_time),
                  decompress_cnt(cs.decompress_cnt),
                  decode_header_time(cs.decode_header_time),
                  decode_value_time(cs.decode_value_time),
                  decode_dict_time(cs.decode_dict_time),
                  decode_level_time(cs.decode_level_time),
                  decode_null_map_time(null_map_time) {}

        int64_t read_time;
        int64_t read_calls;
        int64_t read_bytes;
        int64_t decompress_time;
        int64_t decompress_cnt;
        int64_t decode_header_time;
        int64_t decode_value_time;
        int64_t decode_dict_time;
        int64_t decode_level_time;
        int64_t decode_null_map_time;

        void merge(Statistics& statistics) {
            read_time += statistics.read_time;
            read_calls += statistics.read_calls;
            read_bytes += statistics.read_bytes;
            decompress_time += statistics.decompress_time;
            decompress_cnt += statistics.decompress_cnt;
            decode_header_time += statistics.decode_header_time;
            decode_value_time += statistics.decode_value_time;
            decode_dict_time += statistics.decode_dict_time;
            decode_level_time += statistics.decode_level_time;
            decode_null_map_time += statistics.decode_null_map_time;
        }
    };

    ParquetColumnReader(cctz::time_zone* ctz) : _ctz(ctz) {};
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
                         cctz::time_zone* ctz, std::unique_ptr<ParquetColumnReader>& reader,
                         size_t max_buf_size);
    void init_column_metadata(const tparquet::ColumnChunk& chunk);
    void add_offset_index(tparquet::OffsetIndex* offset_index) { _offset_index = offset_index; }
    Statistics statistics() {
        return Statistics(_stream_reader->statistics(), _chunk_reader->statistics(),
                          _decode_null_map_time);
    }
    virtual void close() = 0;

protected:
    void _generate_read_ranges(int64_t start_index, int64_t end_index,
                               std::list<RowRange>& read_ranges);

    BufferedFileStreamReader* _stream_reader;
    std::unique_ptr<ParquetColumnMetadata> _metadata;
    std::vector<RowRange> _row_ranges;
    cctz::time_zone* _ctz;
    std::unique_ptr<ColumnChunkReader> _chunk_reader;
    tparquet::OffsetIndex* _offset_index;
    int64_t _current_row_index = 0;
    int _row_range_index = 0;
    int64_t _decode_null_map_time = 0;
};

class ScalarColumnReader : public ParquetColumnReader {
public:
    ScalarColumnReader(cctz::time_zone* ctz) : ParquetColumnReader(ctz) {};
    ~ScalarColumnReader() override { close(); };
    Status init(FileReader* file, FieldSchema* field, tparquet::ColumnChunk* chunk,
                std::vector<RowRange>& row_ranges, size_t max_buf_size);
    Status read_column_data(ColumnPtr& doris_column, DataTypePtr& type, size_t batch_size,
                            size_t* read_rows, bool* eof) override;
    Status _skip_values(size_t num_values);
    Status _read_values(size_t num_values, ColumnPtr& doris_column, DataTypePtr& type);
    void close() override;
};

class ArrayColumnReader : public ParquetColumnReader {
public:
    ArrayColumnReader(cctz::time_zone* ctz) : ParquetColumnReader(ctz) {};
    ~ArrayColumnReader() override { close(); };
    Status init(FileReader* file, FieldSchema* field, tparquet::ColumnChunk* chunk,
                std::vector<RowRange>& row_ranges, size_t max_buf_size);
    Status read_column_data(ColumnPtr& doris_column, DataTypePtr& type, size_t batch_size,
                            size_t* read_rows, bool* eof) override;
    void close() override;

private:
    void _reserve_def_levels_buf(size_t size);
    void _init_rep_levels_buf();
    void _load_rep_levels();
    Status _load_nested_column(ColumnPtr& doris_column, DataTypePtr& type, size_t read_values);
    Status _generate_array_offset(std::vector<size_t>& element_offsets, size_t pre_batch_size,
                                  size_t* real_batch_size, size_t* num_values);
    void _fill_array_offset(MutableColumnPtr& doris_column, std::vector<size_t>& element_offsets,
                            int offset_index, size_t num_rows);
    Status _skip_values(size_t num_values);

    std::unique_ptr<level_t[]> _def_levels_buf = nullptr;
    size_t _def_levels_buf_size = 0;
    size_t _def_offset = 0;

    std::unique_ptr<level_t[]> _rep_levels_buf = nullptr;
    size_t _rep_levels_buf_size = 0;
    size_t _rep_size = 0;
    size_t _rep_offset = 0;
    size_t _start_offset = 0;
    size_t _remaining_rep_levels = 0;

    level_t _CONCRETE_ELEMENT = -1;
    level_t _NULL_ELEMENT = -1;
    level_t _EMPTY_ARRAY = -1;
    level_t _NULL_ARRAY = -1;
};
}; // namespace doris::vectorized