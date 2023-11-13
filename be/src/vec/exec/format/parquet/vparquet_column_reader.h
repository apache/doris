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
#include <glog/logging.h>
#include <stddef.h>
#include <stdint.h>

#include <list>
#include <memory>
#include <ostream>
#include <vector>

#include "io/fs/buffered_reader.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "vec/columns/columns_number.h"
#include "vec/data_types/data_type.h"
#include "vec/exec/format/parquet/parquet_common.h"
#include "vparquet_column_chunk_reader.h"

namespace cctz {
class time_zone;
} // namespace cctz
namespace doris {
namespace io {
struct IOContext;
} // namespace io
namespace vectorized {
class ColumnString;
struct FieldSchema;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

class ParquetColumnReader {
public:
    struct Statistics {
        Statistics()
                : read_time(0),
                  read_calls(0),
                  meta_read_calls(0),
                  read_bytes(0),
                  decompress_time(0),
                  decompress_cnt(0),
                  decode_header_time(0),
                  decode_value_time(0),
                  decode_dict_time(0),
                  decode_level_time(0),
                  decode_null_map_time(0) {}

        Statistics(io::BufferedStreamReader::Statistics& fs, ColumnChunkReader::Statistics& cs,
                   int64_t null_map_time)
                : read_time(fs.read_time),
                  read_calls(fs.read_calls),
                  meta_read_calls(0),
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
        int64_t meta_read_calls;
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
            meta_read_calls += statistics.meta_read_calls;
            decompress_time += statistics.decompress_time;
            decompress_cnt += statistics.decompress_cnt;
            decode_header_time += statistics.decode_header_time;
            decode_value_time += statistics.decode_value_time;
            decode_dict_time += statistics.decode_dict_time;
            decode_level_time += statistics.decode_level_time;
            decode_null_map_time += statistics.decode_null_map_time;
        }
    };

    ParquetColumnReader(const std::vector<RowRange>& row_ranges, cctz::time_zone* ctz,
                        io::IOContext* io_ctx)
            : _row_ranges(row_ranges), _ctz(ctz), _io_ctx(io_ctx) {}
    virtual ~ParquetColumnReader() = default;
    virtual Status read_column_data(ColumnPtr& doris_column, DataTypePtr& type,
                                    ColumnSelectVector& select_vector, size_t batch_size,
                                    size_t* read_rows, bool* eof, bool is_dict_filter) = 0;

    virtual Status read_dict_values_to_column(MutableColumnPtr& doris_column, bool* has_dict) {
        return Status::NotSupported("read_dict_values_to_column is not supported");
    }

    virtual Status get_dict_codes(const ColumnString* column_string,
                                  std::vector<int32_t>* dict_codes) {
        return Status::NotSupported("get_dict_codes is not supported");
    }

    virtual MutableColumnPtr convert_dict_column_to_string_column(const ColumnInt32* dict_column) {
        LOG(FATAL) << "Method convert_dict_column_to_string_column is not supported";
    }

    static Status create(io::FileReaderSPtr file, FieldSchema* field,
                         const tparquet::RowGroup& row_group,
                         const std::vector<RowRange>& row_ranges, cctz::time_zone* ctz,
                         io::IOContext* io_ctx, std::unique_ptr<ParquetColumnReader>& reader,
                         size_t max_buf_size);
    void set_nested_column() { _nested_column = true; }
    virtual const std::vector<level_t>& get_rep_level() const = 0;
    virtual const std::vector<level_t>& get_def_level() const = 0;
    virtual Statistics statistics() = 0;
    virtual void close() = 0;

protected:
    void _generate_read_ranges(int64_t start_index, int64_t end_index,
                               std::list<RowRange>& read_ranges);

    FieldSchema* _field_schema;
    // When scalar column is the child of nested column, we should turn off the filtering by page index and lazy read.
    bool _nested_column = false;
    const std::vector<RowRange>& _row_ranges;
    cctz::time_zone* _ctz;
    io::IOContext* _io_ctx;
    int64_t _current_row_index = 0;
    int _row_range_index = 0;
    int64_t _decode_null_map_time = 0;
};

class ScalarColumnReader : public ParquetColumnReader {
    ENABLE_FACTORY_CREATOR(ScalarColumnReader)
public:
    ScalarColumnReader(const std::vector<RowRange>& row_ranges,
                       const tparquet::ColumnChunk& chunk_meta, cctz::time_zone* ctz,
                       io::IOContext* io_ctx)
            : ParquetColumnReader(row_ranges, ctz, io_ctx), _chunk_meta(chunk_meta) {}
    ~ScalarColumnReader() override { close(); }
    Status init(io::FileReaderSPtr file, FieldSchema* field, size_t max_buf_size);
    Status read_column_data(ColumnPtr& doris_column, DataTypePtr& type,
                            ColumnSelectVector& select_vector, size_t batch_size, size_t* read_rows,
                            bool* eof, bool is_dict_filter) override;
    Status read_dict_values_to_column(MutableColumnPtr& doris_column, bool* has_dict) override;
    Status get_dict_codes(const ColumnString* column_string,
                          std::vector<int32_t>* dict_codes) override;
    MutableColumnPtr convert_dict_column_to_string_column(const ColumnInt32* dict_column) override;
    const std::vector<level_t>& get_rep_level() const override { return _rep_levels; }
    const std::vector<level_t>& get_def_level() const override { return _def_levels; }
    Statistics statistics() override {
        return Statistics(_stream_reader->statistics(), _chunk_reader->statistics(),
                          _decode_null_map_time);
    }
    void close() override {}

private:
    tparquet::ColumnChunk _chunk_meta;
    std::unique_ptr<io::BufferedFileStreamReader> _stream_reader;
    std::unique_ptr<ColumnChunkReader> _chunk_reader;
    std::vector<level_t> _rep_levels;
    std::vector<level_t> _def_levels;

    Status _skip_values(size_t num_values);
    Status _read_values(size_t num_values, ColumnPtr& doris_column, DataTypePtr& type,
                        ColumnSelectVector& select_vector, bool is_dict_filter);
    Status _read_nested_column(ColumnPtr& doris_column, DataTypePtr& type,
                               ColumnSelectVector& select_vector, size_t batch_size,
                               size_t* read_rows, bool* eof, bool is_dict_filter, bool align_rows);
    Status _try_load_dict_page(bool* loaded, bool* has_dict);
};

class ArrayColumnReader : public ParquetColumnReader {
    ENABLE_FACTORY_CREATOR(ArrayColumnReader)
public:
    ArrayColumnReader(const std::vector<RowRange>& row_ranges, cctz::time_zone* ctz,
                      io::IOContext* io_ctx)
            : ParquetColumnReader(row_ranges, ctz, io_ctx) {}
    ~ArrayColumnReader() override { close(); }
    Status init(std::unique_ptr<ParquetColumnReader> element_reader, FieldSchema* field);
    Status read_column_data(ColumnPtr& doris_column, DataTypePtr& type,
                            ColumnSelectVector& select_vector, size_t batch_size, size_t* read_rows,
                            bool* eof, bool is_dict_filter) override;
    const std::vector<level_t>& get_rep_level() const override {
        return _element_reader->get_rep_level();
    }
    const std::vector<level_t>& get_def_level() const override {
        return _element_reader->get_def_level();
    }
    Statistics statistics() override { return _element_reader->statistics(); }
    void close() override {}

private:
    std::unique_ptr<ParquetColumnReader> _element_reader = nullptr;
};

class MapColumnReader : public ParquetColumnReader {
    ENABLE_FACTORY_CREATOR(MapColumnReader)
public:
    MapColumnReader(const std::vector<RowRange>& row_ranges, cctz::time_zone* ctz,
                    io::IOContext* io_ctx)
            : ParquetColumnReader(row_ranges, ctz, io_ctx) {}
    ~MapColumnReader() override { close(); }

    Status init(std::unique_ptr<ParquetColumnReader> key_reader,
                std::unique_ptr<ParquetColumnReader> value_reader, FieldSchema* field);
    Status read_column_data(ColumnPtr& doris_column, DataTypePtr& type,
                            ColumnSelectVector& select_vector, size_t batch_size, size_t* read_rows,
                            bool* eof, bool is_dict_filter) override;

    const std::vector<level_t>& get_rep_level() const override {
        return _key_reader->get_rep_level();
    }
    const std::vector<level_t>& get_def_level() const override {
        return _key_reader->get_def_level();
    }

    Statistics statistics() override {
        Statistics kst = _key_reader->statistics();
        Statistics vst = _value_reader->statistics();
        kst.merge(vst);
        return kst;
    }

    void close() override {}

private:
    std::unique_ptr<ParquetColumnReader> _key_reader = nullptr;
    std::unique_ptr<ParquetColumnReader> _value_reader = nullptr;
};

class StructColumnReader : public ParquetColumnReader {
    ENABLE_FACTORY_CREATOR(StructColumnReader)
public:
    StructColumnReader(const std::vector<RowRange>& row_ranges, cctz::time_zone* ctz,
                       io::IOContext* io_ctx)
            : ParquetColumnReader(row_ranges, ctz, io_ctx) {}
    ~StructColumnReader() override { close(); }

    Status init(std::vector<std::unique_ptr<ParquetColumnReader>>&& child_readers,
                FieldSchema* field);
    Status read_column_data(ColumnPtr& doris_column, DataTypePtr& type,
                            ColumnSelectVector& select_vector, size_t batch_size, size_t* read_rows,
                            bool* eof, bool is_dict_filter) override;

    const std::vector<level_t>& get_rep_level() const override {
        return _child_readers[0]->get_rep_level();
    }
    const std::vector<level_t>& get_def_level() const override {
        return _child_readers[0]->get_def_level();
    }

    Statistics statistics() override {
        Statistics st;
        for (const auto& reader : _child_readers) {
            Statistics cst = reader->statistics();
            st.merge(cst);
        }
        return st;
    }

    void close() override {}

private:
    std::vector<std::unique_ptr<ParquetColumnReader>> _child_readers;
};

}; // namespace doris::vectorized