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
#include <gen_cpp/parquet_types.h>
#include <glog/logging.h>
#include <stddef.h>
#include <stdint.h>

#include <list>
#include <memory>
#include <ostream>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "core/data_type/data_type.h"
#include "format/column_type_convert.h"
#include "format/generic_reader.h"
#include "format/parquet/parquet_common.h"
#include "format/table/table_schema_change_helper.h"
#include "format_v2/parquet/reader/native/column_chunk_reader.h"
#include "io/fs/buffered_reader.h"
#include "io/fs/file_reader_writer_fwd.h"

namespace cctz {
class time_zone;
} // namespace cctz

namespace doris::io {
struct IOContext;
} // namespace doris::io

namespace doris::format::parquet::native {
using ::doris::FieldSchema;
using ::doris::ColumnString;

class ColumnReader {
public:
    struct ColumnStatistics {
        ColumnStatistics()
                : page_index_read_calls(0),
                  decompress_time(0),
                  decompress_cnt(0),
                  decode_header_time(0),
                  decode_value_time(0),
                  materialization_time(0),
                  hybrid_selection_batches(0),
                  hybrid_selection_ranges(0),
                  hybrid_selection_null_fallback_batches(0),
                  decode_dict_time(0),
                  decode_level_time(0),
                  decode_null_map_time(0),
                  convert_time(0),
                  skip_page_header_num(0),
                  parse_page_header_num(0),
                  read_page_header_time(0),
                  page_read_counter(0),
                  page_cache_write_counter(0),
                  page_cache_compressed_write_counter(0),
                  page_cache_decompressed_write_counter(0),
                  page_cache_hit_counter(0),
                  page_cache_missing_counter(0),
                  page_cache_compressed_hit_counter(0),
                  page_cache_decompressed_hit_counter(0) {}

        ColumnStatistics(ColumnChunkReaderStatistics& cs, int64_t null_map_time,
                         int64_t convert_time_)
                : page_index_read_calls(0),
                  decompress_time(cs.decompress_time),
                  decompress_cnt(cs.decompress_cnt),
                  decode_header_time(cs.decode_header_time),
                  decode_value_time(cs.decode_value_time),
                  materialization_time(cs.materialization_time),
                  hybrid_selection_batches(cs.hybrid_selection_batches),
                  hybrid_selection_ranges(cs.hybrid_selection_ranges),
                  hybrid_selection_null_fallback_batches(cs.hybrid_selection_null_fallback_batches),
                  decode_dict_time(cs.decode_dict_time),
                  decode_level_time(cs.decode_level_time),
                  decode_null_map_time(null_map_time),
                  convert_time(convert_time_),
                  skip_page_header_num(cs.skip_page_header_num),
                  parse_page_header_num(cs.parse_page_header_num),
                  read_page_header_time(cs.read_page_header_time),
                  page_read_counter(cs.page_read_counter),
                  page_cache_write_counter(cs.page_cache_write_counter),
                  page_cache_compressed_write_counter(cs.page_cache_compressed_write_counter),
                  page_cache_decompressed_write_counter(cs.page_cache_decompressed_write_counter),
                  page_cache_hit_counter(cs.page_cache_hit_counter),
                  page_cache_missing_counter(cs.page_cache_missing_counter),
                  page_cache_compressed_hit_counter(cs.page_cache_compressed_hit_counter),
                  page_cache_decompressed_hit_counter(cs.page_cache_decompressed_hit_counter) {}

        int64_t page_index_read_calls;
        int64_t decompress_time;
        int64_t decompress_cnt;
        int64_t decode_header_time;
        int64_t decode_value_time;
        int64_t materialization_time;
        int64_t hybrid_selection_batches;
        int64_t hybrid_selection_ranges;
        int64_t hybrid_selection_null_fallback_batches;
        int64_t decode_dict_time;
        int64_t decode_level_time;
        int64_t decode_null_map_time;
        int64_t convert_time;
        int64_t skip_page_header_num;
        int64_t parse_page_header_num;
        int64_t read_page_header_time;
        int64_t page_read_counter;
        int64_t page_cache_write_counter;
        int64_t page_cache_compressed_write_counter;
        int64_t page_cache_decompressed_write_counter;
        int64_t page_cache_hit_counter;
        int64_t page_cache_missing_counter;
        int64_t page_cache_compressed_hit_counter;
        int64_t page_cache_decompressed_hit_counter;

        void merge(ColumnStatistics& col_statistics) {
            page_index_read_calls += col_statistics.page_index_read_calls;
            decompress_time += col_statistics.decompress_time;
            decompress_cnt += col_statistics.decompress_cnt;
            decode_header_time += col_statistics.decode_header_time;
            decode_value_time += col_statistics.decode_value_time;
            materialization_time += col_statistics.materialization_time;
            hybrid_selection_batches += col_statistics.hybrid_selection_batches;
            hybrid_selection_ranges += col_statistics.hybrid_selection_ranges;
            hybrid_selection_null_fallback_batches +=
                    col_statistics.hybrid_selection_null_fallback_batches;
            decode_dict_time += col_statistics.decode_dict_time;
            decode_level_time += col_statistics.decode_level_time;
            decode_null_map_time += col_statistics.decode_null_map_time;
            convert_time += col_statistics.convert_time;
            skip_page_header_num += col_statistics.skip_page_header_num;
            parse_page_header_num += col_statistics.parse_page_header_num;
            read_page_header_time += col_statistics.read_page_header_time;
            page_read_counter += col_statistics.page_read_counter;
            page_cache_write_counter += col_statistics.page_cache_write_counter;
            page_cache_compressed_write_counter +=
                    col_statistics.page_cache_compressed_write_counter;
            page_cache_decompressed_write_counter +=
                    col_statistics.page_cache_decompressed_write_counter;
            page_cache_hit_counter += col_statistics.page_cache_hit_counter;
            page_cache_missing_counter += col_statistics.page_cache_missing_counter;
            page_cache_compressed_hit_counter += col_statistics.page_cache_compressed_hit_counter;
            page_cache_decompressed_hit_counter +=
                    col_statistics.page_cache_decompressed_hit_counter;
        }
    };

    ColumnReader(const RowRanges& row_ranges, size_t total_rows, const cctz::time_zone* ctz,
                 io::IOContext* io_ctx)
            : _row_ranges(row_ranges), _total_rows(total_rows), _ctz(ctz), _io_ctx(io_ctx) {}
    virtual ~ColumnReader() = default;
    virtual Status read_column_data(ColumnPtr& doris_column, const DataTypePtr& type,
                                    const std::shared_ptr<TableSchemaChangeHelper::Node>& root_node,
                                    FilterMap& filter_map, size_t batch_size, size_t* read_rows,
                                    bool* eof, bool is_dict_filter,
                                    int64_t real_column_size = -1) = 0;

    // Consume a nested batch while retaining only definition/repetition levels. This is used when
    // schema evolution makes every projected STRUCT child synthetic: the parent still needs one
    // physical leaf's shape, but decoding that leaf's strings or other payload would be wasted.
    virtual Status read_column_levels(FilterMap& filter_map, size_t batch_size, size_t* read_rows,
                                      bool* eof) = 0;

    virtual Result<MutableColumnPtr> convert_dict_column_to_string_column(
            const ColumnInt32* dict_column) {
        throw Exception(
                Status::FatalError("Method convert_dict_column_to_string_column is not supported"));
    }

    static Status create(io::FileReaderSPtr file, FieldSchema* field,
                         const tparquet::RowGroup& row_group, const RowRanges& row_ranges,
                         const cctz::time_zone* ctz, io::IOContext* io_ctx,
                         std::unique_ptr<ColumnReader>& reader, size_t max_buf_size,
                         std::unordered_map<int, tparquet::OffsetIndex>& col_offsets,
                         RuntimeState* state, bool in_collection = false,
                         const std::set<uint64_t>& column_ids = {},
                         const std::set<uint64_t>& filter_column_ids = {});
    virtual const std::vector<level_t>& get_rep_level() const = 0;
    virtual const std::vector<level_t>& get_def_level() const = 0;
    virtual ColumnStatistics column_statistics() = 0;
    virtual void close() = 0;

    // A repeated parent can expand one logical-row batch into millions of leaf values. Keep
    // ordinary batch scratch for reuse, but let the top-level adapter release exceptional
    // high-water allocations after every parent offset/null-map consumer has finished.
    virtual void release_batch_scratch(size_t max_retained_bytes) = 0;

    virtual void reset_filter_map_index() = 0;

    FieldSchema* get_field_schema() const { return _field_schema; }
    void set_column_in_nested() { _in_nested = true; }

protected:
    void _generate_read_ranges(RowRange page_row_range, RowRanges* result_ranges) const;

    FieldSchema* _field_schema = nullptr;
    const RowRanges& _row_ranges;
    size_t _total_rows = 0;
    const cctz::time_zone* _ctz = nullptr;
    io::IOContext* _io_ctx = nullptr;
    int64_t _current_row_index = 0;
    int64_t _decode_null_map_time = 0;

    size_t _filter_map_index = 0;
    std::set<uint64_t> _filter_column_ids;

    // _in_nested: column in struct/map/array
    // IN_COLLECTION : column in map/array
    bool _in_nested = false;
};

template <bool IN_COLLECTION, bool OFFSET_INDEX>
class ScalarColumnReader : public ColumnReader {
    ENABLE_FACTORY_CREATOR(ScalarColumnReader)
public:
    ScalarColumnReader(const RowRanges& row_ranges, size_t total_rows,
                       const tparquet::ColumnChunk& chunk_meta,
                       const tparquet::OffsetIndex* offset_index, const cctz::time_zone* ctz,
                       io::IOContext* io_ctx)
            : ColumnReader(row_ranges, total_rows, ctz, io_ctx),
              _chunk_meta(chunk_meta),
              _offset_index(offset_index) {}
    ~ScalarColumnReader() override { close(); }
    Status init(io::FileReaderSPtr file, FieldSchema* field, size_t max_buf_size,
                RuntimeState* state);
    Status read_column_data(ColumnPtr& doris_column, const DataTypePtr& type,
                            const std::shared_ptr<TableSchemaChangeHelper::Node>& root_node,
                            FilterMap& filter_map, size_t batch_size, size_t* read_rows, bool* eof,
                            bool is_dict_filter, int64_t real_column_size = -1) override;
    Status read_column_levels(FilterMap& filter_map, size_t batch_size, size_t* read_rows,
                              bool* eof) override;
    Result<MutableColumnPtr> convert_dict_column_to_string_column(
            const ColumnInt32* dict_column) override;
    const std::vector<level_t>& get_rep_level() const override { return _rep_levels; }
    const std::vector<level_t>& get_def_level() const override { return _def_levels; }
    ColumnStatistics column_statistics() override {
        return ColumnStatistics(_chunk_reader->chunk_statistics(), _decode_null_map_time,
                                _convert_time);
    }
    void close() override {}

    void release_batch_scratch(size_t max_retained_bytes) override;

#ifdef BE_TEST
    void reserve_batch_scratch_for_test(size_t elements);
    size_t retained_batch_scratch_bytes_for_test() const;
#endif

    void reset_filter_map_index() override {
        _filter_map_index = 0; // nested
    }

private:
    tparquet::ColumnChunk _chunk_meta;
    const tparquet::OffsetIndex* _offset_index = nullptr;
    std::unique_ptr<io::BufferedFileStreamReader> _stream_reader;
    std::unique_ptr<ColumnChunkReader<IN_COLLECTION, OFFSET_INDEX>> _chunk_reader;
    // rep def levels buffer.
    std::vector<level_t> _rep_levels;
    std::vector<level_t> _def_levels;

    size_t _current_range_idx = 0;

    Status gen_nested_null_map(size_t level_start_idx, size_t level_end_idx,
                               std::vector<uint16_t>& null_map,
                               std::unordered_set<size_t>& ancestor_null_indices) {
        size_t has_read = level_start_idx;
        null_map.emplace_back(0);
        bool prev_is_null = false;

        while (has_read < level_end_idx) {
            level_t def_level = _def_levels[has_read++];
            size_t loop_read = 1;
            while (has_read < _def_levels.size() && _def_levels[has_read] == def_level) {
                has_read++;
                loop_read++;
            }

            if (def_level < _field_schema->repeated_parent_def_level) {
                for (size_t i = 0; i < loop_read; i++) {
                    ancestor_null_indices.insert(has_read - level_start_idx - loop_read + i);
                }
                continue;
            }

            bool is_null = def_level < _field_schema->definition_level;

            if (prev_is_null == is_null && (USHRT_MAX - null_map.back() >= loop_read)) {
                null_map.back() += loop_read;
            } else {
                if (!(prev_is_null ^ is_null)) {
                    null_map.emplace_back(0);
                }
                size_t remaining = loop_read;
                while (remaining > USHRT_MAX) {
                    null_map.emplace_back(USHRT_MAX);
                    null_map.emplace_back(0);
                    remaining -= USHRT_MAX;
                }
                null_map.emplace_back((u_short)remaining);
                prev_is_null = is_null;
            }
        }
        return Status::OK();
    }

    Status gen_filter_map(FilterMap& filter_map, size_t filter_loc, size_t level_start_idx,
                          size_t level_end_idx, std::vector<uint8_t>& nested_filter_map_data,
                          FilterMap* nested_filter_map) {
        DORIS_CHECK(nested_filter_map != nullptr);
        nested_filter_map_data.resize(level_end_idx - level_start_idx);
        for (size_t idx = level_start_idx; idx < level_end_idx; idx++) {
            if (idx != level_start_idx && _rep_levels[idx] == 0) {
                filter_loc++;
            }
            nested_filter_map_data[idx - level_start_idx] =
                    filter_map.filter_map_data()[filter_loc];
        }

        return nested_filter_map->init(nested_filter_map_data.data(), nested_filter_map_data.size(),
                                       false);
    }

    DataTypeSerDeSPtr _serde;
    const IDataType* _serde_type = nullptr;
    std::unique_ptr<converter::ColumnTypeConverter> _logical_converter;
    const IDataType* _converter_source_type = nullptr;
    const IDataType* _converter_target_type = nullptr;
    ParquetDecodeContext _decode_context;
    ParquetMaterializationState _materialization_state;
    bool _dictionary_index_only = false;
    // Normal-size batch scratch is retained by the persistent leaf reader. Oversized allocations
    // are released only after the top-level parent has consumed this leaf's level plan.
    std::vector<uint16_t> _null_run_lengths;
    std::unordered_set<size_t> _ancestor_null_indices;
    std::vector<uint8_t> _nested_filter_map_data;
    FilterMap _nested_filter_map;
    ColumnSelectVector _select_vector;
    int64_t _convert_time = 0;
    size_t _logical_conversion_scratch_bytes = 0;

    Status _skip_values(size_t num_values);
    Status _read_values(size_t num_values, ColumnPtr& doris_column, const DataTypePtr& type,
                        FilterMap& filter_map, bool is_dict_filter);
    Status _read_nested_column(ColumnPtr& doris_column, const DataTypePtr& type,
                               FilterMap& filter_map, size_t batch_size, size_t* read_rows,
                               bool* eof, bool is_dict_filter);
    Status _try_load_dict_page(bool* loaded, bool* has_dict);
};

class ArrayColumnReader : public ColumnReader {
    ENABLE_FACTORY_CREATOR(ArrayColumnReader)
public:
    ArrayColumnReader(const RowRanges& row_ranges, size_t total_rows, const cctz::time_zone* ctz,
                      io::IOContext* io_ctx)
            : ColumnReader(row_ranges, total_rows, ctz, io_ctx) {}
    ~ArrayColumnReader() override { close(); }
    Status init(std::unique_ptr<ColumnReader> element_reader, FieldSchema* field);
    Status read_column_data(ColumnPtr& doris_column, const DataTypePtr& type,
                            const std::shared_ptr<TableSchemaChangeHelper::Node>& root_node,
                            FilterMap& filter_map, size_t batch_size, size_t* read_rows, bool* eof,
                            bool is_dict_filter, int64_t real_column_size = -1) override;
    Status read_column_levels(FilterMap& filter_map, size_t batch_size, size_t* read_rows,
                              bool* eof) override {
        return _element_reader->read_column_levels(filter_map, batch_size, read_rows, eof);
    }
    const std::vector<level_t>& get_rep_level() const override {
        return _element_reader->get_rep_level();
    }
    const std::vector<level_t>& get_def_level() const override {
        return _element_reader->get_def_level();
    }
    ColumnStatistics column_statistics() override { return _element_reader->column_statistics(); }
    void close() override {}

    void release_batch_scratch(size_t max_retained_bytes) override {
        _element_reader->release_batch_scratch(max_retained_bytes);
    }

    void reset_filter_map_index() override { _element_reader->reset_filter_map_index(); }

private:
    std::unique_ptr<ColumnReader> _element_reader;
};

class MapColumnReader : public ColumnReader {
    ENABLE_FACTORY_CREATOR(MapColumnReader)
public:
    MapColumnReader(const RowRanges& row_ranges, size_t total_rows, const cctz::time_zone* ctz,
                    io::IOContext* io_ctx)
            : ColumnReader(row_ranges, total_rows, ctz, io_ctx) {}
    ~MapColumnReader() override { close(); }

    Status init(std::unique_ptr<ColumnReader> key_reader,
                std::unique_ptr<ColumnReader> value_reader, FieldSchema* field);
    Status read_column_data(ColumnPtr& doris_column, const DataTypePtr& type,
                            const std::shared_ptr<TableSchemaChangeHelper::Node>& root_node,
                            FilterMap& filter_map, size_t batch_size, size_t* read_rows, bool* eof,
                            bool is_dict_filter, int64_t real_column_size = -1) override;
    Status read_column_levels(FilterMap& filter_map, size_t batch_size, size_t* read_rows,
                              bool* eof) override;

    const std::vector<level_t>& get_rep_level() const override {
        return _key_reader->get_rep_level();
    }
    const std::vector<level_t>& get_def_level() const override {
        return _key_reader->get_def_level();
    }

    ColumnStatistics column_statistics() override {
        ColumnStatistics kst = _key_reader->column_statistics();
        ColumnStatistics vst = _value_reader->column_statistics();
        kst.merge(vst);
        return kst;
    }

    void close() override {}

    void release_batch_scratch(size_t max_retained_bytes) override {
        _key_reader->release_batch_scratch(max_retained_bytes);
        _value_reader->release_batch_scratch(max_retained_bytes);
    }

    void reset_filter_map_index() override {
        _key_reader->reset_filter_map_index();
        _value_reader->reset_filter_map_index();
    }

private:
    std::unique_ptr<ColumnReader> _key_reader;
    std::unique_ptr<ColumnReader> _value_reader;
};

class StructColumnReader : public ColumnReader {
    ENABLE_FACTORY_CREATOR(StructColumnReader)
public:
    StructColumnReader(const RowRanges& row_ranges, size_t total_rows, const cctz::time_zone* ctz,
                       io::IOContext* io_ctx)
            : ColumnReader(row_ranges, total_rows, ctz, io_ctx) {}
    ~StructColumnReader() override { close(); }

    Status init(std::unordered_map<std::string, std::unique_ptr<ColumnReader>>&& child_readers,
                FieldSchema* field);
    Status read_column_data(ColumnPtr& doris_column, const DataTypePtr& type,
                            const std::shared_ptr<TableSchemaChangeHelper::Node>& root_node,
                            FilterMap& filter_map, size_t batch_size, size_t* read_rows, bool* eof,
                            bool is_dict_filter, int64_t real_column_size = -1) override;
    Status read_column_levels(FilterMap& filter_map, size_t batch_size, size_t* read_rows,
                              bool* eof) override;

    const std::vector<level_t>& get_rep_level() const override {
        if (!_read_column_names.empty()) {
            // can't use _child_readers[*_read_column_names.begin()]
            // because the operator[] of std::unordered_map is not const :(
            /*
             * Considering the issue in the `_read_nested_column` function where data may span across pages, leading
             * to missing definition and repetition levels, when filling the null_map of the struct later, it is
             * crucial to use the definition and repetition levels from the first read column,
             * that is `_read_column_names.front()`.
             */
            return _child_readers.find(_read_column_names.front())->second->get_rep_level();
        }
        return _child_readers.begin()->second->get_rep_level();
    }

    const std::vector<level_t>& get_def_level() const override {
        if (!_read_column_names.empty()) {
            return _child_readers.find(_read_column_names.front())->second->get_def_level();
        }
        return _child_readers.begin()->second->get_def_level();
    }

    ColumnStatistics column_statistics() override {
        ColumnStatistics st;
        for (const auto& column_name : _read_column_names) {
            auto reader = _child_readers.find(column_name);
            if (reader != _child_readers.end()) {
                ColumnStatistics cst = reader->second->column_statistics();
                st.merge(cst);
            }
        }
        return st;
    }

    void close() override {}

    void release_batch_scratch(size_t max_retained_bytes) override {
        for (const auto& reader : _child_readers) {
            reader.second->release_batch_scratch(max_retained_bytes);
        }
    }

    void reset_filter_map_index() override {
        for (const auto& reader : _child_readers) {
            reader.second->reset_filter_map_index();
        }
    }

private:
    std::unordered_map<std::string, std::unique_ptr<ColumnReader>> _child_readers;
    std::vector<std::string> _read_column_names;
    //Need to use vector instead of set,see `get_rep_level()` for the reason.
};

// A special reader that skips actual reading but provides empty data with correct structure
// This is used when a column is not needed but its structure is required (e.g., for map keys)
class SkipReadingReader : public ColumnReader {
public:
    SkipReadingReader(const RowRanges& row_ranges, size_t total_rows, const cctz::time_zone* ctz,
                      io::IOContext* io_ctx, FieldSchema* field_schema)
            : ColumnReader(row_ranges, total_rows, ctz, io_ctx) {
        _field_schema = field_schema; // Use inherited member from base class
        VLOG_DEBUG << "[ParquetReader] Created SkipReadingReader for field: "
                   << _field_schema->name;
    }

    Status read_column_data(ColumnPtr& doris_column, const DataTypePtr& type,
                            const std::shared_ptr<TableSchemaChangeHelper::Node>& root_node,
                            FilterMap& filter_map, size_t batch_size, size_t* read_rows, bool* eof,
                            bool is_dict_filter, int64_t real_column_size = -1) override {
        VLOG_DEBUG << "[ParquetReader] SkipReadingReader::read_column_data for field: "
                   << _field_schema->name << ", batch_size: " << batch_size;
        DCHECK(real_column_size >= 0); // real_column_size for filtered column size.

        // Simulate reading without actually reading data
        // Fill with default/null values based on column type
        doris_column = IColumn::mutate(std::move(doris_column));
        MutableColumnPtr data_column = doris_column->assert_mutable();

        if (real_column_size > 0) {
            if (is_column_nullable(*doris_column)) {
                auto* nullable_column = static_cast<ColumnNullable*>(data_column.get());
                nullable_column->insert_many_defaults(real_column_size);
            } else {
                // For non-nullable columns, insert appropriate default values
                for (size_t i = 0; i < real_column_size; ++i) {
                    data_column->insert_default();
                }
            }
        }

        *read_rows = batch_size; // Indicate we "read" batch_size rows
        *eof = false;            // We can always provide more empty data

        VLOG_DEBUG << "[ParquetReader] SkipReadingReader generated " << batch_size
                   << " default values for field: " << _field_schema->name;

        return Status::OK();
    }

    Status read_column_levels(FilterMap&, size_t, size_t*, bool*) override {
        return Status::InternalError("Skip reader cannot provide Parquet levels for field {}",
                                     _field_schema->name);
    }

    static std::unique_ptr<SkipReadingReader> create_unique(const RowRanges& row_ranges,
                                                            size_t total_rows, cctz::time_zone* ctz,
                                                            io::IOContext* io_ctx,
                                                            FieldSchema* field_schema) {
        return std::make_unique<SkipReadingReader>(row_ranges, total_rows, ctz, io_ctx,
                                                   field_schema);
    }

    // These methods should not be called for SkipReadingReader
    // If they are called, it indicates a logic error in the code
    const std::vector<level_t>& get_rep_level() const override {
        LOG(FATAL) << "get_rep_level() should not be called on SkipReadingReader for field: "
                   << _field_schema->name
                   << ". This indicates the SkipReadingReader was incorrectly used as a reference "
                      "column.";
        __builtin_unreachable();
    }

    const std::vector<level_t>& get_def_level() const override {
        LOG(FATAL) << "get_def_level() should not be called on SkipReadingReader for field: "
                   << _field_schema->name
                   << ". This indicates the SkipReadingReader was incorrectly used as a reference "
                      "column.";
        __builtin_unreachable();
    }

    // Implement required pure virtual methods from base class
    ColumnStatistics column_statistics() override {
        return ColumnStatistics(); // Return empty statistics
    }

    void close() override {
        // Nothing to close for skip reading
    }

    void release_batch_scratch(size_t) override {}

    void reset_filter_map_index() override { _filter_map_index = 0; }
};

} // namespace doris::format::parquet::native
