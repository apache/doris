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

#include "format_v2/parquet/reader/native/level_reader.h"

#include <algorithm>
#include <utility>

#include "format/parquet/schema_desc.h"
#include "format_v2/parquet/reader/native/column_chunk_reader.h"
#include "io/fs/buffered_reader.h"
#include "io/fs/tracing_file_reader.h"

namespace doris::format::parquet::native {

class LevelReader::Impl {
public:
    virtual ~Impl() = default;

    virtual Status init() = 0;
    virtual Status read_rows(size_t rows, std::vector<level_t>* repetition_levels,
                             std::vector<level_t>* definition_levels, size_t* rows_read) = 0;
    virtual ColumnChunkReaderStatistics statistics() = 0;
};

template <bool IN_COLLECTION>
class LevelReaderImpl final : public LevelReader::Impl {
public:
    LevelReaderImpl(io::FileReaderSPtr file, tparquet::ColumnChunk column_chunk, FieldSchema* field,
                    size_t total_rows, size_t max_buffer_size, io::IOContext* io_ctx,
                    bool enable_page_cache)
            : _file(std::move(file)),
              _column_chunk(std::move(column_chunk)),
              _field(field),
              _total_rows(total_rows),
              _max_buffer_size(max_buffer_size),
              _io_ctx(io_ctx),
              _enable_page_cache(enable_page_cache) {}

    Status init() override {
        DORIS_CHECK(_file != nullptr);
        DORIS_CHECK(_field != nullptr);
        const auto& metadata = _column_chunk.meta_data;
        const int64_t chunk_start = has_dict_page(metadata) ? metadata.dictionary_page_offset
                                                            : metadata.data_page_offset;
        const size_t chunk_size = metadata.total_compressed_size;
        size_t prefetch_buffer_size = std::min(chunk_size, _max_buffer_size);
        auto* tracing_reader = typeid_cast<io::TracingFileReader*>(_file.get());
        if ((tracing_reader != nullptr &&
             typeid_cast<io::MergeRangeFileReader*>(tracing_reader->inner_reader().get()) !=
                     nullptr) ||
            typeid_cast<io::MergeRangeFileReader*>(_file.get()) != nullptr) {
            prefetch_buffer_size = 0;
        }
        _stream = std::make_unique<io::BufferedFileStreamReader>(_file, chunk_start, chunk_size,
                                                                 prefetch_buffer_size);
        _chunk_reader = std::make_unique<ColumnChunkReader<IN_COLLECTION, false>>(
                _stream.get(), &_column_chunk, _field, nullptr, _total_rows, _io_ctx,
                ParquetPageReadContext(_enable_page_cache));
        return _chunk_reader->init();
    }

    Status read_rows(size_t rows, std::vector<level_t>* repetition_levels,
                     std::vector<level_t>* definition_levels, size_t* rows_read) override {
        DORIS_CHECK(repetition_levels != nullptr);
        DORIS_CHECK(definition_levels != nullptr);
        DORIS_CHECK(rows_read != nullptr);
        repetition_levels->clear();
        definition_levels->clear();
        *rows_read = 0;
        if (_current_row > _total_rows || rows > _total_rows - _current_row) {
            return Status::Corruption("Parquet level reader requested rows [{}, {}) of {}",
                                      _current_row, _current_row + rows, _total_rows);
        }
        if constexpr (IN_COLLECTION) {
            RETURN_IF_ERROR(
                    read_nested_rows(rows, repetition_levels, definition_levels, rows_read));
        } else {
            RETURN_IF_ERROR(read_flat_rows(rows, repetition_levels, definition_levels, rows_read));
        }
        _current_row += *rows_read;
        return Status::OK();
    }

    ColumnChunkReaderStatistics statistics() override { return _chunk_reader->statistics(); }

private:
    Status read_flat_rows(size_t rows, std::vector<level_t>* repetition_levels,
                          std::vector<level_t>* definition_levels, size_t* rows_read) {
        while (*rows_read < rows) {
            RETURN_IF_ERROR(_chunk_reader->parse_page_header());
            RETURN_IF_ERROR(_chunk_reader->load_page_data_idempotent());
            const size_t read_now = std::min(
                    rows - *rows_read, static_cast<size_t>(_chunk_reader->remaining_num_values()));
            if (read_now == 0) {
                return Status::Corruption("Parquet flat level reader made no progress");
            }
            RETURN_IF_ERROR(
                    _chunk_reader->read_levels(read_now, repetition_levels, definition_levels));
            *rows_read += read_now;
            if (_chunk_reader->remaining_num_values() == 0 && _chunk_reader->has_next_page()) {
                RETURN_IF_ERROR(_chunk_reader->next_page());
            }
        }
        return Status::OK();
    }

    Status read_nested_rows(size_t rows, std::vector<level_t>* repetition_levels,
                            std::vector<level_t>* definition_levels, size_t* rows_read) {
        while (*rows_read < rows) {
            RETURN_IF_ERROR(_chunk_reader->seek_to_nested_row(_current_row + *rows_read));
            const size_t start_level = definition_levels->size();
            size_t loaded_rows = 0;
            bool crosses_page = false;
            RETURN_IF_ERROR(_chunk_reader->load_page_nested_rows(
                    *repetition_levels, rows - *rows_read, &loaded_rows, &crosses_page));
            RETURN_IF_ERROR(_chunk_reader->fill_def(*definition_levels));
            RETURN_IF_ERROR(_chunk_reader->skip_nested_values(*definition_levels, start_level));
            while (crosses_page) {
                const size_t continuation_start = definition_levels->size();
                RETURN_IF_ERROR(_chunk_reader->load_cross_page_nested_row(*repetition_levels,
                                                                          &crosses_page));
                RETURN_IF_ERROR(_chunk_reader->fill_def(*definition_levels));
                RETURN_IF_ERROR(
                        _chunk_reader->skip_nested_values(*definition_levels, continuation_start));
            }
            if (loaded_rows == 0) {
                return Status::Corruption("Parquet nested level reader made no progress");
            }
            *rows_read += loaded_rows;
        }
        return Status::OK();
    }

    io::FileReaderSPtr _file;
    tparquet::ColumnChunk _column_chunk;
    FieldSchema* _field = nullptr;
    size_t _total_rows = 0;
    size_t _max_buffer_size = 0;
    io::IOContext* _io_ctx = nullptr;
    bool _enable_page_cache = false;
    size_t _current_row = 0;
    std::unique_ptr<io::BufferedFileStreamReader> _stream;
    std::unique_ptr<ColumnChunkReader<IN_COLLECTION, false>> _chunk_reader;
};

Status LevelReader::create(io::FileReaderSPtr file, tparquet::ColumnChunk column_chunk,
                           FieldSchema* field, size_t total_rows, size_t max_buffer_size,
                           io::IOContext* io_ctx, bool enable_page_cache,
                           std::unique_ptr<LevelReader>* reader) {
    DORIS_CHECK(reader != nullptr);
    DORIS_CHECK(field != nullptr);
    std::unique_ptr<Impl> impl;
    if (field->repetition_level > 0) {
        impl = std::make_unique<LevelReaderImpl<true>>(std::move(file), std::move(column_chunk),
                                                       field, total_rows, max_buffer_size, io_ctx,
                                                       enable_page_cache);
    } else {
        impl = std::make_unique<LevelReaderImpl<false>>(std::move(file), std::move(column_chunk),
                                                        field, total_rows, max_buffer_size, io_ctx,
                                                        enable_page_cache);
    }
    RETURN_IF_ERROR(impl->init());
    reader->reset(new LevelReader(std::move(impl)));
    return Status::OK();
}

LevelReader::LevelReader(std::unique_ptr<Impl> impl) : _impl(std::move(impl)) {}

LevelReader::~LevelReader() = default;

Status LevelReader::read_rows(size_t rows, std::vector<int16_t>* repetition_levels,
                              std::vector<int16_t>* definition_levels, size_t* rows_read) {
    return _impl->read_rows(rows, repetition_levels, definition_levels, rows_read);
}

Status LevelReader::skip_rows(size_t rows) {
    size_t rows_read = 0;
    RETURN_IF_ERROR(
            read_rows(rows, &_skip_repetition_levels, &_skip_definition_levels, &rows_read));
    if (rows_read != rows) {
        return Status::Corruption("Parquet level reader skipped {} of {} rows", rows_read, rows);
    }
    return Status::OK();
}

ColumnChunkReaderStatistics LevelReader::statistics() {
    return _impl->statistics();
}

} // namespace doris::format::parquet::native
