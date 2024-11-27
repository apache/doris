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
#include <stdint.h>

#include <memory>

#include "common/status.h"

namespace doris {
namespace io {
class BufferedStreamReader;
struct IOContext;
} // namespace io
struct Slice;
} // namespace doris

namespace doris::vectorized {

/**
 * Use to deserialize parquet page header, and get the page data in iterator interface.
 */
class PageReader {
public:
    struct Statistics {
        int64_t decode_header_time = 0;
        int64_t skip_page_header_num = 0;
        int64_t parse_page_header_num = 0;
    };

    PageReader(io::BufferedStreamReader* reader, io::IOContext* io_ctx, uint64_t offset,
               uint64_t length);
    virtual ~PageReader() = default;

    // Deprecated
    // Parquet file may not be standardized,
    // _end_offset may exceed the actual data area.
    // ColumnChunkReader::has_next_page() use the number of parsed values for judgment
    // [[deprecated]]
    bool has_next_page() const { return _offset < _end_offset; }

    virtual Status next_page_header() { return _parse_page_header(); }

    virtual Status get_page_header(const tparquet::PageHeader*& page_header) {
        if (UNLIKELY(_state != HEADER_PARSED)) {
            return Status::InternalError("Page header not parsed");
        }
        page_header = &_cur_page_header;
        return Status::OK();
    }

    virtual Status get_num_values(uint32_t& num_values) {
        if (_state != HEADER_PARSED) {
            return Status::InternalError("Page header not parsed");
        }
        if (_cur_page_header.type == tparquet::PageType::DATA_PAGE_V2) {
            num_values = _cur_page_header.data_page_header_v2.num_values;
        } else {
            num_values = _cur_page_header.data_page_header.num_values;
        }
        return Status::OK();
    }

    virtual Status skip_page();

    virtual Status get_page_data(Slice& slice);

    Statistics& statistics() { return _statistics; }

    void seek_to_page(int64_t page_header_offset) {
        _offset = page_header_offset;
        _next_header_offset = page_header_offset;
        _state = INITIALIZED;
    }

protected:
    enum PageReaderState { INITIALIZED, HEADER_PARSED };
    PageReaderState _state = INITIALIZED;
    tparquet::PageHeader _cur_page_header;
    Statistics _statistics;

    Status _parse_page_header();

private:
    io::BufferedStreamReader* _reader = nullptr;
    io::IOContext* _io_ctx = nullptr;
    uint64_t _offset = 0;
    uint64_t _next_header_offset = 0;
    uint64_t _start_offset = 0;
    uint64_t _end_offset = 0;
};

class PageReaderWithOffsetIndex : public PageReader {
public:
    PageReaderWithOffsetIndex(io::BufferedStreamReader* reader, io::IOContext* io_ctx,
                              uint64_t offset, uint64_t length, int64_t num_values,
                              const tparquet::OffsetIndex* offset_index)
            : PageReader(reader, io_ctx, offset, length),
              _num_values(num_values),
              _offset_index(offset_index) {}

    Status next_page_header() override {
        // lazy to parse page header in get_page_header
        return Status::OK();
    }

    Status get_page_header(const tparquet::PageHeader*& page_header) override {
        if (_state != HEADER_PARSED) {
            RETURN_IF_ERROR(_parse_page_header());
        }
        page_header = &_cur_page_header;
        return Status::OK();
    }

    Status get_num_values(uint32_t& num_values) override {
        if (UNLIKELY(_page_index >= _offset_index->page_locations.size())) {
            return Status::IOError("End of page");
        }

        if (_page_index < _offset_index->page_locations.size() - 1) {
            num_values = _offset_index->page_locations[_page_index + 1].first_row_index -
                         _offset_index->page_locations[_page_index].first_row_index;
        } else {
            num_values = _num_values - _offset_index->page_locations[_page_index].first_row_index;
        }
        return Status::OK();
    }

    Status skip_page() override {
        if (UNLIKELY(_page_index >= _offset_index->page_locations.size())) {
            return Status::IOError("End of page");
        }

        if (_state != HEADER_PARSED) {
            _statistics.skip_page_header_num++;
        }

        seek_to_page(_offset_index->page_locations[_page_index].offset +
                     _offset_index->page_locations[_page_index].compressed_page_size);
        _page_index++;
        return Status::OK();
    }

    Status get_page_data(Slice& slice) override {
        if (_page_index >= _offset_index->page_locations.size()) {
            return Status::IOError("End of page");
        }
        if (_state != HEADER_PARSED) {
            RETURN_IF_ERROR(_parse_page_header());
        }

        // dirctionary page is not in page location
        if (LIKELY(_cur_page_header.type != tparquet::PageType::DICTIONARY_PAGE)) {
            _page_index++;
        }

        return PageReader::get_page_data(slice);
    }

private:
    size_t _page_index = 0;
    int64_t _num_values = 0;
    const tparquet::OffsetIndex* _offset_index;
};

std::unique_ptr<PageReader> create_page_reader(io::BufferedStreamReader* reader,
                                               io::IOContext* io_ctx, uint64_t offset,
                                               uint64_t length, int64_t num_values = 0,
                                               const tparquet::OffsetIndex* offset_index = nullptr);

} // namespace doris::vectorized
