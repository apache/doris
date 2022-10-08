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

#include "common/status.h"
#include "gen_cpp/parquet_types.h"
#include "io/buffered_reader.h"

namespace doris::vectorized {

/**
 * Use to deserialize parquet page header, and get the page data in iterator interface.
 */
class PageReader {
public:
    struct Statistics {
        int64_t decode_header_time = 0;
    };

    PageReader(BufferedStreamReader* reader, uint64_t offset, uint64_t length);
    ~PageReader() = default;

    bool has_next_page() const { return _offset < _end_offset; }

    Status next_page_header();

    Status skip_page();

    const tparquet::PageHeader* get_page_header() const { return &_cur_page_header; }

    Status get_page_data(Slice& slice);

    Statistics& statistics() { return _statistics; }

    void seek_to_page(int64_t page_header_offset) {
        _offset = page_header_offset;
        _next_header_offset = page_header_offset;
        _state = INITIALIZED;
    }

private:
    enum PageReaderState { INITIALIZED, HEADER_PARSED };

    BufferedStreamReader* _reader;
    tparquet::PageHeader _cur_page_header;
    Statistics _statistics;
    PageReaderState _state = INITIALIZED;

    uint64_t _offset = 0;
    uint64_t _next_header_offset = 0;

    uint64_t _start_offset = 0;
    uint64_t _end_offset = 0;
};

} // namespace doris::vectorized
