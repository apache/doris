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

#include "common/cast_set.h"
#include "common/status.h"
#include "util/block_compression.h"
#include "vec/exec/format/parquet/parquet_common.h"
namespace doris {
    class BlockCompressionCodec;

    namespace io {
        class BufferedStreamReader;
        struct IOContext;
    } // namespace io

} // namespace doris

namespace doris {
namespace io {
class BufferedStreamReader;
struct IOContext;
} // namespace io
struct Slice;
} // namespace doris

namespace doris::vectorized {
#include "common/compile_check_begin.h"
/**
 * Use to deserialize parquet page header, and get the page data in iterator interface.
 */

template<bool IN_COLLECTION, bool OFFSET_INDEX>
class PageReader {
public:
    struct Statistics {
        int64_t decode_header_time = 0;
        int64_t skip_page_header_num = 0;
        int64_t parse_page_header_num = 0;
    };

    PageReader(io::BufferedStreamReader* reader, io::IOContext* io_ctx, uint64_t offset,
               uint64_t length, size_t total_rows,
               const tparquet::OffsetIndex* offset_index = nullptr);
    ~PageReader() = default;


    bool has_next_page() const {
        if constexpr (OFFSET_INDEX) {
            return _page_index + 1 != _offset_index->page_locations.size();
        } else {
            // Deprecated
            // Parquet file may not be standardized,
            // _end_offset may exceed the actual data area.
            // ColumnChunkReader::has_next_page() use the number of parsed values for judgment
            // ref:https://github.com/duckdb/duckdb/issues/10829
            // [[deprecated]]
            LOG(FATAL) << "has_next_page should not be called when no offset index";
            return _offset < _end_offset;
        }
    }

    Status parse_page_header();

    Status next_page() {
        _statistics.skip_page_header_num += _state == INITIALIZED;
        if constexpr (OFFSET_INDEX) {
            _page_index++;
            _start_row = _offset_index->page_locations[_page_index].first_row_index;
            if (_page_index + 1 < _offset_index->page_locations.size()) {
                _end_row = _offset_index->page_locations[_page_index+1].first_row_index;
            } else {
                _end_row = _total_rows;
            }
            int64_t next_page_offset = _offset_index->page_locations[_page_index].offset;
            _offset = next_page_offset;
            _next_header_offset = next_page_offset;
            _state = INITIALIZED;
        } else {
            if(UNLIKELY(_offset == _start_offset)) {
                return Status::Corruption("should parse first page.");
            }

            if (is_header_v2()) {
                _start_row += _cur_page_header.data_page_header_v2.num_rows;
            } else if constexpr (!IN_COLLECTION) {
                _start_row += _cur_page_header.data_page_header.num_values;
            }

            _offset =_next_header_offset;
            _state = INITIALIZED;
        }

        return Status::OK();
    }

    Status dict_next_page() {
        if constexpr (OFFSET_INDEX) {
            _state = INITIALIZED;
            return Status::OK();
        } else  {
            return next_page();
        }
    }

    Status get_page_header(const tparquet::PageHeader*& page_header) {
        if (UNLIKELY(_state != HEADER_PARSED)) {
            return Status::InternalError("Page header not parsed");
        }
        page_header = &_cur_page_header;
        return Status::OK();
    }

    Status get_page_data(Slice& slice);

    Statistics& statistics() { return _statistics; }

    bool is_header_v2() {
        return _cur_page_header.__isset.data_page_header_v2;
    }

    size_t start_row() const {
        return _start_row;
    }

    size_t end_row() const {
        return _end_row;
    }

private:
    enum PageReaderState { INITIALIZED, HEADER_PARSED, DATA_LOADED};
    PageReaderState _state = INITIALIZED;
    Statistics _statistics;

    io::BufferedStreamReader* _reader = nullptr;
    io::IOContext* _io_ctx = nullptr;
    // current reader offset in file location.
    uint64_t _offset = 0;
    // this page offset in file location.
    uint64_t _start_offset = 0;
    uint64_t _end_offset = 0;
    uint64_t _next_header_offset = 0;
    // current page row range
    size_t _start_row = 0;
    size_t _end_row = 0;
    // total rows in this column chunk
    size_t _total_rows = 0;
    // for page index
    size_t _page_index = 0;
    const tparquet::OffsetIndex* _offset_index;

    tparquet::PageHeader _cur_page_header;
};
//
//template<bool IN_COLLECTION>
//class PageReaderWithOffsetIndex : public PageReader<IN_COLLECTION> {
//public:
//    PageReaderWithOffsetIndex(io::BufferedStreamReader* reader, io::IOContext* io_ctx,
//                              uint64_t offset, uint64_t length, size_t total_rows, int64_t num_values,
//                              const tparquet::OffsetIndex* offset_index)
//            : PageReader<IN_COLLECTION>(reader, io_ctx, offset, length),
//              _num_values(num_values),
//              _total_rows(total_rows),
//              _offset_index(offset_index) {
//        _end_row = _offset_index->page_locations.size() >= 2 ?
//                _offset_index->page_locations[1].first_row_index : _total_rows;
//    }
//
////    Status next_page_header() override {
////        // lazy to parse page header in get_page_header
////        return Status::OK();
////    }
//
//    bool has_next_page() const override {
//        return _page_index + 1 != _offset_index->page_locations.size();
//    }
//
//    Status next_page() override {
//        _page_index++;
//        _start_row = _offset_index->page_locations[_page_index].first_row_index;
//        if (_page_index + 1 < _offset_index->page_locations.size()) {
//            _end_row = _offset_index->page_locations[_page_index+1].first_row_index;
//        } else {
//            _end_row = _total_rows;
//        }
//        int64_t next_page_offset = _offset_index->page_locations[_page_index].offset;
//        _offset = next_page_offset;
//        _next_header_offset = next_page_offset;
//        _state = INITIALIZED;
//        return Status::OK();
//    }
//
//    Status dict_next_page() override {
//        _state = INITIALIZED;
//        return Status::OK();
//    }
//
//
//
//    Status get_page_header(const tparquet::PageHeader*& page_header) override {
//        if (_state != HEADER_PARSED) {
//            RETURN_IF_ERROR(_parse_page_header());
//        }
//        page_header = &_cur_page_header;
//        return Status::OK();
//    }
//
////    Status get_num_values(uint32_t& num_values) override {
////        if (UNLIKELY(_page_index >= _offset_index->page_locations.size())) {
////            return Status::IOError("End of page");
////        }
////
////        if (_page_index < _offset_index->page_locations.size() - 1) {
////            num_values = cast_set<uint32_t>(
////                    _offset_index->page_locations[_page_index + 1].first_row_index -
////                    _offset_index->page_locations[_page_index].first_row_index);
////        } else {
////            num_values = cast_set<uint32_t>(
////                    _num_values - _offset_index->page_locations[_page_index].first_row_index);
////        }
////        return Status::OK();
////    }
//
////    size_t _this_page_start_row = 0;
////    size_t _next_page_start_row = 0;
////    size_t next_page_start_row() override {
////        return _next_page_start_row;
////    }
////    size_t this_page_start_row() override {
////        return _this_page_start_row;
////    }
////    Status skip_page() override {
////        if (UNLIKELY(_page_index >= _offset_index->page_locations.size())) {
////            return Status::IOError("End of page");
////        }
////
////        if (_state != HEADER_PARSED) {
////            _statistics.skip_page_header_num++;
////        }
////
////        seek_to_page(_offset_index->page_locations[_page_index].offset +
////                     _offset_index->page_locations[_page_index].compressed_page_size);
////        _page_index++;
////        return Status::OK();
////    }
//
//    Status get_page_data(Slice& slice) override {
//        if (_page_index >= _offset_index->page_locations.size()) {
//            return Status::IOError("End of page");
//        }
//        if (_state != HEADER_PARSED) {
//            RETURN_IF_ERROR(parse_page_header());
//        }
//
////        // dirctionary page is not in page location
////        if (LIKELY(_cur_page_header.type != tparquet::PageType::DICTIONARY_PAGE)) {
////            _page_index++;
////        }
////        if (LIKELY(_cur_page_header.type == tparquet::PageType::DICTIONARY_PAGE)) {
////            _page_index = 0;
////        }
//
//        return PageReader<IN_COLLECTION>::get_page_data(slice);
//    }
//
//private:
//    size_t _page_index = 0;
//    int64_t _num_values = 0;
//    size_t _total_rows = 0;
//    const tparquet::OffsetIndex* _offset_index;
//};

template<bool IN_COLLECTION, bool OFFSET_INDEX>
std::unique_ptr<PageReader<IN_COLLECTION, OFFSET_INDEX>> create_page_reader(io::BufferedStreamReader* reader,
                                               io::IOContext* io_ctx, uint64_t offset,
                                               uint64_t length,
                                               size_t total_rows,
                                               const tparquet::OffsetIndex* offset_index = nullptr) {
    return std::make_unique<PageReader<IN_COLLECTION, OFFSET_INDEX>>(
            reader, io_ctx, offset, length, total_rows, offset_index);

}
#include "common/compile_check_end.h"

} // namespace doris::vectorized
