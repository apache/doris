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

#include <memory> // for unique_ptr

#include "common/status.h" // for Status
#include "gen_cpp/segment_v2.pb.h" // for EncodingTypePB
#include "olap/rowset/segment_v2/common.h"
#include "olap/rowset/segment_v2/page_pointer.h" // for PagePointer
#include "util/bitmap.h" // for BitmapChange
#include "util/slice.h" // for OwnedSlice

namespace doris {

class TypeInfo;
class WritableFile;
class BlockCompressionCodec;

namespace segment_v2 {

struct ColumnWriterOptions {
    // input and output parameter:
    // - input: column_id/unique_id/type/length/encoding/compression/is_nullable members
    // - output: encoding/indexes/dict_page members
    ColumnMetaPB* meta;
    size_t data_page_size = 64 * 1024;
    // store compressed page only when space saving is above the threshold.
    // space saving = 1 - compressed_size / uncompressed_size
    double compression_min_space_saving = 0.1;
    bool need_zone_map = false;
    bool need_bitmap_index = false;
    bool need_bloom_filter = false;
};

class BitmapIndexWriter;
class EncodingInfo;
class NullBitmapBuilder;
class OrdinalIndexWriter;
class PageBuilder;
class BloomFilterIndexWriter;
class ZoneMapIndexWriter;

// Encode one column's data into some memory slice.
// Because some columns would be stored in a file, we should wait
// until all columns has been finished, and then data can be written
// to file
class ColumnWriter {
public:
    ColumnWriter(const ColumnWriterOptions& opts,
                 std::unique_ptr<Field> field,
                 WritableFile* output_file);
    ~ColumnWriter();

    Status init();

    template<typename CellType>
    Status append(const CellType& cell) {
        if (_is_nullable) {
            uint8_t nullmap = 0;
            BitmapChange(&nullmap, 0, cell.is_null());
            return append_nullable(&nullmap, cell.cell_ptr(), 1);
        } else {
            return append(cell.cell_ptr(), 1);
        }
    }

    // Now we only support append one by one, we should support append
    // multi rows in one call
    Status append(bool is_null, void* data) {
        uint8_t nullmap = 0;
        BitmapChange(&nullmap, 0, is_null);
        return append_nullable(&nullmap, data, 1);
    }

    Status append_nulls(size_t num_rows);
    Status append(const void* data, size_t num_rows);
    Status append_nullable(const uint8_t* nullmap, const void* data, size_t num_rows);

    uint64_t estimate_buffer_size();

    // finish append data
    Status finish();

    // write all data into file
    Status write_data();
    Status write_ordinal_index();
    Status write_zone_map();
    Status write_bitmap_index();
    Status write_bloom_filter_index();

private:
    // All Pages will be organized into a linked list
    struct Page {
        // the data vector may contain:
        //     1. one OwnedSlice if the page body is compressed
        //     2. one OwnedSlice if the page body is not compressed and doesn't have nullmap
        //     3. two OwnedSlice if the page body is not compressed and has nullmap
        // use vector for easier management for lifetime of OwnedSlice
        std::vector<OwnedSlice> data;
        PageFooterPB footer;
        Page* next = nullptr;
    };

    struct PageHead {
        Page* head = nullptr;
        Page* tail = nullptr;
    };

    void _push_back_page(Page* page) {
        // add page to pages' tail
        if (_pages.tail != nullptr) {
            _pages.tail->next = page;
        }
        _pages.tail = page;
        if (_pages.head == nullptr) {
            _pages.head = page;
        }
        for (auto& data_slice : page->data) {
            _data_size += data_slice.slice().size;
        }
        // estimate (page footer + footer size + checksum) took 20 bytes
        _data_size += 20;
    }

    Status _append_data(const uint8_t** ptr, size_t num_rows);
    Status _finish_current_page();
    Status _write_data_page(Page* page);

private:
    ColumnWriterOptions _opts;
    std::unique_ptr<Field> _field;
    WritableFile* _output_file;
    bool _is_nullable;
    // total size of data page list
    uint64_t _data_size;

    // cached generated pages,
    PageHead _pages;
    ordinal_t _first_rowid = 0;
    ordinal_t _next_rowid = 0;

    const EncodingInfo* _encoding_info = nullptr;
    const BlockCompressionCodec* _compress_codec = nullptr;

    std::unique_ptr<PageBuilder> _page_builder;
    std::unique_ptr<NullBitmapBuilder> _null_bitmap_builder;

    std::unique_ptr<OrdinalIndexWriter> _ordinal_index_builder;
    std::unique_ptr<ZoneMapIndexWriter> _zone_map_index_builder;
    std::unique_ptr<BitmapIndexWriter> _bitmap_index_builder;
    std::unique_ptr<BloomFilterIndexWriter> _bloom_filter_index_builder;
};


}
}
