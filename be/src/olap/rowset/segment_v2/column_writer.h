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

#include "common/status.h"         // for Status
#include "gen_cpp/segment_v2.pb.h" // for EncodingTypePB
#include "olap/rowset/segment_v2/common.h"
#include "olap/rowset/segment_v2/page_pointer.h" // for PagePointer
#include "olap/tablet_schema.h"                  // for TabletColumn
#include "util/bitmap.h"                         // for BitmapChange
#include "util/slice.h"                          // for OwnedSlice

namespace doris {

class TypeInfo;
class BlockCompressionCodec;

namespace fs {
class WritableBlock;
}

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
    std::string to_string() {
        std::stringstream ss;
        ss << std::boolalpha << "meta=" << meta->DebugString()
           << ", data_page_size=" << data_page_size
           << ", compression_min_space_saving = " << compression_min_space_saving
           << ", need_zone_map=" << need_zone_map << ", need_bitmap_index=" << need_bitmap_index
           << ", need_bloom_filter" << need_bloom_filter;
        return ss.str();
    }
};

class BitmapIndexWriter;
class EncodingInfo;
class NullBitmapBuilder;
class OrdinalIndexWriter;
class PageBuilder;
class BloomFilterIndexWriter;
class ZoneMapIndexWriter;

class ColumnWriter {
public:
    static Status create(const ColumnWriterOptions& opts, const TabletColumn* column,
                         fs::WritableBlock* _wblock, std::unique_ptr<ColumnWriter>* writer);

    explicit ColumnWriter(std::unique_ptr<Field> field, bool is_nullable)
            : _field(std::move(field)), _is_nullable(is_nullable) {}

    virtual ~ColumnWriter() = default;

    virtual Status init() = 0;

    template <typename CellType>
    Status append(const CellType& cell) {
        if (_is_nullable) {
            uint8_t nullmap = 0;
            BitmapChange(&nullmap, 0, cell.is_null());
            return append_nullable(&nullmap, cell.cell_ptr(), 1);
        } else {
            auto* cel_ptr = cell.cell_ptr();
            return append_data((const uint8_t**)&cel_ptr, 1);
        }
    }

    // Now we only support append one by one, we should support append
    // multi rows in one call
    Status append(bool is_null, void* data) {
        uint8_t nullmap = 0;
        BitmapChange(&nullmap, 0, is_null);
        return append_nullable(&nullmap, data, 1);
    }

    Status append(const uint8_t* nullmap, const void* data, size_t num_rows);

    Status append_nullable(const uint8_t* nullmap, const void* data, size_t num_rows);

    virtual Status append_nulls(size_t num_rows) = 0;

    virtual Status finish_current_page() = 0;

    virtual uint64_t estimate_buffer_size() = 0;

    // finish append data
    virtual Status finish() = 0;

    // write all data into file
    virtual Status write_data() = 0;

    virtual Status write_ordinal_index() = 0;

    virtual Status write_zone_map() = 0;

    virtual Status write_bitmap_index() = 0;

    virtual Status write_bloom_filter_index() = 0;

    virtual ordinal_t get_next_rowid() const = 0;

    // used for append not null data.
    virtual Status append_data(const uint8_t** ptr, size_t num_rows) = 0;

    // used for append not null data. When page is full, will append data not reach num_rows.
    virtual Status append_data_in_current_page(const uint8_t** ptr, size_t* num_rows) = 0;

    // used for append not null data. When page is full, will append data not reach num_rows.
    virtual Status append_data_in_current_page(const uint8_t* ptr, size_t* num_rows) = 0;

    bool is_nullable() const { return _is_nullable; }

    Field* get_field() const { return _field.get(); }

private:
    std::unique_ptr<Field> _field;
    bool _is_nullable;
    std::vector<uint8_t> _null_bitmap;

protected:
    std::shared_ptr<MemTracker> _mem_tracker;
};

class FlushPageCallback {
public:
    virtual Status put_extra_info_in_page(DataPageFooterPB* footer) { return Status::OK(); }
};

// Encode one column's data into some memory slice.
// Because some columns would be stored in a file, we should wait
// until all columns has been finished, and then data can be written
// to file
class ScalarColumnWriter final : public ColumnWriter {
public:
    ScalarColumnWriter(const ColumnWriterOptions& opts, std::unique_ptr<Field> field,
                       fs::WritableBlock* output_file);

    ~ScalarColumnWriter() override;

    Status init() override;

    Status append_nulls(size_t num_rows) override;

    Status finish_current_page() override;

    uint64_t estimate_buffer_size() override;

    // finish append data
    Status finish() override;

    Status write_data() override;
    Status write_ordinal_index() override;
    Status write_zone_map() override;
    Status write_bitmap_index() override;
    Status write_bloom_filter_index() override;
    ordinal_t get_next_rowid() const override { return _next_rowid; }

    void register_flush_page_callback(FlushPageCallback* flush_page_callback) {
        _new_page_callback = flush_page_callback;
    }
    Status append_data(const uint8_t** ptr, size_t num_rows) override;

    Status append_data_in_current_page(const uint8_t** ptr, size_t* num_rows) override;
    Status append_data_in_current_page(const uint8_t* ptr, size_t* num_rows) override;

private:
    std::unique_ptr<PageBuilder> _page_builder;

    std::unique_ptr<NullBitmapBuilder> _null_bitmap_builder;

    ColumnWriterOptions _opts;

    const EncodingInfo* _encoding_info = nullptr;

    ordinal_t _next_rowid = 0;

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

    Status _write_data_page(Page* page);

private:
    fs::WritableBlock* _wblock = nullptr;
    // total size of data page list
    uint64_t _data_size;

    // cached generated pages,
    PageHead _pages;
    ordinal_t _first_rowid = 0;

    const BlockCompressionCodec* _compress_codec = nullptr;

    std::unique_ptr<OrdinalIndexWriter> _ordinal_index_builder;
    std::unique_ptr<ZoneMapIndexWriter> _zone_map_index_builder;
    std::unique_ptr<BitmapIndexWriter> _bitmap_index_builder;
    std::unique_ptr<BloomFilterIndexWriter> _bloom_filter_index_builder;

    // call before flush data page.
    FlushPageCallback* _new_page_callback = nullptr;
};

class ArrayColumnWriter final : public ColumnWriter, public FlushPageCallback {
public:
    explicit ArrayColumnWriter(const ColumnWriterOptions& opts, std::unique_ptr<Field> field,
                               ScalarColumnWriter* offset_writer, ScalarColumnWriter* null_writer,
                               std::unique_ptr<ColumnWriter> item_writer);
    ~ArrayColumnWriter() override = default;

    Status init() override;

    Status append_data(const uint8_t** ptr, size_t num_rows) override;
    Status append_data_in_current_page(const uint8_t** ptr, size_t* num_rows) override {
        return Status::NotSupported(
                "array writer has no data, can not append_data_in_current_page");
    }
    Status append_data_in_current_page(const uint8_t* ptr, size_t* num_rows) override {
        return Status::NotSupported(
                "array writer has no data, can not append_data_in_current_page");
    }

    uint64_t estimate_buffer_size() override;

    Status finish() override;
    Status write_data() override;
    Status write_ordinal_index() override;
    Status append_nulls(size_t num_rows) override;

    Status finish_current_page() override;

    Status write_zone_map() override {
        if (_opts.need_zone_map) {
            return Status::NotSupported("array not support zone map");
        }
        return Status::OK();
    }

    Status write_bitmap_index() override {
        if (_opts.need_bitmap_index) {
            return Status::NotSupported("array not support bitmap index");
        }
        return Status::OK();
    }
    Status write_bloom_filter_index() override {
        if (_opts.need_bloom_filter) {
            return Status::NotSupported("array not support bloom filter index");
        }
        return Status::OK();
    }
    ordinal_t get_next_rowid() const override { return _length_writer->get_next_rowid(); }

private:
    Status put_extra_info_in_page(DataPageFooterPB* header) override;
    Status write_null_column(size_t num_rows, bool is_null); // 写入num_rows个null标记
    bool has_empty_items() const { return _item_writer->get_next_rowid() == 0; }

private:
    std::unique_ptr<ScalarColumnWriter> _length_writer;
    std::unique_ptr<ScalarColumnWriter> _null_writer;
    std::unique_ptr<ColumnWriter> _item_writer;
    ColumnWriterOptions _opts;
    ordinal_t _current_length_page_first_ordinal = 0;
    ordinal_t _length_sum_in_cur_page = 0;
};

} // namespace segment_v2
} // namespace doris
