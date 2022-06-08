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

#include <memory>

#include "common/status.h"
#include "gen_cpp/segment_v2.pb.h"
#include "olap/rowset/segment_v2/common.h"
#include "olap/rowset/segment_v2/encoding_info.h"
#include "olap/rowset/segment_v2/options.h"
#include "olap/rowset/segment_v2/page_decoder.h"
#include "olap/rowset/segment_v2/page_handle.h"
#include "util/rle_encoding.h"

namespace doris {
namespace segment_v2 {

// This contains information when one page is loaded, and ready for read
// This struct can be reused, client should call reset first before reusing
// this object
struct ParsedPage {
    static Status create(PageHandle handle, const Slice& body, const DataPageFooterPB& footer,
                         const EncodingInfo* encoding, const PagePointer& page_pointer,
                         uint32_t page_index, ParsedPage* result) {
        result->~ParsedPage();
        ParsedPage* page = new (result)(ParsedPage);
        page->page_handle = std::make_shared<PageHandle>();
        *page->page_handle = std::move(handle);

        auto null_size = footer.nullmap_size();
        page->has_null = null_size > 0;
        page->null_bitmap = Slice(body.data + body.size - null_size, null_size);

        if (page->has_null) {
            page->null_decoder =
                    RleDecoder<bool>((const uint8_t*)page->null_bitmap.data, null_size, 1);
        }

        Slice data_slice(body.data, body.size - null_size);
        PageDecoderOptions opts;
        PageDecoder* decoder;
        RETURN_IF_ERROR(encoding->create_page_decoder(data_slice, opts, &decoder));
        page->data_decoder.reset(decoder);
        RETURN_IF_ERROR(page->data_decoder->init());

        page->first_ordinal = footer.first_ordinal();
        page->num_rows = footer.num_values();

        page->page_pointer = page_pointer;
        page->page_index = page_index;

        page->first_array_item_ordinal = footer.first_array_item_ordinal();

        return Status::OK();
    }

    ParsedPage() = default;

    ParsedPage(const ParsedPage& other) {
        page_handle = other.page_handle;
        has_null = other.has_null;
        null_decoder = other.null_decoder;
        null_bitmap = other.null_bitmap;
        other.data_decoder->clone_for_cache(data_decoder);
        first_ordinal = other.first_ordinal;
        num_rows = other.num_rows;
        first_array_item_ordinal = other.first_array_item_ordinal;
        page_pointer = other.page_pointer;
        page_index = other.page_index;
        offset_in_page = other.offset_in_page;
    }

    ParsedPage& operator=(ParsedPage&& other) {
        this->~ParsedPage();
        new (this) ParsedPage(other);
        return *this;
    }

    ~ParsedPage() { data_decoder = nullptr; }

    size_t size() const { return sizeof(ParsedPage) + page_handle->data().size; }

    std::shared_ptr<PageHandle> page_handle;

    bool has_null;
    Slice null_bitmap;
    RleDecoder<bool> null_decoder;
    std::unique_ptr<PageDecoder> data_decoder = nullptr;

    // ordinal of the first value in this page
    ordinal_t first_ordinal = 0;
    // number of rows including nulls and not-nulls
    ordinal_t num_rows = 0;
    // just for array type
    ordinal_t first_array_item_ordinal = 0;

    PagePointer page_pointer;
    uint32_t page_index = 0;

    // current offset when read this page
    // this means next row we will read
    ordinal_t offset_in_page = 0;

    bool contains(ordinal_t ord) {
        return ord >= first_ordinal && ord < (first_ordinal + num_rows);
    }

    operator bool() const { return data_decoder != nullptr; }

    bool has_remaining() const { return offset_in_page < num_rows; }

    size_t remaining() const { return num_rows - offset_in_page; }
};

} // namespace segment_v2
} // namespace doris
