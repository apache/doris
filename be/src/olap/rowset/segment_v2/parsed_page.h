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
#include "olap/rowset/segment_v2/binary_dict_page.h"
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
                         uint32_t page_index, ParsedPage* result,
                         PageDecoderOptions opts = PageDecoderOptions()) {
        result->~ParsedPage();
        ParsedPage* page = new (result)(ParsedPage);
        page->page_handle = std::move(handle);

        auto null_size = footer.nullmap_size();
        page->has_null = null_size > 0;
        page->null_bitmap = Slice(body.data + body.size - null_size, null_size);

        if (page->has_null) {
            page->null_decoder =
                    RleDecoder<bool>((const uint8_t*)page->null_bitmap.data, null_size, 1);
        }

        Slice data_slice(body.data, body.size - null_size);
        RETURN_IF_ERROR(encoding->create_page_decoder(data_slice, opts, &page->data_decoder));
        RETURN_IF_ERROR(page->data_decoder->init());

        if (encoding->encoding() == DICT_ENCODING) {
            auto dict_decoder = static_cast<BinaryDictPageDecoder*>(page->data_decoder);
            page->is_dict_encoding = dict_decoder->is_dict_encoding();
        }

        page->first_ordinal = footer.first_ordinal();
        page->num_rows = footer.num_values();

        page->page_pointer = page_pointer;
        page->page_index = page_index;

        page->next_array_item_ordinal = footer.next_array_item_ordinal();

        return Status::OK();
    }

    ~ParsedPage() {
        delete data_decoder;
        data_decoder = nullptr;
    }

    PageHandle page_handle;

    bool has_null;
    Slice null_bitmap;
    RleDecoder<bool> null_decoder;
    PageDecoder* data_decoder = nullptr;

    // ordinal of the first value in this page
    ordinal_t first_ordinal = 0;
    // number of rows including nulls and not-nulls
    ordinal_t num_rows = 0;
    // record it to get the last array element's size
    // should be none zero if set in page
    ordinal_t next_array_item_ordinal = 0;

    PagePointer page_pointer;
    uint32_t page_index = 0;

    // current offset when read this page
    // this means next row we will read
    ordinal_t offset_in_page = 0;

    bool is_dict_encoding = false;

    bool contains(ordinal_t ord) {
        return ord >= first_ordinal && ord < (first_ordinal + num_rows);
    }

    operator bool() const { return data_decoder != nullptr; }

    bool has_remaining() const { return offset_in_page < num_rows; }

    size_t remaining() const { return num_rows - offset_in_page; }
};

} // namespace segment_v2
} // namespace doris
