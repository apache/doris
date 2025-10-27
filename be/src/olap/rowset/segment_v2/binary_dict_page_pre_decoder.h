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

#include "olap/page_cache.h"
#include "olap/rowset/segment_v2/binary_dict_page.h"
#include "olap/rowset/segment_v2/binary_plain_page_v2_pre_decoder.h"
#include "olap/rowset/segment_v2/bitshuffle_page_pre_decoder.h"
#include "olap/rowset/segment_v2/encoding_info.h"
#include "util/coding.h"

namespace doris {
namespace segment_v2 {

/**
 * @brief Pre-decoder for BinaryDictPage data pages
 *
 * BinaryDictPage data pages can have different encoding types:
 * 1. DICT_ENCODING: header(4 bytes) + bitshuffle encoded codeword page
 * 2. PLAIN_ENCODING_V2: header(4 bytes) + BinaryPlainPageV2 encoded data
 * 3. PLAIN_ENCODING: header(4 bytes) + BinaryPlainPage encoded data (no pre-decode needed)
 *
 * This pre-decoder reads the encoding type from the first 4 bytes, strips the header,
 * dispatches to the appropriate pre-decoder (BitShufflePagePreDecoder or
 * BinaryPlainPageV2PreDecoder), and then restores the header.
 */
struct BinaryDictPagePreDecoder : public DataPagePreDecoder {
    /**
     * @brief Decode BinaryDictPage data page
     *
     * @param page unique_ptr to hold page data, may be replaced by decoded data
     * @param page_slice data to decode, will be updated if decoding happens
     * @param size_of_tail including size of footer and null map
     * @param _use_cache whether to use page cache
     * @param page_type the type of page
     * @param file_path file path for error reporting
     * @return Status
     */
    Status decode(std::unique_ptr<DataPage>* page, Slice* page_slice, size_t size_of_tail,
                  bool _use_cache, segment_v2::PageTypePB page_type,
                  const std::string& file_path) override {
        // Validate minimum size (at least 4 bytes for encoding type)
        if (page_slice->size < BINARY_DICT_PAGE_HEADER_SIZE) {
            return Status::Corruption(
                    "Invalid BinaryDictPage size: {}, expected at least {} in file: {}",
                    page_slice->size, BINARY_DICT_PAGE_HEADER_SIZE, file_path);
        }

        // Read encoding type from first 4 bytes
        auto encoding_type =
                static_cast<EncodingTypePB>(decode_fixed32_le((const uint8_t*)page_slice->data));

        // For PLAIN_ENCODING, no pre-decoding needed
        if (encoding_type == PLAIN_ENCODING) {
            return Status::OK();
        }

        // For other encoding types, we need to:
        // 1. Strip the 4-byte header
        // 2. Apply the appropriate pre-decoder
        // 3. Restore the 4-byte header in the decoded result

        Slice data_without_header(page_slice->data + BINARY_DICT_PAGE_HEADER_SIZE,
                                  page_slice->size - BINARY_DICT_PAGE_HEADER_SIZE);

        std::unique_ptr<DataPage> decoded_page_inner;
        Status status;

        switch (encoding_type) {
        case DICT_ENCODING: {
            // Use BitShufflePagePreDecoder (without USED_IN_DICT_ENCODING)
            BitShufflePagePreDecoder bitshuffle_decoder;
            status = bitshuffle_decoder.decode(&decoded_page_inner, &data_without_header,
                                               size_of_tail, _use_cache, page_type, file_path);
            break;
        }
        case PLAIN_ENCODING_V2: {
            // Use BinaryPlainPageV2PreDecoder
            BinaryPlainPageV2PreDecoder v2_decoder;
            status = v2_decoder.decode(&decoded_page_inner, &data_without_header, size_of_tail,
                                       _use_cache, page_type, file_path);
            break;
        }
        default:
            // Unknown encoding type, no pre-decoding needed
            return Status::OK();
        }

        RETURN_IF_ERROR(status);

        // Allocate new page with space for 4-byte header
        Slice final_slice;
        final_slice.size = BINARY_DICT_PAGE_HEADER_SIZE + data_without_header.size;
        std::unique_ptr<DataPage> final_page =
                std::make_unique<DataPage>(final_slice.size, _use_cache, page_type);
        final_slice.data = final_page->data();

        // Copy header
        memcpy(final_slice.data, page_slice->data, BINARY_DICT_PAGE_HEADER_SIZE);

        // Copy decoded data
        memcpy(final_slice.data + BINARY_DICT_PAGE_HEADER_SIZE, data_without_header.data,
               data_without_header.size);

        *page_slice = final_slice;
        *page = std::move(final_page);

        return Status::OK();
    }
};

} // namespace segment_v2
} // namespace doris
