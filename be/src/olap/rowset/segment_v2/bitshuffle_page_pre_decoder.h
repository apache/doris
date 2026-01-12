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
#include "olap/rowset/segment_v2/bitshuffle_page.h"
#include "olap/rowset/segment_v2/encoding_info.h"

namespace doris {
namespace segment_v2 {

/**
 * @brief Pre-decoder for BitShuffle encoded pages
 *
 * This decoder handles pure bitshuffle + lz4 compressed data without any dict page header.
 * For bitshuffle data within BinaryDictPage, use BinaryDictPagePreDecoder instead.
 */
struct BitShufflePagePreDecoder : public DataPagePreDecoder {
    /**
     * @brief Decode bitshuffle data
     *
     * The input should be data encoded by bitshuffle + lz4.
     * This decoder does NOT handle BinaryDictPage headers - use BinaryDictPagePreDecoder for that.
     *
     * @param page unique_ptr to hold page data, will be replaced by decoded data
     * @param page_slice data to decode, will be updated to point to decoded data
     * @param size_of_tail including size of footer and null map
     * @param _use_cache whether to use page cache
     * @param page_type the type of page
     * @param file_path file path for error reporting
     * @param size_of_prefix size of prefix space to reserve (for dict page header)
     * @return Status
     */
    Status decode(std::unique_ptr<DataPage>* page, Slice* page_slice, size_t size_of_tail,
                  bool _use_cache, segment_v2::PageTypePB page_type, const std::string& file_path,
                  size_t size_of_prefix = 0) override {
        size_t num_elements, compressed_size, num_element_after_padding;
        int size_of_element;

        Slice data(page_slice->data, page_slice->size - size_of_tail);

        RETURN_IF_ERROR(parse_bit_shuffle_header(data, num_elements, compressed_size,
                                                 num_element_after_padding, size_of_element));

        if (compressed_size != data.size) {
            return Status::InternalError(
                    "Size information unmatched in file: {}, compressed_size:{}, "
                    "num_elements:{}, data size:{}",
                    file_path, compressed_size, num_elements, data.size);
        }

        Slice decoded_slice;
        decoded_slice.size = size_of_prefix + BITSHUFFLE_PAGE_HEADER_SIZE +
                             num_element_after_padding * size_of_element + size_of_tail;
        std::unique_ptr<DataPage> decoded_page =
                std::make_unique<DataPage>(decoded_slice.size, _use_cache, page_type);
        decoded_slice.data = decoded_page->data();

        // Copy bitshuffle header to the position after prefix
        memcpy(decoded_slice.data + size_of_prefix, data.data, BITSHUFFLE_PAGE_HEADER_SIZE);

        // Decompress data to the position after prefix and header
        auto bytes = bitshuffle::decompress_lz4(
                &data.data[BITSHUFFLE_PAGE_HEADER_SIZE],
                decoded_slice.data + size_of_prefix + BITSHUFFLE_PAGE_HEADER_SIZE,
                num_element_after_padding, size_of_element, 0);
        if (bytes < 0) [[unlikely]] {
            // Ideally, this should not happen.
            warn_with_bitshuffle_error(bytes);
            return Status::RuntimeError("Unshuffle Process failed in file: {}", file_path);
        }

        // Copy tail to the end
        memcpy(decoded_slice.data + decoded_slice.size - size_of_tail,
               page_slice->data + page_slice->size - size_of_tail, size_of_tail);

        *page_slice = decoded_slice;
        *page = std::move(decoded_page);
        return Status::OK();
    }
};

} // namespace segment_v2
} // namespace doris
