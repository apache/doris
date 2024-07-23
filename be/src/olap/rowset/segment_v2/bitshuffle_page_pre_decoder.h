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

template <bool USED_IN_DICT_ENCODING>
struct BitShufflePagePreDecoder : public DataPagePreDecoder {
    /**
     * @brief Decode bitshuffle data
     * The input should be data encoded by bitshuffle + lz4 or
     * the input may be data of BinaryDictPage, if its encoding type is plain,
     * it is no need to decode.
     *
     * @param page unique_ptr to hold page data, maybe be replaced by decoded data
     * @param page_slice data to decode
     * @param size_of_tail including size of footer and null map
     * @return Status
     */
    Status decode(std::unique_ptr<DataPage>* page, Slice* page_slice, size_t size_of_tail,
                  bool _use_cache, segment_v2::PageTypePB page_type) override {
        size_t num_elements, compressed_size, num_element_after_padding;
        int size_of_element;

        size_t size_of_dict_header = 0;
        Slice data(page_slice->data, page_slice->size - size_of_tail);
        if constexpr (USED_IN_DICT_ENCODING) {
            auto type = decode_fixed32_le((const uint8_t*)&data.data[0]);
            if (static_cast<EncodingTypePB>(type) != EncodingTypePB::DICT_ENCODING) {
                return Status::OK();
            }
            size_of_dict_header = BINARY_DICT_PAGE_HEADER_SIZE;
            data.remove_prefix(4);
        }

        RETURN_IF_ERROR(parse_bit_shuffle_header(data, num_elements, compressed_size,
                                                 num_element_after_padding, size_of_element));

        if (compressed_size != data.size) {
            return Status::InternalError(
                    "Size information unmatched, compressed_size:{}, num_elements:{}, data size:{}",
                    compressed_size, num_elements, data.size);
        }

        Slice decoded_slice;
        decoded_slice.size = size_of_dict_header + BITSHUFFLE_PAGE_HEADER_SIZE +
                             num_element_after_padding * size_of_element + size_of_tail;
        std::unique_ptr<DataPage> decoded_page =
                std::make_unique<DataPage>(decoded_slice.size, _use_cache, page_type);
        decoded_slice.data = decoded_page->data();

        if constexpr (USED_IN_DICT_ENCODING) {
            memcpy(decoded_slice.data, page_slice->data, size_of_dict_header);
        }

        memcpy(decoded_slice.data + size_of_dict_header, data.data, BITSHUFFLE_PAGE_HEADER_SIZE);

        auto bytes = bitshuffle::decompress_lz4(
                &data.data[BITSHUFFLE_PAGE_HEADER_SIZE],
                decoded_slice.data + BITSHUFFLE_PAGE_HEADER_SIZE + size_of_dict_header,
                num_element_after_padding, size_of_element, 0);
        if (PREDICT_FALSE(bytes < 0)) {
            // Ideally, this should not happen.
            warn_with_bitshuffle_error(bytes);
            return Status::RuntimeError("Unshuffle Process failed");
        }

        memcpy(decoded_slice.data + decoded_slice.size - size_of_tail,
               page_slice->data + page_slice->size - size_of_tail, size_of_tail);

        *page_slice = decoded_slice;
        *page = std::move(decoded_page);
        return Status::OK();
    }
};

} // namespace segment_v2
} // namespace doris
