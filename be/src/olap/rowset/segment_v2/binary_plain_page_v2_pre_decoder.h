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
#include "olap/rowset/segment_v2/encoding_info.h"
#include "util/coding.h"

namespace doris {
namespace segment_v2 {

/**
 * @brief Pre-decoder for BinaryPlainPageV2
 *
 * Converts BinaryPlainPageV2 format (varint-encoded lengths) to BinaryPlainPage format
 * (offset array) to enable O(1) seeking without scanning the entire page.
 *
 * V2 format (input):
 *   Data: |length1(varuint)|binary1|length2(varuint)|binary2|...
 *   Trailer: num_elems (32-bit)
 *
 * V1 format (output):
 *   Data: |binary1|binary2|...
 *   Trailer: |offset1(32-bit)|offset2(32-bit)|...| num_elems (32-bit)
 */
struct BinaryPlainPageV2PreDecoder : public DataPagePreDecoder {
    /**
     * @brief Decode BinaryPlainPageV2 data to BinaryPlainPage format
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
        // Validate input
        if (page_slice->size < sizeof(uint32_t) + size_of_tail) {
            return Status::Corruption("Invalid page size: {}, expected at least {} in file: {}",
                                      page_slice->size, sizeof(uint32_t) + size_of_tail, file_path);
        }

        // Calculate data portion (excluding tail)
        Slice data(page_slice->data, page_slice->size - size_of_tail);

        // Read num_elems from the last 4 bytes of data portion
        if (data.size < sizeof(uint32_t)) {
            return Status::Corruption("Data too small to contain num_elems in file: {}", file_path);
        }

        uint32_t num_elems = decode_fixed32_le(
                reinterpret_cast<const uint8_t*>(&data[data.size - sizeof(uint32_t)]));

        // Calculate required size for V1 format
        // V1 format: binary_data + offsets_array + num_elems + tail
        // We need to parse V2 to calculate the total binary data size
        const auto* ptr = reinterpret_cast<const uint8_t*>(data.data);
        const uint8_t* limit = ptr + data.size - sizeof(uint32_t);

        std::vector<uint32_t> offsets;
        offsets.reserve(num_elems);

        uint32_t current_offset = 0;
        for (uint32_t i = 0; i < num_elems; i++) {
            if (ptr >= limit) {
                return Status::Corruption(
                        "Unexpected end of data while parsing element {} in file: {}", i,
                        file_path);
            }

            // Decode varuint length
            uint32_t length;
            const uint8_t* data_start = decode_varint32_ptr(ptr, limit, &length);
            if (data_start == nullptr) {
                return Status::Corruption("Failed to decode varuint for element {} in file: {}", i,
                                          file_path);
            }

            // Store offset for this element
            offsets.push_back(current_offset);
            current_offset += length;

            // Move to next entry
            ptr = data_start + length;

            if (ptr > limit) {
                return Status::Corruption("Data extends beyond page for element {} in file: {}", i,
                                          file_path);
            }
        }

        // Calculate size for V1 format
        size_t binary_data_size = current_offset;
        size_t offsets_size = num_elems * sizeof(uint32_t);
        size_t v1_data_size = binary_data_size + offsets_size + sizeof(uint32_t);
        size_t total_size = size_of_prefix + v1_data_size + size_of_tail;

        // Allocate new page
        Slice decoded_slice;
        decoded_slice.size = total_size;
        std::unique_ptr<DataPage> decoded_page =
                std::make_unique<DataPage>(decoded_slice.size, _use_cache, page_type);
        decoded_slice.data = decoded_page->data();

        // Write V1 format data after the prefix
        char* output = decoded_slice.data + size_of_prefix;

        // Step 1: Write binary data (without varint prefixes)
        ptr = reinterpret_cast<const uint8_t*>(data.data);
        for (uint32_t i = 0; i < num_elems; i++) {
            uint32_t length;
            const uint8_t* data_start = decode_varint32_ptr(ptr, limit, &length);

            // Copy binary data
            memcpy(output, data_start, length);
            output += length;

            // Move to next entry
            ptr = data_start + length;
        }

        // Step 2: Write offsets array
        for (uint32_t offset : offsets) {
            encode_fixed32_le(reinterpret_cast<uint8_t*>(output), offset);
            output += sizeof(uint32_t);
        }

        // Step 3: Write num_elems
        encode_fixed32_le(reinterpret_cast<uint8_t*>(output), num_elems);
        output += sizeof(uint32_t);

        // Step 4: Copy tail (footer and null map)
        if (size_of_tail > 0) {
            memcpy(output, page_slice->data + page_slice->size - size_of_tail, size_of_tail);
        }

        // Update output parameters
        *page_slice = decoded_slice;
        *page = std::move(decoded_page);

        return Status::OK();
    }
};

} // namespace segment_v2
} // namespace doris
