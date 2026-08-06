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

#include <cstring>
#include <vector>

#include "storage/cache/page_cache.h"
#include "storage/segment/binary_plain_page_v2_pre_decoder.h" // BinaryPlainV1Entry, write_binary_plain_v1_output
#include "storage/segment/encoding_info.h"
#include "util/coding.h"

namespace doris {
namespace segment_v2 {

/**
 * @brief Pre-decoder for BinaryPlainPageV3.
 *
 * Converts the V3 layout (contiguous data + contiguous varuint length block)
 * to the V1 layout (offsets array trailer) so the cached page can be served
 * with O(1) seek.
 *
 * V3 format (input):
 *   |binary1|binary2|...|binaryN|varuint_len1|...|varuint_lenN|
 *   |data_block_size(32-bit)|num_elems(32-bit)|
 *
 * V1 format (output, same as BinaryPlainPageV2PreDecoder):
 *   |binary1|...|binaryN|offset1(32-bit)|...|offsetN(32-bit)|num_elems(32-bit)|
 *
 * The builder stores the same bytes as V1/V2 (CHAR keeps its '\0' padding, VARCHAR
 * does not); only the layout differs. IS_CHAR selects how the stored lengths are
 * treated on read, mirroring V2:
 *   - IS_CHAR=false: values carry no padding, so the binary block is memcpy'd in a
 *     single shot and the varint loop only fills the running offsets array — the two
 *     passes are independent and the data is touched exactly once. This is the win vs
 *     V2 (which chases a length pointer per entry). Used for all non-CHAR binary types
 *     (VARCHAR/STRING/JSONB/VARIANT/HLL/BITMAP/QUANTILE_STATE/AGG_STATE).
 *   - IS_CHAR=true: each entry is strnlen'd to drop the trailing '\0' padding that CHAR
 *     values carry on disk, then the V1 page is built from the logical lengths. Selected
 *     for (CHAR, PLAIN_ENCODING_V3) on read (e.g. the CHAR dictionary word page).
 */
template <bool IS_CHAR>
struct BinaryPlainPageV3PreDecoder : public DataPagePreDecoder {
    Status decode(std::unique_ptr<DataPage>* page, Slice* page_slice, size_t size_of_tail,
                  bool _use_cache, segment_v2::PageTypePB page_type, const std::string& file_path,
                  size_t size_of_prefix = 0) override {
        // V3 trailer is two u32 words: data_block_size then num_elems.
        constexpr size_t kV3TrailerSize = 2 * sizeof(uint32_t);

        if (page_slice->size < kV3TrailerSize + size_of_tail) {
            return Status::Corruption("Invalid V3 page size: {}, expected at least {} in file: {}",
                                      page_slice->size, kV3TrailerSize + size_of_tail, file_path);
        }

        // page_slice->size >= kV3TrailerSize + size_of_tail is enforced above,
        // so data.size = page_slice->size - size_of_tail >= kV3TrailerSize.
        Slice data(page_slice->data, page_slice->size - size_of_tail);

        const uint8_t* data_begin = reinterpret_cast<const uint8_t*>(data.data);
        const uint8_t* trailer_ptr = data_begin + data.size - kV3TrailerSize;
        uint32_t data_block_size = decode_fixed32_le(trailer_ptr);
        uint32_t num_elems = decode_fixed32_le(trailer_ptr + sizeof(uint32_t));

        // Use subtraction form to avoid uint32_t wraparound on a malicious
        // data_block_size close to UINT32_MAX. data.size >= kV3TrailerSize.
        if (data_block_size > data.size - kV3TrailerSize) {
            return Status::Corruption("V3 data_block_size {} exceeds available data {} in file: {}",
                                      data_block_size, data.size - kV3TrailerSize, file_path);
        }

        const uint8_t* lengths_ptr = data_begin + data_block_size;
        const uint8_t* lengths_limit = trailer_ptr;

        if constexpr (IS_CHAR) {
            // ---- CHAR path: strnlen each entry to strip trailing '\0' padding. ----
            // Walk the contiguous data block in lockstep with the length block:
            // entry i starts at data_begin + running_raw and is `len` bytes wide.
            const uint8_t* ptr = lengths_ptr;
            uint32_t running_raw = 0;
            std::vector<BinaryPlainV1Entry> entries;
            entries.reserve(num_elems);
            uint32_t total_out_len = 0;
            for (uint32_t i = 0; i < num_elems; ++i) {
                if (ptr >= lengths_limit) {
                    return Status::Corruption(
                            "V3 unexpected end of length block at element {} in file: {}", i,
                            file_path);
                }
                uint32_t len = 0;
                ptr = decode_varint32_ptr(ptr, lengths_limit, &len);
                if (ptr == nullptr) {
                    return Status::Corruption(
                            "V3 failed to decode varuint at element {} in file: {}", i, file_path);
                }
                if (len > data_block_size - running_raw) {
                    return Status::Corruption(
                            "V3 entry {} length {} overflows data block in file: {}", i, len,
                            file_path);
                }
                const uint8_t* entry_data = data_begin + running_raw;
                uint32_t out_len = static_cast<uint32_t>(
                        strnlen(reinterpret_cast<const char*>(entry_data), len));
                entries.push_back({entry_data, out_len});
                total_out_len += out_len;
                running_raw += len;
            }

            if (running_raw != data_block_size) {
                return Status::Corruption("V3 sum of lengths {} != data_block_size {} in file: {}",
                                          running_raw, data_block_size, file_path);
            }

            return write_binary_plain_v1_output(entries, num_elems, total_out_len, *page_slice,
                                                size_of_tail, size_of_prefix, _use_cache, page_type,
                                                page, page_slice);
        } else {
            // ---- Fast path (non-CHAR): lengths are already logical. ----
            const size_t offsets_size = static_cast<size_t>(num_elems) * sizeof(uint32_t);
            const size_t v1_data_size = data_block_size + offsets_size + sizeof(uint32_t);
            const size_t total_size = size_of_prefix + v1_data_size + size_of_tail;

            std::unique_ptr<DataPage> decoded_page =
                    std::make_unique<DataPage>(total_size, _use_cache, page_type);
            Slice decoded_slice(decoded_page->data(), total_size);
            char* output = decoded_slice.data + size_of_prefix;

            // 1. Single memcpy of the entire binary payload.
            if (data_block_size > 0) {
                memcpy(output, data_begin, data_block_size);
            }
            output += data_block_size;

            // 2. Walk varints once, write the running offsets array.
            const uint8_t* ptr = lengths_ptr;
            uint32_t running = 0;
            for (uint32_t i = 0; i < num_elems; ++i) {
                if (ptr >= lengths_limit) {
                    return Status::Corruption(
                            "V3 unexpected end of length block at element {} in file: {}", i,
                            file_path);
                }
                uint32_t len = 0;
                ptr = decode_varint32_ptr(ptr, lengths_limit, &len);
                if (ptr == nullptr) {
                    return Status::Corruption(
                            "V3 failed to decode varuint at element {} in file: {}", i, file_path);
                }
                encode_fixed32_le(reinterpret_cast<uint8_t*>(output), running);
                output += sizeof(uint32_t);
                running += len;
            }

            if (running != data_block_size) {
                return Status::Corruption("V3 sum of lengths {} != data_block_size {} in file: {}",
                                          running, data_block_size, file_path);
            }

            // 3. num_elems trailer.
            encode_fixed32_le(reinterpret_cast<uint8_t*>(output), num_elems);
            output += sizeof(uint32_t);

            // 4. Tail (footer + null map) carried through unchanged.
            if (size_of_tail > 0) {
                memcpy(output, page_slice->data + page_slice->size - size_of_tail, size_of_tail);
            }

            *page_slice = decoded_slice;
            *page = std::move(decoded_page);
            return Status::OK();
        }
    }
};

} // namespace segment_v2
} // namespace doris
