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
#include "storage/segment/encoding_info.h"
#include "util/coding.h"

namespace doris {
namespace segment_v2 {

// One source entry feeding the V1 output writer. Variants differ only in how
// `out_len` is derived from the raw input length (raw, strnlen'd, etc.).
struct BinaryPlainV1Entry {
    const uint8_t* start;
    uint32_t out_len;
};

// Allocate a V1 BinaryPlainPage layout output buffer and write
// binary -> offsets -> num_elems -> tail. Shared by V2 pre-decoders (after
// they iterate varint lengths) and the V1 CHAR-strip pre-decoder (after it
// iterates the V1 offsets array). `size_of_prefix` reserves room before the
// V1 data for callers that wrap the page (e.g. BinaryDictPagePreDecoder
// prepending the dict-page header).
inline Status write_binary_plain_v1_output(const std::vector<BinaryPlainV1Entry>& entries,
                                           uint32_t num_elems, uint32_t total_out_len,
                                           const Slice& source_page_slice, size_t size_of_tail,
                                           size_t size_of_prefix, bool use_cache,
                                           segment_v2::PageTypePB page_type,
                                           std::unique_ptr<DataPage>* out_page,
                                           Slice* out_page_slice) {
    size_t offsets_size = num_elems * sizeof(uint32_t);
    size_t v1_data_size = total_out_len + offsets_size + sizeof(uint32_t);
    size_t total_size = size_of_prefix + v1_data_size + size_of_tail;

    std::unique_ptr<DataPage> decoded_page =
            std::make_unique<DataPage>(total_size, use_cache, page_type);
    Slice decoded_slice(decoded_page->data(), total_size);
    char* output = decoded_slice.data + size_of_prefix;

    // Binary payload.
    for (const auto& e : entries) {
        memcpy(output, e.start, e.out_len);
        output += e.out_len;
    }

    // Offsets array (running cursor).
    uint32_t running = 0;
    for (const auto& e : entries) {
        encode_fixed32_le(reinterpret_cast<uint8_t*>(output), running);
        output += sizeof(uint32_t);
        running += e.out_len;
    }

    // num_elems trailer.
    encode_fixed32_le(reinterpret_cast<uint8_t*>(output), num_elems);
    output += sizeof(uint32_t);

    // Tail (footer + null map).
    if (size_of_tail > 0) {
        memcpy(output, source_page_slice.data + source_page_slice.size - size_of_tail,
               size_of_tail);
    }
    *out_page_slice = decoded_slice;
    *out_page = std::move(decoded_page);
    return Status::OK();
}

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
 *
 * The decode pipeline is 7 steps:
 *   1. parse header (validate sizes + extract num_elems + iteration bounds)
 *   2. scan entries: record (data_start, out_len) per entry, sum total out_len
 *   3. allocate the V1 output page (size_of_prefix + binary + offsets + trailer + tail)
 *   4. write binary payload
 *   5. write offsets array (running cursor over out_len)
 *   6. write num_elems trailer
 *   7. copy tail (footer + null map) and publish output params
 *
 * IS_CHAR=true picks the strnlen transform in step 2, so CHAR pages emit
 * unpadded slices to the cached page. IS_CHAR=false keeps raw V2 lengths.
 * The branch is `if constexpr` — compile-time dispatched, no overhead.
 */
template <bool IS_CHAR>
struct BinaryPlainPageV2PreDecoder : public DataPagePreDecoder {
    Status decode(std::unique_ptr<DataPage>* page, Slice* page_slice, size_t size_of_tail,
                  bool _use_cache, segment_v2::PageTypePB page_type, const std::string& file_path,
                  size_t size_of_prefix = 0) override {
        // Step 1.
        Slice data;
        uint32_t num_elems = 0;
        const uint8_t* ptr = nullptr;
        const uint8_t* limit = nullptr;
        RETURN_IF_ERROR(parse_header(*page_slice, size_of_tail, file_path, &data, &num_elems, &ptr,
                                     &limit));

        // Step 2: out_len derived per IS_CHAR.
        std::vector<BinaryPlainV1Entry> entries;
        entries.reserve(num_elems);
        uint32_t total_out_len = 0;
        for (uint32_t i = 0; i < num_elems; ++i) {
            uint32_t raw_len = 0;
            const uint8_t* data_start = nullptr;
            RETURN_IF_ERROR(decode_one(ptr, limit, file_path, i, &data_start, &raw_len));
            uint32_t out_len;
            if constexpr (IS_CHAR) {
                out_len = static_cast<uint32_t>(
                        strnlen(reinterpret_cast<const char*>(data_start), raw_len));
            } else {
                out_len = raw_len;
            }
            entries.push_back({data_start, out_len});
            total_out_len += out_len;
            ptr = data_start + raw_len;
        }

        // Steps 3-7.
        return write_binary_plain_v1_output(entries, num_elems, total_out_len, *page_slice,
                                            size_of_tail, size_of_prefix, _use_cache, page_type,
                                            page, page_slice);
    }

private:
    // Step 1: validate the V2 page and extract iteration bounds.
    static inline Status parse_header(const Slice& page_slice, size_t size_of_tail,
                                      const std::string& file_path, Slice* out_data,
                                      uint32_t* out_num_elems, const uint8_t** out_ptr,
                                      const uint8_t** out_limit) {
        if (page_slice.size < sizeof(uint32_t) + size_of_tail) {
            return Status::Corruption("Invalid page size: {}, expected at least {} in file: {}",
                                      page_slice.size, sizeof(uint32_t) + size_of_tail, file_path);
        }
        *out_data = Slice(page_slice.data, page_slice.size - size_of_tail);
        if (out_data->size < sizeof(uint32_t)) {
            return Status::Corruption("Data too small to contain num_elems in file: {}", file_path);
        }
        *out_num_elems = decode_fixed32_le(
                reinterpret_cast<const uint8_t*>(&(*out_data)[out_data->size - sizeof(uint32_t)]));
        *out_ptr = reinterpret_cast<const uint8_t*>(out_data->data);
        *out_limit = *out_ptr + out_data->size - sizeof(uint32_t);
        return Status::OK();
    }

    // Step 2 helper: decode one varint length and validate the entry bounds.
    // The scan loop in decode() / overrides walks the input with this helper
    // and decides what `out_len` to record (raw_len here, strnlen'd in the
    // CHAR variant).
    static inline Status decode_one(const uint8_t* ptr, const uint8_t* limit,
                                    const std::string& file_path, uint32_t i,
                                    const uint8_t** out_data_start, uint32_t* out_raw_len) {
        if (ptr >= limit) {
            return Status::Corruption("Unexpected end of data while parsing element {} in file: {}",
                                      i, file_path);
        }
        const uint8_t* data_start = decode_varint32_ptr(ptr, limit, out_raw_len);
        if (data_start == nullptr) {
            return Status::Corruption("Failed to decode varuint for element {} in file: {}", i,
                                      file_path);
        }
        if (data_start + *out_raw_len > limit) {
            return Status::Corruption("Data extends beyond page for element {} in file: {}", i,
                                      file_path);
        }
        *out_data_start = data_start;
        return Status::OK();
    }
};

} // namespace segment_v2
} // namespace doris
