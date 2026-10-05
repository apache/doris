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

namespace doris::segment_v2 {

// Pre-decoder for BinaryPlainPage (V1) data pages of CHAR columns.
//
// Segments store CHAR(N) zero-padded to N bytes (the on-disk format). The
// pre-decoder strnlens each slice once at page load time, then rewrites the
// page as a tight V1 layout (no trailing '\0' bytes, adjusted offsets) before
// it is placed in the page cache, so the compute layer reads unpadded CHAR.
//
// Both input and output layout are V1:
//   Data:    |binary1|binary2|...|binaryN|
//   Trailer: |offset1|offset2|...|offsetN| num_elems (32-bit)
//
// Reuses the same writer as the V2 pre-decoders (write_binary_plain_v1_output)
// for steps 3-7; only the input scan differs (offsets array instead of varint
// lengths).
struct BinaryPlainPageCharStripPreDecoder : public DataPagePreDecoder {
    Status decode(std::unique_ptr<DataPage>* page, Slice* page_slice, size_t size_of_tail,
                  bool _use_cache, segment_v2::PageTypePB page_type, const std::string& file_path,
                  size_t size_of_prefix = 0) override {
        // Step 1: validate and locate the V1 trailer.
        if (page_slice->size < size_of_tail + sizeof(uint32_t)) {
            return Status::Corruption(
                    "Invalid CHAR plain page size: {}, expected at least {} in file: {}",
                    page_slice->size, size_of_tail + sizeof(uint32_t), file_path);
        }
        Slice data(page_slice->data, page_slice->size - size_of_tail);
        uint32_t num_elems = decode_fixed32_le(
                reinterpret_cast<const uint8_t*>(&data[data.size - sizeof(uint32_t)]));

        // Always rewrite the page even for num_elems==0 — callers (e.g.
        // BinaryDictPagePreDecoder) may pass a non-zero `size_of_prefix`
        // expecting a fresh output buffer with that prefix area reserved.
        size_t offsets_pos = data.size - (num_elems + 1) * sizeof(uint32_t);
        if (num_elems > 0 && offsets_pos > data.size - sizeof(uint32_t)) {
            return Status::Corruption(
                    "CHAR plain page corruption: offsets pos beyond data, size={}, num_elems={}, "
                    "offsets_pos={} in file: {}",
                    data.size, num_elems, offsets_pos, file_path);
        }
        const auto* offsets_in = reinterpret_cast<const uint8_t*>(&data[offsets_pos]);

        // Step 2: scan entries, strnlen-ing each to drop trailing '\0' padding.
        std::vector<BinaryPlainV1Entry> entries;
        entries.reserve(num_elems);
        uint32_t total_out_len = 0;
        for (uint32_t i = 0; i < num_elems; ++i) {
            uint32_t start = decode_fixed32_le(offsets_in + i * sizeof(uint32_t));
            uint32_t end = (i + 1 < num_elems)
                                   ? decode_fixed32_le(offsets_in + (i + 1) * sizeof(uint32_t))
                                   : static_cast<uint32_t>(offsets_pos);
            if (end < start || end > offsets_pos) {
                return Status::Corruption(
                        "CHAR plain page corruption: bad offset at {}, start={}, end={}, "
                        "offsets_pos={} in file: {}",
                        i, start, end, offsets_pos, file_path);
            }
            uint32_t raw_size = end - start;
            uint32_t out_len = static_cast<uint32_t>(strnlen(data.data + start, raw_size));
            entries.push_back({reinterpret_cast<const uint8_t*>(data.data + start), out_len});
            total_out_len += out_len;
        }

        // Steps 3-7 (shared with V2 pre-decoders).
        return write_binary_plain_v1_output(entries, num_elems, total_out_len, *page_slice,
                                            size_of_tail, size_of_prefix, _use_cache, page_type,
                                            page, page_slice);
    }
};

} // namespace doris::segment_v2
