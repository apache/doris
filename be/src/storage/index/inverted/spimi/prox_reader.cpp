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

#include "storage/index/inverted/spimi/prox_reader.h"

// `_CLTHROWA` for byte-parser hard-fail on untrusted .prx bytes.
// StdHeader.h sets up the CLUCENE_EXPORT / CL_NS_DEF macros that
// debug/error.h depends on.
#include <CLucene/StdHeader.h>
#include <CLucene/debug/error.h>

#include <algorithm>

#include "common/logging.h"

namespace doris::segment_v2::inverted_index::spimi {

namespace {

// VInt decoder over the .prx bytes. Hard-fails on underflow because
// `.prx` is part of the on-disk segment and therefore attacker-
// influenceable; DCHECK is no-op in release builds and would let a
// VInt continuation-byte chain extend past `len` reading arbitrary
// heap.
int32_t ReadVInt(const uint8_t* data, size_t len, size_t* pos) {
    uint32_t v = 0;
    uint32_t shift = 0;
    while (true) {
        if (*pos >= len) [[unlikely]] {
            _CLTHROWA(CL_ERR_IO, "SPIMI .prx VInt underflow");
        }
        const uint8_t b = data[(*pos)++];
        v |= static_cast<uint32_t>(b & 0x7FU) << shift;
        if ((b & 0x80U) == 0) {
            break;
        }
        shift += 7;
        if (shift >= 32U) [[unlikely]] {
            _CLTHROWA(CL_ERR_IO, "SPIMI .prx VInt: shift overflow on crafted input");
        }
    }
    return static_cast<int32_t>(v);
}

} // namespace

std::vector<std::vector<int32_t>> SpimiProxReader::ReadPositions(
        const uint8_t* prx_data, size_t prx_length, const std::vector<int32_t>& freqs_per_doc) {
    std::vector<std::vector<int32_t>> out;
    out.reserve(freqs_per_doc.size());
    size_t pos = 0;
    for (const int32_t freq : freqs_per_doc) {
        if (freq <= 0) [[unlikely]] {
            _CLTHROWA(CL_ERR_IO, "SPIMI .prx: non-positive freq in freqs_per_doc");
        }
        std::vector<int32_t> positions;
        // Cap reserve against the same DoS-bounding limit as
        // term_docs_reader.cpp. Per-doc positions have a much
        // smaller realistic ceiling than docs-per-term, so the
        // cap is 1<<16 (~65k positions per doc).
        constexpr size_t kPosReserveCap = 1U << 16;
        positions.reserve(std::min(static_cast<size_t>(freq), kPosReserveCap));
        int32_t last = 0;
        for (int32_t i = 0; i < freq; ++i) {
            last += ReadVInt(prx_data, prx_length, &pos);
            positions.push_back(last);
        }
        out.push_back(std::move(positions));
    }
    return out;
}

} // namespace doris::segment_v2::inverted_index::spimi
