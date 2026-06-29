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

#include "snii/encoding/pfor.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <vector>

#include "snii/common/slice.h"

namespace snii {
using doris::Status; // RETURN_IF_ERROR expands to bare Status
namespace {

// Unaligned little-endian 64-bit load from a raw byte pointer (single
// instruction on x86; memcpy is the portable, UB-free spelling the compiler
// folds to a mov).
inline uint64_t load_u64_le(const uint8_t* p) {
    uint64_t v;
    std::memcpy(&v, p, sizeof(v));
#if defined(__BIG_ENDIAN__) || (defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
    v = __builtin_bswap64(v);
#endif
    return v;
}

uint8_t bits_for(uint32_t v) {
    uint8_t b = 0;
    while (v) {
        ++b;
        v >>= 1;
    }
    return b;
}

// Choose the bit_width that minimizes total bytes (packed + exceptions).
// Exception cost estimated at ~6 bytes each.
uint8_t choose_width(const uint32_t* v, size_t n) {
    uint8_t maxw = 0;
    for (size_t i = 0; i < n; ++i) {
        maxw = std::max(maxw, bits_for(v[i]));
    }
    uint8_t best = maxw;
    size_t best_cost = SIZE_MAX;
    for (uint8_t w = 0; w <= maxw; ++w) {
        size_t exc = 0;
        for (size_t i = 0; i < n; ++i) {
            if (bits_for(v[i]) > w) {
                ++exc;
            }
        }
        size_t cost = (static_cast<size_t>(w) * n + 7) / 8 + exc * 6;
        if (cost < best_cost) {
            best_cost = cost;
            best = w;
        }
    }
    return best;
}

uint32_t low_mask(uint8_t w) {
    return (w >= 32) ? 0xFFFFFFFFU : ((1U << w) - 1U);
}

void bitpack(const uint32_t* v, size_t n, uint8_t w, ByteSink* out) {
    if (w == 0) {
        return;
    }
    uint64_t acc = 0;
    int filled = 0;
    for (size_t i = 0; i < n; ++i) {
        acc |= static_cast<uint64_t>(v[i] & low_mask(w)) << filled;
        filled += w;
        while (filled >= 8) {
            out->put_u8(static_cast<uint8_t>(acc));
            acc >>= 8;
            filled -= 8;
        }
    }
    if (filled > 0) {
        out->put_u8(static_cast<uint8_t>(acc));
    }
}

void bitunpack_tail(const uint8_t* base, size_t packed, size_t n, uint8_t w, size_t i,
                    uint64_t mask, uint32_t* out) {
    for (; i < n; ++i) {
        const size_t bit_off = static_cast<size_t>(w) * i;
        const size_t byte_off = bit_off >> 3;
        uint64_t word = 0;
        for (size_t b = byte_off; b < packed && b < byte_off + 8; ++b) {
            word |= static_cast<uint64_t>(base[b]) << ((b - byte_off) * 8);
        }
        out[i] = static_cast<uint32_t>((word >> (bit_off & 7)) & mask);
    }
}

void bitunpack_w1(const uint8_t* base, size_t n, uint32_t* out) {
    size_t i = 0;
    size_t byte = 0;
    for (; i + 8 <= n; i += 8, ++byte) {
        const uint8_t v = base[byte];
        out[i] = v & 1U;
        out[i + 1] = (v >> 1) & 1U;
        out[i + 2] = (v >> 2) & 1U;
        out[i + 3] = (v >> 3) & 1U;
        out[i + 4] = (v >> 4) & 1U;
        out[i + 5] = (v >> 5) & 1U;
        out[i + 6] = (v >> 6) & 1U;
        out[i + 7] = (v >> 7) & 1U;
    }
    if (i < n) {
        const uint8_t v = base[byte];
        for (uint8_t bit = 0; i < n; ++i, ++bit) {
            out[i] = (v >> bit) & 1U;
        }
    }
}

void bitunpack_w2(const uint8_t* base, size_t n, uint32_t* out) {
    size_t i = 0;
    size_t byte = 0;
    for (; i + 4 <= n; i += 4, ++byte) {
        const uint8_t v = base[byte];
        out[i] = v & 3U;
        out[i + 1] = (v >> 2) & 3U;
        out[i + 2] = (v >> 4) & 3U;
        out[i + 3] = (v >> 6) & 3U;
    }
    if (i < n) {
        const uint8_t v = base[byte];
        for (uint8_t shift = 0; i < n; ++i, shift += 2) {
            out[i] = (v >> shift) & 3U;
        }
    }
}

void bitunpack_w3(const uint8_t* base, size_t packed, size_t n, uint32_t* out) {
    size_t i = 0;
    size_t byte = 0;
    for (; i + 8 <= n; i += 8, byte += 3) {
        const uint32_t b0 = base[byte];
        const uint32_t b1 = base[byte + 1];
        const uint32_t b2 = base[byte + 2];
        out[i] = b0 & 7U;
        out[i + 1] = (b0 >> 3) & 7U;
        out[i + 2] = ((b0 >> 6) | (b1 << 2)) & 7U;
        out[i + 3] = (b1 >> 1) & 7U;
        out[i + 4] = (b1 >> 4) & 7U;
        out[i + 5] = ((b1 >> 7) | (b2 << 1)) & 7U;
        out[i + 6] = (b2 >> 2) & 7U;
        out[i + 7] = (b2 >> 5) & 7U;
    }
    bitunpack_tail(base, packed, n, 3, i, 7U, out);
}

void bitunpack_w4(const uint8_t* base, size_t n, uint32_t* out) {
    size_t i = 0;
    size_t byte = 0;
    for (; i + 2 <= n; i += 2, ++byte) {
        const uint8_t v = base[byte];
        out[i] = v & 15U;
        out[i + 1] = (v >> 4) & 15U;
    }
    if (i < n) {
        out[i] = base[byte] & 15U;
    }
}

void bitunpack_w5(const uint8_t* base, size_t packed, size_t n, uint32_t* out) {
    size_t i = 0;
    size_t byte = 0;
    for (; i + 8 <= n; i += 8, byte += 5) {
        const uint32_t b0 = base[byte];
        const uint32_t b1 = base[byte + 1];
        const uint32_t b2 = base[byte + 2];
        const uint32_t b3 = base[byte + 3];
        const uint32_t b4 = base[byte + 4];
        out[i] = b0 & 31U;
        out[i + 1] = ((b0 >> 5) | (b1 << 3)) & 31U;
        out[i + 2] = (b1 >> 2) & 31U;
        out[i + 3] = ((b1 >> 7) | (b2 << 1)) & 31U;
        out[i + 4] = ((b2 >> 4) | (b3 << 4)) & 31U;
        out[i + 5] = (b3 >> 1) & 31U;
        out[i + 6] = ((b3 >> 6) | (b4 << 2)) & 31U;
        out[i + 7] = (b4 >> 3) & 31U;
    }
    bitunpack_tail(base, packed, n, 5, i, 31U, out);
}

void bitunpack_w6(const uint8_t* base, size_t packed, size_t n, uint32_t* out) {
    size_t i = 0;
    size_t byte = 0;
    for (; i + 4 <= n; i += 4, byte += 3) {
        const uint32_t b0 = base[byte];
        const uint32_t b1 = base[byte + 1];
        const uint32_t b2 = base[byte + 2];
        out[i] = b0 & 63U;
        out[i + 1] = ((b0 >> 6) | (b1 << 2)) & 63U;
        out[i + 2] = ((b1 >> 4) | (b2 << 4)) & 63U;
        out[i + 3] = (b2 >> 2) & 63U;
    }
    bitunpack_tail(base, packed, n, 6, i, 63U, out);
}

void bitunpack_w7(const uint8_t* base, size_t packed, size_t n, uint32_t* out) {
    size_t i = 0;
    size_t byte = 0;
    for (; i + 8 <= n; i += 8, byte += 7) {
        const uint32_t b0 = base[byte];
        const uint32_t b1 = base[byte + 1];
        const uint32_t b2 = base[byte + 2];
        const uint32_t b3 = base[byte + 3];
        const uint32_t b4 = base[byte + 4];
        const uint32_t b5 = base[byte + 5];
        const uint32_t b6 = base[byte + 6];
        out[i] = b0 & 127U;
        out[i + 1] = ((b0 >> 7) | (b1 << 1)) & 127U;
        out[i + 2] = ((b1 >> 6) | (b2 << 2)) & 127U;
        out[i + 3] = ((b2 >> 5) | (b3 << 3)) & 127U;
        out[i + 4] = ((b3 >> 4) | (b4 << 4)) & 127U;
        out[i + 5] = ((b4 >> 3) | (b5 << 5)) & 127U;
        out[i + 6] = ((b5 >> 2) | (b6 << 6)) & 127U;
        out[i + 7] = (b6 >> 1) & 127U;
    }
    bitunpack_tail(base, packed, n, 7, i, 127U, out);
}

void bitunpack_w8(const uint8_t* base, size_t n, uint32_t* out) {
    for (size_t i = 0; i < n; ++i) {
        out[i] = base[i];
    }
}

void bitunpack_generic(const uint8_t* base, size_t packed, size_t n, uint8_t w, uint32_t* out) {
    const uint64_t mask = low_mask(w);
    size_t i = 0;
    if (packed >= 8) {
        const size_t last_safe_byte = packed - 8;
        for (; i < n; ++i) {
            const size_t bit_off = static_cast<size_t>(w) * i;
            const size_t byte_off = bit_off >> 3;
            if (byte_off > last_safe_byte) {
                break;
            }
            out[i] = static_cast<uint32_t>((load_u64_le(base + byte_off) >> (bit_off & 7)) & mask);
        }
    }
    bitunpack_tail(base, packed, n, w, i, mask, out);
}

doris::Status bitunpack(ByteSource* src, size_t n, uint8_t w, uint32_t* out) {
    if (w == 0) {
        std::memset(out, 0, n * sizeof(uint32_t));
        return doris::Status::OK();
    }
    // Pull the packed run once and unpack from the contiguous slice; this keeps
    // the hot decode path free of per-byte ByteSource calls.
    const size_t packed = (static_cast<size_t>(w) * n + 7) / 8;
    Slice buf;
    RETURN_IF_ERROR(src->get_bytes(packed, &buf));
    const uint8_t* base = buf.data();

    switch (w) {
    case 1:
        bitunpack_w1(base, n, out);
        break;
    case 2:
        bitunpack_w2(base, n, out);
        break;
    case 3:
        bitunpack_w3(base, packed, n, out);
        break;
    case 4:
        bitunpack_w4(base, n, out);
        break;
    case 5:
        bitunpack_w5(base, packed, n, out);
        break;
    case 6:
        bitunpack_w6(base, packed, n, out);
        break;
    case 7:
        bitunpack_w7(base, packed, n, out);
        break;
    case 8:
        bitunpack_w8(base, n, out);
        break;
    default:
        bitunpack_generic(base, packed, n, w, out);
        break;
    }
    return doris::Status::OK();
}

} // namespace

void pfor_encode(const uint32_t* values, size_t n, ByteSink* out) {
    uint8_t w = choose_width(values, n);
    std::vector<std::pair<uint32_t, uint32_t>> exc; // (index, full value)
    std::vector<uint32_t> low(values, values + n);
    for (size_t i = 0; i < n; ++i) {
        if (bits_for(values[i]) > w) {
            exc.emplace_back(static_cast<uint32_t>(i), values[i]);
            low[i] = 0; // Write 0 as placeholder at exception position; true value
                        // stored in exception table
        }
    }
    out->put_u8(w);
    out->put_varint32(static_cast<uint32_t>(exc.size()));
    bitpack(low.data(), n, w, out);
    uint32_t prev = 0;
    for (const auto& e : exc) {
        out->put_varint32(e.first - prev);
        out->put_varint32(e.second);
        prev = e.first;
    }
}

doris::Status pfor_decode(ByteSource* src, size_t n, uint32_t* out) {
    uint8_t w;
    RETURN_IF_ERROR(src->get_u8(&w));
    uint32_t n_exc;
    RETURN_IF_ERROR(src->get_varint32(&n_exc));
    RETURN_IF_ERROR(bitunpack(src, n, w, out));
    uint32_t idx = 0;
    for (uint32_t i = 0; i < n_exc; ++i) {
        uint32_t d, val;
        RETURN_IF_ERROR(src->get_varint32(&d));
        RETURN_IF_ERROR(src->get_varint32(&val));
        idx += d;
        if (idx >= n) {
            return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("pfor exception index out of range");
        }
        out[idx] = val;
    }
    return doris::Status::OK();
}

doris::Status pfor_skip(ByteSource* src, size_t n) {
    uint8_t w = 0;
    RETURN_IF_ERROR(src->get_u8(&w));
    uint32_t n_exc = 0;
    RETURN_IF_ERROR(src->get_varint32(&n_exc));
    const size_t packed = (static_cast<size_t>(w) * n + 7) / 8;
    Slice unused;
    RETURN_IF_ERROR(src->get_bytes(packed, &unused));
    uint32_t idx = 0;
    for (uint32_t i = 0; i < n_exc; ++i) {
        uint32_t d = 0;
        uint32_t val = 0;
        RETURN_IF_ERROR(src->get_varint32(&d));
        RETURN_IF_ERROR(src->get_varint32(&val));
        idx += d;
        if (idx >= n) {
            return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("pfor exception index out of range");
        }
    }
    return doris::Status::OK();
}

} // namespace snii
