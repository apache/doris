#include "snii/encoding/pfor.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <vector>

#include "snii/common/slice.h"

namespace snii {
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
    for (size_t i = 0; i < n; ++i) maxw = std::max(maxw, bits_for(v[i]));
    uint8_t best = maxw;
    size_t best_cost = SIZE_MAX;
    for (int w = 0; w <= maxw; ++w) {
        size_t exc = 0;
        for (size_t i = 0; i < n; ++i)
            if (bits_for(v[i]) > w) ++exc;
        size_t cost = (static_cast<size_t>(w) * n + 7) / 8 + exc * 6;
        if (cost < best_cost) {
            best_cost = cost;
            best = static_cast<uint8_t>(w);
        }
    }
    return best;
}

uint32_t low_mask(uint8_t w) {
    return (w >= 32) ? 0xFFFFFFFFu : ((1u << w) - 1u);
}

void bitpack(const uint32_t* v, size_t n, uint8_t w, ByteSink* out) {
    if (w == 0) return;
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
    if (filled > 0) out->put_u8(static_cast<uint8_t>(acc));
}

Status bitunpack(ByteSource* src, size_t n, uint8_t w, uint32_t* out) {
    if (w == 0) {
        std::memset(out, 0, n * sizeof(uint32_t));
        return Status::OK();
    }
    // Pull the whole packed run in ONE bounds-checked slice (#3: was one get_u8
    // per byte -- a Status-returning call + bounds check each), then unpack
    // straight from the contiguous buffer. Each value's w<=32 bits start at bit
    // offset i*w and span at most ceil((7+32)/8)=5 bytes, so a single unaligned
    // 64-bit load at byte (i*w)/8 always covers it: one load + shift + mask per
    // value, branchless, no per-byte accumulator loop (#2). Measured fewest
    // instructions and fewest cycles of the alternatives -- the dependency-free
    // per-value form lets the core overlap the loads (the unaligned word reads
    // all hit L1, the packed run being only KiB).
    const size_t packed = (static_cast<size_t>(w) * n + 7) / 8;
    Slice buf;
    SNII_RETURN_IF_ERROR(src->get_bytes(packed, &buf));
    const uint8_t* base = buf.data();
    const uint64_t mask = low_mask(w);

    // Fast path: values whose 8-byte load window stays inside the buffer
    // (byte_off + 8
    // <= packed). The final few are finished by the tail loop, which zero-pads
    // past end.
    size_t i = 0;
    if (packed >= 8) {
        const size_t last_safe_byte = packed - 8;
        for (; i < n; ++i) {
            const size_t bit_off = static_cast<size_t>(w) * i;
            const size_t byte_off = bit_off >> 3;
            if (byte_off > last_safe_byte) break;
            out[i] = static_cast<uint32_t>((load_u64_le(base + byte_off) >> (bit_off & 7)) & mask);
        }
    }
    for (; i < n; ++i) {
        const size_t bit_off = static_cast<size_t>(w) * i;
        const size_t byte_off = bit_off >> 3;
        uint64_t word = 0;
        for (size_t b = byte_off; b < packed && b < byte_off + 8; ++b) {
            word |= static_cast<uint64_t>(base[b]) << ((b - byte_off) * 8);
        }
        out[i] = static_cast<uint32_t>((word >> (bit_off & 7)) & mask);
    }
    return Status::OK();
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

Status pfor_decode(ByteSource* src, size_t n, uint32_t* out) {
    uint8_t w;
    SNII_RETURN_IF_ERROR(src->get_u8(&w));
    uint32_t n_exc;
    SNII_RETURN_IF_ERROR(src->get_varint32(&n_exc));
    SNII_RETURN_IF_ERROR(bitunpack(src, n, w, out));
    uint32_t idx = 0;
    for (uint32_t i = 0; i < n_exc; ++i) {
        uint32_t d, val;
        SNII_RETURN_IF_ERROR(src->get_varint32(&d));
        SNII_RETURN_IF_ERROR(src->get_varint32(&val));
        idx += d;
        if (idx >= n) return Status::Corruption("pfor exception index out of range");
        out[idx] = val;
    }
    return Status::OK();
}

Status pfor_skip(ByteSource* src, size_t n) {
    uint8_t w = 0;
    SNII_RETURN_IF_ERROR(src->get_u8(&w));
    uint32_t n_exc = 0;
    SNII_RETURN_IF_ERROR(src->get_varint32(&n_exc));
    const size_t packed = (static_cast<size_t>(w) * n + 7) / 8;
    Slice unused;
    SNII_RETURN_IF_ERROR(src->get_bytes(packed, &unused));
    uint32_t idx = 0;
    for (uint32_t i = 0; i < n_exc; ++i) {
        uint32_t d = 0;
        uint32_t val = 0;
        SNII_RETURN_IF_ERROR(src->get_varint32(&d));
        SNII_RETURN_IF_ERROR(src->get_varint32(&val));
        idx += d;
        if (idx >= n) return Status::Corruption("pfor exception index out of range");
    }
    return Status::OK();
}

} // namespace snii
