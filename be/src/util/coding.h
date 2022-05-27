//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <string>

#include "gutil/endian.h"
#include "olap/olap_common.h"
#include "util/slice.h"

namespace doris {

// TODO(zc): add encode big endian later when we need it
// use big endian when we have order requirement.
// little endian is more efficient when we use X86 CPU, so
// when we have no order needs, we prefer little endian encoding
inline void encode_fixed8(uint8_t* buf, uint8_t val) {
    *buf = val;
}

inline void encode_fixed16_le(uint8_t* buf, uint16_t val) {
#if __BYTE_ORDER == __LITTLE_ENDIAN
    memcpy(buf, &val, sizeof(val));
#else
    uint16_t res = bswap_16(val);
    memcpy(buf, &res, sizeof(res));
#endif
}

inline void encode_fixed32_le(uint8_t* buf, uint32_t val) {
#if __BYTE_ORDER == __LITTLE_ENDIAN
    memcpy(buf, &val, sizeof(val));
#else
    uint32_t res = bswap_32(val);
    memcpy(buf, &res, sizeof(res));
#endif
}

inline void encode_fixed64_le(uint8_t* buf, uint64_t val) {
#if __BYTE_ORDER == __LITTLE_ENDIAN
    memcpy(buf, &val, sizeof(val));
#else
    uint64_t res = gbswap_64(val);
    memcpy(buf, &res, sizeof(res));
#endif
}

inline void encode_fixed128_le(uint8_t* buf, uint128_t val) {
#if __BYTE_ORDER == __LITTLE_ENDIAN
    memcpy(buf, &val, sizeof(val));
#else
    uint128_t res = gbswap_128(val);
    memcpy(buf, &res, sizeof(res));
#endif
}

inline uint8_t decode_fixed8(const uint8_t* buf) {
    return *buf;
}

inline uint16_t decode_fixed16_le(const uint8_t* buf) {
    uint16_t res;
    memcpy(&res, buf, sizeof(res));
#if __BYTE_ORDER == __LITTLE_ENDIAN
    return res;
#else
    return bswap_16(res);
#endif
}

inline uint32_t decode_fixed32_le(const uint8_t* buf) {
    uint32_t res;
    memcpy(&res, buf, sizeof(res));
#if __BYTE_ORDER == __LITTLE_ENDIAN
    return res;
#else
    return bswap_32(res);
#endif
}

inline uint64_t decode_fixed64_le(const uint8_t* buf) {
    uint64_t res;
    memcpy(&res, buf, sizeof(res));
#if __BYTE_ORDER == __LITTLE_ENDIAN
    return res;
#else
    return gbswap_64(res);
#endif
}

inline uint128_t decode_fixed128_le(const uint8_t* buf) {
    uint128_t res;
    memcpy(&res, buf, sizeof(res));
#if __BYTE_ORDER == __LITTLE_ENDIAN
    return res;
#else
    return gbswap_128(res);
#endif
}

template <typename T>
inline void put_fixed32_le(T* dst, uint32_t val) {
    uint8_t buf[sizeof(val)];
    encode_fixed32_le(buf, val);
    dst->append((char*)buf, sizeof(buf));
}

template <typename T>
inline void put_fixed64_le(T* dst, uint64_t val) {
    uint8_t buf[sizeof(val)];
    encode_fixed64_le(buf, val);
    dst->append((char*)buf, sizeof(buf));
}

// Returns the length of the varint32 or varint64 encoding of "v"
inline int varint_length(uint64_t v) {
    int len = 1;
    while (v >= 128) {
        v >>= 7;
        len++;
    }
    return len;
}

template <typename T>
inline void put_fixed128_le(T* dst, uint128_t val) {
    uint8_t buf[sizeof(val)];
    encode_fixed128_le(buf, val);
    dst->append((char*)buf, sizeof(buf));
}

extern uint8_t* encode_varint32(uint8_t* dst, uint32_t value);
extern uint8_t* encode_varint64(uint8_t* dst, uint64_t value);

inline uint8_t* encode_varint64(uint8_t* dst, uint64_t v) {
    static const unsigned int B = 128;
    while (v >= B) {
        // Fetch low seven bits from current v, and the eight bit is marked as compression mark.
        // v | B is optimised from (v & (B-1)) | B, because result is assigned to uint8_t and other bits
        // is cleared by implicit conversion.
        *(dst++) = v | B;
        v >>= 7;
    }
    *(dst++) = static_cast<unsigned char>(v);
    return dst;
}

extern const uint8_t* decode_varint32_ptr_fallback(const uint8_t* p, const uint8_t* limit,
                                                   uint32_t* value);

inline const uint8_t* decode_varint32_ptr(const uint8_t* ptr, const uint8_t* limit,
                                          uint32_t* value) {
    if (ptr < limit) {
        uint32_t result = *ptr;
        if ((result & 128) == 0) {
            *value = result;
            return ptr + 1;
        }
    }
    return decode_varint32_ptr_fallback(ptr, limit, value);
}

extern const uint8_t* decode_varint64_ptr(const uint8_t* p, const uint8_t* limit, uint64_t* value);

template <typename T>
inline void put_varint32(T* dst, uint32_t v) {
    uint8_t buf[16];
    uint8_t* ptr = encode_varint32(buf, v);
    dst->append((char*)buf, static_cast<size_t>(ptr - buf));
}

template <typename T>
inline void put_varint64(T* dst, uint64_t v) {
    uint8_t buf[16];
    uint8_t* ptr = encode_varint64(buf, v);
    dst->append((char*)buf, static_cast<size_t>(ptr - buf));
}

template <typename T>
inline void put_length_prefixed_slice(T* dst, const Slice& value) {
    put_varint32(dst, value.get_size());
    dst->append(value.get_data(), value.get_size());
}

template <typename T>
inline void put_varint64_varint32(T* dst, uint64_t v1, uint32_t v2) {
    uint8_t buf[16];
    uint8_t* ptr = encode_varint64(buf, v1);
    ptr = encode_varint32(ptr, v2);
    dst->append((char*)buf, static_cast<size_t>(ptr - buf));
}

// parse a varint32 from the start of `input` into `val`.
// on success, return true and advance `input` past the parsed value.
// on failure, return false and `input` is not modified.
inline bool get_varint32(Slice* input, uint32_t* val) {
    const uint8_t* p = (const uint8_t*)input->data;
    const uint8_t* limit = p + input->size;
    const uint8_t* q = decode_varint32_ptr(p, limit, val);
    if (q == nullptr) {
        return false;
    } else {
        *input = Slice(q, limit - q);
        return true;
    }
}

// parse a varint64 from the start of `input` into `val`.
// on success, return true and advance `input` past the parsed value.
// on failure, return false and `input` is not modified.
inline bool get_varint64(Slice* input, uint64_t* val) {
    const uint8_t* p = (const uint8_t*)input->data;
    const uint8_t* limit = p + input->size;
    const uint8_t* q = decode_varint64_ptr(p, limit, val);
    if (q == nullptr) {
        return false;
    } else {
        *input = Slice(q, limit - q);
        return true;
    }
}

// parse a length-prefixed-slice from the start of `input` into `val`.
// on success, return true and advance `input` past the parsed value.
// on failure, return false and `input` may or may not be modified.
inline bool get_length_prefixed_slice(Slice* input, Slice* val) {
    uint32_t len;
    if (get_varint32(input, &len) && input->get_size() >= len) {
        *val = Slice(input->get_data(), len);
        input->remove_prefix(len);
        return true;
    } else {
        return false;
    }
}

} // namespace doris
