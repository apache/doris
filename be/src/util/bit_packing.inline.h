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

// the implement of BitPacking is from impala

#include <boost/preprocessor/repetition/repeat_from_to.hpp>

#include "util/bit_packing.h"

namespace doris {

inline int64_t BitPacking::NumValuesToUnpack(int bit_width, int64_t in_bytes, int64_t num_values) {
    // Check if we have enough input bytes to decode 'num_values'.
    if (bit_width == 0 || BitUtil::RoundUpNumBytes(num_values * bit_width) <= in_bytes) {
        // Limited by output space.
        return num_values;
    } else {
        // Limited by the number of input bytes. Compute the number of values that can be
        // unpacked from the input.
        return (in_bytes * CHAR_BIT) / bit_width;
    }
}

constexpr uint64_t GetMask(int num_bits) {
    if (num_bits >= 64) {
        return ~0L;
    }
    return (1ULL << num_bits) - 1;
}

template <typename T>
constexpr bool IsSupportedUnpackingType() {
    return std::is_same<T, uint8_t>::value || std::is_same<T, uint16_t>::value ||
           std::is_same<T, uint32_t>::value || std::is_same<T, uint64_t>::value;
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValues(int bit_width,
                                                            const uint8_t* __restrict__ in,
                                                            int64_t in_bytes, int64_t num_values,
                                                            OutType* __restrict__ out) {
    static_assert(IsSupportedUnpackingType<OutType>(), "Only unsigned integers are supported.");

#pragma push_macro("UNPACK_VALUES_CASE")
#define UNPACK_VALUES_CASE(ignore1, i, ignore2) \
    case i:                                     \
        return UnpackValues<OutType, i>(in, in_bytes, num_values, out);

    switch (bit_width) {
        // Expand cases from 0 to 64.
        BOOST_PP_REPEAT_FROM_TO(0, 65, UNPACK_VALUES_CASE, ignore);
    default:
        DCHECK(false);
        return std::make_pair(nullptr, -1);
    }
#pragma pop_macro("UNPACK_VALUES_CASE")
}

template <typename OutType, int BIT_WIDTH>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValues(const uint8_t* __restrict__ in,
                                                            int64_t in_bytes, int64_t num_values,
                                                            OutType* __restrict__ out) {
    static_assert(IsSupportedUnpackingType<OutType>(), "Only unsigned integers are supported.");

    constexpr int BATCH_SIZE = 32;
    const int64_t values_to_read = NumValuesToUnpack(BIT_WIDTH, in_bytes, num_values);
    const int64_t batches_to_read = values_to_read / BATCH_SIZE;
    const int64_t remainder_values = values_to_read % BATCH_SIZE;
    const uint8_t* in_pos = in;
    OutType* out_pos = out;

    // First unpack as many full batches as possible.
    for (int64_t i = 0; i < batches_to_read; ++i) {
        in_pos = Unpack32Values<OutType, BIT_WIDTH>(in_pos, in_bytes, out_pos);
        out_pos += BATCH_SIZE;
        in_bytes -= (BATCH_SIZE * BIT_WIDTH) / CHAR_BIT;
    }

    // Then unpack the final partial batch.
    if (remainder_values > 0) {
        in_pos =
                UnpackUpTo31Values<OutType, BIT_WIDTH>(in_pos, in_bytes, remainder_values, out_pos);
    }
    return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackAndDecodeValues(
        int bit_width, const uint8_t* __restrict__ in, int64_t in_bytes, OutType* __restrict__ dict,
        int64_t dict_len, int64_t num_values, OutType* __restrict__ out, int64_t stride,
        bool* __restrict__ decode_error) {
#pragma push_macro("UNPACK_VALUES_CASE")
#define UNPACK_VALUES_CASE(ignore1, i, ignore2)                                                 \
    case i:                                                                                     \
        return UnpackAndDecodeValues<OutType, i>(in, in_bytes, dict, dict_len, num_values, out, \
                                                 stride, decode_error);

    switch (bit_width) {
        // Expand cases from 0 to MAX_DICT_BITWIDTH.
        BOOST_PP_REPEAT_FROM_TO(0, 33, UNPACK_VALUES_CASE, ignore);
    default:
        DCHECK(false);
        return std::make_pair(nullptr, -1);
    }
#pragma pop_macro("UNPACK_VALUES_CASE")
}
template <typename OutType, int BIT_WIDTH>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackAndDecodeValues(
        const uint8_t* __restrict__ in, int64_t in_bytes, OutType* __restrict__ dict,
        int64_t dict_len, int64_t num_values, OutType* __restrict__ out, int64_t stride,
        bool* __restrict__ decode_error) {
    constexpr int BATCH_SIZE = 32;
    const int64_t values_to_read = NumValuesToUnpack(BIT_WIDTH, in_bytes, num_values);
    const int64_t batches_to_read = values_to_read / BATCH_SIZE;
    const int64_t remainder_values = values_to_read % BATCH_SIZE;
    const uint8_t* in_pos = in;
    uint8_t* out_pos = reinterpret_cast<uint8_t*>(out);
    // First unpack as many full batches as possible.
    for (int64_t i = 0; i < batches_to_read; ++i) {
        in_pos = UnpackAndDecode32Values<OutType, BIT_WIDTH>(in_pos, in_bytes, dict, dict_len,
                                                             reinterpret_cast<OutType*>(out_pos),
                                                             stride, decode_error);
        out_pos += stride * BATCH_SIZE;
        in_bytes -= (BATCH_SIZE * BIT_WIDTH) / CHAR_BIT;
    }
    // Then unpack the final partial batch.
    if (remainder_values > 0) {
        in_pos = UnpackAndDecodeUpTo31Values<OutType, BIT_WIDTH>(
                in_pos, in_bytes, dict, dict_len, remainder_values,
                reinterpret_cast<OutType*>(out_pos), stride, decode_error);
    }
    return std::make_pair(in_pos, values_to_read);
}

// Loop body of unrolled loop that unpacks the value. BIT_WIDTH is the bit width of
// the packed values. 'in_buf' is the start of the input buffer and 'out_vals' is the
// start of the output values array. This function unpacks the VALUE_IDX'th packed value
// from 'in_buf'.
//
// This implements essentially the same algorithm as the (Apache-licensed) code in
// bpacking.c at https://github.com/lemire/FrameOfReference/, but is much more compact
// because it uses templates rather than source-level unrolling of all combinations.
//
// After the template parameters are expanded and constants are propagated, all branches
// and offset/shift calculations should be optimized out, leaving only shifts by constants
// and bitmasks by constants. Calls to this must be stamped out manually or with
// BOOST_PP_REPEAT_FROM_TO: experimentation revealed that the GCC 4.9.2 optimiser was
// not able to fully propagate constants and remove branches when this was called from
// inside a for loop with constant bounds with VALUE_IDX changed to a function argument.
//
// We compute how many 32 bit words we have to read, which is either 1, 2 or 3. If it is
// at least 2, the first two 32 bit words are read as one 64 bit word. Even if only one
// word needs to be read, we try to read 64 bits if it does not lead to buffer overflow
// because benchmarks show that it has a positive effect on performance.
//
// If 'FULL_BATCH' is true, this function call is part of unpacking 32 values, otherwise
// up to 31 values. This is needed to optimise the length of the reads (32 or 64 bits) and
// avoid buffer overflow (if we are unpacking 32 values, we can safely assume an input
// buffer of length 32 * BIT_WIDTH).
template <int BIT_WIDTH, int VALUE_IDX, bool FULL_BATCH>
inline uint64_t ALWAYS_INLINE UnpackValue(const uint8_t* __restrict__ in_buf) {
    if (BIT_WIDTH == 0) return 0;

    constexpr int FIRST_BIT_IDX = VALUE_IDX * BIT_WIDTH;
    constexpr int FIRST_WORD_IDX = FIRST_BIT_IDX / 32;
    constexpr int LAST_BIT_IDX = FIRST_BIT_IDX + BIT_WIDTH;
    constexpr int LAST_WORD_IDX = BitUtil::round_up_numi32(LAST_BIT_IDX);
    constexpr int WORDS_TO_READ = LAST_WORD_IDX - FIRST_WORD_IDX;
    static_assert(WORDS_TO_READ <= 3, "At most three 32-bit words need to be loaded.");

    constexpr int FIRST_BIT_OFFSET = FIRST_BIT_IDX - FIRST_WORD_IDX * 32;
    constexpr uint64_t mask = GetMask(BIT_WIDTH);
    const uint32_t* const in = reinterpret_cast<const uint32_t*>(in_buf);

    // Avoid reading past the end of the buffer. We can safely read 64 bits if we know that
    // this is a full batch read (so the input buffer is 32 * BIT_WIDTH long) and there is
    // enough space in the buffer from the current reading point.
    // We try to read 64 bits even when it is not necessary because the benchmarks show it
    // is faster.
    constexpr bool CAN_SAFELY_READ_64_BITS =
            FULL_BATCH && FIRST_BIT_IDX - FIRST_BIT_OFFSET + 64 <= BIT_WIDTH * 32;

    // We do not try to read 64 bits when the bit width is a power of two (unless it is
    // necessary) because performance benchmarks show that it is better this way. This seems
    // to be due to compiler optimisation issues, so we can revisit it when we update the
    // compiler version.
    constexpr bool READ_32_BITS =
            WORDS_TO_READ == 1 && (!CAN_SAFELY_READ_64_BITS || BitUtil::IsPowerOf2(BIT_WIDTH));

    if (READ_32_BITS) {
        uint32_t word = in[FIRST_WORD_IDX];
        word >>= FIRST_BIT_OFFSET < 32 ? FIRST_BIT_OFFSET : 0;
        return word & mask;
    }

    uint64_t word = *reinterpret_cast<const uint64_t*>(in + FIRST_WORD_IDX);
    word >>= FIRST_BIT_OFFSET;

    if (WORDS_TO_READ > 2) {
        constexpr int USEFUL_BITS = FIRST_BIT_OFFSET == 0 ? 0 : 64 - FIRST_BIT_OFFSET;
        uint64_t extra_word = in[FIRST_WORD_IDX + 2];
        word |= extra_word << USEFUL_BITS;
    }

    return word & mask;
}

template <typename OutType>
inline void ALWAYS_INLINE DecodeValue(OutType* __restrict__ dict, int64_t dict_len, uint32_t idx,
                                      OutType* __restrict__ out_val,
                                      bool* __restrict__ decode_error) {
    if (UNLIKELY(idx >= dict_len)) {
        *decode_error = true;
    } else {
        // Use memcpy() because we can't assume sufficient alignment in some cases (e.g.
        // 16 byte decimals).
        memcpy(out_val, &dict[idx], sizeof(OutType));
    }
}

template <typename OutType, int BIT_WIDTH>
const uint8_t* BitPacking::Unpack32Values(const uint8_t* __restrict__ in, int64_t in_bytes,
                                          OutType* __restrict__ out) {
    static_assert(BIT_WIDTH >= 0, "BIT_WIDTH too low");
    static_assert(BIT_WIDTH <= MAX_BITWIDTH, "BIT_WIDTH too high");
    DCHECK_LE(BIT_WIDTH, sizeof(OutType) * CHAR_BIT) << "BIT_WIDTH too high for output";
    constexpr int BYTES_TO_READ = BitUtil::RoundUpNumBytes(32 * BIT_WIDTH);
    DCHECK_GE(in_bytes, BYTES_TO_READ);

    // Call UnpackValue for 0 <= i < 32.
#pragma push_macro("UNPACK_VALUE_CALL")
#define UNPACK_VALUE_CALL(ignore1, i, ignore2) \
    out[i] = static_cast<OutType>(UnpackValue<BIT_WIDTH, i, true>(in));

    BOOST_PP_REPEAT_FROM_TO(0, 32, UNPACK_VALUE_CALL, ignore);
    return in + BYTES_TO_READ;
#pragma pop_macro("UNPACK_VALUE_CALL")
}

template <typename OutType>
const uint8_t* BitPacking::Unpack32Values(int bit_width, const uint8_t* __restrict__ in,
                                          int64_t in_bytes, OutType* __restrict__ out) {
#pragma push_macro("UNPACK_VALUES_CASE")
#define UNPACK_VALUES_CASE(ignore1, i, ignore2) \
    case i:                                     \
        return Unpack32Values<OutType, i>(in, in_bytes, out);

    switch (bit_width) {
        // Expand cases from 0 to 64.
        BOOST_PP_REPEAT_FROM_TO(0, 65, UNPACK_VALUES_CASE, ignore);
    default:
        DCHECK(false);
        return in;
    }
#pragma pop_macro("UNPACK_VALUES_CASE")
}

template <typename OutType, int BIT_WIDTH>
const uint8_t* BitPacking::UnpackAndDecode32Values(const uint8_t* __restrict__ in, int64_t in_bytes,
                                                   OutType* __restrict__ dict, int64_t dict_len,
                                                   OutType* __restrict__ out, int64_t stride,
                                                   bool* __restrict__ decode_error) {
    static_assert(BIT_WIDTH >= 0, "BIT_WIDTH too low");
    static_assert(BIT_WIDTH <= MAX_BITWIDTH, "BIT_WIDTH too high");
    constexpr int BYTES_TO_READ = BitUtil::RoundUpNumBytes(32 * BIT_WIDTH);
    DCHECK_GE(in_bytes, BYTES_TO_READ);
    // TODO: this could be optimised further by using SIMD instructions.
    // https://lemire.me/blog/2016/08/25/faster-dictionary-decoding-with-simd-instructions/

    static_assert(BIT_WIDTH <= MAX_DICT_BITWIDTH, "Too high bit width for dictionary index.");

    // Call UnpackValue() and DecodeValue() for 0 <= i < 32.
#pragma push_macro("DECODE_VALUE_CALL")
#define DECODE_VALUE_CALL(ignore1, i, ignore2)                                               \
    {                                                                                        \
        uint32_t idx = UnpackValue<BIT_WIDTH, i, true>(in);                                  \
        uint8_t* out_pos = reinterpret_cast<uint8_t*>(out) + i * stride;                     \
        DecodeValue(dict, dict_len, idx, reinterpret_cast<OutType*>(out_pos), decode_error); \
    }

    BOOST_PP_REPEAT_FROM_TO(0, 32, DECODE_VALUE_CALL, ignore);
    return in + BYTES_TO_READ;
#pragma pop_macro("DECODE_VALUE_CALL")
}

template <typename OutType, int BIT_WIDTH>
const uint8_t* BitPacking::UnpackUpTo31Values(const uint8_t* __restrict__ in, int64_t in_bytes,
                                              int num_values, OutType* __restrict__ out) {
    static_assert(BIT_WIDTH >= 0, "BIT_WIDTH too low");
    static_assert(BIT_WIDTH <= MAX_BITWIDTH, "BIT_WIDTH too high");
    DCHECK_LE(BIT_WIDTH, sizeof(OutType) * CHAR_BIT) << "BIT_WIDTH too high for output";
    constexpr int MAX_BATCH_SIZE = 31;
    const int BYTES_TO_READ = BitUtil::RoundUpNumBytes(num_values * BIT_WIDTH);
    DCHECK_GE(in_bytes, BYTES_TO_READ);
    DCHECK_LE(num_values, MAX_BATCH_SIZE);

    // Make sure the buffer is at least 1 byte.
    constexpr int TMP_BUFFER_SIZE = BIT_WIDTH ? (BIT_WIDTH * (MAX_BATCH_SIZE + 1)) / CHAR_BIT : 1;
    uint8_t tmp_buffer[TMP_BUFFER_SIZE];

    const uint8_t* in_buffer = in;
    // Copy into padded temporary buffer to avoid reading past the end of 'in' if the
    // last 32-bit load would go past the end of the buffer.
    if (BitUtil::round_up(BYTES_TO_READ, sizeof(uint32_t)) > in_bytes) {
        memcpy(tmp_buffer, in, BYTES_TO_READ);
        in_buffer = tmp_buffer;
    }

#pragma push_macro("UNPACK_VALUES_CASE")
#define UNPACK_VALUES_CASE(ignore1, i, ignore2) \
    case 31 - i:                                \
        out[30 - i] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 30 - i, false>(in_buffer));

    // Use switch with fall-through cases to minimise branching.
    switch (num_values) {
        // Expand cases from 31 down to 1.
        BOOST_PP_REPEAT_FROM_TO(0, 31, UNPACK_VALUES_CASE, ignore);
    case 0:
        break;
    default:
        DCHECK(false);
    }
    return in + BYTES_TO_READ;
#pragma pop_macro("UNPACK_VALUES_CASE")
}

template <typename OutType, int BIT_WIDTH>
const uint8_t* BitPacking::UnpackAndDecodeUpTo31Values(const uint8_t* __restrict__ in,
                                                       int64_t in_bytes, OutType* __restrict__ dict,
                                                       int64_t dict_len, int num_values,
                                                       OutType* __restrict__ out, int64_t stride,
                                                       bool* __restrict__ decode_error) {
    static_assert(BIT_WIDTH >= 0, "BIT_WIDTH too low");
    static_assert(BIT_WIDTH <= MAX_BITWIDTH, "BIT_WIDTH too high");
    constexpr int MAX_BATCH_SIZE = 31;
    const int BYTES_TO_READ = BitUtil::RoundUpNumBytes(num_values * BIT_WIDTH);
    DCHECK_GE(in_bytes, BYTES_TO_READ);
    DCHECK_LE(num_values, MAX_BATCH_SIZE);

    // Make sure the buffer is at least 1 byte.
    constexpr int TMP_BUFFER_SIZE = BIT_WIDTH ? (BIT_WIDTH * (MAX_BATCH_SIZE + 1)) / CHAR_BIT : 1;
    uint8_t tmp_buffer[TMP_BUFFER_SIZE];

    const uint8_t* in_buffer = in;
    // Copy into padded temporary buffer to avoid reading past the end of 'in' if the
    // last 32-bit load would go past the end of the buffer.
    if (BitUtil::round_up(BYTES_TO_READ, sizeof(uint32_t)) > in_bytes) {
        memcpy(tmp_buffer, in, BYTES_TO_READ);
        in_buffer = tmp_buffer;
    }

#pragma push_macro("DECODE_VALUES_CASE")
#define DECODE_VALUES_CASE(ignore1, i, ignore2)                                              \
    case 31 - i: {                                                                           \
        uint32_t idx = UnpackValue<BIT_WIDTH, 30 - i, false>(in_buffer);                     \
        uint8_t* out_pos = reinterpret_cast<uint8_t*>(out) + (30 - i) * stride;              \
        DecodeValue(dict, dict_len, idx, reinterpret_cast<OutType*>(out_pos), decode_error); \
    }

    // Use switch with fall-through cases to minimise branching.
    switch (num_values) {
        // Expand cases from 31 down to 1.
        BOOST_PP_REPEAT_FROM_TO(0, 31, DECODE_VALUES_CASE, ignore);
    case 0:
        break;
    default:
        DCHECK(false);
    }
    return in + BYTES_TO_READ;
#pragma pop_macro("DECODE_VALUES_CASE")
}

} // namespace doris
