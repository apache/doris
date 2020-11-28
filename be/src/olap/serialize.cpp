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

#include "olap/serialize.h"

#include "olap/file_stream.h"
#include "olap/out_stream.h"

namespace doris {
namespace ser {

OLAPStatus write_var_unsigned(OutStream* stream, int64_t value) {
    OLAPStatus res = OLAP_SUCCESS;

    while (res == OLAP_SUCCESS) {
        if ((value & ~0x7f) == 0) {
            return stream->write((char)value);
        } else {
            res = stream->write((char)(0x80 | (value & 0x7f)));
            value = ((uint64_t)value) >> 7;
        }
    }

    return res;
}

OLAPStatus read_var_unsigned(ReadOnlyFileStream* stream, int64_t* value) {
    OLAPStatus res;
    int64_t result = 0;
    uint32_t offset = 0;
    char byte = 0;

    do {
        res = stream->read(&byte);

        if (OLAP_UNLIKELY(res != OLAP_SUCCESS)) {
            OLAP_LOG_WARNING("fail to read stream. [res=%d]", res);
            return OLAP_ERR_COLUMN_DATA_READ_VAR_INT;
        }

        result |= ((int64_t)(0x7f & byte)) << offset;
        offset += 7;
    } while (byte & 0x80);

    *value = result;
    return OLAP_SUCCESS;
}

uint32_t find_closet_num_bits(int64_t value) {
    // counting leading zero, builtin function, this will generate BSR(Bit Scan Reverse)
    // instruction for X86
    if (value == 0) {
        return 1;
    }
    auto count = 64 - __builtin_clzll(value);
    return get_closet_fixed_bits(count);
}

OLAPStatus bytes_to_long_be(ReadOnlyFileStream* stream, int32_t n, int64_t* value) {
    OLAPStatus res = OLAP_SUCCESS;
    int64_t out = 0;

    // TODO(lijiao) : 为什么不直接用switch-case(会更快)
    while (n > 0) {
        --n;
        // store it in a long and then shift else integer overflow will occur
        uint8_t byte;
        res = stream->read((char*)&byte);

        if (OLAP_UNLIKELY(res != OLAP_SUCCESS)) {
            OLAP_LOG_WARNING("fail to read stream. [res=%d]", res);
            return res;
        }

        // 小端？
        out |= ((uint64_t)byte << (n * 8));
    }

    *value = out;
    return OLAP_SUCCESS;
}

uint32_t encode_bit_width(uint32_t n) {
    static uint8_t bits_map[65] = {
            ONE, // 0
            ONE,
            TWO,
            THREE,
            FOUR,
            FIVE,
            SIX,
            SEVEN,
            EIGHT, // 1 - 8
            NINE,
            TEN,
            ELEVEN,
            TWELVE,
            THIRTEEN,
            FOURTEEN,
            FIFTEEN,
            SIXTEEN, // 9 - 16
            // 17 - 24
            SEVENTEEN,
            EIGHTEEN,
            NINETEEN,
            TWENTY,
            TWENTYONE,
            TWENTYTWO,
            TWENTYTHREE,
            TWENTYFOUR,
            // 25 - 32
            TWENTYSIX,
            TWENTYSIX,
            TWENTYEIGHT,
            TWENTYEIGHT,
            THIRTY,
            THIRTY,
            THIRTYTWO,
            THIRTYTWO,
            // 33 - 40
            FORTY,
            FORTY,
            FORTY,
            FORTY,
            FORTY,
            FORTY,
            FORTY,
            FORTY,
            // 41 - 48
            FORTYEIGHT,
            FORTYEIGHT,
            FORTYEIGHT,
            FORTYEIGHT,
            FORTYEIGHT,
            FORTYEIGHT,
            FORTYEIGHT,
            FORTYEIGHT,
            // 49 - 56
            FIFTYSIX,
            FIFTYSIX,
            FIFTYSIX,
            FIFTYSIX,
            FIFTYSIX,
            FIFTYSIX,
            FIFTYSIX,
            FIFTYSIX,
            // 57 - 64
            SIXTYFOUR,
            SIXTYFOUR,
            SIXTYFOUR,
            SIXTYFOUR,
            SIXTYFOUR,
            SIXTYFOUR,
            SIXTYFOUR,
            SIXTYFOUR,
    };
    return bits_map[n];
}

uint32_t decode_bit_width(uint32_t n) {
    static uint8_t bits_map[SIXTYFOUR + 1] = {
            1,  2,  3,  4,  5,  6,  7,  8,  // ONE - EIGHT
            9,  10, 11, 12, 13, 14, 15, 16, // NINE - SIXTEEN
            17, 18, 19, 20, 21, 22, 23, 24, // SEVENTEEN - TWENTYFOUR
            26,                             // TWENTYSIX
            28,                             // TWENTYEIGHT
            30,                             // THIRTY
            32,                             // THIRTYTWO
            40,                             // FORTY
            48,                             // FORTYEIGHT
            56,                             // FIFTYSIX
            64                              // SIXTYFOUR
    };
    return bits_map[n];
}

uint32_t percentile_bits(int64_t* data, uint16_t count, double p) {
    // histogram that store the encoded bit requirement for each values.
    // maximum number of bits that can encoded is 32 (refer FixedBitSizes)
    uint16_t hist[65];
    memset(hist, 0, sizeof(hist));
    // compute the histogram
    for (uint32_t i = 0; i < count; i++) {
        hist[used_bits(data[i])]++;
    }
    int32_t per_len = (int32_t)(count * (1.0 - p));
    // return the bits required by pth percentile length
    for (int32_t i = 64; i >= 0; i--) {
        per_len -= hist[i];
        if (per_len < 0) {
            return get_closet_fixed_bits(i);
        }
    }
    return 0;
}

OLAPStatus write_ints(OutStream* output, int64_t* data, uint32_t count, uint32_t bit_width) {
    OLAPStatus res = OLAP_SUCCESS;
    uint32_t bits_left = 8;
    char current = 0;

    for (uint32_t i = 0; i < count; i++) {
        uint64_t value = (uint64_t)data[i];
        uint32_t bits_to_write = bit_width;

        while (bits_to_write > bits_left) {
            // add the bits to the bottom of the current word
            current |= value >> (bits_to_write - bits_left);
            // subtract out the bits we just added
            bits_to_write -= bits_left;
            // zero out the bits above bits_to_write
            value &= (1UL << bits_to_write) - 1;
            res = output->write(current);

            if (OLAP_UNLIKELY(res != OLAP_SUCCESS)) {
                OLAP_LOG_WARNING("fail to write byte to stream.[res=%d]", res);
                return res;
            }

            current = 0;
            bits_left = 8;
        }

        bits_left -= bits_to_write;
        current |= value << bits_left;

        if (bits_left == 0) {
            res = output->write(current);

            if (OLAP_UNLIKELY(res != OLAP_SUCCESS)) {
                OLAP_LOG_WARNING("fail to write byte to stream.[res=%d]", res);
                return res;
            }

            current = 0;
            bits_left = 8;
        }
    }

    if (bits_left != 8) {
        res = output->write(current);

        if (OLAP_UNLIKELY(res != OLAP_SUCCESS)) {
            OLAP_LOG_WARNING("fail to write byte to stream.[res=%d]", res);
            return res;
        }
    }

    return OLAP_SUCCESS;
}

OLAPStatus read_ints(ReadOnlyFileStream* input, int64_t* data, uint32_t count, uint32_t bit_width) {
    OLAPStatus res = OLAP_SUCCESS;
    uint32_t bits_left = 0;
    char current = '\0';

    uint32_t read_bytes = (count * bit_width - 1) / 8 + 1;
    uint32_t remaining_bytes = 0;
    char* buf = nullptr;
    input->get_buf(&buf, &remaining_bytes);
    if (read_bytes <= remaining_bytes) {
        uint32_t pos = 0;
        input->get_position(&pos);
        for (uint32_t i = 0; i < count; i++) {
            int64_t result = 0;
            uint32_t bits_left_to_read = bit_width;

            while (bits_left_to_read > bits_left) {
                result <<= bits_left;
                result |= current & ((1 << bits_left) - 1);
                bits_left_to_read -= bits_left;
                current = buf[pos++];
                bits_left = 8;
            }

            // handle the left over bits
            if (bits_left_to_read > 0) {
                result <<= bits_left_to_read;
                bits_left -= bits_left_to_read;
                result |= (current >> bits_left) & ((1 << bits_left_to_read) - 1);
            }

            data[i] = result;
        }
        input->set_position(pos);
    } else {
        for (uint32_t i = 0; i < count; i++) {
            int64_t result = 0;
            uint32_t bits_left_to_read = bit_width;

            while (bits_left_to_read > bits_left) {
                result <<= bits_left;
                result |= current & ((1 << bits_left) - 1);
                bits_left_to_read -= bits_left;
                res = input->read(&current);

                if (OLAP_UNLIKELY(OLAP_SUCCESS != res)) {
                    OLAP_LOG_WARNING("fail to write byte to stream.[res=%d]", res);
                    return res;
                }

                bits_left = 8;
            }

            // handle the left over bits
            if (bits_left_to_read > 0) {
                result <<= bits_left_to_read;
                bits_left -= bits_left_to_read;
                result |= (current >> bits_left) & ((1 << bits_left_to_read) - 1);
            }

            data[i] = result;
        }
    }

    return res;
}

} // namespace ser
} // namespace doris
