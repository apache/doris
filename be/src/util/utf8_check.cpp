// Copyright (c) cyb70289(https://github.com/cyb70289). All rights reserved.
// Use of this source code is governed by a MIT license that can be
// found in the LICENSE file.

/*
 * These functions are used for validating utf8 string.
 * Details can be seen here: https://github.com/cyb70289/utf8/
 */

#include "util/utf8_check.h"

#if defined(__i386) || defined(__x86_64__)
#include "util/simdutf8check.h"
#elif defined(__aarch64__)
#include <arm_neon.h>
#endif

/*
 * http://www.unicode.org/versions/Unicode6.0.0/ch03.pdf - page 94
 *
 * Table 3-7. Well-Formed UTF-8 Byte Sequences
 *
 * +--------------------+------------+-------------+------------+-------------+
 * | Code Points        | First Byte | Second Byte | Third Byte | Fourth Byte |
 * +--------------------+------------+-------------+------------+-------------+
 * | U+0000..U+007F     | 00..7F     |             |            |             |
 * +--------------------+------------+-------------+------------+-------------+
 * | U+0080..U+07FF     | C2..DF     | 80..BF      |            |             |
 * +--------------------+------------+-------------+------------+-------------+
 * | U+0800..U+0FFF     | E0         | A0..BF      | 80..BF     |             |
 * +--------------------+------------+-------------+------------+-------------+
 * | U+1000..U+CFFF     | E1..EC     | 80..BF      | 80..BF     |             |
 * +--------------------+------------+-------------+------------+-------------+
 * | U+D000..U+D7FF     | ED         | 80..9F      | 80..BF     |             |
 * +--------------------+------------+-------------+------------+-------------+
 * | U+E000..U+FFFF     | EE..EF     | 80..BF      | 80..BF     |             |
 * +--------------------+------------+-------------+------------+-------------+
 * | U+10000..U+3FFFF   | F0         | 90..BF      | 80..BF     | 80..BF      |
 * +--------------------+------------+-------------+------------+-------------+
 * | U+40000..U+FFFFF   | F1..F3     | 80..BF      | 80..BF     | 80..BF      |
 * +--------------------+------------+-------------+------------+-------------+
 * | U+100000..U+10FFFF | F4         | 80..8F      | 80..BF     | 80..BF      |
 * +--------------------+------------+-------------+------------+-------------+
 */
namespace doris {
bool validate_utf8_naive(const char* data, size_t len) {
    while (len) {
        int bytes;
        const unsigned char byte1 = data[0];

        /* 00..7F */
        if (byte1 <= 0x7F) {
            bytes = 1;
            /* C2..DF, 80..BF */
        } else if (len >= 2 && byte1 >= 0xC2 && byte1 <= 0xDF &&
                   (signed char)data[1] <= (signed char)0xBF) {
            bytes = 2;
        } else if (len >= 3) {
            const unsigned char byte2 = data[1];

            /* Is byte2, byte3 between 0x80 ~ 0xBF */
            const int byte2_ok = (signed char)byte2 <= (signed char)0xBF;
            const int byte3_ok = (signed char)data[2] <= (signed char)0xBF;

            if (byte2_ok && byte3_ok &&
                /* E0, A0..BF, 80..BF */
                ((byte1 == 0xE0 && byte2 >= 0xA0) ||
                 /* E1..EC, 80..BF, 80..BF */
                 (byte1 >= 0xE1 && byte1 <= 0xEC) ||
                 /* ED, 80..9F, 80..BF */
                 (byte1 == 0xED && byte2 <= 0x9F) ||
                 /* EE..EF, 80..BF, 80..BF */
                 (byte1 >= 0xEE && byte1 <= 0xEF))) {
                bytes = 3;
            } else if (len >= 4) {
                /* Is byte4 between 0x80 ~ 0xBF */
                const int byte4_ok = (signed char)data[3] <= (signed char)0xBF;

                if (byte2_ok && byte3_ok && byte4_ok &&
                    /* F0, 90..BF, 80..BF, 80..BF */
                    ((byte1 == 0xF0 && byte2 >= 0x90) ||
                     /* F1..F3, 80..BF, 80..BF, 80..BF */
                     (byte1 >= 0xF1 && byte1 <= 0xF3) ||
                     /* F4, 80..8F, 80..BF, 80..BF */
                     (byte1 == 0xF4 && byte2 <= 0x8F))) {
                    bytes = 4;
                } else {
                    return false;
                }
            } else {
                return false;
            }
        } else {
            return false;
        }

        len -= bytes;
        data += bytes;
    }

    return true;
}

#if defined(__i386) || defined(__x86_64__)
bool validate_utf8(const char* src, size_t len) {
    return validate_utf8_fast(src, len);
}
#elif defined(__aarch64__)
/*
 * Map high nibble of "First Byte" to legal character length minus 1
 * 0x00 ~ 0xBF --> 0
 * 0xC0 ~ 0xDF --> 1
 * 0xE0 ~ 0xEF --> 2
 * 0xF0 ~ 0xFF --> 3
 */
const uint8_t _first_len_tbl[] = {
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 2, 3,
};

/* Map "First Byte" to 8-th item of range table (0xC2 ~ 0xF4) */
static const uint8_t _first_range_tbl[] = {
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 8, 8, 8,
};

/*
 * Range table, map range index to min and max values
 * Index 0    : 00 ~ 7F (First Byte, ascii)
 * Index 1,2,3: 80 ~ BF (Second, Third, Fourth Byte)
 * Index 4    : A0 ~ BF (Second Byte after E0)
 * Index 5    : 80 ~ 9F (Second Byte after ED)
 * Index 6    : 90 ~ BF (Second Byte after F0)
 * Index 7    : 80 ~ 8F (Second Byte after F4)
 * Index 8    : C2 ~ F4 (First Byte, non ascii)
 * Index 9~15 : illegal: u >= 255 && u <= 0
 */
static const uint8_t _range_min_tbl[] = {
        0x00, 0x80, 0x80, 0x80, 0xA0, 0x80, 0x90, 0x80,
        0xC2, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
};
static const uint8_t _range_max_tbl[] = {
        0x7F, 0xBF, 0xBF, 0xBF, 0xBF, 0x9F, 0xBF, 0x8F,
        0xF4, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
};

/*
 * This table is for fast handling four special First Bytes(E0,ED,F0,F4), after
 * which the Second Byte are not 80~BF. It contains "range index adjustment".
 * - The idea is to minus byte with E0, use the result(0~31) as the index to
 *   lookup the "range index adjustment". Then add the adjustment to original
 *   range index to get the correct range.
 * - Range index adjustment
 *   +------------+---------------+------------------+----------------+
 *   | First Byte | original range| range adjustment | adjusted range |
 *   +------------+---------------+------------------+----------------+
 *   | E0         | 2             | 2                | 4              |
 *   +------------+---------------+------------------+----------------+
 *   | ED         | 2             | 3                | 5              |
 *   +------------+---------------+------------------+----------------+
 *   | F0         | 3             | 3                | 6              |
 *   +------------+---------------+------------------+----------------+
 *   | F4         | 4             | 4                | 8              |
 *   +------------+---------------+------------------+----------------+
 * - Below is a uint8x16x2 table, data is interleaved in NEON register. So I'm
 *   putting it vertically. 1st column is for E0~EF, 2nd column for F0~FF.
 */
static const uint8_t _range_adjust_tbl[] = {
        /* index -> 0~15  16~31 <- index */
        /*  E0 -> */ 2,
        3, /* <- F0  */
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        4, /* <- F4  */
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        /*  ED -> */ 3,
        0,
        0,
        0,
        0,
        0,
};

/* 2x ~ 4x faster than naive method */
/* Return true on success, false on error */
bool utf8_range(const char* data, size_t len) {
    if (len >= 16) {
        uint8x16_t prev_input = vdupq_n_u8(0);
        uint8x16_t prev_first_len = vdupq_n_u8(0);

        /* Cached tables */
        const uint8x16_t first_len_tbl = vld1q_u8(_first_len_tbl);
        const uint8x16_t first_range_tbl = vld1q_u8(_first_range_tbl);
        const uint8x16_t range_min_tbl = vld1q_u8(_range_min_tbl);
        const uint8x16_t range_max_tbl = vld1q_u8(_range_max_tbl);
        const uint8x16x2_t range_adjust_tbl = vld2q_u8(_range_adjust_tbl);

        /* Cached values */
        const uint8x16_t const_1 = vdupq_n_u8(1);
        const uint8x16_t const_2 = vdupq_n_u8(2);
        const uint8x16_t const_e0 = vdupq_n_u8(0xE0);

        uint8x16_t error = vdupq_n_u8(0);

        while (len >= 16) {
            const uint8x16_t input = vld1q_u8((const uint8_t*)data);

            /* high_nibbles = input >> 4 */
            const uint8x16_t high_nibbles = vshrq_n_u8(input, 4);

            /* first_len = legal character length minus 1 */
            /* 0 for 00~7F, 1 for C0~DF, 2 for E0~EF, 3 for F0~FF */
            /* first_len = first_len_tbl[high_nibbles] */
            const uint8x16_t first_len = vqtbl1q_u8(first_len_tbl, high_nibbles);

            /* First Byte: set range index to 8 for bytes within 0xC0 ~ 0xFF */
            /* range = first_range_tbl[high_nibbles] */
            uint8x16_t range = vqtbl1q_u8(first_range_tbl, high_nibbles);

            /* Second Byte: set range index to first_len */
            /* 0 for 00~7F, 1 for C0~DF, 2 for E0~EF, 3 for F0~FF */
            /* range |= (first_len, prev_first_len) << 1 byte */
            range = vorrq_u8(range, vextq_u8(prev_first_len, first_len, 15));

            /* Third Byte: set range index to saturate_sub(first_len, 1) */
            /* 0 for 00~7F, 0 for C0~DF, 1 for E0~EF, 2 for F0~FF */
            uint8x16_t tmp1, tmp2;
            /* tmp1 = saturate_sub(first_len, 1) */
            tmp1 = vqsubq_u8(first_len, const_1);
            /* tmp2 = saturate_sub(prev_first_len, 1) */
            tmp2 = vqsubq_u8(prev_first_len, const_1);
            /* range |= (tmp1, tmp2) << 2 bytes */
            range = vorrq_u8(range, vextq_u8(tmp2, tmp1, 14));

            /* Fourth Byte: set range index to saturate_sub(first_len, 2) */
            /* 0 for 00~7F, 0 for C0~DF, 0 for E0~EF, 1 for F0~FF */
            /* tmp1 = saturate_sub(first_len, 2) */
            tmp1 = vqsubq_u8(first_len, const_2);
            /* tmp2 = saturate_sub(prev_first_len, 2) */
            tmp2 = vqsubq_u8(prev_first_len, const_2);
            /* range |= (tmp1, tmp2) << 3 bytes */
            range = vorrq_u8(range, vextq_u8(tmp2, tmp1, 13));

            /*
             * Now we have below range indices caluclated
             * Correct cases:
             * - 8 for C0~FF
             * - 3 for 1st byte after F0~FF
             * - 2 for 1st byte after E0~EF or 2nd byte after F0~FF
             * - 1 for 1st byte after C0~DF or 2nd byte after E0~EF or
             *         3rd byte after F0~FF
             * - 0 for others
             * Error cases:
             *   9,10,11 if non ascii First Byte overlaps
             *   E.g., F1 80 C2 90 --> 8 3 10 2, where 10 indicates error
             */

            /* Adjust Second Byte range for special First Bytes(E0,ED,F0,F4) */
            /* See _range_adjust_tbl[] definition for details */
            /* Overlaps lead to index 9~15, which are illegal in range table */
            uint8x16_t shift1 = vextq_u8(prev_input, input, 15);
            uint8x16_t pos = vsubq_u8(shift1, const_e0);
            range = vaddq_u8(range, vqtbl2q_u8(range_adjust_tbl, pos));

            /* Load min and max values per calculated range index */
            uint8x16_t minv = vqtbl1q_u8(range_min_tbl, range);
            uint8x16_t maxv = vqtbl1q_u8(range_max_tbl, range);

            /* Check value range */
            error = vorrq_u8(error, vcltq_u8(input, minv));
            error = vorrq_u8(error, vcgtq_u8(input, maxv));

            prev_input = input;
            prev_first_len = first_len;

            data += 16;
            len -= 16;
        }

        /* Delay error check till loop ends */
        if (vmaxvq_u8(error)) return false;

        /* Find previous token (not 80~BF) */
        uint32_t token4;
        vst1q_lane_u32(&token4, vreinterpretq_u32_u8(prev_input), 3);

        const int8_t* token = (const int8_t*)&token4;
        int lookahead = 0;
        if (token[3] > (int8_t)0xBF)
            lookahead = 1;
        else if (token[2] > (int8_t)0xBF)
            lookahead = 2;
        else if (token[1] > (int8_t)0xBF)
            lookahead = 3;

        data -= lookahead;
        len += lookahead;
    }

    /* Check remaining bytes with naive method */
    return validate_utf8_naive(data, len);
}

bool validate_utf8(const char* src, size_t len) {
    return utf8_range(src, len);
}
#else
bool validate_utf8(const char* src, size_t len) {
    return validate_utf8_naive(src, len);
}
#endif

bool validate_utf8(const TFileScanRangeParams& params, const char* src, size_t len) {
    if (params.__isset.file_attributes && !params.file_attributes.enable_text_validate_utf8) {
        return true;
    }
    return validate_utf8(src, len);
}
} // namespace doris
