// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

#include <arpa/inet.h>
#include <zlib.h>

#include "checksum.h"
#include "compat.h"

namespace palo {

#define HT_F32_DO1(buf, i) \
    sum1 += ((uint16_t)buf[i] << 8) | buf[i + 1]; sum2 += sum1
#define HT_F32_DO2(buf, i)  HT_F32_DO1(buf, i); HT_F32_DO1(buf, i + 2);
#define HT_F32_DO4(buf, i)  HT_F32_DO2(buf, i); HT_F32_DO2(buf, i + 4);
#define HT_F32_DO8(buf, i)  HT_F32_DO4(buf, i); HT_F32_DO4(buf, i + 8);
#define HT_F32_DO16(buf, i) HT_F32_DO8(buf, i); HT_F32_DO8(buf, i + 16);

/* cf. http://en.wikipedia.org/wiki/Fletcher%27s_checksum
*/
uint32_t
fletcher32(const void *data8, size_t len8) {
    /* data may not be aligned properly and would segfault on
     * many systems if cast and used as 16-bit words
     */
    const uint8_t *data = (const uint8_t *)data8;
    uint32_t sum1 = 0xffff;
    uint32_t sum2 = 0xffff;
    size_t len = len8 / 2; /* loop works on 16-bit words */
    while (len) {
        /* 360 is the largest number of sums that can be
         * performed without integer overflow
         */
        unsigned tlen = len > 360 ? 360 : len;
        len -= tlen;
        if (tlen >= 16) {
            do {
                HT_F32_DO16(data, 0);
                data += 32;
                tlen -= 16;
            } while (tlen >= 16);
        }
        if (tlen != 0) {
            do {
                HT_F32_DO1(data, 0);
                data += 2;
            } while (--tlen);
        }
        sum1 = (sum1 & 0xffff) + (sum1 >> 16);
        sum2 = (sum2 & 0xffff) + (sum2 >> 16);
    }
    /* Check for odd number of bytes */
    if (len8 & 1) {
        sum1 += ((uint16_t)*data) << 8;
        sum2 += sum1;
        sum1 = (sum1 & 0xffff) + (sum1 >> 16);
        sum2 = (sum2 & 0xffff) + (sum2 >> 16);
    }
    /* Second reduction step to reduce sums to 16 bits */
    sum1 = (sum1 & 0xffff) + (sum1 >> 16);
    sum2 = (sum2 & 0xffff) + (sum2 >> 16);
    return (sum2 << 16) | sum1;
}

} // namespace palo
