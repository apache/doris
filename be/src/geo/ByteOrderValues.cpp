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
// this file is copied from
// https://github.com/libgeos/geos/blob/main/src/io/ByteOrderValues.cpp
// and modified by Doris

#include "ByteOrderValues.h"

#include <stdint.h>

#include <cstring>

namespace doris {

int32_t ByteOrderValues::getInt(const unsigned char* buf, int byteOrder) {
    if (byteOrder == ENDIAN_BIG) {
        return ((int32_t)(buf[0] & 0xff) << 24) | ((int32_t)(buf[1] & 0xff) << 16) |
               ((int32_t)(buf[2] & 0xff) << 8) | ((int32_t)(buf[3] & 0xff));
    } else { // ENDIAN_LITTLE
        return ((int32_t)(buf[3] & 0xff) << 24) | ((int32_t)(buf[2] & 0xff) << 16) |
               ((int32_t)(buf[1] & 0xff) << 8) | ((int32_t)(buf[0] & 0xff));
    }
}

uint32_t ByteOrderValues::getUnsigned(const unsigned char* buf, int byteOrder) {
    if (byteOrder == ENDIAN_BIG) {
        return ((uint32_t)(buf[0] & 0xff) << 24) | ((uint32_t)(buf[1] & 0xff) << 16) |
               ((uint32_t)(buf[2] & 0xff) << 8) | ((uint32_t)(buf[3] & 0xff));
    } else { // ENDIAN_LITTLE
        return ((uint32_t)(buf[3] & 0xff) << 24) | ((uint32_t)(buf[2] & 0xff) << 16) |
               ((uint32_t)(buf[1] & 0xff) << 8) | ((uint32_t)(buf[0] & 0xff));
    }
}

void ByteOrderValues::putInt(int32_t intValue, unsigned char* buf, int byteOrder) {
    if (byteOrder == ENDIAN_BIG) {
        buf[0] = (unsigned char)(intValue >> 24);
        buf[1] = (unsigned char)(intValue >> 16);
        buf[2] = (unsigned char)(intValue >> 8);
        buf[3] = (unsigned char)intValue;
    } else { // ENDIAN_LITTLE
        buf[3] = (unsigned char)(intValue >> 24);
        buf[2] = (unsigned char)(intValue >> 16);
        buf[1] = (unsigned char)(intValue >> 8);
        buf[0] = (unsigned char)intValue;
    }
}

void ByteOrderValues::putUnsigned(uint32_t intValue, unsigned char* buf, int byteOrder) {
    if (byteOrder == ENDIAN_BIG) {
        buf[0] = (unsigned char)(intValue >> 24);
        buf[1] = (unsigned char)(intValue >> 16);
        buf[2] = (unsigned char)(intValue >> 8);
        buf[3] = (unsigned char)intValue;
    } else { // ENDIAN_LITTLE
        buf[3] = (unsigned char)(intValue >> 24);
        buf[2] = (unsigned char)(intValue >> 16);
        buf[1] = (unsigned char)(intValue >> 8);
        buf[0] = (unsigned char)intValue;
    }
}

int64_t ByteOrderValues::getLong(const unsigned char* buf, int byteOrder) {
    if (byteOrder == ENDIAN_BIG) {
        return (int64_t)(buf[0]) << 56 | (int64_t)(buf[1] & 0xff) << 48 |
               (int64_t)(buf[2] & 0xff) << 40 | (int64_t)(buf[3] & 0xff) << 32 |
               (int64_t)(buf[4] & 0xff) << 24 | (int64_t)(buf[5] & 0xff) << 16 |
               (int64_t)(buf[6] & 0xff) << 8 | (int64_t)(buf[7] & 0xff);
    } else { // ENDIAN_LITTLE
        return (int64_t)(buf[7]) << 56 | (int64_t)(buf[6] & 0xff) << 48 |
               (int64_t)(buf[5] & 0xff) << 40 | (int64_t)(buf[4] & 0xff) << 32 |
               (int64_t)(buf[3] & 0xff) << 24 | (int64_t)(buf[2] & 0xff) << 16 |
               (int64_t)(buf[1] & 0xff) << 8 | (int64_t)(buf[0] & 0xff);
    }
}

void ByteOrderValues::putLong(int64_t longValue, unsigned char* buf, int byteOrder) {
    if (byteOrder == ENDIAN_BIG) {
        buf[0] = (unsigned char)(longValue >> 56);
        buf[1] = (unsigned char)(longValue >> 48);
        buf[2] = (unsigned char)(longValue >> 40);
        buf[3] = (unsigned char)(longValue >> 32);
        buf[4] = (unsigned char)(longValue >> 24);
        buf[5] = (unsigned char)(longValue >> 16);
        buf[6] = (unsigned char)(longValue >> 8);
        buf[7] = (unsigned char)longValue;
    } else { // ENDIAN_LITTLE
        buf[0] = (unsigned char)longValue;
        buf[1] = (unsigned char)(longValue >> 8);
        buf[2] = (unsigned char)(longValue >> 16);
        buf[3] = (unsigned char)(longValue >> 24);
        buf[4] = (unsigned char)(longValue >> 32);
        buf[5] = (unsigned char)(longValue >> 40);
        buf[6] = (unsigned char)(longValue >> 48);
        buf[7] = (unsigned char)(longValue >> 56);
    }
}

double ByteOrderValues::getDouble(const unsigned char* buf, int byteOrder) {
    int64_t longValue = getLong(buf, byteOrder);
    double ret;
    std::memcpy(&ret, &longValue, sizeof(double));
    return ret;
}

void ByteOrderValues::putDouble(double doubleValue, unsigned char* buf, int byteOrder) {
    int64_t longValue;
    std::memcpy(&longValue, &doubleValue, sizeof(double));
    putLong(longValue, buf, byteOrder);
}

} // namespace doris
