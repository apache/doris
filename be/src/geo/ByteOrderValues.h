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
// https://github.com/libgeos/geos/blob/main/include/geos/io/ByteOrderValues.h
// and modified by Doris

#pragma once

#include <sys/types.h>

using int64_t = __int64_t;
using int32_t = __int32_t;
using uint32_t = __uint32_t;

namespace doris {

/**
 * \class ByteOrderValues
 *
 * \brief Methods to read and write primitive datatypes from/to byte
 * sequences, allowing the byte order to be specified.
 *
 */
class ByteOrderValues {
public:
    enum EndianType { ENDIAN_BIG = 0, ENDIAN_LITTLE = 1 };

    static int32_t getInt(const unsigned char* buf, int byteOrder);
    static void putInt(int32_t intValue, unsigned char* buf, int byteOrder);

    static uint32_t getUnsigned(const unsigned char* buf, int byteOrder);
    static void putUnsigned(uint32_t intValue, unsigned char* buf, int byteOrder);

    static int64_t getLong(const unsigned char* buf, int byteOrder);
    static void putLong(int64_t longValue, unsigned char* buf, int byteOrder);

    static double getDouble(const unsigned char* buf, int byteOrder);
    static void putDouble(double doubleValue, unsigned char* buf, int byteOrder);
};

} // namespace doris
