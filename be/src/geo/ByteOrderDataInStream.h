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
//
// this file is copied from
// https://github.com/libgeos/geos/blob/main/include/geos/io/ByteOrderDataInStream.h
// and modified by Doris

#pragma once

#include <cstddef>

#include "ByteOrderValues.h"
#include "geo/geo_common.h"
#include "machine.h"
namespace doris {
class ByteOrderDataInStream {
public:
    ByteOrderDataInStream() : ByteOrderDataInStream(nullptr, 0) {};

    ByteOrderDataInStream(const unsigned char* buff, size_t buffsz)
            : byteOrder(getMachineByteOrder()), buf(buff), end(buff + buffsz) {};

    ~ByteOrderDataInStream() = default;

    void setOrder(int order) { byteOrder = order; };

    unsigned char readByte() {
        if (size() < 1) {
            return GEO_PARSE_WKB_SYNTAX_ERROR;
        }
        auto ret = buf[0];
        buf++;
        return ret;
    };

    int32_t readInt() {
        if (size() < 4) {
            return GEO_PARSE_WKB_SYNTAX_ERROR;
        }
        auto ret = ByteOrderValues::getInt(buf, byteOrder);
        buf += 4;
        return ret;
    };

    uint32_t readUnsigned() {
        if (size() < 4) {
            return GEO_PARSE_WKB_SYNTAX_ERROR;
        }
        auto ret = ByteOrderValues::getUnsigned(buf, byteOrder);
        buf += 4;
        return ret;
    };

    int64_t readLong() {
        if (size() < 8) {
            return GEO_PARSE_WKB_SYNTAX_ERROR;
        }

        auto ret = ByteOrderValues::getLong(buf, byteOrder);
        buf += 8;
        return ret;
    };

    double readDouble() {
        if (size() < 8) {
            return GEO_PARSE_WKB_SYNTAX_ERROR;
        }
        auto ret = ByteOrderValues::getDouble(buf, byteOrder);
        buf += 8;
        return ret;
    };

    size_t size() const { return static_cast<size_t>(end - buf); };

private:
    int byteOrder;
    const unsigned char* buf;
    const unsigned char* end;
};

} // namespace doris
