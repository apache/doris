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

#pragma once

#include <stdint.h>

namespace cctz {
class time_zone;
}
namespace doris {

/**
// Now only support text protocol
 * helper for construct MySQL send row
 * The MYSQL protocol:
 *
 * | flag | (length) | value | flag | (length) | value | ......
 * <--------A column--------><--------A column--------><-.....->
 *
 * The flag means value's length or null value:
 * If value is nullptr, flag is 251
 * If value's length < 251, flag is the value's length
 * If 251 <= value's length < 65536, flag is 252 and the next two bytes is length
 * If 65536 <= value's length < 16777216 , flag is 253 and the next three bytes is length
 * If 16777216 <= value's length, flag is 254 and the next eighth bytes is length
 *
 * the code example:
 *     mrb.push_null();
 *     mrb.push_tinyint(5);
 *     mrb.push_int(120);
 *     mrb.push_string("...my length is 65536...");
 *
 * the protocol contents:
 *
 *  251-1-'5'-3-'120'-253-65536-"...my length is 65536..."
 *
 */
using int128_t = __int128;
class DecimalV2Value;
class IPv4Value;
class IPv6Value;
class TimestampTzValue;

class MysqlRowBuffer {
public:
    MysqlRowBuffer();
    ~MysqlRowBuffer();

    void reset() { _pos = _buf; }

    // Prepare for binary row buffer
    // init bitmap
    void start_binary_row(uint64_t num_cols);

    // TODO(zhaochun): add signed/unsigned support
    int push_tinyint(int8_t data);
    int push_smallint(int16_t data);
    int push_int(int32_t data);
    int push_bigint(int64_t data);
    int push_unsigned_bigint(uint64_t data);
    int push_largeint(int128_t data);
    int push_float(float data);
    int push_double(double data);
    int push_timev2(double data, int scale);
    template <typename DateType>
    int push_datetime(const DateType& data, int scale);
    int push_decimal(const DecimalV2Value& data, int round_scale);
    int push_ipv4(const IPv4Value& ipv4_val);
    int push_ipv6(const IPv6Value& ipv6_val);
    int push_timestamptz(const TimestampTzValue& tz, const cctz::time_zone& local_time_zone,
                         int scale);
    int push_string(const char* str, int64_t length);
    int push_null();

    template <typename DateType>
    int push_vec_datetime(DateType& data, int scale = -1);

    const char* buf() const { return _buf; }
    const char* pos() const { return _pos; }
    int64_t length() const { return _pos - _buf; }

private:
    int reserve(int64_t size);

    // append data into buffer
    int append(const char* data, int64_t len);
    // the same as mysql net_store_data
    // the first few bytes is length, followed by data
    int append_var_string(const char* data, int64_t len);

    char* _pos = nullptr;
    char* _buf = nullptr;
    int64_t _buf_size;

    uint32_t _field_pos = 0;

    char _default_buf[4096];
};

using MysqlRowBinaryBuffer = MysqlRowBuffer;

} // namespace doris
