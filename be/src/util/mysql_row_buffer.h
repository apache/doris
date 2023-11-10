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

template <bool is_binary_format = false>
class MysqlRowBuffer {
public:
    MysqlRowBuffer();
    ~MysqlRowBuffer();

    void reset() { _pos = _buf; }

    // Prepare for binary row buffer
    // init bitmap
    void start_binary_row(uint32_t num_cols);

    // TODO(zhaochun): add signed/unsigned support
    int push_tinyint(int8_t data);
    int push_smallint(int16_t data);
    int push_int(int32_t data);
    int push_bigint(int64_t data);
    int push_unsigned_bigint(uint64_t data);
    int push_largeint(int128_t data);
    int push_float(float data);
    int push_double(double data);
    int push_time(double data);
    int push_timev2(double data, int scale);
    template <typename DateType>
    int push_datetime(const DateType& data);
    int push_decimal(const DecimalV2Value& data, int round_scale);
    int push_ipv4(const IPv4Value& ipv4_val);
    int push_ipv6(const IPv6Value& ipv6_val);
    int push_string(const char* str, int64_t length);
    int push_null();

    template <typename DateType>
    int push_vec_datetime(DateType& data);

    // this function reserved size, change the pos step size, return old pos
    // Becareful when use the returned pointer.
    char* reserved(int64_t size);

    const char* buf() const { return _buf; }
    const char* pos() const { return _pos; }
    int64_t length() const { return _pos - _buf; }

    /**
     * Why?
     * Because the Nested-Type's data need pushed multiple times, but mysql protocol don't
     * support nested type and each push will decide a column data.
     *
     * How?
     * Dynamic mode allow we push data in a column multiple times, and allow recursive push.
     * We will think that the length of the next column is uncertain when open dynamic
     * mode, so we will set the flag to 254(longest flag) and skip 8 bytes which used for
     * record length, then compute the actual data length when close dynamic mode.
     * In a recursive call(special for nested type), the mode will open multiple times, but
     * the data is actually written in one column, so we only need to deal it at the beginning
     * and at the end.
     *
     * the code:
     *     mrb.push_tinyint(5);
     *     mrb.push_smallint(120);
     *     mrb.push_int(-30000);
     *
     * In normal mode, the buffer contains three column:
     *  1-'5'-3-'120'-6-'-30000'
     *
     * Same code in dynamic mode, the buffer contains a column:
     *  254-48-'5'-'120'-'-30000'
     *
     * NOTE: The open_dynamic_mode() and close_dynamic_mode() need appear in pairs
     */
    void open_dynamic_mode();

    /**
     * NOTE: The open_dynamic_mode() and close_dynamic_mode() need appear in pairs
     */
    void close_dynamic_mode();

    void set_faster_float_convert(bool faster) { _faster_float_convert = faster; }

private:
    int reserve(int64_t size);

    // append data into buffer
    int append(const char* data, int64_t len);
    // the same as mysql net_store_data
    // the first few bytes is length, followed by data
    int append_var_string(const char* data, int64_t len);

    char* _pos;
    char* _buf;
    int64_t _buf_size;
    char _default_buf[4096];

    int _dynamic_mode;
    uint64_t _len_pos;
    uint32_t _field_pos = 0;
    uint32_t _field_count = 0;

    bool _faster_float_convert = false;
};

} // namespace doris
