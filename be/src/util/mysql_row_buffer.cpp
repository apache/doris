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

#include "util/mysql_row_buffer.h"

#include <assert.h>
#include <fmt/compile.h>
#include <fmt/format.h>
#include <string.h>
#include <sys/types.h>

#include <algorithm>
#include <new>
#include <string>

#include "common/logging.h"
#include "date_func.h"
#include "olap/olap_common.h"
#include "runtime/decimalv2_value.h"
#include "runtime/large_int_value.h"
#include "util/mysql_global.h"
#include "vec/functions/cast/cast_to_string.h"
#include "vec/runtime/ipv4_value.h"
#include "vec/runtime/ipv6_value.h"
#include "vec/runtime/vdatetime_value.h" // IWYU pragma: keep

namespace doris {

static uint8_t NEXT_TWO_BYTE = 252;
static uint8_t NEXT_THREE_BYTE = 253;
static uint8_t NEXT_EIGHT_BYTE = 254;
// the EXTRA_RESERVE_BYTE wanner to make sure _pos pointer is always in _buf memory
// used in reserve() for allocate current buffer
static size_t EXTRA_RESERVE_BYTE = 16;

// the first byte:
// <= 250: length
// = 251: nullptr
// = 252: the next two byte is length
// = 253: the next three byte is length
// = 254: the next eight byte is length
static char* pack_vlen(char* packet, uint64_t length) {
    if (length < 251ULL) {
        int1store(packet, length);
        return packet + 1;
    }

    /* 251 is reserved for nullptr */
    if (length < 65536ULL) {
        *packet++ = NEXT_TWO_BYTE;
        int2store(packet, length);
        return packet + 2;
    }

    if (length < 16777216ULL) {
        *packet++ = NEXT_THREE_BYTE;
        int3store(packet, length);
        return packet + 3;
    }

    *packet++ = NEXT_EIGHT_BYTE;
    int8store(packet, length);
    return packet + 8;
}

MysqlRowBuffer::MysqlRowBuffer()
        : _pos(_default_buf), _buf(_default_buf), _buf_size(sizeof(_default_buf)) {}

void MysqlRowBuffer::start_binary_row(uint64_t num_cols) {
    auto bit_fields = (num_cols + 9) / 8;
    reserve(bit_fields + 1);
    memset(_pos, 0, 1 + bit_fields);
    _pos += bit_fields + 1;
    _field_pos = 0;
}

MysqlRowBuffer::~MysqlRowBuffer() {
    if (_buf != _default_buf) {
        delete[] _buf;
        _buf = _default_buf;
    }
}

int MysqlRowBuffer::reserve(int64_t size) {
    DCHECK(size > 0);

    int64_t need_size = size + (_pos - _buf);

    if (need_size <= _buf_size) {
        return 0;
    }

    int64_t alloc_size = std::max(need_size, _buf_size * 2) + EXTRA_RESERVE_BYTE;
    char* new_buf = new char[alloc_size];

    size_t offset = _pos - _buf;
    memcpy(new_buf, _buf, offset);

    if (_buf != _default_buf) {
        delete[] _buf;
    }

    _pos = new_buf + offset;
    _buf = new_buf;
    _buf_size = alloc_size;

    return 0;
}

static char* add_decimal(const DecimalV2Value& data, int round_scale, char* pos) {
    int length = data.to_buffer(pos + 1, round_scale);
    int1store(pos++, length);
    return pos + length;
}

int MysqlRowBuffer::append(const char* data, int64_t len) {
    reserve(len);
    memcpy(_pos, data, len);
    _pos += len;
    return 0;
}

int MysqlRowBuffer::append_var_string(const char* data, int64_t len) {
    /*
     The +9 comes from that strings of length longer than 16M require
     9 bytes to be stored (see net_store_length).
    */
    reserve(len + 9);
    _pos = pack_vlen(_pos, len);
    memcpy(_pos, data, len);
    _pos += len;
    return 0;
}

int MysqlRowBuffer::push_tinyint(int8_t data) {
    char buff[1];
    _field_pos++;
    int1store(buff, data);
    return append(buff, 1);
}

int MysqlRowBuffer::push_smallint(int16_t data) {
    char buff[2];
    _field_pos++;
    int2store(buff, data);
    return append(buff, 2);
}

int MysqlRowBuffer::push_int(int32_t data) {
    char buff[4];
    _field_pos++;
    int4store(buff, data);
    return append(buff, 4);
}

int MysqlRowBuffer::push_bigint(int64_t data) {
    char buff[8];
    _field_pos++;
    int8store(buff, data);
    return append(buff, 8);
}

int MysqlRowBuffer::push_unsigned_bigint(uint64_t data) {
    char buff[8];
    _field_pos++;
    int8store(buff, data);
    return append(buff, 8);
}

int MysqlRowBuffer::push_largeint(int128_t data) {
    // large int as type string
    std::string value = LargeIntValue::to_string(data);
    _field_pos++;
    return append_var_string(value.data(), value.size());
}

int MysqlRowBuffer::push_float(float data) {
    char buff[4];
    _field_pos++;
    float4store(buff, data);
    return append(buff, 4);
}

int MysqlRowBuffer::push_double(double data) {
    char buff[8];
    _field_pos++;
    float8store(buff, data);
    return append(buff, 8);
}

// Refer to https://dev.mysql.com/doc/refman/5.7/en/time.html
// Encode time into MySQL binary protocol format with support for scale (microsecond precision)
// Time value is limited between '-838:59:59' and '838:59:59'
static int encode_binary_timev2(char* buff, double time, int scale) {
    // Check if scale is valid (0 to 6)
    if (scale < 0 || scale > 6) {
        return -1; // Return error for invalid scale
    }

    int pos = 0;                      // Current position in the buffer
    bool is_negative = time < 0;      // Determine if the time is negative
    double abs_time = std::abs(time); // Convert time to absolute value

    // Maximum time in microseconds: 838 hours, 59 minutes, 59 seconds
    const int64_t MAX_TIME_MICROSECONDS = (838 * 3600 + 59 * 60 + 59) * 1000000LL;

    // Convert time into microseconds and enforce range limit
    auto total_microseconds = static_cast<int64_t>(abs_time); // Total microseconds
    total_microseconds = std::min(total_microseconds, MAX_TIME_MICROSECONDS);

    // Adjust microseconds precision based on scale
    total_microseconds /= static_cast<int64_t>(std::pow(10, 6 - scale)); // Scale adjustment
    total_microseconds *= static_cast<int64_t>(std::pow(10, 6 - scale)); // Truncate extra precision

    // Extract days, hours, minutes, seconds, and microseconds
    int64_t days = total_microseconds / (3600LL * 24 * 1000000); // Calculate days
    total_microseconds %= (3600LL * 24 * 1000000);

    int64_t hours = total_microseconds / (3600LL * 1000000); // Remaining hours
    total_microseconds %= (3600LL * 1000000);

    int64_t minutes = total_microseconds / (60LL * 1000000); // Remaining minutes
    total_microseconds %= (60LL * 1000000);

    int64_t seconds = total_microseconds / 1000000;      // Remaining seconds
    int64_t microseconds = total_microseconds % 1000000; // Remaining microseconds

    // MySQL binary protocol rules for time encoding
    if (days == 0 && hours == 0 && minutes == 0 && seconds == 0 && microseconds == 0) {
        buff[pos++] = 0; // All zero: length is 0
    } else if (microseconds == 0) {
        buff[pos++] = 8;                                    // No microseconds: length is 8
        buff[pos++] = is_negative ? 1 : 0;                  // Sign byte
        int4store(buff + pos, static_cast<uint32_t>(days)); // Store days (4 bytes)
        pos += 4;
        buff[pos++] = static_cast<char>(hours);   // Store hours (1 byte)
        buff[pos++] = static_cast<char>(minutes); // Store minutes (1 byte)
        buff[pos++] = static_cast<char>(seconds); // Store seconds (1 byte)
    } else {
        buff[pos++] = 12;                                   // Include microseconds: length is 12
        buff[pos++] = is_negative ? 1 : 0;                  // Sign byte
        int4store(buff + pos, static_cast<uint32_t>(days)); // Store days (4 bytes)
        pos += 4;
        buff[pos++] = static_cast<char>(hours);                     // Store hours (1 byte)
        buff[pos++] = static_cast<char>(minutes);                   // Store minutes (1 byte)
        buff[pos++] = static_cast<char>(seconds);                   // Store seconds (1 byte)
        int4store(buff + pos, static_cast<uint32_t>(microseconds)); // Store microseconds (4 bytes)
        pos += 4;
    }

    return pos; // Return total bytes written to buffer
}

int MysqlRowBuffer::push_timev2(double data, int scale) {
    char buff[13];
    _field_pos++;
    int length = encode_binary_timev2(buff, data, scale);
    return append(buff, length);
}

template <typename DateType>
int MysqlRowBuffer::push_vec_datetime(DateType& data, int scale) {
    return push_datetime(data, scale);
}

template <typename DateType>
int MysqlRowBuffer::push_datetime(const DateType& data, int scale) {
    char buff[12], *pos;
    size_t length;
    _field_pos++;
    pos = buff + 1;

    int2store(pos, data.year());
    pos[2] = (uchar)data.month();
    pos[3] = (uchar)data.day();
    pos[4] = (uchar)data.hour();
    pos[5] = (uchar)data.minute();
    pos[6] = (uchar)data.second();
    if (data.hour() || data.minute() || data.second()) {
        length = 7;
    } else if (data.year() || data.month() || data.day()) {
        length = 4;
    } else {
        length = 0;
    }
    if constexpr (std::is_same_v<DateType, DateV2Value<DateV2ValueType>> ||
                  std::is_same_v<DateType, DateV2Value<DateTimeV2ValueType>>) {
        if (scale > 0 || data.microsecond()) {
            int4store(pos + 7, data.microsecond());
            length = 11;
        }
    }

    buff[0] = (char)length; // Length is stored first
    return append(buff, length + 1);
}

int MysqlRowBuffer::push_decimal(const DecimalV2Value& data, int round_scale) {
    ++_field_pos;
    reserve(2 + MAX_DECIMAL_WIDTH);
    _pos = add_decimal(data, round_scale, _pos);
    return 0;
}

int MysqlRowBuffer::push_ipv4(const IPv4Value& ipv4_val) {
    auto ipv4_str = ipv4_val.to_string();
    return push_string(ipv4_str.c_str(), ipv4_str.length());
}

int MysqlRowBuffer::push_ipv6(const IPv6Value& ipv6_val) {
    auto ipv6_str = ipv6_val.to_string();
    return push_string(ipv6_str.c_str(), ipv6_str.length());
}

int MysqlRowBuffer::push_string(const char* str, int64_t length) {
    ++_field_pos;
    DCHECK(str != nullptr) << "input string is nullptr.";
    reserve(9 + length);
    _pos = pack_vlen(_pos, length);
    memcpy(_pos, str, length);
    _pos += length;
    return 0;
}

int MysqlRowBuffer::push_null() {
    uint offset = (_field_pos + 2) / 8 + 1;
    uint bit = (1 << ((_field_pos + 2) & 7));
    /* Room for this as it's allocated start_binary_row*/
    char* to = _buf + offset;
    *to = (char)((uchar)*to | (uchar)bit);
    _field_pos++;
    return 0;
}

template int MysqlRowBuffer::push_vec_datetime<DateV2Value<DateV2ValueType>>(
        DateV2Value<DateV2ValueType>& value, int scale);
template int MysqlRowBuffer::push_vec_datetime<DateV2Value<DateTimeV2ValueType>>(
        DateV2Value<DateTimeV2ValueType>& value, int scale);
template int MysqlRowBuffer::push_vec_datetime<VecDateTimeValue>(VecDateTimeValue& value,
                                                                 int scale);

} // namespace doris
