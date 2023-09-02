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
#include <fmt/format.h>
#include <string.h>
#include <sys/types.h>

#include <algorithm>
#include <new>
#include <ostream>
#include <string>

#include "common/logging.h"
#include "date_func.h"
#include "gutil/strings/numbers.h"
#include "olap/olap_common.h"
#include "runtime/decimalv2_value.h"
#include "runtime/large_int_value.h"
#include "util/mysql_global.h"
#include "vec/runtime/vdatetime_value.h" // IWYU pragma: keep

namespace doris {

static uint8_t NEXT_TWO_BYTE = 252;
static uint8_t NEXT_THREE_BYTE = 253;
static uint8_t NEXT_EIGHT_BYTE = 254;

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

template <bool is_binary_format>
MysqlRowBuffer<is_binary_format>::MysqlRowBuffer()
        : _pos(_default_buf),
          _buf(_default_buf),
          _buf_size(sizeof(_default_buf)),
          _dynamic_mode(0),
          _len_pos(0) {}

template <bool is_binary_format>
void MysqlRowBuffer<is_binary_format>::start_binary_row(uint32_t num_cols) {
    assert(is_binary_format);
    int bit_fields = (num_cols + 9) / 8;
    reserve(bit_fields + 1);
    memset(_pos, 0, 1 + bit_fields);
    _pos += bit_fields + 1;
    _field_pos = 0;
}

template <bool is_binary_format>
MysqlRowBuffer<is_binary_format>::~MysqlRowBuffer() {
    if (_buf != _default_buf) {
        delete[] _buf;
        _buf = _default_buf;
    }
}

template <bool is_binary_format>
void MysqlRowBuffer<is_binary_format>::open_dynamic_mode() {
    if (!_dynamic_mode) {
        *_pos++ = NEXT_EIGHT_BYTE;
        // write length when dynamic mode close
        _len_pos = (_pos - _buf);
        _pos = _pos + 8;
    }
    _dynamic_mode++;
}

template <bool is_binary_format>
void MysqlRowBuffer<is_binary_format>::close_dynamic_mode() {
    _dynamic_mode--;

    // _buf + _len_pos is the position to write length
    if (!_dynamic_mode) {
        int8store((_buf + _len_pos), _pos - (_buf + _len_pos) - 8);
        _len_pos = 0;
    }
}

template <bool is_binary_format>
int MysqlRowBuffer<is_binary_format>::reserve(int64_t size) {
    if (size < 0) {
        LOG(ERROR) << "alloc memory failed. size = " << size;
        return -1;
    }

    int64_t need_size = size + (_pos - _buf);

    if (need_size <= _buf_size) {
        return 0;
    }

    int64_t alloc_size = std::max(need_size, _buf_size * 2);
    char* new_buf = new (std::nothrow) char[alloc_size];

    if (nullptr == new_buf) {
        LOG(ERROR) << "alloc memory failed. size = " << alloc_size;
        return -1;
    }
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

template <typename T>
char* add_int(T data, char* pos, bool dynamic_mode) {
    auto fi = fmt::format_int(data);
    int length = fi.size();
    if (!dynamic_mode) {
        int1store(pos++, length);
    }
    memcpy(pos, fi.data(), length);
    return pos + length;
}

static char* add_largeint(int128_t data, char* pos, bool dynamic_mode) {
    int length = LargeIntValue::to_buffer(data, pos + !dynamic_mode);
    if (!dynamic_mode) {
        int1store(pos++, length);
    }
    return pos + length;
}

template <typename T>
char* add_float(T data, char* pos, bool dynamic_mode) {
    int length = 0;
    if constexpr (std::is_same_v<T, float>) {
        length = FastFloatToBuffer(data, pos + !dynamic_mode);
    } else if constexpr (std::is_same_v<T, double>) {
        length = FastDoubleToBuffer(data, pos + !dynamic_mode);
    }
    if (!dynamic_mode) {
        int1store(pos++, length);
    }
    return pos + length;
}

static char* add_time(double data, char* pos, bool dynamic_mode) {
    int length = time_to_buffer_from_double(data, pos + !dynamic_mode);
    if (!dynamic_mode) {
        int1store(pos++, length);
    }
    return pos + length;
}

static char* add_timev2(double data, char* pos, bool dynamic_mode, int scale) {
    int length = timev2_to_buffer_from_double(data, pos + !dynamic_mode, scale);
    if (!dynamic_mode) {
        int1store(pos++, length);
    }
    return pos + length;
}
template <typename DateType>
static char* add_datetime(const DateType& data, char* pos, bool dynamic_mode) {
    int length = data.to_buffer(pos + !dynamic_mode);
    if (!dynamic_mode) {
        int1store(pos++, length);
    }
    return pos + length;
}

static char* add_decimal(const DecimalV2Value& data, int round_scale, char* pos,
                         bool dynamic_mode) {
    int length = data.to_buffer(pos + !dynamic_mode, round_scale);
    if (!dynamic_mode) {
        int1store(pos++, length);
    }
    return pos + length;
}

template <bool is_binary_format>
int MysqlRowBuffer<is_binary_format>::append(const char* data, int64_t len) {
    int ret = reserve(len);
    if (0 != ret) {
        LOG(ERROR) << "mysql row buffer reserve failed.";
        return ret;
    }
    memcpy(_pos, data, len);
    _pos += len;
    return 0;
}

template <bool is_binary_format>
int MysqlRowBuffer<is_binary_format>::append_var_string(const char* data, int64_t len) {
    /*
     The +9 comes from that strings of length longer than 16M require
     9 bytes to be stored (see net_store_length).
    */
    int ret = reserve(len + 9);
    if (0 != ret) {
        LOG(ERROR) << "mysql row buffer reserve failed.";
        return ret;
    }
    _pos = pack_vlen(_pos, len);
    memcpy(_pos, data, len);
    _pos += len;
    return 0;
}

template <bool is_binary_format>
int MysqlRowBuffer<is_binary_format>::push_tinyint(int8_t data) {
    if (is_binary_format && !_dynamic_mode) {
        char buff[1];
        _field_pos++;
        int1store(buff, data);
        return append(buff, 1);
    }
    // 1 for string trail, 1 for length, 1 for sign, other for digits
    int ret = reserve(3 + MAX_TINYINT_WIDTH);

    if (0 != ret) {
        LOG(ERROR) << "mysql row buffer reserve failed.";
        return ret;
    }

    _pos = add_int(data, _pos, _dynamic_mode);
    return 0;
}

template <bool is_binary_format>
int MysqlRowBuffer<is_binary_format>::push_smallint(int16_t data) {
    if (is_binary_format && !_dynamic_mode) {
        char buff[2];
        _field_pos++;
        int2store(buff, data);
        return append(buff, 2);
    }
    // 1 for string trail, 1 for length, 1 for sign, other for digits
    int ret = reserve(3 + MAX_SMALLINT_WIDTH);

    if (0 != ret) {
        LOG(ERROR) << "mysql row buffer reserve failed.";
        return ret;
    }

    _pos = add_int(data, _pos, _dynamic_mode);
    return 0;
}

template <bool is_binary_format>
int MysqlRowBuffer<is_binary_format>::push_int(int32_t data) {
    if (is_binary_format && !_dynamic_mode) {
        char buff[4];
        _field_pos++;
        int4store(buff, data);
        return append(buff, 4);
    }
    // 1 for string trail, 1 for length, 1 for sign, other for digits
    int ret = reserve(3 + MAX_INT_WIDTH);

    if (0 != ret) {
        LOG(ERROR) << "mysql row buffer reserve failed.";
        return ret;
    }

    _pos = add_int(data, _pos, _dynamic_mode);
    return 0;
}

template <bool is_binary_format>
int MysqlRowBuffer<is_binary_format>::push_bigint(int64_t data) {
    if (is_binary_format && !_dynamic_mode) {
        char buff[8];
        _field_pos++;
        int8store(buff, data);
        return append(buff, 8);
    }
    // 1 for string trail, 1 for length, 1 for sign, other for digits
    int ret = reserve(3 + MAX_BIGINT_WIDTH);

    if (0 != ret) {
        LOG(ERROR) << "mysql row buffer reserve failed.";
        return ret;
    }

    _pos = add_int(data, _pos, _dynamic_mode);
    return 0;
}

template <bool is_binary_format>
int MysqlRowBuffer<is_binary_format>::push_unsigned_bigint(uint64_t data) {
    if (is_binary_format && !_dynamic_mode) {
        char buff[8];
        _field_pos++;
        int8store(buff, data);
        return append(buff, 8);
    }
    // 1 for string trail, 1 for length, 1 for sign, other for digits
    int ret = reserve(4 + MAX_BIGINT_WIDTH);

    if (0 != ret) {
        LOG(ERROR) << "mysql row buffer reserver failed.";
        return ret;
    }

    _pos = add_int(data, _pos, _dynamic_mode);
    return 0;
}

template <bool is_binary_format>
int MysqlRowBuffer<is_binary_format>::push_largeint(int128_t data) {
    if (is_binary_format && !_dynamic_mode) {
        // large int as type string
        std::string value = LargeIntValue::to_string(data);
        _field_pos++;
        return append_var_string(value.data(), value.size());
    }
    // 1 for string trail, 1 for length, 1 for sign, other for digits
    int ret = reserve(3 + MAX_LARGEINT_WIDTH);

    if (0 != ret) {
        LOG(ERROR) << "mysql row buffer reserver failed.";
        return ret;
    }

    _pos = add_largeint(data, _pos, _dynamic_mode);
    return 0;
}

template <bool is_binary_format>
int MysqlRowBuffer<is_binary_format>::push_float(float data) {
    if (is_binary_format && !_dynamic_mode) {
        char buff[4];
        _field_pos++;
        float4store(buff, data);
        return append(buff, 4);
    }
    // 1 for string trail, 1 for length, 1 for sign, other for digits
    int ret = reserve(3 + MAX_FLOAT_STR_LENGTH);

    if (0 != ret) {
        LOG(ERROR) << "mysql row buffer reserve failed.";
        return ret;
    }

    _pos = add_float(data, _pos, _dynamic_mode);
    return 0;
}

template <bool is_binary_format>
int MysqlRowBuffer<is_binary_format>::push_double(double data) {
    if (is_binary_format && !_dynamic_mode) {
        char buff[8];
        _field_pos++;
        float8store(buff, data);
        return append(buff, 8);
    }
    // 1 for string trail, 1 for length, 1 for sign, other for digits
    int ret = reserve(3 + MAX_DOUBLE_STR_LENGTH);

    if (0 != ret) {
        LOG(ERROR) << "mysql row buffer reserve failed.";
        return ret;
    }

    _pos = add_float(data, _pos, _dynamic_mode);
    return 0;
}

template <bool is_binary_format>
int MysqlRowBuffer<is_binary_format>::push_time(double data) {
    if (is_binary_format && !_dynamic_mode) {
        char buff[8];
        _field_pos++;
        float8store(buff, data);
        return append(buff, 8);
    }
    // 1 for string trail, 1 for length, other for time str
    int ret = reserve(2 + MAX_TIME_WIDTH);

    if (0 != ret) {
        LOG(ERROR) << "mysql row buffer reserve failed.";
        return ret;
    }

    _pos = add_time(data, _pos, _dynamic_mode);
    return 0;
}

template <bool is_binary_format>
int MysqlRowBuffer<is_binary_format>::push_timev2(double data, int scale) {
    if (is_binary_format && !_dynamic_mode) {
        char buff[8];
        _field_pos++;
        float8store(buff, data);
        return append(buff, 8);
    }
    // 1 for string trail, 1 for length, other for time str
    int ret = reserve(2 + MAX_TIME_WIDTH);

    if (0 != ret) {
        LOG(ERROR) << "mysql row buffer reserve failed.";
        return ret;
    }

    _pos = add_timev2(data, _pos, _dynamic_mode, scale);
    return 0;
}
template <bool is_binary_format>
template <typename DateType>
int MysqlRowBuffer<is_binary_format>::push_vec_datetime(DateType& data) {
    if (is_binary_format && !_dynamic_mode) {
        return push_datetime(data);
    }

    char buf[64];
    char* pos = data.to_string(buf);
    return push_string(buf, pos - buf - 1);
}

template <bool is_binary_format>
template <typename DateType>
int MysqlRowBuffer<is_binary_format>::push_datetime(const DateType& data) {
    if (is_binary_format && !_dynamic_mode) {
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
        if constexpr (std::is_same_v<DateType,
                                     vectorized::DateV2Value<vectorized::DateV2ValueType>> ||
                      std::is_same_v<DateType,
                                     vectorized::DateV2Value<vectorized::DateTimeV2ValueType>>) {
            int4store(pos + 7, data.microsecond());
            if (data.microsecond()) {
                length = 11;
            }
        } else {
            int4store(pos + 7, 0);
        }

        if (data.hour() || data.minute() || data.second()) {
            length = 7;
        } else if (data.year() || data.month() || data.day()) {
            length = 4;
        } else {
            length = 0;
        }
        buff[0] = (char)length; // Length is stored first
        return append(buff, length + 1);
    }
    // 1 for string trail, 1 for length, other for datetime str
    int ret = reserve(2 + MAX_DATETIME_WIDTH);

    if (0 != ret) {
        LOG(ERROR) << "mysql row buffer reserve failed.";
        return ret;
    }

    _pos = add_datetime(data, _pos, _dynamic_mode);
    return 0;
}

template <bool is_binary_format>
int MysqlRowBuffer<is_binary_format>::push_decimal(const DecimalV2Value& data, int round_scale) {
    if (is_binary_format && !_dynamic_mode) {
        ++_field_pos;
    }
    // 1 for string trail, 1 for length, other for decimal str
    int ret = reserve(2 + MAX_DECIMAL_WIDTH);

    if (0 != ret) {
        LOG(ERROR) << "mysql row buffer reserve failed.";
        return ret;
    }

    _pos = add_decimal(data, round_scale, _pos, _dynamic_mode);
    return 0;
}

template <bool is_binary_format>
int MysqlRowBuffer<is_binary_format>::push_string(const char* str, int64_t length) {
    if (is_binary_format && !_dynamic_mode) {
        ++_field_pos;
    }
    // 9 for length pack max, 1 for sign, other for digits
    if (nullptr == str) {
        LOG(ERROR) << "input string is nullptr.";
        return -1;
    }

    int ret = reserve(9 + length);

    if (0 != ret) {
        LOG(ERROR) << "mysql row buffer reserve failed.";
        return ret;
    }

    if (!_dynamic_mode) {
        _pos = pack_vlen(_pos, length);
    }
    memcpy(_pos, str, length);
    _pos += length;
    return 0;
}

template <bool is_binary_format>
int MysqlRowBuffer<is_binary_format>::push_null() {
    if (_dynamic_mode) {
        // for nested type
        return 0;
    }

    if constexpr (is_binary_format) {
        uint offset = (_field_pos + 2) / 8 + 1;
        uint bit = (1 << ((_field_pos + 2) & 7));
        /* Room for this as it's allocated start_binary_row*/
        char* to = (char*)_buf + offset;
        *to = (char)((uchar)*to | (uchar)bit);
        _field_pos++;
        return 0;
    }

    int ret = reserve(1);

    if (0 != ret) {
        LOG(ERROR) << "mysql row buffer reserve failed.";
        return ret;
    }

    int1store(_pos, 251);
    _pos += 1;
    return 0;
}

template <bool is_binary_format>
char* MysqlRowBuffer<is_binary_format>::reserved(int64_t size) {
    int ret = reserve(size);

    if (0 != ret) {
        LOG(ERROR) << "mysql row buffer reserve failed.";
        return nullptr;
    }

    char* old_buf = _pos;
    _pos += size;

    return old_buf;
}

template class MysqlRowBuffer<true>;
template class MysqlRowBuffer<false>;

template int
MysqlRowBuffer<true>::push_vec_datetime<vectorized::DateV2Value<vectorized::DateV2ValueType>>(
        vectorized::DateV2Value<vectorized::DateV2ValueType>& value);
template int
MysqlRowBuffer<true>::push_vec_datetime<vectorized::DateV2Value<vectorized::DateTimeV2ValueType>>(
        vectorized::DateV2Value<vectorized::DateTimeV2ValueType>& value);
template int MysqlRowBuffer<true>::push_vec_datetime<vectorized::VecDateTimeValue>(
        vectorized::VecDateTimeValue& value);
template int
MysqlRowBuffer<false>::push_vec_datetime<vectorized::DateV2Value<vectorized::DateV2ValueType>>(
        vectorized::DateV2Value<vectorized::DateV2ValueType>& value);
template int
MysqlRowBuffer<false>::push_vec_datetime<vectorized::DateV2Value<vectorized::DateTimeV2ValueType>>(
        vectorized::DateV2Value<vectorized::DateTimeV2ValueType>& value);
template int MysqlRowBuffer<false>::push_vec_datetime<vectorized::VecDateTimeValue>(
        vectorized::VecDateTimeValue& value);

} // namespace doris
