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

#include <fmt/format.h>
#include <stdio.h>
#include <stdlib.h>

#include "common/logging.h"
#include "date_func.h"
#include "gutil/strings/numbers.h"
#include "runtime/large_int_value.h"
#include "util/mysql_global.h"

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
MysqlRowBuffer::MysqlRowBuffer()
        : _pos(_default_buf),
          _buf(_default_buf),
          _buf_size(sizeof(_default_buf)),
          _dynamic_mode(0),
          _len_pos(nullptr) {}

MysqlRowBuffer::~MysqlRowBuffer() {
    if (_buf != _default_buf) {
        delete[] _buf;
    }
}

void MysqlRowBuffer::open_dynamic_mode() {
    if (!_dynamic_mode) {
        *_pos++ = NEXT_EIGHT_BYTE;
        // write length when dynamic mode close
        _len_pos = _pos;
        _pos = _pos + 8;
    }
    _dynamic_mode++;
}

void MysqlRowBuffer::close_dynamic_mode() {
    _dynamic_mode--;

    if (!_dynamic_mode) {
        int8store(_len_pos, _pos - _len_pos - 8);
        _len_pos = nullptr;
    }
}

int MysqlRowBuffer::reserve(int64_t size) {
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

    memcpy(new_buf, _buf, _pos - _buf);

    if (_buf != _default_buf) {
        delete[] _buf;
    }

    _pos = new_buf + (_pos - _buf);
    _buf = new_buf;
    _buf_size = alloc_size;

    return 0;
}

template <typename T>
static char* add_int(T data, char* pos, bool dynamic_mode) {
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
static char* add_float(T data, char* pos, bool dynamic_mode) {
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

static char* add_datetime(const DateTimeValue& data, char* pos, bool dynamic_mode) {
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

int MysqlRowBuffer::push_tinyint(int8_t data) {
    // 1 for string trail, 1 for length, 1 for sign, other for digits
    int ret = reserve(3 + MAX_TINYINT_WIDTH);

    if (0 != ret) {
        LOG(ERROR) << "mysql row buffer reserve failed.";
        return ret;
    }

    _pos = add_int(data, _pos, _dynamic_mode);
    return 0;
}

int MysqlRowBuffer::push_smallint(int16_t data) {
    // 1 for string trail, 1 for length, 1 for sign, other for digits
    int ret = reserve(3 + MAX_SMALLINT_WIDTH);

    if (0 != ret) {
        LOG(ERROR) << "mysql row buffer reserve failed.";
        return ret;
    }

    _pos = add_int(data, _pos, _dynamic_mode);
    return 0;
}

int MysqlRowBuffer::push_int(int32_t data) {
    // 1 for string trail, 1 for length, 1 for sign, other for digits
    int ret = reserve(3 + MAX_INT_WIDTH);

    if (0 != ret) {
        LOG(ERROR) << "mysql row buffer reserve failed.";
        return ret;
    }

    _pos = add_int(data, _pos, _dynamic_mode);
    return 0;
}

int MysqlRowBuffer::push_bigint(int64_t data) {
    // 1 for string trail, 1 for length, 1 for sign, other for digits
    int ret = reserve(3 + MAX_BIGINT_WIDTH);

    if (0 != ret) {
        LOG(ERROR) << "mysql row buffer reserve failed.";
        return ret;
    }

    _pos = add_int(data, _pos, _dynamic_mode);
    return 0;
}

int MysqlRowBuffer::push_unsigned_bigint(uint64_t data) {
    // 1 for string trail, 1 for length, 1 for sign, other for digits
    int ret = reserve(4 + MAX_BIGINT_WIDTH);

    if (0 != ret) {
        LOG(ERROR) << "mysql row buffer reserver failed.";
        return ret;
    }

    _pos = add_int(data, _pos, _dynamic_mode);
    return 0;
}

int MysqlRowBuffer::push_largeint(int128_t data) {
    // 1 for string trail, 1 for length, 1 for sign, other for digits
    int ret = reserve(3 + MAX_LARGEINT_WIDTH);

    if (0 != ret) {
        LOG(ERROR) << "mysql row buffer reserver failed.";
        return ret;
    }

    _pos = add_largeint(data, _pos, _dynamic_mode);
    return 0;
}

int MysqlRowBuffer::push_float(float data) {
    // 1 for string trail, 1 for length, 1 for sign, other for digits
    int ret = reserve(3 + MAX_FLOAT_STR_LENGTH);

    if (0 != ret) {
        LOG(ERROR) << "mysql row buffer reserve failed.";
        return ret;
    }

    _pos = add_float(data, _pos, _dynamic_mode);
    return 0;
}

int MysqlRowBuffer::push_double(double data) {
    // 1 for string trail, 1 for length, 1 for sign, other for digits
    int ret = reserve(3 + MAX_DOUBLE_STR_LENGTH);

    if (0 != ret) {
        LOG(ERROR) << "mysql row buffer reserve failed.";
        return ret;
    }

    _pos = add_float(data, _pos, _dynamic_mode);
    return 0;
}

int MysqlRowBuffer::push_time(double data) {
    // 1 for string trail, 1 for length, other for time str
    int ret = reserve(2 + MAX_TIME_WIDTH);

    if (0 != ret) {
        LOG(ERROR) << "mysql row buffer reserve failed.";
        return ret;
    }

    _pos = add_time(data, _pos, _dynamic_mode);
    return 0;
}

int MysqlRowBuffer::push_datetime(const DateTimeValue& data) {
    // 1 for string trail, 1 for length, other for datetime str
    int ret = reserve(2 + MAX_DATETIME_WIDTH);

    if (0 != ret) {
        LOG(ERROR) << "mysql row buffer reserve failed.";
        return ret;
    }

    _pos = add_datetime(data, _pos, _dynamic_mode);
    return 0;
}

int MysqlRowBuffer::push_decimal(const DecimalV2Value& data, int round_scale) {
    // 1 for string trail, 1 for length, other for decimal str
    int ret = reserve(2 + MAX_DECIMAL_WIDTH);

    if (0 != ret) {
        LOG(ERROR) << "mysql row buffer reserve failed.";
        return ret;
    }

    _pos = add_decimal(data, round_scale, _pos, _dynamic_mode);
    return 0;
}

int MysqlRowBuffer::push_string(const char* str, int64_t length) {
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

int MysqlRowBuffer::push_null() {
    if (_dynamic_mode) {
        // dynamic mode not write
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

char* MysqlRowBuffer::reserved(int64_t size) {
    int ret = reserve(size);

    if (0 != ret) {
        LOG(ERROR) << "mysql row buffer reserve failed.";
        return nullptr;
    }

    char* old_buf = _pos;
    _pos += size;

    return old_buf;
}

} // namespace doris
