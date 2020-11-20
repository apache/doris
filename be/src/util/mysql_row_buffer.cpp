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

#include <stdio.h>
#include <stdlib.h>

#include "common/logging.h"
#include "gutil/strings/numbers.h"
#include "util/mysql_global.h"

namespace doris {

// the first byte:
// <= 250: length
// = 251: NULL
// = 252: the next two byte is length
// = 253: the next three byte is length
// = 254: the next eighth byte is length
static char* pack_vlen(char* packet, uint64_t length) {
    if (length < 251ULL) {
        int1store(packet, length);
        return packet + 1;
    }

    /* 251 is reserved for NULL */
    if (length < 65536ULL) {
        *packet++ = 252;
        int2store(packet, length);
        return packet + 2;
    }

    if (length < 16777216ULL) {
        *packet++ = 253;
        int3store(packet, length);
        return packet + 3;
    }

    *packet++ = 254;
    int8store(packet, length);
    return packet + 8;
}
MysqlRowBuffer::MysqlRowBuffer()
        : _pos(_default_buf), _buf(_default_buf), _buf_size(sizeof(_default_buf)) {}

MysqlRowBuffer::~MysqlRowBuffer() {
    if (_buf != _default_buf) {
        delete[] _buf;
    }
}

int MysqlRowBuffer::reserve(int size) {
    if (size < 0) {
        LOG(ERROR) << "alloc memory failed. size = " << size;
        return -1;
    }

    int need_size = size + (_pos - _buf);

    if (need_size <= _buf_size) {
        return 0;
    }

    int alloc_size = std::max(need_size, _buf_size * 2);
    char* new_buf = new (std::nothrow) char[alloc_size];

    if (NULL == new_buf) {
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

int MysqlRowBuffer::push_tinyint(int8_t data) {
    // 1 for string trail, 1 for length, 1 for sign, other for digits
    int ret = reserve(3 + MAX_TINYINT_WIDTH);

    if (0 != ret) {
        LOG(ERROR) << "mysql row buffer reserve failed.";
        return ret;
    }

    int length = snprintf(_pos + 1, MAX_TINYINT_WIDTH + 2, "%d", data);

    if (length < 0) {
        LOG(ERROR) << "snprintf failed. data = " << data;
        return length;
    }

    int1store(_pos, length);
    _pos += length + 1;
    return 0;
}

int MysqlRowBuffer::push_smallint(int16_t data) {
    // 1 for string trail, 1 for length, 1 for sign, other for digits
    int ret = reserve(3 + MAX_SMALLINT_WIDTH);

    if (0 != ret) {
        LOG(ERROR) << "mysql row buffer reserve failed.";
        return ret;
    }

    int length = snprintf(_pos + 1, MAX_SMALLINT_WIDTH + 2, "%d", data);

    if (length < 0) {
        LOG(ERROR) << "snprintf failed. data = " << data;
        return length;
    }

    int1store(_pos, length);
    _pos += length + 1;
    return 0;
}

int MysqlRowBuffer::push_int(int32_t data) {
    // 1 for string trail, 1 for length, 1 for sign, other for digits
    int ret = reserve(3 + MAX_INT_WIDTH);

    if (0 != ret) {
        LOG(ERROR) << "mysql row buffer reserve failed.";
        return ret;
    }

    int length = snprintf(_pos + 1, MAX_INT_WIDTH + 2, "%d", data);

    if (length < 0) {
        LOG(ERROR) << "snprintf failed. data = " << data;
        return length;
    }

    int1store(_pos, length);
    _pos += length + 1;
    return 0;
}

int MysqlRowBuffer::push_bigint(int64_t data) {
    // 1 for string trail, 1 for length, 1 for sign, other for digits
    int ret = reserve(3 + MAX_BIGINT_WIDTH);

    if (0 != ret) {
        LOG(ERROR) << "mysql row buffer reserve failed.";
        return ret;
    }

    int length = snprintf(_pos + 1, MAX_BIGINT_WIDTH + 2, "%ld", data);

    if (length < 0) {
        LOG(ERROR) << "snprintf failed. data = " << data;
        return length;
    }

    int1store(_pos, length);
    _pos += length + 1;
    return 0;
}

int MysqlRowBuffer::push_unsigned_bigint(uint64_t data) {
    // 1 for string trail, 1 for length, 1 for sign, other for digits
    int ret = reserve(4 + MAX_BIGINT_WIDTH);

    if (0 != ret) {
        LOG(ERROR) << "mysql row buffer reserve failed.";
        return ret;
    }

    int length = snprintf(_pos + 1, MAX_BIGINT_WIDTH + 3, "%ld", data);

    if (length < 0) {
        LOG(ERROR) << "snprintf failed. data = " << data;
        return length;
    }

    int1store(_pos, length);
    _pos += length + 1;
    return 0;
}

int MysqlRowBuffer::push_float(float data) {
    // 1 for string trail, 1 for length, 1 for sign, other for digits
    int ret = reserve(3 + MAX_FLOAT_STR_LENGTH);

    if (0 != ret) {
        LOG(ERROR) << "mysql row buffer reserve failed.";
        return ret;
    }

    int length = FloatToBuffer(data, MAX_FLOAT_STR_LENGTH + 2, _pos + 1);

    if (length < 0) {
        LOG(ERROR) << "gcvt float failed. data = " << data;
        return length;
    }

    int1store(_pos, length);
    _pos += length + 1;
    return 0;
}

int MysqlRowBuffer::push_double(double data) {
    // 1 for string trail, 1 for length, 1 for sign, other for digits
    int ret = reserve(3 + MAX_DOUBLE_STR_LENGTH);

    if (0 != ret) {
        LOG(ERROR) << "mysql row buffer reserve failed.";
        return ret;
    }

    int length = DoubleToBuffer(data, MAX_DOUBLE_STR_LENGTH + 2, _pos + 1);

    if (length < 0) {
        LOG(ERROR) << "gcvt double failed. data = " << data;
        return length;
    }

    int1store(_pos, length);
    _pos += length + 1;
    return 0;
}

int MysqlRowBuffer::push_string(const char* str, int length) {
    // 9 for length pack max, 1 for sign, other for digits
    if (NULL == str) {
        LOG(ERROR) << "input string is NULL.";
        return -1;
    }

    int ret = reserve(9 + length);

    if (0 != ret) {
        LOG(ERROR) << "mysql row buffer reserve failed.";
        return ret;
    }

    _pos = pack_vlen(_pos, length);
    memcpy(_pos, str, length);
    _pos += length;
    return 0;
}

int MysqlRowBuffer::push_null() {
    int ret = reserve(1);

    if (0 != ret) {
        LOG(ERROR) << "mysql row buffer reserve failed.";
        return ret;
    }

    int1store(_pos, 251);
    _pos += 1;
    return 0;
}

char* MysqlRowBuffer::reserved(int size) {
    int ret = reserve(size);

    if (0 != ret) {
        LOG(ERROR) << "mysql row buffer reserve failed.";
        return NULL;
    }

    char* old_buf = _pos;
    _pos += size;

    return old_buf;
}

} // namespace doris

/* vim: set ts=4 sw=4 sts=4 tw=100 */
