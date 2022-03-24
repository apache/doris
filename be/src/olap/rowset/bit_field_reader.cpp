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

#include "olap/rowset/bit_field_reader.h"

#include "olap/in_stream.h"
#include "olap/rowset/column_reader.h"
#include "olap/rowset/run_length_byte_reader.h"

namespace doris {

BitFieldReader::BitFieldReader(ReadOnlyFileStream* input)
        : _input(input), _byte_reader(nullptr), _current('\0'), _bits_left(0) {}

BitFieldReader::~BitFieldReader() {
    SAFE_DELETE(_byte_reader);
}

OLAPStatus BitFieldReader::init() {
    if (nullptr == _byte_reader) {
        _byte_reader = new (std::nothrow) RunLengthByteReader(_input);

        if (nullptr == _byte_reader) {
            OLAP_LOG_WARNING("fail to create RunLengthByteReader");
            return OLAP_ERR_MALLOC_ERROR;
        }
    }

    return OLAP_SUCCESS;
}

OLAPStatus BitFieldReader::_read_byte() {
    OLAPStatus res = OLAP_SUCCESS;

    if (_byte_reader->has_next()) {
        if (OLAP_SUCCESS != (res = _byte_reader->next(&_current))) {
            return res;
        }

        _bits_left = 8;
    } else {
        return OLAP_ERR_DATA_EOF;
    }

    return OLAP_SUCCESS;
}

OLAPStatus BitFieldReader::next(char* value) {
    OLAPStatus res = OLAP_SUCCESS;

    if (0 == _bits_left) {
        if (OLAP_SUCCESS != (res = _read_byte())) {
            return res;
        }
    }

    --_bits_left;

    *value = (_current >> _bits_left) & 0x01;

    return OLAP_SUCCESS;
}

OLAPStatus BitFieldReader::seek(PositionProvider* position) {
    OLAPStatus res = OLAP_SUCCESS;

    if (OLAP_SUCCESS != (res = _byte_reader->seek(position))) {
        return res;
    }

    int64_t consumed = position->get_next();

    if (consumed > 8) {
        OLAP_LOG_WARNING("read past end of bit field");
        return OLAP_ERR_DATA_EOF;
    } else if (consumed != 0) {
        if (OLAP_SUCCESS != (res = _read_byte())) {
            return res;
        }

        _bits_left = 8 - consumed;
    } else {
        _bits_left = 0;
    }

    return OLAP_SUCCESS;
}

OLAPStatus BitFieldReader::skip(uint64_t num_values) {
    OLAPStatus res = OLAP_SUCCESS;

    uint64_t total_bits = num_values;

    if (_bits_left >= total_bits) {
        _bits_left -= total_bits;
    } else {
        total_bits -= _bits_left;

        if (OLAP_SUCCESS != (res = _byte_reader->skip(total_bits / 8))) {
            return res;
        }

        if (OLAP_SUCCESS != (res = _byte_reader->next(&_current))) {
            return res;
        }

        _bits_left = 8 - (total_bits % 8);
    }

    return OLAP_SUCCESS;
}

} // namespace doris