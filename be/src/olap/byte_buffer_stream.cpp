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

#include "olap/byte_buffer_stream.h"

#include "olap/out_stream.h"

namespace doris {

OLAPStatus ByteBufferStream::read(char* byte) {
    OLAPStatus res = _assure_data();

    if (OLAP_SUCCESS != res) {
        return res;
    }

    *byte = _data.data[_cur_pos++];
    return res;
}

OLAPStatus ByteBufferStream::read(char* buffer, uint64_t* buf_size) {
    OLAPStatus res;
    uint64_t read_length = *buf_size;
    *buf_size = 0;

    do {
        res = _assure_data();
        if (OLAP_SUCCESS != res) {
            break;
        }

        uint64_t actual_length = std::min(read_length - *buf_size, (uint64_t)remaining());

        memcpy(buffer, _data.data, actual_length);
        _cur_pos += actual_length;
        *buf_size += actual_length;
        buffer += actual_length;
    } while (*buf_size < read_length);

    return res;
}

OLAPStatus ByteBufferStream::read_all(char* buffer, uint64_t* buf_size) {
    OLAPStatus res = OLAP_SUCCESS;
    uint64_t read_length = 0;
    uint64_t buffer_remain = *buf_size;

    while (OLAP_SUCCESS == _assure_data()) {
        read_length = remaining();

        if (buffer_remain < read_length) {
            res = OLAP_ERR_BUFFER_OVERFLOW;
            break;
        }

        memcpy(buffer, _data.data, read_length);

        buffer_remain -= read_length;
        buffer += read_length;
    }

    if (eof()) {
        *buf_size -= buffer_remain;
        return OLAP_SUCCESS;
    }

    return res;
}

OLAPStatus ByteBufferStream::skip(uint64_t skip_length) {
    OLAPStatus res = _assure_data();

    if (OLAP_SUCCESS != res) {
        return res;
    }

    uint64_t skip_byte = 0;
    uint64_t byte_to_skip = skip_length;

    do {
        skip_byte = std::min((uint64_t)remaining(), byte_to_skip);
        _cur_pos += skip_byte;
        byte_to_skip -= skip_byte;
        // call assure_data to read next StorageByteBuffer if necessary
        res = _assure_data();
    } while (byte_to_skip != 0 && res == OLAP_SUCCESS);

    return res;
}

OLAPStatus ByteBufferStream::_assure_data() {
    if (_cur_pos < _byte_buffer_pos + _byte_buffer_length) {
        return OLAP_SUCCESS;
    } else if (eof()) {
        return OLAP_ERR_COLUMN_STREAM_EOF;
    }
    StreamHead* header = reinterpret_cast<StreamHead*>(&_data.data[_cur_pos]);
    _cur_pos += sizeof(StreamHead);
    _byte_buffer_length = header->length;
    _byte_buffer_pos = _cur_pos;
    return OLAP_SUCCESS;
}

}  // namespace doris
