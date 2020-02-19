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

#include "util/slice.h"
#include "olap/olap_define.h"
#include "olap/file_stream.h"

class PositionProvider;

namespace doris {

// To reuse RunLengthIntegerReader in segment v2, add ByteBufferStream which derive from
// BaseStream. RunLengthIntegerReader will accept BaseStream* instead of ReadOnlyFileStream*
// as argument.
// Unlike ReadOnlyFileStream, ByteBufferStream read data from StorageByteBuffers instead of file.
class ByteBufferStream : public BaseStream {
public:
    ByteBufferStream(const Slice& data) :
            _data(data), _cur_pos(0),
            _byte_buffer_pos(0), _byte_buffer_length(0) { }

    OLAPStatus init() {
        return _assure_data();
    }

    void reset(uint64_t offset, uint64_t length) override {
        _cur_pos = 0;
        _byte_buffer_pos = 0;
        _byte_buffer_length = 0;
    }

    // read one byte from the stream and modify the offset
    // If reach the enc of stream, return OLAP_ERR_COLUMN_STREAM_EOF
    OLAPStatus read(char* byte) override;

    // read buf_size data from stream to buffer
    // return OLAP_ERR_COLUMN_STREAM_EOF if reach end of stream
    OLAPStatus read(char* buffer, uint64_t* buf_size) override;

    // read all data from stream to buffer and set the size to buf_size
    OLAPStatus read_all(char* buffer, uint64_t* buf_size) override;

    // seek to position
    OLAPStatus seek(PositionProvider* position) override {
        return OLAP_ERR_FUNC_NOT_IMPLEMENTED;
    }

    // skip skip_length bytes
    OLAPStatus skip(uint64_t skip_length) override;

    // return stream length
    uint64_t stream_length() override {
        return _data.size;
    }

    // end of stream
    bool eof() override {
        return _cur_pos >= _data.size;
    }

    // remain bytes of the stream
    uint64_t available() override {
        return _data.size - _cur_pos;
    }

    // get the default buffer size
    size_t get_buffer_size() override {
        return _data.size;
    }

    // return the memory buffer's first byte address and the remain bytes
    inline void get_buf(char** buf, uint32_t* remaining_bytes) override {
        if (_byte_buffer_length == 0) {
            *buf = nullptr;
            *remaining_bytes = 0;
        } else {
             *buf = _data.data;
             *remaining_bytes = remaining();
        }
    }

    // return the current read offset position
    inline void get_position(uint32_t* position) override {
        *position = _cur_pos;
    }

    // set the read offset
    void set_position(uint32_t pos) override {
        _cur_pos = pos;
    }

    // return the remain bytes in the stream
    int remaining() override {
        return _byte_buffer_pos + _byte_buffer_length - _cur_pos;
    }

private:
    // read the next StorageByteBuffer data
    OLAPStatus _assure_data();

private:
    // memory byte data
    Slice _data;
    // current data offset
    uint32_t _cur_pos;
    // offset in current StorageByteBuffer
    uint32_t _byte_buffer_pos;
    // length of current StorageByteBuffer
    uint32_t _byte_buffer_length;
};

}  // namespace doris
