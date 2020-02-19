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

#include "olap/olap_define.h"

namespace doris {

class PositionProvider;

// base of stream
// add this base class for ReadOnlyFileStream and ByteBufferStream
// to minimize the modification of RunLengthIntegerReader.
class BaseStream {
public:
    virtual ~BaseStream() { }

    virtual OLAPStatus init() = 0;

    virtual void reset(uint64_t offset, uint64_t length) = 0;

    virtual OLAPStatus read(char* byte) = 0;

    virtual OLAPStatus read(char* buffer, uint64_t* buf_size) = 0;

    virtual OLAPStatus read_all(char* buffer, uint64_t* buf_size) = 0;

    virtual OLAPStatus seek(PositionProvider* position) = 0;

    virtual OLAPStatus skip(uint64_t skip_length) = 0;

    virtual uint64_t stream_length() = 0;

    virtual bool eof() = 0;

    // 返回当前块剩余可读字节数
    virtual uint64_t available() = 0;

    virtual size_t get_buffer_size() = 0;

    virtual void get_buf(char** buf, uint32_t* remaining_bytes) = 0;

    virtual void get_position(uint32_t* position) = 0;

    virtual void set_position(uint32_t pos) = 0;

    virtual int remaining() = 0;
};

}  // namespace doris