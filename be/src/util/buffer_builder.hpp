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

#ifndef DORIS_BE_SRC_COMMON_UTIL_BUFFER_BUILDER_HPP
#define DORIS_BE_SRC_COMMON_UTIL_BUFFER_BUILDER_HPP

#include <glog/loging.h>
#include <stdlib.h>

namespace doris {

// Utility class to build an in-memory buffer.
class BufferBuilder {
public:
    BufferBuilder(uint8_t* dst_buffer, int dst_len)
            : _buffer(dst_buffer), _capacity(dst_len), _size(0) {}

    BufferBuilder(char* dst_buffer, int dst_len)
            : _buffer(reinterpret_cast<uint8_t*>(dst_buffer)), _capacity(dst_len), _size(0) {}

    ~BufferBuilder() {}

    inline void append(const void* buffer, int len) {
        DCHECK_LE(_size + len, _capacity);
        memcpy(_buffer + _size, buffer, len);
        _size += len;
    }

    template <typename T>
    inline void append(const T& v) {
        append(&v, sizeof(T));
    }

    int capacity() const { return _capacity; }
    int size() const { return _size; }

private:
    uint8_t* _buffer;
    int _capacity;
    int _size;
};

} // namespace doris

#endif
