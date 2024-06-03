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
#include <fmt/format.h>

#include <cstring>

#include "vec/columns/column_string.h"
#include "vec/common/string_ref.h"

namespace doris::vectorized {

class BufferWritable final {
public:
    explicit BufferWritable(ColumnString& vector)
            : _data(vector.get_chars()), _offsets(vector.get_offsets()) {}

    void write(const char* data, size_t len) {
        _data.insert(data, data + len);
        _now_offset += len;
    }

    void write(char c) {
        const char* p = &c;
        _data.insert(p, p + 1);
        _now_offset += 1;
    }

    // commit may not be called if exception is thrown in writes(e.g. alloc mem failed)
    void commit() {
        ColumnString::check_chars_length(_offsets.back() + _now_offset, 0);
        _offsets.push_back(_offsets.back() + _now_offset);
        _now_offset = 0;
    }

    template <typename T>
    void write_number(T data) {
        fmt::memory_buffer buffer;
        fmt::format_to(buffer, "{}", data);
        write(buffer.data(), buffer.size());
    }

private:
    ColumnString::Chars& _data;
    ColumnString::Offsets& _offsets;
    size_t _now_offset = 0;
};

using VectorBufferWriter = BufferWritable;
using BufferWriter = BufferWritable;

class BufferReadable {
public:
    explicit BufferReadable(StringRef& ref) : _data(ref.data) {}
    explicit BufferReadable(StringRef&& ref) : _data(ref.data) {}
    ~BufferReadable() = default;

    StringRef read(size_t len) {
        StringRef ref(_data, len);
        _data += len;
        return ref;
    }

    void read(char* data, int len) {
        memcpy(data, _data, len);
        _data += len;
    }

private:
    const char* _data;
};

using VectorBufferReader = BufferReadable;
using BufferReader = BufferReadable;

} // namespace doris::vectorized
