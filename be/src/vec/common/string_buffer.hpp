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
#include <cstring>
#include <fmt/format.h>

#include "vec/columns/column_string.h"

#include "vec/common/string_ref.h"

namespace doris::vectorized {
class BufferWritable {
public:
    virtual void write(const char* data, int len) = 0;
    virtual void commit() = 0;
    virtual ~BufferWritable() = default;

    template <typename T>
    void write_number(T data) {
        fmt::memory_buffer buffer;
        fmt::format_to(buffer, "{}", data);
        write(buffer.data(), buffer.size());
    }
};

class VectorBufferWriter final : public BufferWritable {
public:
    explicit VectorBufferWriter(ColumnString& vector)
            : _data(vector.get_chars()), _offsets(vector.get_offsets()) {}

    void write(const char* data, int len) override {
        _data.insert(data, data + len);
        _now_offset += len;
    }

    void commit() override {
        _data.push_back(0);
        _offsets.push_back(_offsets.back() + _now_offset + 1);
        _now_offset = 0;
    }

    ~VectorBufferWriter() { DCHECK(_now_offset == 0); }

private:
    ColumnString::Chars& _data;
    ColumnString::Offsets& _offsets;
    size_t _now_offset = 0;
};

class BufferReadable {
public:
    virtual void read(char* data, int len) = 0;
    virtual StringRef read(int len) = 0;
};

class VectorBufferReader final : public BufferReadable {
public:
    explicit VectorBufferReader(StringRef& ref) : _data(ref.data) {}
    explicit VectorBufferReader(StringRef&& ref) : _data(ref.data) {}

    StringRef read(int len) override {
        StringRef ref(_data, len);
        _data += len;
        return ref;
    }

    void read(char* data, int len) override {
        memcpy(data, _data, len);
        _data += len;
    }

private:
    const char* _data;
};

} // namespace doris::vectorized
