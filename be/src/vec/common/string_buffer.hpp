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
static constexpr size_t DEFAULT_MAX_STRING_SIZE = 1073741824; // 1GB
static constexpr size_t DEFAULT_MAX_JSON_SIZE = 1073741824;   // 1GB

// store and commit data. only after commit the data is effective on its' base(ColumnString)
// everytime commit, the _data add one row.
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

// There is consumption of the buffer in the read method.
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

    void read(char* data, size_t len) {
        memcpy(data, _data, len);
        _data += len;
    }

private:
    const char* _data;
};

using VectorBufferReader = BufferReadable;
using BufferReader = BufferReadable;

// Write a variable-length unsigned integer to the buffer
// maybe it's better not to use this
inline void write_var_uint(UInt64 x, BufferWritable& buf) {
    char bytes[9];
    uint8_t i = 0;
    while (i < 9) {
        uint8_t byte = x & 0x7F;
        if (x > 0x7F) {
            byte |= 0x80;
        }

        bytes[i++] = byte;

        x >>= 7;
        if (!x) {
            break;
        }
    }
    buf.write((char*)&i, 1);
    buf.write(bytes, i);
}

inline void read_var_uint(UInt64& x, BufferReadable& buf) {
    x = 0;
    // get length from first byte firstly
    uint8_t len = 0;
    buf.read((char*)&len, 1);
    auto ref = buf.read(len);
    // read data and set it to x per byte.
    char* bytes = const_cast<char*>(ref.data);
    for (size_t i = 0; i < 9; ++i) {
        UInt64 byte = bytes[i];
        x |= (byte & 0x7F) << (7 * i);

        if (!(byte & 0x80)) {
            return;
        }
    }
}

template <typename Type>
void write_binary(const Type& x, BufferWritable& buf) {
    static_assert(std::is_standard_layout_v<Type>);
    buf.write(reinterpret_cast<const char*>(&x), sizeof(x));
}

template <typename Type>
void read_binary(Type& x, BufferReadable& buf) {
    static_assert(std::is_standard_layout_v<Type>);
    buf.read(reinterpret_cast<char*>(&x), sizeof(x));
}

template <typename Type>
    requires(std::is_same_v<Type, String> || std::is_same_v<Type, PaddedPODArray<UInt8>>)
inline void write_binary(const Type& s, BufferWritable& buf) {
    write_var_uint(s.size(), buf);
    buf.write(reinterpret_cast<const char*>(s.data()), s.size());
}

template <typename Type>
    requires(std::is_same_v<Type, String> || std::is_same_v<Type, PaddedPODArray<UInt8>>)
inline void read_binary(Type& s, BufferReadable& buf) {
    UInt64 size = 0;
    read_var_uint(size, buf);

    if (size > DEFAULT_MAX_STRING_SIZE) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "Too large string size.");
    }

    s.resize(size);
    buf.read((char*)s.data(), size);
}

inline void write_binary(const StringRef& s, BufferWritable& buf) {
    write_var_uint(s.size, buf);
    buf.write(s.data, s.size);
}

// Note that the StringRef in this function is just a reference, it should be copied outside
inline void read_binary(StringRef& s, BufferReadable& buf) {
    UInt64 size = 0;
    read_var_uint(size, buf);

    if (size > DEFAULT_MAX_STRING_SIZE) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "Too large string size.");
    }

    s = buf.read(size);
}

///TODO: Currently this function is only called in one place, we might need to convert all read_binary(StringRef) to this style? Or directly use read_binary(String)
inline StringRef read_binary_into(Arena& arena, BufferReadable& buf) {
    UInt64 size = 0;
    read_var_uint(size, buf);

    char* data = arena.alloc(size);
    buf.read(data, size);

    return {data, size};
}

} // namespace doris::vectorized
