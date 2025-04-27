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

    // Write a variable-length unsigned integer to the buffer
    // maybe it's better not to use this
    void write_var_uint(UInt64 x) {
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
        write((char*)&i, 1);
        write(bytes, i);
    }

    template <typename Type>
    void write_binary(const Type& x) {
        static_assert(std::is_standard_layout_v<Type>);
        write(reinterpret_cast<const char*>(&x), sizeof(x));
    }

    template <typename Type>
        requires(std::is_same_v<Type, String> || std::is_same_v<Type, PaddedPODArray<UInt8>>)
    void write_binary(const Type& s) {
        write_var_uint(s.size());
        write(reinterpret_cast<const char*>(s.data()), s.size());
    }

    void write_binary(const StringRef& s) {
        write_var_uint(s.size);
        write(s.data, s.size);
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

    void read_var_uint(UInt64& x) {
        x = 0;
        // get length from first byte firstly
        uint8_t len = 0;
        read((char*)&len, 1);
        auto ref = read(len);
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
    void read_binary(Type& x) {
        static_assert(std::is_standard_layout_v<Type>);
        read(reinterpret_cast<char*>(&x), sizeof(x));
    }

    template <typename Type>
        requires(std::is_same_v<Type, String> || std::is_same_v<Type, PaddedPODArray<UInt8>>)
    void read_binary(Type& s) {
        UInt64 size = 0;
        read_var_uint(size);

        if (size > DEFAULT_MAX_STRING_SIZE) {
            throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                                   "Too large string size."
                                   " size: {}, max: {}",
                                   size, DEFAULT_MAX_STRING_SIZE);
        }

        s.resize(size);
        read((char*)s.data(), size);
    }

    // Note that the StringRef in this function is just a reference, it should be copied outside
    void read_binary(StringRef& s) {
        UInt64 size = 0;
        read_var_uint(size);

        if (size > DEFAULT_MAX_STRING_SIZE) {
            throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                                   "Too large string size. "
                                   " size: {}, max: {}",
                                   size, DEFAULT_MAX_STRING_SIZE);
        }

        s = read(size);
    }

private:
    const char* _data;
};

inline void writeChar(char x, BufferWritable& buf) {
    buf.write(x);
}

/** Writes a C-string without creating a temporary object. If the string is a literal, then `strlen` is executed at the compilation stage.
  * Use when the string is a literal.
  */
#define writeCString(s, buf) (buf).write((s), strlen(s))

inline void writeJSONString(const char* begin, const char* end, BufferWritable& buf) {
    writeChar('"', buf);
    for (const char* it = begin; it != end; ++it) {
        switch (*it) {
        case '\b':
            writeChar('\\', buf);
            writeChar('b', buf);
            break;
        case '\f':
            writeChar('\\', buf);
            writeChar('f', buf);
            break;
        case '\n':
            writeChar('\\', buf);
            writeChar('n', buf);
            break;
        case '\r':
            writeChar('\\', buf);
            writeChar('r', buf);
            break;
        case '\t':
            writeChar('\\', buf);
            writeChar('t', buf);
            break;
        case '\\':
            writeChar('\\', buf);
            writeChar('\\', buf);
            break;
        case '/':
            writeChar('/', buf);
            break;
        case '"':
            writeChar('\\', buf);
            writeChar('"', buf);
            break;
        default:
            UInt8 c = *it;
            if (c <= 0x1F) {
                /// Escaping of ASCII control characters.

                UInt8 higher_half = c >> 4;
                UInt8 lower_half = c & 0xF;

                writeCString("\\u00", buf);
                writeChar('0' + higher_half, buf);

                if (lower_half <= 9) {
                    writeChar('0' + lower_half, buf);
                } else {
                    writeChar('A' + lower_half - 10, buf);
                }
            } else if (end - it >= 3 && it[0] == '\xE2' && it[1] == '\x80' &&
                       (it[2] == '\xA8' || it[2] == '\xA9')) {
                /// This is for compatibility with JavaScript, because unescaped line separators are prohibited in string literals,
                ///  and these code points are alternative line separators.

                if (it[2] == '\xA8') {
                    writeCString("\\u2028", buf);
                }
                if (it[2] == '\xA9') {
                    writeCString("\\u2029", buf);
                }

                /// Byte sequence is 3 bytes long. We have additional two bytes to skip.
                it += 2;
            } else {
                writeChar(*it, buf);
            }
        }
    }
    writeChar('"', buf);
}

inline void writeJSONString(std::string_view s, BufferWritable& buf) {
    writeJSONString(s.data(), s.data() + s.size(), buf);
}

using VectorBufferReader = BufferReadable;
using BufferReader = BufferReadable;

///TODO: Currently this function is only called in one place, we might need to convert all read_binary(StringRef) to this style? Or directly use read_binary(String)
inline StringRef read_binary_into(Arena& arena, BufferReadable& buf) {
    UInt64 size = 0;
    buf.read_var_uint(size);

    char* data = arena.alloc(size);
    buf.read(data, size);

    return {data, size};
}

} // namespace doris::vectorized
