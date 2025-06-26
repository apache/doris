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

} // namespace doris::vectorized
