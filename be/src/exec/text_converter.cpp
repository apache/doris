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

#include "text_converter.h"

#include <boost/algorithm/string.hpp>

#include "runtime/mem_pool.h"
#include "runtime/string_value.h"

namespace doris {

TextConverter::TextConverter(char escape_char) : _escape_char(escape_char) {}

void TextConverter::unescape_string(StringValue* value, MemPool* pool) {
    char* new_data = reinterpret_cast<char*>(pool->allocate(value->len));
    unescape_string(value->ptr, new_data, &value->len);
    value->ptr = new_data;
}

void TextConverter::unescape_string(const char* src, char* dest, size_t* len) {
    char* dest_ptr = dest;
    const char* end = src + *len;
    bool escape_next_char = false;

    while (src < end) {
        if (*src == _escape_char) {
            escape_next_char = !escape_next_char;
        } else {
            escape_next_char = false;
        }

        if (escape_next_char) {
            ++src;
        } else {
            *dest_ptr++ = *src++;
        }
    }

    char* dest_start = reinterpret_cast<char*>(dest);
    *len = dest_ptr - dest_start;
}

void TextConverter::unescape_string_on_spot(const char* src, size_t* len) {
    char* dest_ptr = const_cast<char*>(src);
    const char* end = src + *len;
    bool escape_next_char = false;

    while (src < end) {
        if (*src == _escape_char) {
            escape_next_char = !escape_next_char;
        } else {
            escape_next_char = false;
        }

        if (escape_next_char) {
            ++src;
        } else {
            *dest_ptr++ = *src++;
        }
    }

    *len = dest_ptr - src;
}

} // namespace doris
