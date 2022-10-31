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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/URL/domain.h
// and modified by Doris

#pragma once

// #include <base/find_symbols.h>
#include <vec/common/string_utils/string_utils.h>

#include <cstring>

#include "vec/functions/url/protocol.h"

namespace doris::vectorized {

inline StringRef check_and_return_host(const Pos& pos, const Pos& dot_pos,
                                       const Pos& start_of_host) {
    if (!dot_pos || start_of_host >= pos || pos - dot_pos == 1) return StringRef {};

    auto after_dot = *(dot_pos + 1);
    if (after_dot == ':' || after_dot == '/' || after_dot == '?' || after_dot == '#')
        return StringRef {};

    return StringRef(start_of_host, pos - start_of_host);
}

/// Extracts host from given url.
///
/// @return empty StringRef if the host is not valid (i.e. it does not have dot, or there no symbol after dot).
inline StringRef get_url_host(const char* data, size_t size) {
    Pos pos = data;
    Pos end = data + size;

    if (*pos == '/' && *(pos + 1) == '/') {
        pos += 2;
    } else {
        Pos scheme_end = data + std::min(size, 16UL);
        for (++pos; pos < scheme_end; ++pos) {
            if (!is_alpha_numeric_ascii(*pos)) {
                switch (*pos) {
                case '.':
                case '-':
                case '+':
                    break;
                case ' ': /// restricted symbols
                case '\t':
                case '<':
                case '>':
                case '%':
                case '{':
                case '}':
                case '|':
                case '\\':
                case '^':
                case '~':
                case '[':
                case ']':
                case ';':
                case '=':
                case '&':
                    return StringRef {};
                default:
                    goto exloop;
                }
            }
        }
    exloop:
        if ((scheme_end - pos) > 2 && *pos == ':' && *(pos + 1) == '/' && *(pos + 2) == '/')
            pos += 3;
        else
            pos = data;
    }

    Pos dot_pos = nullptr;
    const auto* start_of_host = pos;
    for (; pos < end; ++pos) {
        switch (*pos) {
        case '.':
            dot_pos = pos;
            break;
        case ':': /// end symbols
        case '/':
        case '?':
        case '#':
            return check_and_return_host(pos, dot_pos, start_of_host);
        case '@': /// myemail@gmail.com
            start_of_host = pos + 1;
            break;
        case ' ': /// restricted symbols in whole URL
        case '\t':
        case '<':
        case '>':
        case '%':
        case '{':
        case '}':
        case '|':
        case '\\':
        case '^':
        case '~':
        case '[':
        case ']':
        case ';':
        case '=':
        case '&':
            return StringRef {};
        }
    }

    return check_and_return_host(pos, dot_pos, start_of_host);
}

template <bool without_www>
struct ExtractDomain {
    static size_t get_reserve_length_for_element() { return 15; }

    static void execute(Pos data, size_t size, Pos& res_data, size_t& res_size) {
        StringRef host = get_url_host(data, size);

        if (host.size == 0) {
            res_data = data;
            res_size = 0;
        } else {
            if (without_www && host.size > 4 && !strncmp(host.data, "www.", 4))
                host = {host.data + 4, host.size - 4};

            res_data = host.data;
            res_size = host.size;
        }
    }
};

} // namespace doris::vectorized
