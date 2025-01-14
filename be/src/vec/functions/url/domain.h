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

#include <cstring>

#include "vec/common/string_utils/string_utils.h"
#include "vec/functions/url/find_symbols.h"
#include "vec/functions/url/protocol.h"
#include "vec/functions/url/tldLookup.h"

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

struct ExtractTopLevelDomain {
    static size_t get_reserve_length_for_element() { return 5; }

    static void execute(const char* data, size_t size, const char*& res_data, size_t& res_size) {
        res_data = data;
        res_size = 0;
        StringRef host = get_url_host(data, size);

        if (host.size == 0) {
            return;
        } else {
            auto host_view = host.to_string_view();
            if (host_view[host_view.size() - 1] == '.') {
                host_view.remove_suffix(1);
            }

            const auto* host_end = host_view.data() + host_view.size();
            const char* last_dot = find_last_symbols_or_null<'.'>(host_view.data(), host_end);
            if (!last_dot) {
                return;
            }

            /// For IPv4 addresses select nothing.
            ///
            /// NOTE: it is safe to access last_dot[1]
            /// since getURLHost() will not return a host if there is symbol after dot.
            if (is_numeric_ascii(last_dot[1])) {
                return;
            }

            res_data = last_dot + 1;
            res_size = host_end - res_data;
        }
    }
};

struct ExtractFirstSignificantSubdomain {
    static size_t get_reserve_length_for_element() { return 10; }

    static void execute(const Pos data, const size_t size, Pos& res_data, size_t& res_size,
                        Pos* out_domain_end = nullptr) {
        res_data = data;
        res_size = 0;

        Pos tmp;
        size_t domain_length = 0;
        ExtractDomain<true>::execute(data, size, tmp, domain_length);

        if (domain_length == 0) {
            return;
        }
        if (out_domain_end) {
            *out_domain_end = tmp + domain_length;
        }

        /// cut useless dot
        if (tmp[domain_length - 1] == '.') {
            --domain_length;
        }

        res_data = tmp;
        res_size = domain_length;

        const auto* begin = tmp;
        const auto* end = begin + domain_length;
        std::array<const char*, 3> last_periods {};

        const auto* pos = find_first_symbols<'.'>(begin, end);
        while (pos < end) {
            last_periods[2] = last_periods[1];
            last_periods[1] = last_periods[0];
            last_periods[0] = pos;
            pos = find_first_symbols<'.'>(pos + 1, end);
        }

        if (!last_periods[0]) {
            return;
        }

        if (!last_periods[1]) {
            res_size = last_periods[0] - begin;
            return;
        }

        if (!last_periods[2]) {
            last_periods[2] = begin - 1;
        }

        const auto* end_of_level_domain = find_first_symbols<'/'>(last_periods[0], end);
        if (!end_of_level_domain) {
            end_of_level_domain = end;
        }

        auto host_len = static_cast<size_t>(end_of_level_domain - last_periods[1] - 1);
        StringRef host {last_periods[1] + 1, host_len};
        if (tldLookup::is_valid(host.data, host.size)) {
            res_data += last_periods[2] + 1 - begin;
            res_size = last_periods[1] - last_periods[2] - 1;
        } else {
            res_data += last_periods[1] + 1 - begin;
            res_size = last_periods[0] - last_periods[1] - 1;
        }
    }
};

struct CutToFirstSignificantSubdomain {
    static size_t get_reserve_length_for_element() { return 15; }

    static void execute(const Pos data, const size_t size, Pos& res_data, size_t& res_size) {
        res_data = data;
        res_size = 0;

        Pos tmp_data = data;
        size_t tmp_length;
        Pos domain_end = data;
        ExtractFirstSignificantSubdomain::execute(data, size, tmp_data, tmp_length, &domain_end);

        if (tmp_length == 0) {
            return;
        }
        res_data = tmp_data;
        res_size = domain_end - tmp_data;
    }
};
} // namespace doris::vectorized
