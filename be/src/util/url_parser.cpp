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

#include "util/url_parser.h"

#include <ctype.h>
#include <stdint.h>

#include <algorithm>
#include <string>

#include "core/string_ref.h"
#include "util/string_search.hpp"

namespace doris {
const StringRef UrlParser::_s_url_authority("AUTHORITY", 9);
const StringRef UrlParser::_s_url_file("FILE", 4);
const StringRef UrlParser::_s_url_host("HOST", 4);
const StringRef UrlParser::_s_url_path("PATH", 4);
const StringRef UrlParser::_s_url_protocol("PROTOCOL", 8);
const StringRef UrlParser::_s_url_query("QUERY", 5);
const StringRef UrlParser::_s_url_ref("REF", 3);
const StringRef UrlParser::_s_url_userinfo("USERINFO", 8);
const StringRef UrlParser::_s_url_port("PORT", 4);
const StringRef UrlParser::_s_protocol("://", 3);
const StringRef UrlParser::_s_at("@", 1);
const StringRef UrlParser::_s_slash("/", 1);
const StringRef UrlParser::_s_colon(":", 1);
const StringRef UrlParser::_s_question("?", 1);
const StringRef UrlParser::_s_hash("#", 1);
const StringSearch UrlParser::_s_protocol_search(&_s_protocol);
const StringSearch UrlParser::_s_at_search(&_s_at);
const StringSearch UrlParser::_s_slash_search(&_s_slash);
const StringSearch UrlParser::_s_colon_search(&_s_colon);
const StringSearch UrlParser::_s_question_search(&_s_question);
const StringSearch UrlParser::_s_hash_search(&_s_hash);

bool UrlParser::parse_url(const StringRef& url, UrlPart part, StringRef* result) {
    result->data = nullptr;
    result->size = 0;
    // Remove leading and trailing spaces.
    StringRef trimmed_url = url.trim();

    // All parts require checking for the _s_protocol.
    int32_t protocol_pos = _s_protocol_search.search(&trimmed_url);
    if (protocol_pos < 0) {
        return false;
    }

    // Positioned to first char after '://'.
    const char* after_protocol = trimmed_url.data + protocol_pos + _s_protocol.size;
    const char* url_end = trimmed_url.data + trimmed_url.size;

    switch (part) {
    case AUTHORITY: {
        // Authority ends at first '/'.
        const char* slash = static_cast<const char*>(
                memchr(after_protocol, '/', url_end - after_protocol));
        *result = StringRef(after_protocol, (slash ? slash : url_end) - after_protocol);
        break;
    }

    case FILE:
    case PATH: {
        // Use memchr for fast '/' lookup, then sequential search for terminators.
        const char* slash_pos = static_cast<const char*>(
                memchr(after_protocol, '/', url_end - after_protocol));
        if (!slash_pos) {
            // Return empty string. This is what Hive does.
            return true;
        }
        const size_t remaining = url_end - slash_pos;
        if (part == FILE) {
            // FILE ends at '#' only.
            const char* hash_pos =
                    static_cast<const char*>(memchr(slash_pos, '#', remaining));
            *result = StringRef(slash_pos, (hash_pos ? hash_pos : url_end) - slash_pos);
        } else {
            // PATH ends at '?' or '#', whichever comes first.
            const char* q_pos =
                    static_cast<const char*>(memchr(slash_pos, '?', remaining));
            if (q_pos) {
                *result = StringRef(slash_pos, q_pos - slash_pos);
            } else {
                const char* h_pos =
                        static_cast<const char*>(memchr(slash_pos, '#', remaining));
                *result = StringRef(slash_pos, (h_pos ? h_pos : url_end) - slash_pos);
            }
        }
        break;
    }

    case HOST: {
        // Single pass: track '@' (userinfo separator), stop at ':' '/' '?' '#'.
        const char* pos = after_protocol;
        const char* start_of_host = after_protocol;
        const char* colon_pos = nullptr;
        for (; pos < url_end; ++pos) {
            switch (*pos) {
            case '@':
                start_of_host = pos + 1;
                colon_pos = nullptr; // reset: previous ':' was in userinfo
                break;
            case ':':
                if (!colon_pos) colon_pos = pos;
                break;
            case '/':
            case '?':
            case '#':
                goto host_done;
            }
        }
    host_done:
        if (colon_pos && colon_pos >= start_of_host) {
            *result = StringRef(start_of_host, colon_pos - start_of_host);
        } else {
            *result = StringRef(start_of_host, pos - start_of_host);
        }
        break;
    }

    case PROTOCOL: {
        *result = trimmed_url.substring(0, protocol_pos);
        break;
    }

    case QUERY: {
        // Use memchr for fast '?' and '#' lookup.
        const char* q_pos = static_cast<const char*>(
                memchr(after_protocol, '?', url_end - after_protocol));
        if (!q_pos) {
            return false;
        }
        const char* query_start = q_pos + 1;
        const char* hash_pos = static_cast<const char*>(
                memchr(query_start, '#', url_end - query_start));
        *result = StringRef(query_start, (hash_pos ? hash_pos : url_end) - query_start);
        break;
    }

    case REF: {
        // Find '#' using memchr.
        const char* hash_pos =
                static_cast<const char*>(memchr(after_protocol, '#', url_end - after_protocol));
        if (!hash_pos) {
            return false;
        }
        *result = StringRef(hash_pos + 1, url_end - hash_pos - 1);
        break;
    }

    case USERINFO: {
        // Find '@' using memchr.
        const char* at_pos =
                static_cast<const char*>(memchr(after_protocol, '@', url_end - after_protocol));
        if (!at_pos) {
            return false;
        }
        *result = StringRef(after_protocol, at_pos - after_protocol);
        break;
    }

    case PORT: {
        // Single pass: track '@' and ':', stop at '/' '?' '#'.
        const char* pos = after_protocol;
        const char* start_of_host = after_protocol;
        const char* colon_pos = nullptr;
        for (; pos < url_end; ++pos) {
            switch (*pos) {
            case '@':
                start_of_host = pos + 1;
                colon_pos = nullptr; // reset: previous ':' was in userinfo
                break;
            case ':':
                if (!colon_pos) colon_pos = pos;
                break;
            case '/':
            case '?':
            case '#':
                goto port_done;
            }
        }
    port_done:
        if (!colon_pos || colon_pos < start_of_host) {
            return false;
        }
        *result = StringRef(colon_pos + 1, pos - colon_pos - 1);
        break;
    }

    case INVALID:
        return false;
    }

    return true;
}

bool UrlParser::parse_url_key(const StringRef& url, UrlPart part, const StringRef& key,
                              StringRef* result) {
    // Part must be query to ask for a specific query key.
    if (part != QUERY) {
        return false;
    }

    // Remove leading and trailing spaces.
    StringRef trimmed_url = url.trim();

    // Search for the key in the url, ignoring malformed URLs for now.
    StringSearch key_search(&key);

    while (trimmed_url.size > 0) {
        // Search for the key in the current substring.
        int32_t key_pos = key_search.search(&trimmed_url);
        bool match = true;

        if (key_pos < 0) {
            return false;
        }

        // Key pos must be != 0 because it must be preceded by a '?' or a '&'.
        // Check that the char before key_pos is either '?' or '&'.
        if (key_pos == 0 ||
            (trimmed_url.data[key_pos - 1] != '?' && trimmed_url.data[key_pos - 1] != '&')) {
            match = false;
        }

        // Advance substring beyond matching key.
        trimmed_url = trimmed_url.substring(key_pos + key.size);

        if (!match) {
            continue;
        }

        if (trimmed_url.size <= 0) {
            break;
        }

        // Next character must be '=', otherwise the match cannot be a key in the query part.
        if (trimmed_url.data[0] != '=') {
            continue;
        }

        int32_t pos = 1;

        // Find ending position of key's value by matching '#' or '&'.
        while (pos < trimmed_url.size) {
            switch (trimmed_url.data[pos]) {
            case '#':
            case '&':
                *result = trimmed_url.substring(1, pos - 1);
                return true;
            }

            ++pos;
        }

        // Ending position is end of string.
        *result = trimmed_url.substring(1);
        return true;
    }

    return false;
}

UrlParser::UrlPart UrlParser::get_url_part(const StringRef& part) {
    // Quick filter on requested URL part, based on first character.
    // Hive requires the requested URL part to be all upper case.
    std::string part_str = part.to_string();
    transform(part_str.begin(), part_str.end(), part_str.begin(), ::toupper);
    StringRef newPart = StringRef(part_str);
    switch (newPart.data[0]) {
    case 'A': {
        if (!newPart.eq(_s_url_authority)) {
            return INVALID;
        }

        return AUTHORITY;
    }

    case 'F': {
        if (!newPart.eq(_s_url_file)) {
            return INVALID;
        }

        return FILE;
    }

    case 'H': {
        if (!newPart.eq(_s_url_host)) {
            return INVALID;
        }

        return HOST;
    }

    case 'P': {
        if (newPart.eq(_s_url_path)) {
            return PATH;
        } else if (newPart.eq(_s_url_protocol)) {
            return PROTOCOL;
        } else if (newPart.eq(_s_url_port)) {
            return PORT;
        } else {
            return INVALID;
        }
    }

    case 'Q': {
        if (!newPart.eq(_s_url_query)) {
            return INVALID;
        }

        return QUERY;
    }

    case 'R': {
        if (!newPart.eq(_s_url_ref)) {
            return INVALID;
        }

        return REF;
    }

    case 'U': {
        if (!newPart.eq(_s_url_userinfo)) {
            return INVALID;
        }

        return USERINFO;
    }

    default:
        return INVALID;
    }
}

StringRef UrlParser::extract_url(StringRef url, StringRef name) {
    StringRef result("", 0);
    // Remove leading and trailing spaces.
    StringRef trimmed_url = url.trim();
    // find '?'
    int32_t question_pos = _s_question_search.search(&trimmed_url);
    if (question_pos < 0) {
        // this url no parameters.
        // Example: https://doris.apache.org/
        return result;
    }

    // find '#'
    int32_t hash_pos = _s_hash_search.search(&trimmed_url);
    StringRef sub_url;
    if (hash_pos < 0) {
        sub_url = trimmed_url.substring(question_pos + 1, trimmed_url.size - question_pos - 1);
    } else {
        sub_url = trimmed_url.substring(question_pos + 1, hash_pos - question_pos - 1);
    }

    // find '&' and '=', and extract target parameter
    // Example: k1=aa&k2=bb&k3=cc&test=dd
    int64_t and_pod;
    auto len = sub_url.size;
    StringRef key_url;
    while (true) {
        if (len <= 0) {
            break;
        }
        and_pod = sub_url.find_first_of('&');
        if (and_pod != -1) {
            key_url = sub_url.substring(0, and_pod);
            sub_url = sub_url.substring(and_pod + 1, len - and_pod - 1);
        } else {
            auto end_pos = sub_url.find_first_of('#');
            key_url = end_pos == -1 ? sub_url : sub_url.substring(0, end_pos);
            sub_url = result;
        }
        len = sub_url.size;

        auto eq_pod = key_url.find_first_of('=');
        if (eq_pod == -1) {
            // invalid url. like: k1&k2=bb
            continue;
        }
        auto key_len = key_url.size;
        auto key = key_url.substring(0, eq_pod);
        if (name == key) {
            return key_url.substring(eq_pod + 1, key_len - eq_pod - 1);
        }
    }
    return result;
}
} // namespace doris
