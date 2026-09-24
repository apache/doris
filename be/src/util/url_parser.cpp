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
#include <limits>
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

bool UrlParser::find_query_component(const StringRef& url, StringRef* query) {
    // The query component starts right after the first '?'.
    int32_t query_pos = _s_question_search.search(&url);
    if (query_pos < 0) {
        // Query component is missing.
        return false;
    }

    // The first '#' bounds the query component from the right, so this single search also
    // provides the position of the fragment.
    int32_t fragment_pos = _s_hash_search.search(&url);
    if (fragment_pos >= 0 && fragment_pos < query_pos) {
        // The '#' comes before the '?', so the '?' and everything behind it belongs to the
        // fragment and url has no query component.
        return false;
    }

    int32_t query_start = query_pos + cast_set<int32_t>(_s_question.size);
    int32_t query_end = fragment_pos >= 0 ? fragment_pos : cast_set<int32_t>(url.size);
    *query = url.substring(query_start, query_end - query_start);
    return true;
}

StringRef UrlParser::find_authority(const StringRef& protocol_end) {
    // The authority component runs from the end of '://' up to the first '/', '?' or '#',
    // whichever comes first.
    int32_t end_pos = _s_slash_search.search(&protocol_end);
    int32_t question_pos = _s_question_search.search(&protocol_end);
    if (question_pos >= 0 && (end_pos < 0 || question_pos < end_pos)) {
        end_pos = question_pos;
    }
    int32_t hash_pos = _s_hash_search.search(&protocol_end);
    if (hash_pos >= 0 && (end_pos < 0 || hash_pos < end_pos)) {
        end_pos = hash_pos;
    }
    return protocol_end.substring(0, end_pos);
}

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
    StringRef protocol_end = trimmed_url.substring(protocol_pos + _s_protocol.size);

    switch (part) {
    case AUTHORITY: {
        *result = find_authority(protocol_end);
        break;
    }

    case FILE:
    case PATH: {
        int32_t start_pos = _s_slash_search.search(&protocol_end);

        int32_t question_pos = _s_question_search.search(&protocol_end);
        int32_t hash_pos = _s_hash_search.search(&protocol_end);
        if (start_pos < 0 || (question_pos >= 0 && question_pos < start_pos) ||
            (hash_pos >= 0 && hash_pos < start_pos)) {
            // Return empty string. This is what Hive does.
            return true;
        }

        StringRef path_start = protocol_end.substring(start_pos);
        int32_t end_pos;

        if (part == FILE) {
            // End _s_at '#'.
            end_pos = _s_hash_search.search(&path_start);
        } else {
            // End string _s_at next '?' or '#'.
            end_pos = _s_question_search.search(&path_start);
            int32_t path_hash_pos = _s_hash_search.search(&path_start);
            if (end_pos < 0 || (path_hash_pos >= 0 && path_hash_pos < end_pos)) {
                end_pos = path_hash_pos;
            }
        }

        *result = path_start.substring(0, end_pos);
        break;
    }

    case HOST: {
        StringRef authority = find_authority(protocol_end);
        int32_t userinfo_end = -1;
        for (int32_t i = 0; i < authority.size; ++i) {
            if (authority.data[i] == '@') {
                userinfo_end = i;
            }
        }

        StringRef host_start = authority.substring(userinfo_end + 1);
        int32_t end_pos = host_start.size;
        if (!host_start.empty() && host_start.data[0] == '[') {
            for (int32_t i = 1; i < host_start.size; ++i) {
                if (host_start.data[i] == ']') {
                    end_pos = i + 1;
                    break;
                }
            }
        } else {
            int32_t colon_pos = _s_colon_search.search(&host_start);
            if (colon_pos >= 0) {
                end_pos = colon_pos;
            }
        }
        *result = host_start.substring(0, end_pos);
        break;
    }

    case PROTOCOL: {
        *result = trimmed_url.substring(0, protocol_pos);
        break;
    }

    case QUERY: {
        // The query component starts at the first '?' and ends before the '#' that starts the
        // fragment, so a url whose first '#' comes before its first '?' has no query.
        StringRef query;
        if (!find_query_component(protocol_end, &query)) {
            // Indicate no query was found.
            return false;
        }

        *result = query;
        break;
    }

    case REF: {
        // Find '#'.
        int32_t start_pos = _s_hash_search.search(&protocol_end);

        if (start_pos < 0) {
            // Indicate no user and pass were given.
            return false;
        }

        *result = protocol_end.substring(start_pos + _s_hash.size);
        break;
    }

    case USERINFO: {
        StringRef authority = find_authority(protocol_end);
        int32_t end_pos = -1;
        for (int32_t i = 0; i < authority.size; ++i) {
            if (authority.data[i] == '@') {
                end_pos = i;
            }
        }

        if (end_pos < 0) {
            // Indicate no user and pass were given.
            return false;
        }

        *result = authority.substring(0, end_pos);
        break;
    }

    case PORT: {
        StringRef authority = find_authority(protocol_end);
        int32_t userinfo_end = -1;
        for (int32_t i = 0; i < authority.size; ++i) {
            if (authority.data[i] == '@') {
                userinfo_end = i;
            }
        }

        StringRef host_start = authority.substring(userinfo_end + 1);
        int32_t port_start = -1;
        if (!host_start.empty() && host_start.data[0] == '[') {
            for (int32_t i = 1; i < host_start.size; ++i) {
                if (host_start.data[i] == ']') {
                    if (i + 1 < host_start.size && host_start.data[i + 1] == ':') {
                        port_start = i + 2;
                    }
                    break;
                }
            }
        } else {
            int32_t colon_pos = _s_colon_search.search(&host_start);
            if (colon_pos >= 0) {
                port_start = colon_pos + 1;
            }
        }
        if (port_start < 0) {
            return false;
        }

        *result = host_start.substring(port_start);
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

    // The key can only be found in the query component, which starts at the first '?' and ends
    // before the '#' that starts the fragment (if any).
    StringRef query;
    if (!find_query_component(trimmed_url, &query)) {
        // Query component is missing, the whole url is the path plus the fragment.
        return false;
    }

    // Search for the key inside the query component, ignoring malformed URLs for now.
    StringSearch key_search(&key);
    // Offset of the next search inside the query component. The query component starts right
    // after the '?', so a key at offset 0 is a query key as well.
    int32_t offset = 0;

    while (offset < query.size) {
        // Search for the key in the remaining part of the query component.
        StringRef rest = query.substring(offset);
        int32_t key_pos = key_search.search(&rest);

        if (key_pos < 0) {
            // No (more) key in the query component.
            break;
        }

        offset += key_pos;
        // The key must start the query component or be preceded by a '&'.
        if (offset != 0 && query.data[offset - 1] != '&') {
            // The matched text is not a key, step over it and keep searching.
            offset += cast_set<int32_t>(key.size);
            continue;
        }

        // Positioned to the char right after the key.
        int32_t value_pos = offset + cast_set<int32_t>(key.size);

        // The key must be followed by a '=' and a value, otherwise the match cannot be a key.
        if (value_pos >= cast_set<int32_t>(query.size) || query.data[value_pos] != '=') {
            // Step over the matched text and keep searching.
            offset += cast_set<int32_t>(key.size);
            continue;
        }

        ++value_pos;

        // Find the ending position of the key's value by matching '&'.
        StringRef value_rest = query.substring(value_pos);
        size_t value_end_rel_pos = value_rest.find_first_of('&');
        int32_t value_end_pos = value_end_rel_pos == std::numeric_limits<size_t>::max()
                                        ? cast_set<int32_t>(query.size)
                                        : value_pos + cast_set<int32_t>(value_end_rel_pos);
        // A duplicated key keeps the behaviour of returning the first value.
        *result = query.substring(value_pos, value_end_pos - value_pos);
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
    // The parameters can only be found in the query component, which starts at the first '?'
    // and ends before the '#' that starts the fragment (if any).
    StringRef sub_url;
    if (!find_query_component(trimmed_url, &sub_url)) {
        // this url no parameters.
        // Example: https://doris.apache.org/
        return result;
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
            key_url = sub_url;
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
