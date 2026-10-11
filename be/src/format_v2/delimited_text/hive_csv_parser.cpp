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

#include "format_v2/delimited_text/hive_csv_parser.h"

#include <unicode/uchar.h>
#include <unicode/utf8.h>

#include <string_view>
#include <utility>

namespace doris::format::csv {
namespace {

UChar32 next_character(std::string_view input, size_t& pos) {
    UChar32 character;
    U8_NEXT(input, pos, input.size(), character);
    return character;
}

} // namespace

HiveCsvParser::HiveCsvParser(std::string separator, char quote, char escape, size_t field_limit)
        : _separator(std::move(separator)),
          _quote(quote),
          _escape(escape),
          _field_limit(field_limit) {}

void HiveCsvParser::parse(const Slice& line, std::vector<Slice>* fields) {
    fields->clear();
    _decoded.clear();
    _field_ends.clear();
    std::string_view input(line.data, line.size);
    // A terminal CR may reach here at EOF, before the line reader can look ahead for CRLF.
    if (!input.empty() && input.back() == '\r') {
        input.remove_suffix(1);
    }
    if (input.empty()) {
        return;
    }
    bool in_quotes = false;
    bool in_field = false;
    bool all_whitespace = true;
    bool previous_separator = false;
    size_t field_start = 0;
    size_t java_position = 0;
    for (size_t pos = 0; pos < input.size() && _field_ends.size() < _field_limit;) {
        size_t start = pos;
        const UChar32 character = next_character(input, pos);
        bool separator = input.substr(start, pos - start) == _separator;
        if (character == _escape) {
            // OpenCSV checks the escape branch even for NUL. An escape outside a field is
            // discarded, while quotes/escapes inside a field consume their following byte.
            if ((in_quotes || in_field) && pos < input.size() &&
                (input[pos] == _quote || input[pos] == _escape)) {
                _decoded.push_back(input[pos++]);
                all_whitespace = all_whitespace && u_isWhitespace(input[pos - 1]);
                ++java_position;
            }
        } else if (character == _quote) {
            if ((in_quotes || in_field) && pos < input.size() && input[pos] == _quote) {
                _decoded.push_back(input[pos++]);
                all_whitespace = all_whitespace && u_isWhitespace(_quote);
                ++java_position;
            } else {
                // OpenCSV's embedded-quote rule uses the UTF-16 position in the entire record,
                // not the field offset. Quotes still toggle state after a non-whitespace prefix.
                if (java_position > 2 && !previous_separator && pos < input.size() &&
                    !input.substr(pos).starts_with(_separator)) {
                    if (_decoded.size() > field_start && all_whitespace) {
                        _decoded.resize(field_start);
                    } else {
                        _decoded.push_back(_quote);
                        all_whitespace = all_whitespace && u_isWhitespace(_quote);
                    }
                }
                in_quotes = !in_quotes;
            }
            in_field = !in_field;
        } else if (separator && !in_quotes) {
            _field_ends.push_back(_decoded.size());
            field_start = _decoded.size();
            all_whitespace = true;
            in_field = false;
        } else {
            _decoded.append(input.substr(start, pos - start));
            all_whitespace = all_whitespace && u_isWhitespace(character);
            in_field = true;
        }
        // Escaped lookahead cannot be a separator: FE validates distinct active characters.
        previous_separator = separator;
        java_position += character > 0xffff ? 2 : 1;
    }
    // CSVReader.readNext() returns completed fields at EOF and drops only the pending field.
    // Do not merge the next physical record or discard fields completed before an unmatched quote.
    if (!in_quotes && _field_ends.size() < _field_limit) {
        _field_ends.push_back(_decoded.size());
    }
    size_t start = 0;
    for (size_t end : _field_ends) {
        fields->emplace_back(_decoded.data() + start, end - start);
        start = end;
    }
}

} // namespace doris::format::csv
