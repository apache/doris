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

#include "olap/inverted_index_parser.h"

#include "util/string_util.h"

namespace doris {

std::string inverted_index_parser_type_to_string(InvertedIndexParserType parser_type) {
    switch (parser_type) {
    case InvertedIndexParserType::PARSER_NONE:
        return INVERTED_INDEX_PARSER_NONE;
    case InvertedIndexParserType::PARSER_STANDARD:
        return INVERTED_INDEX_PARSER_STANDARD;
    case InvertedIndexParserType::PARSER_ENGLISH:
        return INVERTED_INDEX_PARSER_ENGLISH;
    case InvertedIndexParserType::PARSER_CHINESE:
        return INVERTED_INDEX_PARSER_CHINESE;
    default:
        return INVERTED_INDEX_PARSER_UNKNOWN;
    }

    return INVERTED_INDEX_PARSER_UNKNOWN;
}

InvertedIndexParserType get_inverted_index_parser_type_from_string(const std::string& parser_str) {
    auto parser_str_lower = to_lower(parser_str);
    if (parser_str_lower == INVERTED_INDEX_PARSER_NONE) {
        return InvertedIndexParserType::PARSER_NONE;
    } else if (parser_str_lower == INVERTED_INDEX_PARSER_STANDARD) {
        return InvertedIndexParserType::PARSER_STANDARD;
    } else if (parser_str_lower == INVERTED_INDEX_PARSER_ENGLISH) {
        return InvertedIndexParserType::PARSER_ENGLISH;
    } else if (parser_str_lower == INVERTED_INDEX_PARSER_CHINESE) {
        return InvertedIndexParserType::PARSER_CHINESE;
    }

    return InvertedIndexParserType::PARSER_UNKNOWN;
}

std::string get_parser_string_from_properties(
        const std::map<std::string, std::string>& properties) {
    if (properties.find(INVERTED_INDEX_PARSER_KEY) != properties.end()) {
        return properties.at(INVERTED_INDEX_PARSER_KEY);
    } else {
        return INVERTED_INDEX_PARSER_NONE;
    }
}

} // namespace doris
