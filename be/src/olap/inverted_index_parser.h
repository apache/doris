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

#include <map>
#include <memory>
#include <string>

#include "util/debug_points.h"

namespace lucene {
namespace analysis {
class Analyzer;
}
} // namespace lucene

namespace doris {

enum class InvertedIndexParserType {
    PARSER_UNKNOWN = 0,
    PARSER_NONE = 1,
    PARSER_STANDARD = 2,
    PARSER_ENGLISH = 3,
    PARSER_CHINESE = 4,
    PARSER_UNICODE = 5,
};

using CharFilterMap = std::map<std::string, std::string>;

struct InvertedIndexCtx {
    InvertedIndexParserType parser_type;
    std::string parser_mode;
    CharFilterMap char_filter_map;
    lucene::analysis::Analyzer* analyzer = nullptr;
};

using InvertedIndexCtxSPtr = std::shared_ptr<InvertedIndexCtx>;

const std::string INVERTED_INDEX_PARSER_TRUE = "true";
const std::string INVERTED_INDEX_PARSER_FALSE = "false";

const std::string INVERTED_INDEX_PARSER_MODE_KEY = "parser_mode";
const std::string INVERTED_INDEX_PARSER_FINE_GRANULARITY = "fine_grained";
const std::string INVERTED_INDEX_PARSER_COARSE_GRANULARITY = "coarse_grained";

const std::string INVERTED_INDEX_PARSER_KEY = "parser";
const std::string INVERTED_INDEX_PARSER_UNKNOWN = "unknown";
const std::string INVERTED_INDEX_PARSER_NONE = "none";
const std::string INVERTED_INDEX_PARSER_STANDARD = "standard";
const std::string INVERTED_INDEX_PARSER_UNICODE = "unicode";
const std::string INVERTED_INDEX_PARSER_ENGLISH = "english";
const std::string INVERTED_INDEX_PARSER_CHINESE = "chinese";

const std::string INVERTED_INDEX_PARSER_PHRASE_SUPPORT_KEY = "support_phrase";
const std::string INVERTED_INDEX_PARSER_PHRASE_SUPPORT_YES = "true";
const std::string INVERTED_INDEX_PARSER_PHRASE_SUPPORT_NO = "false";

const std::string INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE = "char_filter_type";
const std::string INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN = "char_filter_pattern";
const std::string INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT = "char_filter_replacement";

const std::string INVERTED_INDEX_PARSER_IGNORE_ABOVE_KEY = "ignore_above";
const std::string INVERTED_INDEX_PARSER_IGNORE_ABOVE_VALUE = "256";

const std::string INVERTED_INDEX_PARSER_LOWERCASE_KEY = "lower_case";

const std::string INVERTED_INDEX_PARSER_STOPWORDS_KEY = "stopwords";

std::string inverted_index_parser_type_to_string(InvertedIndexParserType parser_type);

InvertedIndexParserType get_inverted_index_parser_type_from_string(const std::string& parser_str);

std::string get_parser_string_from_properties(const std::map<std::string, std::string>& properties);
std::string get_parser_mode_string_from_properties(
        const std::map<std::string, std::string>& properties);
std::string get_parser_phrase_support_string_from_properties(
        const std::map<std::string, std::string>& properties);

CharFilterMap get_parser_char_filter_map_from_properties(
        const std::map<std::string, std::string>& properties);

// get parser ignore_above value from properties
std::string get_parser_ignore_above_value_from_properties(
        const std::map<std::string, std::string>& properties);

template <bool ReturnTrue = false>
std::string get_parser_lowercase_from_properties(
        const std::map<std::string, std::string>& properties) {
    DBUG_EXECUTE_IF("inverted_index_parser.get_parser_lowercase_from_properties", { return ""; })

    if (properties.find(INVERTED_INDEX_PARSER_LOWERCASE_KEY) != properties.end()) {
        return properties.at(INVERTED_INDEX_PARSER_LOWERCASE_KEY);
    } else {
        if constexpr (ReturnTrue) {
            return INVERTED_INDEX_PARSER_TRUE;
        } else {
            return "";
        }
    }
}

std::string get_parser_stopwords_from_properties(
        const std::map<std::string, std::string>& properties);

} // namespace doris
