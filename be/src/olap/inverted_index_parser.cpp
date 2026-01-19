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
    case InvertedIndexParserType::PARSER_UNICODE:
        return INVERTED_INDEX_PARSER_UNICODE;
    case InvertedIndexParserType::PARSER_ENGLISH:
        return INVERTED_INDEX_PARSER_ENGLISH;
    case InvertedIndexParserType::PARSER_CHINESE:
        return INVERTED_INDEX_PARSER_CHINESE;
    case InvertedIndexParserType::PARSER_ICU:
        return INVERTED_INDEX_PARSER_ICU;
    case InvertedIndexParserType::PARSER_BASIC:
        return INVERTED_INDEX_PARSER_BASIC;
    case InvertedIndexParserType::PARSER_IK:
        return INVERTED_INDEX_PARSER_IK;
    default:
        return INVERTED_INDEX_PARSER_UNKNOWN;
    }
}

InvertedIndexParserType get_inverted_index_parser_type_from_string(const std::string& parser_str) {
    auto parser_str_lower = to_lower(parser_str);
    if (parser_str_lower == INVERTED_INDEX_PARSER_NONE) {
        return InvertedIndexParserType::PARSER_NONE;
    } else if (parser_str_lower == INVERTED_INDEX_PARSER_STANDARD) {
        return InvertedIndexParserType::PARSER_STANDARD;
    } else if (parser_str_lower == INVERTED_INDEX_PARSER_UNICODE) {
        return InvertedIndexParserType::PARSER_UNICODE;
    } else if (parser_str_lower == INVERTED_INDEX_PARSER_ENGLISH) {
        return InvertedIndexParserType::PARSER_ENGLISH;
    } else if (parser_str_lower == INVERTED_INDEX_PARSER_CHINESE) {
        return InvertedIndexParserType::PARSER_CHINESE;
    } else if (parser_str_lower == INVERTED_INDEX_PARSER_ICU) {
        return InvertedIndexParserType::PARSER_ICU;
    } else if (parser_str_lower == INVERTED_INDEX_PARSER_BASIC) {
        return InvertedIndexParserType::PARSER_BASIC;
    } else if (parser_str_lower == INVERTED_INDEX_PARSER_IK) {
        return InvertedIndexParserType::PARSER_IK;
    }

    return InvertedIndexParserType::PARSER_UNKNOWN;
}

std::string get_parser_string_from_properties(
        const std::map<std::string, std::string>& properties) {
    auto it = properties.find(INVERTED_INDEX_PARSER_KEY);
    if (it != properties.end()) {
        return it->second;
    }
    it = properties.find(INVERTED_INDEX_PARSER_KEY_ALIAS);
    if (it != properties.end()) {
        return it->second;
    }
    return INVERTED_INDEX_PARSER_NONE;
}

std::string get_parser_mode_string_from_properties(
        const std::map<std::string, std::string>& properties) {
    if (auto it = properties.find(INVERTED_INDEX_PARSER_MODE_KEY); it != properties.end()) {
        return it->second;
    }
    auto parser_it = properties.find(INVERTED_INDEX_PARSER_KEY);
    if (parser_it == properties.end()) {
        parser_it = properties.find(INVERTED_INDEX_PARSER_KEY_ALIAS);
    }
    if (parser_it != properties.end() && parser_it->second == INVERTED_INDEX_PARSER_IK) {
        return INVERTED_INDEX_PARSER_SMART;
    }
    return INVERTED_INDEX_PARSER_COARSE_GRANULARITY;
}

std::string get_parser_phrase_support_string_from_properties(
        const std::map<std::string, std::string>& properties) {
    if (auto it = properties.find(INVERTED_INDEX_PARSER_PHRASE_SUPPORT_KEY);
        it != properties.end()) {
        return it->second;
    }
    return INVERTED_INDEX_PARSER_PHRASE_SUPPORT_NO;
}

CharFilterMap get_parser_char_filter_map_from_properties(
        const std::map<std::string, std::string>& properties) {
    if (!properties.contains(INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE)) {
        return {};
    }

    CharFilterMap char_filter_map;
    std::string type = properties.at(INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE);
    if (type == INVERTED_INDEX_CHAR_FILTER_CHAR_REPLACE) {
        // type
        char_filter_map[INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE] =
                INVERTED_INDEX_CHAR_FILTER_CHAR_REPLACE;

        // pattern
        if (!properties.contains(INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN)) {
            return {};
        }
        std::string pattern = properties.at(INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN);
        char_filter_map[INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN] = pattern;

        // placement
        std::string replacement = " ";
        if (properties.contains(INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT)) {
            replacement = properties.at(INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT);
        }
        char_filter_map[INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT] = replacement;
    } else {
        return {};
    }

    return char_filter_map;
}

std::string get_parser_ignore_above_value_from_properties(
        const std::map<std::string, std::string>& properties) {
    if (auto it = properties.find(INVERTED_INDEX_PARSER_IGNORE_ABOVE_KEY); it != properties.end()) {
        return it->second;
    }
    return INVERTED_INDEX_PARSER_IGNORE_ABOVE_VALUE;
}

std::string get_parser_stopwords_from_properties(
        const std::map<std::string, std::string>& properties) {
    DBUG_EXECUTE_IF("inverted_index_parser.get_parser_stopwords_from_properties", { return ""; })
    if (auto it = properties.find(INVERTED_INDEX_PARSER_STOPWORDS_KEY); it != properties.end()) {
        return it->second;
    }
    return "";
}

std::string get_parser_dict_compression_from_properties(
        const std::map<std::string, std::string>& properties) {
    if (auto it = properties.find(INVERTED_INDEX_PARSER_DICT_COMPRESSION_KEY);
        it != properties.end()) {
        return it->second;
    }
    return "";
}

std::string get_analyzer_name_from_properties(
        const std::map<std::string, std::string>& properties) {
    auto it = properties.find(INVERTED_INDEX_ANALYZER_NAME_KEY);
    if (it != properties.end() && !it->second.empty()) {
        return it->second;
    }

    it = properties.find(INVERTED_INDEX_NORMALIZER_NAME_KEY);
    if (it != properties.end() && !it->second.empty()) {
        return it->second;
    }

    return "";
}

std::string normalize_analyzer_key(std::string_view analyzer) {
    if (analyzer.empty()) {
        return INVERTED_INDEX_DEFAULT_ANALYZER_KEY;
    }
    auto normalized = to_lower(std::string(analyzer));
    if (normalized == INVERTED_INDEX_DEFAULT_ANALYZER_KEY) {
        return INVERTED_INDEX_DEFAULT_ANALYZER_KEY;
    }
    // "none" means no tokenization requested - treat as default so it matches
    // indexes created without a parser (which also have __default__ key).
    // This allows MATCH queries without USING ANALYZER to work with STRING_TYPE indexes.
    if (normalized == INVERTED_INDEX_PARSER_NONE) {
        return INVERTED_INDEX_DEFAULT_ANALYZER_KEY;
    }
    return normalized;
}

std::string build_analyzer_key_from_properties(
        const std::map<std::string, std::string>& properties) {
    if (properties.empty()) {
        return INVERTED_INDEX_DEFAULT_ANALYZER_KEY;
    }

    auto custom_it = properties.find(INVERTED_INDEX_ANALYZER_NAME_KEY);
    if (custom_it != properties.end() && !custom_it->second.empty()) {
        return normalize_analyzer_key(custom_it->second);
    }

    std::string parser;
    auto parser_it = properties.find(INVERTED_INDEX_PARSER_KEY);
    if (parser_it != properties.end()) {
        parser = parser_it->second;
    } else {
        parser_it = properties.find(INVERTED_INDEX_PARSER_KEY_ALIAS);
        if (parser_it != properties.end()) {
            parser = parser_it->second;
        }
    }

    // normalize_analyzer_key handles empty and "none" parser consistently
    return normalize_analyzer_key(parser);
}

// ============================================================================
// AnalyzerConfigParser implementation
// ============================================================================

std::string AnalyzerConfigParser::normalize_to_lower(const std::string& value) {
    return to_lower(value);
}

bool AnalyzerConfigParser::is_builtin_analyzer(const std::string& normalized_name) {
    if (normalized_name.empty()) {
        return false;
    }
    auto parser_type = get_inverted_index_parser_type_from_string(normalized_name);
    return parser_type != InvertedIndexParserType::PARSER_UNKNOWN;
}

std::string AnalyzerConfigParser::compute_analyzer_key(const std::string& value) {
    auto normalized = normalize_analyzer_key(value);
    return normalized.empty() ? INVERTED_INDEX_DEFAULT_ANALYZER_KEY : normalized;
}

AnalyzerConfig AnalyzerConfigParser::parse(const std::string& analyzer_name,
                                           const std::string& parser_type_str) {
    AnalyzerConfig config;

    // Try to determine parser type from parser_type_str first
    auto parser_type = get_inverted_index_parser_type_from_string(parser_type_str);
    const std::string normalized_analyzer = normalize_to_lower(analyzer_name);

    // If parser_type_str didn't yield a valid type, try analyzer_name
    if (parser_type == InvertedIndexParserType::PARSER_UNKNOWN) {
        parser_type = get_inverted_index_parser_type_from_string(normalized_analyzer);
    }

    const bool analyzer_is_builtin = is_builtin_analyzer(normalized_analyzer);

    // Case 1: analyzer_name is non-empty and NOT a builtin type => custom analyzer
    if (!analyzer_name.empty() && !analyzer_is_builtin) {
        config.custom_analyzer = analyzer_name;
        config.parser_type = InvertedIndexParserType::PARSER_NONE;
        config.analyzer_key = compute_analyzer_key(analyzer_name);
    } else {
        // Case 2: builtin analyzer or no analyzer specified
        config.custom_analyzer.clear();
        config.parser_type = (parser_type == InvertedIndexParserType::PARSER_UNKNOWN)
                                     ? InvertedIndexParserType::PARSER_NONE
                                     : parser_type;

        // Determine analyzer_key from available names
        if (!analyzer_name.empty()) {
            config.analyzer_key = compute_analyzer_key(analyzer_name);
        } else if (!parser_type_str.empty()) {
            config.analyzer_key = compute_analyzer_key(parser_type_str);
        } else {
            config.analyzer_key = INVERTED_INDEX_DEFAULT_ANALYZER_KEY;
        }
    }

    return config;
}

} // namespace doris
