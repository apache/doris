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

#include "storage/index/inverted/inverted_index_parser.h"

#include <fmt/format.h>

#include <algorithm>

#include "common/config.h"
#include "storage/index/inverted/analyzer/analyzer.h"
#include "storage/tablet/tablet_schema.h"
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
    case InvertedIndexParserType::PARSER_KUROMOJI:
        return INVERTED_INDEX_PARSER_KUROMOJI;
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
    } else if (parser_str_lower == INVERTED_INDEX_PARSER_KUROMOJI) {
        return InvertedIndexParserType::PARSER_KUROMOJI;
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
    if (parser_it != properties.end()) {
        if (parser_it->second == INVERTED_INDEX_PARSER_IK) {
            return INVERTED_INDEX_PARSER_SMART;
        }
        if (parser_it->second == INVERTED_INDEX_PARSER_KUROMOJI) {
            return INVERTED_INDEX_PARSER_KUROMOJI_SEARCH;
        }
    }
    if (auto analyzer_it = properties.find(INVERTED_INDEX_ANALYZER_NAME_KEY);
        analyzer_it != properties.end() && analyzer_it->second == INVERTED_INDEX_PARSER_KUROMOJI) {
        return INVERTED_INDEX_PARSER_KUROMOJI_SEARCH;
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

bool should_write_index_norms(const TabletIndex& index_meta) {
    // A variant path index (a field_pattern index, or the copy inherited by one extracted
    // subcolumn, which carries the path as its index suffix) is one of possibly thousands in a
    // segment, so its norms can dwarf the data. The config drops them whatever the property says,
    // so that a cluster can reclaim that space without rewriting its index definitions.
    const bool variant_path_index =
            !index_meta.get_index_suffix().empty() || !index_meta.field_pattern().empty();
    if (variant_path_index && config::inverted_index_skip_norms_for_variant) {
        return false;
    }
    const auto& properties = index_meta.properties();
    if (auto it = properties.find(INVERTED_INDEX_NORMS_KEY); it != properties.end()) {
        return it->second == INVERTED_INDEX_PARSER_TRUE;
    }
    return true;
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
    return std::string(analyzer);
}

namespace {

constexpr std::string_view ENCODED_ANALYZER_KEY_PREFIX = "#analysis:";

std::string append_char_filter_key(std::string key, const CharFilterMap& char_filter_map,
                                   bool lowercase_ik) {
    if (char_filter_map.empty()) {
        return key;
    }
    DORIS_CHECK_EQ(char_filter_map.at(INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE),
                   INVERTED_INDEX_CHAR_FILTER_CHAR_REPLACE);
    std::string pattern = char_filter_map.at(INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN);
    const auto& replacement = char_filter_map.at(INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT);
    DORIS_CHECK_EQ(replacement.size(), 1);
    const char replacement_byte = replacement.front();
    std::erase_if(pattern, [replacement_byte, lowercase_ik](char byte) {
        return byte == replacement_byte ||
               (lowercase_ik && replacement_byte >= 'a' && replacement_byte <= 'z' &&
                byte == replacement_byte - ('a' - 'A'));
    });
    std::ranges::sort(pattern);
    pattern.erase(std::ranges::unique(pattern).begin(), pattern.end());
    if (pattern.empty()) {
        return key;
    }
    return fmt::format("{}char_replace={}:{}:{}:{}:{}:{};", ENCODED_ANALYZER_KEY_PREFIX, key.size(),
                       key, pattern.size(), pattern, replacement.size(), replacement);
}

std::string build_selection_key(std::string key, const std::string& parser_mode, bool lowercase,
                                const CharFilterMap& char_filter_map) {
    const bool builtin_ik = key == INVERTED_INDEX_PARSER_IK;
    if (builtin_ik) {
        key = fmt::format("{}ik|mode={}|lower_case={}", ENCODED_ANALYZER_KEY_PREFIX,
                          parser_mode == INVERTED_INDEX_PARSER_SMART
                                  ? INVERTED_INDEX_PARSER_SMART
                                  : INVERTED_INDEX_PARSER_MAX_WORD,
                          lowercase);
    } else if (key.starts_with(ENCODED_ANALYZER_KEY_PREFIX)) {
        // Keep arbitrary policy names separate from encoded index configurations.
        key = fmt::format("{}name={}:{}", ENCODED_ANALYZER_KEY_PREFIX, key.size(), key);
    }
    return append_char_filter_key(std::move(key), char_filter_map, builtin_ik && lowercase);
}

} // namespace

std::string build_analyzer_key_from_properties(
        const std::map<std::string, std::string>& properties) {
    auto key = get_analyzer_name_from_properties(properties);
    if (key.empty()) {
        key = to_lower(get_parser_string_from_properties(properties));
        if (key.empty()) {
            key = INVERTED_INDEX_PARSER_NONE;
        }
    }
    return build_selection_key(
            std::move(key), get_parser_mode_string_from_properties(properties),
            get_parser_lowercase_from_properties(properties) != INVERTED_INDEX_PARSER_FALSE,
            get_parser_char_filter_map_from_properties(properties));
}

// ============================================================================
// AnalyzerConfigParser implementation
// ============================================================================

bool AnalyzerConfigParser::is_builtin_analyzer(const std::string& analyzer_name) {
    return segment_v2::inverted_index::InvertedIndexAnalyzer::is_builtin_analyzer(analyzer_name);
}

AnalyzerConfig AnalyzerConfigParser::parse(const std::string& analyzer_name,
                                           const std::string& parser_type_str,
                                           const std::string& parser_mode, bool lowercase,
                                           const CharFilterMap& char_filter_map) {
    AnalyzerConfig config;

    if (!analyzer_name.empty()) {
        config.analyzer_key =
                build_selection_key(analyzer_name, parser_mode, lowercase, char_filter_map);
        if (is_builtin_analyzer(analyzer_name)) {
            config.parser_type = get_inverted_index_parser_type_from_string(analyzer_name);
        } else {
            config.provider_name = analyzer_name;
            config.parser_type = InvertedIndexParserType::PARSER_NONE;
        }
        return config;
    }

    config.parser_type = get_inverted_index_parser_type_from_string(parser_type_str);

    return config;
}

} // namespace doris
