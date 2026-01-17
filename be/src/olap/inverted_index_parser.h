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
    PARSER_ICU = 6,
    PARSER_BASIC = 7,
    PARSER_IK = 8
};

using CharFilterMap = std::map<std::string, std::string>;

// Configuration for creating analyzer (SRP: only used during analyzer creation)
// This is typically a stack-allocated temporary object, discarded after use
struct InvertedIndexAnalyzerConfig {
    std::string analyzer_name;
    InvertedIndexParserType parser_type = InvertedIndexParserType::PARSER_UNKNOWN;
    std::string parser_mode;
    std::string lower_case;
    std::string stop_words;
    CharFilterMap char_filter_map;
};

const std::string INVERTED_INDEX_PARSER_TRUE = "true";
const std::string INVERTED_INDEX_PARSER_FALSE = "false";

const std::string INVERTED_INDEX_PARSER_MODE_KEY = "parser_mode";
const std::string INVERTED_INDEX_PARSER_FINE_GRANULARITY = "fine_grained";
const std::string INVERTED_INDEX_PARSER_COARSE_GRANULARITY = "coarse_grained";
const std::string INVERTED_INDEX_PARSER_MAX_WORD = "ik_max_word";
const std::string INVERTED_INDEX_PARSER_SMART = "ik_smart";

const std::string INVERTED_INDEX_PARSER_KEY = "parser";
const std::string INVERTED_INDEX_PARSER_KEY_ALIAS = "built_in_analyzer";
const std::string INVERTED_INDEX_PARSER_UNKNOWN = "unknown";
const std::string INVERTED_INDEX_PARSER_NONE = "none";
const std::string INVERTED_INDEX_PARSER_STANDARD = "standard";
const std::string INVERTED_INDEX_PARSER_UNICODE = "unicode";
const std::string INVERTED_INDEX_PARSER_ENGLISH = "english";
const std::string INVERTED_INDEX_PARSER_CHINESE = "chinese";
const std::string INVERTED_INDEX_PARSER_ICU = "icu";
const std::string INVERTED_INDEX_PARSER_BASIC = "basic";
const std::string INVERTED_INDEX_PARSER_IK = "ik";

const std::string INVERTED_INDEX_PARSER_PHRASE_SUPPORT_KEY = "support_phrase";
const std::string INVERTED_INDEX_PARSER_PHRASE_SUPPORT_YES = "true";
const std::string INVERTED_INDEX_PARSER_PHRASE_SUPPORT_NO = "false";

const std::string INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE = "char_filter_type";
const std::string INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN = "char_filter_pattern";
const std::string INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT = "char_filter_replacement";
const std::string INVERTED_INDEX_CHAR_FILTER_CHAR_REPLACE = "char_replace";

const std::string INVERTED_INDEX_PARSER_IGNORE_ABOVE_KEY = "ignore_above";
const std::string INVERTED_INDEX_PARSER_IGNORE_ABOVE_VALUE = "256";

const std::string INVERTED_INDEX_PARSER_LOWERCASE_KEY = "lower_case";

const std::string INVERTED_INDEX_PARSER_STOPWORDS_KEY = "stopwords";

const std::string INVERTED_INDEX_PARSER_DICT_COMPRESSION_KEY = "dict_compression";

const std::string INVERTED_INDEX_ANALYZER_NAME_KEY = "analyzer";
const std::string INVERTED_INDEX_NORMALIZER_NAME_KEY = "normalizer";
const std::string INVERTED_INDEX_PARSER_FIELD_PATTERN_KEY = "field_pattern";
const std::string INVERTED_INDEX_DEFAULT_ANALYZER_KEY = "__default__";

// Normalize an analyzer name to a standardized key format (lowercase, trimmed).
// Used for consistent lookup in multi-analyzer index scenarios.
std::string normalize_analyzer_key(const std::string& analyzer);

// Runtime context for analyzer
// Contains only the fields needed at runtime
struct InvertedIndexAnalyzerCtx {
    // Used by execute_column path to determine if tokenization should be skipped
    std::string analyzer_name;
    InvertedIndexParserType parser_type = InvertedIndexParserType::PARSER_UNKNOWN;

    // Used for creating reader and tokenization
    CharFilterMap char_filter_map;
    std::shared_ptr<lucene::analysis::Analyzer> analyzer;

    // Helper method: returns true if tokenization should be performed
    // For PARSER_NONE (builtin "none" analyzer), no tokenization is performed.
    // For custom analyzers (parser_type=PARSER_NONE but analyzer_name is a custom name),
    // tokenization is performed using the custom analyzer.
    // For "__default__" analyzer_name, returns true to let the caller use index properties
    // to decide the actual analyzer (fallback to index-configured analyzer).
    bool should_tokenize() const {
        if (parser_type != InvertedIndexParserType::PARSER_NONE) {
            return true;
        }
        if (analyzer_name.empty()) {
            return false;
        }
        auto normalized = normalize_analyzer_key(analyzer_name);
        // Only "none" should skip tokenization.
        // "__default__" means use index-configured analyzer, so we should return true
        // to let the caller fallback to index properties for tokenization.
        return normalized != INVERTED_INDEX_PARSER_NONE;
    }
};
using InvertedIndexAnalyzerCtxSPtr = std::shared_ptr<InvertedIndexAnalyzerCtx>;

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

std::string get_parser_dict_compression_from_properties(
        const std::map<std::string, std::string>& properties);

std::string get_analyzer_name_from_properties(const std::map<std::string, std::string>& properties);

// Build a normalized analyzer key from index properties.
// Checks custom_analyzer first, then falls back to parser type.
std::string build_analyzer_key_from_properties(
        const std::map<std::string, std::string>& properties);

// Result structure for analyzer config parsing
struct AnalyzerConfig {
    std::string custom_analyzer;
    InvertedIndexParserType parser_type = InvertedIndexParserType::PARSER_NONE;
    std::string analyzer_key = INVERTED_INDEX_DEFAULT_ANALYZER_KEY;

    // Check if this is a custom analyzer (not builtin)
    bool is_custom() const { return !custom_analyzer.empty(); }
};

// Parser for analyzer configuration from Thrift TMatchPredicate.
// Extracts analyzer_name and parser_type_str, determines if builtin or custom,
// and produces a normalized AnalyzerConfig.
class AnalyzerConfigParser {
public:
    // Parse from raw analyzer name and parser type string (extracted from Thrift).
    // @param analyzer_name: User-specified analyzer name (may be custom or builtin).
    // @param parser_type_str: Parser type string like "chinese", "standard", etc.
    [[nodiscard]] static AnalyzerConfig parse(const std::string& analyzer_name,
                                              const std::string& parser_type_str);

    // Check if a normalized analyzer name looks like a builtin parser type
    [[nodiscard]] static bool is_builtin_analyzer(const std::string& normalized_name);

private:
    static std::string normalize_to_lower(const std::string& value);

    // Compute normalized analyzer_key from raw value, defaulting to __default__ if empty.
    static std::string compute_analyzer_key(const std::string& value);
};

} // namespace doris
