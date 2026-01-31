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

#include <gtest/gtest.h>

#include <map>
#include <string>

namespace doris {

class InvertedIndexParserTest : public testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}
};

// Test inverted_index_parser_type_to_string function
TEST_F(InvertedIndexParserTest, TestParserTypeToString) {
    EXPECT_EQ(inverted_index_parser_type_to_string(InvertedIndexParserType::PARSER_NONE),
              INVERTED_INDEX_PARSER_NONE);
    EXPECT_EQ(inverted_index_parser_type_to_string(InvertedIndexParserType::PARSER_STANDARD),
              INVERTED_INDEX_PARSER_STANDARD);
    EXPECT_EQ(inverted_index_parser_type_to_string(InvertedIndexParserType::PARSER_UNICODE),
              INVERTED_INDEX_PARSER_UNICODE);
    EXPECT_EQ(inverted_index_parser_type_to_string(InvertedIndexParserType::PARSER_ENGLISH),
              INVERTED_INDEX_PARSER_ENGLISH);
    EXPECT_EQ(inverted_index_parser_type_to_string(InvertedIndexParserType::PARSER_CHINESE),
              INVERTED_INDEX_PARSER_CHINESE);
    EXPECT_EQ(inverted_index_parser_type_to_string(InvertedIndexParserType::PARSER_ICU),
              INVERTED_INDEX_PARSER_ICU);
    EXPECT_EQ(inverted_index_parser_type_to_string(InvertedIndexParserType::PARSER_BASIC),
              INVERTED_INDEX_PARSER_BASIC);
    EXPECT_EQ(inverted_index_parser_type_to_string(InvertedIndexParserType::PARSER_IK),
              INVERTED_INDEX_PARSER_IK);
    EXPECT_EQ(inverted_index_parser_type_to_string(InvertedIndexParserType::PARSER_UNKNOWN),
              INVERTED_INDEX_PARSER_UNKNOWN);
}

// Test get_inverted_index_parser_type_from_string function
TEST_F(InvertedIndexParserTest, TestGetParserTypeFromString) {
    // Test all valid parser types (case insensitive)
    EXPECT_EQ(get_inverted_index_parser_type_from_string("none"),
              InvertedIndexParserType::PARSER_NONE);
    EXPECT_EQ(get_inverted_index_parser_type_from_string("NONE"),
              InvertedIndexParserType::PARSER_NONE);
    EXPECT_EQ(get_inverted_index_parser_type_from_string("standard"),
              InvertedIndexParserType::PARSER_STANDARD);
    EXPECT_EQ(get_inverted_index_parser_type_from_string("Standard"),
              InvertedIndexParserType::PARSER_STANDARD);
    EXPECT_EQ(get_inverted_index_parser_type_from_string("unicode"),
              InvertedIndexParserType::PARSER_UNICODE);
    EXPECT_EQ(get_inverted_index_parser_type_from_string("english"),
              InvertedIndexParserType::PARSER_ENGLISH);
    EXPECT_EQ(get_inverted_index_parser_type_from_string("chinese"),
              InvertedIndexParserType::PARSER_CHINESE);
    EXPECT_EQ(get_inverted_index_parser_type_from_string("icu"),
              InvertedIndexParserType::PARSER_ICU);
    EXPECT_EQ(get_inverted_index_parser_type_from_string("basic"),
              InvertedIndexParserType::PARSER_BASIC);
    EXPECT_EQ(get_inverted_index_parser_type_from_string("ik"), InvertedIndexParserType::PARSER_IK);

    // Test unknown parser type
    EXPECT_EQ(get_inverted_index_parser_type_from_string("invalid"),
              InvertedIndexParserType::PARSER_UNKNOWN);
    EXPECT_EQ(get_inverted_index_parser_type_from_string(""),
              InvertedIndexParserType::PARSER_UNKNOWN);
}

// Test get_parser_string_from_properties function
TEST_F(InvertedIndexParserTest, TestGetParserStringFromProperties) {
    std::map<std::string, std::string> properties;

    // Test with empty properties
    EXPECT_EQ(get_parser_string_from_properties(properties), INVERTED_INDEX_PARSER_NONE);

    // Test with parser key present
    properties[INVERTED_INDEX_PARSER_KEY] = INVERTED_INDEX_PARSER_ENGLISH;
    EXPECT_EQ(get_parser_string_from_properties(properties), INVERTED_INDEX_PARSER_ENGLISH);

    // Test with different parser value
    properties[INVERTED_INDEX_PARSER_KEY] = INVERTED_INDEX_PARSER_CHINESE;
    EXPECT_EQ(get_parser_string_from_properties(properties), INVERTED_INDEX_PARSER_CHINESE);
}

// Test get_parser_mode_string_from_properties function
TEST_F(InvertedIndexParserTest, TestGetParserModeStringFromProperties) {
    std::map<std::string, std::string> properties;

    // Test with empty properties
    EXPECT_EQ(get_parser_mode_string_from_properties(properties),
              INVERTED_INDEX_PARSER_COARSE_GRANULARITY);

    // Test with parser_mode key present
    properties[INVERTED_INDEX_PARSER_MODE_KEY] = INVERTED_INDEX_PARSER_FINE_GRANULARITY;
    EXPECT_EQ(get_parser_mode_string_from_properties(properties),
              INVERTED_INDEX_PARSER_FINE_GRANULARITY);

    // Test with IK parser (should return smart mode when no mode specified)
    properties.clear();
    properties[INVERTED_INDEX_PARSER_KEY] = INVERTED_INDEX_PARSER_IK;
    EXPECT_EQ(get_parser_mode_string_from_properties(properties), INVERTED_INDEX_PARSER_SMART);

    // Test with non-IK parser (should return coarse granularity)
    properties[INVERTED_INDEX_PARSER_KEY] = INVERTED_INDEX_PARSER_ENGLISH;
    EXPECT_EQ(get_parser_mode_string_from_properties(properties),
              INVERTED_INDEX_PARSER_COARSE_GRANULARITY);
}

// Test get_parser_phrase_support_string_from_properties function
TEST_F(InvertedIndexParserTest, TestGetParserPhraseSupportStringFromProperties) {
    std::map<std::string, std::string> properties;

    // Test with empty properties
    EXPECT_EQ(get_parser_phrase_support_string_from_properties(properties),
              INVERTED_INDEX_PARSER_PHRASE_SUPPORT_NO);

    // Test with phrase support key present
    properties[INVERTED_INDEX_PARSER_PHRASE_SUPPORT_KEY] = INVERTED_INDEX_PARSER_PHRASE_SUPPORT_YES;
    EXPECT_EQ(get_parser_phrase_support_string_from_properties(properties),
              INVERTED_INDEX_PARSER_PHRASE_SUPPORT_YES);

    properties[INVERTED_INDEX_PARSER_PHRASE_SUPPORT_KEY] = INVERTED_INDEX_PARSER_PHRASE_SUPPORT_NO;
    EXPECT_EQ(get_parser_phrase_support_string_from_properties(properties),
              INVERTED_INDEX_PARSER_PHRASE_SUPPORT_NO);
}

// Test get_parser_char_filter_map_from_properties function
TEST_F(InvertedIndexParserTest, TestGetParserCharFilterMapFromProperties) {
    std::map<std::string, std::string> properties;

    // Test with empty properties
    CharFilterMap result = get_parser_char_filter_map_from_properties(properties);
    EXPECT_TRUE(result.empty());

    // Test with missing char_filter_type
    properties["some_key"] = "some_value";
    result = get_parser_char_filter_map_from_properties(properties);
    EXPECT_TRUE(result.empty());

    // Test with valid char_replace filter but missing pattern
    properties.clear();
    properties[INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE] = "char_replace";
    result = get_parser_char_filter_map_from_properties(properties);
    EXPECT_TRUE(result.empty());

    // Test with valid char_replace filter and pattern
    properties[INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN] = "._";
    result = get_parser_char_filter_map_from_properties(properties);
    EXPECT_EQ(result.size(), 3);
    EXPECT_EQ(result[INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE], "char_replace");
    EXPECT_EQ(result[INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN], "._");
    EXPECT_EQ(result[INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT], " "); // default replacement

    // Test with custom replacement
    properties[INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT] = "-";
    result = get_parser_char_filter_map_from_properties(properties);
    EXPECT_EQ(result.size(), 3);
    EXPECT_EQ(result[INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT], "-");

    // Test with invalid filter type
    properties.clear();
    properties[INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE] = "invalid_type";
    result = get_parser_char_filter_map_from_properties(properties);
    EXPECT_TRUE(result.empty());
}

// Test get_parser_ignore_above_value_from_properties function
TEST_F(InvertedIndexParserTest, TestGetParserIgnoreAboveValueFromProperties) {
    std::map<std::string, std::string> properties;

    // Test with empty properties
    EXPECT_EQ(get_parser_ignore_above_value_from_properties(properties),
              INVERTED_INDEX_PARSER_IGNORE_ABOVE_VALUE);

    // Test with ignore_above key present
    properties[INVERTED_INDEX_PARSER_IGNORE_ABOVE_KEY] = "512";
    EXPECT_EQ(get_parser_ignore_above_value_from_properties(properties), "512");

    properties[INVERTED_INDEX_PARSER_IGNORE_ABOVE_KEY] = "1024";
    EXPECT_EQ(get_parser_ignore_above_value_from_properties(properties), "1024");
}

// Test get_parser_lowercase_from_properties function (template function)
TEST_F(InvertedIndexParserTest, TestGetParserLowercaseFromProperties) {
    std::map<std::string, std::string> properties;

    // Test with empty properties (default template parameter false)
    EXPECT_EQ(get_parser_lowercase_from_properties(properties), "");

    // Test with empty properties (template parameter true)
    EXPECT_EQ(get_parser_lowercase_from_properties<true>(properties), INVERTED_INDEX_PARSER_TRUE);

    // Test with lower_case key present
    properties[INVERTED_INDEX_PARSER_LOWERCASE_KEY] = INVERTED_INDEX_PARSER_TRUE;
    EXPECT_EQ(get_parser_lowercase_from_properties(properties), INVERTED_INDEX_PARSER_TRUE);
    EXPECT_EQ(get_parser_lowercase_from_properties<true>(properties), INVERTED_INDEX_PARSER_TRUE);

    properties[INVERTED_INDEX_PARSER_LOWERCASE_KEY] = INVERTED_INDEX_PARSER_FALSE;
    EXPECT_EQ(get_parser_lowercase_from_properties(properties), INVERTED_INDEX_PARSER_FALSE);
    EXPECT_EQ(get_parser_lowercase_from_properties<true>(properties), INVERTED_INDEX_PARSER_FALSE);
}

// Test get_parser_stopwords_from_properties function
TEST_F(InvertedIndexParserTest, TestGetParserStopwordsFromProperties) {
    std::map<std::string, std::string> properties;

    // Test with empty properties
    EXPECT_EQ(get_parser_stopwords_from_properties(properties), "");

    // Test with stopwords key present
    properties[INVERTED_INDEX_PARSER_STOPWORDS_KEY] = "a,an,the";
    EXPECT_EQ(get_parser_stopwords_from_properties(properties), "a,an,the");

    properties[INVERTED_INDEX_PARSER_STOPWORDS_KEY] = "";
    EXPECT_EQ(get_parser_stopwords_from_properties(properties), "");
}

// Test get_parser_dict_compression_from_properties function
TEST_F(InvertedIndexParserTest, TestGetParserDictCompressionFromProperties) {
    std::map<std::string, std::string> properties;

    // Test with empty properties
    EXPECT_EQ(get_parser_dict_compression_from_properties(properties), "");

    // Test with dict_compression key present
    properties[INVERTED_INDEX_PARSER_DICT_COMPRESSION_KEY] = "true";
    EXPECT_EQ(get_parser_dict_compression_from_properties(properties), "true");

    properties[INVERTED_INDEX_PARSER_DICT_COMPRESSION_KEY] = "false";
    EXPECT_EQ(get_parser_dict_compression_from_properties(properties), "false");
}

TEST_F(InvertedIndexParserTest, TestGetAnalyzerNameFromProperties) {
    std::map<std::string, std::string> properties;

    EXPECT_EQ(get_analyzer_name_from_properties(properties), "");

    properties[INVERTED_INDEX_ANALYZER_NAME_KEY] = "my_analyzer";
    EXPECT_EQ(get_analyzer_name_from_properties(properties), "my_analyzer");

    properties[INVERTED_INDEX_ANALYZER_NAME_KEY] = "";
    properties[INVERTED_INDEX_NORMALIZER_NAME_KEY] = "my_normalizer";
    EXPECT_EQ(get_analyzer_name_from_properties(properties), "my_normalizer");

    properties[INVERTED_INDEX_ANALYZER_NAME_KEY] = "another_analyzer";
    properties[INVERTED_INDEX_NORMALIZER_NAME_KEY] = "another_normalizer";
    EXPECT_EQ(get_analyzer_name_from_properties(properties), "another_analyzer");
}

TEST_F(InvertedIndexParserTest, TestInvertedIndexAnalyzerCtxShouldTokenize) {
    InvertedIndexAnalyzerCtx ctx;

    // New design: should_tokenize() only depends on parser_type
    // PARSER_NONE means no tokenization (keyword index)
    ctx.parser_type = InvertedIndexParserType::PARSER_NONE;
    ctx.analyzer_name.clear();
    EXPECT_FALSE(ctx.should_tokenize());

    // Any parser other than NONE means tokenization
    ctx.parser_type = InvertedIndexParserType::PARSER_ENGLISH;
    EXPECT_TRUE(ctx.should_tokenize());

    ctx.parser_type = InvertedIndexParserType::PARSER_CHINESE;
    EXPECT_TRUE(ctx.should_tokenize());

    ctx.parser_type = InvertedIndexParserType::PARSER_STANDARD;
    EXPECT_TRUE(ctx.should_tokenize());

    // Even with custom_analyzer name, PARSER_NONE means no tokenization
    ctx.parser_type = InvertedIndexParserType::PARSER_NONE;
    ctx.analyzer_name = "custom_analyzer";
    EXPECT_FALSE(ctx.should_tokenize());
}

// Test constants
TEST_F(InvertedIndexParserTest, TestConstants) {
    // Test parser constants
    EXPECT_EQ(INVERTED_INDEX_PARSER_UNKNOWN, "unknown");
    EXPECT_EQ(INVERTED_INDEX_PARSER_NONE, "none");
    EXPECT_EQ(INVERTED_INDEX_PARSER_STANDARD, "standard");
    EXPECT_EQ(INVERTED_INDEX_PARSER_UNICODE, "unicode");
    EXPECT_EQ(INVERTED_INDEX_PARSER_ENGLISH, "english");
    EXPECT_EQ(INVERTED_INDEX_PARSER_CHINESE, "chinese");
    EXPECT_EQ(INVERTED_INDEX_PARSER_ICU, "icu");
    EXPECT_EQ(INVERTED_INDEX_PARSER_BASIC, "basic");
    EXPECT_EQ(INVERTED_INDEX_PARSER_IK, "ik");

    // Test mode constants
    EXPECT_EQ(INVERTED_INDEX_PARSER_FINE_GRANULARITY, "fine_grained");
    EXPECT_EQ(INVERTED_INDEX_PARSER_COARSE_GRANULARITY, "coarse_grained");
    EXPECT_EQ(INVERTED_INDEX_PARSER_MAX_WORD, "ik_max_word");
    EXPECT_EQ(INVERTED_INDEX_PARSER_SMART, "ik_smart");

    // Test boolean constants
    EXPECT_EQ(INVERTED_INDEX_PARSER_TRUE, "true");
    EXPECT_EQ(INVERTED_INDEX_PARSER_FALSE, "false");

    // Test phrase support constants
    EXPECT_EQ(INVERTED_INDEX_PARSER_PHRASE_SUPPORT_YES, "true");
    EXPECT_EQ(INVERTED_INDEX_PARSER_PHRASE_SUPPORT_NO, "false");

    // Test key constants
    EXPECT_EQ(INVERTED_INDEX_PARSER_KEY, "parser");
    EXPECT_EQ(INVERTED_INDEX_PARSER_MODE_KEY, "parser_mode");
    EXPECT_EQ(INVERTED_INDEX_PARSER_PHRASE_SUPPORT_KEY, "support_phrase");
    EXPECT_EQ(INVERTED_INDEX_PARSER_LOWERCASE_KEY, "lower_case");
    EXPECT_EQ(INVERTED_INDEX_PARSER_STOPWORDS_KEY, "stopwords");
    EXPECT_EQ(INVERTED_INDEX_PARSER_DICT_COMPRESSION_KEY, "dict_compression");
    EXPECT_EQ(INVERTED_INDEX_PARSER_IGNORE_ABOVE_KEY, "ignore_above");
    EXPECT_EQ(INVERTED_INDEX_PARSER_IGNORE_ABOVE_VALUE, "256");

    // Test char filter constants
    EXPECT_EQ(INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE, "char_filter_type");
    EXPECT_EQ(INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN, "char_filter_pattern");
    EXPECT_EQ(INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT, "char_filter_replacement");

    // Note: INVERTED_INDEX_DEFAULT_ANALYZER_KEY has been removed in the new design.
    // Empty string now means "user did not specify" (BE auto-selects index).
}

// ============================================================================
// normalize_analyzer_key Tests
// New design: empty string stays empty, non-empty gets lowercased
// ============================================================================

TEST_F(InvertedIndexParserTest, NormalizeAnalyzerKey_EmptyInput) {
    // New design: empty string stays empty (means "user did not specify")
    EXPECT_EQ(normalize_analyzer_key(""), "");
}

TEST_F(InvertedIndexParserTest, NormalizeAnalyzerKey_UppercaseToLowercase) {
    EXPECT_EQ(normalize_analyzer_key("CHINESE"), "chinese");
    EXPECT_EQ(normalize_analyzer_key("STANDARD"), "standard");
    EXPECT_EQ(normalize_analyzer_key("ENGLISH"), "english");
}

TEST_F(InvertedIndexParserTest, NormalizeAnalyzerKey_MixedCase) {
    EXPECT_EQ(normalize_analyzer_key("ChInEsE"), "chinese");
    EXPECT_EQ(normalize_analyzer_key("StAnDaRd"), "standard");
    EXPECT_EQ(normalize_analyzer_key("My_Custom_Analyzer"), "my_custom_analyzer");
}

TEST_F(InvertedIndexParserTest, NormalizeAnalyzerKey_NoneParser) {
    // "none" is a distinct key - means keyword index (no tokenization)
    EXPECT_EQ(normalize_analyzer_key("none"), "none");
    EXPECT_EQ(normalize_analyzer_key("NONE"), "none");
}

TEST_F(InvertedIndexParserTest, NormalizeAnalyzerKey_AlreadyLowercase) {
    EXPECT_EQ(normalize_analyzer_key("chinese"), "chinese");
    EXPECT_EQ(normalize_analyzer_key("my_custom"), "my_custom");
}

// ============================================================================
// build_analyzer_key_from_properties Tests
// New design: returns actual parser/analyzer name, empty means no properties
// ============================================================================

TEST_F(InvertedIndexParserTest, BuildAnalyzerKeyFromProperties_EmptyProperties) {
    std::map<std::string, std::string> properties;
    // Empty properties = empty key (no explicit configuration)
    EXPECT_EQ(build_analyzer_key_from_properties(properties), "");
}

TEST_F(InvertedIndexParserTest, BuildAnalyzerKeyFromProperties_CustomAnalyzer) {
    std::map<std::string, std::string> properties;
    properties[INVERTED_INDEX_ANALYZER_NAME_KEY] = "my_custom_analyzer";
    EXPECT_EQ(build_analyzer_key_from_properties(properties), "my_custom_analyzer");
}

TEST_F(InvertedIndexParserTest, BuildAnalyzerKeyFromProperties_CustomAnalyzerUppercase) {
    std::map<std::string, std::string> properties;
    properties[INVERTED_INDEX_ANALYZER_NAME_KEY] = "MY_CUSTOM";
    EXPECT_EQ(build_analyzer_key_from_properties(properties), "my_custom");
}

TEST_F(InvertedIndexParserTest, BuildAnalyzerKeyFromProperties_ParserKey) {
    std::map<std::string, std::string> properties;
    properties[INVERTED_INDEX_PARSER_KEY] = "chinese";
    EXPECT_EQ(build_analyzer_key_from_properties(properties), "chinese");
}

TEST_F(InvertedIndexParserTest, BuildAnalyzerKeyFromProperties_ParserKeyAlias) {
    std::map<std::string, std::string> properties;
    properties[INVERTED_INDEX_PARSER_KEY_ALIAS] = "standard";
    EXPECT_EQ(build_analyzer_key_from_properties(properties), "standard");
}

TEST_F(InvertedIndexParserTest, BuildAnalyzerKeyFromProperties_ParserNone) {
    std::map<std::string, std::string> properties;
    properties[INVERTED_INDEX_PARSER_KEY] = "none";
    // "none" is a distinct analyzer key - it means no tokenization (keyword analyzer)
    // This is different from __default__ which means use default behavior
    EXPECT_EQ(build_analyzer_key_from_properties(properties), "none");
}

TEST_F(InvertedIndexParserTest, BuildAnalyzerKeyFromProperties_CustomOverridesParser) {
    std::map<std::string, std::string> properties;
    properties[INVERTED_INDEX_ANALYZER_NAME_KEY] = "my_custom";
    properties[INVERTED_INDEX_PARSER_KEY] = "chinese";
    // Custom analyzer takes precedence
    EXPECT_EQ(build_analyzer_key_from_properties(properties), "my_custom");
}

// ============================================================================
// AnalyzerConfigParser Tests
// ============================================================================

TEST_F(InvertedIndexParserTest, AnalyzerConfigParser_EmptyInput) {
    auto config = AnalyzerConfigParser::parse("", "");
    // New design: empty input gives empty analyzer_key (means "user did not specify")
    EXPECT_EQ(config.analyzer_key, "");
    EXPECT_EQ(config.parser_type, InvertedIndexParserType::PARSER_NONE);
    EXPECT_TRUE(config.custom_analyzer.empty());
    EXPECT_FALSE(config.is_custom());
}

TEST_F(InvertedIndexParserTest, AnalyzerConfigParser_OnlyAnalyzerCustom) {
    auto config = AnalyzerConfigParser::parse("my_custom_analyzer", "");
    EXPECT_EQ(config.custom_analyzer, "my_custom_analyzer");
    EXPECT_EQ(config.analyzer_key, "my_custom_analyzer");
    EXPECT_EQ(config.parser_type, InvertedIndexParserType::PARSER_NONE);
    EXPECT_TRUE(config.is_custom());
}

TEST_F(InvertedIndexParserTest, AnalyzerConfigParser_OnlyAnalyzerBuiltin) {
    auto config = AnalyzerConfigParser::parse("chinese", "");
    EXPECT_TRUE(config.custom_analyzer.empty());
    EXPECT_EQ(config.analyzer_key, "chinese");
    EXPECT_EQ(config.parser_type, InvertedIndexParserType::PARSER_CHINESE);
    EXPECT_FALSE(config.is_custom());
}

TEST_F(InvertedIndexParserTest, AnalyzerConfigParser_OnlyParserTypeStr) {
    auto config = AnalyzerConfigParser::parse("", "standard");
    EXPECT_TRUE(config.custom_analyzer.empty());
    EXPECT_EQ(config.analyzer_key, "standard");
    EXPECT_EQ(config.parser_type, InvertedIndexParserType::PARSER_STANDARD);
    EXPECT_FALSE(config.is_custom());
}

TEST_F(InvertedIndexParserTest, AnalyzerConfigParser_BothAnalyzerAndParser) {
    // parser_type_str takes precedence for determining parser_type
    auto config = AnalyzerConfigParser::parse("ik", "chinese");
    EXPECT_TRUE(config.custom_analyzer.empty());
    EXPECT_EQ(config.parser_type, InvertedIndexParserType::PARSER_CHINESE);
    EXPECT_EQ(config.analyzer_key, "ik"); // analyzer_name used for key
}

TEST_F(InvertedIndexParserTest, AnalyzerConfigParser_CaseInsensitive) {
    auto config = AnalyzerConfigParser::parse("CHINESE", "");
    EXPECT_TRUE(config.custom_analyzer.empty());
    EXPECT_EQ(config.analyzer_key, "chinese");
    EXPECT_EQ(config.parser_type, InvertedIndexParserType::PARSER_CHINESE);
}

TEST_F(InvertedIndexParserTest, AnalyzerConfigParser_UnknownAnalyzerAsCustom) {
    auto config = AnalyzerConfigParser::parse("unknown_xyz", "");
    EXPECT_EQ(config.custom_analyzer, "unknown_xyz");
    EXPECT_EQ(config.analyzer_key, "unknown_xyz");
    EXPECT_EQ(config.parser_type, InvertedIndexParserType::PARSER_NONE);
    EXPECT_TRUE(config.is_custom());
}

TEST_F(InvertedIndexParserTest, AnalyzerConfigParser_AllBuiltinTypes) {
    // Test all builtin parser types
    std::vector<std::pair<std::string, InvertedIndexParserType>> builtin_types = {
            {"none", InvertedIndexParserType::PARSER_NONE},
            {"standard", InvertedIndexParserType::PARSER_STANDARD},
            {"unicode", InvertedIndexParserType::PARSER_UNICODE},
            {"english", InvertedIndexParserType::PARSER_ENGLISH},
            {"chinese", InvertedIndexParserType::PARSER_CHINESE},
            {"icu", InvertedIndexParserType::PARSER_ICU},
            {"basic", InvertedIndexParserType::PARSER_BASIC},
            {"ik", InvertedIndexParserType::PARSER_IK},
    };

    for (const auto& [name, expected_type] : builtin_types) {
        auto config = AnalyzerConfigParser::parse(name, "");
        EXPECT_EQ(config.parser_type, expected_type) << "Failed for: " << name;
        EXPECT_TRUE(config.custom_analyzer.empty()) << "Failed for: " << name;
        EXPECT_FALSE(config.is_custom()) << "Failed for: " << name;
    }
}

TEST_F(InvertedIndexParserTest, AnalyzerConfigParser_IsBuiltinAnalyzer) {
    EXPECT_TRUE(AnalyzerConfigParser::is_builtin_analyzer("chinese"));
    EXPECT_TRUE(AnalyzerConfigParser::is_builtin_analyzer("standard"));
    EXPECT_TRUE(AnalyzerConfigParser::is_builtin_analyzer("english"));
    EXPECT_TRUE(AnalyzerConfigParser::is_builtin_analyzer("unicode"));
    EXPECT_TRUE(AnalyzerConfigParser::is_builtin_analyzer("icu"));
    EXPECT_TRUE(AnalyzerConfigParser::is_builtin_analyzer("basic"));
    EXPECT_TRUE(AnalyzerConfigParser::is_builtin_analyzer("ik"));
    EXPECT_TRUE(AnalyzerConfigParser::is_builtin_analyzer("none"));

    EXPECT_FALSE(AnalyzerConfigParser::is_builtin_analyzer("my_custom"));
    EXPECT_FALSE(AnalyzerConfigParser::is_builtin_analyzer("unknown"));
    EXPECT_FALSE(AnalyzerConfigParser::is_builtin_analyzer(""));
}

} // namespace doris
