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

#include <gtest/gtest.h>
#include <rapidjson/document.h>
#include <re2/re2.h>

#include <string>

namespace doris {

class EsScrollParserTest : public testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}
};

// Test timezone pattern matching for ES datetime parsing fix
TEST_F(EsScrollParserTest, TestTimezonePatternMatching) {
    RE2 time_zone_pattern(R"([+-]\d{2}:?\d{2}|Z)");

    std::vector<std::string> valid_timezone_formats = {
            "2025-05-23T20:56:52.052+0900",  "2025-05-23T20:56:52.052-0500",
            "2025-05-23T20:56:52.052+08:00", "2025-05-23T20:56:52.052-04:30",
            "2025-05-23T20:56:52.052Z",      "2022-08-08T12:10:10.151Z",
            "2022-08-08T12:10:10+0900",      "2022-08-08T12:10:10-0500"};

    for (const auto& datetime_str : valid_timezone_formats) {
        re2::StringPiece timezone_value;
        bool matched = time_zone_pattern.Match(datetime_str, 0, datetime_str.size(),
                                               RE2::UNANCHORED, &timezone_value, 1);
        EXPECT_TRUE(matched) << "Failed to match timezone in: " << datetime_str;

        std::string timezone = timezone_value.as_string();
        EXPECT_FALSE(timezone.empty()) << "Empty timezone captured from: " << datetime_str;

        if (timezone == "Z") {
            EXPECT_EQ(timezone, "Z");
        } else {
            EXPECT_TRUE(timezone[0] == '+' || timezone[0] == '-')
                    << "Invalid timezone sign in: " << timezone;
            // Valid timezone lengths: 5 for +0900, 6 for +08:00
            EXPECT_TRUE(timezone.length() == 5 || timezone.length() == 6)
                    << "Invalid timezone length in: " << timezone
                    << " (length: " << timezone.length() << ")";
        }
    }
}

TEST_F(EsScrollParserTest, TestInvalidTimezonePatterns) {
    RE2 time_zone_pattern(R"([+-]\d{2}:?\d{2}|Z)");

    std::vector<std::string> invalid_formats = {
            "2025-05-23T20:56:52.052", "2025-05-23T20:56:52.052+9", "2025-05-23T20:56:52.052+090",
            "2025-05-23T20:56:52.052+9:00"};

    for (const auto& datetime_str : invalid_formats) {
        re2::StringPiece timezone_value;
        bool matched = time_zone_pattern.Match(datetime_str, 0, datetime_str.size(),
                                               RE2::UNANCHORED, &timezone_value, 1);
        if (matched) {
            std::string timezone = timezone_value.as_string();
            EXPECT_TRUE(timezone.empty()) << "Should not capture timezone from: " << datetime_str;
        }
    }
}

TEST_F(EsScrollParserTest, TestBugScenarioTimezoneFormat) {
    RE2 time_zone_pattern(R"([+-]\d{2}:?\d{2}|Z)");

    std::string problematic_format = "2025-05-23T20:56:52.052+0900";

    re2::StringPiece timezone_value;
    bool matched = time_zone_pattern.Match(problematic_format, 0, problematic_format.size(),
                                           RE2::UNANCHORED, &timezone_value, 1);

    EXPECT_TRUE(matched) << "Failed to match the bug scenario format: " << problematic_format;

    std::string timezone = timezone_value.as_string();
    EXPECT_EQ(timezone, "+0900") << "Incorrect timezone captured: " << timezone;
}

TEST_F(EsScrollParserTest, TestEdgeCaseTimezoneFormats) {
    RE2 time_zone_pattern(R"([+-]\d{2}:?\d{2}|Z)");

    std::vector<std::string> edge_cases = {"+00:00", "-00:00", "+23:59", "-23:59",
                                           "+99:99", "Z",      "+0800",  ""};

    // Test each edge case
    std::vector<std::string> test_datetime_strings = {
            "2025-05-23T20:56:52.052+00:00", // +00:00 (UTC with colon)
            "2025-05-23T20:56:52.052-00:00", // -00:00 (UTC with colon)
            "2025-05-23T20:56:52.052+23:59", // +23:59 (max valid timezone)
            "2025-05-23T20:56:52.052-23:59", // -23:59 (max valid timezone)
            "2025-05-23T20:56:52.052+99:99", // +99:99 (invalid but should match pattern)
            "2025-05-23T20:56:52.052Z",      // Z (UTC)
            "2025-05-23T20:56:52.052+0800",  // +0800 (no colon)
            "2025-05-23T20:56:52.052"        // empty timezone (no timezone)
    };

    std::vector<std::string> expected_matches = {"+00:00", "-00:00", "+23:59", "-23:59",
                                                 "+99:99", "Z",      "+0800",  ""};

    std::vector<bool> should_match = {true, true, true, true, true, true, true, false};

    for (size_t i = 0; i < test_datetime_strings.size(); ++i) {
        const std::string& datetime_str = test_datetime_strings[i];
        const std::string& expected_match = expected_matches[i];
        bool should_match_expected = should_match[i];

        re2::StringPiece timezone_value;
        bool matched = time_zone_pattern.Match(datetime_str, 0, datetime_str.size(),
                                               RE2::UNANCHORED, &timezone_value, 1);

        EXPECT_EQ(matched, should_match_expected)
                << "Edge case test failed for: " << datetime_str
                << " (expected match: " << should_match_expected << ")";

        if (matched && should_match_expected) {
            std::string timezone = timezone_value.as_string();
            EXPECT_EQ(timezone, expected_match)
                    << "Incorrect timezone captured from: " << datetime_str
                    << " (expected: " << expected_match << ", got: " << timezone << ")";
        }
    }
}

TEST_F(EsScrollParserTest, TestSpecialTimezoneEdgeCases) {
    RE2 time_zone_pattern(R"([+-]\d{2}:?\d{2}|Z)");

    // Additional edge cases for comprehensive testing
    std::vector<std::pair<std::string, std::pair<std::string, bool>>> special_cases = {
            // {datetime_string, {expected_timezone, should_match}}
            {"2025-05-23T20:56:52+0000", {"+0000", true}},          // +0000 without colon
            {"2025-05-23T20:56:52-0000", {"-0000", true}},          // -0000 without colon
            {"2025-05-23T20:56:52+12:30", {"+12:30", true}},        // +12:30 with colon
            {"2025-05-23T20:56:52-12:30", {"-12:30", true}},        // -12:30 with colon
            {"2025-05-23T20:56:52+1200", {"+1200", true}},          // +1200 without colon
            {"2025-05-23T20:56:52-1200", {"-1200", true}},          // -1200 without colon
            {"2025-05-23T20:56:52.000Z", {"Z", true}},              // Z with milliseconds
            {"2025-05-23T20:56:52.123456+05:30", {"+05:30", true}}, // microseconds with timezone
            {"2025-05-23T20:56:52.123456-05:30", {"-05:30", true}}, // microseconds with timezone
            {"2025-05-23T20:56:52.123456+0530", {"+0530", true}},   // microseconds without colon
            {"2025-05-23T20:56:52.123456-0530", {"-0530", true}},   // microseconds without colon
            {"2025-05-23T20:56:52+14:00", {"+14:00", true}},        // +14:00 (valid max timezone)
            {"2025-05-23T20:56:52-12:00", {"-12:00", true}},        // -12:00 (valid min timezone)
    };

    for (const auto& test_case : special_cases) {
        const std::string& datetime_str = test_case.first;
        const std::string& expected_timezone = test_case.second.first;
        bool should_match = test_case.second.second;

        re2::StringPiece timezone_value;
        bool matched = time_zone_pattern.Match(datetime_str, 0, datetime_str.size(),
                                               RE2::UNANCHORED, &timezone_value, 1);

        EXPECT_EQ(matched, should_match) << "Special case test failed for: " << datetime_str
                                         << " (expected match: " << should_match << ")";

        if (matched && should_match) {
            std::string timezone = timezone_value.as_string();
            EXPECT_EQ(timezone, expected_timezone)
                    << "Incorrect timezone captured from: " << datetime_str
                    << " (expected: " << expected_timezone << ", got: " << timezone << ")";
        }
    }
}

} // namespace doris
