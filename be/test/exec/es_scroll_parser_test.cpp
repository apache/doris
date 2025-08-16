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

} // namespace doris
