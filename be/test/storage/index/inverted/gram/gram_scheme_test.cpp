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

#include "storage/index/inverted/gram/gram_scheme.h"

#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <utility>

namespace doris::segment_v2::gram {

TEST(GramSchemeTest, DefaultsAndRoundTrip) {
    GramScheme s;
    EXPECT_EQ(s.mode, GramMode::SPARSE);
    EXPECT_EQ(s.min_len, 3U);
    EXPECT_EQ(s.max_len, 4U);
    EXPECT_EQ(s.density_permille, 250U);
    GramScheme back;
    ASSERT_TRUE(GramScheme::from_properties(s.to_properties(), &back).ok());
    EXPECT_TRUE(s == back);
    EXPECT_EQ(s.cache_key(), "gram:v1:sparse:3:4:250:lc0");
}

TEST(GramSchemeTest, ParsesTokenizerProperties) {
    std::map<std::string, std::string> props = {{"mode", "dense"},
                                                {"min_gram", "4"},
                                                {"max_gram", "24"},
                                                {"density", "0.33"},
                                                {"lower_case", "true"}};
    GramScheme s;
    ASSERT_TRUE(GramScheme::from_properties(props, &s).ok());
    EXPECT_EQ(s.mode, GramMode::DENSE);
    EXPECT_EQ(s.min_len, 4U);
    EXPECT_EQ(s.max_len, 24U);
    EXPECT_EQ(s.density_permille, 330U);
    EXPECT_TRUE(s.lower_case);
}

TEST(GramSchemeTest, LowerCaseAcceptsBooleanSynonyms) {
    GramScheme s;
    ASSERT_TRUE(GramScheme::from_properties({{"lower_case", "true"}}, &s).ok());
    EXPECT_TRUE(s.lower_case);
    ASSERT_TRUE(GramScheme::from_properties({{"lower_case", "1"}}, &s).ok());
    EXPECT_TRUE(s.lower_case);
    ASSERT_TRUE(GramScheme::from_properties({{"lower_case", "false"}}, &s).ok());
    EXPECT_FALSE(s.lower_case);
    ASSERT_TRUE(GramScheme::from_properties({{"lower_case", "0"}}, &s).ok());
    EXPECT_FALSE(s.lower_case);
}

TEST(GramSchemeTest, RejectsInvalid) {
    GramScheme s;
    EXPECT_FALSE(GramScheme::from_properties({{"mode", "fuzzy"}}, &s).ok());
    EXPECT_FALSE(GramScheme::from_properties({{"min_gram", "0"}}, &s).ok());
    EXPECT_FALSE(GramScheme::from_properties({{"min_gram", "8"}, {"max_gram", "4"}}, &s).ok());
    EXPECT_FALSE(GramScheme::from_properties({{"density", "0"}}, &s).ok());
    EXPECT_FALSE(GramScheme::from_properties({{"density", "1.5"}}, &s).ok());
    EXPECT_FALSE(GramScheme::from_properties({{"lower_case", "yes"}}, &s).ok());
}

// A gram scheme has no high-frequency-gram pruning threshold: no reader consumes one, so the
// parameter is not part of the scheme identity, is not written back to the properties and does
// not reach the cache key. Anything named after it is just an unrecognised property.
TEST(GramSchemeTest, NoStopGramDfConcept) {
    GramScheme parsed;
    ASSERT_TRUE(GramScheme::from_properties({{"stop_gram_df", "0.25"}}, &parsed).ok());
    EXPECT_TRUE(parsed == GramScheme {});

    const auto props = GramScheme {}.to_properties();
    EXPECT_EQ(props.find("stop_gram_df"), props.end());
    EXPECT_EQ(GramScheme {}.cache_key().find("stop"), std::string::npos);
}

TEST(GramSchemeTest, ParsesPortableDecimalProperties) {
    const std::pair<const char*, uint32_t> cases[] = {{"0.25", 250},  {".25", 250},    {"1.", 1000},
                                                      {"+0.25", 250}, {"2.5e-1", 250}, {"0.001", 1},
                                                      {"1", 1000}};
    for (const auto& [value, expected] : cases) {
        SCOPED_TRACE(std::string("density=") + value);
        GramScheme scheme;
        ASSERT_TRUE(GramScheme::from_properties({{"density", value}}, &scheme).ok());
        EXPECT_EQ(scheme.density_permille, expected);
    }
    for (const auto* value : {"0.25f", "0.25D", "0.25 ", "0.25\t"}) {
        GramScheme scheme;
        EXPECT_FALSE(GramScheme::from_properties({{"density", value}}, &scheme).ok());
    }
}

} // namespace doris::segment_v2::gram
