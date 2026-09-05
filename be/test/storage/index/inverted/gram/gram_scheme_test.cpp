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

namespace doris::segment_v2::gram {

TEST(GramSchemeTest, DefaultsAndRoundTrip) {
    GramScheme s;
    EXPECT_EQ(s.mode, GramMode::SPARSE);
    EXPECT_EQ(s.min_len, 3U);
    EXPECT_EQ(s.max_len, 16U);
    EXPECT_EQ(s.density_permille, 250U);
    GramScheme back;
    ASSERT_TRUE(GramScheme::from_properties(s.to_properties(), &back).ok());
    EXPECT_TRUE(s == back);
    EXPECT_EQ(s.cache_key(), "gram:v1:sparse:3:16:250:100:lc0");
}

TEST(GramSchemeTest, ParsesTokenizerProperties) {
    std::map<std::string, std::string> props = {{"mode", "dense"},        {"min_gram", "4"},
                                                {"max_gram", "24"},       {"density", "0.33"},
                                                {"stop_gram_df", "0.25"}, {"lower_case", "true"}};
    GramScheme s;
    ASSERT_TRUE(GramScheme::from_properties(props, &s).ok());
    EXPECT_EQ(s.mode, GramMode::DENSE);
    EXPECT_EQ(s.min_len, 4U);
    EXPECT_EQ(s.max_len, 24U);
    EXPECT_EQ(s.density_permille, 330U);
    EXPECT_EQ(s.stop_df_permille, 250U);
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
    EXPECT_FALSE(GramScheme::from_properties({{"stop_gram_df", "-1"}}, &s).ok());
    EXPECT_FALSE(GramScheme::from_properties({{"lower_case", "yes"}}, &s).ok());
}

} // namespace doris::segment_v2::gram
