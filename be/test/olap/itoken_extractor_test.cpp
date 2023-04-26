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

#include "olap/itoken_extractor.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <algorithm>
#include <string>
#include <vector>

#include "gtest/gtest_pred_impl.h"
#include "util/utf8_check.h"

namespace doris {

class TestITokenExtractor : public testing::Test {
public:
    void SetUp() {}
    void TearDown() {}
};

void runNextInString(const ITokenExtractor& extractor, std::string statement,
                     std::vector<std::string> expect) {
    ASSERT_TRUE(validate_utf8(statement.c_str(), statement.length()));

    std::vector<std::string> actual;
    actual.reserve(expect.size());
    size_t pos = 0;
    size_t token_start = 0;
    size_t token_length = 0;
    while (extractor.next_in_string(statement.c_str(), statement.size(), &pos, &token_start,
                                    &token_length)) {
        actual.push_back(statement.substr(token_start, token_length));
    }
    ASSERT_EQ(expect, actual);
}

void runNextInStringLike(const ITokenExtractor& extractor, std::string statement,
                         std::vector<std::string> expect) {
    std::vector<std::string> actual;
    actual.reserve(expect.size());
    size_t pos = 0;
    std::string str;
    while (extractor.next_in_string_like(statement.c_str(), statement.length(), &pos, str)) {
        actual.push_back(str);
    }
    ASSERT_EQ(expect, actual);
}

#if __cplusplus > 201703L
std::string from_u8string(const std::u8string& s) {
    return std::string(s.begin(), s.end());
}
#else
std::string from_u8string(const std::string& s) {
    return std::string(s.begin(), s.end());
}
#endif

TEST_F(TestITokenExtractor, ngram_extractor) {
    std::string statement = from_u8string(u8"预计09发布i13手机。");
    std::vector<std::string> expect = {
            from_u8string(u8"预计"), from_u8string(u8"计0"),  from_u8string(u8"09"),
            from_u8string(u8"9发"),  from_u8string(u8"发布"), from_u8string(u8"布i"),
            from_u8string(u8"i1"),   from_u8string(u8"13"),   from_u8string(u8"3手"),
            from_u8string(u8"手机"), from_u8string(u8"机。")};
    NgramTokenExtractor ngram_extractor(2);
    runNextInString(ngram_extractor, statement, expect);
}

TEST_F(TestITokenExtractor, ngram_like_extractor) {
    NgramTokenExtractor ngram_extractor(2);
    runNextInStringLike(ngram_extractor, from_u8string(u8"%手机%"), {from_u8string(u8"手机")});
    runNextInStringLike(ngram_extractor, from_u8string(u8"%机%"), {});
    runNextInStringLike(ngram_extractor, {from_u8string(u8"i_%手机%")}, {from_u8string(u8"手机")});
    runNextInStringLike(ngram_extractor, {from_u8string(u8"\\_手机%")},
                        {from_u8string(u8"_手"), from_u8string(u8"手机")});
}
} // namespace doris
