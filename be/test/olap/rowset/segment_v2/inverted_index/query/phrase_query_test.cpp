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

#include "olap/rowset/segment_v2/inverted_index/query/phrase_query.h"

#include <gtest/gtest.h>

#include "io/fs/local_file_system.h"

namespace doris::segment_v2 {

class PhraseQueryTest : public testing::Test {
public:
    const std::string kTestDir = "./ut_dir/phrase_query_test";

    void SetUp() override {
        auto st = io::global_local_filesystem()->delete_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;
    }
    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
    }

    PhraseQueryTest() = default;
    ~PhraseQueryTest() override = default;
};

TEST_F(PhraseQueryTest, test_parser_info) {
    std::map<std::string, std::string> properties;
    properties.insert({"parser", "english"});
    properties.insert({"support_phrase", "true"});
    properties.insert({"lower_case", "true"});

    auto parser_info = [&properties](std::string& search_str, InvertedIndexQueryInfo& query_info,
                                     bool sequential_opt) {
        PhraseQuery::parser_info(search_str, "name", InvertedIndexQueryType::MATCH_REGEXP_QUERY,
                                 properties, query_info, sequential_opt);
    };

    auto parser = [&parser_info](std::string search_str, std::string res1, size_t res2,
                                 int32_t res3, bool res4, size_t res5) {
        InvertedIndexQueryInfo query_info;
        parser_info(search_str, query_info, true);
        EXPECT_EQ(search_str, res1);
        EXPECT_EQ(query_info.terms.size(), res2);
        EXPECT_EQ(query_info.slop, res3);
        EXPECT_EQ(query_info.ordered, res4);
        EXPECT_EQ(query_info.additional_terms.size(), res5);
        std::cout << "--- 1 ---: " << query_info.to_string() << std::endl;
    };

    // "english/history off.gif ~20+" sequential_opt = true
    parser("", "", 0, 0, false, 0);
    parser("english", "english", 1, 0, false, 0);
    parser("english/history", "english/history", 2, 0, false, 0);
    parser("english/history off", "english/history off", 3, 0, false, 0);
    parser("english/history off.gif", "english/history off.gif", 4, 0, false, 0);
    parser("english/history off.gif ", "english/history off.gif ", 4, 0, false, 0);
    parser("english/history off.gif ~", "english/history off.gif ~", 4, 0, false, 0);
    parser("english/history off.gif ~2", "english/history off.gif", 4, 2, false, 0);
    parser("english/history off.gif ~20", "english/history off.gif", 4, 20, false, 0);
    parser("english/history off.gif ~20+", "english/history off.gif", 4, 20, true, 2);
    parser("english/history off.gif ~20+ ", "english/history off.gif ~20+ ", 5, 0, false, 0);
    parser("english/history off.gif ~20+x", "english/history off.gif ~20+x", 6, 0, false, 0);
}

} // namespace doris::segment_v2