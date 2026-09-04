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

#include "storage/index/snii/query/gram_boolean_query.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <map>
#include <string>
#include <string_view>
#include <vector>

#include "storage/index/inverted/gram/gram_query.h"

using namespace doris::snii;
using namespace doris::segment_v2;
using doris::Status;

namespace {

// GramPostingSource 的假实现：用一个 map 模拟 gram -> 有序 docid 列表的词典，
// 不需要构造真实的 SNII 索引文件即可覆盖 gram_boolean_query 的 AND/OR/ALL/NONE
// 求值逻辑。lookups 统计 df() 被调用的次数，用来验证「缺失 gram 只查 df、
// 不读 posting」这条早退路径。
class MapPostingSource final : public query::GramPostingSource {
public:
    std::map<std::string, std::vector<uint32_t>> lists;
    int lookups = 0;

    Status df(std::string_view gram, bool* found, uint64_t* df) override {
        ++lookups;
        auto it = lists.find(std::string(gram));
        *found = it != lists.end();
        *df = *found ? it->second.size() : 0;
        return Status::OK();
    }

    Status postings(std::string_view gram, roaring::Roaring* out) override {
        auto it = lists.find(std::string(gram));
        if (it == lists.end()) {
            return Status::OK();
        }
        out->addMany(it->second.size(), it->second.data());
        return Status::OK();
    }
};

std::vector<uint32_t> ToVec(const roaring::Roaring& r) {
    return {r.begin(), r.end()};
}

} // namespace

using gram::GramQuery;

TEST(GramBooleanQueryTest, AndOrAllNone) {
    MapPostingSource src;
    src.lists["abc"] = {1, 2, 3, 7};
    src.lists["bcd"] = {2, 3, 9};
    src.lists["xyz"] = {7};

    roaring::Roaring out;
    ASSERT_TRUE(query::gram_boolean_query(
                        src, GramQuery::and_(GramQuery::of_gram("abc"), GramQuery::of_gram("bcd")),
                        10, &out)
                        .ok());
    EXPECT_EQ(ToVec(out), (std::vector<uint32_t> {2, 3}));

    out = roaring::Roaring();
    ASSERT_TRUE(query::gram_boolean_query(
                        src, GramQuery::or_(GramQuery::of_gram("bcd"), GramQuery::of_gram("xyz")),
                        10, &out)
                        .ok());
    EXPECT_EQ(ToVec(out), (std::vector<uint32_t> {2, 3, 7, 9}));

    out = roaring::Roaring();
    ASSERT_TRUE(query::gram_boolean_query(src, GramQuery::all(), 4, &out).ok());
    EXPECT_EQ(ToVec(out), (std::vector<uint32_t> {0, 1, 2, 3}));

    out = roaring::Roaring();
    ASSERT_TRUE(query::gram_boolean_query(src, GramQuery::none(), 4, &out).ok());
    EXPECT_TRUE(out.isEmpty());
}

TEST(GramBooleanQueryTest, MissingGramIsNoneAndEarlyExit) {
    MapPostingSource src;
    src.lists["abc"] = {1, 2, 3};

    roaring::Roaring out;
    auto q = GramQuery::and_(GramQuery::of_gram("abc"), GramQuery::of_gram("nope"));
    ASSERT_TRUE(query::gram_boolean_query(src, q, 10, &out).ok());
    EXPECT_TRUE(out.isEmpty());
    // 缺失 gram 只走 df 查找就能确定整个 AND 为空，不应该再读取任何 posting：
    // 两个 gram 各查一次 df，postings() 一次都不会被调用。
    EXPECT_EQ(src.lookups, 2);
}

TEST(GramBooleanQueryTest, NestedAndOfOr) {
    MapPostingSource src;
    src.lists["a"] = {1, 2, 3, 4};
    src.lists["b"] = {2};
    src.lists["c"] = {4, 5};

    roaring::Roaring out;
    auto q = GramQuery::and_(GramQuery::of_gram("a"),
                             GramQuery::or_(GramQuery::of_gram("b"), GramQuery::of_gram("c")));
    ASSERT_TRUE(query::gram_boolean_query(src, q, 10, &out).ok());
    EXPECT_EQ(ToVec(out), (std::vector<uint32_t> {2, 4}));
}
