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

#include "olap/rowset/segment_v2/inverted_index/query_v2/union/simple_union.h"

#include <gtest/gtest.h>

#include "olap/rowset/segment_v2/inverted_index/query_v2/doc_set.h"

namespace doris {

using namespace segment_v2::inverted_index::query_v2;

class SimpleUnionTest : public testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(SimpleUnionTest, EmptyUnion) {
    std::vector<MockDocSetPtr> docsets;
    auto union_docset = SimpleUnion<MockDocSetPtr>::create(std::move(docsets));

    EXPECT_EQ(union_docset->doc(), TERMINATED);
    EXPECT_EQ(union_docset->advance(), TERMINATED);
    EXPECT_EQ(union_docset->size_hint(), 0);
}

TEST_F(SimpleUnionTest, SingleDocSet) {
    std::vector<MockDocSetPtr> docsets;
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {1, 5, 10, 15}));

    auto union_docset = SimpleUnion<MockDocSetPtr>::create(std::move(docsets));

    EXPECT_EQ(union_docset->doc(), 1);
    EXPECT_EQ(union_docset->advance(), 5);
    EXPECT_EQ(union_docset->advance(), 10);
    EXPECT_EQ(union_docset->advance(), 15);
    EXPECT_EQ(union_docset->advance(), TERMINATED);
}

TEST_F(SimpleUnionTest, TwoDisjointDocSets) {
    std::vector<MockDocSetPtr> docsets;
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {1, 3, 5}));
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {2, 4, 6}));

    auto union_docset = SimpleUnion<MockDocSetPtr>::create(std::move(docsets));

    EXPECT_EQ(union_docset->doc(), 1);
    EXPECT_EQ(union_docset->advance(), 2);
    EXPECT_EQ(union_docset->advance(), 3);
    EXPECT_EQ(union_docset->advance(), 4);
    EXPECT_EQ(union_docset->advance(), 5);
    EXPECT_EQ(union_docset->advance(), 6);
    EXPECT_EQ(union_docset->advance(), TERMINATED);
}

TEST_F(SimpleUnionTest, TwoOverlappingDocSets) {
    std::vector<MockDocSetPtr> docsets;
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {1, 3, 5, 7}));
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {3, 5, 9}));

    auto union_docset = SimpleUnion<MockDocSetPtr>::create(std::move(docsets));

    // Union should return unique docs in order: 1, 3, 5, 7, 9
    EXPECT_EQ(union_docset->doc(), 1);
    EXPECT_EQ(union_docset->advance(), 3);
    EXPECT_EQ(union_docset->advance(), 5);
    EXPECT_EQ(union_docset->advance(), 7);
    EXPECT_EQ(union_docset->advance(), 9);
    EXPECT_EQ(union_docset->advance(), TERMINATED);
}

TEST_F(SimpleUnionTest, MultipleDocSets) {
    std::vector<MockDocSetPtr> docsets;
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {1, 10, 20}));
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {5, 15, 25}));
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {3, 13, 23}));

    auto union_docset = SimpleUnion<MockDocSetPtr>::create(std::move(docsets));

    EXPECT_EQ(union_docset->doc(), 1);
    EXPECT_EQ(union_docset->advance(), 3);
    EXPECT_EQ(union_docset->advance(), 5);
    EXPECT_EQ(union_docset->advance(), 10);
    EXPECT_EQ(union_docset->advance(), 13);
    EXPECT_EQ(union_docset->advance(), 15);
    EXPECT_EQ(union_docset->advance(), 20);
    EXPECT_EQ(union_docset->advance(), 23);
    EXPECT_EQ(union_docset->advance(), 25);
    EXPECT_EQ(union_docset->advance(), TERMINATED);
}

TEST_F(SimpleUnionTest, SeekToExactDoc) {
    std::vector<MockDocSetPtr> docsets;
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {1, 5, 10, 15}));
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {2, 6, 12, 16}));

    auto union_docset = SimpleUnion<MockDocSetPtr>::create(std::move(docsets));

    EXPECT_EQ(union_docset->doc(), 1);
    EXPECT_EQ(union_docset->seek(10), 10);
    EXPECT_EQ(union_docset->doc(), 10);
}

TEST_F(SimpleUnionTest, SeekToNonExistentDoc) {
    std::vector<MockDocSetPtr> docsets;
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {1, 5, 10, 15}));
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {2, 6, 12, 16}));

    auto union_docset = SimpleUnion<MockDocSetPtr>::create(std::move(docsets));

    EXPECT_EQ(union_docset->doc(), 1);
    // Seek to 7 should return the next available doc (10)
    EXPECT_EQ(union_docset->seek(7), 10);
    EXPECT_EQ(union_docset->doc(), 10);
}

TEST_F(SimpleUnionTest, SeekBeyondAllDocs) {
    std::vector<MockDocSetPtr> docsets;
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {1, 5, 10}));
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {2, 6, 12}));

    auto union_docset = SimpleUnion<MockDocSetPtr>::create(std::move(docsets));

    EXPECT_EQ(union_docset->seek(100), TERMINATED);
    EXPECT_EQ(union_docset->doc(), TERMINATED);
}

TEST_F(SimpleUnionTest, SeekThenAdvance) {
    std::vector<MockDocSetPtr> docsets;
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {1, 5, 10, 15, 20}));
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {2, 6, 12, 16, 22}));

    auto union_docset = SimpleUnion<MockDocSetPtr>::create(std::move(docsets));

    EXPECT_EQ(union_docset->seek(8), 10);
    EXPECT_EQ(union_docset->advance(), 12);
    EXPECT_EQ(union_docset->advance(), 15);
    EXPECT_EQ(union_docset->advance(), 16);
}

TEST_F(SimpleUnionTest, SizeHint) {
    std::vector<MockDocSetPtr> docsets;
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {1, 2, 3}, 10));
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {4, 5}, 20));
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {6, 7, 8, 9}, 15));

    auto union_docset = SimpleUnion<MockDocSetPtr>::create(std::move(docsets));

    // Size hint should be the maximum of all docsets (20)
    EXPECT_EQ(union_docset->size_hint(), 20);
}

TEST_F(SimpleUnionTest, OneEmptyDocSet) {
    std::vector<MockDocSetPtr> docsets;
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {1, 5, 10}));
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {})); // Empty
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {2, 6, 12}));

    auto union_docset = SimpleUnion<MockDocSetPtr>::create(std::move(docsets));

    // Should work correctly, ignoring the empty docset
    EXPECT_EQ(union_docset->doc(), 1);
    EXPECT_EQ(union_docset->advance(), 2);
    EXPECT_EQ(union_docset->advance(), 5);
    EXPECT_EQ(union_docset->advance(), 6);
    EXPECT_EQ(union_docset->advance(), 10);
    EXPECT_EQ(union_docset->advance(), 12);
    EXPECT_EQ(union_docset->advance(), TERMINATED);
}

TEST_F(SimpleUnionTest, AllEmptyDocSets) {
    std::vector<MockDocSetPtr> docsets;
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {}));
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {}));
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {}));

    auto union_docset = SimpleUnion<MockDocSetPtr>::create(std::move(docsets));

    EXPECT_EQ(union_docset->doc(), TERMINATED);
    EXPECT_EQ(union_docset->advance(), TERMINATED);
}

TEST_F(SimpleUnionTest, IdenticalDocSets) {
    std::vector<MockDocSetPtr> docsets;
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {1, 5, 10}));
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {1, 5, 10}));

    auto union_docset = SimpleUnion<MockDocSetPtr>::create(std::move(docsets));

    // Should return each doc once even though they appear in both sets
    EXPECT_EQ(union_docset->doc(), 1);
    EXPECT_EQ(union_docset->advance(), 5);
    EXPECT_EQ(union_docset->advance(), 10);
    EXPECT_EQ(union_docset->advance(), TERMINATED);
}

TEST_F(SimpleUnionTest, LargeNumberOfDocSets) {
    std::vector<MockDocSetPtr> docsets;
    // Create 10 docsets with docs at multiples of their index
    for (int i = 1; i <= 10; ++i) {
        std::vector<uint32_t> docs;
        for (int j = 1; j <= 5; ++j) {
            docs.push_back(i * j);
        }
        docsets.push_back(std::make_shared<MockDocSet>(docs));
    }

    auto union_docset = SimpleUnion<MockDocSetPtr>::create(std::move(docsets));

    // Should start with smallest doc (1)
    EXPECT_EQ(union_docset->doc(), 1);

    // Count total unique docs
    int count = 1;
    while (union_docset->advance() != TERMINATED) {
        count++;
    }

    // We should have multiple docs (exact count depends on overlaps)
    EXPECT_GT(count, 10);
}

TEST_F(SimpleUnionTest, SeekToCurrentDoc) {
    std::vector<MockDocSetPtr> docsets;
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {1, 5, 10, 15}));
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {2, 6, 12, 16}));

    auto union_docset = SimpleUnion<MockDocSetPtr>::create(std::move(docsets));

    EXPECT_EQ(union_docset->doc(), 1);
    EXPECT_EQ(union_docset->advance(), 2);
    EXPECT_EQ(union_docset->doc(), 2);

    // Seek to current doc should stay at current
    EXPECT_EQ(union_docset->seek(2), 2);
    EXPECT_EQ(union_docset->doc(), 2);
}

TEST_F(SimpleUnionTest, SeekBackwards) {
    std::vector<MockDocSetPtr> docsets;
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {1, 5, 10, 15}));
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {2, 6, 12, 16}));

    auto union_docset = SimpleUnion<MockDocSetPtr>::create(std::move(docsets));

    EXPECT_EQ(union_docset->seek(10), 10);
    EXPECT_EQ(union_docset->doc(), 10);

    // Seek backwards should return next available doc (which is 10 or later)
    EXPECT_EQ(union_docset->seek(5), 10);
    EXPECT_EQ(union_docset->doc(), 10);
}

TEST_F(SimpleUnionTest, FreqWithSingleDocSet) {
    std::map<uint32_t, std::vector<uint32_t>> positions1 = {
            {1, {0, 5, 10}},   // doc 1 有 3 个位置
            {5, {2, 8}},       // doc 5 有 2 个位置
            {10, {1, 3, 7, 9}} // doc 10 有 4 个位置
    };

    std::vector<MockDocSetPtr> docsets;
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {1, 5, 10}, positions1));

    auto union_docset = SimpleUnion<MockDocSetPtr>::create(std::move(docsets));

    EXPECT_EQ(union_docset->doc(), 1);
    EXPECT_EQ(union_docset->freq(), 3);

    EXPECT_EQ(union_docset->advance(), 5);
    EXPECT_EQ(union_docset->freq(), 2);

    EXPECT_EQ(union_docset->advance(), 10);
    EXPECT_EQ(union_docset->freq(), 4);
}

TEST_F(SimpleUnionTest, FreqWithMultipleDocSetsDisjoint) {
    std::map<uint32_t, std::vector<uint32_t>> positions1 = {{1, {0, 2}}, {5, {1, 3, 5}}};
    std::map<uint32_t, std::vector<uint32_t>> positions2 = {{10, {0, 1, 2}}};

    std::vector<MockDocSetPtr> docsets;
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {1, 5}, positions1));
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {10}, positions2));

    auto union_docset = SimpleUnion<MockDocSetPtr>::create(std::move(docsets));

    EXPECT_EQ(union_docset->doc(), 1);
    EXPECT_EQ(union_docset->freq(), 2);

    EXPECT_EQ(union_docset->advance(), 5);
    EXPECT_EQ(union_docset->freq(), 3);

    EXPECT_EQ(union_docset->advance(), 10);
    EXPECT_EQ(union_docset->freq(), 3);
}

TEST_F(SimpleUnionTest, FreqWithOverlappingDocs) {
    std::map<uint32_t, std::vector<uint32_t>> positions1 = {{1, {0, 2}}, {5, {1, 3, 5}}};
    std::map<uint32_t, std::vector<uint32_t>> positions2 = {{5, {2, 4}}, {10, {0, 1, 2}}};

    std::vector<MockDocSetPtr> docsets;
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {1, 5}, positions1));
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {5, 10}, positions2));

    auto union_docset = SimpleUnion<MockDocSetPtr>::create(std::move(docsets));

    EXPECT_EQ(union_docset->doc(), 1);
    EXPECT_EQ(union_docset->freq(), 2);

    EXPECT_EQ(union_docset->advance(), 5);
    EXPECT_EQ(union_docset->freq(), 5); // 3 + 2 = 5

    EXPECT_EQ(union_docset->advance(), 10);
    EXPECT_EQ(union_docset->freq(), 3);
}

TEST_F(SimpleUnionTest, FreqWithThreeDocSets) {
    std::map<uint32_t, std::vector<uint32_t>> positions1 = {{5, {0, 1}}};
    std::map<uint32_t, std::vector<uint32_t>> positions2 = {{5, {2, 3, 4}}};
    std::map<uint32_t, std::vector<uint32_t>> positions3 = {{5, {5}}};

    std::vector<MockDocSetPtr> docsets;
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {5}, positions1));
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {5}, positions2));
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {5}, positions3));

    auto union_docset = SimpleUnion<MockDocSetPtr>::create(std::move(docsets));

    EXPECT_EQ(union_docset->doc(), 5);
    EXPECT_EQ(union_docset->freq(), 6); // 2 + 3 + 1 = 6
}

TEST_F(SimpleUnionTest, FreqAfterSeek) {
    std::map<uint32_t, std::vector<uint32_t>> positions = {
            {1, {0, 1}}, {5, {2, 3, 4}}, {10, {5, 6}}};

    std::vector<MockDocSetPtr> docsets;
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {1, 5, 10}, positions));

    auto union_docset = SimpleUnion<MockDocSetPtr>::create(std::move(docsets));

    EXPECT_EQ(union_docset->seek(5), 5);
    EXPECT_EQ(union_docset->freq(), 3);
}

TEST_F(SimpleUnionTest, NormWithSingleDocSet) {
    std::vector<MockDocSetPtr> docsets;
    docsets.push_back(std::make_shared<MockDocSet>(
            std::vector<uint32_t> {1, 5, 10}, std::map<uint32_t, std::vector<uint32_t>> {}, 0, 42));

    auto union_docset = SimpleUnion<MockDocSetPtr>::create(std::move(docsets));

    EXPECT_EQ(union_docset->doc(), 1);
    EXPECT_EQ(union_docset->norm(), 42);

    EXPECT_EQ(union_docset->advance(), 5);
    EXPECT_EQ(union_docset->norm(), 42);
}

TEST_F(SimpleUnionTest, NormReturnsFirstMatchingDocSet) {
    std::vector<MockDocSetPtr> docsets;
    docsets.push_back(std::make_shared<MockDocSet>(
            std::vector<uint32_t> {1, 5}, std::map<uint32_t, std::vector<uint32_t>> {}, 0, 10));
    docsets.push_back(std::make_shared<MockDocSet>(
            std::vector<uint32_t> {5, 10}, std::map<uint32_t, std::vector<uint32_t>> {}, 0, 20));

    auto union_docset = SimpleUnion<MockDocSetPtr>::create(std::move(docsets));

    EXPECT_EQ(union_docset->doc(), 1);
    EXPECT_EQ(union_docset->norm(), 10);

    EXPECT_EQ(union_docset->advance(), 5);
    EXPECT_EQ(union_docset->norm(), 10);

    EXPECT_EQ(union_docset->advance(), 10);
    EXPECT_EQ(union_docset->norm(), 20);
}

TEST_F(SimpleUnionTest, NormWithDifferentNorms) {
    std::vector<MockDocSetPtr> docsets;
    docsets.push_back(std::make_shared<MockDocSet>(
            std::vector<uint32_t> {1, 3}, std::map<uint32_t, std::vector<uint32_t>> {}, 0, 5));
    docsets.push_back(std::make_shared<MockDocSet>(
            std::vector<uint32_t> {2, 4}, std::map<uint32_t, std::vector<uint32_t>> {}, 0, 15));
    docsets.push_back(std::make_shared<MockDocSet>(
            std::vector<uint32_t> {3, 5}, std::map<uint32_t, std::vector<uint32_t>> {}, 0, 25));

    auto union_docset = SimpleUnion<MockDocSetPtr>::create(std::move(docsets));

    EXPECT_EQ(union_docset->doc(), 1);
    EXPECT_EQ(union_docset->norm(), 5);

    EXPECT_EQ(union_docset->advance(), 2);
    EXPECT_EQ(union_docset->norm(), 15);

    EXPECT_EQ(union_docset->advance(), 3);
    EXPECT_EQ(union_docset->norm(), 5);

    EXPECT_EQ(union_docset->advance(), 4);
    EXPECT_EQ(union_docset->norm(), 15);

    EXPECT_EQ(union_docset->advance(), 5);
    EXPECT_EQ(union_docset->norm(), 25);
}

TEST_F(SimpleUnionTest, AppendPositionsSingleDocSet) {
    std::map<uint32_t, std::vector<uint32_t>> positions = {{1, {0, 5, 10}}, {5, {2, 8, 15}}};

    std::vector<MockDocSetPtr> docsets;
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {1, 5}, positions));

    auto union_docset = SimpleUnion<MockDocSetPtr>::create(std::move(docsets));

    EXPECT_EQ(union_docset->doc(), 1);

    std::vector<uint32_t> output;
    union_docset->append_positions_with_offset(0, output);
    EXPECT_EQ(output, (std::vector<uint32_t> {0, 5, 10}));

    EXPECT_EQ(union_docset->advance(), 5);
    output.clear();
    union_docset->append_positions_with_offset(0, output);
    EXPECT_EQ(output, (std::vector<uint32_t> {2, 8, 15}));
}

TEST_F(SimpleUnionTest, AppendPositionsWithOffset) {
    std::map<uint32_t, std::vector<uint32_t>> positions = {{1, {0, 2, 4}}};

    std::vector<MockDocSetPtr> docsets;
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {1}, positions));

    auto union_docset = SimpleUnion<MockDocSetPtr>::create(std::move(docsets));

    EXPECT_EQ(union_docset->doc(), 1);

    std::vector<uint32_t> output;
    union_docset->append_positions_with_offset(10, output);
    EXPECT_EQ(output, (std::vector<uint32_t> {10, 12, 14}));
}

TEST_F(SimpleUnionTest, AppendPositionsMergeAndDeduplicate) {
    std::map<uint32_t, std::vector<uint32_t>> positions1 = {{5, {1, 3, 5, 7}}};
    std::map<uint32_t, std::vector<uint32_t>> positions2 = {{5, {3, 5, 9, 11}}};

    std::vector<MockDocSetPtr> docsets;
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {5}, positions1));
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {5}, positions2));

    auto union_docset = SimpleUnion<MockDocSetPtr>::create(std::move(docsets));

    EXPECT_EQ(union_docset->doc(), 5);

    std::vector<uint32_t> output;
    union_docset->append_positions_with_offset(0, output);

    EXPECT_EQ(output, (std::vector<uint32_t> {1, 3, 5, 7, 9, 11}));
}

TEST_F(SimpleUnionTest, AppendPositionsMultipleCalls) {
    std::map<uint32_t, std::vector<uint32_t>> positions = {{1, {0, 5}}};

    std::vector<MockDocSetPtr> docsets;
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {1}, positions));

    auto union_docset = SimpleUnion<MockDocSetPtr>::create(std::move(docsets));

    EXPECT_EQ(union_docset->doc(), 1);

    std::vector<uint32_t> output = {100, 200};
    union_docset->append_positions_with_offset(0, output);

    EXPECT_EQ(output, (std::vector<uint32_t> {100, 200, 0, 5}));
}

TEST_F(SimpleUnionTest, AppendPositionsThreeDocSets) {
    std::map<uint32_t, std::vector<uint32_t>> positions1 = {{10, {0, 10}}};
    std::map<uint32_t, std::vector<uint32_t>> positions2 = {{10, {5, 15}}};
    std::map<uint32_t, std::vector<uint32_t>> positions3 = {{10, {10, 20}}};

    std::vector<MockDocSetPtr> docsets;
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {10}, positions1));
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {10}, positions2));
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {10}, positions3));

    auto union_docset = SimpleUnion<MockDocSetPtr>::create(std::move(docsets));

    EXPECT_EQ(union_docset->doc(), 10);

    std::vector<uint32_t> output;
    union_docset->append_positions_with_offset(0, output);

    EXPECT_EQ(output, (std::vector<uint32_t> {0, 5, 10, 15, 20}));
}

TEST_F(SimpleUnionTest, AppendPositionsWithDifferentOffsets) {
    std::map<uint32_t, std::vector<uint32_t>> positions1 = {{5, {0, 1}}};
    std::map<uint32_t, std::vector<uint32_t>> positions2 = {{5, {2, 3}}};

    std::vector<MockDocSetPtr> docsets;
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {5}, positions1));
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {5}, positions2));

    auto union_docset = SimpleUnion<MockDocSetPtr>::create(std::move(docsets));

    EXPECT_EQ(union_docset->doc(), 5);

    std::vector<uint32_t> output;
    union_docset->append_positions_with_offset(100, output);

    EXPECT_EQ(output, (std::vector<uint32_t> {100, 101, 102, 103}));
}

TEST_F(SimpleUnionTest, AppendPositionsPartialMatch) {
    std::map<uint32_t, std::vector<uint32_t>> positions1 = {{1, {0, 1}}, {5, {2, 3}}};
    std::map<uint32_t, std::vector<uint32_t>> positions2 = {{1, {4, 5}}, {10, {6, 7}}};

    std::vector<MockDocSetPtr> docsets;
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {1, 5}, positions1));
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {1, 10}, positions2));

    auto union_docset = SimpleUnion<MockDocSetPtr>::create(std::move(docsets));

    EXPECT_EQ(union_docset->doc(), 1);
    std::vector<uint32_t> output1;
    union_docset->append_positions_with_offset(0, output1);
    EXPECT_EQ(output1, (std::vector<uint32_t> {0, 1, 4, 5}));

    EXPECT_EQ(union_docset->advance(), 5);
    std::vector<uint32_t> output2;
    union_docset->append_positions_with_offset(0, output2);
    EXPECT_EQ(output2, (std::vector<uint32_t> {2, 3}));

    EXPECT_EQ(union_docset->advance(), 10);
    std::vector<uint32_t> output3;
    union_docset->append_positions_with_offset(0, output3);
    EXPECT_EQ(output3, (std::vector<uint32_t> {6, 7}));
}

TEST_F(SimpleUnionTest, AppendPositionsEmptyPositions) {
    std::vector<MockDocSetPtr> docsets;
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {1, 5}));

    auto union_docset = SimpleUnion<MockDocSetPtr>::create(std::move(docsets));

    EXPECT_EQ(union_docset->doc(), 1);

    std::vector<uint32_t> output;
    union_docset->append_positions_with_offset(0, output);

    EXPECT_TRUE(output.empty());
}

} // namespace doris