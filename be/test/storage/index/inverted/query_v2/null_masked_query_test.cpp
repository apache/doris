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

#include "storage/index/inverted/query_v2/null_masked_query.h"

#include <gtest/gtest.h>

#include "storage/index/inverted/query_v2/bit_set_query/bit_set_query.h"
#include "storage/index/inverted/query_v2/boolean_query/boolean_query_builder.h"

namespace doris::segment_v2::inverted_index::query_v2 {
namespace {
roaring::Roaring rows(std::initializer_list<uint32_t> docs) {
    roaring::Roaring result;
    for (auto doc : docs) {
        result.add(doc);
    }
    return result;
}
QueryPtr masked(std::initializer_list<uint32_t> postings, std::initializer_list<uint32_t> nulls) {
    return std::make_shared<NullMaskedQuery>(std::make_shared<BitSetQuery>(rows(postings)),
                                             std::make_shared<roaring::Roaring>(rows(nulls)));
}
roaring::Roaring collect(const QueryPtr& query, bool scoring) {
    QueryExecutionContext context;
    context.segment_num_rows = 5;
    auto scorer = query->weight(scoring)->scorer(context);
    EXPECT_FALSE(scorer->has_null_bitmap());
    roaring::Roaring result;
    for (auto doc = scorer->doc(); doc != TERMINATED; doc = scorer->advance()) {
        result.add(doc);
    }
    return result;
}
} // namespace

TEST(NullMaskedQueryTest, PhysicalNullPostingsAreRemovedBeforeNot) {
    for (bool scoring : {false, true}) {
        auto leaf = masked({1, 2}, {1});
        EXPECT_EQ(rows({2}), collect(leaf, scoring));
        auto builder = create_operator_boolean_query_builder(OperatorType::OP_NOT);
        builder->add(leaf, "");
        EXPECT_EQ(rows({0, 1, 3, 4}), collect(builder->build(), scoring));
    }
}

TEST(NullMaskedQueryTest, MaskIsLocalToLeafBeforeOrAnd) {
    for (bool scoring : {false, true}) {
        auto left = masked({1, 2}, {1});
        auto right = masked({1, 3}, {});
        auto either = create_operator_boolean_query_builder(OperatorType::OP_OR);
        either->add(left, "");
        either->add(right, "");
        EXPECT_EQ(rows({1, 2, 3}), collect(either->build(), scoring));
        auto both = create_operator_boolean_query_builder(OperatorType::OP_AND);
        both->add(left, "");
        both->add(right, "");
        EXPECT_TRUE(collect(both->build(), scoring).isEmpty());
    }
}

TEST(NullMaskedQueryTest, SeekSkipsNullsAndKeepsScores) {
    QueryExecutionContext context;
    context.segment_num_rows = 5;
    auto leaf = masked({0, 1, 2, 3, 4}, {0, 2, 4});
    auto scorer = leaf->weight(true)->scorer(context);
    EXPECT_EQ(1u, scorer->doc());
    EXPECT_EQ(1.0f, scorer->score());
    EXPECT_EQ(3u, scorer->seek(2));
    EXPECT_EQ(1.0f, scorer->score());
    EXPECT_EQ(TERMINATED, scorer->advance());
}

TEST(NullMaskedQueryTest, PruningAndIterationAgree) {
    QueryExecutionContext context;
    context.segment_num_rows = 5;
    auto query = masked({0, 1, 2, 3, 4}, {0, 2, 4});
    auto weight = query->weight(true);
    roaring::Roaring result;
    weight->for_each_pruning(context, "", 0.0f, [&](uint32_t doc, float score) {
        EXPECT_EQ(1.0f, score);
        result.add(doc);
        return 0.0f;
    });
    EXPECT_EQ(rows({1, 3}), result);
}

TEST(NullMaskedQueryTest, SourceBitmapsRemainIndependent) {
    auto nulls = std::make_shared<roaring::Roaring>(rows({1}));
    auto postings = std::make_shared<roaring::Roaring>(rows({1, 2}));
    auto query = std::make_shared<NullMaskedQuery>(std::make_shared<BitSetQuery>(postings), nulls);
    EXPECT_EQ(rows({2}), collect(query, false));
    EXPECT_EQ(rows({1, 2}), *postings);
    EXPECT_EQ(rows({1}), *nulls);
    EXPECT_EQ(rows({2}), collect(query, true));
}
} // namespace doris::segment_v2::inverted_index::query_v2
