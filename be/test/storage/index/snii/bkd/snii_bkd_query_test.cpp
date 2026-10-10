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

#include "storage/index/snii/snii_bkd_query.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <vector>

namespace doris::segment_v2 {
namespace {

const std::vector<uint8_t>& lower_probe() {
    static const std::vector<uint8_t> bytes = {0x80, 0x00, 0x00, 0x2A};
    return bytes;
}

const std::vector<uint8_t>& upper_probe() {
    static const std::vector<uint8_t> bytes = {0x80, 0x00, 0x00, 0x2B};
    return bytes;
}

bool same(snii::Slice bound, const std::vector<uint8_t>& expected) {
    return bound.size() == expected.size() &&
           std::memcmp(bound.data(), expected.data(), bound.size()) == 0;
}

} // namespace

// Equality is the closed interval covering every byte representative SQL treats
// as equal. It is normally a point; signed floating zero uses [-0.0, +0.0].
TEST(SniiBkdQueryTest, EqualityCoversTheSqlEquivalenceClass) {
    BkdQueryBounds bounds;
    ASSERT_TRUE(build_bkd_query_bounds(InvertedIndexQueryType::EQUAL_QUERY,
                                       snii::Slice(lower_probe()), snii::Slice(upper_probe()),
                                       &bounds)
                        .ok());
    EXPECT_TRUE(same(bounds.lower, lower_probe()));
    EXPECT_TRUE(same(bounds.upper, upper_probe()));
    EXPECT_TRUE(bounds.lower_inclusive);
    EXPECT_TRUE(bounds.upper_inclusive);
}

// The one-sided shapes. The open side must be UNBOUNDED -- an empty Slice -- not
// the type's limit: pinning it would make every one-sided query carry an encode
// it never uses, which is what the old implementation did.
TEST(SniiBkdQueryTest, OneSidedQueriesLeaveTheOtherSideUnbounded) {
    struct Case {
        InvertedIndexQueryType type;
        bool lower_side;
        bool inclusive;
    };
    const Case cases[] = {
            {InvertedIndexQueryType::LESS_THAN_QUERY, false, false},
            {InvertedIndexQueryType::LESS_EQUAL_QUERY, false, true},
            {InvertedIndexQueryType::GREATER_THAN_QUERY, true, false},
            {InvertedIndexQueryType::GREATER_EQUAL_QUERY, true, true},
    };
    for (const Case& c : cases) {
        SCOPED_TRACE("query type " + std::to_string(static_cast<int>(c.type)));
        BkdQueryBounds bounds;
        ASSERT_TRUE(build_bkd_query_bounds(c.type, snii::Slice(lower_probe()),
                                           snii::Slice(upper_probe()), &bounds)
                            .ok());
        if (c.lower_side) {
            const auto& expected = c.inclusive ? lower_probe() : upper_probe();
            EXPECT_TRUE(same(bounds.lower, expected));
            EXPECT_EQ(bounds.lower_inclusive, c.inclusive);
            EXPECT_EQ(bounds.upper.size(), 0U) << "the upper side must stay unbounded";
        } else {
            const auto& expected = c.inclusive ? upper_probe() : lower_probe();
            EXPECT_TRUE(same(bounds.upper, expected));
            EXPECT_EQ(bounds.upper_inclusive, c.inclusive);
            EXPECT_EQ(bounds.lower.size(), 0U) << "the lower side must stay unbounded";
        }
    }
}

// Everything else must be REFUSED rather than answered approximately, so the
// caller falls back to a normal predicate. RANGE_QUERY and LIST_QUERY are in the
// enum but only the SEARCH DSL produces them; a BKD reader must not pretend to
// understand either.
TEST(SniiBkdQueryTest, UnsupportedQueryTypesAreRefused) {
    for (const InvertedIndexQueryType type :
         {InvertedIndexQueryType::UNKNOWN_QUERY, InvertedIndexQueryType::MATCH_ANY_QUERY,
          InvertedIndexQueryType::MATCH_ALL_QUERY, InvertedIndexQueryType::MATCH_PHRASE_QUERY,
          InvertedIndexQueryType::RANGE_QUERY, InvertedIndexQueryType::LIST_QUERY}) {
        SCOPED_TRACE("query type " + std::to_string(static_cast<int>(type)));
        BkdQueryBounds bounds;
        const Status status = build_bkd_query_bounds(type, snii::Slice(lower_probe()),
                                                     snii::Slice(upper_probe()), &bounds);
        EXPECT_TRUE(status.is<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>()) << status;
    }
}

} // namespace doris::segment_v2
