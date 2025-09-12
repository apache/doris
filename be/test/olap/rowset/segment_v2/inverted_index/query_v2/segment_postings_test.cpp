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

#include "olap/rowset/segment_v2/inverted_index/query_v2/segment_postings.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <vector>

namespace doris {

using namespace segment_v2::inverted_index::query_v2;

class SegmentPostingsTest : public ::testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}
};

class FakeIter {
public:
    struct Entry {
        int32_t d;
        int32_t f;
        int32_t n;
    };

    explicit FakeIter(std::vector<Entry> postings) : _postings(std::move(postings)) {}

    bool next() {
        if (_idx + 1 < static_cast<int32_t>(_postings.size())) {
            ++_idx;
            return true;
        }
        return false;
    }

    bool skipTo(uint32_t target) {
        int32_t start = std::max(_idx, 0);
        for (int32_t j = start; j < static_cast<int32_t>(_postings.size()); ++j) {
            if (static_cast<uint32_t>(_postings[j].d) >= target) {
                _idx = j;
                return true;
            }
        }
        return false;
    }

    int32_t doc() const { return _postings[_idx].d; }
    int32_t freq() const { return _postings[_idx].f; }
    int32_t norm() const { return _postings[_idx].n; }
    uint32_t docFreq() const { return static_cast<uint32_t>(_postings.size()); }

private:
    std::vector<Entry> _postings;
    int32_t _idx = -1;
};

TEST_F(SegmentPostingsTest, BasicIterationAndScore) {
    using IterPtr = std::shared_ptr<FakeIter>;
    std::vector<FakeIter::Entry> data = {{1, 2, 10}, {3, 1, 7}, {5, 4, 5}};
    auto iter = std::make_shared<FakeIter>(data);

    SegmentPostings<IterPtr> sp(iter);

    EXPECT_EQ(sp.size_hint(), 3u);
    EXPECT_EQ(sp.doc(), 1u);
    EXPECT_EQ(sp.freq(), 2);
    EXPECT_EQ(sp.norm(), 10);

    EXPECT_EQ(sp.advance(), 3u);
    EXPECT_EQ(sp.doc(), 3u);
    EXPECT_EQ(sp.freq(), 1);
    EXPECT_EQ(sp.norm(), 7);

    EXPECT_EQ(sp.advance(), 5u);
    EXPECT_EQ(sp.doc(), 5u);
    EXPECT_EQ(sp.freq(), 4);
    EXPECT_EQ(sp.norm(), 5);

    EXPECT_EQ(sp.advance(), TERMINATED);
    EXPECT_EQ(sp.doc(), TERMINATED);
}

TEST_F(SegmentPostingsTest, SeekBehavior) {
    using IterPtr = std::shared_ptr<FakeIter>;
    std::vector<FakeIter::Entry> data = {{2, 1, 1}, {10, 2, 3}, {15, 3, 9}};
    auto iter = std::make_shared<FakeIter>(data);

    SegmentPostings<IterPtr> sp(iter);

    EXPECT_EQ(sp.doc(), 2u);
    EXPECT_EQ(sp.seek(0), 2u);
    EXPECT_EQ(sp.seek(2), 2u);
    EXPECT_EQ(sp.seek(3), 10u);
    EXPECT_EQ(sp.seek(10), 10u);
    EXPECT_EQ(sp.seek(11), 15u);
    EXPECT_EQ(sp.seek(100), TERMINATED);
    EXPECT_EQ(sp.doc(), TERMINATED);
}

TEST_F(SegmentPostingsTest, NoScoreSegmentPostingAlwaysOne) {
    using IterPtr = std::shared_ptr<FakeIter>;
    std::vector<FakeIter::Entry> data = {{1, 100, 200}, {2, 300, 400}};
    auto iter = std::make_shared<FakeIter>(data);

    NoScoreSegmentPosting<IterPtr> sp(iter);
    EXPECT_EQ(sp.doc(), 1u);
    EXPECT_EQ(sp.freq(), 1);
    EXPECT_EQ(sp.norm(), 1);

    EXPECT_EQ(sp.advance(), 2u);
    EXPECT_EQ(sp.freq(), 1);
    EXPECT_EQ(sp.norm(), 1);

    EXPECT_EQ(sp.advance(), TERMINATED);
    EXPECT_EQ(sp.doc(), TERMINATED);
}

TEST_F(SegmentPostingsTest, EmptySegmentPostingAlwaysTerminated) {
    EmptySegmentPosting<std::shared_ptr<FakeIter>> sp;
    EXPECT_EQ(sp.size_hint(), 0u);
    EXPECT_EQ(sp.doc(), TERMINATED);
    EXPECT_EQ(sp.advance(), TERMINATED);
    EXPECT_EQ(sp.seek(123), TERMINATED);
    EXPECT_EQ(sp.freq(), 1);
    EXPECT_EQ(sp.norm(), 1);
}

TEST_F(SegmentPostingsTest, ConstructorWithEmptyIterator) {
    using IterPtr = std::shared_ptr<FakeIter>;
    std::vector<FakeIter::Entry> data;
    auto iter = std::make_shared<FakeIter>(data);

    SegmentPostings<IterPtr> sp(iter);
    EXPECT_EQ(sp.size_hint(), 0u);
    EXPECT_EQ(sp.doc(), TERMINATED);
}

TEST_F(SegmentPostingsTest, IntMaxDocBecomesTerminatedOnInit) {
    using IterPtr = std::shared_ptr<FakeIter>;
    std::vector<FakeIter::Entry> data = {{INT_MAX, 1, 1}};
    auto iter = std::make_shared<FakeIter>(data);

    SegmentPostings<IterPtr> sp(iter);
    EXPECT_EQ(sp.doc(), TERMINATED);
}

} // namespace doris