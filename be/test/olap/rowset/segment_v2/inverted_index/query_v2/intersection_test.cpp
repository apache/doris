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

#include "olap/rowset/segment_v2/inverted_index/query_v2/intersection.h"

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "common/exception.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/doc_set.h"

namespace doris {

using segment_v2::inverted_index::query_v2::DocSet;
using segment_v2::inverted_index::query_v2::Intersection;
using segment_v2::inverted_index::query_v2::TERMINATED;
using segment_v2::inverted_index::query_v2::MockDocSet;
using segment_v2::inverted_index::query_v2::MockDocSetPtr;

class IntersectionTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

// Test creating intersection with less than 2 docsets (should throw exception)
TEST_F(IntersectionTest, test_create_with_empty_docsets) {
    std::vector<MockDocSetPtr> docsets;

    EXPECT_THROW((Intersection<MockDocSetPtr, MockDocSetPtr>::create(docsets)), Exception);
}

// Test creating intersection with only 1 docset (should throw exception)
TEST_F(IntersectionTest, test_create_with_single_docset) {
    std::vector<MockDocSetPtr> docsets;
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {1, 2, 3}));

    EXPECT_THROW((Intersection<MockDocSetPtr, MockDocSetPtr>::create(docsets)), Exception);
}

// Test creating intersection with exactly 2 docsets
TEST_F(IntersectionTest, test_create_with_two_docsets) {
    std::vector<MockDocSetPtr> docsets;
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {1, 2, 3, 4, 5}));
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {2, 3, 4, 6, 7}));

    auto intersection = Intersection<MockDocSetPtr, MockDocSetPtr>::create(docsets);
    ASSERT_NE(nullptr, intersection);

    // Should start at first matching document
    EXPECT_EQ(2u, intersection->doc());
}

// Test intersection advance with two docsets
TEST_F(IntersectionTest, test_advance_two_docsets) {
    std::vector<MockDocSetPtr> docsets;
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {1, 3, 5, 7, 9}));
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {3, 5, 9, 11}));

    auto intersection = Intersection<MockDocSetPtr, MockDocSetPtr>::create(docsets);
    ASSERT_NE(nullptr, intersection);

    std::vector<uint32_t> results;
    uint32_t doc = intersection->doc();
    while (doc != TERMINATED) {
        results.push_back(doc);
        doc = intersection->advance();
    }

    std::vector<uint32_t> expected {3, 5, 9};
    EXPECT_EQ(expected, results);
}

// Test intersection with three docsets
TEST_F(IntersectionTest, test_three_docsets) {
    std::vector<MockDocSetPtr> docsets;
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {1, 2, 3, 4, 5, 6}));
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {2, 4, 6, 8}));
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {2, 3, 4, 6, 7}));

    auto intersection = Intersection<MockDocSetPtr, MockDocSetPtr>::create(docsets);
    ASSERT_NE(nullptr, intersection);

    std::vector<uint32_t> results;
    uint32_t doc = intersection->doc();
    while (doc != TERMINATED) {
        results.push_back(doc);
        doc = intersection->advance();
    }

    std::vector<uint32_t> expected {2, 4, 6};
    EXPECT_EQ(expected, results);
}

// Test intersection with four docsets
TEST_F(IntersectionTest, test_four_docsets) {
    std::vector<MockDocSetPtr> docsets;
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {1, 2, 3, 4, 5, 10}));
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {2, 4, 5, 10, 12}));
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {2, 3, 5, 10, 11}));
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {5, 10, 15}));

    auto intersection = Intersection<MockDocSetPtr, MockDocSetPtr>::create(docsets);
    ASSERT_NE(nullptr, intersection);

    std::vector<uint32_t> results;
    uint32_t doc = intersection->doc();
    while (doc != TERMINATED) {
        results.push_back(doc);
        doc = intersection->advance();
    }

    std::vector<uint32_t> expected {5, 10};
    EXPECT_EQ(expected, results);
}

// Test intersection with no common documents
TEST_F(IntersectionTest, test_no_intersection) {
    std::vector<MockDocSetPtr> docsets;
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {1, 3, 5}));
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {2, 4, 6}));

    auto intersection = Intersection<MockDocSetPtr, MockDocSetPtr>::create(docsets);
    ASSERT_NE(nullptr, intersection);

    // Should be terminated immediately
    EXPECT_EQ(TERMINATED, intersection->doc());
    EXPECT_EQ(TERMINATED, intersection->advance());
}

// Test intersection with single common document
TEST_F(IntersectionTest, test_single_common_document) {
    std::vector<MockDocSetPtr> docsets;
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {1, 5, 10}));
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {2, 5, 8}));
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {5, 7, 9}));

    auto intersection = Intersection<MockDocSetPtr, MockDocSetPtr>::create(docsets);
    ASSERT_NE(nullptr, intersection);

    EXPECT_EQ(5u, intersection->doc());
    EXPECT_EQ(TERMINATED, intersection->advance());
}

// Test seek functionality
TEST_F(IntersectionTest, test_seek) {
    std::vector<MockDocSetPtr> docsets;
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {1, 5, 10, 15, 20}));
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {5, 10, 15, 20, 25}));

    auto intersection = Intersection<MockDocSetPtr, MockDocSetPtr>::create(docsets);
    ASSERT_NE(nullptr, intersection);

    // Seek to doc 10
    EXPECT_EQ(10u, intersection->seek(8));
    EXPECT_EQ(10u, intersection->doc());

    // Seek to doc 20
    EXPECT_EQ(20u, intersection->seek(18));
    EXPECT_EQ(20u, intersection->doc());

    // Seek beyond all docs
    EXPECT_EQ(TERMINATED, intersection->seek(30));
}

// Test seek to current position
TEST_F(IntersectionTest, test_seek_current_position) {
    std::vector<MockDocSetPtr> docsets;
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {5, 10, 15}));
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {5, 10, 15, 20}));

    auto intersection = Intersection<MockDocSetPtr, MockDocSetPtr>::create(docsets);
    ASSERT_NE(nullptr, intersection);

    EXPECT_EQ(5u, intersection->doc());

    // Seek to current position or before should stay at current
    EXPECT_EQ(5u, intersection->seek(5));
    EXPECT_EQ(5u, intersection->doc());

    EXPECT_EQ(5u, intersection->seek(3));
    EXPECT_EQ(5u, intersection->doc());
}

// Test size_hint - should return smallest docset's size hint
TEST_F(IntersectionTest, test_size_hint) {
    std::vector<MockDocSetPtr> docsets;
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {1, 2, 3, 4, 5}, 100));
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {2, 3, 4}, 50));
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {2, 3, 4, 5, 6}, 75));

    auto intersection = Intersection<MockDocSetPtr, MockDocSetPtr>::create(docsets);
    ASSERT_NE(nullptr, intersection);

    // Should return the smallest size hint (from the smallest docset after sorting)
    EXPECT_EQ(50u, intersection->size_hint());
}

// Test norm - should return left docset's norm
TEST_F(IntersectionTest, test_norm) {
    std::vector<MockDocSetPtr> docsets;
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {1, 2, 3}, 0, 10));
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {2, 3, 4}, 0, 20));

    auto intersection = Intersection<MockDocSetPtr, MockDocSetPtr>::create(docsets);
    ASSERT_NE(nullptr, intersection);

    // After creation, docsets are sorted by size_hint, and smallest becomes left
    // Both have same size (3), so order depends on stable sort
    uint32_t norm = intersection->norm();
    EXPECT_TRUE(norm == 10 || norm == 20);
}

// Test docset_mut_specialized for accessing individual docsets
TEST_F(IntersectionTest, test_docset_mut_specialized) {
    std::vector<MockDocSetPtr> docsets;
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {1, 2, 3}));
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {2, 3, 4}));
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {2, 3, 5}));

    auto intersection = Intersection<MockDocSetPtr, MockDocSetPtr>::create(docsets);
    ASSERT_NE(nullptr, intersection);

    // Access left (ord 0)
    auto& docset0 = intersection->docset_mut_specialized<MockDocSetPtr>(0);
    EXPECT_NE(nullptr, docset0);
    EXPECT_EQ(2u, docset0->doc());

    // Access right (ord 1)
    auto& docset1 = intersection->docset_mut_specialized<MockDocSetPtr>(1);
    EXPECT_NE(nullptr, docset1);

    // Access others (ord 2+)
    auto& docset2 = intersection->docset_mut_specialized<MockDocSetPtr>(2);
    EXPECT_NE(nullptr, docset2);
}

// Test all docsets identical
TEST_F(IntersectionTest, test_all_identical_docsets) {
    std::vector<uint32_t> common_docs {1, 2, 3, 4, 5};

    std::vector<MockDocSetPtr> docsets;
    docsets.push_back(std::make_shared<MockDocSet>(common_docs));
    docsets.push_back(std::make_shared<MockDocSet>(common_docs));
    docsets.push_back(std::make_shared<MockDocSet>(common_docs));

    auto intersection = Intersection<MockDocSetPtr, MockDocSetPtr>::create(docsets);
    ASSERT_NE(nullptr, intersection);

    std::vector<uint32_t> results;
    uint32_t doc = intersection->doc();
    while (doc != TERMINATED) {
        results.push_back(doc);
        doc = intersection->advance();
    }

    EXPECT_EQ(common_docs, results);
}

// Test one empty docset
TEST_F(IntersectionTest, test_one_empty_docset) {
    std::vector<MockDocSetPtr> docsets;
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {1, 2, 3}));
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {}));

    auto intersection = Intersection<MockDocSetPtr, MockDocSetPtr>::create(docsets);
    ASSERT_NE(nullptr, intersection);

    // Should be terminated immediately
    EXPECT_EQ(TERMINATED, intersection->doc());
}

// Test consecutive advance calls after termination
TEST_F(IntersectionTest, test_advance_after_termination) {
    std::vector<MockDocSetPtr> docsets;
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {1}));
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {1}));

    auto intersection = Intersection<MockDocSetPtr, MockDocSetPtr>::create(docsets);
    ASSERT_NE(nullptr, intersection);

    EXPECT_EQ(1u, intersection->doc());
    EXPECT_EQ(TERMINATED, intersection->advance());
    EXPECT_EQ(TERMINATED, intersection->advance());
    EXPECT_EQ(TERMINATED, intersection->advance());
}

// Test large document IDs
TEST_F(IntersectionTest, test_large_document_ids) {
    std::vector<MockDocSetPtr> docsets;
    docsets.push_back(
            std::make_shared<MockDocSet>(std::vector<uint32_t> {1000, 10000, 100000, 1000000}));
    docsets.push_back(
            std::make_shared<MockDocSet>(std::vector<uint32_t> {500, 10000, 50000, 1000000}));

    auto intersection = Intersection<MockDocSetPtr, MockDocSetPtr>::create(docsets);
    ASSERT_NE(nullptr, intersection);

    std::vector<uint32_t> results;
    uint32_t doc = intersection->doc();
    while (doc != TERMINATED) {
        results.push_back(doc);
        doc = intersection->advance();
    }

    std::vector<uint32_t> expected {10000, 1000000};
    EXPECT_EQ(expected, results);
}

// Test sparse docsets with large gaps
TEST_F(IntersectionTest, test_sparse_docsets) {
    std::vector<MockDocSetPtr> docsets;
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {1, 100, 200, 300, 400}));
    docsets.push_back(
            std::make_shared<MockDocSet>(std::vector<uint32_t> {50, 100, 150, 200, 250, 300}));

    auto intersection = Intersection<MockDocSetPtr, MockDocSetPtr>::create(docsets);
    ASSERT_NE(nullptr, intersection);

    std::vector<uint32_t> results;
    uint32_t doc = intersection->doc();
    while (doc != TERMINATED) {
        results.push_back(doc);
        doc = intersection->advance();
    }

    std::vector<uint32_t> expected {100, 200, 300};
    EXPECT_EQ(expected, results);
}

// Test seek after advancing
TEST_F(IntersectionTest, test_seek_after_advance) {
    std::vector<MockDocSetPtr> docsets;
    docsets.push_back(
            std::make_shared<MockDocSet>(std::vector<uint32_t> {1, 5, 10, 15, 20, 25, 30}));
    docsets.push_back(
            std::make_shared<MockDocSet>(std::vector<uint32_t> {5, 10, 15, 20, 25, 30, 35}));

    auto intersection = Intersection<MockDocSetPtr, MockDocSetPtr>::create(docsets);
    ASSERT_NE(nullptr, intersection);

    // Start at doc 5
    EXPECT_EQ(5u, intersection->doc());

    // Advance to doc 10
    EXPECT_EQ(10u, intersection->advance());

    // Seek to doc 25
    EXPECT_EQ(25u, intersection->seek(22));

    // Continue advancing
    EXPECT_EQ(30u, intersection->advance());
    EXPECT_EQ(TERMINATED, intersection->advance());
}

// Test multiple seeks without advance
TEST_F(IntersectionTest, test_multiple_seeks) {
    std::vector<MockDocSetPtr> docsets;
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {10, 20, 30, 40, 50}));
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {10, 20, 30, 40, 50, 60}));

    auto intersection = Intersection<MockDocSetPtr, MockDocSetPtr>::create(docsets);
    ASSERT_NE(nullptr, intersection);

    EXPECT_EQ(10u, intersection->doc());

    // Seek to 25
    EXPECT_EQ(30u, intersection->seek(25));

    // Seek to 35
    EXPECT_EQ(40u, intersection->seek(35));

    // Seek to same position
    EXPECT_EQ(40u, intersection->seek(40));

    // Seek backwards (should stay at current)
    EXPECT_EQ(40u, intersection->seek(35));
}

// Test docsets with different sizes are sorted correctly
TEST_F(IntersectionTest, test_docsets_sorted_by_size) {
    // Create docsets with different sizes
    std::vector<MockDocSetPtr> docsets;
    // Largest
    docsets.push_back(std::make_shared<MockDocSet>(
            std::vector<uint32_t> {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 1000));
    // Smallest
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {2, 4, 6}, 100));
    // Medium
    docsets.push_back(std::make_shared<MockDocSet>(std::vector<uint32_t> {2, 3, 4, 5, 6}, 500));

    auto intersection = Intersection<MockDocSetPtr, MockDocSetPtr>::create(docsets);
    ASSERT_NE(nullptr, intersection);

    // size_hint should be from smallest docset
    EXPECT_EQ(100u, intersection->size_hint());

    std::vector<uint32_t> results;
    uint32_t doc = intersection->doc();
    while (doc != TERMINATED) {
        results.push_back(doc);
        doc = intersection->advance();
    }

    std::vector<uint32_t> expected {2, 4, 6};
    EXPECT_EQ(expected, results);
}

} // namespace doris