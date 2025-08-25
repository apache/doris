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

#include "olap/rowset/segment_v2/inverted_index/util/mock_iterator.h"

#include <gtest/gtest.h>

namespace doris::segment_v2::inverted_index {

TEST(MockIteratorTest, DefaultConstructor) {
    MockIterator iter;
    EXPECT_EQ(iter.doc_id(), INT_MAX);
    EXPECT_EQ(iter.freq(), 0);
    EXPECT_EQ(iter.doc_freq(), 0);
    EXPECT_EQ(iter.next_doc(), INT_MAX);
    EXPECT_EQ(iter.advance(0), INT_MAX);
    EXPECT_EQ(iter.next_position(), -1);
}

TEST(MockIteratorTest, ConstructorWithPostings) {
    std::map<int32_t, std::vector<int32_t>> postings = {
            {1, {10, 20, 30}}, {3, {40, 50}}, {5, {60}}};
    MockIterator iter(postings);
    EXPECT_EQ(iter.doc_id(), 1);
    EXPECT_EQ(iter.freq(), 3);
    EXPECT_EQ(iter.doc_freq(), 3);
    EXPECT_EQ(iter.next_position(), 10);
    EXPECT_EQ(iter.next_position(), 20);
    EXPECT_EQ(iter.next_position(), 30);
    EXPECT_EQ(iter.next_position(), -1);
}

TEST(MockIteratorTest, NextDoc) {
    std::map<int32_t, std::vector<int32_t>> postings = {
            {1, {10, 20, 30}}, {3, {40, 50}}, {5, {60}}};
    MockIterator iter(postings);
    EXPECT_EQ(iter.next_doc(), 3);
    EXPECT_EQ(iter.doc_id(), 3);
    EXPECT_EQ(iter.freq(), 2);
    EXPECT_EQ(iter.next_doc(), 5);
    EXPECT_EQ(iter.doc_id(), 5);
    EXPECT_EQ(iter.freq(), 1);
    EXPECT_EQ(iter.next_doc(), INT_MAX);
    EXPECT_EQ(iter.doc_id(), INT_MAX);
    EXPECT_EQ(iter.freq(), 0);
}

TEST(MockIteratorTest, Advance) {
    std::map<int32_t, std::vector<int32_t>> postings = {
            {1, {10, 20, 30}}, {3, {40, 50}}, {5, {60}}};
    MockIterator iter(postings);
    EXPECT_EQ(iter.advance(0), 1);
    EXPECT_EQ(iter.doc_id(), 1);
    EXPECT_EQ(iter.advance(1), 1);
    EXPECT_EQ(iter.doc_id(), 1);
    EXPECT_EQ(iter.advance(2), 3);
    EXPECT_EQ(iter.doc_id(), 3);
    EXPECT_EQ(iter.advance(4), 5);
    EXPECT_EQ(iter.doc_id(), 5);
    EXPECT_EQ(iter.advance(6), INT_MAX);
    EXPECT_EQ(iter.doc_id(), INT_MAX);
}

TEST(MockIteratorTest, NextPosition) {
    std::map<int32_t, std::vector<int32_t>> postings = {
            {1, {10, 20, 30}}, {3, {40, 50}}, {5, {60}}};
    MockIterator iter(postings);
    EXPECT_EQ(iter.next_position(), 10);
    EXPECT_EQ(iter.next_position(), 20);
    EXPECT_EQ(iter.next_position(), 30);
    EXPECT_EQ(iter.next_position(), -1);
    iter.next_doc();
    EXPECT_EQ(iter.next_position(), 40);
    EXPECT_EQ(iter.next_position(), 50);
    EXPECT_EQ(iter.next_position(), -1);
    iter.next_doc();
    EXPECT_EQ(iter.next_position(), 60);
    EXPECT_EQ(iter.next_position(), -1);
}

TEST(MockIteratorTest, SetPostings) {
    MockIterator iter;
    std::map<int32_t, std::vector<int32_t>> postings = {{2, {15, 25}}, {4, {35}}};
    iter.set_postings(postings);
    EXPECT_EQ(iter.doc_id(), 2);
    EXPECT_EQ(iter.freq(), 2);
    EXPECT_EQ(iter.doc_freq(), 2);
    EXPECT_EQ(iter.next_position(), 15);
    EXPECT_EQ(iter.next_position(), 25);
    EXPECT_EQ(iter.next_position(), -1);
    EXPECT_EQ(iter.next_doc(), 4);
    EXPECT_EQ(iter.doc_id(), 4);
    EXPECT_EQ(iter.freq(), 1);
    EXPECT_EQ(iter.next_position(), 35);
    EXPECT_EQ(iter.next_position(), -1);
    EXPECT_EQ(iter.next_doc(), INT_MAX);
}

} // namespace doris::segment_v2::inverted_index