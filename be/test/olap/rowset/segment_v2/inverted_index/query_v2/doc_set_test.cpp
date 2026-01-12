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

#include "olap/rowset/segment_v2/inverted_index/query_v2/doc_set.h"

#include <gtest/gtest.h>

#include <map>
#include <memory>
#include <vector>

namespace doris::segment_v2::inverted_index::query_v2 {

class DocSetTest : public testing::Test {};

TEST_F(DocSetTest, test_docset_base_class_exceptions) {
    DocSet docset;

    EXPECT_THROW(docset.advance(), Exception);
    EXPECT_THROW(docset.seek(0), Exception);
    EXPECT_THROW(docset.doc(), Exception);
    EXPECT_THROW(docset.size_hint(), Exception);
    EXPECT_THROW(docset.freq(), Exception);
    EXPECT_THROW(docset.norm(), Exception);
}

TEST_F(DocSetTest, test_mockdocset_constructor1_empty_docs) {
    std::vector<uint32_t> docs;
    MockDocSet mock_docset(docs);

    EXPECT_EQ(mock_docset.doc(), TERMINATED);
    EXPECT_EQ(mock_docset.size_hint(), 0);
    EXPECT_EQ(mock_docset.norm(), 1);
}

TEST_F(DocSetTest, test_mockdocset_constructor1_with_docs) {
    std::vector<uint32_t> docs = {5, 2, 8, 1, 9};
    MockDocSet mock_docset(docs);

    EXPECT_EQ(mock_docset.doc(), 1);
    EXPECT_EQ(mock_docset.size_hint(), 5);
    EXPECT_EQ(mock_docset.norm(), 1);
}

TEST_F(DocSetTest, test_mockdocset_constructor1_with_size_hint) {
    std::vector<uint32_t> docs = {3, 1, 4};
    MockDocSet mock_docset(docs, 10);

    EXPECT_EQ(mock_docset.doc(), 1);
    EXPECT_EQ(mock_docset.size_hint(), 10);
    EXPECT_EQ(mock_docset.norm(), 1);
}

TEST_F(DocSetTest, test_mockdocset_constructor1_with_norm) {
    std::vector<uint32_t> docs = {2, 5};
    MockDocSet mock_docset(docs, 0, 5);

    EXPECT_EQ(mock_docset.doc(), 2);
    EXPECT_EQ(mock_docset.size_hint(), 2);
    EXPECT_EQ(mock_docset.norm(), 5);
}

TEST_F(DocSetTest, test_mockdocset_constructor1_with_all_params) {
    std::vector<uint32_t> docs = {7, 3, 9};
    MockDocSet mock_docset(docs, 20, 3);

    EXPECT_EQ(mock_docset.doc(), 3);
    EXPECT_EQ(mock_docset.size_hint(), 20);
    EXPECT_EQ(mock_docset.norm(), 3);
}

TEST_F(DocSetTest, test_mockdocset_constructor2_empty_docs) {
    std::vector<uint32_t> docs;
    std::map<uint32_t, std::vector<uint32_t>> doc_positions;
    MockDocSet mock_docset(docs, doc_positions);

    EXPECT_EQ(mock_docset.doc(), TERMINATED);
    EXPECT_EQ(mock_docset.size_hint(), 0);
    EXPECT_EQ(mock_docset.norm(), 1);
}

TEST_F(DocSetTest, test_mockdocset_constructor2_with_docs_and_positions) {
    std::vector<uint32_t> docs = {5, 2, 8};
    std::map<uint32_t, std::vector<uint32_t>> doc_positions;
    doc_positions[2] = {10, 20};
    doc_positions[5] = {15};
    doc_positions[8] = {30, 40, 50};

    MockDocSet mock_docset(docs, doc_positions);

    EXPECT_EQ(mock_docset.doc(), 2);
    EXPECT_EQ(mock_docset.size_hint(), 3);
    EXPECT_EQ(mock_docset.norm(), 1);
    EXPECT_EQ(mock_docset.freq(), 2);
}

TEST_F(DocSetTest, test_mockdocset_constructor2_with_size_hint) {
    std::vector<uint32_t> docs = {1, 3, 5};
    std::map<uint32_t, std::vector<uint32_t>> doc_positions;
    MockDocSet mock_docset(docs, doc_positions, 15);

    EXPECT_EQ(mock_docset.doc(), 1);
    EXPECT_EQ(mock_docset.size_hint(), 15);
    EXPECT_EQ(mock_docset.norm(), 1);
}

TEST_F(DocSetTest, test_mockdocset_constructor2_with_norm) {
    std::vector<uint32_t> docs = {4, 6};
    std::map<uint32_t, std::vector<uint32_t>> doc_positions;
    MockDocSet mock_docset(docs, doc_positions, 0, 7);

    EXPECT_EQ(mock_docset.doc(), 4);
    EXPECT_EQ(mock_docset.size_hint(), 2);
    EXPECT_EQ(mock_docset.norm(), 7);
}

TEST_F(DocSetTest, test_mockdocset_constructor2_with_all_params) {
    std::vector<uint32_t> docs = {2, 4, 6, 8};
    std::map<uint32_t, std::vector<uint32_t>> doc_positions;
    doc_positions[2] = {1, 2};
    MockDocSet mock_docset(docs, doc_positions, 25, 4);

    EXPECT_EQ(mock_docset.doc(), 2);
    EXPECT_EQ(mock_docset.size_hint(), 25);
    EXPECT_EQ(mock_docset.norm(), 4);
    EXPECT_EQ(mock_docset.freq(), 2);
}

TEST_F(DocSetTest, test_next_empty_docs) {
    std::vector<uint32_t> docs;
    MockDocSet mock_docset(docs);

    EXPECT_FALSE(mock_docset.next());
    EXPECT_EQ(mock_docset.doc(), TERMINATED);
}

TEST_F(DocSetTest, test_next_single_doc) {
    std::vector<uint32_t> docs = {5};
    MockDocSet mock_docset(docs);

    EXPECT_FALSE(mock_docset.next());
    EXPECT_EQ(mock_docset.doc(), TERMINATED);
}

TEST_F(DocSetTest, test_next_multiple_docs) {
    std::vector<uint32_t> docs = {1, 3, 5, 7};
    MockDocSet mock_docset(docs);

    EXPECT_EQ(mock_docset.doc(), 1);
    EXPECT_TRUE(mock_docset.next());
    EXPECT_EQ(mock_docset.doc(), 3);
    EXPECT_TRUE(mock_docset.next());
    EXPECT_EQ(mock_docset.doc(), 5);
    EXPECT_TRUE(mock_docset.next());
    EXPECT_EQ(mock_docset.doc(), 7);
    EXPECT_FALSE(mock_docset.next());
    EXPECT_EQ(mock_docset.doc(), TERMINATED);
}

TEST_F(DocSetTest, test_next_at_boundary) {
    std::vector<uint32_t> docs = {2, 4};
    MockDocSet mock_docset(docs);

    EXPECT_EQ(mock_docset.doc(), 2);
    EXPECT_TRUE(mock_docset.next());
    EXPECT_EQ(mock_docset.doc(), 4);
    EXPECT_FALSE(mock_docset.next());
    EXPECT_FALSE(mock_docset.next());
    EXPECT_EQ(mock_docset.doc(), TERMINATED);
}

TEST_F(DocSetTest, test_skipto_empty_docs) {
    std::vector<uint32_t> docs;
    MockDocSet mock_docset(docs);

    EXPECT_FALSE(mock_docset.skipTo(5));
    EXPECT_EQ(mock_docset.doc(), TERMINATED);
}

TEST_F(DocSetTest, test_skipto_current_doc_already_at_target) {
    std::vector<uint32_t> docs = {1, 3, 5, 7};
    MockDocSet mock_docset(docs);

    EXPECT_EQ(mock_docset.doc(), 1);
    EXPECT_TRUE(mock_docset.skipTo(1));
    EXPECT_EQ(mock_docset.doc(), 1);
}

TEST_F(DocSetTest, test_skipto_current_doc_after_target) {
    std::vector<uint32_t> docs = {1, 3, 5, 7};
    MockDocSet mock_docset(docs);

    mock_docset.seek(5);
    EXPECT_EQ(mock_docset.doc(), 5);
    EXPECT_TRUE(mock_docset.skipTo(3));
    EXPECT_EQ(mock_docset.doc(), 5);
}

TEST_F(DocSetTest, test_skipto_find_target) {
    std::vector<uint32_t> docs = {1, 3, 5, 7, 9};
    MockDocSet mock_docset(docs);

    EXPECT_EQ(mock_docset.doc(), 1);
    EXPECT_TRUE(mock_docset.skipTo(5));
    EXPECT_EQ(mock_docset.doc(), 5);
}

TEST_F(DocSetTest, test_skipto_target_not_found) {
    std::vector<uint32_t> docs = {1, 3, 5};
    MockDocSet mock_docset(docs);

    EXPECT_EQ(mock_docset.doc(), 1);
    EXPECT_FALSE(mock_docset.skipTo(10));
    EXPECT_EQ(mock_docset.doc(), TERMINATED);
}

TEST_F(DocSetTest, test_skipto_find_target_between_docs) {
    std::vector<uint32_t> docs = {1, 3, 5, 7};
    MockDocSet mock_docset(docs);

    EXPECT_EQ(mock_docset.doc(), 1);
    EXPECT_TRUE(mock_docset.skipTo(4));
    EXPECT_EQ(mock_docset.doc(), 5);
}

TEST_F(DocSetTest, test_skipto_after_next) {
    std::vector<uint32_t> docs = {1, 3, 5, 7};
    MockDocSet mock_docset(docs);

    mock_docset.next();
    EXPECT_EQ(mock_docset.doc(), 3);
    EXPECT_TRUE(mock_docset.skipTo(6));
    EXPECT_EQ(mock_docset.doc(), 7);
}

TEST_F(DocSetTest, test_skipto_at_end) {
    std::vector<uint32_t> docs = {1, 3};
    MockDocSet mock_docset(docs);

    mock_docset.next();
    EXPECT_EQ(mock_docset.doc(), 3);
    EXPECT_FALSE(mock_docset.skipTo(5));
    EXPECT_EQ(mock_docset.doc(), TERMINATED);
}

TEST_F(DocSetTest, test_docfreq) {
    std::vector<uint32_t> docs = {1, 2, 3};
    MockDocSet mock_docset(docs, 42);

    EXPECT_EQ(mock_docset.docFreq(), 42);
}

TEST_F(DocSetTest, test_advance) {
    std::vector<uint32_t> docs = {1, 3, 5};
    MockDocSet mock_docset(docs);

    EXPECT_EQ(mock_docset.doc(), 1);
    EXPECT_EQ(mock_docset.advance(), 3);
    EXPECT_EQ(mock_docset.doc(), 3);
    EXPECT_EQ(mock_docset.advance(), 5);
    EXPECT_EQ(mock_docset.doc(), 5);
    EXPECT_EQ(mock_docset.advance(), TERMINATED);
    EXPECT_EQ(mock_docset.doc(), TERMINATED);
}

TEST_F(DocSetTest, test_advance_empty) {
    std::vector<uint32_t> docs;
    MockDocSet mock_docset(docs);

    EXPECT_EQ(mock_docset.advance(), TERMINATED);
    EXPECT_EQ(mock_docset.doc(), TERMINATED);
}

TEST_F(DocSetTest, test_seek) {
    std::vector<uint32_t> docs = {1, 3, 5, 7};
    MockDocSet mock_docset(docs);

    EXPECT_EQ(mock_docset.doc(), 1);
    EXPECT_EQ(mock_docset.seek(5), 5);
    EXPECT_EQ(mock_docset.doc(), 5);
    EXPECT_EQ(mock_docset.seek(1), 5);
    EXPECT_EQ(mock_docset.doc(), 5);
}

TEST_F(DocSetTest, test_seek_empty) {
    std::vector<uint32_t> docs;
    MockDocSet mock_docset(docs);

    EXPECT_EQ(mock_docset.seek(5), TERMINATED);
    EXPECT_EQ(mock_docset.doc(), TERMINATED);
}

TEST_F(DocSetTest, test_seek_not_found) {
    std::vector<uint32_t> docs = {1, 3, 5};
    MockDocSet mock_docset(docs);

    EXPECT_EQ(mock_docset.seek(10), TERMINATED);
    EXPECT_EQ(mock_docset.doc(), TERMINATED);
}

TEST_F(DocSetTest, test_doc) {
    std::vector<uint32_t> docs = {2, 4, 6};
    MockDocSet mock_docset(docs);

    EXPECT_EQ(mock_docset.doc(), 2);
    mock_docset.next();
    EXPECT_EQ(mock_docset.doc(), 4);
    mock_docset.seek(6);
    EXPECT_EQ(mock_docset.doc(), 6);
}

TEST_F(DocSetTest, test_size_hint) {
    std::vector<uint32_t> docs = {1, 2};
    MockDocSet mock_docset(docs, 50);

    EXPECT_EQ(mock_docset.size_hint(), 50);
}

TEST_F(DocSetTest, test_norm) {
    std::vector<uint32_t> docs = {1};
    MockDocSet mock_docset(docs, 0, 99);

    EXPECT_EQ(mock_docset.norm(), 99);
}

TEST_F(DocSetTest, test_freq_terminated) {
    std::vector<uint32_t> docs;
    MockDocSet mock_docset(docs);

    EXPECT_EQ(mock_docset.freq(), 0);
}

TEST_F(DocSetTest, test_freq_with_positions) {
    std::vector<uint32_t> docs = {1, 2, 3};
    std::map<uint32_t, std::vector<uint32_t>> doc_positions;
    doc_positions[1] = {10, 20, 30};
    doc_positions[2] = {15};
    doc_positions[3] = {25, 35};

    MockDocSet mock_docset(docs, doc_positions);

    EXPECT_EQ(mock_docset.doc(), 1);
    EXPECT_EQ(mock_docset.freq(), 3);

    mock_docset.seek(2);
    EXPECT_EQ(mock_docset.freq(), 1);

    mock_docset.seek(3);
    EXPECT_EQ(mock_docset.freq(), 2);
}

TEST_F(DocSetTest, test_freq_without_positions) {
    std::vector<uint32_t> docs = {1, 2};
    std::map<uint32_t, std::vector<uint32_t>> doc_positions;
    MockDocSet mock_docset(docs, doc_positions);

    EXPECT_EQ(mock_docset.doc(), 1);
    EXPECT_EQ(mock_docset.freq(), 1);

    mock_docset.seek(2);
    EXPECT_EQ(mock_docset.freq(), 1);
}

TEST_F(DocSetTest, test_freq_partial_positions) {
    std::vector<uint32_t> docs = {1, 2, 3};
    std::map<uint32_t, std::vector<uint32_t>> doc_positions;
    doc_positions[2] = {10, 20};

    MockDocSet mock_docset(docs, doc_positions);

    EXPECT_EQ(mock_docset.doc(), 1);
    EXPECT_EQ(mock_docset.freq(), 1);

    mock_docset.seek(2);
    EXPECT_EQ(mock_docset.freq(), 2);

    mock_docset.seek(3);
    EXPECT_EQ(mock_docset.freq(), 1);
}

TEST_F(DocSetTest, test_append_positions_with_offset_terminated) {
    std::vector<uint32_t> docs;
    MockDocSet mock_docset(docs);

    std::vector<uint32_t> output;
    mock_docset.append_positions_with_offset(100, output);

    EXPECT_TRUE(output.empty());
}

TEST_F(DocSetTest, test_append_positions_with_offset_with_positions) {
    std::vector<uint32_t> docs = {1, 2};
    std::map<uint32_t, std::vector<uint32_t>> doc_positions;
    doc_positions[1] = {10, 20};
    doc_positions[2] = {30, 40, 50};

    MockDocSet mock_docset(docs, doc_positions);

    std::vector<uint32_t> output;
    output.push_back(5);

    mock_docset.append_positions_with_offset(100, output);

    EXPECT_EQ(output.size(), 3);
    EXPECT_EQ(output[0], 5);
    EXPECT_EQ(output[1], 110);
    EXPECT_EQ(output[2], 120);
}

TEST_F(DocSetTest, test_append_positions_with_offset_multiple_appends) {
    std::vector<uint32_t> docs = {1, 2};
    std::map<uint32_t, std::vector<uint32_t>> doc_positions;
    doc_positions[1] = {10};
    doc_positions[2] = {20};

    MockDocSet mock_docset(docs, doc_positions);

    std::vector<uint32_t> output;
    mock_docset.append_positions_with_offset(100, output);

    EXPECT_EQ(output.size(), 1);
    EXPECT_EQ(output[0], 110);

    mock_docset.seek(2);
    mock_docset.append_positions_with_offset(200, output);

    EXPECT_EQ(output.size(), 2);
    EXPECT_EQ(output[0], 110);
    EXPECT_EQ(output[1], 220);
}

TEST_F(DocSetTest, test_append_positions_with_offset_no_positions) {
    std::vector<uint32_t> docs = {1};
    std::map<uint32_t, std::vector<uint32_t>> doc_positions;
    MockDocSet mock_docset(docs, doc_positions);

    std::vector<uint32_t> output;
    output.push_back(5);

    mock_docset.append_positions_with_offset(100, output);

    EXPECT_EQ(output.size(), 1);
    EXPECT_EQ(output[0], 5);
}

TEST_F(DocSetTest, test_positions_with_offset) {
    std::vector<uint32_t> docs = {1};
    std::map<uint32_t, std::vector<uint32_t>> doc_positions;
    doc_positions[1] = {10, 20, 30};

    MockDocSet mock_docset(docs, doc_positions);

    std::vector<uint32_t> output;
    output.push_back(999);

    mock_docset.positions_with_offset(100, output);

    EXPECT_EQ(output.size(), 3);
    EXPECT_EQ(output[0], 110);
    EXPECT_EQ(output[1], 120);
    EXPECT_EQ(output[2], 130);
}

TEST_F(DocSetTest, test_positions_with_offset_terminated) {
    std::vector<uint32_t> docs;
    MockDocSet mock_docset(docs);

    std::vector<uint32_t> output;
    output.push_back(999);

    mock_docset.positions_with_offset(100, output);

    EXPECT_TRUE(output.empty());
}

TEST_F(DocSetTest, test_positions_with_offset_empty_positions) {
    std::vector<uint32_t> docs = {1};
    std::map<uint32_t, std::vector<uint32_t>> doc_positions;
    MockDocSet mock_docset(docs, doc_positions);

    std::vector<uint32_t> output;
    output.push_back(999);

    mock_docset.positions_with_offset(100, output);

    EXPECT_TRUE(output.empty());
}

TEST_F(DocSetTest, test_shared_ptr_types) {
    std::vector<uint32_t> docs = {1, 2, 3};
    DocSetPtr docset_ptr = std::make_shared<MockDocSet>(docs);

    EXPECT_EQ(docset_ptr->doc(), 1);
    EXPECT_EQ(docset_ptr->advance(), 2);

    MockDocSetPtr mock_ptr = std::make_shared<MockDocSet>(docs);
    EXPECT_EQ(mock_ptr->doc(), 1);
}

TEST_F(DocSetTest, test_unsorted_docs_auto_sorted) {
    std::vector<uint32_t> docs = {9, 1, 5, 3, 7};
    MockDocSet mock_docset(docs);

    EXPECT_EQ(mock_docset.doc(), 1);
    EXPECT_EQ(mock_docset.advance(), 3);
    EXPECT_EQ(mock_docset.advance(), 5);
    EXPECT_EQ(mock_docset.advance(), 7);
    EXPECT_EQ(mock_docset.advance(), 9);
}

TEST_F(DocSetTest, test_skipto_from_middle) {
    std::vector<uint32_t> docs = {1, 3, 5, 7, 9};
    MockDocSet mock_docset(docs);

    mock_docset.seek(5);
    EXPECT_EQ(mock_docset.doc(), 5);
    EXPECT_TRUE(mock_docset.skipTo(7));
    EXPECT_EQ(mock_docset.doc(), 7);
    EXPECT_FALSE(mock_docset.skipTo(10));
    EXPECT_EQ(mock_docset.doc(), TERMINATED);
}

} // namespace doris::segment_v2::inverted_index::query_v2
