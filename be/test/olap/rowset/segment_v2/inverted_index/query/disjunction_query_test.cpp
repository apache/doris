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

#include "olap/rowset/segment_v2/inverted_index/query/disjunction_query.h"

#include <CLucene.h>
#include <gtest/gtest.h>

#include <map>
#include <memory>
#include <roaring/roaring.hh>
#include <vector>

#include "gen_cpp/PaloInternalService_types.h"
#include "olap/rowset/segment_v2/index_query_context.h"
#include "olap/rowset/segment_v2/inverted_index/util/mock_iterator.h"
#include "runtime/runtime_state.h"

namespace doris::segment_v2 {

class DisjunctionQueryTest : public testing::Test {
public:
    void SetUp() override {
        TQueryGlobals query_globals;

        TQueryOptions query_options;
        query_options.inverted_index_conjunction_opt_threshold = 1000;

        _runtime_state = std::make_shared<RuntimeState>(query_globals);
        _runtime_state->set_query_options(query_options);

        _context = std::make_shared<IndexQueryContext>();
        _context->runtime_state = _runtime_state.get();
        _context->collection_similarity = nullptr;

        _searcher = nullptr;
    }

    void TearDown() override {}

protected:
    IndexQueryContextPtr _context;
    SearcherPtr _searcher;
    std::shared_ptr<RuntimeState> _runtime_state;
};

TEST_F(DisjunctionQueryTest, AddMethodTest) {
    auto query = std::make_unique<DisjunctionQuery>(_searcher, _context);

    InvertedIndexQueryInfo multi_term_query_info;
    multi_term_query_info.field_name = L"test_field";
    multi_term_query_info.use_mock_iter = true;

    TermInfo term_info1;
    term_info1.term = "test_term1";
    term_info1.position = 0;

    TermInfo term_info2;
    term_info2.term = "test_term2";
    term_info2.position = 1;

    multi_term_query_info.term_infos.push_back(term_info1);
    multi_term_query_info.term_infos.push_back(term_info2);

    EXPECT_NO_THROW(query->add(multi_term_query_info));

    EXPECT_EQ(query->_iterators.size(), 2);

    EXPECT_EQ(query->_field_name, L"test_field");

    InvertedIndexQueryInfo empty_query_info;
    empty_query_info.field_name = L"test_field";

    EXPECT_THROW(query->add(empty_query_info), Exception);
}

TEST_F(DisjunctionQueryTest, SearchMethodTest) {
    auto query = std::make_unique<DisjunctionQuery>(_searcher, _context);

    auto iter1 = std::make_shared<MockIterator>();
    auto iter2 = std::make_shared<MockIterator>();

    iter1->set_postings({{0, {0}}, {1, {0}}, {3, {0}}, {5, {0}}});
    iter2->set_postings({{2, {0}}, {3, {0}}, {4, {0}}, {5, {0}}});

    query->_iterators.resize(2);
    query->_iterators[0].push_back(iter1);
    query->_iterators[1].push_back(iter2);

    roaring::Roaring result;
    query->search(result);

    EXPECT_TRUE(result.contains(0));
    EXPECT_TRUE(result.contains(1));
    EXPECT_TRUE(result.contains(2));
    EXPECT_TRUE(result.contains(3));
    EXPECT_TRUE(result.contains(4));
    EXPECT_TRUE(result.contains(5));

    EXPECT_EQ(result.cardinality(), 6);
}

} // namespace doris::segment_v2