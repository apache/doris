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

// Use #define private public to access private members for white-box testing
// of SegmentIterator::_can_opt_limit_reads() and its dependent state. Mirrors
// the convention in segment_iterator_apply_index_expr_test.cpp.
#include "gtest/gtest.h"
#include "storage/olap_common.h"
#include "storage/predicate/block_column_predicate.h"
#include "storage/predicate/null_predicate.h"
#include "storage/tablet/tablet_schema.h"

#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wkeyword-macro"
#endif
#define private public
#define protected public
#include "storage/segment/segment_iterator.h"
#undef private
#undef protected
#if defined(__clang__)
#pragma clang diagnostic pop
#endif

namespace doris::segment_v2 {

class SegmentIteratorLimitOptTest : public ::testing::Test {
protected:
    void SetUp() override {
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(KeysType::DUP_KEYS);
        auto* col = schema_pb.add_column();
        col->set_unique_id(0);
        col->set_name("k1");
        col->set_type("INT");
        col->set_is_key(true);
        col->set_is_nullable(true);

        _tablet_schema = std::make_shared<TabletSchema>();
        _tablet_schema->init_from_pb(schema_pb);
        _read_schema = std::make_shared<Schema>(_tablet_schema);
    }

    // Build a SegmentIterator with minimal opts for _can_opt_limit_reads() testing.
    // The segment pointer is null — only _opts and internal maps are accessed.
    std::unique_ptr<SegmentIterator> make_iter() {
        auto iter = std::make_unique<SegmentIterator>(nullptr, _read_schema);
        iter->_opts.tablet_schema = _tablet_schema;
        iter->_opts.stats = &_stats;
        // delete_condition_predicates is default-constructed (empty)
        return iter;
    }

    std::shared_ptr<TabletSchema> _tablet_schema;
    SchemaSPtr _read_schema;
    OlapReaderStatistics _stats;
};

// No limit at all → optimization disabled.
TEST_F(SegmentIteratorLimitOptTest, no_limit_returns_false) {
    auto iter = make_iter();
    iter->_opts.read_limit = 0;
    EXPECT_FALSE(iter->_can_opt_limit_reads());
}

// topn_limit > 0, no predicates, no delete conditions → should return true.
// All columns pass the index check vacuously because
// _column_predicate_index_exec_status is empty and default_return=true.
TEST_F(SegmentIteratorLimitOptTest, topn_limit_no_predicates) {
    auto iter = make_iter();
    iter->_opts.read_limit = 100;
    EXPECT_TRUE(iter->_can_opt_limit_reads());
}

// Has delete condition predicates → should return false even with limit set.
TEST_F(SegmentIteratorLimitOptTest, delete_predicates_returns_false) {
    auto iter = make_iter();
    iter->_opts.read_limit = 100;

    // Add a column predicate to delete_condition_predicates so
    // num_of_column_predicate() > 0.
    auto null_pred = NullPredicate::create_shared(0, "k1", true, PrimitiveType::TYPE_INT);
    auto single = SingleColumnBlockPredicate::create_unique(null_pred);
    iter->_opts.delete_condition_predicates->add_column_predicate(std::move(single));

    EXPECT_FALSE(iter->_can_opt_limit_reads());
}

// Column has a predicate that did NOT pass inverted index → should return false.
TEST_F(SegmentIteratorLimitOptTest, column_predicate_not_passed_index) {
    auto iter = make_iter();
    iter->_opts.read_limit = 100;

    auto pred = NullPredicate::create_shared(0, "k1", true, PrimitiveType::TYPE_INT);
    iter->_column_predicate_index_exec_status[0][pred] = false;

    EXPECT_FALSE(iter->_can_opt_limit_reads());
}

// Column has a predicate that passed inverted index → should return true.
TEST_F(SegmentIteratorLimitOptTest, column_predicate_passed_index) {
    auto iter = make_iter();
    iter->_opts.read_limit = 100;

    auto pred = NullPredicate::create_shared(0, "k1", true, PrimitiveType::TYPE_INT);
    iter->_column_predicate_index_exec_status[0][pred] = true;

    EXPECT_TRUE(iter->_can_opt_limit_reads());
}

} // namespace doris::segment_v2
