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

#include "exec/runtime_filter/runtime_filter_bucket_pruner.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <set>
#include <utility>
#include <vector>

#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "exec/runtime_filter/runtime_filter_definitions.h"
#include "exprs/create_predicate_function.h"
#include "exprs/runtime_filter_expr.h"
#include "exprs/vdirect_in_predicate.h"
#include "exprs/vexpr_context.h"
#include "exprs/vslot_ref.h"

namespace doris {

class RuntimeFilterBucketPrunerTest : public testing::Test {
protected:
    static constexpr int SCAN_NODE_ID = 10;

    VExprContextSPtr make_in_conjunct(int filter_id, const std::vector<int32_t>& values) {
        std::shared_ptr<HybridSetBase> set(create_set(TYPE_INT, false));
        for (const int32_t value : values) {
            set->insert(&value);
        }

        TExprNode node;
        node.__set_type(create_type_desc(TYPE_BOOLEAN));
        node.__set_node_type(TExprNodeType::IN_PRED);
        node.in_predicate.__set_is_not_in(false);
        node.__set_opcode(TExprOpcode::FILTER_IN);
        node.__set_is_nullable(false);
        auto impl = VDirectInPredicate::create_shared(node, std::move(set), true);
        impl->add_child(VSlotRef::create_shared(/*slot_id=*/1, /*column_id=*/0,
                                                /*column_uniq_id=*/1,
                                                std::make_shared<DataTypeInt32>(), "dist_col"));
        auto wrapper = RuntimeFilterExpr::create_shared(node, impl, 0, false, filter_id);
        return std::make_shared<VExprContext>(wrapper);
    }

    VExprContextSPtr make_non_exact_conjunct(int filter_id) {
        TExprNode node;
        node.__set_type(create_type_desc(TYPE_BOOLEAN));
        node.__set_node_type(TExprNodeType::BLOOM_PRED);
        node.__set_opcode(TExprOpcode::RT_FILTER);
        node.__set_is_nullable(false);
        auto impl = VDirectInPredicate::create_shared(node, nullptr, true);
        impl->add_child(VSlotRef::create_shared(/*slot_id=*/1, /*column_id=*/0,
                                                /*column_uniq_id=*/1,
                                                std::make_shared<DataTypeInt32>(), "dist_col"));
        auto wrapper = RuntimeFilterExpr::create_shared(node, impl, 0, false, filter_id);
        return std::make_shared<VExprContext>(wrapper);
    }

    TRuntimeFilterDesc bucket_prune_desc(int filter_id) {
        TRuntimeFilterDesc desc;
        desc.__set_filter_id(filter_id);
        desc.__set_bucket_pruning_target_ids({SCAN_NODE_ID});
        return desc;
    }

    std::vector<RuntimeFilterBucketPruneRange> four_bucket_ranges() {
        std::vector<RuntimeFilterBucketPruneRange> ranges;
        for (int32_t bucket_seq = 0; bucket_seq < 4; ++bucket_seq) {
            ranges.push_back({100 + bucket_seq, bucket_seq, 4});
        }
        return ranges;
    }

    int32_t bucket_for_value(int32_t value, int32_t bucket_num) {
        auto column = ColumnInt32::create();
        column->insert_value(value);
        uint32_t hash = 0;
        column->update_crcs_with_value(&hash, TYPE_INT, 1, 0, nullptr);
        return static_cast<int32_t>(hash % static_cast<uint32_t>(bucket_num));
    }
};

TEST_F(RuntimeFilterBucketPrunerTest, ExactInKeepsOnlyMatchingBucket) {
    constexpr int filter_id = 7;
    constexpr int32_t value = 10;
    VExprContextSPtrs conjuncts {make_in_conjunct(filter_id, {value})};
    std::vector<TRuntimeFilterDesc> rf_descs {bucket_prune_desc(filter_id)};

    RuntimeFilterBucketPruner pruner;
    int64_t newly_pruned = 0;
    ASSERT_TRUE(pruner.prune_by_runtime_filters(four_bucket_ranges(), conjuncts, rf_descs,
                                                SCAN_NODE_ID, /*max_in_num=*/1024, &newly_pruned)
                        .ok());

    EXPECT_EQ(newly_pruned, 3);
    EXPECT_EQ(pruner.pruned_tablet_count(), 3);
    int32_t selected_bucket = bucket_for_value(value, 4);
    for (int32_t bucket_seq = 0; bucket_seq < 4; ++bucket_seq) {
        EXPECT_EQ(pruner.is_tablet_pruned(100 + bucket_seq), bucket_seq != selected_bucket);
    }

    ASSERT_TRUE(pruner.prune_by_runtime_filters(four_bucket_ranges(), conjuncts, rf_descs,
                                                SCAN_NODE_ID, /*max_in_num=*/1024, &newly_pruned)
                        .ok());
    EXPECT_EQ(newly_pruned, 0);
}

TEST_F(RuntimeFilterBucketPrunerTest, SupportsDifferentBucketCountsAcrossPartitions) {
    constexpr int filter_id = 11;
    constexpr int32_t value = 10;
    VExprContextSPtrs conjuncts {make_in_conjunct(filter_id, {value})};
    std::vector<TRuntimeFilterDesc> rf_descs {bucket_prune_desc(filter_id)};
    std::vector<RuntimeFilterBucketPruneRange> ranges;
    for (int32_t bucket_seq = 0; bucket_seq < 4; ++bucket_seq) {
        ranges.push_back({100 + bucket_seq, bucket_seq, 4});
    }
    for (int32_t bucket_seq = 0; bucket_seq < 7; ++bucket_seq) {
        ranges.push_back({200 + bucket_seq, bucket_seq, 7});
    }

    RuntimeFilterBucketPruner pruner;
    int64_t newly_pruned = 0;
    ASSERT_TRUE(pruner.prune_by_runtime_filters(ranges, conjuncts, rf_descs, SCAN_NODE_ID,
                                                /*max_in_num=*/1024, &newly_pruned)
                        .ok());

    EXPECT_EQ(newly_pruned, 9);
    EXPECT_FALSE(pruner.is_tablet_pruned(100 + bucket_for_value(value, 4)));
    EXPECT_FALSE(pruner.is_tablet_pruned(200 + bucket_for_value(value, 7)));
}

TEST_F(RuntimeFilterBucketPrunerTest, EmptyExactInPrunesAllBuckets) {
    constexpr int filter_id = 8;
    VExprContextSPtrs conjuncts {make_in_conjunct(filter_id, {})};
    std::vector<TRuntimeFilterDesc> rf_descs {bucket_prune_desc(filter_id)};

    RuntimeFilterBucketPruner pruner;
    int64_t newly_pruned = 0;
    ASSERT_TRUE(pruner.prune_by_runtime_filters(four_bucket_ranges(), conjuncts, rf_descs,
                                                SCAN_NODE_ID, /*max_in_num=*/1024, &newly_pruned)
                        .ok());
    EXPECT_EQ(newly_pruned, 4);
    EXPECT_EQ(pruner.pruned_tablet_count(), 4);
}

TEST_F(RuntimeFilterBucketPrunerTest, NonExactRuntimeRepresentationIsIgnored) {
    constexpr int filter_id = 9;
    VExprContextSPtrs conjuncts {make_non_exact_conjunct(filter_id)};
    std::vector<TRuntimeFilterDesc> rf_descs {bucket_prune_desc(filter_id)};

    RuntimeFilterBucketPruner pruner;
    int64_t newly_pruned = 0;
    ASSERT_TRUE(pruner.prune_by_runtime_filters(four_bucket_ranges(), conjuncts, rf_descs,
                                                SCAN_NODE_ID, /*max_in_num=*/1024, &newly_pruned)
                        .ok());
    EXPECT_EQ(newly_pruned, 0);
    EXPECT_EQ(pruner.pruned_tablet_count(), 0);
}

TEST_F(RuntimeFilterBucketPrunerTest, DescriptorMustMarkScanAsEligible) {
    constexpr int filter_id = 10;
    VExprContextSPtrs conjuncts {make_in_conjunct(filter_id, {10})};
    TRuntimeFilterDesc desc;
    desc.__set_filter_id(filter_id);

    RuntimeFilterBucketPruner pruner;
    int64_t newly_pruned = 0;
    ASSERT_TRUE(pruner.prune_by_runtime_filters(four_bucket_ranges(), conjuncts, {desc},
                                                SCAN_NODE_ID, /*max_in_num=*/1024, &newly_pruned)
                        .ok());
    EXPECT_EQ(newly_pruned, 0);
    EXPECT_EQ(pruner.pruned_tablet_count(), 0);
}

} // namespace doris
