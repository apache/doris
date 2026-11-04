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

#include <gen_cpp/PlanNodes_types.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <numeric>
#include <set>
#include <utility>
#include <vector>

#include "core/column/column_vector.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "exec/runtime_filter/runtime_filter_definitions.h"
#include "exec/runtime_filter/runtime_filter_wrapper.h"
#include "exprs/create_predicate_function.h"
#include "exprs/hybrid_set.h"
#include "exprs/runtime_filter_expr.h"
#include "exprs/vdirect_in_predicate.h"
#include "exprs/vexpr_context.h"
#include "exprs/vslot_ref.h"
#include "util/hash_util.hpp"
#include "util/raw_value.h"

namespace doris {

class RuntimeFilterBucketPrunerTest : public testing::Test {
protected:
    static constexpr int SCAN_NODE_ID = 10;
    using BucketPruneRanges = std::vector<std::unique_ptr<TPaloScanRange>>;

    std::shared_ptr<RuntimeFilterWrapper> make_in_wrapper(int filter_id,
                                                          const std::vector<int32_t>& values,
                                                          bool null_aware = false,
                                                          int max_in_num = 1024) {
        RuntimeFilterParams params {.filter_id = filter_id,
                                    .filter_type = RuntimeFilterType::IN_FILTER,
                                    .column_return_type = TYPE_INT,
                                    .null_aware = null_aware,
                                    .max_in_num = max_in_num};
        auto wrapper = std::make_shared<RuntimeFilterWrapper>(&params);
        for (const int32_t value : values) {
            wrapper->hybrid_set()->insert(&value);
        }
        if (null_aware) {
            wrapper->hybrid_set()->insert(static_cast<const void*>(nullptr));
        }
        wrapper->set_state(RuntimeFilterWrapper::State::READY);
        return wrapper;
    }

    VExprContextSPtr make_in_conjunct(
            int filter_id, const std::vector<int32_t>& values,
            std::shared_ptr<RuntimeFilterWrapper> runtime_filter_wrapper = nullptr) {
        if (runtime_filter_wrapper == nullptr) {
            runtime_filter_wrapper = make_in_wrapper(filter_id, values);
        }

        TExprNode node;
        node.__set_type(create_type_desc(TYPE_BOOLEAN));
        node.__set_node_type(TExprNodeType::IN_PRED);
        node.in_predicate.__set_is_not_in(false);
        node.__set_opcode(TExprOpcode::FILTER_IN);
        node.__set_is_nullable(false);
        auto impl =
                VDirectInPredicate::create_shared(node, runtime_filter_wrapper->hybrid_set(), true);
        impl->add_child(VSlotRef::create_shared(/*slot_id=*/1, /*column_id=*/0,
                                                /*column_uniq_id=*/1,
                                                std::make_shared<DataTypeInt32>(), "dist_col"));
        auto wrapper = RuntimeFilterExpr::create_shared(node, impl, 0, false, filter_id,
                                                        RuntimeFilterSelectivity::DISABLE_SAMPLING,
                                                        std::move(runtime_filter_wrapper));
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

    VExprContextSPtr make_null_aware_in_conjunct(int filter_id) {
        auto runtime_filter_wrapper = make_in_wrapper(filter_id, {}, true);

        TExprNode node;
        node.__set_type(create_type_desc(TYPE_BOOLEAN));
        node.__set_node_type(TExprNodeType::NULL_AWARE_IN_PRED);
        node.in_predicate.__set_is_not_in(false);
        node.__set_opcode(TExprOpcode::FILTER_IN);
        node.__set_is_nullable(false);
        auto impl =
                VDirectInPredicate::create_shared(node, runtime_filter_wrapper->hybrid_set(), true);
        impl->add_child(VSlotRef::create_shared(
                /*slot_id=*/1, /*column_id=*/0, /*column_uniq_id=*/1,
                std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()), "dist_col"));
        auto wrapper = RuntimeFilterExpr::create_shared(node, impl, 0, false, filter_id,
                                                        RuntimeFilterSelectivity::DISABLE_SAMPLING,
                                                        std::move(runtime_filter_wrapper));
        return std::make_shared<VExprContext>(wrapper);
    }

    TRuntimeFilterDesc bucket_prune_desc(
            int filter_id, TDistributionHashType::type hash_type = TDistributionHashType::CRC32) {
        TRuntimeFilterDesc desc;
        desc.__set_filter_id(filter_id);
        desc.__set_bucket_pruning_target_ids({SCAN_NODE_ID});
        desc.__set_bucket_pruning_target_hash_types({{SCAN_NODE_ID, hash_type}});
        return desc;
    }

    void add_range(BucketPruneRanges* ranges, int64_t tablet_id, int32_t bucket_seq,
                   int32_t bucket_num) {
        auto range = std::make_unique<TPaloScanRange>();
        range->__set_tablet_id(tablet_id);
        range->__set_bucket_seq(bucket_seq);
        range->__set_bucket_num(bucket_num);
        ranges->push_back(std::move(range));
    }

    BucketPruneRanges four_bucket_ranges() {
        BucketPruneRanges ranges;
        for (int32_t bucket_seq = 0; bucket_seq < 4; ++bucket_seq) {
            add_range(&ranges, 100 + bucket_seq, bucket_seq, 4);
        }
        return ranges;
    }

    int32_t bucket_for_value(int32_t value, int32_t bucket_num) {
        uint32_t hash = RawValue::zlib_crc32(&value, sizeof(value), TYPE_INT, 0);
        return static_cast<int32_t>(hash % static_cast<uint32_t>(bucket_num));
    }

    int32_t bucket_for_null(int32_t bucket_num) {
        uint32_t hash = HashUtil::zlib_crc_hash_null(0);
        return static_cast<int32_t>(hash % static_cast<uint32_t>(bucket_num));
    }
};

TEST_F(RuntimeFilterBucketPrunerTest, ExactSetHashesSharedAcrossConsumers) {
    constexpr int filter_id = 13;
    auto runtime_filter_wrapper = make_in_wrapper(filter_id, {1, 2, 3}, true);
    auto first = make_in_conjunct(filter_id, {}, runtime_filter_wrapper);
    auto second = make_in_conjunct(filter_id, {}, runtime_filter_wrapper);
    auto target_type = first->root()->get_impl()->children()[0]->data_type();

    auto first_hashes =
            assert_cast<RuntimeFilterExpr*>(first->root().get())
                    ->get_bucket_prune_hashes(target_type, TDistributionHashType::CRC32, 8);
    auto second_hashes =
            assert_cast<RuntimeFilterExpr*>(second->root().get())
                    ->get_bucket_prune_hashes(target_type, TDistributionHashType::CRC32, 8);
    auto nullable_hashes =
            assert_cast<RuntimeFilterExpr*>(first->root().get())
                    ->get_bucket_prune_hashes(
                            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()),
                            TDistributionHashType::CRC32, 8);

    EXPECT_EQ(first_hashes.get(), second_hashes.get());
    EXPECT_EQ(first_hashes.get(), nullable_hashes.get());
    EXPECT_EQ(first_hashes.get(), runtime_filter_wrapper
                                          ->get_or_compute_bucket_prune_hashes(
                                                  target_type, TDistributionHashType::CRC32, 97)
                                          .get());
    ASSERT_EQ(first_hashes->size(), 4);
    EXPECT_EQ(first_hashes->back(), HashUtil::zlib_crc_hash_null(0));
}

TEST_F(RuntimeFilterBucketPrunerTest, RejectsMergeAfterBucketHashesStart) {
    constexpr int filter_id = 15;
    auto wrapper = make_in_wrapper(filter_id, {1});
    auto other = make_in_wrapper(filter_id, {2});

    static_cast<void>(wrapper->get_or_compute_bucket_prune_hashes(std::make_shared<DataTypeInt32>(),
                                                                  TDistributionHashType::CRC32, 8));

    EXPECT_DEATH({ static_cast<void>(wrapper->merge(other.get())); }, "Check failed");
}

TEST_F(RuntimeFilterBucketPrunerTest, IdentityExactInKeepsIdentityBucket) {
    constexpr int filter_id = 17;
    // value 2 separates the algorithms on 4 buckets: crc32(2) % 4 == 3 while 2 % 4 == 2, so
    // this case fails if the pruning silently fell back to CRC32 (the previous value 1 gave
    // crc32(1) % 4 == 1 == 1 % 4 and could not tell the two apart).
    constexpr int32_t value = 2;
    VExprContextSPtrs conjuncts {make_in_conjunct(filter_id, {value})};
    std::vector<TRuntimeFilterDesc> rf_descs {
            bucket_prune_desc(filter_id, TDistributionHashType::IDENTITY)};

    RuntimeFilterBucketPruner pruner;
    int64_t newly_pruned = 0;
    ASSERT_TRUE(pruner.prune_by_runtime_filters(four_bucket_ranges(), conjuncts, rf_descs,
                                                SCAN_NODE_ID, 1024, &newly_pruned)
                        .ok());
    EXPECT_EQ(newly_pruned, 3);
    for (int32_t bucket_seq = 0; bucket_seq < 4; ++bucket_seq) {
        EXPECT_EQ(pruner.is_bucket_pruned(bucket_seq, 4), bucket_seq != value % 4);
    }
}

TEST_F(RuntimeFilterBucketPrunerTest, IdentityExactInSeparatesFromCrc32AcrossCounts) {
    constexpr int filter_id = 18;
    constexpr int32_t value = 10;
    VExprContextSPtrs conjuncts {make_in_conjunct(filter_id, {value})};

    // For every bucket count, the IDENTITY descriptor must keep exactly value % n while the
    // CRC32 fallback would keep zlib_crc32(value) % n; the two must disagree for at least
    // one count so a regression to CRC32 cannot pass unnoticed.
    int disagreements = 0;
    for (int32_t bucket_num : {4, 5, 8, 97, 257}) {
        BucketPruneRanges ranges;
        for (int32_t bucket_seq = 0; bucket_seq < bucket_num; ++bucket_seq) {
            add_range(&ranges, ranges.size(), bucket_seq, bucket_num);
        }
        std::vector<TRuntimeFilterDesc> identity_descs {
                bucket_prune_desc(filter_id, TDistributionHashType::IDENTITY)};
        RuntimeFilterBucketPruner identity_pruner;
        int64_t newly_pruned = 0;
        ASSERT_TRUE(identity_pruner
                            .prune_by_runtime_filters(ranges, conjuncts, identity_descs,
                                                      SCAN_NODE_ID, 1024, &newly_pruned)
                            .ok());
        EXPECT_EQ(newly_pruned, bucket_num - 1);
        for (int32_t bucket_seq = 0; bucket_seq < bucket_num; ++bucket_seq) {
            EXPECT_EQ(identity_pruner.is_bucket_pruned(bucket_seq, bucket_num),
                      bucket_seq != value % bucket_num);
        }
        if (bucket_for_value(value, bucket_num) != value % bucket_num) {
            ++disagreements;
        }
    }
    EXPECT_GE(disagreements, 1);
}

// Count values actually visited, so the full-coverage shortcut is checked without timing tests.
class CountingIntSet : public HybridSet<TYPE_INT> {
public:
    CountingIntSet() : HybridSet<TYPE_INT>(false) {}

    class CountingIterator : public IteratorBase {
    public:
        CountingIterator(IteratorBase* inner, size_t& visited) : _inner(inner), _visited(visited) {}
        const void* get_value() override {
            ++_visited;
            return _inner->get_value();
        }
        bool has_next() const override { return _inner->has_next(); }
        void next() override { _inner->next(); }

    private:
        IteratorBase* _inner;
        size_t& _visited;
    };

    IteratorBase* begin() override {
        _iterator =
                std::make_unique<CountingIterator>(HybridSet<TYPE_INT>::begin(), values_visited);
        return _iterator.get();
    }

    size_t values_visited = 0;

private:
    std::unique_ptr<CountingIterator> _iterator;
};

TEST_F(RuntimeFilterBucketPrunerTest, IdentityCacheStopsAfterFullCoverage) {
    auto wrapper = make_in_wrapper(17, {});
    auto values = std::make_shared<CountingIntSet>();
    for (int32_t value = 0; value < 1024; ++value) {
        values->insert(&value);
    }
    wrapper->_hybrid_set = values;
    auto buckets = wrapper->get_or_compute_bucket_prune_hashes(std::make_shared<DataTypeInt32>(),
                                                               TDistributionHashType::IDENTITY, 1);
    ASSERT_EQ(buckets->size(), 1);
    EXPECT_EQ(buckets->front(), 0U);
    EXPECT_EQ(values->values_visited, 1);
    EXPECT_EQ(values->size(), 1024);
}

TEST_F(RuntimeFilterBucketPrunerTest, IdentityCacheDeduplicatesAndSharesBuckets) {
    auto wrapper = make_in_wrapper(17, {0, 4, 8, 12, -4}, true);
    auto first = make_in_conjunct(17, {}, wrapper);
    auto second = make_in_conjunct(17, {}, wrapper);
    auto target_type = std::make_shared<DataTypeInt32>();
    auto buckets =
            assert_cast<RuntimeFilterExpr*>(first->root().get())
                    ->get_bucket_prune_hashes(target_type, TDistributionHashType::IDENTITY, 4);
    // Every non-null value and NULL select the same bucket; retain it only once.
    ASSERT_EQ(buckets->size(), 1);
    EXPECT_EQ(buckets->front(), 0U);
    EXPECT_EQ(buckets.get(),
              assert_cast<RuntimeFilterExpr*>(second->root().get())
                      ->get_bucket_prune_hashes(target_type, TDistributionHashType::IDENTITY, 4)
                      .get());
    EXPECT_EQ(buckets.get(), wrapper->get_or_compute_bucket_prune_hashes(
                                            std::make_shared<DataTypeNullable>(target_type),
                                            TDistributionHashType::IDENTITY, 4)
                                     .get());
}

TEST_F(RuntimeFilterBucketPrunerTest, IdentityCacheHandlesEmptyAndNullOnlySets) {
    auto target_type = std::make_shared<DataTypeInt32>();
    for (bool null_aware : {false, true}) {
        auto wrapper = make_in_wrapper(17, {}, null_aware);
        for (uint32_t bucket_num : {1U, 7U, 768U}) {
            auto buckets = wrapper->get_or_compute_bucket_prune_hashes(
                    target_type, TDistributionHashType::IDENTITY, bucket_num);
            if (null_aware) {
                ASSERT_EQ(buckets->size(), 1);
                EXPECT_EQ(buckets->front(), 0U);
            } else {
                EXPECT_TRUE(buckets->empty());
            }
        }
    }
}

TEST_F(RuntimeFilterBucketPrunerTest, IdentityCacheHandlesLargeBucketCount) {
    auto wrapper = make_in_wrapper(17, {-1, 0, 1}, true);
    auto buckets = wrapper->get_or_compute_bucket_prune_hashes(
            std::make_shared<DataTypeInt32>(), TDistributionHashType::IDENTITY,
            std::numeric_limits<uint32_t>::max());
    // Scratch space must also be bounded by the set size, not by this huge bucket count.
    const std::set<uint32_t> expected {0, 1};
    EXPECT_EQ(std::set<uint32_t>(buckets->begin(), buckets->end()), expected);
    EXPECT_LE(buckets->capacity(), expected.size());
}

TEST_F(RuntimeFilterBucketPrunerTest, IdentityCacheRetainsOnlyBucketsAcrossManyCounts) {
    constexpr int value_count = 40960;
    constexpr uint32_t max_bucket_num = 768;
    std::vector<int32_t> values(value_count);
    std::iota(values.begin(), values.end(), 0);
    auto wrapper = make_in_wrapper(17, values, true, value_count);
    auto target_type = std::make_shared<DataTypeInt32>();
    size_t retained_capacity = 0;
    for (uint32_t bucket_num = 1; bucket_num <= max_bucket_num; ++bucket_num) {
        SCOPED_TRACE(bucket_num);
        auto buckets = wrapper->get_or_compute_bucket_prune_hashes(
                target_type, TDistributionHashType::IDENTITY, bucket_num);
        // Contiguous values cover every bucket. The old cache retained value_count + 1
        // entries per count (~120 MiB); duplicates must not survive in size OR capacity.
        ASSERT_EQ(buckets->size(), bucket_num);
        EXPECT_LE(buckets->capacity(), bucket_num);
        EXPECT_EQ(std::set<uint32_t>(buckets->begin(), buckets->end()).size(), bucket_num);
        for (uint32_t bucket : *buckets) {
            EXPECT_LT(bucket, bucket_num);
        }
        retained_capacity += buckets->capacity();
        EXPECT_EQ(buckets.get(),
                  wrapper->get_or_compute_bucket_prune_hashes(
                                 target_type, TDistributionHashType::IDENTITY, bucket_num)
                          .get());
    }
    EXPECT_LE(retained_capacity, max_bucket_num * (max_bucket_num + 1) / 2);
    EXPECT_EQ(wrapper->hybrid_set()->size(), value_count);
    EXPECT_TRUE(wrapper->hybrid_set()->contain_null());
}

TEST_F(RuntimeFilterBucketPrunerTest, IdentitySparseBucketsRemainCorrectAcrossCounts) {
    constexpr int filter_id = 17;
    const std::vector<int32_t> values {-1, 0, 4, 8, 12};
    auto wrapper = make_in_wrapper(filter_id, values, true);
    VExprContextSPtrs conjuncts {make_in_conjunct(filter_id, {}, wrapper)};
    std::vector<TRuntimeFilterDesc> rf_descs {
            bucket_prune_desc(filter_id, TDistributionHashType::IDENTITY)};
    BucketPruneRanges ranges;
    std::map<int32_t, std::set<uint32_t>> expected_by_num;
    int64_t expected_pruned = 0;
    for (int32_t bucket_num : {4, 7, 97}) {
        auto& expected = expected_by_num[bucket_num];
        expected.insert(0); // NULL's canonical bytes select bucket zero.
        for (int32_t value : values) {
            expected.insert(static_cast<uint32_t>(value) % static_cast<uint32_t>(bucket_num));
        }
        expected_pruned += bucket_num - static_cast<int64_t>(expected.size());
        for (int32_t bucket = 0; bucket < bucket_num; ++bucket) {
            add_range(&ranges, ranges.size(), bucket, bucket_num);
        }
    }
    RuntimeFilterBucketPruner pruner;
    int64_t newly_pruned = 0;
    ASSERT_TRUE(pruner.prune_by_runtime_filters(ranges, conjuncts, rf_descs, SCAN_NODE_ID, 1024,
                                                &newly_pruned)
                        .ok());
    EXPECT_EQ(newly_pruned, expected_pruned);
    for (const auto& [bucket_num, expected] : expected_by_num) {
        auto buckets = wrapper->get_or_compute_bucket_prune_hashes(
                std::make_shared<DataTypeInt32>(), TDistributionHashType::IDENTITY, bucket_num);
        EXPECT_EQ(std::set<uint32_t>(buckets->begin(), buckets->end()), expected);
        EXPECT_EQ(buckets->size(), expected.size());
        for (int32_t bucket = 0; bucket < bucket_num; ++bucket) {
            EXPECT_EQ(pruner.is_bucket_pruned(bucket, bucket_num), !expected.contains(bucket));
        }
    }
}

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
        EXPECT_EQ(pruner.is_bucket_pruned(bucket_seq, 4), bucket_seq != selected_bucket);
    }

    ASSERT_TRUE(pruner.prune_by_runtime_filters(four_bucket_ranges(), conjuncts, rf_descs,
                                                SCAN_NODE_ID, /*max_in_num=*/1024, &newly_pruned)
                        .ok());
    EXPECT_EQ(newly_pruned, 0);
}

TEST_F(RuntimeFilterBucketPrunerTest, MultipleFiltersCountEachPrunedBucketOnce) {
    constexpr int first_filter_id = 17;
    constexpr int second_filter_id = 18;
    constexpr int32_t first_value = 10;
    int32_t second_value = first_value + 1;
    while (bucket_for_value(second_value, 4) == bucket_for_value(first_value, 4)) {
        ++second_value;
    }
    VExprContextSPtrs conjuncts {make_in_conjunct(first_filter_id, {first_value}),
                                 make_in_conjunct(second_filter_id, {second_value})};
    std::vector<TRuntimeFilterDesc> rf_descs {bucket_prune_desc(first_filter_id),
                                              bucket_prune_desc(second_filter_id)};

    RuntimeFilterBucketPruner pruner;
    int64_t newly_pruned = 0;
    ASSERT_TRUE(pruner.prune_by_runtime_filters(four_bucket_ranges(), conjuncts, rf_descs,
                                                SCAN_NODE_ID, /*max_in_num=*/1024, &newly_pruned)
                        .ok());

    EXPECT_EQ(newly_pruned, 4);
    EXPECT_EQ(pruner.pruned_tablet_count(), 4);
}

TEST_F(RuntimeFilterBucketPrunerTest, NonNullableTargetConservativelyKeepsNullBucket) {
    constexpr int filter_id = 14;
    constexpr int32_t value = 1;
    auto runtime_filter_wrapper = make_in_wrapper(filter_id, {value}, true);
    VExprContextSPtrs conjuncts {
            make_in_conjunct(filter_id, {}, std::move(runtime_filter_wrapper))};
    std::vector<TRuntimeFilterDesc> rf_descs {bucket_prune_desc(filter_id)};

    std::set<int32_t> selected_buckets {bucket_for_value(value, 4), bucket_for_null(4)};
    ASSERT_EQ(selected_buckets.size(), 2);

    RuntimeFilterBucketPruner pruner;
    int64_t newly_pruned = 0;
    ASSERT_TRUE(pruner.prune_by_runtime_filters(four_bucket_ranges(), conjuncts, rf_descs,
                                                SCAN_NODE_ID, /*max_in_num=*/1024, &newly_pruned)
                        .ok());

    EXPECT_EQ(newly_pruned, 2);
    for (int32_t bucket_seq = 0; bucket_seq < 4; ++bucket_seq) {
        EXPECT_EQ(pruner.is_bucket_pruned(bucket_seq, 4), !selected_buckets.contains(bucket_seq));
    }
}

TEST_F(RuntimeFilterBucketPrunerTest, SupportsDifferentBucketCountsAcrossPartitions) {
    constexpr int filter_id = 11;
    constexpr int32_t value = 10;
    VExprContextSPtrs conjuncts {make_in_conjunct(filter_id, {value})};
    std::vector<TRuntimeFilterDesc> rf_descs {bucket_prune_desc(filter_id)};
    BucketPruneRanges ranges;
    for (int32_t bucket_seq = 0; bucket_seq < 4; ++bucket_seq) {
        add_range(&ranges, 100 + bucket_seq, bucket_seq, 4);
    }
    for (int32_t bucket_seq = 0; bucket_seq < 7; ++bucket_seq) {
        add_range(&ranges, 200 + bucket_seq, bucket_seq, 7);
    }

    RuntimeFilterBucketPruner pruner;
    int64_t newly_pruned = 0;
    ASSERT_TRUE(pruner.prune_by_runtime_filters(ranges, conjuncts, rf_descs, SCAN_NODE_ID,
                                                /*max_in_num=*/1024, &newly_pruned)
                        .ok());

    EXPECT_EQ(newly_pruned, 9);
    EXPECT_FALSE(pruner.is_bucket_pruned(bucket_for_value(value, 4), 4));
    EXPECT_FALSE(pruner.is_bucket_pruned(bucket_for_value(value, 7), 7));
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

TEST_F(RuntimeFilterBucketPrunerTest, NullAwareInKeepsNullBucket) {
    constexpr int filter_id = 12;
    VExprContextSPtrs conjuncts {make_null_aware_in_conjunct(filter_id)};
    std::vector<TRuntimeFilterDesc> rf_descs {bucket_prune_desc(filter_id)};

    RuntimeFilterBucketPruner pruner;
    int64_t newly_pruned = 0;
    ASSERT_TRUE(pruner.prune_by_runtime_filters(four_bucket_ranges(), conjuncts, rf_descs,
                                                SCAN_NODE_ID, /*max_in_num=*/1024, &newly_pruned)
                        .ok());

    EXPECT_EQ(newly_pruned, 3);
    EXPECT_EQ(pruner.pruned_tablet_count(), 3);
    int32_t null_bucket = bucket_for_null(4);
    for (int32_t bucket_seq = 0; bucket_seq < 4; ++bucket_seq) {
        EXPECT_EQ(pruner.is_bucket_pruned(bucket_seq, 4), bucket_seq != null_bucket);
    }
}

TEST_F(RuntimeFilterBucketPrunerTest, HighRangeCountRetainsBucketState) {
    constexpr int filter_id = 16;
    constexpr int32_t value = 10;
    constexpr int32_t partition_count = 128;
    constexpr int32_t bucket_num = 256;
    BucketPruneRanges ranges;
    ranges.reserve(partition_count * bucket_num);
    for (int32_t partition = 0; partition < partition_count; ++partition) {
        for (int32_t bucket_seq = 0; bucket_seq < bucket_num; ++bucket_seq) {
            add_range(&ranges, static_cast<int64_t>(partition) * bucket_num + bucket_seq,
                      bucket_seq, bucket_num);
        }
    }
    VExprContextSPtrs conjuncts {make_in_conjunct(filter_id, {value})};
    std::vector<TRuntimeFilterDesc> rf_descs {bucket_prune_desc(filter_id)};

    RuntimeFilterBucketPruner pruner;
    int64_t newly_pruned = 0;
    ASSERT_TRUE(pruner.prune_by_runtime_filters(ranges, conjuncts, rf_descs, SCAN_NODE_ID,
                                                /*max_in_num=*/1024, &newly_pruned)
                        .ok());

    EXPECT_EQ(newly_pruned, static_cast<int64_t>(partition_count) * (bucket_num - 1));
    EXPECT_EQ(pruner.pruned_tablet_count(), newly_pruned);
    EXPECT_FALSE(pruner.is_bucket_pruned(bucket_for_value(value, bucket_num), bucket_num));
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
