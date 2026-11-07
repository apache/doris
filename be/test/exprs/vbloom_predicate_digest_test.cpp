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

#include <butil/iobuf.h>
#include <gen_cpp/Exprs_types.h>
#include <gtest/gtest.h>

#include <array>
#include <barrier>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <thread>
#include <utility>
#include <vector>

#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "exec/runtime_filter/runtime_filter_definitions.h"
#include "exprs/bloom_filter_func.h"
#include "exprs/hybrid_set.h"
#include "exprs/vbloom_predicate.h"
#include "exprs/vslot_ref.h"
#include "runtime/thread_context.h"
#include "util/hash_util.hpp"

namespace doris {
namespace {

void insert_value(BloomFilterFuncBase& filter, int32_t value) {
    auto values = ColumnInt32::create();
    values->insert_value(value);
    ColumnPtr column = std::move(values);
    filter.insert_fixed_len(column, 0);
}

std::shared_ptr<BloomFilterFuncBase> make_filter(int32_t value, size_t bytes = 1024) {
    auto filter = std::make_shared<BloomFilterFunc<TYPE_INT>>(false);
    RuntimeFilterParams params;
    params.bloom_filter_size = bytes;
    filter->init_params(&params);
    EXPECT_TRUE(filter->init_with_fixed_length(0).ok());
    insert_value(*filter, value);
    return filter;
}

std::shared_ptr<VBloomPredicate> make_predicate(std::shared_ptr<BloomFilterFuncBase> filter) {
    TExprNode node;
    node.__set_node_type(TExprNodeType::BLOOM_PRED);
    node.__set_opcode(TExprOpcode::RT_FILTER);
    node.__set_is_nullable(false);
    node.__set_type(std::make_shared<DataTypeUInt8>()->to_thrift());
    auto predicate = VBloomPredicate::create_shared(node);
    predicate->set_filter(std::move(filter));
    predicate->add_child(
            VSlotRef::create_shared(1, 0, -1, std::make_shared<DataTypeInt32>(), "rf_key"));
    return predicate;
}

uint64_t uncached_digest(const VBloomPredicate& predicate, uint64_t seed) {
    seed = predicate.get_child(0)->get_digest(seed);
    if (seed == 0) {
        return 0;
    }
    char* data;
    int len;
    predicate.get_bloom_filter_func()->get_data(&data, &len);
    return HashUtil::hash64(data, len, seed);
}

class UndigestibleInput final : public VExpr {
public:
    UndigestibleInput() : VExpr(std::make_shared<DataTypeInt32>(), false) {}
    const std::string& expr_name() const override {
        static const std::string name = "undigestible_input";
        return name;
    }
    uint64_t get_digest(uint64_t) const override { return 0; }
    Status execute_column_impl(VExprContext*, const Block*, const Selector*, size_t,
                               ColumnPtr&) const override {
        return Status::NotSupported("Only used for digest tests");
    }
};

} // namespace

TEST(VBloomPredicateDigestTest, LargePayloadIsHashedOncePerSeed) {
    // Match the oversized RF case: repeated split initialization must not rehash 64 MiB.
    auto predicate = make_predicate(make_filter(7, 64 * 1024 * 1024));
    constexpr uint64_t seed = 12345;
    const auto expected = uncached_digest(*predicate, seed);
    for (int split = 0; split < 128; ++split) {
        EXPECT_EQ(predicate->get_digest(seed), expected);
    }
    EXPECT_EQ(predicate->TEST_digest_computations(), 1);
    EXPECT_EQ(predicate->get_digest(seed + 1), uncached_digest(*predicate, seed + 1));
    EXPECT_EQ(predicate->TEST_digest_computations(), 2);
}

TEST(VBloomPredicateDigestTest, ClonesShareCacheButKeepTheirOwnChildDigest) {
    auto predicate = make_predicate(make_filter(7));
    VExprSPtr clone;
    ASSERT_TRUE(predicate->deep_clone(&clone).ok());
    auto cloned = std::static_pointer_cast<VBloomPredicate>(clone);
    constexpr uint64_t seed = 12345;
    EXPECT_EQ(cloned->get_digest(seed), uncached_digest(*cloned, seed));
    EXPECT_EQ(predicate->get_digest(seed), cloned->get_digest(seed));
    EXPECT_EQ(predicate->TEST_digest_computations(), 1);

    // File localization can replace the child without changing the published Bloom payload.
    cloned->set_children(
            {VSlotRef::create_shared(2, 1, -1, std::make_shared<DataTypeInt32>(), "mapped_key")});
    EXPECT_EQ(cloned->get_digest(seed), uncached_digest(*cloned, seed));
    EXPECT_NE(cloned->get_digest(seed), predicate->get_digest(seed));
    EXPECT_EQ(predicate->TEST_digest_computations(), 2);
}

TEST(VBloomPredicateDigestTest, ReplacementDoesNotInvalidateExistingClones) {
    auto predicate = make_predicate(make_filter(7));
    VExprSPtr clone;
    ASSERT_TRUE(predicate->deep_clone(&clone).ok());
    auto cloned = std::static_pointer_cast<VBloomPredicate>(clone);
    constexpr uint64_t seed = 12345;
    const auto old_digest = predicate->get_digest(seed);
    predicate->set_filter(make_filter(8));
    EXPECT_EQ(predicate->TEST_digest_computations(), 0);
    EXPECT_EQ(predicate->get_digest(seed), uncached_digest(*predicate, seed));
    EXPECT_NE(predicate->get_digest(seed), old_digest);
    EXPECT_EQ(cloned->get_digest(seed), old_digest);
    EXPECT_EQ(cloned->TEST_digest_computations(), 1);
    EXPECT_EQ(predicate->TEST_digest_computations(), 1);
}

TEST(VBloomPredicateDigestTest, ZeroChildDigestDisablesCaching) {
    auto predicate = make_predicate(make_filter(7));
    predicate->set_children({std::make_shared<UndigestibleInput>()});
    EXPECT_EQ(predicate->get_digest(12345), 0);
    EXPECT_EQ(predicate->TEST_digest_computations(), 0);
}

TEST(VBloomPredicateDigestTest, InPlaceAssignmentInvalidatesCachedClones) {
    auto filter = make_filter(7);
    insert_value(*filter, 8);
    auto predicate = make_predicate(filter);
    VExprSPtr clone;
    ASSERT_TRUE(predicate->deep_clone(&clone).ok());
    constexpr uint64_t seed = 12345;
    const auto old_digest = predicate->get_digest(seed);

    // Remote delivery can replace an older superset on the same func/adaptor after the first
    // digest. A later split must key its result with the new payload, not the old superset.
    auto replacement = make_filter(8);
    char* data;
    int len;
    replacement->get_data(&data, &len);
    butil::IOBuf buffer;
    ASSERT_EQ(buffer.append(data, len), 0);
    butil::IOBufAsZeroCopyInputStream stream(buffer);
    ASSERT_TRUE(filter->assign(&stream, len, false).ok());
    const auto expected = uncached_digest(*predicate, seed);
    EXPECT_NE(expected, old_digest);
    EXPECT_EQ(predicate->get_digest(seed), expected);
    EXPECT_EQ(clone->get_digest(seed), expected);
    EXPECT_EQ(predicate->TEST_digest_computations(), 2);
}

TEST(VBloomPredicateDigestTest, LightCopyMutationsInvalidateSharedPayload) {
    auto filter = make_filter(7);
    auto alias = make_filter(8);
    alias->light_copy(filter.get());
    auto predicate = make_predicate(filter);
    constexpr uint64_t seed = 12345;
    auto previous = predicate->get_digest(seed);

    insert_value(*alias, 8);
    EXPECT_EQ(predicate->get_digest(seed), uncached_digest(*predicate, seed));
    EXPECT_NE(predicate->get_digest(seed), previous);
    EXPECT_EQ(predicate->TEST_digest_computations(), 2);
    previous = predicate->get_digest(seed);

    auto other = make_filter(9);
    ASSERT_TRUE(alias->merge(other.get()).ok());
    EXPECT_EQ(predicate->get_digest(seed), uncached_digest(*predicate, seed));
    EXPECT_NE(predicate->get_digest(seed), previous);
    EXPECT_EQ(predicate->TEST_digest_computations(), 3);
    previous = predicate->get_digest(seed);

    auto set = std::make_shared<HybridSet<TYPE_INT>>(false);
    int32_t value = 10;
    set->insert(&value);
    alias->insert_set(set);
    EXPECT_EQ(predicate->get_digest(seed), uncached_digest(*predicate, seed));
    EXPECT_NE(predicate->get_digest(seed), previous);
    EXPECT_EQ(predicate->TEST_digest_computations(), 4);
}

TEST(VBloomPredicateDigestTest, ReplacingAdaptorDoesNotReuseEqualGeneration) {
    auto filter = make_filter(7);
    auto predicate = make_predicate(filter);
    constexpr uint64_t seed = 12345;
    const auto old_version = filter->get_data_version();
    auto previous = predicate->get_digest(seed);

    ASSERT_TRUE(filter->init_with_fixed_length(0).ok());
    insert_value(*filter, 8);
    auto new_version = filter->get_data_version();
    ASSERT_EQ(old_version.second, new_version.second);
    ASSERT_NE(old_version.first, new_version.first);
    EXPECT_EQ(predicate->get_digest(seed), uncached_digest(*predicate, seed));
    EXPECT_NE(predicate->get_digest(seed), previous);
    EXPECT_EQ(predicate->TEST_digest_computations(), 2);
    previous = predicate->get_digest(seed);

    auto replacement = make_filter(9);
    filter->light_copy(replacement.get());
    new_version = filter->get_data_version();
    ASSERT_EQ(old_version.second, new_version.second);
    ASSERT_NE(old_version.first, new_version.first);
    EXPECT_EQ(predicate->get_digest(seed), uncached_digest(*predicate, seed));
    EXPECT_NE(predicate->get_digest(seed), previous);
    EXPECT_EQ(predicate->TEST_digest_computations(), 3);
}

TEST(VBloomPredicateDigestTest, ConcurrentClonesHashEachSeedOnlyOnce) {
    constexpr size_t threads = 8;
    constexpr uint64_t seed = 12345;
    auto predicate = make_predicate(make_filter(7, 1024 * 1024));
    const std::array expected {uncached_digest(*predicate, seed),
                               uncached_digest(*predicate, seed + 1)};
    std::array<VExprSPtr, threads> clones;
    for (auto& clone : clones) {
        ASSERT_TRUE(predicate->deep_clone(&clone).ok());
    }
    std::barrier ready(static_cast<std::ptrdiff_t>(threads));
    std::array<bool, threads> correct {};
    std::vector<std::thread> workers;
    for (size_t i = 0; i < threads; ++i) {
        workers.emplace_back([&, i] {
            SCOPED_INIT_THREAD_CONTEXT();
            ready.arrive_and_wait();
            bool matches = true;
            for (size_t split = 0; split < 128; ++split) {
                const auto which = (split + i) % expected.size();
                matches &= clones[i]->get_digest(seed + which) == expected[which];
            }
            correct[i] = matches;
        });
    }
    for (auto& worker : workers) {
        worker.join();
    }
    for (const bool matches : correct) {
        EXPECT_TRUE(matches);
    }
    EXPECT_EQ(predicate->TEST_digest_computations(), expected.size());
}

} // namespace doris
