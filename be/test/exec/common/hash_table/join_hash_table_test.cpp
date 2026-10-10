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

#include "exec/common/hash_table/join_hash_table.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <tuple>
#include <vector>

#include "exec/common/hash_table/hash.h"

namespace doris {

// A null aware mark join with other join conjuncts uses
// `JoinHashTable::find_null_aware_with_other_conjuncts` to find, for one probe row, all the build
// rows which match its key and all the build rows which have a null key. `null_flags` tells the
// probe operator which of the returned rows matched a null key: the mark of the join is null for
// those rows and false for the row which is returned for a probe row without any match, so every
// row which the search returns must have a null flag, including the row of a probe row without any
// match (the flag buffers are reused by the following batches).
class JoinHashTableTest : public ::testing::Test {
public:
    // the bucket of a null key is `bucket_size` (see `MethodOneNumberDirect`), so the bucket size
    // is a part of the layout of these tests
    static constexpr uint32_t BUCKET_SIZE = 8;
    static constexpr int BATCH_SIZE = 8;

    using HashTable = JoinHashTable<uint64_t, DefaultHash<uint64_t>, true>;

    // the direct mapping of the hash table is replaced by the explicit bucket numbers of the build
    // rows, the build keys themselves are unused
    static void init(HashTable& table, std::vector<uint64_t>& build_keys,
                     const std::vector<uint32_t>& build_buckets, bool has_null_key) {
        build_keys.assign(build_buckets.size(), 0);
        table.template prepare_build<TJoinOp::NULL_AWARE_LEFT_SEMI_JOIN>(
                build_keys.size(), BATCH_SIZE, has_null_key, BUCKET_SIZE);
        table.build(build_keys.data(), build_buckets.data(), build_keys.size(),
                    /*keep_null_key=*/true);
    }

    // the probe operator resolves the bucket of a probe row to the head of its chain before the
    // search (`ProcessHashTableProbe::_init_probe_side`)
    static void initProbe(HashTable& table, DorisVector<uint32_t>& probe_buckets) {
        table.pre_build_idxs(probe_buckets);
    }

    static std::tuple<uint32_t, uint32_t, uint32_t, bool> find(
            HashTable& table, const std::vector<uint64_t>& probe_keys,
            DorisVector<uint32_t>& probe_buckets, std::vector<uint32_t>& probe_idxs,
            std::vector<uint32_t>& build_idxs, std::vector<uint8_t>& null_flags) {
        return table.find_null_aware_with_other_conjuncts(
                probe_keys.data(), probe_buckets.data(), 0, 0, static_cast<int>(probe_keys.size()),
                probe_idxs.data(), build_idxs.data(), null_flags.data(), false, nullptr);
    }
};

// A probe row without any match is returned as a row with the build index 0, and its null flag is
// initialized to 0: the probe operator copies the whole flag buffer into the null map of the mark
// column, so a value left in the buffer by a previous batch would turn a false mark into a null
// mark.
TEST_F(JoinHashTableTest, UnmatchedProbeRowClearsStaleNullFlag) {
    std::vector<uint64_t> build_keys;
    HashTable table;
    // one build row in bucket 4, no null key
    init(table, build_keys, {0, 4}, /*has_null_key=*/false);

    std::vector<uint64_t> probe_keys {3, 9};
    DorisVector<uint32_t> probe_buckets {0, 0}; // bucket 0 is empty, so nothing matches
    initProbe(table, probe_buckets);

    std::vector<uint32_t> probe_idxs(probe_keys.size(), 0);
    std::vector<uint32_t> build_idxs(probe_keys.size(), 0);
    // the values left by a previous batch of the probe operator
    std::vector<uint8_t> null_flags(probe_keys.size(), 0xff);

    auto [probe_idx, build_idx, matched, picking_null_keys] =
            find(table, probe_keys, probe_buckets, probe_idxs, build_idxs, null_flags);

    EXPECT_EQ(probe_keys.size(), matched);
    EXPECT_EQ(probe_keys.size(), probe_idx);
    EXPECT_EQ(0, build_idx);
    EXPECT_FALSE(picking_null_keys);
    for (size_t i = 0; i != matched; ++i) {
        EXPECT_EQ(i, probe_idxs[i]);
        // the row of a probe row without any match
        EXPECT_EQ(0, build_idxs[i]);
        // a false mark must not become a null mark
        EXPECT_EQ(0, null_flags[i])
                << "row " << i << " inherited the null flag of a previous batch";
    }
}

// The build rows with a null key are returned with a null flag, so the mark of a probe row which
// only matches those rows is null; the row which is returned for the probe row itself has to clear
// the flag again.
TEST_F(JoinHashTableTest, NullKeyMatchKeepsNullFlagAndTheFollowingRowClearsIt) {
    std::vector<uint64_t> build_keys;
    HashTable table;
    // one build row in bucket 4 and two build rows with a null key
    init(table, build_keys, {0, 4, BUCKET_SIZE, BUCKET_SIZE}, /*has_null_key=*/true);

    std::vector<uint64_t> probe_keys {3};
    DorisVector<uint32_t> probe_buckets {0};
    initProbe(table, probe_buckets);

    std::vector<uint32_t> probe_idxs(4, 0);
    std::vector<uint32_t> build_idxs(4, 0);
    std::vector<uint8_t> null_flags(4, 0xff);

    auto [probe_idx, build_idx, matched, picking_null_keys] =
            find(table, probe_keys, probe_buckets, probe_idxs, build_idxs, null_flags);

    // the two null keys and the row of the probe row itself are returned
    ASSERT_EQ(3, matched);
    // the build rows with a null key are linked in reverse order their buckets were built in
    EXPECT_EQ(3, build_idxs[0]);
    EXPECT_EQ(2, build_idxs[1]);
    EXPECT_EQ(1, null_flags[0]);
    EXPECT_EQ(1, null_flags[1]);
    // the row of the probe row itself is not a null match
    EXPECT_EQ(0, build_idxs[2]);
    EXPECT_EQ(0, null_flags[2]) << "the row of a probe row without any key match must be false";
}

// A probe row which matches the key of a build row is not marked as a null match, the build row
// with a null key which follows it is.
TEST_F(JoinHashTableTest, KeyMatchIsNotMarkedAsNullMatch) {
    std::vector<uint64_t> build_keys;
    HashTable table;
    // one build row in bucket 4 and one build row with a null key
    init(table, build_keys, {0, 4, BUCKET_SIZE}, /*has_null_key=*/true);

    std::vector<uint64_t> probe_keys {4};
    DorisVector<uint32_t> probe_buckets {4}; // bucket 4 contains the build row of the probe key
    initProbe(table, probe_buckets);
    ASSERT_EQ(1, probe_buckets[0]);

    std::vector<uint32_t> probe_idxs(4, 0);
    std::vector<uint32_t> build_idxs(4, 0);
    std::vector<uint8_t> null_flags(4, 0xff);

    auto [probe_idx, build_idx, matched, picking_null_keys] =
            find(table, probe_keys, probe_buckets, probe_idxs, build_idxs, null_flags);

    // the equal key, the null key and the row of the probe row itself are returned
    ASSERT_EQ(3, matched);
    EXPECT_EQ(1, build_idxs[0]);
    EXPECT_EQ(0, null_flags[0]) << "an equal key is not a null match";
    EXPECT_EQ(2, build_idxs[1]);
    EXPECT_EQ(1, null_flags[1]) << "the build row with the null key is a null match";
    EXPECT_EQ(0, build_idxs[2]);
    EXPECT_EQ(0, null_flags[2]) << "the last row is the row of the probe row itself";
}

} // namespace doris
