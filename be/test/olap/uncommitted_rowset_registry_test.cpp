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

#include "olap/uncommitted_rowset_registry.h"

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "common/config.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/tablet_schema.h"
#include "testutil/mock_rowset.h"

namespace doris {

class UncommittedRowsetRegistryTest : public testing::Test {
protected:
    // Helper: create a MockRowset with a unique RowsetId.
    static RowsetSharedPtr create_mock_rowset(int64_t id_high) {
        auto schema = std::make_shared<TabletSchema>();
        auto rs_meta = std::make_shared<RowsetMeta>();
        RowsetId rid;
        rid.init(id_high);
        rs_meta->set_rowset_id(rid);
        rs_meta->set_rowset_type(BETA_ROWSET);
        RowsetSharedPtr rowset;
        EXPECT_EQ(Status::OK(), MockRowset::create_rowset(schema, rs_meta, &rowset));
        return rowset;
    }

    // Helper: create an UncommittedRowsetEntry with the given fields.
    static std::shared_ptr<UncommittedRowsetEntry> make_entry(int64_t tablet_id, int64_t txn_id,
                                                              int64_t id_high) {
        auto entry = std::make_shared<UncommittedRowsetEntry>();
        entry->rowset = create_mock_rowset(id_high);
        entry->transaction_id = txn_id;
        entry->partition_id = 1;
        entry->tablet_id = tablet_id;
        entry->register_time_ms = 0; // will be overwritten by register_rowset
        return entry;
    }
};

TEST_F(UncommittedRowsetRegistryTest, RegisterAndGet) {
    UncommittedRowsetRegistry registry;

    auto entry = make_entry(/*tablet_id=*/100, /*txn_id=*/1, /*id_high=*/1);
    registry.register_rowset(entry);

    std::vector<std::shared_ptr<UncommittedRowsetEntry>> result;
    registry.get_uncommitted_rowsets(100, &result);

    ASSERT_EQ(result.size(), 1);
    EXPECT_EQ(result[0]->tablet_id, 100);
    EXPECT_EQ(result[0]->transaction_id, 1);
    EXPECT_GT(result[0]->register_time_ms, 0);
}

TEST_F(UncommittedRowsetRegistryTest, GetEmpty) {
    UncommittedRowsetRegistry registry;

    std::vector<std::shared_ptr<UncommittedRowsetEntry>> result;
    registry.get_uncommitted_rowsets(999, &result);

    EXPECT_TRUE(result.empty());
}

TEST_F(UncommittedRowsetRegistryTest, MultipleTabletsIsolation) {
    UncommittedRowsetRegistry registry;

    registry.register_rowset(make_entry(100, 1, 1));
    registry.register_rowset(make_entry(200, 2, 2));

    std::vector<std::shared_ptr<UncommittedRowsetEntry>> result100;
    registry.get_uncommitted_rowsets(100, &result100);
    ASSERT_EQ(result100.size(), 1);
    EXPECT_EQ(result100[0]->transaction_id, 1);

    std::vector<std::shared_ptr<UncommittedRowsetEntry>> result200;
    registry.get_uncommitted_rowsets(200, &result200);
    ASSERT_EQ(result200.size(), 1);
    EXPECT_EQ(result200[0]->transaction_id, 2);
}

TEST_F(UncommittedRowsetRegistryTest, MultipleEntriesSameTablet) {
    UncommittedRowsetRegistry registry;

    registry.register_rowset(make_entry(100, 10, 10));
    registry.register_rowset(make_entry(100, 20, 20));
    registry.register_rowset(make_entry(100, 30, 30));

    std::vector<std::shared_ptr<UncommittedRowsetEntry>> result;
    registry.get_uncommitted_rowsets(100, &result);

    ASSERT_EQ(result.size(), 3);
    // Verify all three transaction IDs are present
    std::set<int64_t> txn_ids;
    for (const auto& e : result) {
        txn_ids.insert(e->transaction_id);
    }
    EXPECT_TRUE(txn_ids.count(10));
    EXPECT_TRUE(txn_ids.count(20));
    EXPECT_TRUE(txn_ids.count(30));
}

TEST_F(UncommittedRowsetRegistryTest, UnregisterByTxnId) {
    UncommittedRowsetRegistry registry;

    registry.register_rowset(make_entry(100, 1, 1));

    // Verify it exists
    std::vector<std::shared_ptr<UncommittedRowsetEntry>> result;
    registry.get_uncommitted_rowsets(100, &result);
    ASSERT_EQ(result.size(), 1);

    // Unregister
    registry.unregister_rowset(100, 1);

    result.clear();
    registry.get_uncommitted_rowsets(100, &result);
    EXPECT_TRUE(result.empty());
}

TEST_F(UncommittedRowsetRegistryTest, UnregisterNonexistent) {
    UncommittedRowsetRegistry registry;

    // Should not crash or throw on empty registry
    registry.unregister_rowset(999, 1);

    // Register something, then unregister a non-existent txn_id
    registry.register_rowset(make_entry(100, 1, 1));
    registry.unregister_rowset(100, 999);

    // Original entry should still be present
    std::vector<std::shared_ptr<UncommittedRowsetEntry>> result;
    registry.get_uncommitted_rowsets(100, &result);
    ASSERT_EQ(result.size(), 1);
    EXPECT_EQ(result[0]->transaction_id, 1);
}

TEST_F(UncommittedRowsetRegistryTest, UnregisterLeavesOtherEntries) {
    UncommittedRowsetRegistry registry;

    registry.register_rowset(make_entry(100, 1, 1));
    registry.register_rowset(make_entry(100, 2, 2));

    registry.unregister_rowset(100, 1);

    std::vector<std::shared_ptr<UncommittedRowsetEntry>> result;
    registry.get_uncommitted_rowsets(100, &result);
    ASSERT_EQ(result.size(), 1);
    EXPECT_EQ(result[0]->transaction_id, 2);
}

TEST_F(UncommittedRowsetRegistryTest, RemoveExpiredEntries) {
    UncommittedRowsetRegistry registry;

    // Save and override the config
    int32_t saved_expire = config::uncommitted_rowset_expire_sec;
    config::uncommitted_rowset_expire_sec = 60; // 60 seconds

    // Register two entries
    auto entry_old = make_entry(100, 1, 1);
    auto entry_fresh = make_entry(100, 2, 2);
    registry.register_rowset(entry_old);
    registry.register_rowset(entry_fresh);

    // Simulate: make entry_old look like it was registered 120 seconds ago
    entry_old->register_time_ms -= 120 * 1000L;

    registry.remove_expired_entries();

    std::vector<std::shared_ptr<UncommittedRowsetEntry>> result;
    registry.get_uncommitted_rowsets(100, &result);
    ASSERT_EQ(result.size(), 1);
    EXPECT_EQ(result[0]->transaction_id, 2);

    // Restore config
    config::uncommitted_rowset_expire_sec = saved_expire;
}

TEST_F(UncommittedRowsetRegistryTest, ShardingDistribution) {
    UncommittedRowsetRegistry registry;

    // Tablets 0 and 16 both map to shard 0 (0 % 16 == 16 % 16 == 0).
    // They should still be isolated by tablet_id in the shard's map.
    registry.register_rowset(make_entry(0, 1, 1));
    registry.register_rowset(make_entry(16, 2, 2));

    std::vector<std::shared_ptr<UncommittedRowsetEntry>> result0;
    registry.get_uncommitted_rowsets(0, &result0);
    ASSERT_EQ(result0.size(), 1);
    EXPECT_EQ(result0[0]->transaction_id, 1);

    std::vector<std::shared_ptr<UncommittedRowsetEntry>> result16;
    registry.get_uncommitted_rowsets(16, &result16);
    ASSERT_EQ(result16.size(), 1);
    EXPECT_EQ(result16[0]->transaction_id, 2);

    // Tablets 1 and 2 map to different shards (1 % 16 != 2 % 16).
    // Basic correctness check for different-shard tablets.
    registry.register_rowset(make_entry(1, 3, 3));
    registry.register_rowset(make_entry(2, 4, 4));

    std::vector<std::shared_ptr<UncommittedRowsetEntry>> result1;
    registry.get_uncommitted_rowsets(1, &result1);
    ASSERT_EQ(result1.size(), 1);
    EXPECT_EQ(result1[0]->transaction_id, 3);

    std::vector<std::shared_ptr<UncommittedRowsetEntry>> result2;
    registry.get_uncommitted_rowsets(2, &result2);
    ASSERT_EQ(result2.size(), 1);
    EXPECT_EQ(result2[0]->transaction_id, 4);
}

} // namespace doris
