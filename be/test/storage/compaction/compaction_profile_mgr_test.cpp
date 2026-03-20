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

#include "storage/compaction/compaction_profile_mgr.h"

#include <gtest/gtest.h>

#include <thread>
#include <vector>

#include "common/config.h"

namespace doris {

class CompactionProfileMgrTest : public testing::Test {
public:
    void SetUp() override {
        _saved_max_records = config::compaction_profile_max_records;
        config::compaction_profile_max_records = 100;
        // Clear any residual records from other tests
        auto* mgr = CompactionProfileManager::instance();
        config::compaction_profile_max_records = 0;
        mgr->get_records(); // triggers clear via write lock
        config::compaction_profile_max_records = 100;
    }

    void TearDown() override { config::compaction_profile_max_records = _saved_max_records; }

protected:
    static CompactionProfileRecord make_record(int64_t tablet_id, CompactionProfileType type,
                                               bool success = true) {
        auto* mgr = CompactionProfileManager::instance();
        CompactionProfileRecord r;
        r.compaction_id = mgr->next_compaction_id();
        r.compaction_type = type;
        r.tablet_id = tablet_id;
        r.start_time_ms = 1000;
        r.end_time_ms = 1100;
        r.cost_time_ms = 100;
        r.success = success;
        r.input_rowsets_data_size = 1024;
        r.input_rowsets_count = 3;
        r.input_row_num = 500;
        r.input_segments_num = 5;
        r.output_rowset_data_size = 512;
        r.output_row_num = 490;
        r.output_segments_num = 1;
        r.output_version = "[0-3]";
        if (!success) {
            r.status_msg = "test failure";
        }
        return r;
    }

    int32_t _saved_max_records = 0;
};

TEST_F(CompactionProfileMgrTest, BasicAddAndGet) {
    auto* mgr = CompactionProfileManager::instance();
    mgr->add_record(make_record(100, CompactionProfileType::BASE));
    mgr->add_record(make_record(200, CompactionProfileType::CUMULATIVE));

    auto records = mgr->get_records();
    ASSERT_EQ(records.size(), 2);
    // Most recent first
    EXPECT_EQ(records[0].tablet_id, 200);
    EXPECT_EQ(records[1].tablet_id, 100);
    EXPECT_EQ(to_string(records[0].compaction_type), std::string("cumulative"));
    EXPECT_EQ(to_string(records[1].compaction_type), std::string("base"));
}

TEST_F(CompactionProfileMgrTest, FilterByTabletId) {
    auto* mgr = CompactionProfileManager::instance();
    mgr->add_record(make_record(100, CompactionProfileType::BASE));
    mgr->add_record(make_record(200, CompactionProfileType::CUMULATIVE));
    mgr->add_record(make_record(100, CompactionProfileType::FULL));
    mgr->add_record(make_record(300, CompactionProfileType::BASE));

    auto records = mgr->get_records(100);
    ASSERT_EQ(records.size(), 2);
    EXPECT_EQ(records[0].tablet_id, 100);
    EXPECT_EQ(records[1].tablet_id, 100);

    records = mgr->get_records(999);
    EXPECT_TRUE(records.empty());
}

TEST_F(CompactionProfileMgrTest, TopN) {
    auto* mgr = CompactionProfileManager::instance();
    for (int i = 0; i < 20; i++) {
        mgr->add_record(make_record(100 + i, CompactionProfileType::BASE));
    }

    auto records = mgr->get_records(0, 5);
    ASSERT_EQ(records.size(), 5);
    // Most recent first
    EXPECT_EQ(records[0].tablet_id, 119);
    EXPECT_EQ(records[4].tablet_id, 115);
}

TEST_F(CompactionProfileMgrTest, TrimOldRecords) {
    auto* mgr = CompactionProfileManager::instance();
    config::compaction_profile_max_records = 10;

    for (int i = 0; i < 20; i++) {
        mgr->add_record(make_record(i, CompactionProfileType::CUMULATIVE));
    }

    auto records = mgr->get_records();
    ASSERT_EQ(records.size(), 10);
    // Only the 10 most recent remain (tablet_id 10..19)
    EXPECT_EQ(records[0].tablet_id, 19);
    EXPECT_EQ(records[9].tablet_id, 10);
}

TEST_F(CompactionProfileMgrTest, DynamicConfigDisable) {
    auto* mgr = CompactionProfileManager::instance();
    mgr->add_record(make_record(100, CompactionProfileType::BASE));
    mgr->add_record(make_record(200, CompactionProfileType::BASE));

    ASSERT_EQ(mgr->get_records().size(), 2);

    // Disable: get_records returns empty and clears deque
    config::compaction_profile_max_records = 0;
    auto records = mgr->get_records();
    EXPECT_TRUE(records.empty());

    // New records should not be stored
    mgr->add_record(make_record(300, CompactionProfileType::BASE));
    records = mgr->get_records();
    EXPECT_TRUE(records.empty());
}

TEST_F(CompactionProfileMgrTest, DynamicConfigShrink) {
    auto* mgr = CompactionProfileManager::instance();
    config::compaction_profile_max_records = 50;

    for (int i = 0; i < 50; i++) {
        mgr->add_record(make_record(i, CompactionProfileType::BASE));
    }
    ASSERT_EQ(mgr->get_records().size(), 50);

    // Shrink to 10
    config::compaction_profile_max_records = 10;
    auto records = mgr->get_records();
    // get_records limits by current config
    EXPECT_LE(static_cast<int32_t>(records.size()), 10);

    // After add, physical trim happens
    mgr->add_record(make_record(999, CompactionProfileType::BASE));
    records = mgr->get_records();
    ASSERT_EQ(records.size(), 10);
    EXPECT_EQ(records[0].tablet_id, 999);
}

TEST_F(CompactionProfileMgrTest, DynamicConfigRestore) {
    auto* mgr = CompactionProfileManager::instance();

    // Add some records
    mgr->add_record(make_record(100, CompactionProfileType::BASE));
    mgr->add_record(make_record(200, CompactionProfileType::BASE));
    ASSERT_EQ(mgr->get_records().size(), 2);

    // Disable: clears old records
    config::compaction_profile_max_records = 0;
    mgr->get_records(); // trigger clear

    // Restore
    config::compaction_profile_max_records = 100;
    auto records = mgr->get_records();
    // Old records should not reappear
    EXPECT_TRUE(records.empty());

    // New records work
    mgr->add_record(make_record(300, CompactionProfileType::BASE));
    records = mgr->get_records();
    ASSERT_EQ(records.size(), 1);
    EXPECT_EQ(records[0].tablet_id, 300);
}

TEST_F(CompactionProfileMgrTest, FailedRecordFields) {
    auto* mgr = CompactionProfileManager::instance();
    auto record = make_record(100, CompactionProfileType::FULL, false);
    record.output_rowset_data_size = 512;
    record.output_version = "[0-5]";
    mgr->add_record(std::move(record));

    auto records = mgr->get_records();
    ASSERT_EQ(records.size(), 1);
    EXPECT_FALSE(records[0].success);
    EXPECT_EQ(records[0].status_msg, "test failure");
    // Output fields preserved even on failure
    EXPECT_EQ(records[0].output_rowset_data_size, 512);
    EXPECT_EQ(records[0].output_version, "[0-5]");
}

TEST_F(CompactionProfileMgrTest, ToJson) {
    auto* mgr = CompactionProfileManager::instance();
    mgr->add_record(make_record(100, CompactionProfileType::BASE));

    auto records = mgr->get_records();
    ASSERT_EQ(records.size(), 1);

    rapidjson::Document doc;
    rapidjson::Value obj;
    records[0].to_json(obj, doc.GetAllocator());

    EXPECT_TRUE(obj.HasMember("compaction_id"));
    EXPECT_TRUE(obj.HasMember("compaction_type"));
    EXPECT_STREQ(obj["compaction_type"].GetString(), "base");
    EXPECT_EQ(obj["tablet_id"].GetInt64(), 100);
    EXPECT_TRUE(obj["success"].GetBool());
    EXPECT_TRUE(obj.HasMember("start_time"));
    EXPECT_TRUE(obj.HasMember("end_time"));
    EXPECT_EQ(obj["cost_time_ms"].GetInt64(), 100);
    EXPECT_EQ(obj["input_rowsets_data_size"].GetInt64(), 1024);
    EXPECT_EQ(obj["output_rowset_data_size"].GetInt64(), 512);
    EXPECT_STREQ(obj["output_version"].GetString(), "[0-3]");
}

TEST_F(CompactionProfileMgrTest, AllProfileTypes) {
    EXPECT_STREQ(to_string(CompactionProfileType::BASE), "base");
    EXPECT_STREQ(to_string(CompactionProfileType::CUMULATIVE), "cumulative");
    EXPECT_STREQ(to_string(CompactionProfileType::FULL), "full");
    EXPECT_STREQ(to_string(CompactionProfileType::SINGLE_REPLICA), "single_replica");
    EXPECT_STREQ(to_string(CompactionProfileType::COLD_DATA), "cold_data");
    EXPECT_STREQ(to_string(CompactionProfileType::INDEX_CHANGE), "index_change");
}

TEST_F(CompactionProfileMgrTest, ConcurrentSafety) {
    auto* mgr = CompactionProfileManager::instance();
    config::compaction_profile_max_records = 500;

    constexpr int kWriters = 4;
    constexpr int kRecordsPerWriter = 100;
    constexpr int kReaders = 2;

    std::atomic<bool> stop {false};
    std::vector<std::thread> threads;

    // Writers
    for (int w = 0; w < kWriters; w++) {
        threads.emplace_back([&, w]() {
            for (int i = 0; i < kRecordsPerWriter; i++) {
                mgr->add_record(make_record(w * 1000 + i, CompactionProfileType::BASE));
            }
        });
    }

    // Readers
    for (int r = 0; r < kReaders; r++) {
        threads.emplace_back([&]() {
            while (!stop.load(std::memory_order_relaxed)) {
                auto records = mgr->get_records();
                // Just verify no crash
                for (const auto& rec : records) {
                    (void)rec.tablet_id;
                }
            }
        });
    }

    // Wait for writers
    for (int i = 0; i < kWriters; i++) {
        threads[i].join();
    }
    stop.store(true, std::memory_order_relaxed);

    // Wait for readers
    for (int i = kWriters; i < static_cast<int>(threads.size()); i++) {
        threads[i].join();
    }

    auto records = mgr->get_records();
    EXPECT_EQ(records.size(), kWriters * kRecordsPerWriter);
}

TEST_F(CompactionProfileMgrTest, MonotonicallyIncreasingIds) {
    auto* mgr = CompactionProfileManager::instance();
    int64_t prev_id = mgr->next_compaction_id();
    for (int i = 0; i < 100; i++) {
        int64_t id = mgr->next_compaction_id();
        EXPECT_GT(id, prev_id);
        prev_id = id;
    }
}

} // namespace doris
