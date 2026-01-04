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

#include "cloud/cloud_meta_mgr.h"

#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <random>
#include <set>

#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "cpp/sync_point.h"
#include "gen_cpp/cloud.pb.h"
#include "olap/olap_common.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/tablet_meta.h"
#include "util/uid_util.h"

namespace doris {
using namespace cloud;
using namespace std::chrono;

class CloudMetaMgrTest : public testing::Test {
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(CloudMetaMgrTest, bthread_fork_join_test) {
    // clang-format off
    std::atomic<int> task_counter{0};
    std::atomic<int> concurrent_tasks{0};
    std::atomic<int> max_concurrent_tasks{0};
    int num_tasks = 7;
    int concurrency = 3;
    int sleep_us = 10000;

    std::mt19937 rng(system_clock::now().time_since_epoch().count());
    std::uniform_int_distribution<int> dist(0, sleep_us);
    std::vector<std::function<Status()>> tasks (
        num_tasks,
        [&]{
            int current = ++concurrent_tasks;
            int old_max = max_concurrent_tasks.load();
            while (current > old_max && !max_concurrent_tasks.compare_exchange_weak(old_max, current)) {
                // Update max if needed
            }
            bthread_usleep(sleep_us);
            task_counter.fetch_add(1);
            --concurrent_tasks;
            return Status::OK();
        }
    );

    // Test synchronous execution
    {
        auto start = steady_clock::now();
        task_counter.store(0);
        concurrent_tasks.store(0);
        max_concurrent_tasks.store(0);

        EXPECT_TRUE(bthread_fork_join(tasks, concurrency).ok());
        auto end = steady_clock::now();

        // All 7 tasks should have completed
        EXPECT_EQ(task_counter.load(), num_tasks);
        // With concurrency 3, we should see at most 3 concurrent tasks
        EXPECT_LE(max_concurrent_tasks.load(), concurrency);
        EXPECT_GT(max_concurrent_tasks.load(), 0);
        EXPECT_GE(duration_cast<microseconds>(end - start).count(), (num_tasks * sleep_us / concurrency));
    }

    // Test asynchronous execution
    {
        auto start = steady_clock::now();
        task_counter.store(0);
        concurrent_tasks.store(0);
        max_concurrent_tasks.store(0);

        std::future<Status> fut;
        auto t = tasks;
        EXPECT_TRUE(bthread_fork_join(std::move(t), concurrency, &fut).ok()); // return immediately
        EXPECT_LT(task_counter.load(), num_tasks);

        // Initially no tasks should have completed
        EXPECT_EQ(task_counter.load(), 0);

        // Wait for completion
        EXPECT_TRUE(fut.get().ok());
        auto end = steady_clock::now();

        // All 7 tasks should have completed
        EXPECT_EQ(task_counter.load(), num_tasks);
        // With concurrency 3, we should see at most 3 concurrent tasks
        EXPECT_LE(max_concurrent_tasks.load(), concurrency);
        EXPECT_GT(max_concurrent_tasks.load(), 0);
        EXPECT_GE(duration_cast<microseconds>(end - start).count(), (num_tasks * sleep_us / concurrency));
    }

    // Test error handling - make the first task fail fast
    {
        auto start = steady_clock::now();
        task_counter.store(0);
        concurrent_tasks.store(0);
        max_concurrent_tasks.store(0);

        auto error_tasks = tasks;
        error_tasks.insert(error_tasks.begin(), [&]{
            // This task fails immediately
            return Status::InternalError<false>("error");
        });

        EXPECT_FALSE(bthread_fork_join(error_tasks, concurrency).ok());
        auto end = steady_clock::now();

        // When first task fails, not all tasks may complete
        // We can only verify that at least one task ran
        EXPECT_LE(task_counter.load(), concurrency);
        EXPECT_GE(duration_cast<microseconds>(end - start).count(), sleep_us);
    }

    // Test asynchronous error handling
    {
        auto start = steady_clock::now();
        auto end = steady_clock::now();
        task_counter.store(0);
        concurrent_tasks.store(0);
        max_concurrent_tasks.store(0);

        auto error_tasks = tasks;
        error_tasks.insert(error_tasks.begin(), [&]{
            // This task fails immediately
            return Status::InternalError<false>("error");
        });

        std::future<Status> fut;
        auto t = error_tasks;
        EXPECT_TRUE(bthread_fork_join(std::move(t), concurrency, &fut).ok()); // return immediately
        EXPECT_LT(task_counter.load(), concurrency);

        // Initially no tasks should have completed
        EXPECT_EQ(task_counter.load(), 0);

        // Wait for completion - should fail
        EXPECT_FALSE(fut.get().ok());
        end = steady_clock::now();

        // When first task fails, not all tasks may complete
        // We can only verify that at least one task ran
        EXPECT_LE(task_counter.load(), concurrency);
        EXPECT_GE(duration_cast<microseconds>(end - start).count(), sleep_us);
    }
    // clang-format on
}

TEST_F(CloudMetaMgrTest, test_fill_version_holes_no_holes) {
    CloudStorageEngine engine(EngineOptions {});
    CloudMetaMgr meta_mgr;

    TabletMetaSharedPtr tablet_meta(
            new TabletMeta(1001, 2, 15673, 15674, 4, 5, TTabletSchema(), 6, {{7, 8}},
                           UniqueId(9, 10), TTabletType::TABLET_TYPE_DISK, TCompressionType::LZ4F));
    auto tablet = std::make_shared<CloudTablet>(engine, std::make_shared<TabletMeta>(*tablet_meta));

    // Add consecutive versions: 0, 1, 2, 3, 4
    std::vector<RowsetSharedPtr> rowsets;
    for (int64_t version = 0; version <= 4; ++version) {
        auto rs_meta = std::make_shared<RowsetMeta>();
        rs_meta->set_tablet_id(1001);
        rs_meta->set_index_id(2);
        rs_meta->set_partition_id(15673);
        rs_meta->set_tablet_uid(UniqueId(9, 10));
        rs_meta->set_version(Version(version, version));
        rs_meta->set_rowset_type(BETA_ROWSET);
        rs_meta->set_rowset_id(engine.next_rowset_id());
        rs_meta->set_num_rows(100);
        rs_meta->set_empty(false);
        rs_meta->set_tablet_schema(tablet->tablet_schema());

        // Create rowset and add it to tablet
        RowsetSharedPtr rowset;
        auto status = RowsetFactory::create_rowset(tablet->tablet_schema(), tablet->tablet_path(),
                                                   rs_meta, &rowset);
        EXPECT_TRUE(status.ok());
        rowsets.push_back(rowset);
    }

    // Add all rowsets to tablet
    {
        std::unique_lock<std::shared_mutex> lock(tablet->get_header_lock());
        tablet->add_rowsets(std::move(rowsets), false, lock, false);
    }

    // Test fill_version_holes directly - should not add any rowsets since there are no holes
    std::unique_lock<std::shared_mutex> wlock(tablet->get_header_lock());
    Status status = meta_mgr.fill_version_holes(tablet.get(), 4, wlock);
    EXPECT_TRUE(status.ok());

    // Verify tablet still has the same number of rowsets (no holes to fill)
    EXPECT_EQ(tablet->tablet_meta()->all_rs_metas().size(), 5);
    // Verify rows number is correct
    for (const auto& [_, rs_meta] : tablet->tablet_meta()->all_rs_metas()) {
        EXPECT_EQ(rs_meta->num_rows(), 100);
    }
}

TEST_F(CloudMetaMgrTest, test_fill_version_holes_with_holes) {
    CloudStorageEngine engine(EngineOptions {});
    CloudMetaMgr meta_mgr;

    TabletMetaSharedPtr tablet_meta(
            new TabletMeta(1001, 2, 15673, 15674, 4, 5, TTabletSchema(), 6, {{7, 8}},
                           UniqueId(9, 10), TTabletType::TABLET_TYPE_DISK, TCompressionType::LZ4F));
    auto tablet = std::make_shared<CloudTablet>(engine, std::make_shared<TabletMeta>(*tablet_meta));

    // Add non-consecutive versions: 0, 2, 4 (missing 1, 3)
    std::vector<int64_t> versions = {0, 2, 4};
    std::vector<RowsetSharedPtr> rowsets;
    for (int64_t version : versions) {
        auto rs_meta = std::make_shared<RowsetMeta>();
        rs_meta->set_tablet_id(1001);
        rs_meta->set_index_id(2);
        rs_meta->set_partition_id(15673);
        rs_meta->set_tablet_uid(UniqueId(9, 10));
        rs_meta->set_version(Version(version, version));
        rs_meta->set_rowset_type(BETA_ROWSET);
        rs_meta->set_rowset_id(engine.next_rowset_id());
        rs_meta->set_num_rows(100);
        rs_meta->set_empty(false);
        rs_meta->set_tablet_schema(tablet->tablet_schema());

        // Create rowset and add it to list
        RowsetSharedPtr rowset;
        auto status = RowsetFactory::create_rowset(tablet->tablet_schema(), tablet->tablet_path(),
                                                   rs_meta, &rowset);
        EXPECT_TRUE(status.ok());
        rowsets.push_back(rowset);
    }

    // Add all rowsets to tablet
    {
        std::unique_lock<std::shared_mutex> lock(tablet->get_header_lock());
        tablet->add_rowsets(std::move(rowsets), false, lock, false);
    }

    // Initially we have 3 rowsets (versions 0, 2, 4)
    EXPECT_EQ(tablet->tablet_meta()->all_rs_metas().size(), 3);

    // Test fill_version_holes directly to fill missing versions 1 and 3
    std::unique_lock<std::shared_mutex> wlock(tablet->get_header_lock());
    Status status = meta_mgr.fill_version_holes(tablet.get(), 4, wlock);
    EXPECT_TRUE(status.ok());

    // After filling holes, we should have 5 rowsets (versions 0, 1, 2, 3, 4)
    EXPECT_EQ(tablet->tablet_meta()->all_rs_metas().size(), 5);

    // Verify all versions are present
    auto rs_metas = tablet->tablet_meta()->all_rs_metas();
    std::set<int64_t> found_versions;
    for (const auto& [_, rs_meta] : rs_metas) {
        found_versions.insert(rs_meta->version().first);
    }
    EXPECT_EQ(found_versions.size(), 5);
    EXPECT_TRUE(found_versions.contains(0));
    EXPECT_TRUE(found_versions.contains(1));
    EXPECT_TRUE(found_versions.contains(2));
    EXPECT_TRUE(found_versions.contains(3));
    EXPECT_TRUE(found_versions.contains(4));

    // Verify the hole rowsets (versions 1 and 3) are empty
    for (const auto& [_, rs_meta] : rs_metas) {
        if (rs_meta->version().first == 1 || rs_meta->version().first == 3) {
            EXPECT_TRUE(rs_meta->empty());
            EXPECT_EQ(rs_meta->num_rows(), 0);
        } else {
            EXPECT_FALSE(rs_meta->empty());
            EXPECT_EQ(rs_meta->num_rows(), 100);
        }
    }
}

// Test create_empty_rowset_for_hole function
TEST_F(CloudMetaMgrTest, test_create_empty_rowset_for_hole) {
    CloudStorageEngine engine(EngineOptions {});
    CloudMetaMgr meta_mgr;

    TabletMetaSharedPtr tablet_meta(
            new TabletMeta(1001, 2, 15673, 15674, 4, 5, TTabletSchema(), 6, {{7, 8}},
                           UniqueId(9, 10), TTabletType::TABLET_TYPE_DISK, TCompressionType::LZ4F));
    auto tablet = std::make_shared<CloudTablet>(engine, std::make_shared<TabletMeta>(*tablet_meta));

    // Create a previous rowset meta to pass as reference
    auto prev_rs_meta = std::make_shared<RowsetMeta>();
    prev_rs_meta->set_tablet_id(1001);
    prev_rs_meta->set_index_id(2);
    prev_rs_meta->set_partition_id(15673);
    prev_rs_meta->set_tablet_uid(UniqueId(9, 10));
    prev_rs_meta->set_version(Version(1, 1));
    prev_rs_meta->set_rowset_type(BETA_ROWSET);
    prev_rs_meta->set_rowset_id(engine.next_rowset_id());
    prev_rs_meta->set_num_rows(100);
    prev_rs_meta->set_empty(false);
    prev_rs_meta->set_tablet_schema(tablet->tablet_schema());

    // Test creating an empty rowset for version hole
    RowsetSharedPtr hole_rowset;
    Status status =
            meta_mgr.create_empty_rowset_for_hole(tablet.get(), 2, prev_rs_meta, &hole_rowset);
    EXPECT_TRUE(status.ok()) << "Failed to create empty rowset for hole: " << status;
    EXPECT_NE(hole_rowset, nullptr);

    // Verify the hole rowset properties
    auto hole_rs_meta = hole_rowset->rowset_meta();
    EXPECT_EQ(hole_rs_meta->tablet_id(), 15673);
    EXPECT_EQ(hole_rs_meta->index_id(), 0);
    EXPECT_EQ(hole_rs_meta->partition_id(), 2);
    EXPECT_EQ(hole_rs_meta->tablet_uid(), UniqueId(9, 10));
    EXPECT_EQ(hole_rs_meta->version(), Version(2, 2));
    EXPECT_EQ(hole_rs_meta->rowset_type(), BETA_ROWSET);
    EXPECT_EQ(hole_rs_meta->num_rows(), 0);
    EXPECT_EQ(hole_rs_meta->total_disk_size(), 0);
    EXPECT_EQ(hole_rs_meta->data_disk_size(), 0);
    EXPECT_EQ(hole_rs_meta->index_disk_size(), 0);
    EXPECT_TRUE(hole_rs_meta->empty());
    EXPECT_EQ(hole_rs_meta->num_segments(), 0);
    EXPECT_EQ(hole_rs_meta->segments_overlap(), NONOVERLAPPING);
    EXPECT_EQ(hole_rs_meta->rowset_state(), VISIBLE);
    EXPECT_TRUE(hole_rowset->is_hole_rowset());
    EXPECT_EQ(hole_rowset->txn_id(), 2); // txn_id should match version
    RowsetId expected_rowset_id;
    expected_rowset_id.init(2, 0, 15673, 2);
    EXPECT_EQ(hole_rowset->rowset_meta()->rowset_id(), expected_rowset_id);

    // Test creating multiple hole rowsets with different versions
    RowsetSharedPtr hole_rowset_v3;
    status = meta_mgr.create_empty_rowset_for_hole(tablet.get(), 3, prev_rs_meta, &hole_rowset_v3);
    EXPECT_TRUE(status.ok());
    EXPECT_NE(hole_rowset_v3, nullptr);
    EXPECT_EQ(hole_rowset_v3->rowset_meta()->version(), Version(3, 3));
    EXPECT_TRUE(hole_rowset_v3->is_hole_rowset());

    // Verify different hole rowsets have different rowset IDs
    EXPECT_NE(hole_rowset->rowset_meta()->rowset_id(), hole_rowset_v3->rowset_meta()->rowset_id());
}

TEST_F(CloudMetaMgrTest, test_fill_version_holes_edge_cases) {
    CloudStorageEngine engine(EngineOptions {});
    CloudMetaMgr meta_mgr;

    // Test case 1: max_version <= 0
    {
        TabletMetaSharedPtr tablet_meta(new TabletMeta(
                1001, 2, 15673, 15674, 4, 5, TTabletSchema(), 6, {{7, 8}}, UniqueId(9, 10),
                TTabletType::TABLET_TYPE_DISK, TCompressionType::LZ4F));
        auto tablet =
                std::make_shared<CloudTablet>(engine, std::make_shared<TabletMeta>(*tablet_meta));

        std::unique_lock<std::shared_mutex> wlock(tablet->get_header_lock());
        Status status = meta_mgr.fill_version_holes(tablet.get(), 0, wlock);
        EXPECT_TRUE(status.ok());

        status = meta_mgr.fill_version_holes(tablet.get(), -1, wlock);
        EXPECT_TRUE(status.ok());

        EXPECT_EQ(tablet->tablet_meta()->all_rs_metas().size(), 0);
    }

    // Test case 2: empty tablet (no existing versions)
    {
        TabletMetaSharedPtr tablet_meta(new TabletMeta(
                1002, 2, 15673, 15674, 4, 5, TTabletSchema(), 6, {{7, 8}}, UniqueId(9, 10),
                TTabletType::TABLET_TYPE_DISK, TCompressionType::LZ4F));
        auto tablet =
                std::make_shared<CloudTablet>(engine, std::make_shared<TabletMeta>(*tablet_meta));

        std::unique_lock<std::shared_mutex> wlock(tablet->get_header_lock());
        Status status = meta_mgr.fill_version_holes(tablet.get(), 5, wlock);
        EXPECT_TRUE(status.ok());

        // Should still have no rowsets
        EXPECT_EQ(tablet->tablet_meta()->all_rs_metas().size(), 0);
    }
}

TEST_F(CloudMetaMgrTest, test_fill_version_holes_trailing_holes) {
    CloudStorageEngine engine(EngineOptions {});
    CloudMetaMgr meta_mgr;

    TabletMetaSharedPtr tablet_meta(
            new TabletMeta(1003, 2, 15673, 15674, 4, 5, TTabletSchema(), 6, {{7, 8}},
                           UniqueId(9, 10), TTabletType::TABLET_TYPE_DISK, TCompressionType::LZ4F));
    auto tablet = std::make_shared<CloudTablet>(engine, std::make_shared<TabletMeta>(*tablet_meta));

    // Add only versions 0, 1, 2 but max_version is 5 (missing 3, 4, 5)
    std::vector<RowsetSharedPtr> rowsets;
    for (int64_t version = 0; version <= 2; ++version) {
        auto rs_meta = std::make_shared<RowsetMeta>();
        rs_meta->set_tablet_id(1003);
        rs_meta->set_index_id(2);
        rs_meta->set_partition_id(15673);
        rs_meta->set_tablet_uid(UniqueId(9, 10));
        rs_meta->set_version(Version(version, version));
        rs_meta->set_rowset_type(BETA_ROWSET);
        rs_meta->set_rowset_id(engine.next_rowset_id());
        rs_meta->set_num_rows(100);
        rs_meta->set_empty(false);
        rs_meta->set_tablet_schema(tablet->tablet_schema());

        RowsetSharedPtr rowset;
        auto status = RowsetFactory::create_rowset(tablet->tablet_schema(), tablet->tablet_path(),
                                                   rs_meta, &rowset);
        EXPECT_TRUE(status.ok());
        rowsets.push_back(rowset);
    }

    // Add all rowsets to tablet
    {
        std::unique_lock<std::shared_mutex> lock(tablet->get_header_lock());
        tablet->add_rowsets(std::move(rowsets), false, lock, false);
    }

    // Initially we have 3 rowsets (versions 0, 1, 2)
    EXPECT_EQ(tablet->tablet_meta()->all_rs_metas().size(), 3);

    // Test fill_version_holes to fill trailing holes (versions 3, 4, 5)
    std::unique_lock<std::shared_mutex> wlock(tablet->get_header_lock());
    Status status = meta_mgr.fill_version_holes(tablet.get(), 5, wlock);
    EXPECT_TRUE(status.ok());

    // After filling holes, we should have 6 rowsets (versions 0, 1, 2, 3, 4, 5)
    EXPECT_EQ(tablet->tablet_meta()->all_rs_metas().size(), 6);

    // Verify all versions are present
    auto rs_metas = tablet->tablet_meta()->all_rs_metas();
    std::set<int64_t> found_versions;
    for (const auto& [_, rs_meta] : rs_metas) {
        found_versions.insert(rs_meta->version().first);
    }
    EXPECT_EQ(found_versions.size(), 6);
    for (int64_t v = 0; v <= 5; ++v) {
        EXPECT_TRUE(found_versions.contains(v)) << "Missing version " << v;
    }

    // Verify the trailing hole rowsets (versions 3, 4, 5) are empty
    for (const auto& [_, rs_meta] : rs_metas) {
        if (rs_meta->version().first >= 3) {
            EXPECT_TRUE(rs_meta->empty())
                    << "Version " << rs_meta->version().first << " should be empty";
            EXPECT_EQ(rs_meta->num_rows(), 0);
            EXPECT_EQ(rs_meta->total_disk_size(), 0);
        } else {
            EXPECT_FALSE(rs_meta->empty())
                    << "Version " << rs_meta->version().first << " should not be empty";
            EXPECT_EQ(rs_meta->num_rows(), 100);
        }
    }
}

TEST_F(CloudMetaMgrTest, test_fill_version_holes_single_hole) {
    CloudStorageEngine engine(EngineOptions {});
    CloudMetaMgr meta_mgr;

    TabletMetaSharedPtr tablet_meta(
            new TabletMeta(1004, 2, 15673, 15674, 4, 5, TTabletSchema(), 6, {{7, 8}},
                           UniqueId(9, 10), TTabletType::TABLET_TYPE_DISK, TCompressionType::LZ4F));
    auto tablet = std::make_shared<CloudTablet>(engine, std::make_shared<TabletMeta>(*tablet_meta));

    // Add versions 0, 2 (missing only version 1)
    std::vector<int64_t> versions = {0, 2};
    std::vector<RowsetSharedPtr> rowsets;
    for (int64_t version : versions) {
        auto rs_meta = std::make_shared<RowsetMeta>();
        rs_meta->set_tablet_id(1004);
        rs_meta->set_index_id(2);
        rs_meta->set_partition_id(15673);
        rs_meta->set_tablet_uid(UniqueId(9, 10));
        rs_meta->set_version(Version(version, version));
        rs_meta->set_rowset_type(BETA_ROWSET);
        rs_meta->set_rowset_id(engine.next_rowset_id());
        rs_meta->set_num_rows(100);
        rs_meta->set_empty(false);
        rs_meta->set_tablet_schema(tablet->tablet_schema());

        RowsetSharedPtr rowset;
        auto status = RowsetFactory::create_rowset(tablet->tablet_schema(), tablet->tablet_path(),
                                                   rs_meta, &rowset);
        EXPECT_TRUE(status.ok());
        rowsets.push_back(rowset);
    }

    // Add all rowsets to tablet
    {
        std::unique_lock<std::shared_mutex> lock(tablet->get_header_lock());
        tablet->add_rowsets(std::move(rowsets), false, lock, false);
    }

    // Initially we have 2 rowsets (versions 0, 2)
    EXPECT_EQ(tablet->tablet_meta()->all_rs_metas().size(), 2);

    // Test fill_version_holes to fill single hole (version 1)
    std::unique_lock<std::shared_mutex> wlock(tablet->get_header_lock());
    Status status = meta_mgr.fill_version_holes(tablet.get(), 2, wlock);
    EXPECT_TRUE(status.ok());

    // After filling holes, we should have 3 rowsets (versions 0, 1, 2)
    EXPECT_EQ(tablet->tablet_meta()->all_rs_metas().size(), 3);

    // Verify all versions are present
    auto rs_metas = tablet->tablet_meta()->all_rs_metas();
    std::set<int64_t> found_versions;
    for (const auto& [_, rs_meta] : rs_metas) {
        found_versions.insert(rs_meta->version().first);
    }
    EXPECT_EQ(found_versions.size(), 3);
    EXPECT_TRUE(found_versions.contains(0));
    EXPECT_TRUE(found_versions.contains(1));
    EXPECT_TRUE(found_versions.contains(2));

    // Verify the hole rowset (version 1) is empty
    for (const auto& [_, rs_meta] : rs_metas) {
        if (rs_meta->version().first == 1) {
            EXPECT_TRUE(rs_meta->empty());
            EXPECT_EQ(rs_meta->num_rows(), 0);
            EXPECT_EQ(rs_meta->total_disk_size(), 0);
        } else {
            EXPECT_FALSE(rs_meta->empty());
            EXPECT_EQ(rs_meta->num_rows(), 100);
        }
    }
}

TEST_F(CloudMetaMgrTest, test_fill_version_holes_multiple_consecutive_holes) {
    CloudStorageEngine engine(EngineOptions {});
    CloudMetaMgr meta_mgr;

    TabletMetaSharedPtr tablet_meta(
            new TabletMeta(1005, 2, 15673, 15674, 4, 5, TTabletSchema(), 6, {{7, 8}},
                           UniqueId(9, 10), TTabletType::TABLET_TYPE_DISK, TCompressionType::LZ4F));
    auto tablet = std::make_shared<CloudTablet>(engine, std::make_shared<TabletMeta>(*tablet_meta));

    // Add versions 0, 5 (missing 1, 2, 3, 4 - multiple consecutive holes)
    std::vector<int64_t> versions = {0, 5};
    std::vector<RowsetSharedPtr> rowsets;
    for (int64_t version : versions) {
        auto rs_meta = std::make_shared<RowsetMeta>();
        rs_meta->set_tablet_id(1005);
        rs_meta->set_index_id(2);
        rs_meta->set_partition_id(15673);
        rs_meta->set_tablet_uid(UniqueId(9, 10));
        rs_meta->set_version(Version(version, version));
        rs_meta->set_rowset_type(BETA_ROWSET);
        rs_meta->set_rowset_id(engine.next_rowset_id());
        rs_meta->set_num_rows(100);
        rs_meta->set_empty(false);
        rs_meta->set_tablet_schema(tablet->tablet_schema());

        RowsetSharedPtr rowset;
        auto status = RowsetFactory::create_rowset(tablet->tablet_schema(), tablet->tablet_path(),
                                                   rs_meta, &rowset);
        EXPECT_TRUE(status.ok());
        rowsets.push_back(rowset);
    }

    // Add all rowsets to tablet
    {
        std::unique_lock<std::shared_mutex> lock(tablet->get_header_lock());
        tablet->add_rowsets(std::move(rowsets), false, lock, false);
    }

    // Initially we have 2 rowsets (versions 0, 5)
    EXPECT_EQ(tablet->tablet_meta()->all_rs_metas().size(), 2);

    // Test fill_version_holes to fill multiple consecutive holes (versions 1, 2, 3, 4)
    std::unique_lock<std::shared_mutex> wlock(tablet->get_header_lock());
    Status status = meta_mgr.fill_version_holes(tablet.get(), 5, wlock);
    EXPECT_TRUE(status.ok());

    // After filling holes, we should have 6 rowsets (versions 0, 1, 2, 3, 4, 5)
    EXPECT_EQ(tablet->tablet_meta()->all_rs_metas().size(), 6);

    // Verify all versions are present
    auto rs_metas = tablet->tablet_meta()->all_rs_metas();
    std::set<int64_t> found_versions;
    for (const auto& [_, rs_meta] : rs_metas) {
        found_versions.insert(rs_meta->version().first);
    }
    EXPECT_EQ(found_versions.size(), 6);
    for (int64_t v = 0; v <= 5; ++v) {
        EXPECT_TRUE(found_versions.contains(v)) << "Missing version " << v;
    }

    // Verify the hole rowsets (versions 1, 2, 3, 4) are empty
    for (const auto& [_, rs_meta] : rs_metas) {
        if (rs_meta->version().first >= 1 && rs_meta->version().first <= 4) {
            EXPECT_TRUE(rs_meta->empty())
                    << "Version " << rs_meta->version().first << " should be empty";
            EXPECT_EQ(rs_meta->num_rows(), 0);
            EXPECT_EQ(rs_meta->total_disk_size(), 0);
        } else {
            EXPECT_FALSE(rs_meta->empty())
                    << "Version " << rs_meta->version().first << " should not be empty";
            EXPECT_EQ(rs_meta->num_rows(), 100);
        }
    }
}

TEST_F(CloudMetaMgrTest, test_fill_version_holes_mixed_holes) {
    CloudStorageEngine engine(EngineOptions {});
    CloudMetaMgr meta_mgr;

    TabletMetaSharedPtr tablet_meta(
            new TabletMeta(1006, 2, 15673, 15674, 4, 5, TTabletSchema(), 6, {{7, 8}},
                           UniqueId(9, 10), TTabletType::TABLET_TYPE_DISK, TCompressionType::LZ4F));
    auto tablet = std::make_shared<CloudTablet>(engine, std::make_shared<TabletMeta>(*tablet_meta));

    // Add versions 0, 2, 5, 6 (missing 1, 3, 4 and potential trailing holes up to max_version)
    std::vector<int64_t> versions = {0, 2, 5, 6};
    std::vector<RowsetSharedPtr> rowsets;
    for (int64_t version : versions) {
        auto rs_meta = std::make_shared<RowsetMeta>();
        rs_meta->set_tablet_id(1006);
        rs_meta->set_index_id(2);
        rs_meta->set_partition_id(15673);
        rs_meta->set_tablet_uid(UniqueId(9, 10));
        rs_meta->set_version(Version(version, version));
        rs_meta->set_rowset_type(BETA_ROWSET);
        rs_meta->set_rowset_id(engine.next_rowset_id());
        rs_meta->set_num_rows(100);
        rs_meta->set_empty(false);
        rs_meta->set_tablet_schema(tablet->tablet_schema());

        RowsetSharedPtr rowset;
        auto status = RowsetFactory::create_rowset(tablet->tablet_schema(), tablet->tablet_path(),
                                                   rs_meta, &rowset);
        EXPECT_TRUE(status.ok());
        rowsets.push_back(rowset);
    }

    // Add all rowsets to tablet
    {
        std::unique_lock<std::shared_mutex> lock(tablet->get_header_lock());
        tablet->add_rowsets(std::move(rowsets), false, lock, false);
    }

    // Initially we have 4 rowsets (versions 0, 2, 5, 6)
    EXPECT_EQ(tablet->tablet_meta()->all_rs_metas().size(), 4);

    // Test fill_version_holes with max_version = 8 (should fill 1, 3, 4, 7, 8)
    std::unique_lock<std::shared_mutex> wlock(tablet->get_header_lock());
    Status status = meta_mgr.fill_version_holes(tablet.get(), 8, wlock);
    EXPECT_TRUE(status.ok());

    // After filling holes, we should have 9 rowsets (versions 0-8)
    EXPECT_EQ(tablet->tablet_meta()->all_rs_metas().size(), 9);

    // Verify all versions are present
    auto rs_metas = tablet->tablet_meta()->all_rs_metas();
    std::set<int64_t> found_versions;
    for (const auto& [_, rs_meta] : rs_metas) {
        found_versions.insert(rs_meta->version().first);
    }
    EXPECT_EQ(found_versions.size(), 9);
    for (int64_t v = 0; v <= 8; ++v) {
        EXPECT_TRUE(found_versions.contains(v)) << "Missing version " << v;
    }

    // Verify the hole rowsets (versions 1, 3, 4, 7, 8) are empty
    std::set<int64_t> original_versions = {0, 2, 5, 6};
    std::set<int64_t> hole_versions = {1, 3, 4, 7, 8};
    for (const auto& [_, rs_meta] : rs_metas) {
        int64_t version = rs_meta->version().first;
        if (hole_versions.contains(version)) {
            EXPECT_TRUE(rs_meta->empty()) << "Version " << version << " should be empty";
            EXPECT_EQ(rs_meta->num_rows(), 0);
            EXPECT_EQ(rs_meta->total_disk_size(), 0);
        } else if (original_versions.contains(version)) {
            EXPECT_FALSE(rs_meta->empty()) << "Version " << version << " should not be empty";
            EXPECT_EQ(rs_meta->num_rows(), 100);
        }
    }
}

// Helper class to access private methods for testing
class CloudMetaMgrTestHelper {
public:
    static Status call_get_delete_bitmap_from_ms_by_batch(CloudMetaMgr& meta_mgr,
                                                          GetDeleteBitmapRequest& req,
                                                          GetDeleteBitmapResponse& res,
                                                          int64_t bytes_threshold) {
        return meta_mgr._get_delete_bitmap_from_ms_by_batch(req, res, bytes_threshold);
    }
};

TEST_F(CloudMetaMgrTest, test_get_delete_bitmap_from_ms_by_batch) {
    CloudMetaMgr meta_mgr;
    auto sp = SyncPoint::get_instance();

    // Test case 1: Single batch (no more data)
    {
        sp->clear_all_call_backs();
        sp->enable_processing();

        GetDeleteBitmapRequest req;
        req.set_tablet_id(12345);
        req.set_base_compaction_cnt(1);
        req.set_cumulative_compaction_cnt(2);
        req.set_cumulative_point(10);
        req.set_store_version(100);
        req.add_rowset_ids("rowset_1");
        req.add_begin_versions(1);
        req.add_end_versions(1);
        req.add_rowset_ids("rowset_2");
        req.add_begin_versions(2);
        req.add_end_versions(2);

        // Mock the _get_delete_bitmap_from_ms method to return success with no more data
        sp->set_call_back("CloudMetaMgr::_get_delete_bitmap_from_ms", [](auto&& args) {
            auto* res = try_any_cast<GetDeleteBitmapResponse*>(args[1]);

            // Simulate successful response
            res->mutable_status()->set_code(MetaServiceCode::OK);
            res->add_rowset_ids("rowset_1");
            res->add_segment_ids(0);
            res->add_versions(1);
            res->add_segment_delete_bitmaps("delete_bitmap_1");
            res->add_rowset_ids("rowset_2");
            res->add_segment_ids(0);
            res->add_versions(2);
            res->add_segment_delete_bitmaps("delete_bitmap_2");
            res->add_returned_rowset_ids("rowset_1");
            res->add_returned_rowset_ids("rowset_2");
            res->set_has_more(false); // No more data
        });

        GetDeleteBitmapResponse res;
        Status status = CloudMetaMgrTestHelper::call_get_delete_bitmap_from_ms_by_batch(
                meta_mgr, req, res, 1024);

        EXPECT_TRUE(status.ok()) << "Status: " << status;
        EXPECT_EQ(res.rowset_ids_size(), 2);
        EXPECT_EQ(res.rowset_ids(0), "rowset_1");
        EXPECT_EQ(res.rowset_ids(1), "rowset_2");
        EXPECT_EQ(res.segment_delete_bitmaps_size(), 2);

        sp->disable_processing();
        sp->clear_all_call_backs();
    }

    // Test case 2: Two batches (has_more = true)
    {
        sp->clear_all_call_backs();
        sp->enable_processing();

        GetDeleteBitmapRequest req;
        req.set_tablet_id(12345);
        req.set_base_compaction_cnt(1);
        req.set_cumulative_compaction_cnt(2);
        req.set_cumulative_point(10);
        req.set_store_version(100);
        req.add_rowset_ids("rowset_1");
        req.add_begin_versions(1);
        req.add_end_versions(1);
        req.add_rowset_ids("rowset_2");
        req.add_begin_versions(2);
        req.add_end_versions(2);
        req.add_rowset_ids("rowset_3");
        req.add_begin_versions(3);
        req.add_end_versions(3);

        int call_count = 0;
        // Mock the _get_delete_bitmap_from_ms method to simulate two batches
        sp->set_call_back("CloudMetaMgr::_get_delete_bitmap_from_ms", [&call_count](auto&& args) {
            auto* res = try_any_cast<GetDeleteBitmapResponse*>(args[1]);

            call_count++;
            res->mutable_status()->set_code(MetaServiceCode::OK);

            if (call_count == 1) {
                // First batch: return partial data with has_more=true
                res->add_rowset_ids("rowset_1");
                res->add_segment_ids(0);
                res->add_versions(1);
                res->add_segment_delete_bitmaps("delete_bitmap_1");
                res->add_returned_rowset_ids("rowset_1");
                res->set_has_more(true); // More data available
            } else if (call_count == 2) {
                // Second batch: return remaining data with has_more=false
                res->add_rowset_ids("rowset_2");
                res->add_segment_ids(0);
                res->add_versions(2);
                res->add_segment_delete_bitmaps("delete_bitmap_2");
                res->add_rowset_ids("rowset_3");
                res->add_segment_ids(0);
                res->add_versions(3);
                res->add_segment_delete_bitmaps("delete_bitmap_3");
                res->add_returned_rowset_ids("rowset_2");
                res->add_returned_rowset_ids("rowset_3");
                res->set_has_more(false); // No more data
            }
        });

        GetDeleteBitmapResponse res;
        Status status = CloudMetaMgrTestHelper::call_get_delete_bitmap_from_ms_by_batch(
                meta_mgr, req, res, 512);

        EXPECT_TRUE(status.ok()) << "Status: " << status;
        EXPECT_EQ(call_count, 2); // Should have made 2 RPC calls
        EXPECT_EQ(res.rowset_ids_size(), 3);
        EXPECT_EQ(res.rowset_ids(0), "rowset_1");
        EXPECT_EQ(res.rowset_ids(1), "rowset_2");
        EXPECT_EQ(res.rowset_ids(2), "rowset_3");
        EXPECT_EQ(res.segment_delete_bitmaps_size(), 3);

        sp->disable_processing();
        sp->clear_all_call_backs();
    }

    // Test case 3: Multiple batches (more than 2 batches)
    {
        sp->clear_all_call_backs();
        sp->enable_processing();

        GetDeleteBitmapRequest req;
        req.set_tablet_id(12345);
        req.set_base_compaction_cnt(1);
        req.set_cumulative_compaction_cnt(2);
        req.set_cumulative_point(10);
        req.set_store_version(100);
        // Add 5 rowsets to test multiple batches
        for (int i = 1; i <= 5; ++i) {
            req.add_rowset_ids("rowset_" + std::to_string(i));
            req.add_begin_versions(i);
            req.add_end_versions(i);
        }

        int call_count = 0;
        // Mock the _get_delete_bitmap_from_ms method to simulate 3 batches
        sp->set_call_back("CloudMetaMgr::_get_delete_bitmap_from_ms", [&call_count](auto&& args) {
            auto* res = try_any_cast<GetDeleteBitmapResponse*>(args[1]);

            call_count++;
            res->mutable_status()->set_code(MetaServiceCode::OK);

            if (call_count == 1) {
                // First batch: return rowset_1 and rowset_2
                res->add_rowset_ids("rowset_1");
                res->add_segment_ids(0);
                res->add_versions(1);
                res->add_segment_delete_bitmaps("delete_bitmap_1");
                res->add_rowset_ids("rowset_2");
                res->add_segment_ids(0);
                res->add_versions(2);
                res->add_segment_delete_bitmaps("delete_bitmap_2");
                res->add_returned_rowset_ids("rowset_1");
                res->add_returned_rowset_ids("rowset_2");
                res->set_has_more(true); // More data available
            } else if (call_count == 2) {
                // Second batch: return rowset_3 and rowset_4
                res->add_rowset_ids("rowset_3");
                res->add_segment_ids(0);
                res->add_versions(3);
                res->add_segment_delete_bitmaps("delete_bitmap_3");
                res->add_rowset_ids("rowset_4");
                res->add_segment_ids(0);
                res->add_versions(4);
                res->add_segment_delete_bitmaps("delete_bitmap_4");
                res->add_returned_rowset_ids("rowset_3");
                res->add_returned_rowset_ids("rowset_4");
                res->set_has_more(true); // Still more data available
            } else if (call_count == 3) {
                // Third batch: return rowset_5
                res->add_rowset_ids("rowset_5");
                res->add_segment_ids(0);
                res->add_versions(5);
                res->add_segment_delete_bitmaps("delete_bitmap_5");
                res->add_returned_rowset_ids("rowset_5");
                res->set_has_more(false); // No more data
            }
        });

        GetDeleteBitmapResponse res;
        Status status = CloudMetaMgrTestHelper::call_get_delete_bitmap_from_ms_by_batch(
                meta_mgr, req, res, 256); // Small threshold to force multiple batches

        EXPECT_TRUE(status.ok()) << "Status: " << status;
        EXPECT_EQ(call_count, 3); // Should have made 3 RPC calls
        EXPECT_EQ(res.rowset_ids_size(), 5);
        EXPECT_EQ(res.segment_delete_bitmaps_size(), 5);
        for (int i = 1; i <= 5; ++i) {
            EXPECT_EQ(res.rowset_ids(i - 1), "rowset_" + std::to_string(i));
            EXPECT_EQ(res.segment_delete_bitmaps(i - 1), "delete_bitmap_" + std::to_string(i));
        }

        sp->disable_processing();
        sp->clear_all_call_backs();
    }

    // Test case 4: RPC failure
    {
        sp->clear_all_call_backs();
        sp->enable_processing();

        GetDeleteBitmapRequest req;
        req.set_tablet_id(12345);
        req.add_rowset_ids("rowset_1");
        req.add_begin_versions(1);
        req.add_end_versions(1);

        // Mock to simulate connection failure or service unavailable
        sp->set_call_back("CloudMetaMgr::_get_delete_bitmap_from_ms", [](auto&& args) {
            auto* res = try_any_cast<GetDeleteBitmapResponse*>(args[1]);
            res->mutable_status()->set_code(MetaServiceCode::TABLET_NOT_FOUND);
            res->mutable_status()->set_msg("Tablet not found");
        });

        GetDeleteBitmapResponse res;
        Status status = CloudMetaMgrTestHelper::call_get_delete_bitmap_from_ms_by_batch(
                meta_mgr, req, res, 1024);

        // The method should handle the error from _get_delete_bitmap_from_ms
        // Since _get_delete_bitmap_from_ms_by_batch calls RETURN_IF_ERROR, it should propagate the error
        EXPECT_FALSE(status.ok());

        sp->disable_processing();
        sp->clear_all_call_backs();
    }

    // Test case 5: V2 delete bitmap handling with multiple batches
    {
        sp->clear_all_call_backs();
        sp->enable_processing();

        GetDeleteBitmapRequest req;
        req.set_tablet_id(12345);
        req.add_rowset_ids("rowset_1");
        req.add_begin_versions(1);
        req.add_end_versions(1);
        req.add_rowset_ids("rowset_2");
        req.add_begin_versions(2);
        req.add_end_versions(2);

        int call_count = 0;
        // Mock to return v2 delete bitmap data across multiple batches
        sp->set_call_back("CloudMetaMgr::_get_delete_bitmap_from_ms", [&call_count](auto&& args) {
            auto* res = try_any_cast<GetDeleteBitmapResponse*>(args[1]);

            call_count++;
            res->mutable_status()->set_code(MetaServiceCode::OK);

            if (call_count == 1) {
                // First batch: v2 delete bitmap data with actual content (stored in FDB)
                res->add_delta_rowset_ids("delta_rowset_1");
                auto* storage1 = res->add_delete_bitmap_storages();
                storage1->set_store_in_fdb(true); // Has delete bitmap data
                auto* delete_bitmap1 = storage1->mutable_delete_bitmap();
                delete_bitmap1->add_rowset_ids("rowset_1");
                delete_bitmap1->add_segment_ids(0);
                delete_bitmap1->add_versions(1);
                delete_bitmap1->add_segment_delete_bitmaps("v2_bitmap_1");
                res->add_returned_rowset_ids("rowset_1");
                res->set_has_more(true);
            } else if (call_count == 2) {
                // Second batch: v2 delete bitmap without local data
                res->add_delta_rowset_ids("delta_rowset_2");
                auto* storage2 = res->add_delete_bitmap_storages();
                storage2->set_store_in_fdb(false); // No local bitmap data
                res->add_returned_rowset_ids("rowset_2");
                res->set_has_more(false);
            }
        });

        GetDeleteBitmapResponse res;
        Status status = CloudMetaMgrTestHelper::call_get_delete_bitmap_from_ms_by_batch(
                meta_mgr, req, res, 512);

        EXPECT_TRUE(status.ok()) << "Status: " << status;
        EXPECT_EQ(call_count, 2);
        EXPECT_EQ(res.delta_rowset_ids_size(), 2);
        EXPECT_EQ(res.delta_rowset_ids(0), "delta_rowset_1");
        EXPECT_EQ(res.delta_rowset_ids(1), "delta_rowset_2");
        EXPECT_EQ(res.delete_bitmap_storages_size(), 2);

        // First storage: store_in_fdb=true, has delete bitmap data
        EXPECT_TRUE(res.delete_bitmap_storages(0).store_in_fdb());
        EXPECT_TRUE(res.delete_bitmap_storages(0).has_delete_bitmap());
        EXPECT_EQ(res.delete_bitmap_storages(0).delete_bitmap().rowset_ids(0), "rowset_1");
        EXPECT_EQ(res.delete_bitmap_storages(0).delete_bitmap().segment_delete_bitmaps(0),
                  "v2_bitmap_1");

        // Second storage: store_in_fdb=false, no local bitmap data
        EXPECT_FALSE(res.delete_bitmap_storages(1).store_in_fdb());
        EXPECT_FALSE(res.delete_bitmap_storages(1).has_delete_bitmap());

        sp->disable_processing();
        sp->clear_all_call_backs();
    }

    // Test case 6: Mixed V1 and V2 delete bitmap handling
    {
        sp->clear_all_call_backs();
        sp->enable_processing();

        GetDeleteBitmapRequest req;
        req.set_tablet_id(12345);
        req.add_rowset_ids("rowset_1");
        req.add_begin_versions(1);
        req.add_end_versions(1);
        req.add_rowset_ids("rowset_2");
        req.add_begin_versions(2);
        req.add_end_versions(2);
        req.add_rowset_ids("rowset_3");
        req.add_begin_versions(3);
        req.add_end_versions(3);

        int call_count = 0;
        // Mock to return both v1 and v2 delete bitmap data in multiple batches
        sp->set_call_back("CloudMetaMgr::_get_delete_bitmap_from_ms", [&call_count](auto&& args) {
            auto* res = try_any_cast<GetDeleteBitmapResponse*>(args[1]);

            call_count++;
            res->mutable_status()->set_code(MetaServiceCode::OK);

            if (call_count == 1) {
                // First batch: return both v1 and v2 data
                // V1 delete bitmap data
                res->add_rowset_ids("rowset_1");
                res->add_segment_ids(0);
                res->add_versions(1);
                res->add_segment_delete_bitmaps("v1_delete_bitmap_1");

                // V2 delete bitmap data - has data when store_in_fdb=true
                res->add_delta_rowset_ids("delta_rowset_1");
                auto* storage1 = res->add_delete_bitmap_storages();
                storage1->set_store_in_fdb(true); // Has delete bitmap data
                auto* delete_bitmap1 = storage1->mutable_delete_bitmap();
                delete_bitmap1->add_rowset_ids("rowset_1");
                delete_bitmap1->add_segment_ids(0);
                delete_bitmap1->add_versions(1);
                delete_bitmap1->add_segment_delete_bitmaps("v2_bitmap_1");

                res->add_returned_rowset_ids("rowset_1");
                res->set_has_more(true);
            } else if (call_count == 2) {
                // Second batch: return more mixed v1 and v2 data
                // More V1 data
                res->add_rowset_ids("rowset_2");
                res->add_segment_ids(1);
                res->add_versions(2);
                res->add_segment_delete_bitmaps("v1_delete_bitmap_2");
                res->add_rowset_ids("rowset_3");
                res->add_segment_ids(0);
                res->add_versions(3);
                res->add_segment_delete_bitmaps("v1_delete_bitmap_3");

                // More V2 data - mixed scenarios
                res->add_delta_rowset_ids("delta_rowset_2");
                auto* storage2 = res->add_delete_bitmap_storages();
                storage2->set_store_in_fdb(true); // Has delete bitmap data
                auto* delete_bitmap2 = storage2->mutable_delete_bitmap();
                delete_bitmap2->add_rowset_ids("rowset_2");
                delete_bitmap2->add_segment_ids(1);
                delete_bitmap2->add_versions(2);
                delete_bitmap2->add_segment_delete_bitmaps("v2_bitmap_2");
                res->add_delta_rowset_ids("delta_rowset_3");
                auto* storage3 = res->add_delete_bitmap_storages();
                storage3->set_store_in_fdb(false); // No local bitmap data

                res->add_returned_rowset_ids("rowset_2");
                res->add_returned_rowset_ids("rowset_3");
                res->set_has_more(false);
            }
        });

        GetDeleteBitmapResponse res;
        Status status = CloudMetaMgrTestHelper::call_get_delete_bitmap_from_ms_by_batch(
                meta_mgr, req, res, 512);

        EXPECT_TRUE(status.ok()) << "Status: " << status;
        EXPECT_EQ(call_count, 2);

        // Verify V1 delete bitmap data was merged correctly
        EXPECT_EQ(res.rowset_ids_size(), 3);
        EXPECT_EQ(res.rowset_ids(0), "rowset_1");
        EXPECT_EQ(res.rowset_ids(1), "rowset_2");
        EXPECT_EQ(res.rowset_ids(2), "rowset_3");
        EXPECT_EQ(res.segment_ids_size(), 3);
        EXPECT_EQ(res.segment_ids(0), 0);
        EXPECT_EQ(res.segment_ids(1), 1);
        EXPECT_EQ(res.segment_ids(2), 0);
        EXPECT_EQ(res.versions_size(), 3);
        EXPECT_EQ(res.versions(0), 1);
        EXPECT_EQ(res.versions(1), 2);
        EXPECT_EQ(res.versions(2), 3);
        EXPECT_EQ(res.segment_delete_bitmaps_size(), 3);
        EXPECT_EQ(res.segment_delete_bitmaps(0), "v1_delete_bitmap_1");
        EXPECT_EQ(res.segment_delete_bitmaps(1), "v1_delete_bitmap_2");
        EXPECT_EQ(res.segment_delete_bitmaps(2), "v1_delete_bitmap_3");

        // Verify V2 delete bitmap data was merged correctly
        EXPECT_EQ(res.delta_rowset_ids_size(), 3);
        EXPECT_EQ(res.delta_rowset_ids(0), "delta_rowset_1");
        EXPECT_EQ(res.delta_rowset_ids(1), "delta_rowset_2");
        EXPECT_EQ(res.delta_rowset_ids(2), "delta_rowset_3");
        EXPECT_EQ(res.delete_bitmap_storages_size(), 3);

        // First storage: store_in_fdb=true, has delete bitmap data
        EXPECT_TRUE(res.delete_bitmap_storages(0).store_in_fdb());
        EXPECT_TRUE(res.delete_bitmap_storages(0).has_delete_bitmap());
        EXPECT_EQ(res.delete_bitmap_storages(0).delete_bitmap().rowset_ids(0), "rowset_1");
        EXPECT_EQ(res.delete_bitmap_storages(0).delete_bitmap().segment_delete_bitmaps(0),
                  "v2_bitmap_1");

        // Second storage: store_in_fdb=true, has delete bitmap data
        EXPECT_TRUE(res.delete_bitmap_storages(1).store_in_fdb());
        EXPECT_TRUE(res.delete_bitmap_storages(1).has_delete_bitmap());
        EXPECT_EQ(res.delete_bitmap_storages(1).delete_bitmap().rowset_ids(0), "rowset_2");
        EXPECT_EQ(res.delete_bitmap_storages(1).delete_bitmap().segment_delete_bitmaps(0),
                  "v2_bitmap_2");

        // Third storage: store_in_fdb=false, no local bitmap data
        EXPECT_FALSE(res.delete_bitmap_storages(2).store_in_fdb());
        EXPECT_FALSE(res.delete_bitmap_storages(2).has_delete_bitmap());

        sp->disable_processing();
        sp->clear_all_call_backs();
    }
}

} // namespace doris
