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
#include <set>

#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
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
    std::vector<std::function<Status()>> tasks {
        []{ bthread_usleep(20000); return Status::OK(); },
        []{ bthread_usleep(20000); return Status::OK(); },
        []{ bthread_usleep(20000); return Status::OK(); },
        []{ bthread_usleep(20000); return Status::OK(); },
        []{ bthread_usleep(20000); return Status::OK(); },
        []{ bthread_usleep(20000); return Status::OK(); },
        []{ bthread_usleep(20000); return Status::OK(); },
    };
    {
        auto start = steady_clock::now();
        EXPECT_TRUE(bthread_fork_join(tasks, 3).ok());
        auto end = steady_clock::now();
        auto elapsed = duration_cast<milliseconds>(end - start).count();
        EXPECT_GT(elapsed, 40); // at least 2 rounds running for 7 tasks
    }
    {
        std::future<Status> fut;
        auto start = steady_clock::now();
        auto t = tasks;
        EXPECT_TRUE(bthread_fork_join(std::move(t), 3, &fut).ok()); // return immediately
        auto end = steady_clock::now();
        auto elapsed = duration_cast<milliseconds>(end - start).count();
        EXPECT_LE(elapsed, 40); // async
        EXPECT_TRUE(fut.get().ok());
        end = steady_clock::now();
        elapsed = duration_cast<milliseconds>(end - start).count();
        EXPECT_GT(elapsed, 40); // at least 2 rounds running for 7 tasks
    }

    // make the first batch fail fast
    tasks.insert(tasks.begin(), []{ bthread_usleep(20000); return Status::InternalError<false>("error"); });
    {
        auto start = steady_clock::now();
        EXPECT_FALSE(bthread_fork_join(tasks, 3).ok());
        auto end = steady_clock::now();
        auto elapsed = duration_cast<milliseconds>(end - start).count();
        EXPECT_LE(elapsed, 40); // at most 1 round running for 7 tasks
    }
    {
        std::future<Status> fut;
        auto start = steady_clock::now();
        auto t = tasks;
        EXPECT_TRUE(bthread_fork_join(std::move(t), 3, &fut).ok()); // return immediately
        auto end = steady_clock::now();
        auto elapsed = duration_cast<milliseconds>(end - start).count();
        EXPECT_LE(elapsed, 40); // async
        EXPECT_FALSE(fut.get().ok());
        end = steady_clock::now();
        elapsed = duration_cast<milliseconds>(end - start).count();
        EXPECT_LE(elapsed, 40); // at most 1 round running for 7 tasks
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
    for (const auto& rs_meta : tablet->tablet_meta()->all_rs_metas()) {
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
    for (const auto& rs_meta : rs_metas) {
        found_versions.insert(rs_meta->version().first);
    }
    EXPECT_EQ(found_versions.size(), 5);
    EXPECT_TRUE(found_versions.contains(0));
    EXPECT_TRUE(found_versions.contains(1));
    EXPECT_TRUE(found_versions.contains(2));
    EXPECT_TRUE(found_versions.contains(3));
    EXPECT_TRUE(found_versions.contains(4));

    // Verify the hole rowsets (versions 1 and 3) are empty
    for (const auto& rs_meta : rs_metas) {
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
    for (const auto& rs_meta : rs_metas) {
        found_versions.insert(rs_meta->version().first);
    }
    EXPECT_EQ(found_versions.size(), 6);
    for (int64_t v = 0; v <= 5; ++v) {
        EXPECT_TRUE(found_versions.contains(v)) << "Missing version " << v;
    }

    // Verify the trailing hole rowsets (versions 3, 4, 5) are empty
    for (const auto& rs_meta : rs_metas) {
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
    for (const auto& rs_meta : rs_metas) {
        found_versions.insert(rs_meta->version().first);
    }
    EXPECT_EQ(found_versions.size(), 3);
    EXPECT_TRUE(found_versions.contains(0));
    EXPECT_TRUE(found_versions.contains(1));
    EXPECT_TRUE(found_versions.contains(2));

    // Verify the hole rowset (version 1) is empty
    for (const auto& rs_meta : rs_metas) {
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
    for (const auto& rs_meta : rs_metas) {
        found_versions.insert(rs_meta->version().first);
    }
    EXPECT_EQ(found_versions.size(), 6);
    for (int64_t v = 0; v <= 5; ++v) {
        EXPECT_TRUE(found_versions.contains(v)) << "Missing version " << v;
    }

    // Verify the hole rowsets (versions 1, 2, 3, 4) are empty
    for (const auto& rs_meta : rs_metas) {
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
    for (const auto& rs_meta : rs_metas) {
        found_versions.insert(rs_meta->version().first);
    }
    EXPECT_EQ(found_versions.size(), 9);
    for (int64_t v = 0; v <= 8; ++v) {
        EXPECT_TRUE(found_versions.contains(v)) << "Missing version " << v;
    }

    // Verify the hole rowsets (versions 1, 3, 4, 7, 8) are empty
    std::set<int64_t> original_versions = {0, 2, 5, 6};
    std::set<int64_t> hole_versions = {1, 3, 4, 7, 8};
    for (const auto& rs_meta : rs_metas) {
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

} // namespace doris
