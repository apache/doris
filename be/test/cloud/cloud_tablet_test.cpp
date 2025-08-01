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

#include "cloud/cloud_tablet.h"

#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <ranges>

#include "cloud/cloud_storage_engine.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/tablet_meta.h"
#include "util/uid_util.h"

namespace doris {

class TestFreshnessTolerance : public testing::Test {
public:
    TestFreshnessTolerance() : _engine(CloudStorageEngine(EngineOptions {})) {}

    void SetUp() override {
        _tablet_meta.reset(new TabletMeta(1, 2, 15673, 15674, 4, 5, TTabletSchema(), 6, {{7, 8}},
                                          UniqueId(9, 10), TTabletType::TABLET_TYPE_DISK,
                                          TCompressionType::LZ4F));
    }
    void TearDown() override {}

    RowsetSharedPtr create_rowset(Version version, bool warmed_up,
                                  int64_t newest_write_timestamp = ::time(nullptr) - 100) {
        auto rs_meta = std::make_shared<RowsetMeta>();
        rs_meta->set_rowset_type(BETA_ROWSET);
        rs_meta->set_version(version);
        rs_meta->set_newest_write_timestamp(newest_write_timestamp);
        RowsetSharedPtr rowset;
        Status st = RowsetFactory::create_rowset(nullptr, "", rs_meta, &rowset);
        if (!st.ok()) {
            return nullptr;
        }
        rowset->set_is_warmed_up(warmed_up);
        return rowset;
    }

    CloudTabletSPtr create_tablet_with_initial_rowsets(int max_version, bool is_mow = false) {
        CloudTabletSPtr tablet =
                std::make_shared<CloudTablet>(_engine, std::make_shared<TabletMeta>(*_tablet_meta));
        tablet->tablet_meta()->set_enable_unique_key_merge_on_write(is_mow);
        std::vector<RowsetSharedPtr> rowsets;
        rowsets.emplace_back(create_rowset(Version {0, 1}, true));
        for (int ver = 2; ver <= max_version; ver++) {
            RowsetMetaSharedPtr ptr1(new RowsetMeta());
            rowsets.emplace_back(create_rowset(Version {ver, ver}, true));
        }
        {
            std::unique_lock wlock {tablet->get_header_lock()};
            tablet->add_rowsets(rowsets, false, wlock, false);
        }
        return tablet;
    }

    void add_new_version_rowset(CloudTabletSPtr tablet, int64_t version, bool warmed_up,
                                int64_t newest_write_timestamp) {
        auto rowset = create_rowset(Version {version, version}, warmed_up, newest_write_timestamp);
        std::unique_lock wlock {tablet->get_header_lock()};
        tablet->add_rowsets({rowset}, false, wlock, false);
    }

    void do_cumu_compaction(CloudTabletSPtr tablet, int64_t start_version, int64_t end_version,
                            bool warmed_up, int64_t newest_write_timestamp) {
        std::unique_lock wrlock {tablet->get_header_lock()};
        std::vector<RowsetSharedPtr> input_rowsets;
        auto output_rowset = create_rowset(Version {start_version, end_version}, warmed_up,
                                           newest_write_timestamp);
        std::ranges::copy_if(std::views::values(tablet->rowset_map()),
                             std::back_inserter(input_rowsets), [=](const RowsetSharedPtr& rowset) {
                                 return rowset->version().first >= start_version &&
                                        rowset->version().first <= end_version;
                             });
        if (input_rowsets.size() == 1) {
            tablet->add_rowsets({output_rowset}, true, wrlock);
        } else {
            tablet->delete_rowsets(input_rowsets, wrlock);
            tablet->add_rowsets({output_rowset}, false, wrlock);
        }
    }

    void check_capture_result(CloudTabletSPtr tablet, Version spec_version,
                              int64_t query_freshness_tolerance_ms,
                              const std::vector<Version>& expected_versions) {
        std::vector<RowSetSplits> rs_splits;
        auto st = tablet->capture_rs_readers_with_freshness_tolerance(
                spec_version, &rs_splits, false, query_freshness_tolerance_ms);
        ASSERT_TRUE(st.ok());
        auto dump_versions = [](const std::vector<Version>& expected_versions,
                                const std::vector<RowSetSplits>& splits) {
            std::vector<std::string> expected_str;
            for (const auto& version : expected_versions) {
                expected_str.push_back(version.to_string());
            }
            std::vector<std::string> versions;
            for (const auto& split : splits) {
                versions.push_back(split.rs_reader->rowset()->version().to_string());
            }
            return fmt::format("expected_versions: {}, actual_versions: {}",
                               fmt::join(expected_str, ", "), fmt::join(versions, ", "));
        };
        ASSERT_EQ(rs_splits.size(), expected_versions.size())
                << dump_versions(expected_versions, rs_splits);
        for (size_t i = 0; i < rs_splits.size(); i++) {
            ASSERT_EQ(rs_splits[i].rs_reader->rowset()->version(), expected_versions[i])
                    << dump_versions(expected_versions, rs_splits);
        }
    }

protected:
    std::string _json_rowset_meta;
    TabletMetaSharedPtr _tablet_meta;

private:
    CloudStorageEngine _engine;
};

TEST_F(TestFreshnessTolerance, testCaputure_1) {
    /*
                                   now-10s                     now
                                                                  
                                    │           10s             │ 
                                    ◄───────────────────────────┤ 
┌────────┐  ┌─────────┐  ┌─────────┐│  ┌────────┐   ┌───────┐   │ 
│in cache│  │ in cache│  │in cache ││  │        │   │       │   │ 
│        │  │         │  │         ││  │        │   │       │   │ 
│ [2-10] │  │ [11-15] │  │ [16-16] ││  │ [17-17]│   │[18-18]│   │ 
└────────┘  └─────────┘  └─────────┘│  └────────┘   └───────┘   │ 
                                    │                           │ 
 now-40s      now-20s      now-15s  │   now-7s        now-3s    │ 
                                    │                           │ 
                                    │                           │ 
 return: [2-10],[11-15],[16-16]
*/
    auto tablet = create_tablet_with_initial_rowsets(15);
    do_cumu_compaction(tablet, 2, 10, true, ::time(nullptr) - 40);
    do_cumu_compaction(tablet, 11, 15, true, ::time(nullptr) - 20);
    add_new_version_rowset(tablet, 16, true, ::time(nullptr) - 15);
    add_new_version_rowset(tablet, 17, false, ::time(nullptr) - 7);
    add_new_version_rowset(tablet, 18, false, ::time(nullptr) - 3);

    std::string compaction_status;
    tablet->get_compaction_status(&compaction_status);
    std::cout << compaction_status << std::endl;

    int64_t query_freshness_tolerance_ms = 10000; // 10s
    std::vector<Version> expected_versions = {{0, 1}, {2, 10}, {11, 15}, {16, 16}};
    check_capture_result(tablet, Version {0, 18}, query_freshness_tolerance_ms, expected_versions);
}

TEST_F(TestFreshnessTolerance, testCaputure_2) {
    /*
                                   now-10s                     now
                                                                  
                                    │           10s             │ 
                                    ◄───────────────────────────┤ 
┌────────┐  ┌─────────┐  ┌─────────┐│  ┌────────┐   ┌───────┐   │ 
│in cache│  │ in cache│  │         ││  │        │   │       │   │ 
│        │  │         │  │         ││  │        │   │       │   │ 
│ [2-10] │  │ [11-15] │  │ [16-16] ││  │ [17-17]│   │[18-18]│   │ 
└────────┘  └─────────┘  └─────────┘│  └────────┘   └───────┘   │ 
                                    │                           │ 
 now-40s      now-20s      now-15s  │   now-7s        now-3s    │ 
                                    │                           │ 
                                    │                           │ 
 return: [2-10],[11-15],[16-16],[17-17],[18-18]
 NOTE: rowset[16-16] must be visible becasue it's within the query freshness tolerance time limit.
       However, since the data files of rowset[16-16] is not in the cache, there is no difference between
       capturing up to version 16 and capturing up to version 18
*/
    auto tablet = create_tablet_with_initial_rowsets(15);
    do_cumu_compaction(tablet, 2, 10, true, ::time(nullptr) - 40);
    do_cumu_compaction(tablet, 11, 15, true, ::time(nullptr) - 20);
    add_new_version_rowset(tablet, 16, false, ::time(nullptr) - 15);
    add_new_version_rowset(tablet, 17, false, ::time(nullptr) - 7);
    add_new_version_rowset(tablet, 18, false, ::time(nullptr) - 3);

    std::string compaction_status;
    tablet->get_compaction_status(&compaction_status);
    std::cout << compaction_status << std::endl;

    int64_t query_freshness_tolerance_ms = 10000; // 10s
    std::vector<Version> expected_versions = {{0, 1},   {2, 10},  {11, 15},
                                              {16, 16}, {17, 17}, {18, 18}};
    check_capture_result(tablet, Version {0, 18}, query_freshness_tolerance_ms, expected_versions);
}

TEST_F(TestFreshnessTolerance, testCaputure_3) {
    /*
                                   now-10s                     now
                                                                  
                                    │           10s             │ 
                                    ◄───────────────────────────┤ 
┌────────┐  ┌─────────┐  ┌─────────┐│  ┌────────┐   ┌───────┐   │ 
│in cache│  │ in cache│  │in cache ││  │in cache│   │       │   │ 
│        │  │         │  │         ││  │        │   │       │   │ 
│ [2-10] │  │ [11-15] │  │ [16-16] ││  │ [17-17]│   │[18-18]│   │ 
└────────┘  └─────────┘  └─────────┘│  └────────┘   └───────┘   │ 
                                    │                           │ 
 now-40s      now-20s      now-15s  │   now-7s        now-3s    │ 
                                    │                           │ 
                                    │                           │ 
 return: [2-10],[11-15],[16-16],[17-17]
*/
    auto tablet = create_tablet_with_initial_rowsets(15);
    do_cumu_compaction(tablet, 2, 10, true, ::time(nullptr) - 40);
    do_cumu_compaction(tablet, 11, 15, true, ::time(nullptr) - 20);
    add_new_version_rowset(tablet, 16, true, ::time(nullptr) - 15);
    add_new_version_rowset(tablet, 17, true, ::time(nullptr) - 7);
    add_new_version_rowset(tablet, 18, false, ::time(nullptr) - 3);

    std::string compaction_status;
    tablet->get_compaction_status(&compaction_status);
    std::cout << compaction_status << std::endl;

    int64_t query_freshness_tolerance_ms = 10000; // 10s
    std::vector<Version> expected_versions = {{0, 1}, {2, 10}, {11, 15}, {16, 16}, {17, 17}};
    check_capture_result(tablet, Version {0, 18}, query_freshness_tolerance_ms, expected_versions);
}

TEST_F(TestFreshnessTolerance, testCaputure_4) {
    /*
                                        now-10s                    now     
                                           │          10s           │      
                                           ◄────────────────────────┼      
                                           │                        │      
    ┌────────┐                             │┌────────┐    ┌───────┐ │      
    │in cache│                             ││        │    │       │ │      
    │        │                             ││        │    │       │ │      
    │ [2-10] │                             ││ [11-17]│    │[18-18]│ │      
    └────────┘                             │└────────┘    └───────┘ │      
                                           │                        │      
     now-40s                               │ now-1s         now-3s  │      
┌───────────────────────────────────────────────────────────────────────┐  
│                                          │                        │   │  
│  stale rowsets                           │                        │   │  
│                 ┌─────────┐  ┌─────────┐ │┌────────┐              │   │  
│                 │ in cache│  │in cache │ ││        │              │   │  
│                 │         │  │         │ ││        │              │   │  
│                 │ [11-15] │  │ [16-16] │ ││ [17-17]│              │   │  
│                 └─────────┘  └─────────┘ │└────────┘              │   │  
│                                          │                        │   │  
│                   now-20s      now-15s   │ now-7s                 │   │  
└───────────────────────────────────────────────────────────────────────┘  
                                           │                        │      
                                                                           
 return: [2-10],[11-15],[16-16]                                                                     
 */
    auto tablet = create_tablet_with_initial_rowsets(15);
    do_cumu_compaction(tablet, 2, 10, true, ::time(nullptr) - 40);
    do_cumu_compaction(tablet, 11, 15, true, ::time(nullptr) - 20);
    add_new_version_rowset(tablet, 16, true, ::time(nullptr) - 15);
    add_new_version_rowset(tablet, 17, false, ::time(nullptr) - 7);
    add_new_version_rowset(tablet, 18, false, ::time(nullptr) - 3);
    do_cumu_compaction(tablet, 11, 17, false, ::time(nullptr) - 1);

    std::string compaction_status;
    tablet->get_compaction_status(&compaction_status);
    std::cout << compaction_status << std::endl;

    int64_t query_freshness_tolerance_ms = 10000; // 10s
    std::vector<Version> expected_versions = {{0, 1}, {2, 10}, {11, 15}, {16, 16}};
    check_capture_result(tablet, Version {0, 18}, query_freshness_tolerance_ms, expected_versions);
}

TEST_F(TestFreshnessTolerance, testCaputure_5) {
    /*
                                        now-10s                    now     
                                           │          10s           │      
                                           ◄────────────────────────┼      
                                           │                        │      
    ┌────────┐                             │┌────────┐    ┌───────┐ │      
    │in cache│                             ││        │    │       │ │      
    │        │                             ││        │    │       │ │      
    │ [2-10] │                             ││ [11-17]│    │[18-18]│ │      
    └────────┘                             │└────────┘    └───────┘ │      
                                           │                        │      
     now-40s                               │ now-1s         now-3s  │      
┌───────────────────────────────────────────────────────────────────────┐  
│                                          │                        │   │  
│  stale rowsets                           │                        │   │  
│                 ┌─────────┐  ┌─────────┐ │┌────────┐              │   │  
│                 │ in cache│  │in cache │ ││in cache│              │   │  
│                 │         │  │         │ ││        │              │   │  
│                 │ [11-15] │  │ [16-16] │ ││ [17-17]│              │   │  
│                 └─────────┘  └─────────┘ │└────────┘              │   │  
│                                          │                        │   │  
│                   now-20s      now-15s   │ now-7s                 │   │  
└───────────────────────────────────────────────────────────────────────┘  
                                           │                        │      
                                                                           
 return: [2-10],[11-15],[16-16],[17-17]                                                                 
 */
    auto tablet = create_tablet_with_initial_rowsets(15);
    do_cumu_compaction(tablet, 2, 10, true, ::time(nullptr) - 40);
    do_cumu_compaction(tablet, 11, 15, true, ::time(nullptr) - 20);
    add_new_version_rowset(tablet, 16, true, ::time(nullptr) - 15);
    add_new_version_rowset(tablet, 17, true, ::time(nullptr) - 7);
    add_new_version_rowset(tablet, 18, false, ::time(nullptr) - 3);
    do_cumu_compaction(tablet, 11, 17, false, ::time(nullptr) - 1);

    std::string compaction_status;
    tablet->get_compaction_status(&compaction_status);
    std::cout << compaction_status << std::endl;

    int64_t query_freshness_tolerance_ms = 10000; // 10s
    std::vector<Version> expected_versions = {{0, 1}, {2, 10}, {11, 15}, {16, 16}, {17, 17}};
    check_capture_result(tablet, Version {0, 18}, query_freshness_tolerance_ms, expected_versions);
}

TEST_F(TestFreshnessTolerance, testCaputure_6) {
    /*
                                        now-10s                    now     
                                           │          10s           │      
                                           ◄────────────────────────┼      
                                           │                        │      
    ┌────────┐                             │┌────────┐    ┌────────┐│      
    │in cache│                             ││        │    │        ││      
    │        │                             ││        │    │in cache││      
    │ [2-10] │                             ││ [17-17]│    │[18-18] ││      
    └────────┘                             │└────────┘    └────────┘│      
                                           │                        │      
     now-40s                               │ now-1s         now-3s  │      
┌───────────────────────────────────────────────────────────────────────┐  
│                                          │                        │   │  
│  stale rowsets                           │                        │   │  
│                 ┌─────────┐  ┌─────────┐ │┌────────┐              │   │  
│                 │ in cache│  │in cache │ ││in cache│              │   │  
│                 │         │  │         │ ││        │              │   │  
│                 │ [11-15] │  │ [16-16] │ ││ [17-17]│              │   │  
│                 └─────────┘  └─────────┘ │└────────┘              │   │  
│                                          │                        │   │  
│                   now-20s      now-15s   │ now-7s                 │   │  
└───────────────────────────────────────────────────────────────────────┘  
                                           │                        │      
 return: [2-10],[11-15],[16-16],[17-17],[18-18]                                                                     
 */
    auto tablet = create_tablet_with_initial_rowsets(15);
    do_cumu_compaction(tablet, 2, 10, true, ::time(nullptr) - 40);
    do_cumu_compaction(tablet, 11, 15, true, ::time(nullptr) - 20);
    add_new_version_rowset(tablet, 16, true, ::time(nullptr) - 15);
    add_new_version_rowset(tablet, 17, true, ::time(nullptr) - 7);
    add_new_version_rowset(tablet, 18, true, ::time(nullptr) - 3);
    do_cumu_compaction(tablet, 11, 17, false, ::time(nullptr) - 1);

    std::string compaction_status;
    tablet->get_compaction_status(&compaction_status);
    std::cout << compaction_status << std::endl;

    int64_t query_freshness_tolerance_ms = 10000; // 10s
    std::vector<Version> expected_versions = {{0, 1},   {2, 10},  {11, 15},
                                              {16, 16}, {17, 17}, {18, 18}};
    check_capture_result(tablet, Version {0, 18}, query_freshness_tolerance_ms, expected_versions);
}

TEST_F(TestFreshnessTolerance, testCaputureMow_1) {
    /*
                                   now-10s                     now
                                                                  
                                    │           10s             │ 
                                    ◄───────────────────────────┤ 
┌────────┐  ┌─────────┐  ┌─────────┐│  ┌────────┐   ┌───────┐   │ 
│in cache│  │ in cache│  │in cache ││  │        │   │       │   │ 
│        │  │         │  │         ││  │        │   │       │   │ 
│ [2-10] │  │ [11-15] │  │ [16-16] ││  │ [17-17]│   │[18-18]│   │ 
└────────┘  └─────────┘  └─────────┘│  └────────┘   └───────┘   │ 
                                    │                           │ 
 now-40s      now-20s      now-15s  │   now-7s        now-3s    │ 
                                    │                           │ 
                                    │                           │ 
 return: [2-10],[11-15],[16-16]
*/
    auto tablet = create_tablet_with_initial_rowsets(15, true);
    do_cumu_compaction(tablet, 2, 10, true, ::time(nullptr) - 40);
    do_cumu_compaction(tablet, 11, 15, true, ::time(nullptr) - 20);
    add_new_version_rowset(tablet, 16, true, ::time(nullptr) - 15);
    add_new_version_rowset(tablet, 17, false, ::time(nullptr) - 7);
    add_new_version_rowset(tablet, 18, false, ::time(nullptr) - 3);

    std::string compaction_status;
    tablet->get_compaction_status(&compaction_status);
    std::cout << compaction_status << std::endl;

    int64_t query_freshness_tolerance_ms = 10000; // 10s
    std::vector<Version> expected_versions = {{0, 1}, {2, 10}, {11, 15}, {16, 16}};
    check_capture_result(tablet, Version {0, 18}, query_freshness_tolerance_ms, expected_versions);
}

TEST_F(TestFreshnessTolerance, testCaputureMow_2) {
    /*
                                   now-10s                     now
                                                                  
                                    │           10s             │ 
                                    ◄───────────────────────────┤ 
┌────────┐  ┌─────────┐  ┌─────────┐│  ┌────────┐   ┌───────┐   │ 
│in cache│  │ in cache│  │         ││  │        │   │       │   │ 
│        │  │         │  │         ││  │        │   │       │   │ 
│ [2-10] │  │ [11-15] │  │ [16-16] ││  │ [17-17]│   │[18-18]│   │ 
└────────┘  └─────────┘  └─────────┘│  └────────┘   └───────┘   │ 
                                    │                           │ 
 now-40s      now-20s      now-15s  │   now-7s        now-3s    │ 
                                    │                           │ 
                                    │                           │ 
 return: [2-10],[11-15],[16-16],[17-17],[18-18]
 NOTE: rowset[16-16] must be visible becasue it's within the query freshness tolerance time limit.
       However, since the data files of rowset[16-16] is not in the cache, there is no difference between
       capturing up to version 16 and capturing up to version 18
*/
    auto tablet = create_tablet_with_initial_rowsets(15, true);
    do_cumu_compaction(tablet, 2, 10, true, ::time(nullptr) - 40);
    do_cumu_compaction(tablet, 11, 15, true, ::time(nullptr) - 20);
    add_new_version_rowset(tablet, 16, false, ::time(nullptr) - 15);
    add_new_version_rowset(tablet, 17, false, ::time(nullptr) - 7);
    add_new_version_rowset(tablet, 18, false, ::time(nullptr) - 3);

    std::string compaction_status;
    tablet->get_compaction_status(&compaction_status);
    std::cout << compaction_status << std::endl;

    int64_t query_freshness_tolerance_ms = 10000; // 10s
    std::vector<Version> expected_versions = {{0, 1},   {2, 10},  {11, 15},
                                              {16, 16}, {17, 17}, {18, 18}};
    check_capture_result(tablet, Version {0, 18}, query_freshness_tolerance_ms, expected_versions);
}

TEST_F(TestFreshnessTolerance, testCaputureMow_3) {
    /*
                                   now-10s                     now
                                                                  
                                    │           10s             │ 
                                    ◄───────────────────────────┤ 
┌────────┐  ┌─────────┐  ┌─────────┐│  ┌────────┐   ┌───────┐   │ 
│in cache│  │ in cache│  │in cache ││  │in cache│   │       │   │ 
│        │  │         │  │         ││  │        │   │       │   │ 
│ [2-10] │  │ [11-15] │  │ [16-16] ││  │ [17-17]│   │[18-18]│   │ 
└────────┘  └─────────┘  └─────────┘│  └────────┘   └───────┘   │ 
                                    │                           │ 
 now-40s      now-20s      now-15s  │   now-7s        now-3s    │ 
                                    │                           │ 
                                    │                           │ 
 return: [2-10],[11-15],[16-16],[17-17]
*/
    auto tablet = create_tablet_with_initial_rowsets(15, true);
    do_cumu_compaction(tablet, 2, 10, true, ::time(nullptr) - 40);
    do_cumu_compaction(tablet, 11, 15, true, ::time(nullptr) - 20);
    add_new_version_rowset(tablet, 16, true, ::time(nullptr) - 15);
    add_new_version_rowset(tablet, 17, true, ::time(nullptr) - 7);
    add_new_version_rowset(tablet, 18, false, ::time(nullptr) - 3);

    std::string compaction_status;
    tablet->get_compaction_status(&compaction_status);
    std::cout << compaction_status << std::endl;

    int64_t query_freshness_tolerance_ms = 10000; // 10s
    std::vector<Version> expected_versions = {{0, 1}, {2, 10}, {11, 15}, {16, 16}, {17, 17}};
    check_capture_result(tablet, Version {0, 18}, query_freshness_tolerance_ms, expected_versions);
}

TEST_F(TestFreshnessTolerance, testCaputureMow_4) {
    /*
                                        now-10s                    now     
                                           │          10s           │      
                                           ◄────────────────────────┼      
                                           │                        │      
    ┌────────┐                             │┌────────┐    ┌───────┐ │      
    │in cache│                             ││        │    │       │ │      
    │        │                             ││        │    │       │ │      
    │ [2-10] │                             ││ [11-17]│    │[18-18]│ │      
    └────────┘                             │└────────┘    └───────┘ │      
                                           │                        │      
     now-40s                               │ now-1s         now-3s  │      
┌───────────────────────────────────────────────────────────────────────┐  
│                                          │                        │   │  
│  stale rowsets                           │                        │   │  
│                 ┌─────────┐  ┌─────────┐ │┌────────┐              │   │  
│                 │ in cache│  │in cache │ ││        │              │   │  
│                 │         │  │         │ ││        │              │   │  
│                 │ [11-15] │  │ [16-16] │ ││ [17-17]│              │   │  
│                 └─────────┘  └─────────┘ │└────────┘              │   │  
│                                          │                        │   │  
│                   now-20s      now-15s   │ now-7s                 │   │  
└───────────────────────────────────────────────────────────────────────┘  
                                           │                        │      
                                                                           
 return: [2-10],[11-15],[16-16]                                                                     
 */
    auto tablet = create_tablet_with_initial_rowsets(15, true);
    do_cumu_compaction(tablet, 2, 10, true, ::time(nullptr) - 40);
    do_cumu_compaction(tablet, 11, 15, true, ::time(nullptr) - 20);
    add_new_version_rowset(tablet, 16, true, ::time(nullptr) - 15);
    add_new_version_rowset(tablet, 17, false, ::time(nullptr) - 7);
    add_new_version_rowset(tablet, 18, false, ::time(nullptr) - 3);
    do_cumu_compaction(tablet, 11, 17, false, ::time(nullptr) - 1);

    std::string compaction_status;
    tablet->get_compaction_status(&compaction_status);
    std::cout << compaction_status << std::endl;

    int64_t query_freshness_tolerance_ms = 10000; // 10s
    std::vector<Version> expected_versions = {{0, 1}, {2, 10}, {11, 15}, {16, 16}};
    check_capture_result(tablet, Version {0, 18}, query_freshness_tolerance_ms, expected_versions);
}

TEST_F(TestFreshnessTolerance, testCaputureMow_5) {
    /*
                                        now-10s                    now     
                                           │          10s           │      
                                           ◄────────────────────────┼      
                                           │                        │      
    ┌────────┐                             │┌────────┐    ┌───────┐ │      
    │in cache│                             ││        │    │       │ │      
    │        │                             ││        │    │       │ │      
    │ [2-10] │                             ││ [11-17]│    │[18-18]│ │      
    └────────┘                             │└────────┘    └───────┘ │      
                                           │                        │      
     now-40s                               │ now-1s         now-3s  │      
┌───────────────────────────────────────────────────────────────────────┐  
│                                          │                        │   │  
│  stale rowsets                           │                        │   │  
│                 ┌─────────┐  ┌─────────┐ │┌────────┐              │   │  
│                 │ in cache│  │in cache │ ││in cache│              │   │  
│                 │         │  │         │ ││        │              │   │  
│                 │ [11-15] │  │ [16-16] │ ││ [17-17]│              │   │  
│                 └─────────┘  └─────────┘ │└────────┘              │   │  
│                                          │                        │   │  
│                   now-20s      now-15s   │ now-7s                 │   │  
└───────────────────────────────────────────────────────────────────────┘  
                                           │                        │      
                                                                           
 return: [2-10],[11-15],[16-16],[17-17]                                                                 
 */
    auto tablet = create_tablet_with_initial_rowsets(15, true);
    do_cumu_compaction(tablet, 2, 10, true, ::time(nullptr) - 40);
    do_cumu_compaction(tablet, 11, 15, true, ::time(nullptr) - 20);
    add_new_version_rowset(tablet, 16, true, ::time(nullptr) - 15);
    add_new_version_rowset(tablet, 17, true, ::time(nullptr) - 7);
    add_new_version_rowset(tablet, 18, false, ::time(nullptr) - 3);
    do_cumu_compaction(tablet, 11, 17, false, ::time(nullptr) - 1);

    std::string compaction_status;
    tablet->get_compaction_status(&compaction_status);
    std::cout << compaction_status << std::endl;

    int64_t query_freshness_tolerance_ms = 10000; // 10s
    std::vector<Version> expected_versions = {{0, 1}, {2, 10}, {11, 15}, {16, 16}, {17, 17}};
    check_capture_result(tablet, Version {0, 18}, query_freshness_tolerance_ms, expected_versions);
}
} // namespace doris
