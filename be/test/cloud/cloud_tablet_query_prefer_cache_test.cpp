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

#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <ranges>

#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "olap/base_tablet.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/tablet_meta.h"
#include "util/uid_util.h"

namespace doris {

using namespace std::chrono;

class TestQueryPreferCache : public testing::Test {
public:
    TestQueryPreferCache() : _engine(CloudStorageEngine(EngineOptions {})) {}

    void SetUp() override {
        config::read_cluster_cache_opt_verbose_log = true;
        _tablet_meta.reset(new TabletMeta(1, 2, 15673, 15674, 4, 5, TTabletSchema(), 6, {{7, 8}},
                                          UniqueId(9, 10), TTabletType::TABLET_TYPE_DISK,
                                          TCompressionType::LZ4F));
    }
    void TearDown() override { config::read_cluster_cache_opt_verbose_log = false; }

    RowsetSharedPtr create_rowset_without_visible_time(Version version) {
        auto rs_meta = std::make_shared<RowsetMeta>();
        rs_meta->set_rowset_type(BETA_ROWSET);
        rs_meta->set_version(version);
        rs_meta->set_rowset_id(_engine.next_rowset_id());
        RowsetSharedPtr rowset;
        Status st = RowsetFactory::create_rowset(nullptr, "", rs_meta, &rowset);
        if (!st.ok()) {
            return nullptr;
        }
        return rowset;
    }

    RowsetSharedPtr create_rowset(Version version,
                                  time_point<system_clock> visible_timestamp = system_clock::now() -
                                                                               seconds(100)) {
        auto rs = create_rowset_without_visible_time(version);
        if (!rs) {
            return nullptr;
        }
        rs->rowset_meta()->set_visible_ts_ms(
                duration_cast<milliseconds>(visible_timestamp.time_since_epoch()).count());
        return rs;
    }

    CloudTabletSPtr create_tablet_with_initial_rowsets(int max_version, bool is_mow = false,
                                                       bool warmup = true) {
        CloudTabletSPtr tablet =
                std::make_shared<CloudTablet>(_engine, std::make_shared<TabletMeta>(*_tablet_meta));
        tablet->tablet_meta()->set_enable_unique_key_merge_on_write(is_mow);
        std::vector<RowsetSharedPtr> rowsets;
        auto rs1 = create_rowset(Version {0, 1});
        rowsets.emplace_back(rs1);
        tablet->add_warmed_up_rowset(rs1->rowset_id());
        for (int ver = 2; ver <= max_version; ver++) {
            auto rs = create_rowset(Version {ver, ver});
            if (warmup) {
                tablet->add_warmed_up_rowset(rs->rowset_id());
            }
            rowsets.emplace_back(rs);
        }
        {
            std::unique_lock wlock {tablet->get_header_lock()};
            tablet->add_rowsets(rowsets, false, wlock, false);
        }
        return tablet;
    }

    void add_new_version_rowset(CloudTabletSPtr tablet, int64_t version, bool warmed_up,
                                time_point<system_clock> visible_timestamp) {
        auto rowset = create_rowset(Version {version, version}, visible_timestamp);
        if (warmed_up) {
            tablet->add_warmed_up_rowset(rowset->rowset_id());
        }
        std::unique_lock wlock {tablet->get_header_lock()};
        tablet->add_rowsets({rowset}, false, wlock, false);
    }

    void do_cumu_compaction(CloudTabletSPtr tablet, int64_t start_version, int64_t end_version,
                            bool warmed_up, time_point<system_clock> visible_timestamp) {
        std::unique_lock wrlock {tablet->get_header_lock()};
        std::vector<RowsetSharedPtr> input_rowsets;
        auto output_rowset = create_rowset(Version {start_version, end_version}, visible_timestamp);
        if (warmed_up) {
            tablet->add_warmed_up_rowset(output_rowset->rowset_id());
        }
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
                              const std::vector<Version>& expected_versions) {
        CaptureRowsetOps opts {.skip_missing_versions = false,
                               .enable_prefer_cached_rowset = true,
                               .query_freshness_tolerance_ms = -1};
        auto res = tablet->capture_read_source(spec_version, opts);
        ASSERT_TRUE(res.has_value());
        std::vector<RowSetSplits> rs_splits = std::move(res.value().rs_splits);
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

TEST_F(TestQueryPreferCache, testCapture_1_1) {
    /*
    be startup time                now-10s                     now
       now - 30s                                                  
          │                         │           10s             │ 
          │                         ◄───────────────────────────┤ 
┌────────┐│ ┌─────────┐  ┌─────────┐│  ┌────────┐   ┌───────┐   │ 
│        ││ │ in cache│  │in cache ││  │in cache│   │incache│   │ 
│        ││ │         │  │         ││  │        │   │       │   │ 
│ [2-10] ││ │ [11-15] │  │ [16-16] ││  │ [17-17]│   │[18-18]│   │ 
└────────┘│ └─────────┘  └─────────┘│  └────────┘   └───────┘   │ 
          │                         │                           │ 
 now-40s  │   now-20s      now-15s  │   now-7s        now-3s    │ 
          │                         │                           │ 
          │                         │                           │           
          
 return: [2-10],[11-15],[16-16],[17-17],[18-18]
 note: We only care about rowsets that are created after startup time point. For other historical rowsets,
       we just assume that they are warmuped up.
*/
    _engine.set_startup_timepoint(system_clock::now() - seconds(30));
    auto tablet = create_tablet_with_initial_rowsets(15);
    do_cumu_compaction(tablet, 2, 10, false, system_clock::now() - seconds(40));
    do_cumu_compaction(tablet, 11, 15, true, system_clock::now() - seconds(20));
    add_new_version_rowset(tablet, 16, true, system_clock::now() - seconds(15));
    add_new_version_rowset(tablet, 17, true, system_clock::now() - seconds(7));
    add_new_version_rowset(tablet, 18, true, system_clock::now() - seconds(3));

    std::string compaction_status;
    tablet->get_compaction_status(&compaction_status);
    std::cout << compaction_status << std::endl;

    std::vector<Version> expected_versions = {{0, 1},   {2, 10},  {11, 15},
                                              {16, 16}, {17, 17}, {18, 18}};
    check_capture_result(tablet, Version {0, 18}, expected_versions);
}

TEST_F(TestQueryPreferCache, testCapture_1_2) {
    /*
                                        now-10s                    now     
                                           │          10s           │      
                                           ◄────────────────────────┼      
                                           │                        │      
    ┌────────┐                             │┌────────┐    ┌────────┐│      
    │in cache│                             ││        │    │        ││      
    │        │                             ││        │    │in cache││      
    │ [2-10] │                             ││ [11-17]│    │[18-18] ││      
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
    _engine.set_startup_timepoint(system_clock::now() - seconds(200));
    auto tablet = create_tablet_with_initial_rowsets(15);
    do_cumu_compaction(tablet, 2, 10, true, system_clock::now() - seconds(40));
    do_cumu_compaction(tablet, 11, 15, true, system_clock::now() - seconds(20));
    add_new_version_rowset(tablet, 16, true, system_clock::now() - seconds(15));
    add_new_version_rowset(tablet, 17, true, system_clock::now() - seconds(7));
    add_new_version_rowset(tablet, 18, true, system_clock::now() - seconds(3));
    do_cumu_compaction(tablet, 11, 17, false, system_clock::now() - seconds(1));

    std::string compaction_status;
    tablet->get_compaction_status(&compaction_status);
    std::cout << compaction_status << std::endl;

    std::vector<Version> expected_versions = {{0, 1},   {2, 10},  {11, 15},
                                              {16, 16}, {17, 17}, {18, 18}};
    check_capture_result(tablet, Version {0, 18}, expected_versions);
}

TEST_F(TestQueryPreferCache, testCapture_1_3) {
    /*
                                        now-10s                    now   
                                           │          10s           │    
                                           ◄────────────────────────┼    
                                           │                        │    
                         ┌────────┐        │              ┌────────┐│    
                         │        │        │              │        ││    
                         │        │        │              │in cache││    
                         │ [2-16] │        │              │[18-18] ││    
                         └────────┘        │              └────────┘│    
                                           │                        │    
                          now-13s          │                now-3s  │    
                                           │                        │    
                                           │                        │    
┌───────────────────────────────────────────────────────────────────────┐
│                                          │                        │   │
│  stale rowsets                           │                        │   │
│    ┌────────┐   ┌─────────┐  ┌─────────┐ │┌────────┐              │   │
│    │in cache│   │ in cache│  │in cache │ ││in cache│              │   │
│    │        │   │         │  │         │ ││        │              │   │
│    │ [2-10] │   │ [11-15] │  │ [16-16] │ ││ [17-17]│              │   │
│    └────────┘   └─────────┘  └─────────┘ │└────────┘              │   │
│                                          │                        │   │
│     now-40s       now-20s      now-15s   │ now-7s                 │   │
│                                          │                        │   │
│                                          │                        │   │
└───────────────────────────────────────────────────────────────────────┘
                                           │                        │    
 return: [2-10],[11-15],[16-16],[17-17],[18-18]
 note: should not capture [2-16], otherwise we will meet cache miss
*/
    _engine.set_startup_timepoint(system_clock::now() - seconds(200));
    auto tablet = create_tablet_with_initial_rowsets(15);
    do_cumu_compaction(tablet, 2, 10, true, system_clock::now() - seconds(40));
    do_cumu_compaction(tablet, 11, 15, true, system_clock::now() - seconds(20));
    add_new_version_rowset(tablet, 16, true, system_clock::now() - seconds(15));
    add_new_version_rowset(tablet, 17, true, system_clock::now() - seconds(7));
    add_new_version_rowset(tablet, 18, true, system_clock::now() - seconds(3));
    do_cumu_compaction(tablet, 2, 16, false, system_clock::now() - seconds(13));

    std::string compaction_status;
    tablet->get_compaction_status(&compaction_status);
    std::cout << compaction_status << std::endl;

    std::vector<Version> expected_versions = {{0, 1},   {2, 10},  {11, 15},
                                              {16, 16}, {17, 17}, {18, 18}};
    check_capture_result(tablet, Version {0, 18}, expected_versions);
}

TEST_F(TestQueryPreferCache, testCapture_1_4) {
    /*
                                        now-10s                    now   
                                           │          10s           │    
                                           ◄────────────────────────┼    
                                           │                        │    
                                           │ ┌────────┐   ┌────────┐│    
                                           │ │        │   │        ││    
                                           │ │        │   │in cache││    
                                           │ │ [2-17] │   │[18-18] ││    
                                           │ └────────┘   └────────┘│    
                                           │                        │    
                                           │  now-1s        now-3s  │    
                                           │                        │    
                                           │                        │    
┌───────────────────────────────────────────────────────────────────────┐
│                                          │                        │   │
│  stale rowsets                           │                        │   │
│                                          │                        │   │
│                       ┌────────┐         │                        │   │
│                       │        │         │                        │   │
│                       │        │         │                        │   │
│                       │ [2-16] │         │                        │   │
│                       └────────┘         │                        │   │
│                                          │                        │   │
│                        now-13s           │                        │   │
│                                          │                        │   │
│                                          │                        │   │
│    ┌────────┐   ┌─────────┐  ┌─────────┐ │┌────────┐              │   │
│    │in cache│   │ in cache│  │in cache │ ││in cache│              │   │
│    │        │   │         │  │         │ ││        │              │   │
│    │ [2-10] │   │ [11-15] │  │ [16-16] │ ││ [17-17]│              │   │
│    └────────┘   └─────────┘  └─────────┘ │└────────┘              │   │
│                                          │                        │   │
│     now-40s       now-20s      now-15s   │ now-7s                 │   │
│                                          │                        │   │
│                                          │                        │   │
└───────────────────────────────────────────────────────────────────────┘
                                           │                        │    
 return: [2-10],[11-15],[16-16],[17-17],[18-18]
*/
    _engine.set_startup_timepoint(system_clock::now() - seconds(200));
    auto tablet = create_tablet_with_initial_rowsets(15);
    do_cumu_compaction(tablet, 2, 10, true, system_clock::now() - seconds(40));
    do_cumu_compaction(tablet, 11, 15, true, system_clock::now() - seconds(20));
    add_new_version_rowset(tablet, 16, true, system_clock::now() - seconds(15));
    add_new_version_rowset(tablet, 17, true, system_clock::now() - seconds(7));
    add_new_version_rowset(tablet, 18, true, system_clock::now() - seconds(3));
    do_cumu_compaction(tablet, 2, 16, false, system_clock::now() - seconds(13));
    do_cumu_compaction(tablet, 2, 17, false, system_clock::now() - seconds(1));

    std::string compaction_status;
    tablet->get_compaction_status(&compaction_status);
    std::cout << compaction_status << std::endl;

    std::vector<Version> expected_versions = {{0, 1},   {2, 10},  {11, 15},
                                              {16, 16}, {17, 17}, {18, 18}};
    check_capture_result(tablet, Version {0, 18}, expected_versions);
}

TEST_F(TestQueryPreferCache, testCapture_2_1) {
    /*
    be startup time                now-10s                     now
       now - 30s                                                  
          │                         │           10s             │ 
          │                         ◄───────────────────────────┤ 
┌────────┐│ ┌─────────┐  ┌─────────┐│  ┌────────┐   ┌───────┐   │ 
│        ││ │ in cache│  │in cache ││  │in cache│   │       │   │ 
│        ││ │         │  │         ││  │        │   │       │   │ 
│ [2-10] ││ │ [11-15] │  │ [16-16] ││  │ [17-17]│   │[18-18]│   │ 
└────────┘│ └─────────┘  └─────────┘│  └────────┘   └───────┘   │ 
          │                         │                           │ 
 now-40s  │   now-20s      now-15s  │   now-7s        now-3s    │ 
          │                         │                           │ 
          │                         │                           │           
          
 return: [2-10],[11-15],[16-16],[17-17],[18-18]
 note: We only care about rowsets that are created after startup time point. For other historical rowsets,
       we just assume that they are warmuped up.
*/
    _engine.set_startup_timepoint(system_clock::now() - seconds(30));
    auto tablet = create_tablet_with_initial_rowsets(15);
    do_cumu_compaction(tablet, 2, 10, false, system_clock::now() - seconds(40));
    do_cumu_compaction(tablet, 11, 15, true, system_clock::now() - seconds(20));
    add_new_version_rowset(tablet, 16, true, system_clock::now() - seconds(15));
    add_new_version_rowset(tablet, 17, true, system_clock::now() - seconds(7));
    add_new_version_rowset(tablet, 18, false, system_clock::now() - seconds(3));

    std::string compaction_status;
    tablet->get_compaction_status(&compaction_status);
    std::cout << compaction_status << std::endl;

    std::vector<Version> expected_versions = {{0, 1},   {2, 10},  {11, 15},
                                              {16, 16}, {17, 17}, {18, 18}};
    check_capture_result(tablet, Version {0, 18}, expected_versions);
}

TEST_F(TestQueryPreferCache, testCapture_2_2) {
    /*
                                        now-10s                    now     
                                           │          10s           │      
                                           ◄────────────────────────┼      
                                           │                        │      
    ┌────────┐                             │┌────────┐    ┌────────┐│      
    │in cache│                             ││        │    │        ││      
    │        │                             ││        │    │        ││      
    │ [2-10] │                             ││ [11-17]│    │[18-18] ││      
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
    _engine.set_startup_timepoint(system_clock::now() - seconds(200));
    auto tablet = create_tablet_with_initial_rowsets(15);
    do_cumu_compaction(tablet, 2, 10, true, system_clock::now() - seconds(40));
    do_cumu_compaction(tablet, 11, 15, true, system_clock::now() - seconds(20));
    add_new_version_rowset(tablet, 16, true, system_clock::now() - seconds(15));
    add_new_version_rowset(tablet, 17, true, system_clock::now() - seconds(7));
    add_new_version_rowset(tablet, 18, false, system_clock::now() - seconds(3));
    do_cumu_compaction(tablet, 11, 17, false, system_clock::now() - seconds(1));

    std::string compaction_status;
    tablet->get_compaction_status(&compaction_status);
    std::cout << compaction_status << std::endl;

    std::vector<Version> expected_versions = {{0, 1},   {2, 10},  {11, 15},
                                              {16, 16}, {17, 17}, {18, 18}};
    check_capture_result(tablet, Version {0, 18}, expected_versions);
}

TEST_F(TestQueryPreferCache, testCapture_2_3) {
    /*
                                        now-10s                    now   
                                           │          10s           │    
                                           ◄────────────────────────┼    
                                           │                        │    
                         ┌────────┐        │              ┌────────┐│    
                         │        │        │              │        ││    
                         │        │        │              │        ││    
                         │ [2-16] │        │              │[18-18] ││    
                         └────────┘        │              └────────┘│    
                                           │                        │    
                          now-13s          │                now-3s  │    
                                           │                        │    
                                           │                        │    
┌───────────────────────────────────────────────────────────────────────┐
│                                          │                        │   │
│  stale rowsets                           │                        │   │
│    ┌────────┐   ┌─────────┐  ┌─────────┐ │┌────────┐              │   │
│    │in cache│   │ in cache│  │in cache │ ││in cache│              │   │
│    │        │   │         │  │         │ ││        │              │   │
│    │ [2-10] │   │ [11-15] │  │ [16-16] │ ││ [17-17]│              │   │
│    └────────┘   └─────────┘  └─────────┘ │└────────┘              │   │
│                                          │                        │   │
│     now-40s       now-20s      now-15s   │ now-7s                 │   │
│                                          │                        │   │
│                                          │                        │   │
└───────────────────────────────────────────────────────────────────────┘
                                           │                        │    
 return: [2-10],[11-15],[16-16],[17-17],[18-18]
 note: should not capture [2-16], otherwise we will meet cache miss
*/
    _engine.set_startup_timepoint(system_clock::now() - seconds(200));
    auto tablet = create_tablet_with_initial_rowsets(15);
    do_cumu_compaction(tablet, 2, 10, true, system_clock::now() - seconds(40));
    do_cumu_compaction(tablet, 11, 15, true, system_clock::now() - seconds(20));
    add_new_version_rowset(tablet, 16, true, system_clock::now() - seconds(15));
    add_new_version_rowset(tablet, 17, true, system_clock::now() - seconds(7));
    add_new_version_rowset(tablet, 18, false, system_clock::now() - seconds(3));
    do_cumu_compaction(tablet, 2, 16, false, system_clock::now() - seconds(13));

    std::string compaction_status;
    tablet->get_compaction_status(&compaction_status);
    std::cout << compaction_status << std::endl;

    std::vector<Version> expected_versions = {{0, 1},   {2, 10},  {11, 15},
                                              {16, 16}, {17, 17}, {18, 18}};
    check_capture_result(tablet, Version {0, 18}, expected_versions);
}

TEST_F(TestQueryPreferCache, testCapture_2_4) {
    /*
                                        now-10s                    now   
                                           │          10s           │    
                                           ◄────────────────────────┼    
                                           │                        │    
                                           │ ┌────────┐   ┌────────┐│    
                                           │ │        │   │        ││    
                                           │ │        │   │        ││    
                                           │ │ [2-17] │   │[18-18] ││    
                                           │ └────────┘   └────────┘│    
                                           │                        │    
                                           │  now-1s        now-3s  │    
                                           │                        │    
                                           │                        │    
┌───────────────────────────────────────────────────────────────────────┐
│                                          │                        │   │
│  stale rowsets                           │                        │   │
│                                          │                        │   │
│                       ┌────────┐         │                        │   │
│                       │        │         │                        │   │
│                       │        │         │                        │   │
│                       │ [2-16] │         │                        │   │
│                       └────────┘         │                        │   │
│                                          │                        │   │
│                        now-13s           │                        │   │
│                                          │                        │   │
│                                          │                        │   │
│    ┌────────┐   ┌─────────┐  ┌─────────┐ │┌────────┐              │   │
│    │in cache│   │ in cache│  │in cache │ ││in cache│              │   │
│    │        │   │         │  │         │ ││        │              │   │
│    │ [2-10] │   │ [11-15] │  │ [16-16] │ ││ [17-17]│              │   │
│    └────────┘   └─────────┘  └─────────┘ │└────────┘              │   │
│                                          │                        │   │
│     now-40s       now-20s      now-15s   │ now-7s                 │   │
│                                          │                        │   │
│                                          │                        │   │
└───────────────────────────────────────────────────────────────────────┘
                                           │                        │    
 return: [2-10],[11-15],[16-16],[17-17],[18-18]
*/
    _engine.set_startup_timepoint(system_clock::now() - seconds(200));
    auto tablet = create_tablet_with_initial_rowsets(15);
    do_cumu_compaction(tablet, 2, 10, true, system_clock::now() - seconds(40));
    do_cumu_compaction(tablet, 11, 15, true, system_clock::now() - seconds(20));
    add_new_version_rowset(tablet, 16, true, system_clock::now() - seconds(15));
    add_new_version_rowset(tablet, 17, true, system_clock::now() - seconds(7));
    add_new_version_rowset(tablet, 18, false, system_clock::now() - seconds(3));
    do_cumu_compaction(tablet, 2, 16, false, system_clock::now() - seconds(13));
    do_cumu_compaction(tablet, 2, 17, false, system_clock::now() - seconds(1));

    std::string compaction_status;
    tablet->get_compaction_status(&compaction_status);
    std::cout << compaction_status << std::endl;

    std::vector<Version> expected_versions = {{0, 1},   {2, 10},  {11, 15},
                                              {16, 16}, {17, 17}, {18, 18}};
    check_capture_result(tablet, Version {0, 18}, expected_versions);
}

TEST_F(TestQueryPreferCache, testCapture_3_1) {
    /*
    be startup time                now-10s                     now
       now - 30s                                                  
          │                         │           10s             │ 
          │                         ◄───────────────────────────┤ 
┌────────┐│ ┌─────────┐  ┌─────────┐│  ┌────────┐   ┌───────┐   │ 
│        ││ │ in cache│  │in cache ││  │        │   │       │   │ 
│        ││ │         │  │         ││  │        │   │       │   │ 
│ [2-10] ││ │ [11-15] │  │ [16-16] ││  │ [17-17]│   │[18-18]│   │ 
└────────┘│ └─────────┘  └─────────┘│  └────────┘   └───────┘   │ 
          │                         │                           │ 
 now-40s  │   now-20s      now-15s  │   now-7s        now-3s    │ 
          │                         │                           │ 
          │                         │                           │           
          
 return: [2-10],[11-15],[16-16],[17-17],[18-18]
 note: We only care about rowsets that are created after startup time point. For other historical rowsets,
       we just assume that they are warmuped up.
*/
    _engine.set_startup_timepoint(system_clock::now() - seconds(30));
    auto tablet = create_tablet_with_initial_rowsets(15);
    do_cumu_compaction(tablet, 2, 10, false, system_clock::now() - seconds(40));
    do_cumu_compaction(tablet, 11, 15, true, system_clock::now() - seconds(20));
    add_new_version_rowset(tablet, 16, true, system_clock::now() - seconds(15));
    add_new_version_rowset(tablet, 17, false, system_clock::now() - seconds(7));
    add_new_version_rowset(tablet, 18, false, system_clock::now() - seconds(3));

    std::string compaction_status;
    tablet->get_compaction_status(&compaction_status);
    std::cout << compaction_status << std::endl;

    std::vector<Version> expected_versions = {{0, 1},   {2, 10},  {11, 15},
                                              {16, 16}, {17, 17}, {18, 18}};
    check_capture_result(tablet, Version {0, 18}, expected_versions);
}

TEST_F(TestQueryPreferCache, testCapture_3_2) {
    /*
                                        now-10s                    now     
                                           │          10s           │      
                                           ◄────────────────────────┼      
                                           │                        │      
    ┌────────┐                             │┌────────┐    ┌────────┐│      
    │in cache│                             ││        │    │        ││      
    │        │                             ││        │    │        ││      
    │ [2-10] │                             ││ [11-17]│    │[18-18] ││      
    └────────┘                             │└────────┘    └────────┘│      
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
 return: [2-10],[11-15],[16-16],[17-17],[18-18]                                                                     
 */
    _engine.set_startup_timepoint(system_clock::now() - seconds(200));
    auto tablet = create_tablet_with_initial_rowsets(15);
    do_cumu_compaction(tablet, 2, 10, true, system_clock::now() - seconds(40));
    do_cumu_compaction(tablet, 11, 15, true, system_clock::now() - seconds(20));
    add_new_version_rowset(tablet, 16, true, system_clock::now() - seconds(15));
    add_new_version_rowset(tablet, 17, false, system_clock::now() - seconds(7));
    add_new_version_rowset(tablet, 18, false, system_clock::now() - seconds(3));
    do_cumu_compaction(tablet, 11, 17, false, system_clock::now() - seconds(1));

    std::string compaction_status;
    tablet->get_compaction_status(&compaction_status);
    std::cout << compaction_status << std::endl;

    std::vector<Version> expected_versions = {{0, 1},   {2, 10},  {11, 15},
                                              {16, 16}, {17, 17}, {18, 18}};
    check_capture_result(tablet, Version {0, 18}, expected_versions);
}

TEST_F(TestQueryPreferCache, testCapture_3_3) {
    /*
                                        now-10s                    now   
                                           │          10s           │    
                                           ◄────────────────────────┼    
                                           │                        │    
                         ┌────────┐        │              ┌────────┐│    
                         │        │        │              │        ││    
                         │        │        │              │        ││    
                         │ [2-16] │        │              │[18-18] ││    
                         └────────┘        │              └────────┘│    
                                           │                        │    
                          now-13s          │                now-3s  │    
                                           │                        │    
                                           │                        │    
┌───────────────────────────────────────────────────────────────────────┐
│                                          │                        │   │
│  stale rowsets                           │                        │   │
│    ┌────────┐   ┌─────────┐  ┌─────────┐ │┌────────┐              │   │
│    │in cache│   │ in cache│  │in cache │ ││        │              │   │
│    │        │   │         │  │         │ ││        │              │   │
│    │ [2-10] │   │ [11-15] │  │ [16-16] │ ││ [17-17]│              │   │
│    └────────┘   └─────────┘  └─────────┘ │└────────┘              │   │
│                                          │                        │   │
│     now-40s       now-20s      now-15s   │ now-7s                 │   │
│                                          │                        │   │
│                                          │                        │   │
└───────────────────────────────────────────────────────────────────────┘
                                           │                        │    
 return: [2-10],[11-15],[16-16],[17-17],[18-18]
 note: should not capture [2-16], otherwise we will meet cache miss
*/
    _engine.set_startup_timepoint(system_clock::now() - seconds(200));
    auto tablet = create_tablet_with_initial_rowsets(15);
    do_cumu_compaction(tablet, 2, 10, true, system_clock::now() - seconds(40));
    do_cumu_compaction(tablet, 11, 15, true, system_clock::now() - seconds(20));
    add_new_version_rowset(tablet, 16, true, system_clock::now() - seconds(15));
    add_new_version_rowset(tablet, 17, false, system_clock::now() - seconds(7));
    add_new_version_rowset(tablet, 18, false, system_clock::now() - seconds(3));
    do_cumu_compaction(tablet, 2, 16, false, system_clock::now() - seconds(13));

    std::string compaction_status;
    tablet->get_compaction_status(&compaction_status);
    std::cout << compaction_status << std::endl;

    std::vector<Version> expected_versions = {{0, 1},   {2, 10},  {11, 15},
                                              {16, 16}, {17, 17}, {18, 18}};
    check_capture_result(tablet, Version {0, 18}, expected_versions);
}

TEST_F(TestQueryPreferCache, testCapture_3_4) {
    /*
                                        now-10s                    now   
                                           │          10s           │    
                                           ◄────────────────────────┼    
                                           │                        │    
                                           │ ┌────────┐   ┌────────┐│    
                                           │ │        │   │        ││    
                                           │ │        │   │        ││    
                                           │ │ [2-17] │   │[18-18] ││    
                                           │ └────────┘   └────────┘│    
                                           │                        │    
                                           │  now-1s        now-3s  │    
                                           │                        │    
                                           │                        │    
┌───────────────────────────────────────────────────────────────────────┐
│                                          │                        │   │
│  stale rowsets                           │                        │   │
│                                          │                        │   │
│                       ┌────────┐         │                        │   │
│                       │        │         │                        │   │
│                       │        │         │                        │   │
│                       │ [2-16] │         │                        │   │
│                       └────────┘         │                        │   │
│                                          │                        │   │
│                        now-13s           │                        │   │
│                                          │                        │   │
│                                          │                        │   │
│    ┌────────┐   ┌─────────┐  ┌─────────┐ │┌────────┐              │   │
│    │in cache│   │ in cache│  │in cache │ ││        │              │   │
│    │        │   │         │  │         │ ││        │              │   │
│    │ [2-10] │   │ [11-15] │  │ [16-16] │ ││ [17-17]│              │   │
│    └────────┘   └─────────┘  └─────────┘ │└────────┘              │   │
│                                          │                        │   │
│     now-40s       now-20s      now-15s   │ now-7s                 │   │
│                                          │                        │   │
│                                          │                        │   │
└───────────────────────────────────────────────────────────────────────┘
                                           │                        │    
 return: [2-10],[11-15],[16-16],[17-17],[18-18]
*/
    _engine.set_startup_timepoint(system_clock::now() - seconds(200));
    auto tablet = create_tablet_with_initial_rowsets(15);
    do_cumu_compaction(tablet, 2, 10, true, system_clock::now() - seconds(40));
    do_cumu_compaction(tablet, 11, 15, true, system_clock::now() - seconds(20));
    add_new_version_rowset(tablet, 16, true, system_clock::now() - seconds(15));
    add_new_version_rowset(tablet, 17, false, system_clock::now() - seconds(7));
    add_new_version_rowset(tablet, 18, false, system_clock::now() - seconds(3));
    do_cumu_compaction(tablet, 2, 16, false, system_clock::now() - seconds(13));
    do_cumu_compaction(tablet, 2, 17, false, system_clock::now() - seconds(1));

    std::string compaction_status;
    tablet->get_compaction_status(&compaction_status);
    std::cout << compaction_status << std::endl;

    std::vector<Version> expected_versions = {{0, 1},   {2, 10},  {11, 15},
                                              {16, 16}, {17, 17}, {18, 18}};
    check_capture_result(tablet, Version {0, 18}, expected_versions);
}

TEST_F(TestQueryPreferCache, testCapture_4_1) {
    /*
                                        now-10s                    now   
                                           │          10s           │    
                                           ◄────────────────────────┼    
                                           │                        │    
                                           │ ┌────────┐   ┌────────┐│    
                                           │ │        │   │        ││    
                                           │ │        │   │        ││    
                                           │ │ [2-17] │   │[18-18] ││    
                                           │ └────────┘   └────────┘│    
                                           │                        │    
                                           │  now-1s        now-3s  │    
                                           │                        │    
                                           │                        │    
┌───────────────────────────────────────────────────────────────────────┐
│                                          │                        │   │
│  stale rowsets                           │                        │   │
│                                          │                        │   │
│                       ┌────────┐         │                        │   │
│                       │        │         │                        │   │
│                       │        │         │                        │   │
│                       │ [11-16]│         │                        │   │
│                       └────────┘         │                        │   │
│                                          │                        │   │
│                        now-13s           │                        │   │
│                                          │                        │   │
│                                          │                        │   │
│    ┌────────┐   ┌─────────┐  ┌─────────┐ │┌────────┐              │   │
│    │in cache│   │         │  │         │ ││        │              │   │
│    │        │   │         │  │         │ ││        │              │   │
│    │ [2-10] │   │ [11-15] │  │ [16-16] │ ││ [17-17]│              │   │
│    └────────┘   └─────────┘  └─────────┘ │└────────┘              │   │
│                                          │                        │   │
│     now-40s       now-20s      now-15s   │ now-7s                 │   │
│                                          │                        │   │
│                                          │                        │   │
└───────────────────────────────────────────────────────────────────────┘
                                           │                        │    
 return: [2-10],[11-16],[17-17],[18-18]
 note: when there are no warmed up rowset at some vertex, choose the latest edge
*/
    _engine.set_startup_timepoint(system_clock::now() - seconds(200));
    auto tablet = create_tablet_with_initial_rowsets(15, false, false);
    do_cumu_compaction(tablet, 2, 10, true, system_clock::now() - seconds(40));
    do_cumu_compaction(tablet, 11, 15, false, system_clock::now() - seconds(20));
    add_new_version_rowset(tablet, 16, false, system_clock::now() - seconds(15));
    add_new_version_rowset(tablet, 17, false, system_clock::now() - seconds(7));
    add_new_version_rowset(tablet, 18, false, system_clock::now() - seconds(3));
    do_cumu_compaction(tablet, 11, 16, false, system_clock::now() - seconds(13));
    do_cumu_compaction(tablet, 2, 17, false, system_clock::now() - seconds(1));

    std::string compaction_status;
    tablet->get_compaction_status(&compaction_status);
    std::cout << compaction_status << std::endl;

    std::vector<Version> expected_versions = {{0, 1}, {2, 10}, {11, 16}, {17, 17}, {18, 18}};
    check_capture_result(tablet, Version {0, 18}, expected_versions);
}

} // namespace doris
