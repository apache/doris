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

#include "olap/tablet.h"

#include <gtest/gtest.h>

#include <sstream>

#include "olap/olap_define.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/storage_policy_mgr.h"
#include "olap/tablet_meta.h"
#include "util/time.h"

using namespace std;

namespace doris {

using RowsetMetaSharedContainerPtr = std::shared_ptr<std::vector<RowsetMetaSharedPtr>>;

class TestTablet : public testing::Test {
public:
    virtual ~TestTablet() {}

    virtual void SetUp() {
        _tablet_meta = static_cast<TabletMetaSharedPtr>(new TabletMeta(
                1, 2, 15673, 15674, 4, 5, TTabletSchema(), 6, {{7, 8}}, UniqueId(9, 10),
                TTabletType::TABLET_TYPE_DISK, TStorageMedium::HDD, "", TCompressionType::LZ4F));
        _json_rowset_meta = R"({
            "rowset_id": 540081,
            "tablet_id": 15673,
            "txn_id": 4042,
            "tablet_schema_hash": 567997577,
            "rowset_type": "BETA_ROWSET",
            "rowset_state": "VISIBLE",
            "start_version": 2,
            "end_version": 2,
            "num_rows": 3929,
            "total_disk_size": 84699,
            "data_disk_size": 84464,
            "index_disk_size": 235,
            "empty": false,
            "load_id": {
                "hi": -5350970832824939812,
                "lo": -6717994719194512122
            },
            "creation_time": 1553765670,
            "alpha_rowset_extra_meta_pb": {
                "segment_groups": [
                {
                    "segment_group_id": 0,
                    "num_segments": 2,
                    "index_size": 132,
                    "data_size": 576,
                    "num_rows": 5,
                    "zone_maps": [
                    {
                        "min": "MQ==",
                        "max": "NQ==",
                        "null_flag": false
                    },
                    {
                        "min": "MQ==",
                        "max": "Mw==",
                        "null_flag": false
                    },
                    {
                        "min": "J2J1c2gn",
                        "max": "J3RvbSc=",
                        "null_flag": false
                    }
                    ],
                    "empty": false
                }]
            }
        })";
    }

    virtual void TearDown() {}

    void init_rs_meta(RowsetMetaSharedPtr& pb1, int64_t start, int64_t end) {
        pb1->init_from_json(_json_rowset_meta);
        pb1->set_start_version(start);
        pb1->set_end_version(end);
        pb1->set_creation_time(10000);
    }

    void init_rs_meta(RowsetMetaSharedPtr& pb1, int64_t start, int64_t end, int64_t earliest_ts,
                      int64_t latest_ts) {
        pb1->init_from_json(_json_rowset_meta);
        pb1->set_oldest_write_timestamp(earliest_ts);
        pb1->set_newest_write_timestamp(latest_ts);
        pb1->set_start_version(start);
        pb1->set_end_version(end);
        pb1->set_creation_time(10000);
        pb1->set_num_segments(2);
    }

    void init_all_rs_meta(std::vector<RowsetMetaSharedPtr>* rs_metas) {
        RowsetMetaSharedPtr ptr1(new RowsetMeta());
        init_rs_meta(ptr1, 0, 0);
        rs_metas->push_back(ptr1);

        RowsetMetaSharedPtr ptr2(new RowsetMeta());
        init_rs_meta(ptr2, 1, 1);
        rs_metas->push_back(ptr2);

        RowsetMetaSharedPtr ptr3(new RowsetMeta());
        init_rs_meta(ptr3, 2, 5);
        rs_metas->push_back(ptr3);

        RowsetMetaSharedPtr ptr4(new RowsetMeta());
        init_rs_meta(ptr4, 6, 9);
        rs_metas->push_back(ptr4);

        RowsetMetaSharedPtr ptr5(new RowsetMeta());
        init_rs_meta(ptr5, 10, 11);
        rs_metas->push_back(ptr5);
    }

    void fetch_expired_row_rs_meta(std::vector<RowsetMetaSharedContainerPtr>* rs_metas) {
        RowsetMetaSharedContainerPtr v2(new std::vector<RowsetMetaSharedPtr>());
        RowsetMetaSharedPtr ptr1(new RowsetMeta());
        init_rs_meta(ptr1, 2, 3);
        v2->push_back(ptr1);

        RowsetMetaSharedPtr ptr2(new RowsetMeta());
        init_rs_meta(ptr2, 4, 5);
        v2->push_back(ptr2);

        RowsetMetaSharedContainerPtr v3(new std::vector<RowsetMetaSharedPtr>());
        RowsetMetaSharedPtr ptr3(new RowsetMeta());
        init_rs_meta(ptr3, 6, 6);
        v3->push_back(ptr3);

        RowsetMetaSharedPtr ptr4(new RowsetMeta());
        init_rs_meta(ptr4, 7, 8);
        v3->push_back(ptr4);

        RowsetMetaSharedContainerPtr v4(new std::vector<RowsetMetaSharedPtr>());
        RowsetMetaSharedPtr ptr5(new RowsetMeta());
        init_rs_meta(ptr5, 6, 8);
        v4->push_back(ptr5);

        RowsetMetaSharedPtr ptr6(new RowsetMeta());
        init_rs_meta(ptr6, 9, 9);
        v4->push_back(ptr6);

        RowsetMetaSharedContainerPtr v5(new std::vector<RowsetMetaSharedPtr>());
        RowsetMetaSharedPtr ptr7(new RowsetMeta());
        init_rs_meta(ptr7, 10, 10);
        v5->push_back(ptr7);

        RowsetMetaSharedPtr ptr8(new RowsetMeta());
        init_rs_meta(ptr8, 11, 11);
        v5->push_back(ptr8);

        rs_metas->push_back(v2);
        rs_metas->push_back(v3);
        rs_metas->push_back(v4);
        rs_metas->push_back(v5);
    }

protected:
    std::string _json_rowset_meta;
    TabletMetaSharedPtr _tablet_meta;
};

TEST_F(TestTablet, delete_expired_stale_rowset) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    std::vector<RowsetMetaSharedContainerPtr> expired_rs_metas;

    init_all_rs_meta(&rs_metas);
    fetch_expired_row_rs_meta(&expired_rs_metas);

    for (auto& rowset : rs_metas) {
        _tablet_meta->add_rs_meta(rowset);
    }

    StorageParamPB storage_param;
    storage_param.set_storage_medium(StorageMediumPB::HDD);
    TabletSharedPtr _tablet(new Tablet(_tablet_meta, storage_param, nullptr));
    _tablet->init();

    for (auto ptr : expired_rs_metas) {
        for (auto rs : *ptr) {
            _tablet->_timestamped_version_tracker.add_version(rs->version());
        }
        _tablet->_timestamped_version_tracker.add_stale_path_version(*ptr);
    }
    _tablet->delete_expired_stale_rowset();

    EXPECT_EQ(0, _tablet->_timestamped_version_tracker._stale_version_path_map.size());
    _tablet.reset();
}

TEST_F(TestTablet, cooldown_policy) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    RowsetMetaSharedPtr ptr1(new RowsetMeta());
    init_rs_meta(ptr1, 1, 2, 100, 200);
    rs_metas.push_back(ptr1);
    RowsetSharedPtr rowset1 = make_shared<BetaRowset>(nullptr, "", ptr1);

    RowsetMetaSharedPtr ptr2(new RowsetMeta());
    init_rs_meta(ptr2, 3, 4, 300, 600);
    rs_metas.push_back(ptr2);
    RowsetSharedPtr rowset2 = make_shared<BetaRowset>(nullptr, "", ptr2);

    RowsetMetaSharedPtr ptr3(new RowsetMeta());
    init_rs_meta(ptr3, 5, 5, 800, 800);
    rs_metas.push_back(ptr3);
    RowsetSharedPtr rowset3 = make_shared<BetaRowset>(nullptr, "", ptr3);

    RowsetMetaSharedPtr ptr4(new RowsetMeta());
    init_rs_meta(ptr4, 6, 7, 1100, 1400);
    rs_metas.push_back(ptr4);
    RowsetSharedPtr rowset4 = make_shared<BetaRowset>(nullptr, "", ptr4);

    RowsetMetaSharedPtr ptr5(new RowsetMeta());
    init_rs_meta(ptr5, 8, 9, 1800, 2000);
    rs_metas.push_back(ptr5);
    RowsetSharedPtr rowset5 = make_shared<BetaRowset>(nullptr, "", ptr5);

    for (auto& rowset : rs_metas) {
        _tablet_meta->add_rs_meta(rowset);
    }

    StorageParamPB storage_param;
    storage_param.set_storage_medium(StorageMediumPB::HDD);
    TabletSharedPtr _tablet(new Tablet(_tablet_meta, storage_param, nullptr));
    _tablet->init();
    _tablet->set_cooldown_resource("test_policy_name");

    _tablet->_rs_version_map[ptr1->version()] = rowset1;
    _tablet->_rs_version_map[ptr2->version()] = rowset2;
    _tablet->_rs_version_map[ptr3->version()] = rowset3;
    _tablet->_rs_version_map[ptr4->version()] = rowset4;
    _tablet->_rs_version_map[ptr5->version()] = rowset5;

    _tablet->set_cumulative_layer_point(20);

    ExecEnv::GetInstance()->_storage_policy_mgr = new StoragePolicyMgr();

    {
        StoragePolicy* policy = new StoragePolicy();
        policy->storage_policy_name = "test_policy_name";
        policy->cooldown_datetime = 250;
        policy->cooldown_ttl = -1;

        std::shared_ptr<StoragePolicy> policy_ptr;
        policy_ptr.reset(policy);

        ExecEnv::GetInstance()->storage_policy_mgr()->_policy_map["test_policy_name"] = policy_ptr;

        int64_t cooldown_timestamp = -1;
        size_t file_size = -1;
        bool ret = _tablet->need_cooldown(&cooldown_timestamp, &file_size);
        ASSERT_TRUE(ret);
        ASSERT_EQ(cooldown_timestamp, 250);
        ASSERT_EQ(file_size, -1);
    }

    {
        StoragePolicy* policy = new StoragePolicy();
        policy->storage_policy_name = "test_policy_name";
        policy->cooldown_datetime = -1;
        policy->cooldown_ttl = 3600;

        std::shared_ptr<StoragePolicy> policy_ptr;
        policy_ptr.reset(policy);

        ExecEnv::GetInstance()->storage_policy_mgr()->_policy_map["test_policy_name"] = policy_ptr;

        int64_t cooldown_timestamp = -1;
        size_t file_size = -1;
        bool ret = _tablet->need_cooldown(&cooldown_timestamp, &file_size);
        ASSERT_TRUE(ret);
        ASSERT_EQ(cooldown_timestamp, 3700);
        ASSERT_EQ(file_size, -1);
    }

    {
        StoragePolicy* policy = new StoragePolicy();
        policy->storage_policy_name = "test_policy_name";
        policy->cooldown_datetime = UnixSeconds() + 100;
        policy->cooldown_ttl = -1;

        std::shared_ptr<StoragePolicy> policy_ptr;
        policy_ptr.reset(policy);

        ExecEnv::GetInstance()->storage_policy_mgr()->_policy_map["test_policy_name"] = policy_ptr;

        int64_t cooldown_timestamp = -1;
        size_t file_size = -1;
        bool ret = _tablet->need_cooldown(&cooldown_timestamp, &file_size);
        ASSERT_FALSE(ret);
        ASSERT_EQ(cooldown_timestamp, -1);
        ASSERT_EQ(file_size, -1);
    }

    {
        StoragePolicy* policy = new StoragePolicy();
        policy->storage_policy_name = "test_policy_name";
        policy->cooldown_datetime = UnixSeconds() + 100;
        policy->cooldown_ttl = UnixSeconds() - 250;

        std::shared_ptr<StoragePolicy> policy_ptr;
        policy_ptr.reset(policy);

        ExecEnv::GetInstance()->storage_policy_mgr()->_policy_map["test_policy_name"] = policy_ptr;

        int64_t cooldown_timestamp = -1;
        size_t file_size = -1;
        bool ret = _tablet->need_cooldown(&cooldown_timestamp, &file_size);
        ASSERT_TRUE(ret);
        ASSERT_EQ(cooldown_timestamp, -1);
        ASSERT_EQ(file_size, 84699);
    }
}

} // namespace doris
