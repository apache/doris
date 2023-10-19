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

#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <unistd.h>

#include "gtest/gtest_pred_impl.h"
#include "gutil/strings/numbers.h"
#include "http/action/pad_rowset_action.h"
#include "io/fs/local_file_system.h"
#include "json2pb/json_to_pb.h"
#include "olap/options.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/storage_engine.h"
#include "olap/storage_policy.h"
#include "olap/tablet_meta.h"
#include "olap/utils.h"
#include "runtime/exec_env.h"
#include "testutil/mock_rowset.h"
#include "util/time.h"
#include "util/uid_util.h"

using namespace std;

namespace doris {
using RowsetMetaSharedContainerPtr = std::shared_ptr<std::vector<RowsetMetaSharedPtr>>;

static StorageEngine* k_engine = nullptr;
static const std::string kTestDir = "/data_test/data/tablet_test";
static const uint32_t MAX_PATH_LEN = 1024;

class TestTablet : public testing::Test {
public:
    virtual ~TestTablet() {}

    void SetUp() override {
        _tablet_meta = new_tablet_meta(TTabletSchema());
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
            "creation_time": 1553765670
        })";
        char buffer[MAX_PATH_LEN];
        EXPECT_NE(getcwd(buffer, MAX_PATH_LEN), nullptr);
        absolute_dir = std::string(buffer) + kTestDir;

        EXPECT_TRUE(io::global_local_filesystem()->delete_and_create_directory(absolute_dir).ok());
        EXPECT_TRUE(io::global_local_filesystem()
                            ->create_directory(absolute_dir + "/tablet_path")
                            .ok());
        _data_dir = std::make_unique<DataDir>(absolute_dir);
        static_cast<void>(_data_dir->update_capacity());

        doris::EngineOptions options;
        k_engine = new StorageEngine(options);
        ExecEnv::GetInstance()->set_storage_engine(k_engine);
    }

    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(absolute_dir).ok());
        if (k_engine != nullptr) {
            k_engine->stop();
            delete k_engine;
            k_engine = nullptr;
            ExecEnv::GetInstance()->set_storage_engine(nullptr);
        }
    }

    TabletMetaSharedPtr new_tablet_meta(TTabletSchema schema, bool enable_merge_on_write = false) {
        return static_cast<TabletMetaSharedPtr>(new TabletMeta(
                1, 2, 15673, 15674, 4, 5, schema, 6, {{7, 8}}, UniqueId(9, 10),
                TTabletType::TABLET_TYPE_DISK, TCompressionType::LZ4F, 0, enable_merge_on_write));
    }

    void init_rs_meta(RowsetMetaSharedPtr& pb1, int64_t start, int64_t end) {
        RowsetMetaPB rowset_meta_pb;
        json2pb::JsonToProtoMessage(_json_rowset_meta, &rowset_meta_pb);
        rowset_meta_pb.set_start_version(start);
        rowset_meta_pb.set_end_version(end);
        rowset_meta_pb.set_creation_time(10000);

        pb1->init_from_pb(rowset_meta_pb);
        pb1->set_tablet_schema(_tablet_meta->tablet_schema());
    }

    void init_rs_meta(RowsetMetaSharedPtr& pb1, int64_t start, int64_t end, int64_t latest_ts) {
        RowsetMetaPB rowset_meta_pb;
        json2pb::JsonToProtoMessage(_json_rowset_meta, &rowset_meta_pb);
        rowset_meta_pb.set_start_version(start);
        rowset_meta_pb.set_end_version(end);
        rowset_meta_pb.set_creation_time(10000);

        pb1->init_from_pb(rowset_meta_pb);
        pb1->set_newest_write_timestamp(latest_ts);
        pb1->set_num_segments(2);
        pb1->set_tablet_schema(_tablet_meta->tablet_schema());
    }

    void init_rs_meta(RowsetMetaSharedPtr& pb1, int64_t start, int64_t end,
                      std::vector<KeyBoundsPB> keybounds) {
        RowsetMetaPB rowset_meta_pb;
        json2pb::JsonToProtoMessage(_json_rowset_meta, &rowset_meta_pb);
        rowset_meta_pb.set_start_version(start);
        rowset_meta_pb.set_end_version(end);
        rowset_meta_pb.set_creation_time(10000);

        pb1->init_from_pb(rowset_meta_pb);
        pb1->set_segments_key_bounds(keybounds);
        pb1->set_num_segments(keybounds.size());
        pb1->set_tablet_schema(_tablet_meta->tablet_schema());
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

    std::vector<KeyBoundsPB> convert_key_bounds(
            std::vector<std::pair<std::string, std::string>> key_pairs) {
        std::vector<KeyBoundsPB> res;
        for (auto pair : key_pairs) {
            KeyBoundsPB key_bounds;
            key_bounds.set_min_key(pair.first);
            key_bounds.set_max_key(pair.second);
            res.push_back(key_bounds);
        }
        return res;
    }

protected:
    std::string _json_rowset_meta;
    TabletMetaSharedPtr _tablet_meta;
    string absolute_dir;
    std::unique_ptr<DataDir> _data_dir;
};

TEST_F(TestTablet, delete_expired_stale_rowset) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    std::vector<RowsetMetaSharedContainerPtr> expired_rs_metas;

    init_all_rs_meta(&rs_metas);
    fetch_expired_row_rs_meta(&expired_rs_metas);

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    TabletSharedPtr _tablet(new Tablet(_tablet_meta, nullptr));
    static_cast<void>(_tablet->init());

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

TEST_F(TestTablet, pad_rowset) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    auto ptr1 = std::make_shared<RowsetMeta>();
    init_rs_meta(ptr1, 1, 2);
    rs_metas.push_back(ptr1);
    RowsetSharedPtr rowset1 = make_shared<BetaRowset>(nullptr, "", ptr1);

    auto ptr2 = std::make_shared<RowsetMeta>();
    init_rs_meta(ptr2, 3, 4);
    rs_metas.push_back(ptr2);
    RowsetSharedPtr rowset2 = make_shared<BetaRowset>(nullptr, "", ptr2);

    auto ptr3 = std::make_shared<RowsetMeta>();
    init_rs_meta(ptr3, 6, 7);
    rs_metas.push_back(ptr3);
    RowsetSharedPtr rowset3 = make_shared<BetaRowset>(nullptr, "", ptr3);

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    static_cast<void>(_data_dir->init());
    TabletSharedPtr _tablet(new Tablet(_tablet_meta, _data_dir.get()));
    static_cast<void>(_tablet->init());

    Version version(5, 5);
    std::vector<RowSetSplits> splits;
    ASSERT_FALSE(_tablet->capture_rs_readers(version, &splits).ok());
    splits.clear();

    PadRowsetAction action(nullptr, TPrivilegeHier::GLOBAL, TPrivilegeType::ADMIN);
    static_cast<void>(action._pad_rowset(_tablet, version));
    ASSERT_TRUE(_tablet->capture_rs_readers(version, &splits).ok());
}

TEST_F(TestTablet, cooldown_policy) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    RowsetMetaSharedPtr ptr1(new RowsetMeta());
    init_rs_meta(ptr1, 0, 2, 200);
    rs_metas.push_back(ptr1);
    RowsetSharedPtr rowset1 = make_shared<BetaRowset>(nullptr, "", ptr1);

    RowsetMetaSharedPtr ptr2(new RowsetMeta());
    init_rs_meta(ptr2, 3, 4, 600);
    rs_metas.push_back(ptr2);
    RowsetSharedPtr rowset2 = make_shared<BetaRowset>(nullptr, "", ptr2);

    RowsetMetaSharedPtr ptr3(new RowsetMeta());
    init_rs_meta(ptr3, 5, 5, 800);
    rs_metas.push_back(ptr3);
    RowsetSharedPtr rowset3 = make_shared<BetaRowset>(nullptr, "", ptr3);

    RowsetMetaSharedPtr ptr4(new RowsetMeta());
    init_rs_meta(ptr4, 6, 7, 1400);
    rs_metas.push_back(ptr4);
    RowsetSharedPtr rowset4 = make_shared<BetaRowset>(nullptr, "", ptr4);

    RowsetMetaSharedPtr ptr5(new RowsetMeta());
    init_rs_meta(ptr5, 8, 9, 2000);
    rs_metas.push_back(ptr5);
    RowsetSharedPtr rowset5 = make_shared<BetaRowset>(nullptr, "", ptr5);

    for (auto& rowset : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rowset));
    }

    TabletSharedPtr _tablet(new Tablet(_tablet_meta, nullptr));
    static_cast<void>(_tablet->init());
    constexpr int64_t storage_policy_id = 10000;
    _tablet->set_storage_policy_id(storage_policy_id);

    _tablet->_rs_version_map[ptr1->version()] = rowset1;
    _tablet->_rs_version_map[ptr2->version()] = rowset2;
    _tablet->_rs_version_map[ptr3->version()] = rowset3;
    _tablet->_rs_version_map[ptr4->version()] = rowset4;
    _tablet->_rs_version_map[ptr5->version()] = rowset5;

    _tablet->set_cumulative_layer_point(20);
    sleep(30);

    {
        auto storage_policy = std::make_shared<StoragePolicy>();
        storage_policy->cooldown_datetime = 250;
        storage_policy->cooldown_ttl = -1;
        put_storage_policy(storage_policy_id, storage_policy);

        int64_t cooldown_timestamp = -1;
        size_t file_size = -1;
        bool ret = _tablet->need_cooldown(&cooldown_timestamp, &file_size);
        ASSERT_TRUE(ret);
        ASSERT_EQ(cooldown_timestamp, 250);
        ASSERT_EQ(file_size, 84699);
    }

    {
        auto storage_policy = std::make_shared<StoragePolicy>();
        storage_policy->cooldown_datetime = -1;
        storage_policy->cooldown_ttl = 3600;
        put_storage_policy(storage_policy_id, storage_policy);

        int64_t cooldown_timestamp = -1;
        size_t file_size = -1;
        bool ret = _tablet->need_cooldown(&cooldown_timestamp, &file_size);
        ASSERT_TRUE(ret);
        ASSERT_EQ(cooldown_timestamp, 3800);
        ASSERT_EQ(file_size, 84699);
    }

    {
        auto storage_policy = std::make_shared<StoragePolicy>();
        storage_policy->cooldown_datetime = UnixSeconds() + 100;
        storage_policy->cooldown_ttl = -1;
        put_storage_policy(storage_policy_id, storage_policy);

        int64_t cooldown_timestamp = -1;
        size_t file_size = -1;
        bool ret = _tablet->need_cooldown(&cooldown_timestamp, &file_size);
        ASSERT_FALSE(ret);
        ASSERT_EQ(cooldown_timestamp, -1);
        ASSERT_EQ(file_size, -1);
    }

    {
        auto storage_policy = std::make_shared<StoragePolicy>();
        storage_policy->cooldown_datetime = UnixSeconds() + 100;
        storage_policy->cooldown_ttl = UnixSeconds() - 250;
        put_storage_policy(storage_policy_id, storage_policy);

        int64_t cooldown_timestamp = -1;
        size_t file_size = -1;
        bool ret = _tablet->need_cooldown(&cooldown_timestamp, &file_size);
        // the rowset with earliest version woule be picked up to do cooldown of which the timestamp
        // is UnixSeconds() - 250
        int64_t expect_cooldown_timestamp = UnixSeconds() - 50;
        ASSERT_TRUE(ret);
        ASSERT_EQ(cooldown_timestamp, expect_cooldown_timestamp);
        ASSERT_EQ(file_size, 84699);
    }
}

} // namespace doris
