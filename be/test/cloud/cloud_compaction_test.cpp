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

#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include <memory>

#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "cloud/cloud_tablet_mgr.h"
#include "gtest/gtest_pred_impl.h"
#include "json2pb/json_to_pb.h"
#include "olap/olap_common.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/tablet_meta.h"
#include "util/uid_util.h"

namespace doris {
class TabletMap;

class CloudCompactionTest : public testing::Test {
    CloudCompactionTest() : _engine(CloudStorageEngine(EngineOptions {})) {}
    void SetUp() override {
        config::compaction_promotion_size_mbytes = 1024;
        config::compaction_promotion_ratio = 0.05;
        config::compaction_promotion_min_size_mbytes = 64;
        config::compaction_min_size_mbytes = 64;

        _tablet_meta.reset(new TabletMeta(1, 2, 15673, 15674, 4, 5, TTabletSchema(), 6, {{7, 8}},
                                          UniqueId(9, 10), TTabletType::TABLET_TYPE_DISK,
                                          TCompressionType::LZ4F));

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
            "total_disk_size": 41,
            "data_disk_size": 41,
            "index_disk_size": 235,
            "empty": false,
            "load_id": {
                "hi": -5350970832824939812,
                "lo": -6717994719194512122
            },
            "creation_time": 1553765670,
            "num_segments": 3
        })";
    }
    void TearDown() override {}

    void init_rs_meta(RowsetMetaSharedPtr& pb1, int64_t start, int64_t end) {
        RowsetMetaPB rowset_meta_pb;
        json2pb::JsonToProtoMessage(_json_rowset_meta, &rowset_meta_pb);
        rowset_meta_pb.set_start_version(start);
        rowset_meta_pb.set_end_version(end);
        rowset_meta_pb.set_creation_time(10000);

        pb1->init_from_pb(rowset_meta_pb);
        pb1->set_total_disk_size(41);
        pb1->set_tablet_schema(_tablet_meta->tablet_schema());
    }

    void init_rs_meta_small_base(std::vector<RowsetMetaSharedPtr>* rs_metas) {
        RowsetMetaSharedPtr ptr1(new RowsetMeta());
        init_rs_meta(ptr1, 0, 0);
        rs_metas->push_back(ptr1);

        RowsetMetaSharedPtr ptr2(new RowsetMeta());
        init_rs_meta(ptr2, 1, 1);
        rs_metas->push_back(ptr2);

        RowsetMetaSharedPtr ptr3(new RowsetMeta());
        init_rs_meta(ptr3, 2, 2);
        rs_metas->push_back(ptr3);

        RowsetMetaSharedPtr ptr4(new RowsetMeta());
        init_rs_meta(ptr4, 3, 3);
        rs_metas->push_back(ptr4);

        RowsetMetaSharedPtr ptr5(new RowsetMeta());
        init_rs_meta(ptr5, 4, 4);
        rs_metas->push_back(ptr5);
    }

protected:
    std::string _json_rowset_meta;
    TabletMetaSharedPtr _tablet_meta;

public:
    CloudStorageEngine _engine;
};

TEST_F(CloudCompactionTest, failure_base_compaction_tablet_sleep_test) {
    auto filter_out = [](CloudTablet* t) { return false; };
    CloudTabletMgr mgr(_engine);

    std::vector<RowsetMetaSharedPtr> rs_metas;
    init_rs_meta_small_base(&rs_metas);

    CloudTabletSPtr tablet1 = std::make_shared<CloudTablet>(_engine, _tablet_meta);
    for (auto& rs_meta : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rs_meta));
    }
    tablet1->tablet_meta()->_tablet_id = 10000;
    tablet1->set_last_base_compaction_failure_time(
            duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch())
                    .count() -
            100000);
    tablet1->set_last_base_compaction_failure_time(0);
    tablet1->tablet_meta()->tablet_schema()->set_disable_auto_compaction(false);
    tablet1->_approximate_num_rowsets = 10;
    mgr.put_tablet_for_UT(tablet1);

    int64_t max_score;
    std::vector<std::shared_ptr<CloudTablet>> tablets {};
    Status st = mgr.get_topn_tablets_to_compact(1, CompactionType::BASE_COMPACTION, filter_out,
                                                &tablets, &max_score);
    ASSERT_EQ(st, Status::OK());
    ASSERT_EQ(tablets.size(), 1);

    tablet1->set_last_base_compaction_failure_time(
            duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch())
                    .count());
    st = mgr.get_topn_tablets_to_compact(1, CompactionType::BASE_COMPACTION, filter_out, &tablets,
                                         &max_score);
    ASSERT_EQ(st, Status::OK());
    ASSERT_EQ(tablets.size(), 0);
}

TEST_F(CloudCompactionTest, failure_cumu_compaction_tablet_sleep_test) {
    auto filter_out = [](CloudTablet* t) { return false; };
    CloudTabletMgr mgr(_engine);

    std::vector<RowsetMetaSharedPtr> rs_metas;
    init_rs_meta_small_base(&rs_metas);

    CloudTabletSPtr tablet1 = std::make_shared<CloudTablet>(_engine, _tablet_meta);
    for (auto& rs_meta : rs_metas) {
        static_cast<void>(_tablet_meta->add_rs_meta(rs_meta));
    }
    tablet1->tablet_meta()->_tablet_id = 10000;
    tablet1->set_last_cumu_compaction_failure_time(
            duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch())
                    .count() -
            100000);
    tablet1->set_last_cumu_compaction_failure_time(0);
    tablet1->tablet_meta()->tablet_schema()->set_disable_auto_compaction(false);
    tablet1->_approximate_cumu_num_deltas = 10;
    mgr.put_tablet_for_UT(tablet1);

    int64_t max_score;
    std::vector<std::shared_ptr<CloudTablet>> tablets {};
    Status st = mgr.get_topn_tablets_to_compact(1, CompactionType::CUMULATIVE_COMPACTION,
                                                filter_out, &tablets, &max_score);
    ASSERT_EQ(st, Status::OK());
    ASSERT_EQ(tablets.size(), 1);

    tablet1->set_last_cumu_compaction_failure_time(
            duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch())
                    .count());
    st = mgr.get_topn_tablets_to_compact(1, CompactionType::BASE_COMPACTION, filter_out, &tablets,
                                         &max_score);
    ASSERT_EQ(st, Status::OK());
    ASSERT_EQ(tablets.size(), 0);
}
} // namespace doris
