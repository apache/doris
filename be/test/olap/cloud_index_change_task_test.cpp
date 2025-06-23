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

#include <gtest/gtest.h>

#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "common/status.h"
#include "cpp/sync_point.h"
#include "json2pb/json_to_pb.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/task/engine_cloud_index_change_task.h"

namespace doris {

class CloudIndexChangeTaskTest : public testing::Test {
public:
    void SetUp() {
        _engine = std::make_unique<CloudStorageEngine>(EngineOptions {});
        auto sp = SyncPoint::get_instance();
        sp->enable_processing();
    }

    void TearDown() {}

    void init_rs_meta(RowsetMetaSharedPtr& pb1, int64_t start, int64_t end) {
        std::string json_rowset_meta = R"({
            "rowset_id": 540085,
            "tablet_id": 15674,
            "partition_id": 10000,
            "txn_id": 4045,
            "tablet_schema_hash": 567997588,
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

        RowsetMetaPB rowset_meta_pb;
        json2pb::JsonToProtoMessage(json_rowset_meta, &rowset_meta_pb);
        rowset_meta_pb.set_start_version(start);
        rowset_meta_pb.set_end_version(end);
        rowset_meta_pb.set_creation_time(10000);

        pb1->init_from_pb(rowset_meta_pb);
    }

    bool contains_str(std::string origin, std::string sub_str) {
        return origin.find(sub_str) != std::string::npos;
    }

public:
    std::unique_ptr<CloudStorageEngine> _engine;
};

TOlapTableIndex make_t_index(int index_id, std::string index_name, std::string col_name,
                             int col_unique_id, TIndexType::type index_type) {
    TOlapTableIndex index;
    index.index_id = index_id;
    index.index_name = index_name;
    index.columns.emplace_back(col_name);
    index.column_unique_ids.push_back(col_unique_id);
    index.index_type = index_type;
    return index;
}

TEST_F(CloudIndexChangeTaskTest, pick_tablet_rowset_test) {
    int col_id = 2222;
    std::string col_name = "col1_index";
    int index_id = 1111;
    std::string index_name = "index1";
    std::set<int64_t> index_ids;
    index_ids.insert(index_id);
    // make alter index
    std::vector<TOlapTableIndex> alter_inverted_indexes;
    TOlapTableIndex tindex =
            make_t_index(index_id, index_name, col_name, col_id, TIndexType::type::INVERTED);
    alter_inverted_indexes.push_back(tindex);

    // make rowset without index
    auto tablet_schema1 = std::make_shared<TabletSchema>();
    auto rowset_meta1 = std::make_shared<RowsetMeta>();
    RowsetSharedPtr rowset_without_index =
            std::make_shared<BetaRowset>(tablet_schema1, rowset_meta1, "");

    // make rowset with index
    TabletSchemaPB tablet_schema_with_index_pb;
    TabletIndexPB* index1_pb = tablet_schema_with_index_pb.add_index();
    index1_pb->set_index_id(index_id);
    index1_pb->set_index_name(index_name);
    index1_pb->set_index_type(IndexType::INVERTED);
    index1_pb->add_col_unique_id(col_id);
    auto tablet_with_index_schema = std::make_shared<TabletSchema>();
    tablet_with_index_schema->init_from_pb(tablet_schema_with_index_pb);
    auto rowset_meta2 = std::make_shared<RowsetMeta>();

    RowsetSharedPtr rowset_with_index =
            std::make_shared<BetaRowset>(tablet_with_index_schema, rowset_meta2, "");

    // make tablet
    auto tablet_meta3 = std::make_shared<TabletMeta>();
    CloudTabletSPtr tablet = std::make_shared<CloudTablet>(*_engine, tablet_meta3);
    Version v1;
    v1.first = 1;
    v1.second = 2;
    tablet->_rs_version_map[v1] = rowset_without_index;

    Version v2;
    v2.first = 10;
    v2.second = 11;
    tablet->_rs_version_map[v2] = rowset_with_index;

    tablet->_cumulative_point = 5;

    // 1. is drop, pick rowset with index
    bool is_drop = true;
    bool is_base_rowset = false;
    auto pick_rowset_with_index =
            tablet->pick_a_rowset_for_index_change(index_ids, is_drop, is_base_rowset);
    ASSERT_TRUE(!is_base_rowset);
    ASSERT_TRUE(pick_rowset_with_index == rowset_with_index);
    ASSERT_TRUE(pick_rowset_with_index != rowset_without_index);

    // 2. is add, pick rowset without index
    is_drop = false;
    is_base_rowset = false;
    auto pick_rowset_with_out_index =
            tablet->pick_a_rowset_for_index_change(index_ids, is_drop, is_base_rowset);
    ASSERT_TRUE(is_base_rowset);
    ASSERT_TRUE(pick_rowset_with_out_index != rowset_with_index);
    ASSERT_TRUE(pick_rowset_with_out_index == rowset_without_index);
}

TEST_F(CloudIndexChangeTaskTest, task_execute_err_exit) {
    auto sp = SyncPoint::get_instance();
    sp->clear_all_call_backs();

    TabletMetaPB input_meta_pb;
    input_meta_pb.set_tablet_id(1000);
    input_meta_pb.set_schema_hash(123456);
    input_meta_pb.set_tablet_state(PB_RUNNING);
    *input_meta_pb.mutable_tablet_uid() = TabletUid::gen_uid().to_proto();

    auto tablet_meta = std::make_shared<TabletMeta>();
    tablet_meta->init_from_pb(input_meta_pb);

    CloudTabletSPtr tablet = std::make_shared<CloudTablet>(*_engine, tablet_meta);

    {
        sp->set_call_back("EngineCloudIndexChangeTask::_get_tablet", [](auto&& outcome) {
            auto* pair = try_any_cast_ret<Result<std::shared_ptr<CloudTablet>>>(outcome);
            pair->second = true;
            pair->first = ResultError(Status::InternalError("mock no tablet error"));
        });

        TAlterInvertedIndexReq request;
        EngineCloudIndexChangeTask task(*_engine, request);
        Status ret = task.execute();
        ASSERT_TRUE(!ret.ok());
        ASSERT_TRUE(contains_str(ret.to_string(), "mock no tablet error"));
    }

    sp->set_call_back("EngineCloudIndexChangeTask::_get_tablet", [tablet](auto&& outcome) {
        auto* pair = try_any_cast_ret<Result<std::shared_ptr<CloudTablet>>>(outcome);
        pair->second = true;
        pair->first = Result<std::shared_ptr<CloudTablet>>(tablet);
    });

    // test sync failed
    sp->set_call_back("CloudMetaMgr::sync_tablet_rowsets", [](auto&& outcome) {
        auto* pairs = try_any_cast_ret<Status>(outcome);
        pairs->second = true;
        pairs->first = Status::InternalError<false>("mock sync meta failed");
    });

    {
        TAlterInvertedIndexReq request;
        EngineCloudIndexChangeTask task(*_engine, request);
        Status ret = task.execute();
        ASSERT_TRUE(!ret.ok());
        ASSERT_TRUE(contains_str(ret.to_string(), "mock sync meta failed"));
    }

    sp->set_call_back("CloudMetaMgr::sync_tablet_rowsets", [](auto&& outcome) {
        auto* pairs = try_any_cast_ret<Status>(outcome);
        pairs->second = true;
        pairs->first = Status::OK();
    });

    // test get null rowset
    sp->set_call_back("CloudTablet::pick_a_rowset_for_index_change", [](auto&& outcome) {
        auto* pairs = try_any_cast_ret<Result<RowsetSharedPtr>>(outcome);
        pairs->second = true;
        pairs->first = Result<RowsetSharedPtr>(nullptr);
    });

    {
        TAlterInvertedIndexReq request;
        EngineCloudIndexChangeTask task(*_engine, request);
        Status ret = task.execute();
        ASSERT_TRUE(ret.ok());
    }

    RowsetSharedPtr rowset = std::make_shared<BetaRowset>(std::make_shared<TabletSchema>(),
                                                          std::make_shared<RowsetMeta>(), "");

    sp->set_call_back("CloudTablet::pick_a_rowset_for_index_change", [rowset](auto&& outcome) {
        auto* pairs = try_any_cast_ret<Result<RowsetSharedPtr>>(outcome);
        pairs->second = true;
        pairs->first = Result<RowsetSharedPtr>(rowset);
    });

    // test prepare failed
    sp->set_call_back("CloudIndexChangeCompaction::prepare_compact", [](auto&& outcome) {
        auto* pairs = try_any_cast_ret<Status>(outcome);
        pairs->second = true;
        pairs->first = Status::InternalError<false>("mock prepare compact failed");
    });

    {
        TAlterInvertedIndexReq request;
        EngineCloudIndexChangeTask task(*_engine, request);
        Status ret = task.execute();
        ASSERT_TRUE(!ret.ok());
        ASSERT_TRUE(contains_str(ret.to_string(), "prepare compact failed"));
    }
}

TEST_F(CloudIndexChangeTaskTest, task_execute_err_exit2) {
    auto sp = SyncPoint::get_instance();
    sp->clear_all_call_backs();

    TabletMetaPB input_meta_pb;
    input_meta_pb.set_tablet_id(1000);
    input_meta_pb.set_schema_hash(123456);
    input_meta_pb.set_tablet_state(PB_RUNNING);
    *input_meta_pb.mutable_tablet_uid() = TabletUid::gen_uid().to_proto();

    auto tablet_meta = std::make_shared<TabletMeta>();
    tablet_meta->init_from_pb(input_meta_pb);

    CloudTabletSPtr tablet = std::make_shared<CloudTablet>(*_engine, tablet_meta);

    TAlterInvertedIndexReq request;
    std::vector<TOlapTableIndex> index_list;
    TOlapTableIndex index;
    index.index_id = 1;
    index.columns.emplace_back("col1");
    index.index_type = TIndexType::type::INVERTED;
    index_list.push_back(index);
    request.__set_alter_inverted_indexes(std::move(index_list));

    sp->set_call_back("EngineCloudIndexChangeTask::_get_tablet", [tablet](auto&& outcome) {
        auto* pair = try_any_cast_ret<Result<std::shared_ptr<CloudTablet>>>(outcome);
        pair->second = true;
        pair->first = Result<std::shared_ptr<CloudTablet>>(tablet);
    });

    sp->set_call_back("CloudMetaMgr::sync_tablet_rowsets", [](auto&& outcome) {
        auto* pairs = try_any_cast_ret<Status>(outcome);
        pairs->second = true;
        pairs->first = Status::OK();
    });

    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);

    ColumnPB* column_pb = schema_pb.add_column();
    column_pb->set_unique_id(10000);
    column_pb->set_name("col1");
    column_pb->set_type("int");
    column_pb->set_is_key(true);
    column_pb->set_is_nullable(true);
    auto tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->init_from_pb(schema_pb);

    auto rowset_meta = std::make_shared<RowsetMeta>();
    init_rs_meta(rowset_meta, 1, 1);
    rowset_meta->set_num_segments(2);
    RowsetSharedPtr rowset_ptr = std::make_shared<BetaRowset>(tablet_schema, rowset_meta, "");

    sp->set_call_back("CloudTablet::pick_a_rowset_for_index_change", [rowset_ptr](auto&& outcome) {
        auto* pairs = try_any_cast_ret<Result<RowsetSharedPtr>>(outcome);
        pairs->second = true;
        pairs->first = Result<RowsetSharedPtr>(rowset_ptr);
    });

    // test request lock failed
    sp->set_call_back("CloudMetaMgr::prepare_tablet_job", [](auto&& outcome) {
        auto* pairs = try_any_cast_ret<Status>(outcome);
        pairs->second = true;
        pairs->first = Status::InternalError<false>("mock tablet not found");

        auto* resp = try_any_cast<cloud::StartTabletJobResponse*>(outcome[1]);
        resp->mutable_status()->set_code(cloud::TABLET_NOT_FOUND);
        resp->mutable_status()->set_msg("mock tablet not found");
    });

    {
        EngineCloudIndexChangeTask task(*_engine, request);
        Status ret = task.execute();
        ASSERT_TRUE(!ret.ok());
        ASSERT_TRUE(contains_str(ret.to_string(), "mock tablet not found"));
    }

    // test execute failed
    sp->set_call_back("CloudMetaMgr::prepare_tablet_job", [](auto&& outcome) {
        auto* pairs = try_any_cast_ret<Status>(outcome);
        pairs->second = true;
        pairs->first = Status::OK();
    });

    sp->set_call_back("CloudIndexChangeCompaction::execute_compact", [](auto&& outcome) {
        auto* pairs = try_any_cast_ret<Status>(outcome);
        pairs->second = true;
        pairs->first = Status::InternalError<false>("compaction exec failed");
    });

    {
        EngineCloudIndexChangeTask task(*_engine, request);
        Status ret = task.execute();
        ASSERT_TRUE(!ret.ok());
        ASSERT_TRUE(contains_str(ret.to_string(), "compaction exec failed"));
    }
}

TEST_F(CloudIndexChangeTaskTest, register_task_conflict_test) {
    _engine->_submitted_base_compactions.clear();
    _engine->_submitted_index_change_base_compaction.clear();
    _engine->_submitted_cumu_compactions.clear();
    _engine->_submitted_index_change_cumu_compaction.clear();

    std::shared_ptr<CloudIndexChangeCompaction> index_change_compact = nullptr;

    int64_t tablet_id = 10001;
    std::string err_reason = "";
    // conflict with base compaction
    bool ret = _engine->register_index_change_compaction(index_change_compact, tablet_id, true,
                                                         err_reason);
    ASSERT_TRUE(ret);
    _engine->_submitted_base_compactions[tablet_id] = nullptr;
    ret = _engine->register_index_change_compaction(index_change_compact, tablet_id, true,
                                                    err_reason);
    ASSERT_FALSE(ret);

    // conflict with cumu compaction
    ret = _engine->register_index_change_compaction(index_change_compact, tablet_id, false,
                                                    err_reason);
    ASSERT_TRUE(ret);
    _engine->_submitted_cumu_compactions[tablet_id] = {};
    ret = _engine->register_index_change_compaction(index_change_compact, tablet_id, false,
                                                    err_reason);
    ASSERT_FALSE(ret);
}

} // namespace doris