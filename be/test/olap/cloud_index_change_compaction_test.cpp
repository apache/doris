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

#include "cloud/cloud_index_change_compaction.h"

#include <gtest/gtest.h>

#include "cloud/cloud_base_compaction.h"
#include "cloud/cloud_cumulative_compaction.h"
#include "cpp/sync_point.h"
#include "json2pb/json_to_pb.h"
#include "olap/rowset/beta_rowset.h"

namespace doris {

class CloudIndexChangeCompactionTest : public testing::Test {
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

TEST_F(CloudIndexChangeCompactionTest, ms_ret_status_test) {
    std::vector<TOlapTableIndex> index_list;
    TOlapTableIndex index;
    index.index_id = 1;
    index.columns.emplace_back("col1");
    index_list.push_back(index);

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

    TabletMetaPB input_meta_pb;
    input_meta_pb.set_tablet_id(1000);
    input_meta_pb.set_schema_hash(123456);
    input_meta_pb.set_tablet_state(PB_RUNNING);
    *input_meta_pb.mutable_tablet_uid() = TabletUid::gen_uid().to_proto();

    auto tablet_meta = std::make_shared<TabletMeta>();
    tablet_meta->init_from_pb(input_meta_pb);

    CloudTabletSPtr tablet = std::make_shared<CloudTablet>(*_engine, tablet_meta);

    Version v1;
    v1.first = 1;
    v1.second = 2;
    tablet->_rs_version_map[v1] = rowset_ptr;

    // 1 test prepare
    {
        auto sp = SyncPoint::get_instance();
        sp->set_call_back("CloudMetaMgr::sync_tablet_rowsets", [](auto&& outcome) {
            auto* pairs = try_any_cast_ret<Status>(outcome);
            pairs->second = true;
            pairs->first = Status::OK();
        });
        std::shared_ptr<CloudIndexChangeCompaction> index_change_compact =
                std::make_shared<CloudIndexChangeCompaction>(*_engine, tablet, false, index_list);
        Status prepare_ret = index_change_compact->prepare_compact();
        ASSERT_TRUE(prepare_ret.ok());
    }

    {
        auto sp = SyncPoint::get_instance();
        sp->set_call_back("CloudMetaMgr::sync_tablet_rowsets", [](auto&& outcome) {
            auto* pairs = try_any_cast_ret<Status>(outcome);
            pairs->second = true;
            pairs->first = Status::InternalError<false>("mock sync meta failed");
        });

        std::shared_ptr<CloudIndexChangeCompaction> index_change_compact =
                std::make_shared<CloudIndexChangeCompaction>(*_engine, tablet, false, index_list);
        Status prepare_ret = index_change_compact->prepare_compact();
        ASSERT_TRUE(!prepare_ret.ok());
        ASSERT_TRUE(contains_str(prepare_ret.to_string(), "mock sync meta failed"));
    }

    // 2 test request global lock
    {
        auto sp = SyncPoint::get_instance();
        sp->set_call_back("CloudMetaMgr::prepare_tablet_job", [](auto&& outcome) {
            auto* pairs = try_any_cast_ret<Status>(outcome);
            pairs->second = true;
            pairs->first = Status::OK();
        });

        std::shared_ptr<CloudIndexChangeCompaction> index_change_compact =
                std::make_shared<CloudIndexChangeCompaction>(*_engine, tablet, false, index_list);
        index_change_compact->_input_rowsets.push_back(rowset_ptr);
        bool should_skip_err = false;
        Status ret = index_change_compact->request_global_lock(should_skip_err);
        ASSERT_TRUE(ret.ok());
        ASSERT_TRUE(!should_skip_err);
    }

    {
        // stale tablet error
        auto sp = SyncPoint::get_instance();
        sp->set_call_back("CloudMetaMgr::prepare_tablet_job", [](auto&& outcome) {
            auto* pairs = try_any_cast_ret<Status>(outcome);
            pairs->second = true;
            pairs->first = Status::InternalError<false>("mock stale tablet error");

            auto* resp = try_any_cast<cloud::StartTabletJobResponse*>(outcome[1]);
            resp->mutable_status()->set_code(cloud::STALE_TABLET_CACHE);
            resp->mutable_status()->set_msg("mock stale tablet error");
        });

        std::shared_ptr<CloudIndexChangeCompaction> index_change_compact =
                std::make_shared<CloudIndexChangeCompaction>(*_engine, tablet, false, index_list);
        index_change_compact->_input_rowsets.push_back(rowset_ptr);
        bool should_skip_err = false;
        Status ret = index_change_compact->request_global_lock(should_skip_err);
        ASSERT_TRUE(!ret.ok());
        ASSERT_TRUE(should_skip_err);
        ASSERT_TRUE(contains_str(ret.to_string(), "mock stale tablet error"));
    }

    {
        // tablet not found
        auto sp = SyncPoint::get_instance();
        sp->set_call_back("CloudMetaMgr::prepare_tablet_job", [](auto&& outcome) {
            auto* pairs = try_any_cast_ret<Status>(outcome);
            pairs->second = true;
            pairs->first = Status::InternalError<false>("mock tablet not found");

            auto* resp = try_any_cast<cloud::StartTabletJobResponse*>(outcome[1]);
            resp->mutable_status()->set_code(cloud::TABLET_NOT_FOUND);
            resp->mutable_status()->set_msg("mock tablet not found");
        });

        std::shared_ptr<CloudIndexChangeCompaction> index_change_compact =
                std::make_shared<CloudIndexChangeCompaction>(*_engine, tablet, false, index_list);
        index_change_compact->_input_rowsets.push_back(rowset_ptr);
        bool should_skip_err = false;
        Status ret = index_change_compact->request_global_lock(should_skip_err);
        ASSERT_TRUE(!ret.ok());
        ASSERT_TRUE(!should_skip_err);
        ASSERT_TRUE(contains_str(ret.to_string(), "mock tablet not found"));
    }

    {
        // job tablet busy
        auto sp = SyncPoint::get_instance();
        sp->set_call_back("CloudMetaMgr::prepare_tablet_job", [](auto&& outcome) {
            auto* pairs = try_any_cast_ret<Status>(outcome);
            pairs->second = true;
            pairs->first = Status::InternalError<false>("mock job tablet busy");

            auto* resp = try_any_cast<cloud::StartTabletJobResponse*>(outcome[1]);
            resp->mutable_status()->set_code(cloud::JOB_TABLET_BUSY);
            resp->mutable_status()->set_msg("mock job tablet busy");
        });

        std::shared_ptr<CloudIndexChangeCompaction> index_change_compact =
                std::make_shared<CloudIndexChangeCompaction>(*_engine, tablet, false, index_list);
        index_change_compact->_input_rowsets.push_back(rowset_ptr);
        bool should_skip_err = false;
        Status ret = index_change_compact->request_global_lock(should_skip_err);
        ASSERT_TRUE(!ret.ok());
        ASSERT_TRUE(!should_skip_err);
        ASSERT_TRUE(contains_str(ret.to_string(), "index change compaction no suitable versions"));
    }

    {
        // job check alter version
        auto sp = SyncPoint::get_instance();
        sp->set_call_back("CloudMetaMgr::prepare_tablet_job", [](auto&& outcome) {
            auto* pairs = try_any_cast_ret<Status>(outcome);
            pairs->second = true;
            pairs->first = Status::InternalError<false>("mock job check alter version");

            auto* resp = try_any_cast<cloud::StartTabletJobResponse*>(outcome[1]);
            resp->mutable_status()->set_code(cloud::JOB_CHECK_ALTER_VERSION);
            resp->mutable_status()->set_msg("mock job check alter version");
        });

        std::shared_ptr<CloudIndexChangeCompaction> index_change_compact =
                std::make_shared<CloudIndexChangeCompaction>(*_engine, tablet, false, index_list);
        index_change_compact->_input_rowsets.push_back(rowset_ptr);
        bool should_skip_err = false;
        Status ret = index_change_compact->request_global_lock(should_skip_err);
        ASSERT_TRUE(!ret.ok());
        ASSERT_TRUE(!should_skip_err);
        ASSERT_TRUE(contains_str(ret.to_string(), "failed in schema change."));
    }

    // modify rowsets
    {
        // tablet not found
        auto sp = SyncPoint::get_instance();
        sp->set_call_back("CloudMetaMgr::commit_tablet_job", [](auto&& outcome) {
            auto* pairs = try_any_cast_ret<Status>(outcome);
            pairs->second = true;
            pairs->first = Status::InternalError<false>("mock tablet not found");

            auto* resp = try_any_cast<cloud::FinishTabletJobResponse*>(outcome[1]);
            resp->mutable_status()->set_code(cloud::TABLET_NOT_FOUND);
            resp->mutable_status()->set_msg("mock tablet not found");
        });

        std::shared_ptr<CloudIndexChangeCompaction> index_change_compact =
                std::make_shared<CloudIndexChangeCompaction>(*_engine, tablet, false, index_list);
        index_change_compact->_input_rowsets.push_back(rowset_ptr);
        index_change_compact->_output_rowset = rowset_ptr;
        index_change_compact->_compact_type = cloud::TabletCompactionJobPB::BASE;
        Status ret = index_change_compact->modify_rowsets();
        ASSERT_TRUE(!ret.ok());
        ASSERT_TRUE(contains_str(ret.to_string(), "mock tablet not found"));
    }

    {
        // job check alter version
        auto sp = SyncPoint::get_instance();
        sp->set_call_back("CloudMetaMgr::commit_tablet_job", [](auto&& outcome) {
            auto* pairs = try_any_cast_ret<Status>(outcome);
            pairs->second = true;
            pairs->first = Status::InternalError<false>("mock check alter version");

            auto* resp = try_any_cast<cloud::FinishTabletJobResponse*>(outcome[1]);
            resp->mutable_status()->set_code(cloud::JOB_CHECK_ALTER_VERSION);
            resp->mutable_status()->set_msg("mock check alter version");
        });

        std::shared_ptr<CloudIndexChangeCompaction> index_change_compact =
                std::make_shared<CloudIndexChangeCompaction>(*_engine, tablet, false, index_list);
        index_change_compact->_input_rowsets.push_back(rowset_ptr);
        index_change_compact->_output_rowset = rowset_ptr;
        index_change_compact->_compact_type = cloud::TabletCompactionJobPB::BASE;
        Status ret = index_change_compact->modify_rowsets();
        ASSERT_TRUE(!ret.ok());
        ASSERT_TRUE(contains_str(ret.to_string(), "failed in schema change"));
    }
}

TEST_F(CloudIndexChangeCompactionTest, build_output_schema_drop_test) {
    // prepare tablet
    TabletMetaPB mock_pb_schema;
    mock_pb_schema.set_tablet_id(1000);
    mock_pb_schema.set_schema_hash(123456);
    mock_pb_schema.set_tablet_state(PB_RUNNING);
    *mock_pb_schema.mutable_tablet_uid() = TabletUid::gen_uid().to_proto();

    auto tablet_meta = std::make_shared<TabletMeta>();
    tablet_meta->init_from_pb(mock_pb_schema);

    CloudTabletSPtr tablet = std::make_shared<CloudTablet>(*_engine, tablet_meta);

    // drop
    std::vector<TOlapTableIndex> alter_inverted_indexes;

    TOlapTableIndex drop_index;
    drop_index.index_id = 123;
    drop_index.index_name = "col2_index";
    drop_index.columns.emplace_back("col2");
    drop_index.column_unique_ids.push_back(2222);
    drop_index.index_type = TIndexType::type::INVERTED;
    alter_inverted_indexes.push_back(drop_index);

    TOlapTableIndex invalid_index;
    invalid_index.index_id = 2;
    invalid_index.index_name = "invalid_index";
    invalid_index.columns.emplace_back("invalid_col");
    invalid_index.column_unique_ids.push_back(3);
    invalid_index.index_type = TIndexType::type::INVERTED;
    alter_inverted_indexes.push_back(invalid_index);

    // build delete schema
    TabletSchemaPB input_delete_schema_pb;
    ColumnPB* col1 = input_delete_schema_pb.add_column();
    col1->set_unique_id(1111);
    col1->set_name("col1");
    col1->set_type("int");
    col1->set_is_key(true);
    col1->set_is_nullable(true);

    ColumnPB* col2 = input_delete_schema_pb.add_column();
    col2->set_unique_id(2222);
    col2->set_name("col2");
    col2->set_type("int");
    col2->set_is_key(true);
    col2->set_is_nullable(true);

    TabletIndexPB* index1 = input_delete_schema_pb.add_index();
    index1->set_index_id(drop_index.index_id);
    index1->set_index_name(drop_index.index_name);
    index1->set_index_type(IndexType::INVERTED);
    index1->add_col_unique_id(2222);

    auto input_tablet_schema = std::make_shared<TabletSchema>();
    input_tablet_schema->init_from_pb(input_delete_schema_pb);

    auto& col2_column = input_tablet_schema->column_by_uid(2222);
    const TabletIndex* col2_index = input_tablet_schema->inverted_indexs(col2_column)[0];
    ASSERT_TRUE(col2_index != nullptr);
    ASSERT_TRUE(col2_index->index_id() == drop_index.index_id);
    ASSERT_TRUE(col2_index->index_name() == drop_index.index_name);
    ASSERT_TRUE(input_tablet_schema->_indexes.size() == 1);

    std::shared_ptr<CloudIndexChangeCompaction> index_change_compact =
            std::make_shared<CloudIndexChangeCompaction>(*_engine, tablet, false,
                                                         alter_inverted_indexes);
    TabletSchemaSPtr output_schema =
            index_change_compact->_build_output_rs_index_schema_for_drop(input_tablet_schema);

    ASSERT_TRUE(output_schema->inverted_indexs(col2_column).size() == 0);
    ASSERT_TRUE(output_schema->_indexes.size() == 0);
}

TEST_F(CloudIndexChangeCompactionTest, build_output_schema_add_test) {
    // prepare tablet
    TabletMetaPB mock_pb_schema;
    mock_pb_schema.set_tablet_id(1000);
    mock_pb_schema.set_schema_hash(123456);
    mock_pb_schema.set_tablet_state(PB_RUNNING);
    *mock_pb_schema.mutable_tablet_uid() = TabletUid::gen_uid().to_proto();

    auto tablet_meta = std::make_shared<TabletMeta>();
    tablet_meta->init_from_pb(mock_pb_schema);

    CloudTabletSPtr tablet = std::make_shared<CloudTablet>(*_engine, tablet_meta);

    // make input schema
    TabletSchemaPB input_schema_pb;
    ColumnPB* col1 = input_schema_pb.add_column();
    col1->set_unique_id(1111);
    col1->set_name("col1");
    col1->set_type("int");
    col1->set_is_key(true);
    col1->set_is_nullable(true);

    ColumnPB* col2 = input_schema_pb.add_column();
    col2->set_unique_id(2222);
    col2->set_name("col2");
    col2->set_type("int");
    col2->set_is_key(true);
    col2->set_is_nullable(true);

    ColumnPB* col3 = input_schema_pb.add_column();
    col2->set_unique_id(3333);
    col2->set_name("col3");
    col2->set_type("int");
    col2->set_is_key(false);
    col2->set_is_nullable(true);

    TabletIndexPB* col3_index_pb = input_schema_pb.add_index();
    col3_index_pb->set_index_id(4444);
    col3_index_pb->set_index_name("col3_index");
    col3_index_pb->set_index_type(IndexType::INVERTED);
    col3_index_pb->add_col_unique_id(col3->unique_id());

    auto input_tablet_schema = std::make_shared<TabletSchema>();
    input_tablet_schema->init_from_pb(input_schema_pb);

    // build add indexes
    std::vector<TOlapTableIndex> alter_inverted_indexes;

    TOlapTableIndex add_col2_index;
    add_col2_index.index_id = 123;
    add_col2_index.index_name = "col2_index";
    add_col2_index.columns.emplace_back(col2->name());
    add_col2_index.column_unique_ids.push_back(col2->unique_id());
    add_col2_index.index_type = TIndexType::type::INVERTED;
    alter_inverted_indexes.push_back(add_col2_index);

    TOlapTableIndex override_col3_index;
    override_col3_index.index_id = col3_index_pb->index_id() + 1;
    override_col3_index.index_name = "col3_index_override";
    override_col3_index.columns.emplace_back(col3->name());
    override_col3_index.column_unique_ids.push_back(col3->unique_id());
    override_col3_index.index_type = TIndexType::type::INVERTED;
    alter_inverted_indexes.push_back(override_col3_index);

    // build output schema
    std::shared_ptr<CloudIndexChangeCompaction> index_change_compact =
            std::make_shared<CloudIndexChangeCompaction>(*_engine, tablet, false,
                                                         alter_inverted_indexes);

    ASSERT_TRUE(input_tablet_schema->_indexes.size() == 1);
    auto& col3_column = input_tablet_schema->column_by_uid(col3->unique_id());
    const TabletIndex* col3_old_index = input_tablet_schema->inverted_indexs(col3_column)[0];
    ASSERT_TRUE(col3_old_index->index_id() == col3_index_pb->index_id());
    ASSERT_TRUE(col3_old_index->index_name() == col3_index_pb->index_name());

    TabletSchemaSPtr output_schema =
            index_change_compact->_build_output_rs_index_schema_for_add(input_tablet_schema);

    ASSERT_TRUE(output_schema->_indexes.size() == 2);
    const TabletIndex* col3_new_index = output_schema->inverted_indexs(col3_column)[0];
    ASSERT_TRUE(col3_new_index->index_id() != col3_index_pb->index_id());
    ASSERT_TRUE(col3_new_index->index_name() != col3_index_pb->index_name());
    ASSERT_TRUE(col3_new_index->index_id() == override_col3_index.index_id);
    ASSERT_TRUE(col3_new_index->index_name() == override_col3_index.index_name);

    auto& col2_column = output_schema->column_by_uid(col2->unique_id());
    const TabletIndex* col2_new_index = output_schema->inverted_indexs(col2_column)[0];
    ASSERT_TRUE(col2_new_index->index_id() == add_col2_index.index_id);
    ASSERT_TRUE(col2_new_index->index_name() == add_col2_index.index_name);
}

TEST_F(CloudIndexChangeCompactionTest, basic_compaction_test) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);

    ColumnPB* column_pb = schema_pb.add_column();
    column_pb->set_unique_id(10000);
    column_pb->set_name("col1");
    column_pb->set_type("int");
    column_pb->set_is_key(true);
    column_pb->set_is_nullable(true);
    auto tablet_schema1 = std::make_shared<TabletSchema>();
    tablet_schema1->init_from_pb(schema_pb);

    auto tablet_schema2 = std::make_shared<TabletSchema>();
    tablet_schema2->init_from_pb(schema_pb);

    auto rowset_meta = std::make_shared<RowsetMeta>();
    init_rs_meta(rowset_meta, 0, 1);
    rowset_meta->set_num_segments(2);
    rowset_meta->_schema = tablet_schema1;
    RowsetSharedPtr rowset_ptr = std::make_shared<BetaRowset>(tablet_schema1, rowset_meta, "");

    auto output_rowset_meta = std::make_shared<RowsetMeta>();
    init_rs_meta(output_rowset_meta, 0, 1);
    output_rowset_meta->set_num_segments(2);
    output_rowset_meta->_schema = tablet_schema1;
    Version v0;
    v0.first = 10;
    v0.second = 10;
    output_rowset_meta->set_version(v0);
    RowsetSharedPtr output_rowset_ptr =
            std::make_shared<BetaRowset>(tablet_schema1, output_rowset_meta, "");

    CloudTabletSPtr tablet =
            std::make_shared<CloudTablet>(*_engine, std::make_shared<TabletMeta>());

    Version v1;
    v1.first = 0;
    v1.second = 2;
    tablet->_rs_version_map[v1] = rowset_ptr;

    std::vector<TOlapTableIndex> index_list;
    CloudIndexChangeCompaction cloud_index_change_compaction(*_engine, tablet, false, index_list);
    cloud_index_change_compaction._output_schema = tablet_schema2;

    // test is_index_change_compaction
    CloudBaseCompaction cloud_base_compaction(*_engine, tablet);
    CloudCumulativeCompaction cloud_cumu_compaction(*_engine, tablet);
    ASSERT_TRUE(cloud_index_change_compaction.is_index_change_compaction());
    ASSERT_FALSE(cloud_base_compaction.is_index_change_compaction());
    ASSERT_FALSE(cloud_cumu_compaction.is_index_change_compaction());

    // test get_output_schema
    Status ret;
    cloud_base_compaction._input_rowsets.push_back(rowset_ptr);
    reinterpret_cast<CloudCompactionMixin*>(&cloud_base_compaction)
            ->_enable_vertical_compact_variant_subcolumns = false;
    ret = reinterpret_cast<CloudCompactionMixin*>(&cloud_base_compaction)->build_basic_info();
    ASSERT_TRUE(cloud_base_compaction.get_output_schema() == tablet_schema1);
    ASSERT_TRUE(ret.ok());

    cloud_cumu_compaction._input_rowsets.push_back(rowset_ptr);
    reinterpret_cast<CloudCompactionMixin*>(&cloud_cumu_compaction)
            ->_enable_vertical_compact_variant_subcolumns = false;
    ret = reinterpret_cast<CloudCompactionMixin*>(&cloud_cumu_compaction)->build_basic_info();
    ASSERT_TRUE(cloud_cumu_compaction.get_output_schema() == tablet_schema1);
    ASSERT_TRUE(ret.ok());

    cloud_index_change_compaction._input_rowsets.push_back(rowset_ptr);
    reinterpret_cast<CloudCompactionMixin*>(&cloud_index_change_compaction)
            ->_enable_vertical_compact_variant_subcolumns = false;
    ret = reinterpret_cast<CloudCompactionMixin*>(&cloud_index_change_compaction)
                  ->build_basic_info();
    ASSERT_TRUE(cloud_index_change_compaction.get_output_schema() != tablet_schema1);
    ASSERT_TRUE(ret.ok());

    // test delete predicate
    cloud_index_change_compaction._input_rowsets.clear();
    cloud_index_change_compaction._input_rowsets.push_back(rowset_ptr);
    cloud_index_change_compaction._output_rowset = output_rowset_ptr;

    DeletePredicatePB del_pred_pb;
    InPredicatePB* in_pred = del_pred_pb.add_in_predicates();
    in_pred->set_column_name("col1");
    in_pred->set_is_not_in(true);
    in_pred->add_values("123");
    in_pred->add_values("456");
    rowset_ptr->rowset_meta()->set_delete_predicate(del_pred_pb);

    cloud_index_change_compaction._allow_delete_in_cumu_compaction = false;
    reinterpret_cast<Compaction*>(&cloud_index_change_compaction)
            ->set_delete_predicate_for_output_rowset();

    auto& in_predicates = cloud_index_change_compaction._output_rowset->rowset_meta()
                                  ->delete_predicate()
                                  .in_predicates();
    ASSERT_TRUE(in_predicates[0].column_name() == "col1");
    ASSERT_TRUE(in_predicates[0].is_not_in() == true);
    ASSERT_TRUE(in_predicates[0].values().size() == 2);
}

} // namespace doris