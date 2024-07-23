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

#include <brpc/controller.h>
#include <gen_cpp/cloud.pb.h>
#include <gen_cpp/olap_file.pb.h>
#include <google/protobuf/arena.h>
#include <gtest/gtest.h>

#include <random>

#include "common/config.h"
#include "cpp/sync_point.h"
#include "meta-service/keys.h"
#include "meta-service/meta_service.h"
#include "meta-service/txn_kv.h"
#include "meta-service/txn_kv_error.h"

static std::string instance_id = "schema_kv_test";

namespace doris::cloud {
extern std::unique_ptr<MetaServiceProxy> get_meta_service();

static std::string next_rowset_id() {
    static int cnt = 0;
    return std::to_string(++cnt);
}

static void add_tablet(CreateTabletsRequest& req, int64_t table_id, int64_t index_id,
                       int64_t partition_id, int64_t tablet_id, const std::string& rowset_id,
                       int32_t schema_version) {
    auto tablet = req.add_tablet_metas();
    tablet->set_table_id(table_id);
    tablet->set_index_id(index_id);
    tablet->set_partition_id(partition_id);
    tablet->set_tablet_id(tablet_id);
    auto schema = tablet->mutable_schema();
    schema->set_schema_version(schema_version);
    auto first_rowset = tablet->add_rs_metas();
    first_rowset->set_rowset_id(0); // required
    first_rowset->set_rowset_id_v2(rowset_id);
    first_rowset->set_start_version(0);
    first_rowset->set_end_version(1);
    first_rowset->mutable_tablet_schema()->CopyFrom(*schema);
}

static void create_tablet(MetaServiceProxy* meta_service, int64_t table_id, int64_t index_id,
                          int64_t partition_id, int64_t tablet_id, const std::string& rowset_id,
                          int32_t schema_version) {
    brpc::Controller cntl;
    CreateTabletsRequest req;
    CreateTabletsResponse res;
    add_tablet(req, table_id, index_id, partition_id, tablet_id, rowset_id, schema_version);
    meta_service->create_tablets(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << tablet_id;
}

static void get_rowset(MetaServiceProxy* meta_service, int64_t table_id, int64_t index_id,
                       int64_t partition_id, int64_t tablet_id, GetRowsetResponse& res) {
    brpc::Controller cntl;
    GetRowsetRequest req;
    auto tablet_idx = req.mutable_idx();
    tablet_idx->set_table_id(table_id);
    tablet_idx->set_index_id(index_id);
    tablet_idx->set_partition_id(partition_id);
    tablet_idx->set_tablet_id(tablet_id);
    req.set_start_version(0);
    req.set_end_version(-1);
    req.set_cumulative_compaction_cnt(0);
    req.set_base_compaction_cnt(0);
    req.set_cumulative_point(2);
    meta_service->get_rowset(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << tablet_id;
}

static void check_get_tablet(MetaServiceProxy* meta_service, int64_t tablet_id,
                             int32_t schema_version) {
    brpc::Controller cntl;
    GetTabletRequest req;
    GetTabletResponse res;
    req.set_tablet_id(tablet_id);
    meta_service->get_tablet(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << tablet_id;
    ASSERT_TRUE(res.has_tablet_meta()) << tablet_id;
    EXPECT_TRUE(res.tablet_meta().has_schema()) << tablet_id;
    EXPECT_EQ(res.tablet_meta().schema_version(), schema_version) << tablet_id;
};

TEST(DetachSchemaKVTest, TabletTest) {
    auto meta_service = get_meta_service();
    // meta_service->resource_mgr().reset(); // Do not use resource manager

    auto sp = SyncPoint::get_instance();
    std::unique_ptr<int, std::function<void(int*)>> defer(
            (int*)0x01, [](int*) { SyncPoint::get_instance()->clear_all_call_backs(); });
    sp->set_call_back("get_instance_id", [&](auto&& args) {
        auto* ret = try_any_cast_ret<std::string>(args);
        ret->first = instance_id;
        ret->second = true;
    });
    sp->enable_processing();

    // new MS write with write_schema_kv=false, old MS read
    {
        constexpr auto table_id = 10001, index_id = 10002, partition_id = 10003, tablet_id = 10004;
        config::write_schema_kv = false;
        ASSERT_NO_FATAL_FAILURE(create_tablet(meta_service.get(), table_id, index_id, partition_id,
                                              tablet_id, next_rowset_id(), 1));
        // check saved values in txn_kv
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string tablet_key, tablet_val;
        meta_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id}, &tablet_key);
        ASSERT_EQ(txn->get(tablet_key, &tablet_val), TxnErrorCode::TXN_OK);
        doris::TabletMetaCloudPB saved_tablet;
        ASSERT_TRUE(saved_tablet.ParseFromString(tablet_val));
        EXPECT_TRUE(saved_tablet.has_schema());
        std::string rowset_key, rowset_val;
        meta_rowset_key({instance_id, tablet_id, 1}, &rowset_key);
        ASSERT_EQ(txn->get(rowset_key, &rowset_val), TxnErrorCode::TXN_OK);
        doris::RowsetMetaCloudPB saved_rowset;
        ASSERT_TRUE(saved_rowset.ParseFromString(rowset_val));
        EXPECT_TRUE(saved_rowset.has_tablet_schema());
    }

    // old MS write, new MS read
    {
        constexpr auto table_id = 10011, index_id = 10012, partition_id = 10013, tablet_id = 10014;
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        doris::TabletMetaCloudPB saved_tablet;
        saved_tablet.set_table_id(table_id);
        saved_tablet.set_index_id(index_id);
        saved_tablet.set_partition_id(partition_id);
        saved_tablet.set_tablet_id(tablet_id);
        saved_tablet.mutable_schema()->set_schema_version(1);
        std::string tablet_key, tablet_val;
        meta_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id}, &tablet_key);
        ASSERT_TRUE(saved_tablet.SerializeToString(&tablet_val));
        txn->put(tablet_key, tablet_val);
        TabletIndexPB saved_tablet_idx;
        saved_tablet_idx.set_table_id(table_id);
        saved_tablet_idx.set_index_id(index_id);
        saved_tablet_idx.set_partition_id(partition_id);
        saved_tablet_idx.set_tablet_id(tablet_id);
        std::string tablet_idx_key, tablet_idx_val;
        meta_tablet_idx_key({instance_id, tablet_id}, &tablet_idx_key);
        ASSERT_TRUE(saved_tablet_idx.SerializeToString(&tablet_idx_val));
        txn->put(tablet_idx_key, tablet_idx_val);
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
        // check get tablet response
        check_get_tablet(meta_service.get(), tablet_id, 1);
    }

    auto check_new_saved_tablet_val = [](Transaction* txn, int64_t table_id, int64_t index_id,
                                         int64_t partition_id, int64_t tablet_id,
                                         int32_t schema_version) {
        std::string tablet_key, tablet_val;
        meta_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id}, &tablet_key);
        ASSERT_EQ(txn->get(tablet_key, &tablet_val), TxnErrorCode::TXN_OK);
        doris::TabletMetaCloudPB saved_tablet;
        ASSERT_TRUE(saved_tablet.ParseFromString(tablet_val));
        EXPECT_FALSE(saved_tablet.has_schema()) << tablet_id;
        EXPECT_EQ(saved_tablet.schema_version(), schema_version) << tablet_id;
    };

    // new MS write with write_schema_kv=true, new MS read
    {
        constexpr auto table_id = 10021, index_id = 10022, partition_id = 10023, tablet_id = 10024;
        config::write_schema_kv = true;
        ASSERT_NO_FATAL_FAILURE(create_tablet(meta_service.get(), table_id, index_id, partition_id,
                                              tablet_id, next_rowset_id(), 1));
        // check saved values in txn_kv
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        check_new_saved_tablet_val(txn.get(), table_id, index_id, partition_id, tablet_id, 1);
        std::string rowset_key, rowset_val;
        meta_rowset_key({instance_id, tablet_id, 1}, &rowset_key);
        ASSERT_EQ(txn->get(rowset_key, &rowset_val), TxnErrorCode::TXN_OK);
        doris::RowsetMetaCloudPB saved_rowset;
        ASSERT_TRUE(saved_rowset.ParseFromString(rowset_val));
        EXPECT_FALSE(saved_rowset.has_tablet_schema());
        EXPECT_EQ(saved_rowset.index_id(), index_id);
        EXPECT_EQ(saved_rowset.schema_version(), 1);
        // check get tablet response
        check_get_tablet(meta_service.get(), tablet_id, 1);
        // check get rowset response
        GetRowsetResponse get_rowset_res;
        get_rowset(meta_service.get(), table_id, index_id, partition_id, tablet_id, get_rowset_res);
        ASSERT_EQ(get_rowset_res.rowset_meta_size(), 1);
        EXPECT_TRUE(get_rowset_res.rowset_meta(0).has_tablet_schema());
        EXPECT_EQ(get_rowset_res.rowset_meta(0).index_id(), index_id);
        EXPECT_EQ(get_rowset_res.rowset_meta(0).schema_version(), 1);
        ASSERT_TRUE(get_rowset_res.has_stats());
        EXPECT_EQ(get_rowset_res.stats().num_rows(), 0);
        EXPECT_EQ(get_rowset_res.stats().num_rowsets(), 1);
        EXPECT_EQ(get_rowset_res.stats().num_segments(), 0);
        EXPECT_EQ(get_rowset_res.stats().data_size(), 0);
    }

    // new MS batch create tablets with write_schema_kv=true
    {
        config::write_schema_kv = true;
        brpc::Controller cntl;
        CreateTabletsRequest req;
        CreateTabletsResponse res;
        add_tablet(req, 10031, 10032, 10033, 100031, next_rowset_id(), 1);
        add_tablet(req, 10031, 10032, 10033, 100032, next_rowset_id(), 2);
        add_tablet(req, 10031, 10032, 10033, 100033, next_rowset_id(), 2);
        add_tablet(req, 10031, 10032, 10033, 100034, next_rowset_id(), 3);
        add_tablet(req, 10031, 10032, 10033, 100035, next_rowset_id(), 3);
        add_tablet(req, 10031, 10032, 10033, 100036, next_rowset_id(), 3);
        add_tablet(req, 10031, 10034, 10033, 100037, next_rowset_id(), 1);
        add_tablet(req, 10031, 10034, 10033, 100038, next_rowset_id(), 2);
        add_tablet(req, 10031, 10034, 10033, 100039, next_rowset_id(), 2);
        meta_service->create_tablets(&cntl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        // check saved values in txn_kv
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        check_new_saved_tablet_val(txn.get(), 10031, 10032, 10033, 100031, 1);
        check_new_saved_tablet_val(txn.get(), 10031, 10032, 10033, 100032, 2);
        check_new_saved_tablet_val(txn.get(), 10031, 10032, 10033, 100033, 2);
        check_new_saved_tablet_val(txn.get(), 10031, 10032, 10033, 100034, 3);
        check_new_saved_tablet_val(txn.get(), 10031, 10032, 10033, 100035, 3);
        check_new_saved_tablet_val(txn.get(), 10031, 10032, 10033, 100036, 3);
        check_new_saved_tablet_val(txn.get(), 10031, 10034, 10033, 100037, 1);
        check_new_saved_tablet_val(txn.get(), 10031, 10034, 10033, 100038, 2);
        check_new_saved_tablet_val(txn.get(), 10031, 10034, 10033, 100039, 2);
        // check get tablet response
        check_get_tablet(meta_service.get(), 100031, 1);
        check_get_tablet(meta_service.get(), 100032, 2);
        check_get_tablet(meta_service.get(), 100033, 2);
        check_get_tablet(meta_service.get(), 100034, 3);
        check_get_tablet(meta_service.get(), 100035, 3);
        check_get_tablet(meta_service.get(), 100036, 3);
        check_get_tablet(meta_service.get(), 100037, 1);
        check_get_tablet(meta_service.get(), 100038, 2);
        check_get_tablet(meta_service.get(), 100039, 2);
    }
}

static void begin_txn(MetaServiceProxy* meta_service, int64_t db_id, const std::string& label,
                      int64_t table_id, int64_t& txn_id) {
    brpc::Controller cntl;
    BeginTxnRequest req;
    BeginTxnResponse res;
    auto txn_info = req.mutable_txn_info();
    txn_info->set_db_id(db_id);
    txn_info->set_label(label);
    txn_info->add_table_ids(table_id);
    txn_info->set_timeout_ms(36000);
    meta_service->begin_txn(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << label;
    ASSERT_TRUE(res.has_txn_id()) << label;
    txn_id = res.txn_id();
}

static void commit_txn(MetaServiceProxy* meta_service, int64_t db_id, int64_t txn_id,
                       const std::string& label) {
    brpc::Controller cntl;
    CommitTxnRequest req;
    CommitTxnResponse res;
    req.set_db_id(db_id);
    req.set_txn_id(txn_id);
    meta_service->commit_txn(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << label;
}

static doris::RowsetMetaCloudPB create_rowset(int64_t txn_id, int64_t tablet_id,
                                              const std::string& rowset_id, int32_t schema_version,
                                              int64_t version = -1,
                                              const TabletSchemaCloudPB* schema = nullptr) {
    doris::RowsetMetaCloudPB rowset;
    rowset.set_rowset_id(0); // required
    rowset.set_rowset_id_v2(rowset_id);
    rowset.set_tablet_id(tablet_id);
    rowset.set_txn_id(txn_id);
    rowset.set_num_rows(100);
    rowset.set_num_segments(1);
    rowset.set_data_disk_size(10000);
    if (version > 0) {
        rowset.set_start_version(version);
        rowset.set_end_version(version);
    }
    rowset.mutable_tablet_schema()->set_schema_version(schema_version);
    rowset.set_txn_expiration(::time(nullptr)); // Required by DCHECK
    if (schema != nullptr) {
        rowset.mutable_tablet_schema()->CopyFrom(*schema);
        rowset.mutable_tablet_schema()->set_schema_version(schema_version);
    }
    return rowset;
}

static void prepare_rowset(MetaServiceProxy* meta_service, const doris::RowsetMetaCloudPB& rowset,
                           CreateRowsetResponse& res) {
    brpc::Controller cntl;
    auto arena = res.GetArena();
    auto req = google::protobuf::Arena::CreateMessage<CreateRowsetRequest>(arena);
    req->mutable_rowset_meta()->CopyFrom(rowset);
    meta_service->prepare_rowset(&cntl, req, &res, nullptr);
    if (!arena) delete req;
}

static void commit_rowset(MetaServiceProxy* meta_service, const doris::RowsetMetaCloudPB& rowset,
                          CreateRowsetResponse& res) {
    brpc::Controller cntl;
    auto arena = res.GetArena();
    auto req = google::protobuf::Arena::CreateMessage<CreateRowsetRequest>(arena);
    req->mutable_rowset_meta()->CopyFrom(rowset);
    meta_service->commit_rowset(&cntl, req, &res, nullptr);
    if (!arena) delete req;
}

static void insert_rowset(MetaServiceProxy* meta_service, int64_t db_id, const std::string& label,
                          int64_t table_id, int64_t tablet_id, int32_t schema_version,
                          const TabletSchemaCloudPB* schema = nullptr) {
    int64_t txn_id = 0;
    ASSERT_NO_FATAL_FAILURE(begin_txn(meta_service, db_id, label, table_id, txn_id));
    CreateRowsetResponse res;
    auto rowset = create_rowset(txn_id, tablet_id, next_rowset_id(), schema_version, -1, schema);
    rowset.set_has_variant_type_in_schema(schema != nullptr);
    prepare_rowset(meta_service, rowset, res);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << label;
    res.Clear();
    ASSERT_NO_FATAL_FAILURE(commit_rowset(meta_service, rowset, res));
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << label << ", msg=" << res.status().msg();
    commit_txn(meta_service, db_id, txn_id, label);
}

static TabletSchemaCloudPB getVariantSchema() {
    TabletSchemaCloudPB schema;
    schema.set_schema_version(3);
    // columns
    ColumnPB var;
    var.set_type("VARIANT");
    var.set_unique_id(10);
    ColumnPB var_sub1;
    var_sub1.set_type("INT");
    var_sub1.set_unique_id(-1);
    schema.add_column()->CopyFrom(var_sub1);
    ColumnPB var_sub2;
    var_sub2.set_type("DOUBLE");
    var_sub2.set_unique_id(-1);
    schema.add_column()->CopyFrom(var_sub2);
    ColumnPB var_sparse_sub1;
    var_sparse_sub1.set_type("DOUBLE");
    var_sparse_sub1.set_unique_id(-1);
    var.add_sparse_columns()->CopyFrom(var_sparse_sub1);
    schema.add_column()->CopyFrom(var);

    // indexes
    doris::TabletIndexPB index1;
    index1.set_index_id(111);
    index1.set_index_suffix_name("aaabbbccc");
    schema.add_index()->CopyFrom(index1);

    doris::TabletIndexPB index2;
    index2.set_index_id(222);
    schema.add_index()->CopyFrom(index2);
    return schema;
}

TEST(DetachSchemaKVTest, RowsetTest) {
    auto meta_service = get_meta_service();
    // meta_service->resource_mgr().reset(); // Do not use resource manager

    auto sp = SyncPoint::get_instance();
    std::unique_ptr<int, std::function<void(int*)>> defer(
            (int*)0x01, [](int*) { SyncPoint::get_instance()->clear_all_call_backs(); });
    sp->set_call_back("get_instance_id", [&](auto&& args) {
        auto* ret = try_any_cast_ret<std::string>(args);
        ret->first = instance_id;
        ret->second = true;
    });
    sp->enable_processing();

    constexpr int64_t db_id = 10000;

    // new MS write with write_schema_kv=false, old MS read
    {
        constexpr auto table_id = 10001, index_id = 10002, partition_id = 10003, tablet_id = 10004;
        config::write_schema_kv = false;
        ASSERT_NO_FATAL_FAILURE(create_tablet(meta_service.get(), table_id, index_id, partition_id,
                                              tablet_id, next_rowset_id(), 1));
        ASSERT_NO_FATAL_FAILURE(
                insert_rowset(meta_service.get(), db_id, "101", table_id, tablet_id, 2)); // [2-2]
        // check saved values in txn_kv
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string rowset_key, rowset_val;
        meta_rowset_key({instance_id, tablet_id, 2}, &rowset_key); // [2-2]
        ASSERT_EQ(txn->get(rowset_key, &rowset_val), TxnErrorCode::TXN_OK);
        doris::RowsetMetaCloudPB saved_rowset;
        ASSERT_TRUE(saved_rowset.ParseFromString(rowset_val));
        ASSERT_TRUE(saved_rowset.has_tablet_schema());
        EXPECT_EQ(saved_rowset.tablet_schema().schema_version(), 2);
    }

    // old MS write, new MS read
    {
        constexpr auto table_id = 10011, index_id = 10012, partition_id = 10013, tablet_id = 10014;
        config::write_schema_kv = false;
        ASSERT_NO_FATAL_FAILURE(create_tablet(meta_service.get(), table_id, index_id, partition_id,
                                              tablet_id, next_rowset_id(), 1));
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        auto saved_rowset = create_rowset(10015, tablet_id, next_rowset_id(), 2, 2);
        std::string rowset_key, rowset_val;
        meta_rowset_key({instance_id, tablet_id, 2}, &rowset_key); // version=[2-2]
        ASSERT_TRUE(saved_rowset.SerializeToString(&rowset_val));
        txn->put(rowset_key, rowset_val);
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
        // check get rowset response
        GetRowsetResponse get_rowset_res;
        get_rowset(meta_service.get(), table_id, index_id, partition_id, tablet_id, get_rowset_res);
        ASSERT_EQ(get_rowset_res.rowset_meta_size(), 2);
        ASSERT_EQ(get_rowset_res.rowset_meta(0).end_version(), 1); // [0-1]
        ASSERT_TRUE(get_rowset_res.rowset_meta(0).has_tablet_schema());
        EXPECT_EQ(get_rowset_res.rowset_meta(0).tablet_schema().schema_version(), 1);
        EXPECT_EQ(get_rowset_res.rowset_meta(0).index_id(), index_id);
        EXPECT_EQ(get_rowset_res.rowset_meta(0).schema_version(), 1);
        ASSERT_EQ(get_rowset_res.rowset_meta(1).end_version(), 2); // [2-2]
        ASSERT_TRUE(get_rowset_res.rowset_meta(1).has_tablet_schema());
        EXPECT_EQ(get_rowset_res.rowset_meta(1).tablet_schema().schema_version(), 2);
        EXPECT_EQ(get_rowset_res.rowset_meta(1).index_id(), index_id);
        EXPECT_EQ(get_rowset_res.rowset_meta(1).schema_version(), 2);
    }

    // new MS write with write_schema_kv=true, new MS read
    {
        constexpr auto table_id = 10021, index_id = 10022, partition_id = 10023, tablet_id = 10024;
        config::write_schema_kv = true;
        ASSERT_NO_FATAL_FAILURE(create_tablet(meta_service.get(), table_id, index_id, partition_id,
                                              tablet_id, next_rowset_id(), 1));
        ASSERT_NO_FATAL_FAILURE(
                insert_rowset(meta_service.get(), db_id, "201", table_id, tablet_id, 2)); // [2-2]
        // check saved values in txn_kv
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string rowset_key, rowset_val;
        meta_rowset_key({instance_id, 10024, 2}, &rowset_key); // [2-2]
        ASSERT_EQ(txn->get(rowset_key, &rowset_val), TxnErrorCode::TXN_OK);
        doris::RowsetMetaCloudPB saved_rowset;
        ASSERT_TRUE(saved_rowset.ParseFromString(rowset_val));
        EXPECT_FALSE(saved_rowset.has_tablet_schema());
        EXPECT_EQ(saved_rowset.index_id(), index_id);
        EXPECT_EQ(saved_rowset.schema_version(), 2);
        // check get rowset response
        GetRowsetResponse get_rowset_res;
        get_rowset(meta_service.get(), table_id, index_id, partition_id, tablet_id, get_rowset_res);
        ASSERT_EQ(get_rowset_res.rowset_meta_size(), 2);
        ASSERT_EQ(get_rowset_res.rowset_meta(0).end_version(), 1); // [0-1]
        ASSERT_TRUE(get_rowset_res.rowset_meta(0).has_tablet_schema());
        EXPECT_EQ(get_rowset_res.rowset_meta(0).tablet_schema().schema_version(), 1);
        EXPECT_EQ(get_rowset_res.rowset_meta(0).index_id(), index_id);
        EXPECT_EQ(get_rowset_res.rowset_meta(0).schema_version(), 1);
        ASSERT_EQ(get_rowset_res.rowset_meta(1).end_version(), 2); // [2-2]
        ASSERT_TRUE(get_rowset_res.rowset_meta(1).has_tablet_schema());
        EXPECT_EQ(get_rowset_res.rowset_meta(1).tablet_schema().schema_version(), 2);
        EXPECT_EQ(get_rowset_res.rowset_meta(1).index_id(), index_id);
        EXPECT_EQ(get_rowset_res.rowset_meta(1).schema_version(), 2);
        ASSERT_TRUE(get_rowset_res.has_stats());
        EXPECT_EQ(get_rowset_res.stats().num_rows(), 100);
        EXPECT_EQ(get_rowset_res.stats().num_rowsets(), 2);
        EXPECT_EQ(get_rowset_res.stats().num_segments(), 1);
        EXPECT_EQ(get_rowset_res.stats().data_size(), 10000);
    }

    // new MS read rowsets committed by both old and new MS
    auto insert_and_get_rowset = [&meta_service](int64_t table_id, int64_t index_id,
                                                 int64_t partition_id, int64_t tablet_id,
                                                 int label_base,
                                                 google::protobuf::Arena* arena = nullptr,
                                                 const TabletSchemaCloudPB* schema = nullptr) {
        config::write_schema_kv = false;
        std::mt19937 rng(std::chrono::system_clock::now().time_since_epoch().count());
        std::uniform_int_distribution<int> dist1(1, 4);
        std::uniform_int_distribution<int> dist2(2, 7);
        std::vector<int> schema_versions {1};
        ASSERT_NO_FATAL_FAILURE(create_tablet(meta_service.get(), table_id, index_id, partition_id,
                                              tablet_id, next_rowset_id(), schema_versions[0]));
        for (int i = 0; i < 10; ++i) {
            schema_versions.push_back(dist1(rng));
            ASSERT_NO_FATAL_FAILURE(insert_rowset(meta_service.get(), db_id,
                                                  std::to_string(++label_base), table_id, tablet_id,
                                                  schema_versions.back(), schema));
        }
        config::write_schema_kv = true;
        for (int i = 0; i < 15; ++i) {
            schema_versions.push_back(dist2(rng));
            ASSERT_NO_FATAL_FAILURE(insert_rowset(meta_service.get(), db_id,
                                                  std::to_string(++label_base), table_id, tablet_id,
                                                  schema_versions.back(), schema));
        }
        // check get rowset response
        auto get_rowset_res = google::protobuf::Arena::CreateMessage<GetRowsetResponse>(arena);
        std::unique_ptr<int, std::function<void(int*)>> defer((int*)0x01, [&](int*) {
            if (!arena) delete get_rowset_res;
        });
        get_rowset(meta_service.get(), table_id, index_id, partition_id, tablet_id,
                   *get_rowset_res);
        ASSERT_EQ(get_rowset_res->rowset_meta_size(), schema_versions.size());
        for (int i = 0; i < schema_versions.size(); ++i) {
            auto& rowset = get_rowset_res->rowset_meta(i);
            ASSERT_EQ(rowset.end_version(), i + 1);
            ASSERT_TRUE(rowset.has_tablet_schema());
            EXPECT_EQ(rowset.tablet_schema().schema_version(), schema_versions[i]);
            EXPECT_EQ(rowset.index_id(), index_id);
            EXPECT_EQ(rowset.schema_version(), schema_versions[i]);
        }
        ASSERT_TRUE(get_rowset_res->has_stats());
        EXPECT_EQ(get_rowset_res->stats().num_rows(), 2500);
        EXPECT_EQ(get_rowset_res->stats().num_rowsets(), 26);
        EXPECT_EQ(get_rowset_res->stats().num_segments(), 25);
        EXPECT_EQ(get_rowset_res->stats().data_size(), 250000);
        if (schema != nullptr) {
            auto schema_version = get_rowset_res->rowset_meta(10).schema_version();
            get_rowset_res->mutable_rowset_meta(10)->mutable_tablet_schema()->set_schema_version(3);
            EXPECT_EQ(get_rowset_res->rowset_meta(10).tablet_schema().SerializeAsString(),
                      schema->SerializeAsString());
            get_rowset_res->mutable_rowset_meta(10)->mutable_tablet_schema()->set_schema_version(
                    schema_version);
        }
    };
    insert_and_get_rowset(10031, 10032, 10033, 10034, 300);
    // use arena
    google::protobuf::Arena arena;
    insert_and_get_rowset(10041, 10042, 10043, 10044, 400, &arena);
    TabletSchemaCloudPB schema = getVariantSchema();
    insert_and_get_rowset(10051, 10052, 10053, 10054, 500, &arena, &schema);
}

TEST(DetachSchemaKVTest, InsertExistedRowsetTest) {
    auto meta_service = get_meta_service();
    // meta_service->resource_mgr().reset(); // Do not use resource manager

    auto sp = SyncPoint::get_instance();
    std::unique_ptr<int, std::function<void(int*)>> defer(
            (int*)0x01, [](int*) { SyncPoint::get_instance()->clear_all_call_backs(); });
    sp->set_call_back("get_instance_id", [&](auto&& args) {
        auto* ret = try_any_cast_ret<std::string>(args);
        ret->first = instance_id;
        ret->second = true;
    });
    sp->enable_processing();

    // old MS commit rowset, new MS commit rowset again
    {
        constexpr auto table_id = 10001, index_id = 10002, partition_id = 10003, tablet_id = 10004;
        config::write_schema_kv = false;
        ASSERT_NO_FATAL_FAILURE(create_tablet(meta_service.get(), table_id, index_id, partition_id,
                                              tablet_id, next_rowset_id(), 1));
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        auto committed_rowset = create_rowset(10005, tablet_id, next_rowset_id(), 2, 2);
        std::string tmp_rowset_key, tmp_rowset_val;
        // 0:instance_id  1:txn_id  2:tablet_id
        meta_rowset_tmp_key({instance_id, 10005, tablet_id}, &tmp_rowset_key);
        ASSERT_TRUE(committed_rowset.SerializeToString(&tmp_rowset_val));
        txn->put(tmp_rowset_key, tmp_rowset_val);
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
        CreateRowsetResponse res;
        auto new_rowset = create_rowset(10005, tablet_id, next_rowset_id(), 2, 2);
        prepare_rowset(meta_service.get(), new_rowset, res);
        ASSERT_EQ(res.status().code(), MetaServiceCode::ALREADY_EXISTED);
        ASSERT_TRUE(res.has_existed_rowset_meta());
        EXPECT_EQ(res.existed_rowset_meta().rowset_id_v2(), committed_rowset.rowset_id_v2());
        EXPECT_EQ(res.existed_rowset_meta().index_id(), index_id);
        EXPECT_EQ(res.existed_rowset_meta().schema_version(), 2);
        ASSERT_TRUE(res.existed_rowset_meta().has_tablet_schema());
        EXPECT_EQ(res.existed_rowset_meta().tablet_schema().schema_version(), 2);
        res.Clear();
        commit_rowset(meta_service.get(), new_rowset, res);
        ASSERT_EQ(res.status().code(), MetaServiceCode::ALREADY_EXISTED);
        ASSERT_TRUE(res.has_existed_rowset_meta());
        EXPECT_EQ(res.existed_rowset_meta().rowset_id_v2(), committed_rowset.rowset_id_v2());
        EXPECT_EQ(res.existed_rowset_meta().index_id(), index_id);
        EXPECT_EQ(res.existed_rowset_meta().schema_version(), 2);
        ASSERT_TRUE(res.existed_rowset_meta().has_tablet_schema());
        EXPECT_EQ(res.existed_rowset_meta().tablet_schema().schema_version(), 2);
    }

    // new MS commit rowset, new MS commit rowset again
    auto insert_existed_rowset = [&meta_service](int64_t table_id, int64_t index_id,
                                                 int64_t partition_id, int64_t tablet_id,
                                                 int64_t txn_id,
                                                 google::protobuf::Arena* arena = nullptr) {
        config::write_schema_kv = true;
        ASSERT_NO_FATAL_FAILURE(create_tablet(meta_service.get(), table_id, index_id, partition_id,
                                              tablet_id, next_rowset_id(), 1));
        auto committed_rowset = create_rowset(txn_id, tablet_id, next_rowset_id(), 2, 2);
        auto res = google::protobuf::Arena::CreateMessage<CreateRowsetResponse>(arena);
        std::unique_ptr<int, std::function<void(int*)>> defer((int*)0x01, [&](int*) {
            if (!arena) delete res;
        });
        prepare_rowset(meta_service.get(), committed_rowset, *res);
        ASSERT_EQ(res->status().code(), MetaServiceCode::OK);
        res->Clear();
        commit_rowset(meta_service.get(), committed_rowset, *res);
        ASSERT_EQ(res->status().code(), MetaServiceCode::OK);
        res->Clear();
        auto new_rowset = create_rowset(txn_id, tablet_id, next_rowset_id(), 2, 2);
        prepare_rowset(meta_service.get(), new_rowset, *res);
        ASSERT_EQ(res->status().code(), MetaServiceCode::ALREADY_EXISTED);
        ASSERT_TRUE(res->has_existed_rowset_meta());
        EXPECT_EQ(res->existed_rowset_meta().rowset_id_v2(), committed_rowset.rowset_id_v2());
        EXPECT_EQ(res->existed_rowset_meta().index_id(), index_id);
        EXPECT_EQ(res->existed_rowset_meta().schema_version(), 2);
        ASSERT_TRUE(res->existed_rowset_meta().has_tablet_schema());
        EXPECT_EQ(res->existed_rowset_meta().tablet_schema().schema_version(), 2);
        res->Clear();
        commit_rowset(meta_service.get(), new_rowset, *res);
        ASSERT_EQ(res->status().code(), MetaServiceCode::ALREADY_EXISTED);
        ASSERT_TRUE(res->has_existed_rowset_meta());
        EXPECT_EQ(res->existed_rowset_meta().rowset_id_v2(), committed_rowset.rowset_id_v2());
        EXPECT_EQ(res->existed_rowset_meta().index_id(), index_id);
        EXPECT_EQ(res->existed_rowset_meta().schema_version(), 2);
        ASSERT_TRUE(res->existed_rowset_meta().has_tablet_schema());
        EXPECT_EQ(res->existed_rowset_meta().tablet_schema().schema_version(), 2);
    };
    insert_existed_rowset(10011, 10012, 10013, 10014, 10015);
    google::protobuf::Arena arena;
    insert_existed_rowset(10021, 10022, 10023, 10024, 10025, &arena);
}

TEST(SchemaKVTest, InsertExistedRowsetTest) {
    auto meta_service = get_meta_service();
    // meta_service->resource_mgr().reset(); // Do not use resource manager

    auto sp = SyncPoint::get_instance();
    std::unique_ptr<int, std::function<void(int*)>> defer(
            (int*)0x01, [](int*) { SyncPoint::get_instance()->clear_all_call_backs(); });
    sp->set_call_back("get_instance_id", [&](auto&& args) {
        auto* ret = try_any_cast_ret<std::string>(args);
        ret->first = instance_id;
        ret->second = true;
    });
    sp->enable_processing();

    config::write_schema_kv = true;
    config::meta_schema_value_version = 0;
    ASSERT_NO_FATAL_FAILURE(
            create_tablet(meta_service.get(), 10001, 10002, 10003, 10004, next_rowset_id(), 1));
    check_get_tablet(meta_service.get(), 10004, 1);

    config::meta_schema_value_version = 1;
    ASSERT_NO_FATAL_FAILURE(
            create_tablet(meta_service.get(), 10001, 10002, 10003, 10005, next_rowset_id(), 2));
    check_get_tablet(meta_service.get(), 10005, 2);
}

} // namespace doris::cloud
