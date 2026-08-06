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
#include <gtest/gtest.h>

#include "common/config.h"
#include "meta-service/meta_service.h"
#include "meta-store/keys.h"
#include "meta-store/mem_txn_kv.h"
#include "mock_resource_manager.h"
#include "rate-limiter/rate_limiter.h"

namespace doris::cloud {

// Test: commit_txn always updates last_active_cluster_id
// MS always updates this field, BE controls whether to use it for compaction skipping
TEST(CompactionRWSeparationTest, CommitTxnUpdatesLastActiveCluster) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    // Clear data
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->remove("\x00", "\xfe");
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    auto rs = std::make_shared<MockResourceManager>(txn_kv);
    auto rl = std::make_shared<RateLimiter>();
    auto snapshot = std::make_shared<SnapshotManager>(txn_kv);
    MetaServiceImpl meta_service(txn_kv, rs, rl, snapshot);

    int64_t db_id = 1;
    int64_t table_id = 100;
    int64_t index_id = 100;
    int64_t partition_id = 100;
    int64_t tablet_id = 10001;

    // Create tablet
    {
        brpc::Controller cntl;
        CreateTabletsRequest req;
        CreateTabletsResponse res;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_db_id(db_id);

        auto tablet = req.add_tablet_metas();
        tablet->set_table_id(table_id);
        tablet->set_index_id(index_id);
        tablet->set_partition_id(partition_id);
        tablet->set_tablet_id(tablet_id);
        auto schema = tablet->mutable_schema();
        schema->set_schema_version(0);
        auto first_rowset = tablet->add_rs_metas();
        first_rowset->set_rowset_id(0);
        first_rowset->set_rowset_id_v2("rowset_0");
        first_rowset->set_start_version(0);
        first_rowset->set_end_version(1);
        first_rowset->mutable_tablet_schema()->CopyFrom(*schema);

        meta_service.create_tablets(&cntl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();
    }

    // Begin txn
    int64_t txn_id = -1;
    {
        brpc::Controller cntl;
        BeginTxnRequest req;
        BeginTxnResponse res;
        req.set_cloud_unique_id("test_cloud_unique_id");
        auto txn_info = req.mutable_txn_info();
        txn_info->set_db_id(db_id);
        txn_info->set_label("test_label_1");
        txn_info->add_table_ids(table_id);
        txn_info->set_timeout_ms(36000);
        // FE sets load_cluster_id during begin_txn for compaction RW separation
        txn_info->set_load_cluster_id(mock_cluster_id);

        meta_service.begin_txn(&cntl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();
        txn_id = res.txn_id();
    }

    // Prepare rowset
    {
        brpc::Controller cntl;
        CreateRowsetRequest req;
        CreateRowsetResponse res;
        req.set_cloud_unique_id("test_cloud_unique_id");

        auto rowset = req.mutable_rowset_meta();
        rowset->set_rowset_id(0);
        rowset->set_rowset_id_v2("rowset_txn_1");
        rowset->set_tablet_id(tablet_id);
        rowset->set_partition_id(partition_id);
        rowset->set_txn_id(txn_id);
        rowset->set_num_segments(1);
        rowset->set_num_rows(100);
        rowset->set_data_disk_size(1024);
        rowset->mutable_tablet_schema()->set_schema_version(0);
        rowset->set_txn_expiration(::time(nullptr) + 3600);

        meta_service.prepare_rowset(&cntl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();
    }

    // Commit rowset
    {
        brpc::Controller cntl;
        CreateRowsetRequest req;
        CreateRowsetResponse res;
        req.set_cloud_unique_id("test_cloud_unique_id");

        auto rowset = req.mutable_rowset_meta();
        rowset->set_rowset_id(0);
        rowset->set_rowset_id_v2("rowset_txn_1");
        rowset->set_tablet_id(tablet_id);
        rowset->set_partition_id(partition_id);
        rowset->set_txn_id(txn_id);
        rowset->set_num_segments(1);
        rowset->set_num_rows(100);
        rowset->set_data_disk_size(1024);
        rowset->mutable_tablet_schema()->set_schema_version(0);
        rowset->set_txn_expiration(::time(nullptr) + 3600);

        meta_service.commit_rowset(&cntl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();
    }

    // Commit txn
    {
        brpc::Controller cntl;
        CommitTxnRequest req;
        CommitTxnResponse res;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_db_id(db_id);
        req.set_txn_id(txn_id);

        meta_service.commit_txn(&cntl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();
    }

    // Get rowset and check last_active_cluster_id
    {
        brpc::Controller cntl;
        GetRowsetRequest req;
        GetRowsetResponse res;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.mutable_idx()->set_tablet_id(tablet_id);
        req.set_start_version(0);
        req.set_end_version(-1);
        req.set_base_compaction_cnt(0);
        req.set_cumulative_compaction_cnt(0);
        req.set_cumulative_point(2);

        meta_service.get_rowset(&cntl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();

        // Check that last_active_cluster_id is always set (MS always updates it)
        EXPECT_TRUE(res.stats().has_last_active_cluster_id());
        EXPECT_EQ(res.stats().last_active_cluster_id(), mock_cluster_id);
    }
}

} // namespace doris::cloud
