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
#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "common/config.h"
#include "common/logging.h"
#include "meta-service/meta_service.h"
#include "meta-store/keys.h"
#include "meta-store/mem_txn_kv.h"
#include "meta-store/txn_kv.h"
#include "rate-limiter/rate_limiter.h"
#include "resource-manager/resource_manager.h"

namespace doris::cloud {

// Mock resource manager that supports multiple clusters
class MultiClusterMockResourceManager : public ResourceManager {
public:
    MultiClusterMockResourceManager(std::shared_ptr<TxnKv> txn_kv) : ResourceManager(txn_kv) {}
    ~MultiClusterMockResourceManager() override = default;

    int init() override { return 0; }

    void set_cluster_for_node(const std::string& cloud_unique_id, const std::string& cluster_id) {
        node_to_cluster_[cloud_unique_id] = cluster_id;
    }

    void add_cluster(const std::string& cluster_id, ClusterStatus status) {
        ClusterPB cluster;
        cluster.set_cluster_id(cluster_id);
        cluster.set_cluster_name(cluster_id + "_name");
        cluster.set_cluster_status(status);
        cluster.set_mtime(::time(nullptr) * 1000);
        clusters_[cluster_id] = cluster;
    }

    std::string get_node(const std::string& cloud_unique_id,
                         std::vector<NodeInfo>* nodes) override {
        auto it = node_to_cluster_.find(cloud_unique_id);
        if (it != node_to_cluster_.end()) {
            NodeInfo info {Role::COMPUTE_NODE, "test_instance", it->second + "_name", it->second};
            nodes->push_back(info);
            return "";
        }
        // Default cluster
        NodeInfo info {Role::COMPUTE_NODE, "test_instance", "default_cluster_name",
                       "default_cluster"};
        nodes->push_back(info);
        return "";
    }

    std::pair<TxnErrorCode, std::string> get_instance(std::shared_ptr<Transaction> txn,
                                                      const std::string& instance_id,
                                                      InstanceInfoPB* inst_pb) override {
        inst_pb->set_instance_id(instance_id);
        for (const auto& [id, cluster] : clusters_) {
            *inst_pb->add_clusters() = cluster;
        }
        return {TxnErrorCode::TXN_OK, ""};
    }

    std::pair<MetaServiceCode, std::string> add_cluster(const std::string& instance_id,
                                                        const ClusterInfo& cluster) override {
        return std::make_pair(MetaServiceCode::OK, "");
    }

    std::pair<MetaServiceCode, std::string> drop_cluster(const std::string& instance_id,
                                                         const ClusterInfo& cluster) override {
        return std::make_pair(MetaServiceCode::OK, "");
    }

    std::pair<MetaServiceCode, std::string> refresh_instance(
            const std::string& instance_id) override {
        return std::make_pair(MetaServiceCode::OK, "");
    }

    std::string update_cluster(
            const std::string& instance_id, const ClusterInfo& cluster,
            std::function<bool(const ClusterPB&)> filter,
            std::function<std::string(ClusterPB&, std::vector<ClusterPB>& clusters_in_instance)>
                    action,
            bool replace_if_existing_empty_target_cluster) override {
        return "";
    }

    std::string modify_nodes(const std::string& instance_id, const std::vector<NodeInfo>& to_add,
                             const std::vector<NodeInfo>& to_del) override {
        return "";
    }

private:
    std::unordered_map<std::string, std::string> node_to_cluster_;
    std::unordered_map<std::string, ClusterPB> clusters_;
};

class CompactionRWSeparationTest : public testing::Test {
public:
    void SetUp() override {
        // Save original config
        original_enable_ = config::enable_compaction_rw_separation;
        config::enable_compaction_rw_separation = true;

        // Create TxnKv
        txn_kv_ = std::make_shared<MemTxnKv>();
        ASSERT_EQ(txn_kv_->init(), 0);

        // Clear all data
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
        txn->remove("\x00", "\xfe");
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        // Create mock resource manager
        resource_mgr_ = std::make_shared<MultiClusterMockResourceManager>(txn_kv_);
        resource_mgr_->set_cluster_for_node("cloud_unique_id_write", "cluster_write");
        resource_mgr_->set_cluster_for_node("cloud_unique_id_read", "cluster_read");
        resource_mgr_->add_cluster("cluster_write", ClusterStatus::NORMAL);
        resource_mgr_->add_cluster("cluster_read", ClusterStatus::NORMAL);

        // Create meta service
        auto rl = std::make_shared<RateLimiter>();
        auto snapshot = std::make_shared<SnapshotManager>(txn_kv_);
        meta_service_ = std::make_unique<MetaServiceImpl>(txn_kv_, resource_mgr_, rl, snapshot);
    }

    void TearDown() override {
        config::enable_compaction_rw_separation = original_enable_;
        meta_service_.reset();
        resource_mgr_.reset();
        txn_kv_.reset();
    }

protected:
    void create_tablet(int64_t table_id, int64_t index_id, int64_t partition_id,
                       int64_t tablet_id) {
        brpc::Controller cntl;
        CreateTabletsRequest req;
        CreateTabletsResponse res;
        req.set_cloud_unique_id("cloud_unique_id_write");
        req.set_db_id(1);

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

        meta_service_->create_tablets(&cntl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    int64_t begin_txn(int64_t db_id, const std::string& label, int64_t table_id,
                      const std::string& cloud_unique_id) {
        brpc::Controller cntl;
        BeginTxnRequest req;
        BeginTxnResponse res;
        req.set_cloud_unique_id(cloud_unique_id);
        auto txn_info = req.mutable_txn_info();
        txn_info->set_db_id(db_id);
        txn_info->set_label(label);
        txn_info->add_table_ids(table_id);
        txn_info->set_timeout_ms(36000);

        meta_service_->begin_txn(&cntl, &req, &res, nullptr);
        EXPECT_EQ(res.status().code(), MetaServiceCode::OK);
        return res.txn_id();
    }

    void prepare_rowset(int64_t txn_id, int64_t tablet_id, int64_t partition_id,
                        const std::string& cloud_unique_id) {
        brpc::Controller cntl;
        CreateRowsetRequest req;
        CreateRowsetResponse res;
        req.set_cloud_unique_id(cloud_unique_id);

        auto rowset = req.mutable_rowset_meta();
        rowset->set_rowset_id(0);
        rowset->set_rowset_id_v2("rowset_" + std::to_string(txn_id));
        rowset->set_tablet_id(tablet_id);
        rowset->set_partition_id(partition_id);
        rowset->set_txn_id(txn_id);
        rowset->set_num_segments(1);
        rowset->set_num_rows(100);
        rowset->set_data_disk_size(1024);

        meta_service_->prepare_rowset(&cntl, &req, &res, nullptr);
        EXPECT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    void commit_txn(int64_t db_id, int64_t txn_id, const std::string& cloud_unique_id) {
        brpc::Controller cntl;
        CommitTxnRequest req;
        CommitTxnResponse res;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_db_id(db_id);
        req.set_txn_id(txn_id);

        meta_service_->commit_txn(&cntl, &req, &res, nullptr);
        EXPECT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    GetRowsetResponse get_rowset(int64_t tablet_id, const std::string& cloud_unique_id) {
        brpc::Controller cntl;
        GetRowsetRequest req;
        GetRowsetResponse res;
        req.set_cloud_unique_id(cloud_unique_id);
        req.mutable_idx()->set_tablet_id(tablet_id);
        req.set_start_version(0);
        req.set_end_version(-1);
        req.set_base_compaction_cnt(0);
        req.set_cumulative_compaction_cnt(0);
        req.set_cumulative_point(0);

        meta_service_->get_rowset(&cntl, &req, &res, nullptr);
        return res;
    }

    TabletStatsPB get_tablet_stats_pb(int64_t tablet_id) {
        std::string key = stats_tablet_key({"test_instance", 100, 100, 100, tablet_id});
        std::string val;
        std::unique_ptr<Transaction> txn;
        txn_kv_->create_txn(&txn);
        txn->get(key, &val);

        TabletStatsPB stats;
        stats.ParseFromString(val);
        return stats;
    }

    std::shared_ptr<TxnKv> txn_kv_;
    std::shared_ptr<MultiClusterMockResourceManager> resource_mgr_;
    std::unique_ptr<MetaServiceImpl> meta_service_;
    bool original_enable_;
};

// Test: commit_txn updates last_active_cluster_id when load has data
TEST_F(CompactionRWSeparationTest, CommitTxnUpdatesLastActiveCluster) {
    int64_t tablet_id = 10002;
    create_tablet(100, 100, 100, tablet_id);

    // Begin and commit a transaction from write cluster
    int64_t txn_id = begin_txn(1, "label_1", 100, "cloud_unique_id_write");
    prepare_rowset(txn_id, tablet_id, 100, "cloud_unique_id_write");
    commit_txn(1, txn_id, "cloud_unique_id_write");

    // Get rowset and verify last_active_cluster_id is set
    auto res = get_rowset(tablet_id, "cloud_unique_id_read");
    EXPECT_EQ(res.status().code(), MetaServiceCode::OK);
    EXPECT_TRUE(res.stats().has_last_active_cluster_id());
    EXPECT_EQ(res.stats().last_active_cluster_id(), "cluster_write");
}

// Test: Feature disabled should not update last_active_cluster_id
TEST_F(CompactionRWSeparationTest, FeatureDisabledNoClusterUpdate) {
    config::enable_compaction_rw_separation = false;

    int64_t tablet_id = 10004;
    create_tablet(100, 100, 100, tablet_id);

    // Load from write cluster
    int64_t txn_id = begin_txn(1, "label_3", 100, "cloud_unique_id_write");
    prepare_rowset(txn_id, tablet_id, 100, "cloud_unique_id_write");
    commit_txn(1, txn_id, "cloud_unique_id_write");

    // Get rowset should not have last_active_cluster_id when feature is disabled
    auto res = get_rowset(tablet_id, "cloud_unique_id_read");
    EXPECT_EQ(res.status().code(), MetaServiceCode::OK);
    EXPECT_FALSE(res.stats().has_last_active_cluster_id());
}

// Test: Different clusters loading to different tablets
TEST_F(CompactionRWSeparationTest, DifferentClustersLoadDifferentTablets) {
    int64_t tablet_id_1 = 10005;
    int64_t tablet_id_2 = 10006;
    create_tablet(100, 100, 100, tablet_id_1);
    create_tablet(100, 100, 100, tablet_id_2);

    // Write cluster loads to tablet 1
    int64_t txn_id_1 = begin_txn(1, "label_4", 100, "cloud_unique_id_write");
    prepare_rowset(txn_id_1, tablet_id_1, 100, "cloud_unique_id_write");
    commit_txn(1, txn_id_1, "cloud_unique_id_write");

    // Read cluster loads to tablet 2
    int64_t txn_id_2 = begin_txn(1, "label_5", 100, "cloud_unique_id_read");
    prepare_rowset(txn_id_2, tablet_id_2, 100, "cloud_unique_id_read");
    commit_txn(1, txn_id_2, "cloud_unique_id_read");

    // Verify each tablet has correct last_active_cluster_id
    auto res1 = get_rowset(tablet_id_1, "cloud_unique_id_read");
    EXPECT_EQ(res1.stats().last_active_cluster_id(), "cluster_write");

    auto res2 = get_rowset(tablet_id_2, "cloud_unique_id_write");
    EXPECT_EQ(res2.stats().last_active_cluster_id(), "cluster_read");
}

// Test: get_cluster_status RPC returns all cluster status
TEST_F(CompactionRWSeparationTest, GetClusterStatusRpc) {
    brpc::Controller cntl;
    GetClusterStatusRequest req;
    GetClusterStatusResponse res;
    req.add_cloud_unique_ids("cloud_unique_id_write");

    meta_service_->get_cluster_status(&cntl, &req, &res, nullptr);
    EXPECT_EQ(res.status().code(), MetaServiceCode::OK);

    // Should have at least one detail with clusters
    ASSERT_GE(res.details_size(), 1);
    bool found_write_cluster = false;
    bool found_read_cluster = false;
    for (const auto& detail : res.details()) {
        for (const auto& cluster : detail.clusters()) {
            if (cluster.cluster_id() == "cluster_write") {
                found_write_cluster = true;
                EXPECT_EQ(cluster.cluster_status(), ClusterStatus::NORMAL);
            }
            if (cluster.cluster_id() == "cluster_read") {
                found_read_cluster = true;
                EXPECT_EQ(cluster.cluster_status(), ClusterStatus::NORMAL);
            }
        }
    }
    EXPECT_TRUE(found_write_cluster);
    EXPECT_TRUE(found_read_cluster);
}

} // namespace doris::cloud
