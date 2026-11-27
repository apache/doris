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
#include <gtest/gtest.h>

#include <ctime>

#include "common/config.h"
#include "common/defer.h"
#include "cpp/sync_point.h"
#include "gen_cpp/cloud.pb.h"
#include "gen_cpp/olap_file.pb.h"
#include "meta-service/meta_service.h"
#include "meta-store/blob_message.h"
#include "meta-store/keys.h"
#include "meta-store/txn_kv.h"

namespace doris::cloud {
std::unique_ptr<MetaServiceProxy> get_meta_service();
void add_tablet(CreateTabletsRequest& request, int64_t table, int64_t index, int64_t partition,
                int64_t tablet);
RowsetMetaCloudPB create_rowset(int64_t txn, int64_t tablet, int partition, int64_t version,
                                int rows);
void prepare_rowset(MetaServiceProxy* service, const RowsetMetaCloudPB& rowset,
                    CreateRowsetResponse& response);
void commit_rowset(MetaServiceProxy* service, const RowsetMetaCloudPB& rowset,
                   CreateRowsetResponse& response);

namespace {

class SequenceRetentionServiceTest : public testing::Test {
protected:
    void SetUp() override {
        service = get_meta_service();
        ASSERT_NO_FATAL_FAILURE(create_tablet(22, 33));
    }

    void create_tablet(int64_t partition_id, int64_t tablet_id) {
        CreateTabletsRequest request;
        request.set_db_id(1);
        add_tablet(request, 11, 44, partition_id, tablet_id);
        auto* meta = request.mutable_tablet_metas(0);
        meta->set_sequence_retention_gap(kRetentionGap);
        meta->mutable_rs_metas(0)->set_requires_sequence_retention(true);
        CreateTabletsResponse response;
        brpc::Controller controller;
        service->create_tablets(&controller, &request, &response, nullptr);
        ASSERT_EQ(MetaServiceCode::OK, response.status().code()) << response.status().msg();
    }

    void begin_txn(const std::string& label = "sequence_retention_input") {
        BeginTxnRequest begin;
        auto* info = begin.mutable_txn_info();
        info->set_db_id(1);
        info->add_table_ids(11);
        info->set_label(label);
        info->set_timeout_ms(60000);
        BeginTxnResponse begun;
        brpc::Controller controller;
        service->begin_txn(&controller, &begin, &begun, nullptr);
        ASSERT_EQ(MetaServiceCode::OK, begun.status().code()) << begun.status().msg();
        txn_id = begun.txn_id();
    }

    GetTxnRequest get_txn_request() const {
        GetTxnRequest request;
        request.set_cloud_unique_id("test_cloud_unique_id");
        request.set_db_id(1);
        request.set_txn_id(txn_id);
        return request;
    }

    RecordSequenceRetentionPlanRequest record_request(int64_t partition_id = 22,
                                                      int64_t visible_version = 1) const {
        RecordSequenceRetentionPlanRequest request;
        request.set_cloud_unique_id("test_cloud_unique_id");
        request.set_db_id(1);
        request.set_txn_id(txn_id);
        auto* plan = request.mutable_plan();
        plan->set_table_id(11);
        plan->set_version_gap(kRetentionGap);
        plan->mutable_partition_versions()->emplace(partition_id, visible_version);
        return request;
    }

    void write_input(bool retention, int64_t finalized_version = 2, int64_t input_plan = 1) {
        ASSERT_NO_FATAL_FAILURE(begin_txn());
        if (retention) {
            auto request = record_request();
            RecordSequenceRetentionPlanResponse recorded;
            brpc::Controller controller;
            service->record_sequence_retention_plan(&controller, &request, &recorded, nullptr);
            ASSERT_EQ(MetaServiceCode::OK, recorded.status().code()) << recorded.status().msg();
            ASSERT_EQ(1, recorded.plan().partition_versions().at(22));
        }
        ASSERT_NO_FATAL_FAILURE(
                write_pending_rowset(retention ? input_plan : 0, finalized_version));
    }

    void write_pending_rowset(int64_t plan_version, int64_t finalized_version) {
        auto input = create_rowset(txn_id, 33, 22, -1, 1);
        if (plan_version > 0) {
            input.set_requires_sequence_retention(true);
            auto* retained = input.mutable_sequence_retention_input();
            retained->set_plan_version(plan_version);
            retained->set_original_segment_count(input.num_segments());
            retained->set_finalized_version(finalized_version);
        }
        CreateRowsetResponse prepared;
        ASSERT_NO_FATAL_FAILURE(prepare_rowset(service.get(), input, prepared));
        ASSERT_EQ(MetaServiceCode::OK, prepared.status().code()) << prepared.status().msg();
        CreateRowsetResponse committed;
        ASSERT_NO_FATAL_FAILURE(commit_rowset(service.get(), input, committed));
        ASSERT_EQ(MetaServiceCode::OK, committed.status().code()) << committed.status().msg();
        GetDeleteBitmapUpdateLockRequest lock;
        lock.set_cloud_unique_id("test_cloud_unique_id");
        lock.set_table_id(11);
        lock.add_partition_ids(22);
        lock.set_lock_id(txn_id);
        lock.set_initiator(1);
        lock.set_expiration(60);
        GetDeleteBitmapUpdateLockResponse locked;
        brpc::Controller controller;
        service->get_delete_bitmap_update_lock(&controller, &lock, &locked, nullptr);
        ASSERT_EQ(MetaServiceCode::OK, locked.status().code()) << locked.status().msg();
    }

    CommitTxnResponse commit(bool lazy = false, bool two_phase = false) {
        CommitTxnRequest request;
        request.set_cloud_unique_id("test_cloud_unique_id");
        request.set_db_id(1);
        request.set_txn_id(txn_id);
        request.add_mow_table_ids(11);
        request.set_enable_txn_lazy_commit(lazy);
        request.set_is_2pc(two_phase);
        CommitTxnResponse response;
        brpc::Controller controller;
        service->commit_txn(&controller, &request, &response, nullptr);
        return response;
    }

    void set_partition_version(int64_t version) {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(TxnErrorCode::TXN_OK, service->txn_kv()->create_txn(&txn));
        VersionPB value;
        value.set_version(version);
        txn->put(partition_version_key({"test_instance", 1, 11, 22}), value.SerializeAsString());
        ASSERT_EQ(TxnErrorCode::TXN_OK, txn->commit());
    }

    static constexpr int64_t kRetentionGap = 100;
    std::unique_ptr<MetaServiceProxy> service;
    int64_t txn_id = 0;
};

TEST_F(SequenceRetentionServiceTest, CreateValidatesEveryGapBeforeWritingTablets) {
    CreateTabletsRequest request;
    request.set_cloud_unique_id("test_cloud_unique_id");
    request.set_db_id(1);
    auto* valid = request.add_tablet_metas();
    valid->set_tablet_id(90001);
    valid->set_table_id(11);
    valid->set_index_id(44);
    valid->set_partition_id(22);
    auto* invalid = request.add_tablet_metas();
    invalid->set_tablet_id(90002);
    invalid->set_sequence_retention_gap(-1);
    CreateTabletsResponse response;
    brpc::Controller controller;
    service->create_tablets(&controller, &request, &response, nullptr);
    ASSERT_EQ(MetaServiceCode::INVALID_ARGUMENT, response.status().code());
    ASSERT_TRUE(response.supports_sequence_retention());
    EXPECT_NE(std::string::npos, response.status().msg().find("Invalid sequence retention gap"));
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(TxnErrorCode::TXN_OK, service->txn_kv()->create_txn(&txn));
    std::string value;
    EXPECT_EQ(TxnErrorCode::TXN_KEY_NOT_FOUND,
              txn->get(meta_tablet_idx_key({"test_instance", 90001}), &value));
}

TEST_F(SequenceRetentionServiceTest, CapturedPartitionRequiresRetainedInput) {
    ASSERT_NO_FATAL_FAILURE(write_input(false));
    auto request = record_request();
    RecordSequenceRetentionPlanResponse recorded;
    brpc::Controller controller;
    service->record_sequence_retention_plan(&controller, &request, &recorded, nullptr);
    ASSERT_EQ(MetaServiceCode::OK, recorded.status().code()) << recorded.status().msg();
    auto response = commit();
    EXPECT_EQ(MetaServiceCode::INVALID_ARGUMENT, response.status().code())
            << response.status().msg();
    EXPECT_NE(std::string::npos,
              response.status().msg().find("input does not match the planning version"));
}

TEST_F(SequenceRetentionServiceTest, InputRequiresPersistedPlanningVersion) {
    ASSERT_NO_FATAL_FAILURE(begin_txn());
    ASSERT_NO_FATAL_FAILURE(write_pending_rowset(1, 2));
    auto response = commit();
    EXPECT_EQ(MetaServiceCode::INVALID_ARGUMENT, response.status().code())
            << response.status().msg();
    EXPECT_NE(std::string::npos, response.status().msg().find("not captured"));
}

TEST_F(SequenceRetentionServiceTest, InputWithoutFinalDecisionsCannotCommit) {
    ASSERT_NO_FATAL_FAILURE(write_input(true, 0));
    auto response = commit();
    EXPECT_EQ(MetaServiceCode::INVALID_ARGUMENT, response.status().code())
            << response.status().msg();
    EXPECT_NE(std::string::npos, response.status().msg().find("no final decisions"));
}

TEST_F(SequenceRetentionServiceTest, CommitWindowUsesActualAssignedVersion) {
    ASSERT_NO_FATAL_FAILURE(write_input(true));
    ASSERT_NO_FATAL_FAILURE(set_partition_version(101));
    auto response = commit();
    EXPECT_EQ(MetaServiceCode::INVALID_ARGUMENT, response.status().code())
            << response.status().msg();
    EXPECT_NE(std::string::npos, response.status().msg().find("exceeded version window"));
}

TEST_F(SequenceRetentionServiceTest, CommitWindowIncludesItsUpperBound) {
    ASSERT_NO_FATAL_FAILURE(write_input(true, 101));
    ASSERT_NO_FATAL_FAILURE(set_partition_version(100));
    auto response = commit();
    EXPECT_EQ(MetaServiceCode::OK, response.status().code()) << response.status().msg();
}

TEST_F(SequenceRetentionServiceTest, InputPlanningVersionMustMatchTheTransaction) {
    ASSERT_NO_FATAL_FAILURE(write_input(true, 2, 2));
    auto response = commit();
    EXPECT_EQ(MetaServiceCode::INVALID_ARGUMENT, response.status().code());
    EXPECT_NE(std::string::npos,
              response.status().msg().find("input does not match the planning version"));
}

TEST_F(SequenceRetentionServiceTest, CommittedRetryKeepsItsOriginalDecisionAfterVersionsAdvance) {
    ASSERT_NO_FATAL_FAILURE(write_input(true));
    auto committed = commit();
    ASSERT_EQ(MetaServiceCode::OK, committed.status().code()) << committed.status().msg();
    ASSERT_NO_FATAL_FAILURE(set_partition_version(1000));
    auto retried = commit();
    EXPECT_TRUE(retried.status().code() == MetaServiceCode::OK ||
                retried.status().code() == MetaServiceCode::TXN_ALREADY_VISIBLE)
            << retried.status().msg();
}

TEST_F(SequenceRetentionServiceTest, LazyCommitChecksTheVersionWindowBeforeItsDurableDecision) {
    int32_t old_threshold = config::txn_lazy_commit_rowsets_thresold;
    DORIS_CLOUD_DEFER {
        config::txn_lazy_commit_rowsets_thresold = old_threshold;
    };
    config::txn_lazy_commit_rowsets_thresold = 0;
    ASSERT_NO_FATAL_FAILURE(write_input(true));
    ASSERT_NO_FATAL_FAILURE(set_partition_version(101));
    auto response = commit(true);
    EXPECT_TRUE(response.is_lazy_commit());
    EXPECT_EQ(MetaServiceCode::INVALID_ARGUMENT, response.status().code())
            << response.status().msg();
    EXPECT_NE(std::string::npos, response.status().msg().find("exceeded version window"));
}

TEST_F(SequenceRetentionServiceTest, TwoPhasePrepareDoesNotReserveTheVersionWindow) {
    ASSERT_NO_FATAL_FAILURE(write_input(true));
    PrecommitTxnRequest request;
    request.set_cloud_unique_id("test_cloud_unique_id");
    request.set_db_id(1);
    request.set_txn_id(txn_id);
    request.set_precommit_timeout_ms(60000);
    PrecommitTxnResponse prepared;
    brpc::Controller controller;
    service->precommit_txn(&controller, &request, &prepared, nullptr);
    ASSERT_EQ(MetaServiceCode::OK, prepared.status().code()) << prepared.status().msg();

    auto record = record_request(23);
    RecordSequenceRetentionPlanResponse denied;
    service->record_sequence_retention_plan(&controller, &record, &denied, nullptr);
    EXPECT_EQ(MetaServiceCode::INVALID_ARGUMENT, denied.status().code());
    EXPECT_NE(std::string::npos, denied.status().msg().find("Cannot capture a new"));

    ASSERT_NO_FATAL_FAILURE(set_partition_version(101));
    auto response = commit(false, true);
    EXPECT_EQ(MetaServiceCode::INVALID_ARGUMENT, response.status().code())
            << response.status().msg();
    EXPECT_NE(std::string::npos, response.status().msg().find("exceeded version window"));
}

TEST_F(SequenceRetentionServiceTest, RecordsSeveralObservedVersionsWithoutRefreshingThem) {
    ASSERT_NO_FATAL_FAILURE(begin_txn());
    brpc::Controller controller;
    // The version advanced after FE read it. Recording still uses the supplied observation.
    ASSERT_NO_FATAL_FAILURE(set_partition_version(20));
    auto request = record_request(22, 19);
    request.mutable_plan()->mutable_partition_versions()->emplace(23, 1);
    RecordSequenceRetentionPlanResponse recorded;
    service->record_sequence_retention_plan(&controller, &request, &recorded, nullptr);
    ASSERT_EQ(MetaServiceCode::OK, recorded.status().code()) << recorded.status().msg();
    const auto& plan = recorded.plan();
    EXPECT_EQ(kRetentionGap, plan.version_gap());
    ASSERT_EQ(2, plan.partition_versions_size());
    EXPECT_EQ(19, plan.partition_versions().at(22));
    EXPECT_EQ(1, plan.partition_versions().at(23));

    (*request.mutable_plan()->mutable_partition_versions())[22] = 20;
    (*request.mutable_plan()->mutable_partition_versions())[23] = 2;
    RecordSequenceRetentionPlanResponse retried;
    service->record_sequence_retention_plan(&controller, &request, &retried, nullptr);
    ASSERT_EQ(MetaServiceCode::OK, retried.status().code()) << retried.status().msg();
    const auto& versions = retried.plan().partition_versions();
    EXPECT_EQ(19, versions.at(22));
    EXPECT_EQ(1, versions.at(23));
}

TEST_F(SequenceRetentionServiceTest, PlanningContextSurvivesReadbackAndKeepsFirstVersion) {
    ASSERT_NO_FATAL_FAILURE(write_input(true));
    auto request = record_request(22, 20);
    brpc::Controller controller;
    RecordSequenceRetentionPlanResponse retried;
    service->record_sequence_retention_plan(&controller, &request, &retried, nullptr);
    ASSERT_EQ(MetaServiceCode::OK, retried.status().code()) << retried.status().msg();
    EXPECT_EQ(1, retried.plan().partition_versions().at(22));
    request.mutable_plan()->set_version_gap(kRetentionGap + 1);
    RecordSequenceRetentionPlanResponse changed;
    service->record_sequence_retention_plan(&controller, &request, &changed, nullptr);
    EXPECT_EQ(MetaServiceCode::INVALID_ARGUMENT, changed.status().code());
    auto get = get_txn_request();
    GetTxnResponse recovered;
    service->get_txn(&controller, &get, &recovered, nullptr);
    ASSERT_EQ(MetaServiceCode::OK, recovered.status().code());
    ASSERT_EQ(1, recovered.txn_info().sequence_retention_plans_size());
    const auto& plan = recovered.txn_info().sequence_retention_plans(0);
    EXPECT_EQ(kRetentionGap, plan.version_gap());
    EXPECT_EQ(1, plan.partition_versions().at(22));
}

TEST_F(SequenceRetentionServiceTest, ConcurrentPlanningCannotReplaceTheFirstCapturedSnapshot) {
    ASSERT_NO_FATAL_FAILURE(begin_txn("concurrent_sequence_planning"));
    brpc::Controller controller;
    auto request = record_request(22, 19);
    auto* sync = SyncPoint::get_instance();
    DORIS_CLOUD_DEFER {
        sync->disable_processing();
        sync->clear_all_call_backs();
    };
    bool raced = false;
    sync->set_call_back("record_sequence_retention_plan::before_commit", [&](auto&&) {
        if (raced) return;
        raced = true;
        auto second = record_request(22, 20);
        RecordSequenceRetentionPlanResponse winner;
        brpc::Controller inner;
        service->record_sequence_retention_plan(&inner, &second, &winner, nullptr);
        ASSERT_EQ(MetaServiceCode::OK, winner.status().code()) << winner.status().msg();
    });
    sync->enable_processing();
    RecordSequenceRetentionPlanResponse outer;
    service->record_sequence_retention_plan(&controller, &request, &outer, nullptr);
    EXPECT_TRUE(raced);
    if (outer.status().code() == MetaServiceCode::OK) {
        EXPECT_EQ(20, outer.plan().partition_versions().at(22));
    } else {
        EXPECT_EQ(MetaServiceCode::KV_TXN_CONFLICT, outer.status().code());
    }
    auto get = get_txn_request();
    GetTxnResponse state;
    service->get_txn(&controller, &get, &state, nullptr);
    ASSERT_EQ(MetaServiceCode::OK, state.status().code());
    ASSERT_EQ(1, state.txn_info().sequence_retention_plans_size());
    EXPECT_EQ(20, state.txn_info().sequence_retention_plans(0).partition_versions().at(22));
}

} // namespace
} // namespace doris::cloud
