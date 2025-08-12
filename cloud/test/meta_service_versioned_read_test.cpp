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
#include <bvar/window.h>
#include <fmt/core.h>
#include <gen_cpp/cloud.pb.h>
#include <gen_cpp/olap_file.pb.h>
#include <google/protobuf/repeated_field.h>
#include <gtest/gtest.h>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <random>
#include <string>
#include <thread>

#include "common/config.h"
#include "common/logging.h"
#include "common/util.h"
#include "cpp/sync_point.h"
#include "meta-service/meta_service.h"
#include "meta-service/meta_service_helper.h"
#include "meta-store/document_message.h"
#include "meta-store/keys.h"
#include "meta-store/mem_txn_kv.h"
#include "meta-store/meta_reader.h"
#include "meta-store/txn_kv.h"
#include "meta-store/txn_kv_error.h"
#include "meta-store/versioned_value.h"
#include "mock_resource_manager.h"
#include "rate-limiter/rate_limiter.h"
#include "resource-manager/resource_manager.h"

namespace doris::cloud {

// External functions from meta_service_test.cpp
extern std::unique_ptr<MetaServiceProxy> get_meta_service();
extern std::unique_ptr<MetaServiceProxy> get_meta_service(bool mock_resource_mgr);
extern void create_tablet(MetaServiceProxy* meta_service, int64_t table_id, int64_t index_id,
                          int64_t partition_id, int64_t tablet_id);
extern doris::RowsetMetaCloudPB create_rowset(int64_t txn_id, int64_t tablet_id, int partition_id,
                                              int64_t version, int num_rows);
extern void commit_rowset(MetaServiceProxy* meta_service, const doris::RowsetMetaCloudPB& rowset,
                          CreateRowsetResponse& res);
extern void insert_rowset(MetaServiceProxy* meta_service, int64_t db_id, const std::string& label,
                          int64_t table_id, int64_t partition_id, int64_t tablet_id);
extern void add_tablet(CreateTabletsRequest& req, int64_t table_id, int64_t index_id,
                       int64_t partition_id, int64_t tablet_id);

void insert_compact_rowset(Transaction* txn, std::string instance_id, int64_t tablet_id,
                           int64_t partition_id, int64_t start_version, int64_t end_version,
                           int num_rows) {
    doris::RowsetMetaCloudPB compact_rowset =
            create_rowset(1, tablet_id, partition_id, start_version, num_rows);
    compact_rowset.set_end_version(end_version);
    std::string key = versioned::meta_rowset_compact_key({instance_id, tablet_id, end_version});
    ASSERT_TRUE(versioned::document_put(txn, key, std::move(compact_rowset)));
}

// Create a MULTI_VERSION_READ_WRITE instance and refresh the resource manager.
static void create_and_refresh_instance(MetaServiceProxy* service, std::string instance_id) {
    // write instance
    InstanceInfoPB instance_info;
    instance_info.set_instance_id(instance_id);
    instance_info.set_multi_version_status(MULTI_VERSION_READ_WRITE);
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->put(instance_key(instance_id), instance_info.SerializeAsString());
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    service->resource_mgr()->refresh_instance(instance_id);
    ASSERT_TRUE(service->resource_mgr()->is_version_write_enabled(instance_id));
}

#define MOCK_GET_INSTANCE_ID(instance_id)                                          \
    DORIS_CLOUD_DEFER {                                                            \
        SyncPoint::get_instance()->clear_all_call_backs();                         \
    };                                                                             \
    SyncPoint::get_instance()->set_call_back("get_instance_id", [&](auto&& args) { \
        auto* ret = try_any_cast_ret<std::string>(args);                           \
        ret->first = instance_id;                                                  \
        ret->second = true;                                                        \
    });                                                                            \
    SyncPoint::get_instance()->enable_processing();

TEST(MetaServiceVersionedReadTest, GetVersion) {
    auto service = get_meta_service(false);

    int64_t table_id = 1;
    int64_t partition_id = 1;
    int64_t tablet_id = 1;

    std::string instance_id = "test_cloud_instance_id";
    std::string cloud_unique_id = fmt::format("1:{}:1", instance_id);

    MOCK_GET_INSTANCE_ID(instance_id);
    create_and_refresh_instance(service.get(), instance_id);

    // INVALID_ARGUMENT
    {
        brpc::Controller ctrl;
        GetVersionRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_table_id(table_id);
        req.set_partition_id(partition_id);

        GetVersionResponse resp;
        service->get_version(&ctrl, &req, &resp, nullptr);

        ASSERT_EQ(resp.status().code(), MetaServiceCode::INVALID_ARGUMENT)
                << " status is " << resp.status().DebugString();
    }

    {
        brpc::Controller ctrl;
        GetVersionRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_db_id(1);
        req.set_table_id(table_id);
        req.set_partition_id(partition_id);

        GetVersionResponse resp;
        service->get_version(&ctrl, &req, &resp, nullptr);

        ASSERT_EQ(resp.status().code(), MetaServiceCode::VERSION_NOT_FOUND)
                << " status is " << resp.status().DebugString();
    }

    {
        brpc::Controller ctrl;
        GetVersionRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_db_id(1);
        req.set_table_id(table_id);
        req.set_partition_id(partition_id);
        req.set_is_table_version(true);

        GetVersionResponse resp;
        service->get_version(&ctrl, &req, &resp, nullptr);

        ASSERT_EQ(resp.status().code(), MetaServiceCode::VERSION_NOT_FOUND)
                << " status is " << resp.status().DebugString();
    }

    create_tablet(service.get(), table_id, 1, partition_id, tablet_id);
    insert_rowset(service.get(), 1, "get_version_label_1", table_id, partition_id, tablet_id);

    {
        brpc::Controller ctrl;
        GetVersionRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_db_id(1);
        req.set_table_id(table_id);
        req.set_partition_id(partition_id);

        GetVersionResponse resp;
        service->get_version(&ctrl, &req, &resp, nullptr);

        ASSERT_EQ(resp.status().code(), MetaServiceCode::OK)
                << " status is " << resp.status().DebugString();
        ASSERT_EQ(resp.version(), 2);
    }

    {
        brpc::Controller ctrl;
        GetVersionRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_db_id(1);
        req.set_table_id(table_id);
        req.set_partition_id(partition_id);
        req.set_is_table_version(true);

        GetVersionResponse resp;
        service->get_version(&ctrl, &req, &resp, nullptr);

        ASSERT_EQ(resp.status().code(), MetaServiceCode::OK)
                << " status is " << resp.status().DebugString();
        ASSERT_GT(resp.version(), 2);
    }
}

TEST(MetaServiceVersionedReadTest, BatchGetVersion) {
    struct TestCase {
        std::vector<int64_t> table_ids;
        std::vector<int64_t> partition_ids;
        std::vector<int64_t> expected_versions;
        std::vector<
                std::tuple<int64_t /*table_id*/, int64_t /*partition_id*/, int64_t /*tablet_id*/>>
                insert_rowsets;
    };

    // table ids: 2, 3, 4, 5
    // partition ids: 6, 7, 8, 9
    std::vector<TestCase> cases = {
            // all version are missing
            {{1, 2, 3, 4}, {6, 7, 8, 9}, {-1, -1, -1, -1}, {}},
            // update table 1, partition 6
            {{1, 2, 3, 4}, {6, 7, 8, 9}, {2, -1, -1, -1}, {{1, 6, 1}}},
            // update table 2, partition 6
            // update table 3, partition 7
            {{1, 2, 3, 4}, {6, 7, 8, 9}, {2, -1, 2, 2}, {{3, 8, 3}, {4, 9, 4}}},
            // update table 1, partition 7 twice
            {{1, 2, 3, 4}, {6, 7, 8, 9}, {2, 3, 2, 2}, {{2, 7, 2}, {2, 7, 2}}},
    };

    auto service = get_meta_service(false);

    std::string instance_id = "test_cloud_instance_id";
    std::string cloud_unique_id = fmt::format("1:{}:1", instance_id);

    MOCK_GET_INSTANCE_ID(instance_id);
    create_and_refresh_instance(service.get(), instance_id);

    create_tablet(service.get(), 1, 1, 6, 1);
    create_tablet(service.get(), 2, 1, 7, 2);
    create_tablet(service.get(), 3, 1, 8, 3);
    create_tablet(service.get(), 4, 1, 9, 4);

    size_t num_cases = cases.size();
    size_t label_index = 0;
    for (size_t i = 0; i < num_cases; ++i) {
        auto& [table_ids, partition_ids, expected_versions, insert_rowsets] = cases[i];
        for (auto [table_id, partition_id, tablet_id] : insert_rowsets) {
            LOG(INFO) << "insert rowset for table " << table_id << " partition " << partition_id
                      << " table_id " << tablet_id;
            insert_rowset(service.get(), 1, std::to_string(++label_index), table_id, partition_id,
                          tablet_id);
        }

        brpc::Controller ctrl;
        GetVersionRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_db_id(-1);
        req.set_table_id(-1);
        req.set_partition_id(-1);
        req.set_batch_mode(true);
        for (size_t i = 0; i < table_ids.size(); ++i) req.add_db_ids(1);
        std::copy(table_ids.begin(), table_ids.end(),
                  google::protobuf::RepeatedFieldBackInserter(req.mutable_table_ids()));
        std::copy(partition_ids.begin(), partition_ids.end(),
                  google::protobuf::RepeatedFieldBackInserter(req.mutable_partition_ids()));

        GetVersionResponse resp;
        service->get_version(&ctrl, &req, &resp, nullptr);

        ASSERT_EQ(resp.status().code(), MetaServiceCode::OK)
                << "case " << i << " status is " << resp.status().msg()
                << ", code=" << resp.status().code();

        std::vector<int64_t> versions(resp.versions().begin(), resp.versions().end());
        EXPECT_EQ(versions, expected_versions) << "case " << i;

        // Batch get table versions
        req.set_is_table_version(true);

        resp.Clear();
        service->get_version(&ctrl, &req, &resp, nullptr);
        ASSERT_EQ(resp.status().code(), MetaServiceCode::OK)
                << "case " << i << " status is " << resp.status().msg()
                << ", code=" << resp.status().code();
        for (size_t j = 0; j < resp.versions_size(); ++j) {
            if (expected_versions[j] == -1) {
                ASSERT_LT(resp.versions(j), 0)
                        << "case " << i << ", j=" << j << ", resp=" << resp.DebugString();
            } else {
                ASSERT_GT(resp.versions(j), 0)
                        << "case " << i << ", j=" << j << ", resp=" << resp.DebugString();
            }
        }
    }

    // INVALID_ARGUMENT
    {
        brpc::Controller ctrl;
        GetVersionRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_batch_mode(true);
        GetVersionResponse resp;
        service->get_version(&ctrl, &req, &resp, nullptr);
        ASSERT_EQ(resp.status().code(), MetaServiceCode::INVALID_ARGUMENT)
                << " status is " << resp.status().msg() << ", code=" << resp.status().code();
    }
}

TEST(MetaServiceVersionedReadTest, BatchGetVersionFallback) {
    constexpr size_t N = 100;
    size_t i = 0;
    auto sp = SyncPoint::get_instance();
    DORIS_CLOUD_DEFER {
        SyncPoint::get_instance()->clear_all_call_backs();
    };
    sp->set_call_back("batch_get_version_err", [&](auto&& args) {
        if (i++ == N / 10) {
            *try_any_cast<TxnErrorCode*>(args) = TxnErrorCode::TXN_TOO_OLD;
        }
    });

    sp->enable_processing();

    auto service = get_meta_service(false);
    std::string instance_id = "test_cloud_instance_id";
    std::string cloud_unique_id = fmt::format("1:{}:1", instance_id);

    MOCK_GET_INSTANCE_ID(instance_id);
    create_and_refresh_instance(service.get(), instance_id);

    for (int64_t i = 1; i <= N; ++i) {
        create_tablet(service.get(), 1, 1, i, i);
        insert_rowset(service.get(), 1, std::to_string(i), 1, i, i);
    }

    brpc::Controller ctrl;
    GetVersionRequest req;
    req.set_cloud_unique_id(cloud_unique_id);
    req.set_db_id(-1);
    req.set_table_id(-1);
    req.set_partition_id(-1);
    req.set_batch_mode(true);
    for (size_t i = 1; i <= N; ++i) {
        req.add_db_ids(1);
        req.add_table_ids(1);
        req.add_partition_ids(i);
    }

    GetVersionResponse resp;
    service->get_version(&ctrl, &req, &resp, nullptr);

    ASSERT_EQ(resp.status().code(), MetaServiceCode::OK)
            << "case " << i << " status is " << resp.status().msg()
            << ", code=" << resp.status().code();

    ASSERT_EQ(resp.versions_size(), N);
}

TEST(MetaServiceVersionedReadTest, GetTablet) {
    auto service = get_meta_service(false);
    std::string instance_id = "test_cloud_instance_id";

    MOCK_GET_INSTANCE_ID(instance_id);
    create_and_refresh_instance(service.get(), instance_id);

    int64_t table_id = 2;
    int64_t index_id = 3;
    int64_t partition_id = 4;
    int64_t tablet_id = 5;

    // Create tablet
    create_tablet(service.get(), table_id, index_id, partition_id, tablet_id);

    {
        // Get the tablet meta
        brpc::Controller cntl;
        GetTabletRequest req;
        req.set_cloud_unique_id(fmt::format("1:{}:1", instance_id));
        req.set_tablet_id(tablet_id);
        GetTabletResponse resp;
        service->get_tablet(&cntl, &req, &resp, nullptr);
        ASSERT_EQ(resp.status().code(), MetaServiceCode::OK)
                << " status is " << resp.status().DebugString();
        ASSERT_EQ(resp.tablet_meta().table_id(), table_id);
        ASSERT_EQ(resp.tablet_meta().index_id(), index_id);
        ASSERT_EQ(resp.tablet_meta().partition_id(), partition_id);
        ASSERT_EQ(resp.tablet_meta().tablet_id(), tablet_id);

        // Verify the tablet schema
        ASSERT_TRUE(resp.tablet_meta().has_schema());
    }
}

TEST(MetaServiceVersionedReadTest, GetRowsetMetas) {
    auto service = get_meta_service(false);
    std::string instance_id = "test_cloud_instance_id";

    MOCK_GET_INSTANCE_ID(instance_id);
    create_and_refresh_instance(service.get(), instance_id);

    int64_t db_id = 1;
    int64_t table_id = 2;
    int64_t index_id = 3;
    int64_t partition_id = 4;
    int64_t tablet_id = 5;

    // Create tablet
    create_tablet(service.get(), table_id, index_id, partition_id, tablet_id);

    // Helper function to get rowsets using MetaService get_rowset interface
    auto get_rowsets = [&](int64_t start_version, int64_t end_version) -> GetRowsetResponse {
        brpc::Controller cntl;
        GetRowsetRequest req;
        auto* tablet_idx = req.mutable_idx();
        tablet_idx->set_db_id(db_id);
        tablet_idx->set_table_id(table_id);
        tablet_idx->set_index_id(index_id);
        tablet_idx->set_partition_id(partition_id);
        tablet_idx->set_tablet_id(tablet_id);
        req.set_start_version(start_version);
        req.set_end_version(end_version);
        req.set_cumulative_compaction_cnt(0);
        req.set_base_compaction_cnt(0);
        req.set_cumulative_point(2);

        GetRowsetResponse resp;
        service->get_rowset(&cntl, &req, &resp, nullptr);
        return resp;
    };

    // Test 1: Contains the first rowset
    {
        auto resp = get_rowsets(1, 10);
        ASSERT_EQ(resp.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(resp.rowset_meta_size(), 1);
    }

    // Step 1: Insert loading rowsets with versions 2, 3, 4, 5, 6, 7, 8, 9, 10
    LOG(INFO) << "Inserting loading rowsets for versions 2-10";
    for (int64_t version = 2; version <= 10; ++version) {
        insert_rowset(service.get(), 1, fmt::format("load_label_{}", version), table_id,
                      partition_id, tablet_id);
    }

    // Test 2: Get all loading rowsets
    {
        auto resp = get_rowsets(1, 10);
        ASSERT_EQ(resp.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(resp.rowset_meta_size(), 10);

        // Verify rowsets are sorted by version
        for (int i = 0; i < resp.rowset_meta_size(); ++i) {
            const auto& meta = resp.rowset_meta(i);
            ASSERT_EQ(meta.end_version(), i + 1) << meta.DebugString();
        }
    }

    // Step 2: Insert compact rowsets [3-5] and [9-10]
    LOG(INFO) << "Inserting compact rowsets [3-5] and [9-10]";

    // Create compact rowset [3-4]
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        insert_compact_rowset(txn.get(), instance_id, tablet_id, partition_id, 3, 4, 300);
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Create compact rowset [3-5]
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        insert_compact_rowset(txn.get(), instance_id, tablet_id, partition_id, 3, 5, 300);
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Create compact rowset [9-10]
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        insert_compact_rowset(txn.get(), instance_id, tablet_id, partition_id, 9, 10, 190);
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Test 3: Verify compact rowsets override load rowsets
    {
        auto resp = get_rowsets(1, 10);
        ASSERT_EQ(resp.status().code(), MetaServiceCode::OK);

        // 1, 2, [3-5], 6, 7, 8, [9-10]
        ASSERT_EQ(resp.rowset_meta_size(), 7) << resp.ShortDebugString();

        // Expected sequence: v1, v2, [3-5], v6, v7, v8, [9-10]
        std::vector<std::pair<int64_t, int64_t>> expected_versions = {
                {0, 1}, {2, 2}, {3, 5}, {6, 6}, {7, 7}, {8, 8}, {9, 10}};

        for (int i = 0; i < resp.rowset_meta_size(); ++i) {
            const auto& meta = resp.rowset_meta(i);
            ASSERT_EQ(meta.start_version(), expected_versions[i].first)
                    << "Rowset " << i << " start_version mismatch";
            ASSERT_EQ(meta.end_version(), expected_versions[i].second)
                    << "Rowset " << i << " end_version mismatch";
        }
    }

    // Test 4: Boundary cases - query ranges that test boundaries
    struct TestCase {
        int64_t start_version;
        int64_t end_version;
        std::vector<std::pair<int64_t, int64_t>> expected_ranges;
        std::string description;
    };

    std::vector<TestCase> test_cases = {
            // Test exact version match
            {1, 1, {{0, 1}}, "Single version 1"},
            {2, 2, {{2, 2}}, "Single version 2"},

            // Test range before compact
            {1, 2, {{0, 1}, {2, 2}}, "Range [1-2] before compact"},

            // Test range ending at compact boundary
            {2, 5, {{2, 2}, {3, 5}}, "Range [2-5] ending at compact boundary"},

            // Test range starting at compact boundary
            {3, 6, {{3, 5}, {6, 6}}, "Range [3-6] starting at compact boundary"},

            // Test exact compact range
            {3, 5, {{3, 5}}, "Exact compact range [3-5]"},
            {9, 10, {{9, 10}}, "Exact compact range [9-10]"},

            // Test range spanning compact
            {2, 6, {{2, 2}, {3, 5}, {6, 6}}, "Range [2-6] spanning compact"},

            // Test range between compacts
            {6, 8, {{6, 6}, {7, 7}, {8, 8}}, "Range [6-8] between compacts"},

            // Test range crossing second compact
            {8, 10, {{8, 8}, {9, 10}}, "Range [8-10] crossing second compact"},

            // Test partial compact overlap
            {4, 6, {{3, 5}, {6, 6}}, "Range [4-6] partial compact overlap"},
            {9, 10, {{9, 10}}, "Range [9-10] contains last version"},

            // Test edge cases
            {5,
             10,
             {{3, 5}, {6, 6}, {7, 7}, {8, 8}, {9, 10}},
             "Range [5-10] crossing both compacts"},
            {5, 9, {{3, 5}, {6, 6}, {7, 7}, {8, 8}, {9, 9}}, "Range [5-9] includes first compact"},
            {3, 4, {{3, 4}}, "Range [3-4] within first compact"},
    };

    for (const auto& test_case : test_cases) {
        LOG(INFO) << "Testing: " << test_case.description;
        auto resp = get_rowsets(test_case.start_version, test_case.end_version);
        ASSERT_EQ(resp.status().code(), MetaServiceCode::OK)
                << "Failed for " << test_case.description;

        ASSERT_EQ(resp.rowset_meta_size(), test_case.expected_ranges.size())
                << "Rowset count mismatch for " << test_case.description << ". Got "
                << resp.rowset_meta_size() << " rowsets, expected "
                << test_case.expected_ranges.size();

        for (int i = 0; i < resp.rowset_meta_size(); ++i) {
            const auto& meta = resp.rowset_meta(i);
            const auto& expected = test_case.expected_ranges[i];
            ASSERT_EQ(meta.start_version(), expected.first)
                    << "Start version mismatch for " << test_case.description << " at index " << i;
            ASSERT_EQ(meta.end_version(), expected.second)
                    << "End version mismatch for " << test_case.description << " at index " << i
                    << " \nRowsetMeta: " << meta.ShortDebugString()
                    << " \nResponse: " << resp.ShortDebugString();
        }
    }

    // Test 5: Test empty ranges
    {
        auto resp = get_rowsets(11, 15);
        ASSERT_EQ(resp.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(resp.rowset_meta_size(), 0) << "Should return empty for non-existent versions";
    }

    LOG(INFO) << "GetRowsetMetas test completed successfully";
}

TEST(MetaServiceVersionedReadTest, UpdateTablet) {
    auto service = get_meta_service(false);
    std::string instance_id = "test_cloud_instance_id";
    std::string cloud_unique_id = fmt::format("1:{}:1", instance_id);

    MOCK_GET_INSTANCE_ID(instance_id);
    create_and_refresh_instance(service.get(), instance_id);

    constexpr auto table_id = 11231, index_id = 11232, partition_id = 11233, tablet_id1 = 11234,
                   tablet_id2 = 21234;
    ASSERT_NO_FATAL_FAILURE(
            create_tablet(service.get(), table_id, index_id, partition_id, tablet_id1));
    ASSERT_NO_FATAL_FAILURE(
            create_tablet(service.get(), table_id, index_id, partition_id, tablet_id2));
    auto get_and_check_tablet_meta = [&](int tablet_id, int64_t ttl_seconds, bool in_memory,
                                         bool is_persistent) {
        brpc::Controller cntl;
        GetTabletRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_tablet_id(tablet_id);
        GetTabletResponse resp;
        service->get_tablet(&cntl, &req, &resp, nullptr);
        ASSERT_EQ(resp.status().code(), MetaServiceCode::OK) << tablet_id;
        EXPECT_EQ(resp.tablet_meta().ttl_seconds(), ttl_seconds);
        EXPECT_EQ(resp.tablet_meta().is_in_memory(), in_memory);
        EXPECT_EQ(resp.tablet_meta().is_persistent(), is_persistent);
    };
    get_and_check_tablet_meta(tablet_id1, 0, false, false);
    get_and_check_tablet_meta(tablet_id2, 0, false, false);
    {
        brpc::Controller cntl;
        UpdateTabletRequest req;
        UpdateTabletResponse resp;
        req.set_cloud_unique_id(cloud_unique_id);
        TabletMetaInfoPB* tablet_meta_info = req.add_tablet_meta_infos();
        tablet_meta_info->set_tablet_id(tablet_id1);
        tablet_meta_info->set_ttl_seconds(300);
        tablet_meta_info = req.add_tablet_meta_infos();
        tablet_meta_info->set_tablet_id(tablet_id2);
        tablet_meta_info->set_ttl_seconds(3000);
        service->update_tablet(&cntl, &req, &resp, nullptr);
        ASSERT_EQ(resp.status().code(), MetaServiceCode::OK);
    }
    get_and_check_tablet_meta(tablet_id1, 300, false, false);
    get_and_check_tablet_meta(tablet_id2, 3000, false, false);
    {
        brpc::Controller cntl;
        UpdateTabletRequest req;
        UpdateTabletResponse resp;
        req.set_cloud_unique_id(cloud_unique_id);
        TabletMetaInfoPB* tablet_meta_info = req.add_tablet_meta_infos();
        tablet_meta_info->set_tablet_id(tablet_id1);
        tablet_meta_info->set_is_in_memory(true);
        service->update_tablet(&cntl, &req, &resp, nullptr);
        ASSERT_EQ(resp.status().code(), MetaServiceCode::OK);
    }
    {
        brpc::Controller cntl;
        UpdateTabletRequest req;
        UpdateTabletResponse resp;
        req.set_cloud_unique_id(cloud_unique_id);
        TabletMetaInfoPB* tablet_meta_info = req.add_tablet_meta_infos();
        tablet_meta_info->set_tablet_id(tablet_id1);
        tablet_meta_info->set_is_persistent(true);
        service->update_tablet(&cntl, &req, &resp, nullptr);
        ASSERT_EQ(resp.status().code(), MetaServiceCode::OK);
    }
    get_and_check_tablet_meta(tablet_id1, 300, true, true);
}

} // namespace doris::cloud
