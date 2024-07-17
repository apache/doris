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
#include <brpc/server.h>
#include <foundationdb/fdb_c_options.g.h>
#include <foundationdb/fdb_c_types.h>
#include <gen_cpp/cloud.pb.h>
#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>

#include "common/bvars.h"
#include "common/config.h"
#include "common/logging.h"
#include "cpp/sync_point.h"
#include "meta-service/meta_service.h"
#include "meta-service/txn_kv.h"

using namespace doris;

static std::unique_ptr<cloud::MetaServiceProxy> create_meta_service() {
    std::shared_ptr<cloud::TxnKv> txn_kv = std::make_unique<cloud::FdbTxnKv>();
    [&]() { ASSERT_EQ(txn_kv->init(), 0); }();

    auto rate_limiter = std::make_shared<cloud::RateLimiter>();
    auto rc_mgr = std::make_shared<cloud::ResourceManager>(txn_kv);
    [&]() { ASSERT_EQ(rc_mgr->init(), 0); }();

    auto meta_service_impl = std::make_unique<cloud::MetaServiceImpl>(txn_kv, rc_mgr, rate_limiter);
    return std::make_unique<cloud::MetaServiceProxy>(std::move(meta_service_impl));
}

static std::unique_ptr<cloud::MetaServiceProxy> meta_service;

int main(int argc, char** argv) {
    const std::string conf_file = "doris_cloud.conf";
    if (!cloud::config::init(conf_file.c_str(), true)) {
        std::cerr << "failed to init config file, conf=" << conf_file << std::endl;
        return -1;
    }

    if (!cloud::init_glog("fdb_injection_test")) {
        std::cerr << "failed to init glog" << std::endl;
        return -1;
    }

    ::testing::InitGoogleTest(&argc, argv);

    cloud::config::enable_txn_store_retry = true;
    cloud::config::txn_store_retry_times = 100;
    cloud::config::txn_store_retry_base_intervals_ms = 1;
    cloud::config::fdb_cluster_file_path = "fdb.cluster";
    cloud::config::write_schema_kv = true;

    auto sp = SyncPoint::get_instance();
    sp->enable_processing();
    sp->set_call_back("encrypt_ak_sk:get_encryption_key", [](auto&& args) {
        auto* ret = try_any_cast<int*>(args[0]);
        *ret = 0;
        auto* key = try_any_cast<std::string*>(args[1]);
        *key = "test";
        auto* key_id = try_any_cast<int64_t*>(args[2]);
        *key_id = 1;
    });
    sp->set_call_back("decrypt_ak_sk:get_encryption_key", [](auto&& args) {
        auto* key = try_any_cast<std::string*>(args[0]);
        *key = "test";
        auto* ret = try_any_cast<int*>(args[1]);
        *ret = 0;
    });
    sp->set_call_back("MetaServiceProxy::call_impl_duration_ms",
                      [](auto&& args) { *try_any_cast<uint64_t*>(args[0]) = 0; });
    sp->set_call_back("put_schema_kv:schema_key_exists_return",
                      [](auto&& args) { *try_any_cast<bool*>(args.back()) = true; });

    meta_service = create_meta_service();

    int ret = RUN_ALL_TESTS();
    if (ret != 0) {
        std::cerr << "run first round of tests failed" << std::endl;
        return ret;
    }

    std::vector<std::string> sync_points {
            "transaction:init:create_transaction_err",
            "transaction:commit:get_err",
            "transaction:get:get_err",
            "transaction:get_range:get_err",
            "transaction:get_read_version:get_err",
            "range_get_iterator:init:get_keyvalue_array_err",
    };

    // See
    // 1. https://apple.github.io/foundationdb/api-error-codes.html#developer-guide-error-codes
    // 2. FDB source code: flow/include/flow/error_definitions.h
    std::vector<fdb_error_t> retryable_not_committed {
            // future version
            1009,
            // process_behind
            1037,
            // database locked
            1038,
            // commit_proxy_memory_limit_exceeded
            1042,
            // batch_transaction_throttled
            1051,
            // grv_proxy_memory_limit_exceeded
            1078,
            // tag_throttled
            1213,
            // proxy_tag_throttled
            1223,
    };

    for (auto err : retryable_not_committed) {
        if (!fdb_error_predicate(FDB_ERROR_PREDICATE_RETRYABLE_NOT_COMMITTED, err)) {
            LOG_WARNING("skip unknown err").tag("err", err).tag("msg", fdb_get_error(err));
            continue;
        }
        for (auto&& name : sync_points) {
            for (auto&& clear_name : sync_points) {
                sp->clear_call_back(clear_name);
            }

            auto count = std::make_shared<std::atomic<uint64_t>>(0);
            auto inject_at = std::make_shared<std::atomic<uint64_t>>(0);
            sp->set_call_back(name, [=](auto&& args) mutable {
                size_t n = count->fetch_add(1);
                if (n == *inject_at) {
                    *try_any_cast<fdb_error_t*>(args[0]) = err;
                }
            });
            sp->set_call_back("MetaServiceProxy::call_impl:1", [=](auto&&) {
                // For each RPC invoking, inject every fdb txn kv call.
                count->store(0);
                inject_at->store(0);
            });
            sp->set_call_back("MetaServiceProxy::call_impl:2", [=](auto&&) {
                count->store(0);
                inject_at->fetch_add(1);
            });

            ret = RUN_ALL_TESTS();
            if (ret != 0) {
                std::cerr << "run test failed, sync_point=" << name << ", err=" << err
                          << ", msg=" << fdb_get_error(err) << std::endl;
                return ret;
            }
        }
    }

    meta_service.reset();

    return 0;
}

namespace doris::cloud {

using Status = MetaServiceResponseStatus;

static std::string cloud_unique_id(const std::string& instance_id) {
    // degraded format
    return fmt::format("1:{}:unique_id", instance_id);
}

static int create_instance(MetaService* service, const std::string& instance_id) {
    CreateInstanceRequest req;
    CreateInstanceResponse resp;
    req.set_instance_id(instance_id);
    req.set_user_id("user_id");
    req.set_name(fmt::format("instance-{}", instance_id));
    req.set_sse_enabled(false);

    auto* obj_info = req.mutable_obj_info();
    obj_info->set_ak("access-key");
    obj_info->set_sk("secret-key");
    obj_info->set_bucket("cloud-test-bucket");
    obj_info->set_prefix("cloud-test");
    obj_info->set_endpoint("endpoint");
    obj_info->set_external_endpoint("endpoint");
    obj_info->set_region("region");
    obj_info->set_provider(ObjectStoreInfoPB_Provider_COS);

    brpc::Controller ctrl;
    service->create_instance(&ctrl, &req, &resp, nullptr);
    if (ctrl.Failed()) {
        LOG_ERROR("create instance")
                .tag("instance_id", instance_id)
                .tag("code", ctrl.ErrorCode())
                .tag("msg", ctrl.ErrorText());
        return -1;
    }

    auto code = resp.status().code();
    if (code != cloud::MetaServiceCode::OK && code != cloud::MetaServiceCode::ALREADY_EXISTED) {
        LOG_ERROR("create instance")
                .tag("instance_id", instance_id)
                .tag("code", code)
                .tag("msg", resp.status().msg());
        return -1;
    }

    return 0;
}

static int remove_instance(MetaService* service, const std::string& instance_id) {
    AlterInstanceRequest req;
    AlterInstanceResponse resp;
    req.set_instance_id(instance_id);
    req.set_op(AlterInstanceRequest_Operation_DROP);

    brpc::Controller ctrl;
    service->alter_instance(&ctrl, &req, &resp, nullptr);
    if (ctrl.Failed()) {
        LOG_ERROR("alter_instance")
                .tag("instance_id", instance_id)
                .tag("code", ctrl.ErrorCode())
                .tag("msg", ctrl.ErrorText());
        return -1;
    }

    auto code = resp.status().code();
    if (code != cloud::MetaServiceCode::OK) {
        LOG_ERROR("create instance")
                .tag("instance_id", instance_id)
                .tag("code", code)
                .tag("msg", resp.status().msg());
        return -1;
    }

    return 0;
}

static int add_cluster(MetaService* service, const std::string& instance_id) {
    bool retry = false;
    while (true) {
        brpc::Controller ctrl;
        cloud::AlterClusterRequest req;
        cloud::AlterClusterResponse resp;

        req.set_instance_id(instance_id);
        req.set_op(cloud::AlterClusterRequest_Operation::AlterClusterRequest_Operation_ADD_CLUSTER);
        auto* cluster = req.mutable_cluster();
        auto name = fmt::format("instance_{}_cluster", instance_id);
        cluster->set_cluster_id(name);
        cluster->set_cluster_name(name);
        cluster->set_type(cloud::ClusterPB_Type::ClusterPB_Type_SQL);
        cluster->set_desc("cluster description");
        auto* node = cluster->add_nodes();
        node->set_ip("0.0.0.0");
        node->set_node_type(cloud::NodeInfoPB_NodeType::NodeInfoPB_NodeType_FE_MASTER);
        node->set_cloud_unique_id(cloud_unique_id(instance_id));
        node->set_edit_log_port(123);
        node->set_heartbeat_port(456);
        node->set_name("default_node");

        service->alter_cluster(&ctrl, &req, &resp, nullptr);
        if (ctrl.Failed()) {
            LOG_ERROR("alter cluster")
                    .tag("instance_id", instance_id)
                    .tag("code", ctrl.ErrorCode())
                    .tag("msg", ctrl.ErrorText());
            return -1;
        }

        auto code = resp.status().code();
        if (code == cloud::MetaServiceCode::OK) {
            return 0;
        } else if (code == cloud::MetaServiceCode::ALREADY_EXISTED) {
            if (!retry) {
                retry = true;
                req.set_op(cloud::AlterClusterRequest_Operation::
                                   AlterClusterRequest_Operation_DROP_CLUSTER);
                ctrl.Reset();
                service->alter_cluster(&ctrl, &req, &resp, nullptr);
            }
            return 0;
        } else {
            LOG_ERROR("add default cluster")
                    .tag("instance_id", instance_id)
                    .tag("code", code)
                    .tag("msg", resp.status().msg());
            return -1;
        }
    }
}

static int drop_cluster(MetaService* service, const std::string& instance_id) {
    AlterClusterRequest req;
    AlterClusterResponse resp;
    req.set_instance_id(instance_id);
    req.set_op(AlterClusterRequest_Operation_DROP_CLUSTER);
    auto cluster = req.mutable_cluster();
    cluster->set_cluster_id(fmt::format("instance_{}_cluster", instance_id));

    brpc::Controller ctrl;
    service->alter_cluster(&ctrl, &req, &resp, nullptr);
    if (ctrl.Failed()) {
        LOG_ERROR("drop cluster")
                .tag("instance_id", instance_id)
                .tag("code", ctrl.ErrorCode())
                .tag("msg", ctrl.ErrorText());
        return -1;
    }
    if (resp.status().code() != cloud::MetaServiceCode::OK) {
        LOG_ERROR("drop cluster")
                .tag("instance_id", instance_id)
                .tag("code", resp.status().code())
                .tag("msg", resp.status().msg());
        return -1;
    }
    return 0;
}

static doris::TabletMetaCloudPB add_tablet(int64_t table_id, int64_t index_id, int64_t partition_id,
                                           int64_t tablet_id) {
    doris::TabletMetaCloudPB tablet;
    tablet.set_table_id(table_id);
    tablet.set_index_id(index_id);
    tablet.set_partition_id(partition_id);
    tablet.set_tablet_id(tablet_id);
    auto schema = tablet.mutable_schema();
    schema->set_schema_version(1);
    auto first_rowset = tablet.add_rs_metas();
    first_rowset->set_rowset_id(0); // required
    first_rowset->set_rowset_id_v2(std::to_string(1));
    first_rowset->set_start_version(0);
    first_rowset->set_end_version(1);
    first_rowset->mutable_tablet_schema()->CopyFrom(*schema);
    return tablet;
}

static int create_tablet(MetaService* meta_service, const std::string& instance_id,
                         int64_t table_id, int64_t index_id, int64_t partition_id,
                         int64_t tablet_id) {
    if (tablet_id < 0) {
        LOG_ERROR("invalid tablet id").tag("id", tablet_id);
        return -1;
    }

    brpc::Controller ctrl;
    cloud::CreateTabletsRequest req;
    cloud::CreateTabletsResponse resp;
    req.set_cloud_unique_id(cloud_unique_id(instance_id));
    req.add_tablet_metas()->CopyFrom(add_tablet(table_id, index_id, partition_id, tablet_id));
    meta_service->create_tablets(&ctrl, &req, &resp, nullptr);
    if (ctrl.Failed()) {
        LOG_ERROR("create_tablets")
                .tag("instance_id", instance_id)
                .tag("tablet_id", tablet_id)
                .tag("code", ctrl.ErrorCode())
                .tag("msg", ctrl.ErrorText());
        return -1;
    }
    if (resp.status().code() != cloud::MetaServiceCode::OK) {
        LOG_ERROR("create tablet")
                .tag("instance_id", instance_id)
                .tag("tablet_id", tablet_id)
                .tag("code", resp.status().code())
                .tag("msg", resp.status().msg());
        return -1;
    }
    return 0;
}

static Status begin_txn(MetaService* service, const std::string& instance_id, std::string label,
                        int64_t db_id, const std::vector<uint64_t>& tablet_ids, int64_t* txn_id) {
    brpc::Controller ctrl;
    cloud::BeginTxnRequest req;
    cloud::BeginTxnResponse resp;
    req.set_cloud_unique_id(cloud_unique_id(instance_id));
    auto* txn_info = req.mutable_txn_info();
    txn_info->set_label(label);
    txn_info->set_db_id(db_id);
    txn_info->mutable_table_ids()->Add(tablet_ids.begin(), tablet_ids.end());
    txn_info->set_timeout_ms(1000 * 60 * 60);

    service->begin_txn(&ctrl, &req, &resp, nullptr);
    if (ctrl.Failed()) {
        LOG_ERROR("begin_txn")
                .tag("instance_id", instance_id)
                .tag("table_ids", fmt::format("{}", fmt::join(tablet_ids, ",")))
                .tag("label", label)
                .tag("code", ctrl.ErrorCode())
                .tag("msg", ctrl.ErrorText());
        Status status;
        status.set_code(MetaServiceCode::UNDEFINED_ERR);
        status.set_msg(ctrl.ErrorText());
        return status;
    }

    *txn_id = resp.txn_id();
    return resp.status();
}

static Status commit_txn(MetaService* service, const std::string& instance_id, int64_t txn_id,
                         int64_t db_id) {
    brpc::Controller ctrl;
    cloud::CommitTxnRequest req;
    cloud::CommitTxnResponse resp;
    req.set_cloud_unique_id(cloud_unique_id(instance_id));
    req.set_db_id(db_id);
    req.set_txn_id(txn_id);

    service->commit_txn(&ctrl, &req, &resp, nullptr);
    if (ctrl.Failed()) {
        LOG_ERROR("commit_txn")
                .tag("instance_id", instance_id)
                .tag("txn_id", txn_id)
                .tag("code", ctrl.ErrorCode())
                .tag("msg", ctrl.ErrorText());
        Status status;
        status.set_code(MetaServiceCode::UNDEFINED_ERR);
        status.set_msg(ctrl.ErrorText());
        return status;
    }

    return resp.status();
}

static doris::RowsetMetaCloudPB create_rowset(int64_t tablet_id, std::string rowset_id,
                                              int64_t txn_id, int64_t index_id,
                                              int64_t partition_id, int64_t version = -1,
                                              int num_rows = 100) {
    doris::RowsetMetaCloudPB rowset;
    rowset.set_rowset_id(0); // required
    rowset.set_rowset_id_v2(rowset_id);
    rowset.set_index_id(index_id);
    rowset.set_partition_id(partition_id);
    rowset.set_tablet_id(tablet_id);
    rowset.set_txn_id(txn_id);
    if (version >= 0) {
        rowset.set_start_version(version);
        rowset.set_end_version(version);
    }
    rowset.set_num_segments(1);
    rowset.set_num_rows(num_rows);
    rowset.set_data_disk_size(num_rows * 100);
    rowset.set_txn_expiration(10000);
    rowset.set_schema_version(1);

    // auto* schema = rowset.mutable_tablet_schema();
    // schema->set_schema_version(1);
    // schema->set_disable_auto_compaction(true);

    auto* bound = rowset.add_segments_key_bounds();
    bound->set_min_key("min_key");
    bound->set_max_key("max_key");

    return rowset;
}

static Status prepare_rowset(MetaService* service, const std::string& instance_id, int64_t txn_id,
                             int64_t tablet_id, std::string rowset_id, int64_t index_id,
                             int64_t partition_id) {
    brpc::Controller ctrl;
    cloud::CreateRowsetRequest req;
    cloud::CreateRowsetResponse resp;
    req.set_cloud_unique_id(cloud_unique_id(instance_id));
    req.mutable_rowset_meta()->CopyFrom(
            create_rowset(tablet_id, rowset_id, txn_id, index_id, partition_id));

    service->prepare_rowset(&ctrl, &req, &resp, nullptr);
    if (ctrl.Failed()) {
        LOG_ERROR("prepare_rowset")
                .tag("instance_id", instance_id)
                .tag("txn_id", txn_id)
                .tag("tablet_id", tablet_id)
                .tag("rowset_id", rowset_id)
                .tag("code", ctrl.ErrorCode())
                .tag("msg", ctrl.ErrorText());
        Status status;
        status.set_code(MetaServiceCode::UNDEFINED_ERR);
        status.set_msg(ctrl.ErrorText());
        return status;
    }
    return resp.status();
}

static Status commit_rowset(MetaService* service, const std::string& instance_id, int64_t txn_id,
                            int64_t tablet_id, std::string rowset_id, int64_t index_id,
                            int64_t partition_id) {
    brpc::Controller ctrl;
    cloud::CreateRowsetRequest req;
    cloud::CreateRowsetResponse resp;
    req.set_cloud_unique_id(cloud_unique_id(instance_id));
    req.mutable_rowset_meta()->CopyFrom(
            create_rowset(tablet_id, rowset_id, txn_id, index_id, partition_id));
    LOG_INFO("send commit rowset request");

    service->commit_rowset(&ctrl, &req, &resp, nullptr);
    if (ctrl.Failed()) {
        LOG_ERROR("commit_rowset")
                .tag("instance_id", instance_id)
                .tag("txn_id", txn_id)
                .tag("tablet_id", tablet_id)
                .tag("rowset_id", rowset_id)
                .tag("code", ctrl.ErrorCode())
                .tag("msg", ctrl.ErrorText());

        Status status;
        status.set_code(MetaServiceCode::UNDEFINED_ERR);
        status.set_msg(ctrl.ErrorText());
        return status;
    }
    return resp.status();
}

static Status insert_rowset(MetaService* service, const std::string& instance_id, int64_t txn_id,
                            int64_t tablet_id, std::string rowset_id, int64_t index_id,
                            int64_t partition_id) {
    auto status = prepare_rowset(service, instance_id, txn_id, tablet_id, rowset_id, index_id,
                                 partition_id);
    if (status.code() != cloud::MetaServiceCode::OK) {
        LOG_ERROR("prepare_rowset")
                .tag("instance_id", instance_id)
                .tag("txn_id", txn_id)
                .tag("tablet_id", tablet_id)
                .tag("rowset_id", rowset_id)
                .tag("code", status.code())
                .tag("msg", status.msg());
        if (status.code() != cloud::MetaServiceCode::ALREADY_EXISTED) {
            return status;
        }
    }

    LOG_INFO("prepare rowset").tag("code", status.code()).tag("msg", status.msg());
    status = commit_rowset(service, instance_id, txn_id, tablet_id, rowset_id, index_id,
                           partition_id);
    if (status.code() != cloud::MetaServiceCode::OK) {
        LOG_ERROR("commit_rowset")
                .tag("instance_id", instance_id)
                .tag("txn_id", txn_id)
                .tag("tablet_id", tablet_id)
                .tag("rowset_id", rowset_id)
                .tag("code", status.code())
                .tag("msg", status.msg());
        return status;
    }

    return Status();
}

static Status copy_into(MetaService* service, const std::string& instance_id, std::string label,
                        int64_t db_id, int64_t index_id, int64_t partition_id,
                        const std::vector<uint64_t>& tablet_ids) {
    int64_t txn_id = 0;
    auto status = begin_txn(service, instance_id, label, db_id, tablet_ids, &txn_id);
    if (status.code() != cloud::MetaServiceCode::OK) {
        LOG_ERROR("begin_txn")
                .tag("instance_id", instance_id)
                .tag("label", label)
                .tag("code", status.code())
                .tag("msg", status.msg());
        return status;
    }

    for (uint64_t tablet_id : tablet_ids) {
        auto rowset_id = fmt::format("rowset_{}_{}", label, tablet_id);
        status = insert_rowset(service, instance_id, txn_id, tablet_id, rowset_id, index_id,
                               partition_id);
        if (status.code() != cloud::MetaServiceCode::OK) {
            return status;
        }
    }

    status = commit_txn(service, instance_id, txn_id, db_id);
    if (status.code() != cloud::MetaServiceCode::OK) {
        LOG_ERROR("commit_txn")
                .tag("instance_id", instance_id)
                .tag("label", label)
                .tag("code", status.code())
                .tag("msg", status.msg());
        return status;
    }

    return Status();
}

static Status get_read_version(MetaService* service, const std::string& instance_id,
                               int64_t* version) {
    brpc::Controller ctrl;
    cloud::GetCurrentMaxTxnRequest req;
    cloud::GetCurrentMaxTxnResponse resp;
    req.set_cloud_unique_id(cloud_unique_id(instance_id));
    service->get_current_max_txn_id(&ctrl, &req, &resp, nullptr);

    if (ctrl.Failed()) {
        LOG_ERROR("get read version")
                .tag("instance_id", instance_id)
                .tag("code", ctrl.ErrorCode())
                .tag("msg", ctrl.ErrorText());
        Status status;
        status.set_code(MetaServiceCode::UNDEFINED_ERR);
        status.set_msg(ctrl.ErrorText());
        return status;
    }

    auto status = resp.status();
    if (status.code() != cloud::MetaServiceCode::OK) {
        LOG_ERROR("get read version")
                .tag("instance_id", instance_id)
                .tag("code", status.code())
                .tag("msg", status.msg());
        return status;
    }

    *version = resp.current_max_txn_id();

    return Status();
}

static Status get_version(MetaService* service, const std::string& instance_id, int64_t db_id,
                          int64_t partition_id, int64_t table_id, int64_t* version) {
    brpc::Controller ctrl;
    cloud::GetVersionRequest req;
    cloud::GetVersionResponse resp;
    req.set_cloud_unique_id(cloud_unique_id(instance_id));
    req.set_db_id(db_id);
    req.set_partition_id(partition_id);
    req.set_table_id(table_id);

    service->get_version(&ctrl, &req, &resp, nullptr);
    if (ctrl.Failed()) {
        LOG_ERROR("get version")
                .tag("instance_id", instance_id)
                .tag("db_id", db_id)
                .tag("partition_id", partition_id)
                .tag("table_id", table_id)
                .tag("code", ctrl.ErrorCode())
                .tag("msg", ctrl.ErrorText());
        Status status;
        status.set_code(MetaServiceCode::UNDEFINED_ERR);
        status.set_msg(ctrl.ErrorText());
        return status;
    }

    auto status = resp.status();
    if (status.code() != cloud::MetaServiceCode::OK) {
        LOG_ERROR("get version")
                .tag("instance_id", instance_id)
                .tag("db_id", db_id)
                .tag("partition_id", partition_id)
                .tag("table_id", table_id)
                .tag("code", status.code())
                .tag("msg", status.msg());
        return status;
    }

    *version = resp.version();

    return Status();
}

static Status get_tablet_meta(MetaService* service, const std::string& instance_id,
                              int64_t tablet_id) {
    brpc::Controller ctrl;
    cloud::GetTabletRequest req;
    cloud::GetTabletResponse resp;
    req.set_cloud_unique_id(cloud_unique_id(instance_id));
    req.set_tablet_id(tablet_id);
    service->get_tablet(&ctrl, &req, &resp, nullptr);
    if (ctrl.Failed()) {
        LOG_ERROR("get_tablet")
                .tag("instance_id", instance_id)
                .tag("tablet_id", tablet_id)
                .tag("code", ctrl.ErrorCode())
                .tag("msg", ctrl.ErrorText());
        Status status;
        status.set_code(MetaServiceCode::UNDEFINED_ERR);
        status.set_msg(ctrl.ErrorText());
        return status;
    }

    auto status = resp.status();
    if (status.code() != cloud::MetaServiceCode::OK) {
        LOG_ERROR("get_tablet")
                .tag("instance_id", instance_id)
                .tag("tablet_id", tablet_id)
                .tag("code", status.code())
                .tag("msg", status.msg());
        return status;
    }

    return Status();
}

static Status get_tablet_stats(MetaService* service, const std::string& instance_id,
                               int64_t tablet_id, cloud::TabletStatsPB* stats) {
    brpc::Controller ctrl;
    cloud::GetTabletStatsRequest req;
    cloud::GetTabletStatsResponse resp;
    req.set_cloud_unique_id(cloud_unique_id(instance_id));
    auto* tablet_idx = req.mutable_tablet_idx()->Add();
    tablet_idx->set_tablet_id(tablet_id);
    service->get_tablet_stats(&ctrl, &req, &resp, nullptr);
    if (ctrl.Failed()) {
        LOG_ERROR("get tablet stats")
                .tag("instance_id", instance_id)
                .tag("tablet_id", tablet_id)
                .tag("code", ctrl.ErrorCode())
                .tag("msg", ctrl.ErrorText());
        Status status;
        status.set_code(MetaServiceCode::UNDEFINED_ERR);
        status.set_msg(ctrl.ErrorText());
        return status;
    }

    auto status = resp.status();
    if (status.code() != cloud::MetaServiceCode::OK) {
        LOG_ERROR("get_tablet_stats")
                .tag("instance_id", instance_id)
                .tag("tablet_id", tablet_id)
                .tag("code", status.code())
                .tag("msg", status.msg());
        return status;
    }

    stats->CopyFrom(resp.tablet_stats().at(0));

    return Status();
}

static Status get_rowset_meta(MetaService* service, const std::string& instance_id,
                              int64_t tablet_id, int64_t version,
                              const cloud::TabletStatsPB& stats) {
    brpc::Controller ctrl;
    cloud::GetRowsetRequest req;
    cloud::GetRowsetResponse resp;
    req.set_cloud_unique_id(cloud_unique_id(instance_id));
    req.mutable_idx()->set_tablet_id(tablet_id);
    req.set_start_version(0);
    req.set_end_version(version);
    req.set_base_compaction_cnt(stats.base_compaction_cnt());
    req.set_cumulative_compaction_cnt(stats.cumulative_compaction_cnt());
    req.set_cumulative_point(stats.cumulative_point());
    service->get_rowset(&ctrl, &req, &resp, nullptr);
    if (ctrl.Failed()) {
        LOG_ERROR("get_rowset")
                .tag("instance_id", instance_id)
                .tag("tablet_id", tablet_id)
                .tag("code", ctrl.ErrorCode())
                .tag("msg", ctrl.ErrorText());
        Status status;
        status.set_code(MetaServiceCode::UNDEFINED_ERR);
        status.set_msg(ctrl.ErrorText());
        return status;
    }

    auto status = resp.status();
    if (status.code() != cloud::MetaServiceCode::OK) {
        LOG_ERROR("get_rowset")
                .tag("instance_id", instance_id)
                .tag("tablet_id", tablet_id)
                .tag("code", status.code())
                .tag("msg", status.msg());
        return status;
    }

    return Status();
}

TEST(FdbInjectionTest, AllInOne) {
    auto now = std::chrono::high_resolution_clock::now();
    auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch());
    std::string instance_id = fmt::format("fdb_injection_test_{}", ns.count());
    int ret = create_instance(meta_service.get(), instance_id);
    ASSERT_EQ(ret, 0);

    ret = add_cluster(meta_service.get(), instance_id);
    ASSERT_EQ(ret, 0);

    int64_t db_id = 1;
    int64_t table_id = 1;
    int64_t index_id = 1;
    int64_t partition_id = 1;
    std::vector<uint64_t> tablet_ids;
    for (int64_t tablet_id = 1; tablet_id <= 10; ++tablet_id) {
        ret = create_tablet(meta_service.get(), instance_id, table_id, index_id, partition_id,
                            tablet_id);
        ASSERT_EQ(ret, 0) << tablet_id;
        tablet_ids.push_back(tablet_id);
    }

    // Run copy into.
    std::string label = fmt::format("{}-label", instance_id);
    auto status = copy_into(meta_service.get(), instance_id, label, db_id, index_id, partition_id,
                            tablet_ids);
    ASSERT_EQ(status.code(), MetaServiceCode::OK) << status.msg();

    // Get version
    int64_t version = 0;
    status = get_version(meta_service.get(), instance_id, db_id, partition_id, table_id, &version);
    ASSERT_EQ(status.code(), MetaServiceCode::OK) << status.msg();

    // Get tablet meta & stats
    TabletStatsPB stats;
    status = get_tablet_stats(meta_service.get(), instance_id, tablet_ids.back(), &stats);
    ASSERT_EQ(status.code(), MetaServiceCode::OK) << status.msg();

    status = get_tablet_meta(meta_service.get(), instance_id, tablet_ids.back());
    ASSERT_EQ(status.code(), MetaServiceCode::OK) << status.msg();

    // Get rowset metas.
    status = get_rowset_meta(meta_service.get(), instance_id, tablet_ids.back(), version, stats);
    ASSERT_EQ(status.code(), MetaServiceCode::OK) << status.msg();

    int64_t max_txn_id = 0;
    status = get_read_version(meta_service.get(), instance_id, &max_txn_id);
    ASSERT_EQ(status.code(), MetaServiceCode::OK) << status.msg();
    ASSERT_GE(max_txn_id, version);

    ret = drop_cluster(meta_service.get(), instance_id);
    ASSERT_EQ(ret, 0);

    ret = remove_instance(meta_service.get(), instance_id);
    ASSERT_EQ(ret, 0);
}

} // namespace doris::cloud
