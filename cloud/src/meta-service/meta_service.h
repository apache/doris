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

#pragma once

#include <brpc/controller.h>
#include <bthread/bthread.h>
#include <gen_cpp/cloud.pb.h>
#include <google/protobuf/message.h>
#include <google/protobuf/service.h>

#include <chrono>
#include <random>
#include <type_traits>

#include "common/config.h"
#include "cpp/sync_point.h"
#include "meta-service/txn_kv.h"
#include "meta-service/txn_lazy_committer.h"
#include "rate-limiter/rate_limiter.h"
#include "resource-manager/resource_manager.h"

namespace doris::cloud {

class Transaction;

constexpr std::string_view BUILT_IN_STORAGE_VAULT_NAME = "built_in_storage_vault";

class MetaServiceImpl : public cloud::MetaService {
public:
    MetaServiceImpl(std::shared_ptr<TxnKv> txn_kv, std::shared_ptr<ResourceManager> resource_mgr,
                    std::shared_ptr<RateLimiter> rate_controller);
    ~MetaServiceImpl() override;

    [[nodiscard]] const std::shared_ptr<TxnKv>& txn_kv() const { return txn_kv_; }
    [[nodiscard]] const std::shared_ptr<RateLimiter>& rate_limiter() const { return rate_limiter_; }
    [[nodiscard]] const std::shared_ptr<ResourceManager>& resource_mgr() const {
        return resource_mgr_;
    }

    void begin_txn(::google::protobuf::RpcController* controller, const BeginTxnRequest* request,
                   BeginTxnResponse* response, ::google::protobuf::Closure* done) override;

    void precommit_txn(::google::protobuf::RpcController* controller,
                       const PrecommitTxnRequest* request, PrecommitTxnResponse* response,
                       ::google::protobuf::Closure* done) override;

    void commit_txn(::google::protobuf::RpcController* controller, const CommitTxnRequest* request,
                    CommitTxnResponse* response, ::google::protobuf::Closure* done) override;

    void abort_txn(::google::protobuf::RpcController* controller, const AbortTxnRequest* request,
                   AbortTxnResponse* response, ::google::protobuf::Closure* done) override;

    // clang-format off
    void get_txn(::google::protobuf::RpcController* controller,
                 const GetTxnRequest* request,
                 GetTxnResponse* response,
                 ::google::protobuf::Closure* done) override;
    // clang-format on

    void get_current_max_txn_id(::google::protobuf::RpcController* controller,
                                const GetCurrentMaxTxnRequest* request,
                                GetCurrentMaxTxnResponse* response,
                                ::google::protobuf::Closure* done) override;

    void begin_sub_txn(::google::protobuf::RpcController* controller,
                       const BeginSubTxnRequest* request, BeginSubTxnResponse* response,
                       ::google::protobuf::Closure* done) override;

    void abort_sub_txn(::google::protobuf::RpcController* controller,
                       const AbortSubTxnRequest* request, AbortSubTxnResponse* response,
                       ::google::protobuf::Closure* done) override;

    void check_txn_conflict(::google::protobuf::RpcController* controller,
                            const CheckTxnConflictRequest* request,
                            CheckTxnConflictResponse* response,
                            ::google::protobuf::Closure* done) override;

    void abort_txn_with_coordinator(::google::protobuf::RpcController* controller,
                                    const AbortTxnWithCoordinatorRequest* request,
                                    AbortTxnWithCoordinatorResponse* response,
                                    ::google::protobuf::Closure* done) override;

    void clean_txn_label(::google::protobuf::RpcController* controller,
                         const CleanTxnLabelRequest* request, CleanTxnLabelResponse* response,
                         ::google::protobuf::Closure* done) override;

    void get_version(::google::protobuf::RpcController* controller,
                     const GetVersionRequest* request, GetVersionResponse* response,
                     ::google::protobuf::Closure* done) override;

    void batch_get_version(::google::protobuf::RpcController* controller,
                           const GetVersionRequest* request, GetVersionResponse* response,
                           ::google::protobuf::Closure* done);

    void create_tablets(::google::protobuf::RpcController* controller,
                        const CreateTabletsRequest* request, CreateTabletsResponse* response,
                        ::google::protobuf::Closure* done) override;

    void update_tablet(::google::protobuf::RpcController* controller,
                       const UpdateTabletRequest* request, UpdateTabletResponse* response,
                       ::google::protobuf::Closure* done) override;

    void update_tablet_schema(::google::protobuf::RpcController* controller,
                              const UpdateTabletSchemaRequest* request,
                              UpdateTabletSchemaResponse* response,
                              ::google::protobuf::Closure* done) override;

    void get_tablet(::google::protobuf::RpcController* controller, const GetTabletRequest* request,
                    GetTabletResponse* response, ::google::protobuf::Closure* done) override;

    void prepare_rowset(::google::protobuf::RpcController* controller,
                        const CreateRowsetRequest* request, CreateRowsetResponse* response,
                        ::google::protobuf::Closure* done) override;

    void commit_rowset(::google::protobuf::RpcController* controller,
                       const CreateRowsetRequest* request, CreateRowsetResponse* response,
                       ::google::protobuf::Closure* done) override;

    void update_tmp_rowset(::google::protobuf::RpcController* controller,
                           const CreateRowsetRequest* request, CreateRowsetResponse* response,
                           ::google::protobuf::Closure* done) override;

    void get_rowset(::google::protobuf::RpcController* controller, const GetRowsetRequest* request,
                    GetRowsetResponse* response, ::google::protobuf::Closure* done) override;

    void prepare_index(::google::protobuf::RpcController* controller, const IndexRequest* request,
                       IndexResponse* response, ::google::protobuf::Closure* done) override;

    void commit_index(::google::protobuf::RpcController* controller, const IndexRequest* request,
                      IndexResponse* response, ::google::protobuf::Closure* done) override;

    void drop_index(::google::protobuf::RpcController* controller, const IndexRequest* request,
                    IndexResponse* response, ::google::protobuf::Closure* done) override;

    void check_kv(::google::protobuf::RpcController* controller, const CheckKVRequest* request,
                  CheckKVResponse* response, ::google::protobuf::Closure* done) override;

    void prepare_partition(::google::protobuf::RpcController* controller,
                           const PartitionRequest* request, PartitionResponse* response,
                           ::google::protobuf::Closure* done) override;

    void commit_partition(::google::protobuf::RpcController* controller,
                          const PartitionRequest* request, PartitionResponse* response,
                          ::google::protobuf::Closure* done) override;

    void drop_partition(::google::protobuf::RpcController* controller,
                        const PartitionRequest* request, PartitionResponse* response,
                        ::google::protobuf::Closure* done) override;

    void get_tablet_stats(::google::protobuf::RpcController* controller,
                          const GetTabletStatsRequest* request, GetTabletStatsResponse* response,
                          ::google::protobuf::Closure* done) override;

    void start_tablet_job(::google::protobuf::RpcController* controller,
                          const StartTabletJobRequest* request, StartTabletJobResponse* response,
                          ::google::protobuf::Closure* done) override;

    void finish_tablet_job(::google::protobuf::RpcController* controller,
                           const FinishTabletJobRequest* request, FinishTabletJobResponse* response,
                           ::google::protobuf::Closure* done) override;

    void http(::google::protobuf::RpcController* controller, const MetaServiceHttpRequest* request,
              MetaServiceHttpResponse* response, ::google::protobuf::Closure* done) override;

    void get_obj_store_info(google::protobuf::RpcController* controller,
                            const GetObjStoreInfoRequest* request,
                            GetObjStoreInfoResponse* response,
                            ::google::protobuf::Closure* done) override;

    void alter_obj_store_info(google::protobuf::RpcController* controller,
                              const AlterObjStoreInfoRequest* request,
                              AlterObjStoreInfoResponse* response,
                              ::google::protobuf::Closure* done) override;

    void alter_storage_vault(google::protobuf::RpcController* controller,
                             const AlterObjStoreInfoRequest* request,
                             AlterObjStoreInfoResponse* response,
                             ::google::protobuf::Closure* done) override;

    void update_ak_sk(google::protobuf::RpcController* controller, const UpdateAkSkRequest* request,
                      UpdateAkSkResponse* response, ::google::protobuf::Closure* done) override;

    void create_instance(google::protobuf::RpcController* controller,
                         const CreateInstanceRequest* request, CreateInstanceResponse* response,
                         ::google::protobuf::Closure* done) override;

    void alter_instance(google::protobuf::RpcController* controller,
                        const AlterInstanceRequest* request, AlterInstanceResponse* response,
                        ::google::protobuf::Closure* done) override;

    void get_instance(google::protobuf::RpcController* controller,
                      const GetInstanceRequest* request, GetInstanceResponse* response,
                      ::google::protobuf::Closure* done) override;

    void alter_cluster(google::protobuf::RpcController* controller,
                       const AlterClusterRequest* request, AlterClusterResponse* response,
                       ::google::protobuf::Closure* done) override;

    void get_cluster(google::protobuf::RpcController* controller, const GetClusterRequest* request,
                     GetClusterResponse* response, ::google::protobuf::Closure* done) override;

    void create_stage(google::protobuf::RpcController* controller,
                      const CreateStageRequest* request, CreateStageResponse* response,
                      ::google::protobuf::Closure* done) override;

    void get_stage(google::protobuf::RpcController* controller, const GetStageRequest* request,
                   GetStageResponse* response, ::google::protobuf::Closure* done) override;

    void drop_stage(google::protobuf::RpcController* controller, const DropStageRequest* request,
                    DropStageResponse* response, ::google::protobuf::Closure* done) override;

    void get_iam(google::protobuf::RpcController* controller, const GetIamRequest* request,
                 GetIamResponse* response, ::google::protobuf::Closure* done) override;

    void alter_iam(google::protobuf::RpcController* controller, const AlterIamRequest* request,
                   AlterIamResponse* response, ::google::protobuf::Closure* done) override;

    void alter_ram_user(google::protobuf::RpcController* controller,
                        const AlterRamUserRequest* request, AlterRamUserResponse* response,
                        ::google::protobuf::Closure* done) override;

    void begin_copy(google::protobuf::RpcController* controller, const BeginCopyRequest* request,
                    BeginCopyResponse* response, ::google::protobuf::Closure* done) override;

    void finish_copy(google::protobuf::RpcController* controller, const FinishCopyRequest* request,
                     FinishCopyResponse* response, ::google::protobuf::Closure* done) override;

    void get_copy_job(google::protobuf::RpcController* controller, const GetCopyJobRequest* request,
                      GetCopyJobResponse* response, ::google::protobuf::Closure* done) override;

    void get_copy_files(google::protobuf::RpcController* controller,
                        const GetCopyFilesRequest* request, GetCopyFilesResponse* response,
                        ::google::protobuf::Closure* done) override;

    // filter files that are loading or loaded in the input files, return files that are not loaded
    void filter_copy_files(google::protobuf::RpcController* controller,
                           const FilterCopyFilesRequest* request, FilterCopyFilesResponse* response,
                           ::google::protobuf::Closure* done) override;

    void update_delete_bitmap(google::protobuf::RpcController* controller,
                              const UpdateDeleteBitmapRequest* request,
                              UpdateDeleteBitmapResponse* response,
                              ::google::protobuf::Closure* done) override;
    void get_delete_bitmap(google::protobuf::RpcController* controller,
                           const GetDeleteBitmapRequest* request, GetDeleteBitmapResponse* response,
                           ::google::protobuf::Closure* done) override;

    void get_delete_bitmap_update_lock(google::protobuf::RpcController* controller,
                                       const GetDeleteBitmapUpdateLockRequest* request,
                                       GetDeleteBitmapUpdateLockResponse* response,
                                       ::google::protobuf::Closure* done) override;

    // cloud control get cluster's status by this api
    void get_cluster_status(google::protobuf::RpcController* controller,
                            const GetClusterStatusRequest* request,
                            GetClusterStatusResponse* response,
                            ::google::protobuf::Closure* done) override;

    void get_rl_task_commit_attach(::google::protobuf::RpcController* controller,
                                   const GetRLTaskCommitAttachRequest* request,
                                   GetRLTaskCommitAttachResponse* response,
                                   ::google::protobuf::Closure* done) override;

    void reset_rl_progress(::google::protobuf::RpcController* controller,
                           const ResetRLProgressRequest* request, ResetRLProgressResponse* response,
                           ::google::protobuf::Closure* done) override;

    void get_txn_id(::google::protobuf::RpcController* controller, const GetTxnIdRequest* request,
                    GetTxnIdResponse* response, ::google::protobuf::Closure* done) override;

    // ATTN: If you add a new method, please also add the corresponding implementation in `MetaServiceProxy`.

    std::pair<MetaServiceCode, std::string> get_instance_info(const std::string& instance_id,
                                                              const std::string& cloud_unique_id,
                                                              InstanceInfoPB* instance);

private:
    std::pair<MetaServiceCode, std::string> alter_instance(
            const AlterInstanceRequest* request,
            std::function<std::pair<MetaServiceCode, std::string>(InstanceInfoPB*)> action);

    std::shared_ptr<TxnKv> txn_kv_;
    std::shared_ptr<ResourceManager> resource_mgr_;
    std::shared_ptr<RateLimiter> rate_limiter_;
    std::shared_ptr<TxnLazyCommitter> txn_lazy_committer_;
};

class MetaServiceProxy final : public MetaService {
public:
    MetaServiceProxy(std::unique_ptr<MetaServiceImpl> service) : impl_(std::move(service)) {}
    ~MetaServiceProxy() override = default;
    MetaServiceProxy(const MetaServiceProxy&) = delete;
    MetaServiceProxy& operator=(const MetaServiceProxy&) = delete;

    [[nodiscard]] const std::shared_ptr<TxnKv>& txn_kv() const { return impl_->txn_kv(); }
    [[nodiscard]] const std::shared_ptr<RateLimiter>& rate_limiter() const {
        return impl_->rate_limiter();
    }
    [[nodiscard]] const std::shared_ptr<ResourceManager>& resource_mgr() const {
        return impl_->resource_mgr();
    }

    void begin_txn(::google::protobuf::RpcController* controller, const BeginTxnRequest* request,
                   BeginTxnResponse* response, ::google::protobuf::Closure* done) override {
        call_impl(&cloud::MetaService::begin_txn, controller, request, response, done);
    }

    void precommit_txn(::google::protobuf::RpcController* controller,
                       const PrecommitTxnRequest* request, PrecommitTxnResponse* response,
                       ::google::protobuf::Closure* done) override {
        call_impl(&cloud::MetaService::precommit_txn, controller, request, response, done);
    }

    void commit_txn(::google::protobuf::RpcController* controller, const CommitTxnRequest* request,
                    CommitTxnResponse* response, ::google::protobuf::Closure* done) override {
        call_impl(&cloud::MetaService::commit_txn, controller, request, response, done);
    }

    void abort_txn(::google::protobuf::RpcController* controller, const AbortTxnRequest* request,
                   AbortTxnResponse* response, ::google::protobuf::Closure* done) override {
        call_impl(&cloud::MetaService::abort_txn, controller, request, response, done);
    }

    void get_txn(::google::protobuf::RpcController* controller, const GetTxnRequest* request,
                 GetTxnResponse* response, ::google::protobuf::Closure* done) override {
        call_impl(&cloud::MetaService::get_txn, controller, request, response, done);
    }

    void get_current_max_txn_id(::google::protobuf::RpcController* controller,
                                const GetCurrentMaxTxnRequest* request,
                                GetCurrentMaxTxnResponse* response,
                                ::google::protobuf::Closure* done) override {
        call_impl(&cloud::MetaService::get_current_max_txn_id, controller, request, response, done);
    }

    void begin_sub_txn(::google::protobuf::RpcController* controller,
                       const BeginSubTxnRequest* request, BeginSubTxnResponse* response,
                       ::google::protobuf::Closure* done) override {
        call_impl(&cloud::MetaService::begin_sub_txn, controller, request, response, done);
    }

    void abort_sub_txn(::google::protobuf::RpcController* controller,
                       const AbortSubTxnRequest* request, AbortSubTxnResponse* response,
                       ::google::protobuf::Closure* done) override {
        call_impl(&cloud::MetaService::abort_sub_txn, controller, request, response, done);
    }

    void check_txn_conflict(::google::protobuf::RpcController* controller,
                            const CheckTxnConflictRequest* request,
                            CheckTxnConflictResponse* response,
                            ::google::protobuf::Closure* done) override {
        call_impl(&cloud::MetaService::check_txn_conflict, controller, request, response, done);
    }

    void abort_txn_with_coordinator(::google::protobuf::RpcController* controller,
                                    const AbortTxnWithCoordinatorRequest* request,
                                    AbortTxnWithCoordinatorResponse* response,
                                    ::google::protobuf::Closure* done) override {
        call_impl(&cloud::MetaService::abort_txn_with_coordinator, controller, request, response,
                  done);
    }

    void clean_txn_label(::google::protobuf::RpcController* controller,
                         const CleanTxnLabelRequest* request, CleanTxnLabelResponse* response,
                         ::google::protobuf::Closure* done) override {
        call_impl(&cloud::MetaService::clean_txn_label, controller, request, response, done);
    }

    void get_version(::google::protobuf::RpcController* controller,
                     const GetVersionRequest* request, GetVersionResponse* response,
                     ::google::protobuf::Closure* done) override {
        call_impl(&cloud::MetaService::get_version, controller, request, response, done);
    }

    void create_tablets(::google::protobuf::RpcController* controller,
                        const CreateTabletsRequest* request, CreateTabletsResponse* response,
                        ::google::protobuf::Closure* done) override {
        call_impl(&cloud::MetaService::create_tablets, controller, request, response, done);
    }

    void update_tablet(::google::protobuf::RpcController* controller,
                       const UpdateTabletRequest* request, UpdateTabletResponse* response,
                       ::google::protobuf::Closure* done) override {
        call_impl(&cloud::MetaService::update_tablet, controller, request, response, done);
    }

    void update_tablet_schema(::google::protobuf::RpcController* controller,
                              const UpdateTabletSchemaRequest* request,
                              UpdateTabletSchemaResponse* response,
                              ::google::protobuf::Closure* done) override {
        call_impl(&cloud::MetaService::update_tablet_schema, controller, request, response, done);
    }

    void get_tablet(::google::protobuf::RpcController* controller, const GetTabletRequest* request,
                    GetTabletResponse* response, ::google::protobuf::Closure* done) override {
        call_impl(&cloud::MetaService::get_tablet, controller, request, response, done);
    }

    void prepare_rowset(::google::protobuf::RpcController* controller,
                        const CreateRowsetRequest* request, CreateRowsetResponse* response,
                        ::google::protobuf::Closure* done) override {
        call_impl(&cloud::MetaService::prepare_rowset, controller, request, response, done);
    }

    void commit_rowset(::google::protobuf::RpcController* controller,
                       const CreateRowsetRequest* request, CreateRowsetResponse* response,
                       ::google::protobuf::Closure* done) override {
        call_impl(&cloud::MetaService::commit_rowset, controller, request, response, done);
    }

    void update_tmp_rowset(::google::protobuf::RpcController* controller,
                           const CreateRowsetRequest* request, CreateRowsetResponse* response,
                           ::google::protobuf::Closure* done) override {
        call_impl(&cloud::MetaService::update_tmp_rowset, controller, request, response, done);
    }

    void get_rowset(::google::protobuf::RpcController* controller, const GetRowsetRequest* request,
                    GetRowsetResponse* response, ::google::protobuf::Closure* done) override {
        call_impl(&cloud::MetaService::get_rowset, controller, request, response, done);
    }

    void prepare_index(::google::protobuf::RpcController* controller, const IndexRequest* request,
                       IndexResponse* response, ::google::protobuf::Closure* done) override {
        call_impl(&cloud::MetaService::prepare_index, controller, request, response, done);
    }

    void commit_index(::google::protobuf::RpcController* controller, const IndexRequest* request,
                      IndexResponse* response, ::google::protobuf::Closure* done) override {
        call_impl(&cloud::MetaService::commit_index, controller, request, response, done);
    }

    void drop_index(::google::protobuf::RpcController* controller, const IndexRequest* request,
                    IndexResponse* response, ::google::protobuf::Closure* done) override {
        call_impl(&cloud::MetaService::drop_index, controller, request, response, done);
    }

    void check_kv(::google::protobuf::RpcController* controller, const CheckKVRequest* request,
                  CheckKVResponse* response, ::google::protobuf::Closure* done) override {
        call_impl(&cloud::MetaService::check_kv, controller, request, response, done);
    }

    void prepare_partition(::google::protobuf::RpcController* controller,
                           const PartitionRequest* request, PartitionResponse* response,
                           ::google::protobuf::Closure* done) override {
        call_impl(&cloud::MetaService::prepare_partition, controller, request, response, done);
    }

    void commit_partition(::google::protobuf::RpcController* controller,
                          const PartitionRequest* request, PartitionResponse* response,
                          ::google::protobuf::Closure* done) override {
        call_impl(&cloud::MetaService::commit_partition, controller, request, response, done);
    }

    void drop_partition(::google::protobuf::RpcController* controller,
                        const PartitionRequest* request, PartitionResponse* response,
                        ::google::protobuf::Closure* done) override {
        call_impl(&cloud::MetaService::drop_partition, controller, request, response, done);
    }

    void get_tablet_stats(::google::protobuf::RpcController* controller,
                          const GetTabletStatsRequest* request, GetTabletStatsResponse* response,
                          ::google::protobuf::Closure* done) override {
        call_impl(&cloud::MetaService::get_tablet_stats, controller, request, response, done);
    }

    void start_tablet_job(::google::protobuf::RpcController* controller,
                          const StartTabletJobRequest* request, StartTabletJobResponse* response,
                          ::google::protobuf::Closure* done) override {
        call_impl(&cloud::MetaService::start_tablet_job, controller, request, response, done);
    }

    void finish_tablet_job(::google::protobuf::RpcController* controller,
                           const FinishTabletJobRequest* request, FinishTabletJobResponse* response,
                           ::google::protobuf::Closure* done) override {
        call_impl(&cloud::MetaService::finish_tablet_job, controller, request, response, done);
    }

    void http(::google::protobuf::RpcController* controller, const MetaServiceHttpRequest* request,
              MetaServiceHttpResponse* response, ::google::protobuf::Closure* done) override {
        impl_->http(controller, request, response, done);
    }

    void get_obj_store_info(google::protobuf::RpcController* controller,
                            const GetObjStoreInfoRequest* request,
                            GetObjStoreInfoResponse* response,
                            ::google::protobuf::Closure* done) override {
        call_impl(&cloud::MetaService::get_obj_store_info, controller, request, response, done);
    }

    void alter_obj_store_info(google::protobuf::RpcController* controller,
                              const AlterObjStoreInfoRequest* request,
                              AlterObjStoreInfoResponse* response,
                              ::google::protobuf::Closure* done) override {
        call_impl(&cloud::MetaService::alter_obj_store_info, controller, request, response, done);
    }

    void alter_storage_vault(google::protobuf::RpcController* controller,
                             const AlterObjStoreInfoRequest* request,
                             AlterObjStoreInfoResponse* response,
                             ::google::protobuf::Closure* done) override {
        call_impl(&cloud::MetaService::alter_storage_vault, controller, request, response, done);
    }

    void update_ak_sk(google::protobuf::RpcController* controller, const UpdateAkSkRequest* request,
                      UpdateAkSkResponse* response, ::google::protobuf::Closure* done) override {
        call_impl(&cloud::MetaService::update_ak_sk, controller, request, response, done);
    }

    void create_instance(google::protobuf::RpcController* controller,
                         const CreateInstanceRequest* request, CreateInstanceResponse* response,
                         ::google::protobuf::Closure* done) override {
        call_impl(&cloud::MetaService::create_instance, controller, request, response, done);
    }

    void get_instance(google::protobuf::RpcController* controller,
                      const GetInstanceRequest* request, GetInstanceResponse* response,
                      ::google::protobuf::Closure* done) override {
        call_impl(&cloud::MetaService::get_instance, controller, request, response, done);
    }

    void alter_instance(google::protobuf::RpcController* controller,
                        const AlterInstanceRequest* request, AlterInstanceResponse* response,
                        ::google::protobuf::Closure* done) override {
        call_impl(&cloud::MetaService::alter_instance, controller, request, response, done);
    }

    void alter_cluster(google::protobuf::RpcController* controller,
                       const AlterClusterRequest* request, AlterClusterResponse* response,
                       ::google::protobuf::Closure* done) override {
        call_impl(&cloud::MetaService::alter_cluster, controller, request, response, done);
    }

    void get_cluster(google::protobuf::RpcController* controller, const GetClusterRequest* request,
                     GetClusterResponse* response, ::google::protobuf::Closure* done) override {
        call_impl(&cloud::MetaService::get_cluster, controller, request, response, done);
    }

    void create_stage(google::protobuf::RpcController* controller,
                      const CreateStageRequest* request, CreateStageResponse* response,
                      ::google::protobuf::Closure* done) override {
        call_impl(&cloud::MetaService::create_stage, controller, request, response, done);
    }

    void get_stage(google::protobuf::RpcController* controller, const GetStageRequest* request,
                   GetStageResponse* response, ::google::protobuf::Closure* done) override {
        call_impl(&cloud::MetaService::get_stage, controller, request, response, done);
    }

    void drop_stage(google::protobuf::RpcController* controller, const DropStageRequest* request,
                    DropStageResponse* response, ::google::protobuf::Closure* done) override {
        call_impl(&cloud::MetaService::drop_stage, controller, request, response, done);
    }

    void get_iam(google::protobuf::RpcController* controller, const GetIamRequest* request,
                 GetIamResponse* response, ::google::protobuf::Closure* done) override {
        call_impl(&cloud::MetaService::get_iam, controller, request, response, done);
    }

    void alter_iam(google::protobuf::RpcController* controller, const AlterIamRequest* request,
                   AlterIamResponse* response, ::google::protobuf::Closure* done) override {
        call_impl(&cloud::MetaService::alter_iam, controller, request, response, done);
    }

    void alter_ram_user(google::protobuf::RpcController* controller,
                        const AlterRamUserRequest* request, AlterRamUserResponse* response,
                        ::google::protobuf::Closure* done) override {
        call_impl(&cloud::MetaService::alter_ram_user, controller, request, response, done);
    }

    void begin_copy(google::protobuf::RpcController* controller, const BeginCopyRequest* request,
                    BeginCopyResponse* response, ::google::protobuf::Closure* done) override {
        call_impl(&cloud::MetaService::begin_copy, controller, request, response, done);
    }

    void finish_copy(google::protobuf::RpcController* controller, const FinishCopyRequest* request,
                     FinishCopyResponse* response, ::google::protobuf::Closure* done) override {
        call_impl(&cloud::MetaService::finish_copy, controller, request, response, done);
    }

    void get_copy_job(google::protobuf::RpcController* controller, const GetCopyJobRequest* request,
                      GetCopyJobResponse* response, ::google::protobuf::Closure* done) override {
        call_impl(&cloud::MetaService::get_copy_job, controller, request, response, done);
    }

    void get_copy_files(google::protobuf::RpcController* controller,
                        const GetCopyFilesRequest* request, GetCopyFilesResponse* response,
                        ::google::protobuf::Closure* done) override {
        call_impl(&cloud::MetaService::get_copy_files, controller, request, response, done);
    }

    // filter files that are loading or loaded in the input files, return files that are not loaded
    void filter_copy_files(google::protobuf::RpcController* controller,
                           const FilterCopyFilesRequest* request, FilterCopyFilesResponse* response,
                           ::google::protobuf::Closure* done) override {
        call_impl(&cloud::MetaService::filter_copy_files, controller, request, response, done);
    }

    void update_delete_bitmap(google::protobuf::RpcController* controller,
                              const UpdateDeleteBitmapRequest* request,
                              UpdateDeleteBitmapResponse* response,
                              ::google::protobuf::Closure* done) override {
        call_impl(&cloud::MetaService::update_delete_bitmap, controller, request, response, done);
    }

    void get_delete_bitmap(google::protobuf::RpcController* controller,
                           const GetDeleteBitmapRequest* request, GetDeleteBitmapResponse* response,
                           ::google::protobuf::Closure* done) override {
        call_impl(&cloud::MetaService::get_delete_bitmap, controller, request, response, done);
    }

    void get_delete_bitmap_update_lock(google::protobuf::RpcController* controller,
                                       const GetDeleteBitmapUpdateLockRequest* request,
                                       GetDeleteBitmapUpdateLockResponse* response,
                                       ::google::protobuf::Closure* done) override {
        call_impl(&cloud::MetaService::get_delete_bitmap_update_lock, controller, request, response,
                  done);
    }

    // cloud control get cluster's status by this api
    void get_cluster_status(google::protobuf::RpcController* controller,
                            const GetClusterStatusRequest* request,
                            GetClusterStatusResponse* response,
                            ::google::protobuf::Closure* done) override {
        call_impl(&cloud::MetaService::get_cluster_status, controller, request, response, done);
    }

    void get_rl_task_commit_attach(::google::protobuf::RpcController* controller,
                                   const GetRLTaskCommitAttachRequest* request,
                                   GetRLTaskCommitAttachResponse* response,
                                   ::google::protobuf::Closure* done) override {
        call_impl(&cloud::MetaService::get_rl_task_commit_attach, controller, request, response,
                  done);
    }

    void reset_rl_progress(::google::protobuf::RpcController* controller,
                           const ResetRLProgressRequest* request, ResetRLProgressResponse* response,
                           ::google::protobuf::Closure* done) override {
        call_impl(&cloud::MetaService::reset_rl_progress, controller, request, response, done);
    }

    void get_txn_id(::google::protobuf::RpcController* controller, const GetTxnIdRequest* request,
                    GetTxnIdResponse* response, ::google::protobuf::Closure* done) override {
        call_impl(&cloud::MetaService::get_txn_id, controller, request, response, done);
    }

private:
    template <typename Request, typename Response>
    using MetaServiceMethod = void (cloud::MetaService::*)(::google::protobuf::RpcController*,
                                                           const Request*, Response*,
                                                           ::google::protobuf::Closure*);

    template <typename Request, typename Response>
    void call_impl(MetaServiceMethod<Request, Response> method,
                   ::google::protobuf::RpcController* ctrl, const Request* req, Response* resp,
                   ::google::protobuf::Closure* done) {
        static_assert(std::is_base_of_v<::google::protobuf::Message, Request>);
        static_assert(std::is_base_of_v<::google::protobuf::Message, Response>);

        using namespace std::chrono;

        brpc::ClosureGuard done_guard(done);
        if (!config::enable_txn_store_retry) {
            (impl_.get()->*method)(ctrl, req, resp, brpc::DoNothing());
            if (DCHECK_IS_ON()) {
                MetaServiceCode code = resp->status().code();
                DCHECK_NE(code, MetaServiceCode::KV_TXN_STORE_GET_RETRYABLE)
                        << "KV_TXN_STORE_GET_RETRYABLE should not be sent back to client";
                DCHECK_NE(code, MetaServiceCode::KV_TXN_STORE_COMMIT_RETRYABLE)
                        << "KV_TXN_STORE_COMMIT_RETRYABLE should not be sent back to client";
                DCHECK_NE(code, MetaServiceCode::KV_TXN_STORE_CREATE_RETRYABLE)
                        << "KV_TXN_STORE_CREATE_RETRYABLE should not be sent back to client";
            }
            return;
        }

        TEST_SYNC_POINT("MetaServiceProxy::call_impl:1");

        int32_t retry_times = 0;
        uint64_t duration_ms = 0, retry_drift_ms = 0;
        while (true) {
            (impl_.get()->*method)(ctrl, req, resp, brpc::DoNothing());
            MetaServiceCode code = resp->status().code();
            if (code != MetaServiceCode::KV_TXN_STORE_GET_RETRYABLE &&
                code != MetaServiceCode::KV_TXN_STORE_COMMIT_RETRYABLE &&
                code != MetaServiceCode::KV_TXN_STORE_CREATE_RETRYABLE &&
                code != MetaServiceCode::KV_TXN_TOO_OLD &&
                (!config::enable_retry_txn_conflict || code != MetaServiceCode::KV_TXN_CONFLICT)) {
                return;
            }

            TEST_SYNC_POINT("MetaServiceProxy::call_impl:2");
            if (retry_times == 0) {
                // the first retry, add random drift.
                duration seed = duration_cast<nanoseconds>(steady_clock::now().time_since_epoch());
                std::default_random_engine rng(static_cast<uint64_t>(seed.count()));
                retry_drift_ms = std::uniform_int_distribution<uint64_t>(
                        0, config::txn_store_retry_base_intervals_ms)(rng);
            }

            if (retry_times >= config::txn_store_retry_times ||
                // Retrying KV_TXN_TOO_OLD is very expensive, so we only retry once.
                (retry_times > 1 && code == MetaServiceCode::KV_TXN_TOO_OLD)) {
                // For KV_TXN_CONFLICT, we should return KV_TXN_CONFLICT_RETRY_EXCEEDED_MAX_TIMES,
                // because BE will retries the KV_TXN_CONFLICT error.
                resp->mutable_status()->set_code(
                        code == MetaServiceCode::KV_TXN_STORE_COMMIT_RETRYABLE   ? KV_TXN_COMMIT_ERR
                        : code == MetaServiceCode::KV_TXN_STORE_GET_RETRYABLE    ? KV_TXN_GET_ERR
                        : code == MetaServiceCode::KV_TXN_STORE_CREATE_RETRYABLE ? KV_TXN_CREATE_ERR
                        : code == MetaServiceCode::KV_TXN_CONFLICT
                                ? KV_TXN_CONFLICT_RETRY_EXCEEDED_MAX_TIMES
                                : MetaServiceCode::KV_TXN_TOO_OLD);
                return;
            }

            // 1 2 4 8 ...
            duration_ms =
                    (1 << retry_times) * config::txn_store_retry_base_intervals_ms + retry_drift_ms;
            TEST_SYNC_POINT_CALLBACK("MetaServiceProxy::call_impl_duration_ms", &duration_ms);

            retry_times += 1;
            LOG(WARNING) << __PRETTY_FUNCTION__ << " sleep " << duration_ms
                         << " ms before next round, retry times left: "
                         << (config::txn_store_retry_times - retry_times)
                         << ", code: " << MetaServiceCode_Name(code)
                         << ", msg: " << resp->status().msg();
            bthread_usleep(duration_ms * 1000);
        }
    }

    std::unique_ptr<MetaServiceImpl> impl_;
};

} // namespace doris::cloud
