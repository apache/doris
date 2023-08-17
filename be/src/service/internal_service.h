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

#include <gen_cpp/internal_service.pb.h>
#include <stdint.h>

#include <string>

#include "common/status.h"
#include "util/work_thread_pool.hpp"

namespace google {
namespace protobuf {
class Closure;
class RpcController;
} // namespace protobuf
} // namespace google

namespace doris {

class ExecEnv;
class PHandShakeRequest;
class PHandShakeResponse;

class PInternalServiceImpl : public PBackendService {
public:
    PInternalServiceImpl(ExecEnv* exec_env);
    ~PInternalServiceImpl() override;

    void transmit_data(::google::protobuf::RpcController* controller,
                       const ::doris::PTransmitDataParams* request,
                       ::doris::PTransmitDataResult* response,
                       ::google::protobuf::Closure* done) override;

    void transmit_data_by_http(::google::protobuf::RpcController* controller,
                               const ::doris::PEmptyRequest* request,
                               ::doris::PTransmitDataResult* response,
                               ::google::protobuf::Closure* done) override;

    void exec_plan_fragment(google::protobuf::RpcController* controller,
                            const PExecPlanFragmentRequest* request,
                            PExecPlanFragmentResult* result,
                            google::protobuf::Closure* done) override;

    void exec_plan_fragment_prepare(google::protobuf::RpcController* controller,
                                    const PExecPlanFragmentRequest* request,
                                    PExecPlanFragmentResult* result,
                                    google::protobuf::Closure* done) override;

    void exec_plan_fragment_start(google::protobuf::RpcController* controller,
                                  const PExecPlanFragmentStartRequest* request,
                                  PExecPlanFragmentResult* result,
                                  google::protobuf::Closure* done) override;

    void cancel_plan_fragment(google::protobuf::RpcController* controller,
                              const PCancelPlanFragmentRequest* request,
                              PCancelPlanFragmentResult* result,
                              google::protobuf::Closure* done) override;

    void fetch_data(google::protobuf::RpcController* controller, const PFetchDataRequest* request,
                    PFetchDataResult* result, google::protobuf::Closure* done) override;

    void fetch_table_schema(google::protobuf::RpcController* controller,
                            const PFetchTableSchemaRequest* request,
                            PFetchTableSchemaResult* result,
                            google::protobuf::Closure* done) override;

    void tablet_writer_open(google::protobuf::RpcController* controller,
                            const PTabletWriterOpenRequest* request,
                            PTabletWriterOpenResult* response,
                            google::protobuf::Closure* done) override;

    void tablet_writer_add_block(google::protobuf::RpcController* controller,
                                 const PTabletWriterAddBlockRequest* request,
                                 PTabletWriterAddBlockResult* response,
                                 google::protobuf::Closure* done) override;

    void tablet_writer_add_block_by_http(google::protobuf::RpcController* controller,
                                         const ::doris::PEmptyRequest* request,
                                         PTabletWriterAddBlockResult* response,
                                         google::protobuf::Closure* done) override;

    void tablet_writer_cancel(google::protobuf::RpcController* controller,
                              const PTabletWriterCancelRequest* request,
                              PTabletWriterCancelResult* response,
                              google::protobuf::Closure* done) override;

    void get_info(google::protobuf::RpcController* controller, const PProxyRequest* request,
                  PProxyResult* response, google::protobuf::Closure* done) override;

    void update_cache(google::protobuf::RpcController* controller,
                      const PUpdateCacheRequest* request, PCacheResponse* response,
                      google::protobuf::Closure* done) override;

    void fetch_cache(google::protobuf::RpcController* controller, const PFetchCacheRequest* request,
                     PFetchCacheResult* result, google::protobuf::Closure* done) override;

    void clear_cache(google::protobuf::RpcController* controller, const PClearCacheRequest* request,
                     PCacheResponse* response, google::protobuf::Closure* done) override;

    void merge_filter(::google::protobuf::RpcController* controller,
                      const ::doris::PMergeFilterRequest* request,
                      ::doris::PMergeFilterResponse* response,
                      ::google::protobuf::Closure* done) override;

    void apply_filter(::google::protobuf::RpcController* controller,
                      const ::doris::PPublishFilterRequest* request,
                      ::doris::PPublishFilterResponse* response,
                      ::google::protobuf::Closure* done) override;
    void apply_filterv2(::google::protobuf::RpcController* controller,
                        const ::doris::PPublishFilterRequestV2* request,
                        ::doris::PPublishFilterResponse* response,
                        ::google::protobuf::Closure* done) override;
    void transmit_block(::google::protobuf::RpcController* controller,
                        const ::doris::PTransmitDataParams* request,
                        ::doris::PTransmitDataResult* response,
                        ::google::protobuf::Closure* done) override;
    void transmit_block_by_http(::google::protobuf::RpcController* controller,
                                const ::doris::PEmptyRequest* request,
                                ::doris::PTransmitDataResult* response,
                                ::google::protobuf::Closure* done) override;

    void send_data(google::protobuf::RpcController* controller, const PSendDataRequest* request,
                   PSendDataResult* response, google::protobuf::Closure* done) override;
    void commit(google::protobuf::RpcController* controller, const PCommitRequest* request,
                PCommitResult* response, google::protobuf::Closure* done) override;
    void rollback(google::protobuf::RpcController* controller, const PRollbackRequest* request,
                  PRollbackResult* response, google::protobuf::Closure* done) override;
    void fold_constant_expr(google::protobuf::RpcController* controller,
                            const PConstantExprRequest* request, PConstantExprResult* response,
                            google::protobuf::Closure* done) override;
    void check_rpc_channel(google::protobuf::RpcController* controller,
                           const PCheckRPCChannelRequest* request,
                           PCheckRPCChannelResponse* response,
                           google::protobuf::Closure* done) override;
    void reset_rpc_channel(google::protobuf::RpcController* controller,
                           const PResetRPCChannelRequest* request,
                           PResetRPCChannelResponse* response,
                           google::protobuf::Closure* done) override;
    void hand_shake(google::protobuf::RpcController* controller, const PHandShakeRequest* request,
                    PHandShakeResponse* response, google::protobuf::Closure* done) override;
    void request_slave_tablet_pull_rowset(google::protobuf::RpcController* controller,
                                          const PTabletWriteSlaveRequest* request,
                                          PTabletWriteSlaveResult* response,
                                          google::protobuf::Closure* done) override;
    void response_slave_tablet_pull_rowset(google::protobuf::RpcController* controller,
                                           const PTabletWriteSlaveDoneRequest* request,
                                           PTabletWriteSlaveDoneResult* response,
                                           google::protobuf::Closure* done) override;
    void multiget_data(google::protobuf::RpcController* controller, const PMultiGetRequest* request,
                       PMultiGetResponse* response, google::protobuf::Closure* done) override;

    void tablet_fetch_data(google::protobuf::RpcController* controller,
                           const PTabletKeyLookupRequest* request,
                           PTabletKeyLookupResponse* response,
                           google::protobuf::Closure* done) override;

    void get_column_ids_by_tablet_ids(google::protobuf::RpcController* controller,
                                      const PFetchColIdsRequest* request,
                                      PFetchColIdsResponse* response,
                                      google::protobuf::Closure* done) override;

    void get_tablet_rowset_versions(google::protobuf::RpcController* controller,
                                    const PGetTabletVersionsRequest* request,
                                    PGetTabletVersionsResponse* response,
                                    google::protobuf::Closure* done) override;

    void glob(google::protobuf::RpcController* controller, const PGlobRequest* request,
              PGlobResponse* response, google::protobuf::Closure* done) override;

    void fetch_remote_tablet_schema(google::protobuf::RpcController* controller,
                                    const PFetchRemoteSchemaRequest* request,
                                    PFetchRemoteSchemaResponse* response,
                                    google::protobuf::Closure* done) override;

private:
    void _exec_plan_fragment_in_pthread(google::protobuf::RpcController* controller,
                                        const PExecPlanFragmentRequest* request,
                                        PExecPlanFragmentResult* result,
                                        google::protobuf::Closure* done);

    Status _exec_plan_fragment_impl(const std::string& s_request, PFragmentRequestVersion version,
                                    bool compact);

    Status _fold_constant_expr(const std::string& ser_request, PConstantExprResult* response);

    Status _tablet_fetch_data(const PTabletKeyLookupRequest* request,
                              PTabletKeyLookupResponse* response);

    void _transmit_data(::google::protobuf::RpcController* controller,
                        const ::doris::PTransmitDataParams* request,
                        ::doris::PTransmitDataResult* response, ::google::protobuf::Closure* done,
                        const Status& extract_st);

    void _transmit_block(::google::protobuf::RpcController* controller,
                         const ::doris::PTransmitDataParams* request,
                         ::doris::PTransmitDataResult* response, ::google::protobuf::Closure* done,
                         const Status& extract_st);

    void _tablet_writer_add_block(google::protobuf::RpcController* controller,
                                  const PTabletWriterAddBlockRequest* request,
                                  PTabletWriterAddBlockResult* response,
                                  google::protobuf::Closure* done);

    void _response_pull_slave_rowset(const std::string& remote_host, int64_t brpc_port,
                                     int64_t txn_id, int64_t tablet_id, int64_t node_id,
                                     bool is_succeed);
    Status _multi_get(const PMultiGetRequest& request, PMultiGetResponse* response);

    void _get_column_ids_by_tablet_ids(google::protobuf::RpcController* controller,
                                       const PFetchColIdsRequest* request,
                                       PFetchColIdsResponse* response,
                                       google::protobuf::Closure* done);

private:
    ExecEnv* _exec_env;

    // every brpc service request should put into thread pool
    // the reason see issue #16634
    // define the interface for reading and writing data as heavy interface
    // otherwise as light interface
    FifoThreadPool _heavy_work_pool;
    FifoThreadPool _light_work_pool;
};

} // namespace doris
