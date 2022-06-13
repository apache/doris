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

#include "common/status.h"
#include "gen_cpp/internal_service.pb.h"
#include "runtime/cache/result_cache.h"
#include "util/priority_thread_pool.hpp"

namespace brpc {
class Controller;
}

namespace doris {

class ExecEnv;

template <typename T>
class PInternalServiceImpl : public T {
public:
    PInternalServiceImpl(ExecEnv* exec_env);
    virtual ~PInternalServiceImpl();

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

    void tablet_writer_open(google::protobuf::RpcController* controller,
                            const PTabletWriterOpenRequest* request,
                            PTabletWriterOpenResult* response,
                            google::protobuf::Closure* done) override;

    void tablet_writer_add_batch(google::protobuf::RpcController* controller,
                                 const PTabletWriterAddBatchRequest* request,
                                 PTabletWriterAddBatchResult* response,
                                 google::protobuf::Closure* done) override;

    void tablet_writer_add_batch_by_http(google::protobuf::RpcController* controller,
                                         const ::doris::PEmptyRequest* request,
                                         PTabletWriterAddBatchResult* response,
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

private:
    Status _exec_plan_fragment(const std::string& s_request, PFragmentRequestVersion version,
                               bool compact);

    Status _fold_constant_expr(const std::string& ser_request, PConstantExprResult* response);

    void _transmit_data(::google::protobuf::RpcController* controller,
                        const ::doris::PTransmitDataParams* request,
                        ::doris::PTransmitDataResult* response, ::google::protobuf::Closure* done,
                        const Status& extract_st);

    void _transmit_block(::google::protobuf::RpcController* controller,
                         const ::doris::PTransmitDataParams* request,
                         ::doris::PTransmitDataResult* response, ::google::protobuf::Closure* done,
                         const Status& extract_st);

    void _tablet_writer_add_batch(google::protobuf::RpcController* controller,
                                  const PTabletWriterAddBatchRequest* request,
                                  PTabletWriterAddBatchResult* response,
                                  google::protobuf::Closure* done);

private:
    ExecEnv* _exec_env;
    PriorityThreadPool _tablet_worker_pool;
};

} // namespace doris
