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

#include "service/internal_service.h"

#include "common/config.h"
#include "runtime/tablet_writer_mgr.h"
#include "gen_cpp/BackendService.h"
#include "runtime/exec_env.h"
#include "runtime/data_stream_mgr.h"
#include "runtime/fragment_mgr.h"
#include "service/brpc.h"
#include "util/uid_util.h"
#include "util/thrift_util.h"
#include "runtime/buffer_control_block.h"
#include "runtime/result_buffer_mgr.h"
#include "runtime/routine_load/routine_load_task_executor.h"

namespace doris {

template<typename T>
PInternalServiceImpl<T>::PInternalServiceImpl(ExecEnv* exec_env)
        : _exec_env(exec_env),
        _tablet_worker_pool(config::number_tablet_writer_threads, 10240) {
}

template<typename T>
PInternalServiceImpl<T>::~PInternalServiceImpl() {
}

template<typename T>
void PInternalServiceImpl<T>::transmit_data(google::protobuf::RpcController* cntl_base,
                                         const PTransmitDataParams* request,
                                         PTransmitDataResult* response,
                                         google::protobuf::Closure* done) {
    VLOG_ROW << "transmit data: fragment_instance_id=" << print_id(request->finst_id())
            << " node=" << request->node_id();
    _exec_env->stream_mgr()->transmit_data(request, &done);
    if (done != nullptr) {
        done->Run();
    }
}

template<typename T>
void PInternalServiceImpl<T>::tablet_writer_open(google::protobuf::RpcController* controller,
                                              const PTabletWriterOpenRequest* request,
                                              PTabletWriterOpenResult* response,
                                              google::protobuf::Closure* done) {
    VLOG_RPC << "tablet writer open, id=" << request->id()
        << ", index_id=" << request->index_id() << ", txn_id=" << request->txn_id();
    brpc::ClosureGuard closure_guard(done);
    auto st = _exec_env->tablet_writer_mgr()->open(*request);
    if (!st.ok()) {
        LOG(WARNING) << "tablet writer open failed, message=" << st.get_error_msg()
            << ", id=" << request->id()
            << ", index_id=" << request->index_id()
            << ", txn_id=" << request->txn_id();
    }
    st.to_protobuf(response->mutable_status());
}

template<typename T>
void PInternalServiceImpl<T>::exec_plan_fragment(
        google::protobuf::RpcController* cntl_base,
        const PExecPlanFragmentRequest* request,
        PExecPlanFragmentResult* response,
        google::protobuf::Closure* done) {
    brpc::ClosureGuard closure_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
    auto st = _exec_plan_fragment(cntl);
    if (!st.ok()) {
        LOG(WARNING) << "exec plan fragment failed, errmsg=" << st.get_error_msg();
    }
    st.to_protobuf(response->mutable_status());
}

template<typename T>
void PInternalServiceImpl<T>::tablet_writer_add_batch(google::protobuf::RpcController* controller,
                                                   const PTabletWriterAddBatchRequest* request,
                                                   PTabletWriterAddBatchResult* response,
                                                   google::protobuf::Closure* done) {
    VLOG_RPC << "tablet writer add batch, id=" << request->id()
        << ", index_id=" << request->index_id()
        << ", sender_id=" << request->sender_id();
    // add batch maybe cost a lot of time, and this callback thread will be held.
    // this will influence query execute, because of no bthread. So, we put this to 
    // a local thread pool to process
    _tablet_worker_pool.offer(
        [request, response, done, this] () {
            brpc::ClosureGuard closure_guard(done);
            int64_t execution_time_ns = 0;
            int64_t wait_lock_time_ns = 0;
            { 
                SCOPED_RAW_TIMER(&execution_time_ns);
                auto st = _exec_env->tablet_writer_mgr()->add_batch(*request, response->mutable_tablet_vec(), &wait_lock_time_ns);
                if (!st.ok()) {
                    LOG(WARNING) << "tablet writer add batch failed, message=" << st.get_error_msg()
                        << ", id=" << request->id()
                        << ", index_id=" << request->index_id()
                        << ", sender_id=" << request->sender_id();
                }
                st.to_protobuf(response->mutable_status());
            }
            response->set_execution_time_us(execution_time_ns / 1000);
            response->set_wait_lock_time_us(wait_lock_time_ns / 1000);
        });
}

template<typename T>
void PInternalServiceImpl<T>::tablet_writer_cancel(google::protobuf::RpcController* controller,
                                                const PTabletWriterCancelRequest* request,
                                                PTabletWriterCancelResult* response,
                                                google::protobuf::Closure* done) {
    VLOG_RPC << "tablet writer cancel, id=" << request->id()
        << ", index_id=" << request->index_id()
        << ", sender_id=" << request->sender_id();
    brpc::ClosureGuard closure_guard(done);
    auto st = _exec_env->tablet_writer_mgr()->cancel(*request);
    if (!st.ok()) {
        LOG(WARNING) << "tablet writer cancel failed, id=" << request->id()
        << ", index_id=" << request->index_id()
        << ", sender_id=" << request->sender_id();
    }
}

template<typename T>
Status PInternalServiceImpl<T>::_exec_plan_fragment(brpc::Controller* cntl) {
    auto ser_request = cntl->request_attachment().to_string();
    TExecPlanFragmentParams t_request;
    {
        const uint8_t* buf = (const uint8_t*)ser_request.data();
        uint32_t len = ser_request.size();
        RETURN_IF_ERROR(deserialize_thrift_msg(buf, &len, false, &t_request));
    }
    LOG(INFO) << "exec plan fragment, fragment_instance_id=" << print_id(t_request.params.fragment_instance_id)
        << ", coord=" << t_request.coord << ", backend=" << t_request.backend_num;
    return _exec_env->fragment_mgr()->exec_plan_fragment(t_request);
}

template<typename T>
void PInternalServiceImpl<T>::cancel_plan_fragment(
        google::protobuf::RpcController* cntl_base,
        const PCancelPlanFragmentRequest* request,
        PCancelPlanFragmentResult* result,
        google::protobuf::Closure* done) {
    brpc::ClosureGuard closure_guard(done);
    TUniqueId tid;
    tid.__set_hi(request->finst_id().hi());
    tid.__set_lo(request->finst_id().lo());

    Status st;
    if (request->has_cancel_reason())  {
        LOG(INFO) << "cancel framgent, fragment_instance_id=" << print_id(tid) << ", reason: " << request->cancel_reason();
        st = _exec_env->fragment_mgr()->cancel(tid, request->cancel_reason());
    } else {
        LOG(INFO) << "cancel framgent, fragment_instance_id=" << print_id(tid);
        st = _exec_env->fragment_mgr()->cancel(tid);
    }
    if (!st.ok()) {
        LOG(WARNING) << "cancel plan fragment failed, errmsg=" << st.get_error_msg();
    }
    st.to_protobuf(result->mutable_status());
}

template<typename T>
void PInternalServiceImpl<T>::fetch_data(
        google::protobuf::RpcController* cntl_base,
        const PFetchDataRequest* request,
        PFetchDataResult* result,
        google::protobuf::Closure* done) {
    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
    GetResultBatchCtx* ctx = new GetResultBatchCtx(cntl, result, done);
    _exec_env->result_mgr()->fetch_data(request->finst_id(), ctx);
}

template<typename T>
void PInternalServiceImpl<T>::trigger_profile_report(
        google::protobuf::RpcController* controller,
        const PTriggerProfileReportRequest* request,
        PTriggerProfileReportResult* result,
        google::protobuf::Closure* done) {
    brpc::ClosureGuard closure_guard(done);
    auto st = _exec_env->fragment_mgr()->trigger_profile_report(request);
    st.to_protobuf(result->mutable_status());
}

template<typename T>
void PInternalServiceImpl<T>::get_info(
    google::protobuf::RpcController* controller,
    const PProxyRequest* request,
    PProxyResult* response,
    google::protobuf::Closure* done) {

    brpc::ClosureGuard closure_guard(done);
    if (request->has_kafka_meta_request()) {
        std::vector<int32_t> partition_ids;
        Status st = _exec_env->routine_load_task_executor()->get_kafka_partition_meta(request->kafka_meta_request(), &partition_ids);
        if (st.ok()) {
            PKafkaMetaProxyResult* kafka_result = response->mutable_kafka_meta_result();
            for (int32_t id : partition_ids) {
                kafka_result->add_partition_ids(id);
            } 
        }
        st.to_protobuf(response->mutable_status());
        return;
    }
    Status::OK().to_protobuf(response->mutable_status());
}


template class PInternalServiceImpl<PBackendService>;
template class PInternalServiceImpl<palo::PInternalService>;

}
