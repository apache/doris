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
#include "gen_cpp/BackendService.h"
#include "gen_cpp/internal_service.pb.h"
#include "runtime/buffer_control_block.h"
#include "runtime/data_stream_mgr.h"
#include "runtime/exec_env.h"
#include "runtime/fold_constant_mgr.h"
#include "runtime/fragment_mgr.h"
#include "runtime/load_channel_mgr.h"
#include "runtime/result_buffer_mgr.h"
#include "runtime/routine_load/routine_load_task_executor.h"
#include "runtime/runtime_state.h"
#include "service/brpc.h"
#include "util/thrift_util.h"
#include "util/uid_util.h"

namespace doris {

DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(add_batch_task_queue_size, MetricUnit::NOUNIT);

template <typename T>
PInternalServiceImpl<T>::PInternalServiceImpl(ExecEnv* exec_env)
        : _exec_env(exec_env), _tablet_worker_pool(config::number_tablet_writer_threads, 10240) {
    REGISTER_HOOK_METRIC(add_batch_task_queue_size,
                         [this]() { return _tablet_worker_pool.get_queue_size(); });
}

template <typename T>
PInternalServiceImpl<T>::~PInternalServiceImpl() {
    DEREGISTER_HOOK_METRIC(add_batch_task_queue_size);
}

template <typename T>
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

template <typename T>
void PInternalServiceImpl<T>::tablet_writer_open(google::protobuf::RpcController* controller,
                                                 const PTabletWriterOpenRequest* request,
                                                 PTabletWriterOpenResult* response,
                                                 google::protobuf::Closure* done) {
    VLOG_RPC << "tablet writer open, id=" << request->id() << ", index_id=" << request->index_id()
             << ", txn_id=" << request->txn_id();
    brpc::ClosureGuard closure_guard(done);
    auto st = _exec_env->load_channel_mgr()->open(*request);
    if (!st.ok()) {
        LOG(WARNING) << "load channel open failed, message=" << st.get_error_msg()
                     << ", id=" << request->id() << ", index_id=" << request->index_id()
                     << ", txn_id=" << request->txn_id();
    }
    st.to_protobuf(response->mutable_status());
}

template <typename T>
void PInternalServiceImpl<T>::exec_plan_fragment(google::protobuf::RpcController* cntl_base,
                                                 const PExecPlanFragmentRequest* request,
                                                 PExecPlanFragmentResult* response,
                                                 google::protobuf::Closure* done) {
    brpc::ClosureGuard closure_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
    auto st = Status::OK();
    if (request->has_request()) {
        st = _exec_plan_fragment(request->request());
    } else {
        // TODO(yangzhengguo) this is just for compatible with old version, this should be removed in the release 0.15
        st = _exec_plan_fragment(cntl->request_attachment().to_string());
    }
    if (!st.ok()) {
        LOG(WARNING) << "exec plan fragment failed, errmsg=" << st.get_error_msg();
    }
    st.to_protobuf(response->mutable_status());
}

template <typename T>
void PInternalServiceImpl<T>::tablet_writer_add_batch(google::protobuf::RpcController* controller,
                                                      const PTabletWriterAddBatchRequest* request,
                                                      PTabletWriterAddBatchResult* response,
                                                      google::protobuf::Closure* done) {
    VLOG_RPC << "tablet writer add batch, id=" << request->id()
             << ", index_id=" << request->index_id() << ", sender_id=" << request->sender_id()
             << ", current_queued_size=" << _tablet_worker_pool.get_queue_size();
    // add batch maybe cost a lot of time, and this callback thread will be held.
    // this will influence query execution, because the pthreads under bthread may be
    // exhausted, so we put this to a local thread pool to process
    int64_t submit_task_time_ns = MonotonicNanos();
    _tablet_worker_pool.offer([request, response, done, submit_task_time_ns, this]() {
        int64_t wait_execution_time_ns = MonotonicNanos() - submit_task_time_ns;
        brpc::ClosureGuard closure_guard(done);
        int64_t execution_time_ns = 0;
        {
            SCOPED_RAW_TIMER(&execution_time_ns);
            auto st = _exec_env->load_channel_mgr()->add_batch(*request,
                                                               response->mutable_tablet_vec());
            if (!st.ok()) {
                LOG(WARNING) << "tablet writer add batch failed, message=" << st.get_error_msg()
                             << ", id=" << request->id() << ", index_id=" << request->index_id()
                             << ", sender_id=" << request->sender_id();
            }
            st.to_protobuf(response->mutable_status());
        }
        response->set_execution_time_us(execution_time_ns / NANOS_PER_MICRO);
        response->set_wait_execution_time_us(wait_execution_time_ns / NANOS_PER_MICRO);
    });
}

template <typename T>
void PInternalServiceImpl<T>::tablet_writer_cancel(google::protobuf::RpcController* controller,
                                                   const PTabletWriterCancelRequest* request,
                                                   PTabletWriterCancelResult* response,
                                                   google::protobuf::Closure* done) {
    VLOG_RPC << "tablet writer cancel, id=" << request->id() << ", index_id=" << request->index_id()
             << ", sender_id=" << request->sender_id();
    brpc::ClosureGuard closure_guard(done);
    auto st = _exec_env->load_channel_mgr()->cancel(*request);
    if (!st.ok()) {
        LOG(WARNING) << "tablet writer cancel failed, id=" << request->id()
                     << ", index_id=" << request->index_id()
                     << ", sender_id=" << request->sender_id();
    }
}

template <typename T>
Status PInternalServiceImpl<T>::_exec_plan_fragment(const std::string& ser_request) {
    TExecPlanFragmentParams t_request;
    {
        const uint8_t* buf = (const uint8_t*)ser_request.data();
        uint32_t len = ser_request.size();
        RETURN_IF_ERROR(deserialize_thrift_msg(buf, &len, false, &t_request));
    }
    // LOG(INFO) << "exec plan fragment, fragment_instance_id=" << print_id(t_request.params.fragment_instance_id)
    //  << ", coord=" << t_request.coord << ", backend=" << t_request.backend_num;
    return _exec_env->fragment_mgr()->exec_plan_fragment(t_request);
}

template <typename T>
void PInternalServiceImpl<T>::cancel_plan_fragment(google::protobuf::RpcController* cntl_base,
                                                   const PCancelPlanFragmentRequest* request,
                                                   PCancelPlanFragmentResult* result,
                                                   google::protobuf::Closure* done) {
    brpc::ClosureGuard closure_guard(done);
    TUniqueId tid;
    tid.__set_hi(request->finst_id().hi());
    tid.__set_lo(request->finst_id().lo());

    Status st;
    if (request->has_cancel_reason()) {
        LOG(INFO) << "cancel fragment, fragment_instance_id=" << print_id(tid)
                  << ", reason: " << request->cancel_reason();
        st = _exec_env->fragment_mgr()->cancel(tid, request->cancel_reason());
    } else {
        LOG(INFO) << "cancel fragment, fragment_instance_id=" << print_id(tid);
        st = _exec_env->fragment_mgr()->cancel(tid);
    }
    if (!st.ok()) {
        LOG(WARNING) << "cancel plan fragment failed, errmsg=" << st.get_error_msg();
    }
    st.to_protobuf(result->mutable_status());
}

template <typename T>
void PInternalServiceImpl<T>::fetch_data(google::protobuf::RpcController* cntl_base,
                                         const PFetchDataRequest* request, PFetchDataResult* result,
                                         google::protobuf::Closure* done) {
    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
    bool resp_in_attachment =
            request->has_resp_in_attachment() ? request->resp_in_attachment() : true;
    GetResultBatchCtx* ctx = new GetResultBatchCtx(cntl, resp_in_attachment, result, done);
    _exec_env->result_mgr()->fetch_data(request->finst_id(), ctx);
}

template <typename T>
void PInternalServiceImpl<T>::get_info(google::protobuf::RpcController* controller,
                                       const PProxyRequest* request, PProxyResult* response,
                                       google::protobuf::Closure* done) {
    brpc::ClosureGuard closure_guard(done);
    // PProxyRequest is defined in gensrc/proto/internal_service.proto
    // Currently it supports 2 kinds of requests:
    // 1. get all kafka partition ids for given topic
    // 2. get all kafka partition offsets for given topic and timestamp.
    if (request->has_kafka_meta_request()) {
        const PKafkaMetaProxyRequest& kafka_request = request->kafka_meta_request();
        if (!kafka_request.offset_times().empty()) {
            // if offset_times() has elements, which means this request is to get offset by timestamp.
            std::vector<PIntegerPair> partition_offsets;
            Status st =
                    _exec_env->routine_load_task_executor()->get_kafka_partition_offsets_for_times(
                            request->kafka_meta_request(), &partition_offsets);
            if (st.ok()) {
                PKafkaPartitionOffsets* part_offsets = response->mutable_partition_offsets();
                for (const auto& entry : partition_offsets) {
                    PIntegerPair* res = part_offsets->add_offset_times();
                    res->set_key(entry.key());
                    res->set_val(entry.val());
                }
            }
            st.to_protobuf(response->mutable_status());
            return;
        } else {
            // get partition ids of topic
            std::vector<int32_t> partition_ids;
            Status st = _exec_env->routine_load_task_executor()->get_kafka_partition_meta(
                    request->kafka_meta_request(), &partition_ids);
            if (st.ok()) {
                PKafkaMetaProxyResult* kafka_result = response->mutable_kafka_meta_result();
                for (int32_t id : partition_ids) {
                    kafka_result->add_partition_ids(id);
                }
            }
            st.to_protobuf(response->mutable_status());
            return;
        }
    }
    Status::OK().to_protobuf(response->mutable_status());
}

template <typename T>
void PInternalServiceImpl<T>::update_cache(google::protobuf::RpcController* controller,
                                           const PUpdateCacheRequest* request,
                                           PCacheResponse* response,
                                           google::protobuf::Closure* done) {
    brpc::ClosureGuard closure_guard(done);
    _exec_env->result_cache()->update(request, response);
}

template <typename T>
void PInternalServiceImpl<T>::fetch_cache(google::protobuf::RpcController* controller,
                                          const PFetchCacheRequest* request,
                                          PFetchCacheResult* result,
                                          google::protobuf::Closure* done) {
    brpc::ClosureGuard closure_guard(done);
    _exec_env->result_cache()->fetch(request, result);
}

template <typename T>
void PInternalServiceImpl<T>::clear_cache(google::protobuf::RpcController* controller,
                                          const PClearCacheRequest* request,
                                          PCacheResponse* response,
                                          google::protobuf::Closure* done) {
    brpc::ClosureGuard closure_guard(done);
    _exec_env->result_cache()->clear(request, response);
}

template <typename T>
void PInternalServiceImpl<T>::merge_filter(::google::protobuf::RpcController* controller,
                                           const ::doris::PMergeFilterRequest* request,
                                           ::doris::PMergeFilterResponse* response,
                                           ::google::protobuf::Closure* done) {
    brpc::ClosureGuard closure_guard(done);
    auto buf = static_cast<brpc::Controller*>(controller)->request_attachment();
    Status st = _exec_env->fragment_mgr()->merge_filter(request, buf.to_string().data());
    if (!st.ok()) {
        LOG(WARNING) << "merge meet error" << st.to_string();
    }
    st.to_protobuf(response->mutable_status());
}

template <typename T>
void PInternalServiceImpl<T>::apply_filter(::google::protobuf::RpcController* controller,
                                           const ::doris::PPublishFilterRequest* request,
                                           ::doris::PPublishFilterResponse* response,
                                           ::google::protobuf::Closure* done) {
    brpc::ClosureGuard closure_guard(done);
    auto attachment = static_cast<brpc::Controller*>(controller)->request_attachment();
    UniqueId unique_id(request->query_id());
    // TODO: avoid copy attachment copy
    LOG(INFO) << "rpc apply_filter recv";
    Status st = _exec_env->fragment_mgr()->apply_filter(request, attachment.to_string().data());
    if (!st.ok()) {
        LOG(WARNING) << "apply filter meet error" << st.to_string();
    }
    st.to_protobuf(response->mutable_status());
}

template <typename T>
void PInternalServiceImpl<T>::send_data(google::protobuf::RpcController* controller,
                                        const PSendDataRequest* request, PSendDataResult* response,
                                        google::protobuf::Closure* done) {
    brpc::ClosureGuard closure_guard(done);
    TUniqueId fragment_instance_id;
    fragment_instance_id.hi = request->fragment_instance_id().hi();
    fragment_instance_id.lo = request->fragment_instance_id().lo();
    auto pipe = _exec_env->fragment_mgr()->get_pipe(fragment_instance_id);
    if (pipe == nullptr) {
        response->mutable_status()->set_status_code(1);
        response->mutable_status()->add_error_msgs("pipe is null");
    } else {
        for (int i = 0; i < request->data_size(); ++i) {
            PDataRow* row = new PDataRow();
            row->CopyFrom(request->data(i));
            pipe->append_and_flush(reinterpret_cast<char*>(&row), sizeof(row),
                                   sizeof(row) + row->ByteSize());
        }
        response->mutable_status()->set_status_code(0);
    }
}

template <typename T>
void PInternalServiceImpl<T>::commit(google::protobuf::RpcController* controller,
                                     const PCommitRequest* request, PCommitResult* response,
                                     google::protobuf::Closure* done) {
    brpc::ClosureGuard closure_guard(done);
    TUniqueId fragment_instance_id;
    fragment_instance_id.hi = request->fragment_instance_id().hi();
    fragment_instance_id.lo = request->fragment_instance_id().lo();
    auto pipe = _exec_env->fragment_mgr()->get_pipe(fragment_instance_id);
    if (pipe == nullptr) {
        response->mutable_status()->set_status_code(1);
        response->mutable_status()->add_error_msgs("pipe is null");
    } else {
        pipe->finish();
        response->mutable_status()->set_status_code(0);
    }
}

template <typename T>
void PInternalServiceImpl<T>::rollback(google::protobuf::RpcController* controller,
                                       const PRollbackRequest* request, PRollbackResult* response,
                                       google::protobuf::Closure* done) {
    brpc::ClosureGuard closure_guard(done);
    TUniqueId fragment_instance_id;
    fragment_instance_id.hi = request->fragment_instance_id().hi();
    fragment_instance_id.lo = request->fragment_instance_id().lo();
    auto pipe = _exec_env->fragment_mgr()->get_pipe(fragment_instance_id);
    if (pipe == nullptr) {
        response->mutable_status()->set_status_code(1);
        response->mutable_status()->add_error_msgs("pipe is null");
    } else {
        pipe->cancel();
        response->mutable_status()->set_status_code(0);
    }
}

template <typename T>
void PInternalServiceImpl<T>::fold_constant_expr(google::protobuf::RpcController* cntl_base,
                                                 const PConstantExprRequest* request,
                                                 PConstantExprResult* response,
                                                 google::protobuf::Closure* done) {
    brpc::ClosureGuard closure_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);

    Status st = Status::OK();
    if (request->has_request()) {
        st = _fold_constant_expr(request->request(), response);
    } else {
        // TODO(yangzhengguo) this is just for compatible with old version, this should be removed in the release 0.15
        st = _fold_constant_expr(cntl->request_attachment().to_string(), response);
    }
    if (!st.ok()) {
        LOG(WARNING) << "exec fold constant expr failed, errmsg=" << st.get_error_msg();
    }
    st.to_protobuf(response->mutable_status());
}

template <typename T>
Status PInternalServiceImpl<T>::_fold_constant_expr(const std::string& ser_request,
                                                    PConstantExprResult* response) {
    TFoldConstantParams t_request;
    {
        const uint8_t* buf = (const uint8_t*)ser_request.data();
        uint32_t len = ser_request.size();
        RETURN_IF_ERROR(deserialize_thrift_msg(buf, &len, false, &t_request));
    }
    FoldConstantMgr mgr(_exec_env);
    return mgr.fold_constant_expr(t_request, response);
}

template <typename T>
void PInternalServiceImpl<T>::transmit_block(google::protobuf::RpcController* cntl_base,
                                             const PTransmitDataParams* request,
                                             PTransmitDataResult* response,
                                             google::protobuf::Closure* done) {
    VLOG_ROW << "transmit data: fragment_instance_id=" << print_id(request->finst_id())
             << " node=" << request->node_id();
    if (done != nullptr) {
        done->Run();
    }
}

template class PInternalServiceImpl<PBackendService>;

} // namespace doris
