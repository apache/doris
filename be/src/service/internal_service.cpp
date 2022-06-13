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
#include "runtime/fold_constant_executor.h"
#include "runtime/fragment_mgr.h"
#include "runtime/load_channel_mgr.h"
#include "runtime/result_buffer_mgr.h"
#include "runtime/routine_load/routine_load_task_executor.h"
#include "runtime/runtime_state.h"
#include "service/brpc.h"
#include "util/brpc_client_cache.h"
#include "util/md5.h"
#include "util/proto_util.h"
#include "util/string_util.h"
#include "util/thrift_util.h"
#include "util/uid_util.h"
#include "vec/runtime/vdata_stream_mgr.h"

namespace doris {

DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(add_batch_task_queue_size, MetricUnit::NOUNIT);

template <typename T>
class NewHttpClosure : public ::google::protobuf::Closure {
public:
    NewHttpClosure(T* request, google::protobuf::Closure* done) : _request(request), _done(done) {}
    ~NewHttpClosure() {}

    void Run() {
        if (_request != nullptr) {
            delete _request;
            _request = nullptr;
        }
        if (_done != nullptr) {
            _done->Run();
        }
        delete this;
    }

private:
    T* _request = nullptr;
    google::protobuf::Closure* _done = nullptr;
};

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
    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
    attachment_transfer_request_row_batch<PTransmitDataParams>(request, cntl);

    _transmit_data(cntl_base, request, response, done, Status::OK());
}

template <typename T>
void PInternalServiceImpl<T>::transmit_data_by_http(google::protobuf::RpcController* cntl_base,
                                                 const PEmptyRequest* request,
                                                 PTransmitDataResult* response,
                                                 google::protobuf::Closure* done) {
    PTransmitDataParams* request_raw = new PTransmitDataParams();
    google::protobuf::Closure* done_raw =
            new NewHttpClosure<PTransmitDataParams>(request_raw, done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
    Status st = attachment_extract_request_contain_tuple<PTransmitDataParams>(request_raw, cntl);
    _transmit_data(cntl_base, request_raw, response, done_raw, st);
}

template <typename T>
void PInternalServiceImpl<T>::_transmit_data(google::protobuf::RpcController* cntl_base,
                                          const PTransmitDataParams* request,
                                          PTransmitDataResult* response,
                                          google::protobuf::Closure* done,
                                          const Status& extract_st) {
    std::string query_id;
    if (request->has_query_id()) {
        query_id = print_id(request->query_id());
        TUniqueId finst_id;
        finst_id.__set_hi(request->finst_id().hi());
        finst_id.__set_lo(request->finst_id().lo());
    }
    VLOG_ROW << "transmit data: fragment_instance_id=" << print_id(request->finst_id())
             << " query_id=" << query_id << " node=" << request->node_id();
    // The response is accessed when done->Run is called in transmit_data(),
    // give response a default value to avoid null pointers in high concurrency.
    Status st;
    st.to_protobuf(response->mutable_status());
    if (extract_st.ok()) {
        st = _exec_env->stream_mgr()->transmit_data(request, &done);
        if (!st.ok()) {
            LOG(WARNING) << "transmit_data failed, message=" << st.get_error_msg()
                         << ", fragment_instance_id=" << print_id(request->finst_id())
                         << ", node=" << request->node_id();
        }
    } else {
        st = extract_st;
    }
    if (done != nullptr) {
        st.to_protobuf(response->mutable_status());
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
    auto st = Status::OK();
    bool compact = request->has_compact() ? request->compact() : false;
    PFragmentRequestVersion version =
            request->has_version() ? request->version() : PFragmentRequestVersion::VERSION_1;
    st = _exec_plan_fragment(request->request(), version, compact);
    if (!st.ok()) {
        LOG(WARNING) << "exec plan fragment failed, errmsg=" << st.get_error_msg();
    }
    st.to_protobuf(response->mutable_status());
}

template <typename T>
void PInternalServiceImpl<T>::exec_plan_fragment_prepare(google::protobuf::RpcController* cntl_base,
                                                      const PExecPlanFragmentRequest* request,
                                                      PExecPlanFragmentResult* response,
                                                      google::protobuf::Closure* done) {
    exec_plan_fragment(cntl_base, request, response, done);
}

template <typename T>
void PInternalServiceImpl<T>::exec_plan_fragment_start(google::protobuf::RpcController* controller,
                                                    const PExecPlanFragmentStartRequest* request,
                                                    PExecPlanFragmentResult* result,
                                                    google::protobuf::Closure* done) {
    brpc::ClosureGuard closure_guard(done);
    auto st = _exec_env->fragment_mgr()->start_query_execution(request);
    st.to_protobuf(result->mutable_status());
}

template <typename T>
void PInternalServiceImpl<T>::tablet_writer_add_batch(google::protobuf::RpcController* cntl_base,
                                                   const PTabletWriterAddBatchRequest* request,
                                                   PTabletWriterAddBatchResult* response,
                                                   google::protobuf::Closure* done) {
    _tablet_writer_add_batch(cntl_base, request, response, done);
}

template <typename T>
void PInternalServiceImpl<T>::tablet_writer_add_batch_by_http(
        google::protobuf::RpcController* cntl_base, const ::doris::PEmptyRequest* request,
        PTabletWriterAddBatchResult* response, google::protobuf::Closure* done) {
    PTabletWriterAddBatchRequest* request_raw = new PTabletWriterAddBatchRequest();
    google::protobuf::Closure* done_raw =
            new NewHttpClosure<PTabletWriterAddBatchRequest>(request_raw, done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
    Status st = attachment_extract_request_contain_tuple<PTabletWriterAddBatchRequest>(request_raw,
                                                                                       cntl);
    if (st.ok()) {
        _tablet_writer_add_batch(cntl_base, request_raw, response, done_raw);
    } else {
        st.to_protobuf(response->mutable_status());
    }
}

template <typename T>
void PInternalServiceImpl<T>::_tablet_writer_add_batch(google::protobuf::RpcController* cntl_base,
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
    _tablet_worker_pool.offer([cntl_base, request, response, done, submit_task_time_ns, this]() {
        int64_t wait_execution_time_ns = MonotonicNanos() - submit_task_time_ns;
        brpc::ClosureGuard closure_guard(done);
        int64_t execution_time_ns = 0;
        {
            SCOPED_RAW_TIMER(&execution_time_ns);
            // TODO(zxy) delete in 1.2 version
            brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
            attachment_transfer_request_row_batch<PTabletWriterAddBatchRequest>(request, cntl);

            auto st = _exec_env->load_channel_mgr()->add_batch(*request, response);
            if (!st.ok()) {
                LOG(WARNING) << "tablet writer add batch failed, message=" << st.get_error_msg()
                             << ", id=" << request->id() << ", index_id=" << request->index_id()
                             << ", sender_id=" << request->sender_id()
                             << ", backend id=" << request->backend_id();
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
Status PInternalServiceImpl<T>::_exec_plan_fragment(const std::string& ser_request,
                                                 PFragmentRequestVersion version, bool compact) {
    if (version == PFragmentRequestVersion::VERSION_1) {
        // VERSION_1 should be removed in v1.2
        TExecPlanFragmentParams t_request;
        {
            const uint8_t* buf = (const uint8_t*)ser_request.data();
            uint32_t len = ser_request.size();
            RETURN_IF_ERROR(deserialize_thrift_msg(buf, &len, compact, &t_request));
        }
        return _exec_env->fragment_mgr()->exec_plan_fragment(t_request);
    } else if (version == PFragmentRequestVersion::VERSION_2) {
        TExecPlanFragmentParamsList t_request;
        {
            const uint8_t* buf = (const uint8_t*)ser_request.data();
            uint32_t len = ser_request.size();
            RETURN_IF_ERROR(deserialize_thrift_msg(buf, &len, compact, &t_request));
        }

        for (const TExecPlanFragmentParams& params : t_request.paramsList) {
            RETURN_IF_ERROR(_exec_env->fragment_mgr()->exec_plan_fragment(params));
        }
        return Status::OK();
    } else {
        return Status::InternalError("invalid version");
    }
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
    GetResultBatchCtx* ctx = new GetResultBatchCtx(cntl, result, done);
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
        if (!kafka_request.partition_id_for_latest_offsets().empty()) {
            // get latest offsets for specified partition ids
            std::vector<PIntegerPair> partition_offsets;
            Status st = _exec_env->routine_load_task_executor()
                                ->get_kafka_latest_offsets_for_partitions(
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
        } else if (!kafka_request.offset_times().empty()) {
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
    VLOG_NOTICE << "rpc apply_filter recv";
    Status st = _exec_env->fragment_mgr()->apply_filter(request, attachment.to_string().data());
    if (!st.ok()) {
        LOG(WARNING) << "apply filter meet error: " << st.to_string();
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
                                   sizeof(row) + row->ByteSizeLong());
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
        pipe->cancel("rollback");
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
    if (!t_request.__isset.vec_exec || !t_request.vec_exec)
        return FoldConstantExecutor().fold_constant_expr(t_request, response);

    return FoldConstantExecutor().fold_constant_vexpr(t_request, response);
}

template <typename T>
void PInternalServiceImpl<T>::transmit_block(google::protobuf::RpcController* cntl_base,
                                          const PTransmitDataParams* request,
                                          PTransmitDataResult* response,
                                          google::protobuf::Closure* done) {
    // TODO(zxy) delete in 1.2 version
    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
    attachment_transfer_request_block<PTransmitDataParams>(request, cntl);

    _transmit_block(cntl_base, request, response, done, Status::OK());
}

template <typename T>
void PInternalServiceImpl<T>::transmit_block_by_http(google::protobuf::RpcController* cntl_base,
                                                  const PEmptyRequest* request,
                                                  PTransmitDataResult* response,
                                                  google::protobuf::Closure* done) {
    PTransmitDataParams* request_raw = new PTransmitDataParams();
    google::protobuf::Closure* done_raw =
            new NewHttpClosure<PTransmitDataParams>(request_raw, done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
    Status st = attachment_extract_request_contain_block<PTransmitDataParams>(request_raw, cntl);
    _transmit_block(cntl_base, request_raw, response, done_raw, st);
}

template <typename T>
void PInternalServiceImpl<T>::_transmit_block(google::protobuf::RpcController* cntl_base,
                                           const PTransmitDataParams* request,
                                           PTransmitDataResult* response,
                                           google::protobuf::Closure* done,
                                           const Status& extract_st) {
    std::string query_id;
    if (request->has_query_id()) {
        query_id = print_id(request->query_id());
        TUniqueId finst_id;
        finst_id.__set_hi(request->finst_id().hi());
        finst_id.__set_lo(request->finst_id().lo());
    }
    VLOG_ROW << "transmit block: fragment_instance_id=" << print_id(request->finst_id())
             << " query_id=" << query_id << " node=" << request->node_id();
    // The response is accessed when done->Run is called in transmit_block(),
    // give response a default value to avoid null pointers in high concurrency.
    Status st;
    st.to_protobuf(response->mutable_status());
    if (extract_st.ok()) {
        st = _exec_env->vstream_mgr()->transmit_block(request, &done);
        if (!st.ok()) {
            LOG(WARNING) << "transmit_block failed, message=" << st.get_error_msg()
                         << ", fragment_instance_id=" << print_id(request->finst_id())
                         << ", node=" << request->node_id();
        }
    } else {
        st = extract_st;
    }
    if (done != nullptr) {
        st.to_protobuf(response->mutable_status());
        done->Run();
    }
}

template <typename T>
void PInternalServiceImpl<T>::check_rpc_channel(google::protobuf::RpcController* controller,
                                                const PCheckRPCChannelRequest* request,
                                                PCheckRPCChannelResponse* response,
                                                google::protobuf::Closure* done) {
    brpc::ClosureGuard closure_guard(done);
    response->mutable_status()->set_status_code(0);
    if (request->data().size() != request->size()) {
        std::stringstream ss;
        ss << "data size not same, expected: " << request->size()
           << ", actrual: " << request->data().size();
        response->mutable_status()->add_error_msgs(ss.str());
        response->mutable_status()->set_status_code(1);

    } else {
        Md5Digest digest;
        digest.update(static_cast<const void*>(request->data().c_str()), request->data().size());
        digest.digest();
        if (!iequal(digest.hex(), request->md5())) {
            std::stringstream ss;
            ss << "md5 not same, expected: " << request->md5() << ", actrual: " << digest.hex();
            response->mutable_status()->add_error_msgs(ss.str());
            response->mutable_status()->set_status_code(1);
        }
    }
}

template <typename T>
void PInternalServiceImpl<T>::reset_rpc_channel(google::protobuf::RpcController* controller,
                                                const PResetRPCChannelRequest* request,
                                                PResetRPCChannelResponse* response,
                                                google::protobuf::Closure* done) {
    brpc::ClosureGuard closure_guard(done);
    response->mutable_status()->set_status_code(0);
    if (request->all()) {
        int size = ExecEnv::GetInstance()->brpc_internal_client_cache()->size();
        if (size > 0) {
            std::vector<std::string> endpoints;
            ExecEnv::GetInstance()->brpc_internal_client_cache()->get_all(&endpoints);
            ExecEnv::GetInstance()->brpc_internal_client_cache()->clear();
            *response->mutable_channels() = {endpoints.begin(), endpoints.end()};
        }
    } else {
        for (const std::string& endpoint : request->endpoints()) {
            if (!ExecEnv::GetInstance()->brpc_internal_client_cache()->exist(endpoint)) {
                response->mutable_status()->add_error_msgs(endpoint + ": not found.");
                continue;
            }

            if (ExecEnv::GetInstance()->brpc_internal_client_cache()->erase(endpoint)) {
                response->add_channels(endpoint);
            } else {
                response->mutable_status()->add_error_msgs(endpoint + ": reset failed.");
            }
        }
        if (request->endpoints_size() != response->channels_size()) {
            response->mutable_status()->set_status_code(1);
        }
    }
}

template <typename T>
void PInternalServiceImpl<T>::hand_shake(google::protobuf::RpcController* cntl_base,
                                         const PHandShakeRequest* request,
                                         PHandShakeResponse* response,
                                         google::protobuf::Closure* done) {
    brpc::ClosureGuard closure_guard(done);
    if (request->has_hello()) {
        response->set_hello(request->hello());
    }
    response->mutable_status()->set_status_code(0);
}

template class PInternalServiceImpl<PBackendService>;

} // namespace doris
