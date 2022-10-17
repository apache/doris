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

#include <string>

#include "common/config.h"
#include "gen_cpp/BackendService.h"
#include "gen_cpp/internal_service.pb.h"
#include "http/http_client.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "runtime/buffer_control_block.h"
#include "runtime/data_stream_mgr.h"
#include "runtime/exec_env.h"
#include "runtime/fold_constant_executor.h"
#include "runtime/fragment_mgr.h"
#include "runtime/load_channel_mgr.h"
#include "runtime/memory/mem_tracker_task_pool.h"
#include "runtime/result_buffer_mgr.h"
#include "runtime/routine_load/routine_load_task_executor.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "service/brpc.h"
#include "util/brpc_client_cache.h"
#include "util/md5.h"
#include "util/proto_util.h"
#include "util/ref_count_closure.h"
#include "util/string_util.h"
#include "util/telemetry/brpc_carrier.h"
#include "util/telemetry/telemetry.h"
#include "util/thrift_util.h"
#include "util/uid_util.h"
#include "vec/runtime/vdata_stream_mgr.h"

namespace doris {

const uint32_t DOWNLOAD_FILE_MAX_RETRY = 3;

DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(add_batch_task_queue_size, MetricUnit::NOUNIT);

bthread_key_t btls_key;

static void thread_context_deleter(void* d) {
    delete static_cast<ThreadContext*>(d);
}

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

PInternalServiceImpl::PInternalServiceImpl(ExecEnv* exec_env)
        : _exec_env(exec_env),
          _tablet_worker_pool(config::number_tablet_writer_threads, 10240),
          _slave_replica_worker_pool(config::number_slave_replica_download_threads, 10240) {
    REGISTER_HOOK_METRIC(add_batch_task_queue_size,
                         [this]() { return _tablet_worker_pool.get_queue_size(); });
    CHECK_EQ(0, bthread_key_create(&btls_key, thread_context_deleter));
}

PInternalServiceImpl::~PInternalServiceImpl() {
    DEREGISTER_HOOK_METRIC(add_batch_task_queue_size);
    CHECK_EQ(0, bthread_key_delete(btls_key));
}

void PInternalServiceImpl::transmit_data(google::protobuf::RpcController* cntl_base,
                                         const PTransmitDataParams* request,
                                         PTransmitDataResult* response,
                                         google::protobuf::Closure* done) {
    // TODO(zxy) delete in 1.2 version
    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
    attachment_transfer_request_row_batch<PTransmitDataParams>(request, cntl);

    _transmit_data(cntl_base, request, response, done, Status::OK());
}

void PInternalServiceImpl::transmit_data_by_http(google::protobuf::RpcController* cntl_base,
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

void PInternalServiceImpl::_transmit_data(google::protobuf::RpcController* cntl_base,
                                          const PTransmitDataParams* request,
                                          PTransmitDataResult* response,
                                          google::protobuf::Closure* done,
                                          const Status& extract_st) {
    std::string query_id;
    TUniqueId finst_id;
    std::shared_ptr<MemTrackerLimiter> transmit_tracker;
    if (request->has_query_id()) {
        query_id = print_id(request->query_id());
        finst_id.__set_hi(request->finst_id().hi());
        finst_id.__set_lo(request->finst_id().lo());
        // In some cases, query mem tracker does not exist in BE when transmit block, will get null pointer.
        transmit_tracker = std::make_shared<MemTrackerLimiter>(
                -1, fmt::format("QueryTransmit#queryId={}", query_id),
                _exec_env->task_pool_mem_tracker_registry()->get_task_mem_tracker(query_id));
    } else {
        query_id = "unkown_transmit_data";
        transmit_tracker = std::make_shared<MemTrackerLimiter>(-1, "unkown_transmit_data");
    }
    SCOPED_ATTACH_TASK(transmit_tracker, ThreadContext::TaskType::QUERY, query_id, finst_id);
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

void PInternalServiceImpl::tablet_writer_open(google::protobuf::RpcController* controller,
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

void PInternalServiceImpl::exec_plan_fragment(google::protobuf::RpcController* cntl_base,
                                              const PExecPlanFragmentRequest* request,
                                              PExecPlanFragmentResult* response,
                                              google::protobuf::Closure* done) {
    auto span = telemetry::start_rpc_server_span("exec_plan_fragment", cntl_base);
    auto scope = OpentelemetryScope {span};
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

void PInternalServiceImpl::exec_plan_fragment_prepare(google::protobuf::RpcController* cntl_base,
                                                      const PExecPlanFragmentRequest* request,
                                                      PExecPlanFragmentResult* response,
                                                      google::protobuf::Closure* done) {
    exec_plan_fragment(cntl_base, request, response, done);
}

void PInternalServiceImpl::exec_plan_fragment_start(google::protobuf::RpcController* controller,
                                                    const PExecPlanFragmentStartRequest* request,
                                                    PExecPlanFragmentResult* result,
                                                    google::protobuf::Closure* done) {
    auto span = telemetry::start_rpc_server_span("exec_plan_fragment_start", controller);
    auto scope = OpentelemetryScope {span};
    brpc::ClosureGuard closure_guard(done);
    auto st = _exec_env->fragment_mgr()->start_query_execution(request);
    st.to_protobuf(result->mutable_status());
}

void PInternalServiceImpl::tablet_writer_add_block(google::protobuf::RpcController* cntl_base,
                                                   const PTabletWriterAddBlockRequest* request,
                                                   PTabletWriterAddBlockResult* response,
                                                   google::protobuf::Closure* done) {
    // TODO(zxy) delete in 1.2 version
    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
    attachment_transfer_request_block<PTabletWriterAddBlockRequest>(request, cntl);

    _tablet_writer_add_block(cntl_base, request, response, done);
}

void PInternalServiceImpl::tablet_writer_add_block_by_http(
        google::protobuf::RpcController* cntl_base, const ::doris::PEmptyRequest* request,
        PTabletWriterAddBlockResult* response, google::protobuf::Closure* done) {
    PTabletWriterAddBlockRequest* request_raw = new PTabletWriterAddBlockRequest();
    google::protobuf::Closure* done_raw =
            new NewHttpClosure<PTabletWriterAddBlockRequest>(request_raw, done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
    Status st = attachment_extract_request_contain_block<PTabletWriterAddBlockRequest>(request_raw,
                                                                                       cntl);
    if (st.ok()) {
        _tablet_writer_add_block(cntl_base, request_raw, response, done_raw);
    } else {
        st.to_protobuf(response->mutable_status());
    }
}

void PInternalServiceImpl::_tablet_writer_add_block(google::protobuf::RpcController* cntl_base,
                                                    const PTabletWriterAddBlockRequest* request,
                                                    PTabletWriterAddBlockResult* response,
                                                    google::protobuf::Closure* done) {
    VLOG_RPC << "tablet writer add block, id=" << request->id()
             << ", index_id=" << request->index_id() << ", sender_id=" << request->sender_id()
             << ", current_queued_size=" << _tablet_worker_pool.get_queue_size();
    int64_t submit_task_time_ns = MonotonicNanos();
    _tablet_worker_pool.offer([request, response, done, submit_task_time_ns, this]() {
        int64_t wait_execution_time_ns = MonotonicNanos() - submit_task_time_ns;
        brpc::ClosureGuard closure_guard(done);
        int64_t execution_time_ns = 0;
        {
            SCOPED_RAW_TIMER(&execution_time_ns);

            auto st = _exec_env->load_channel_mgr()->add_batch(*request, response);
            if (!st.ok()) {
                LOG(WARNING) << "tablet writer add block failed, message=" << st.get_error_msg()
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

void PInternalServiceImpl::tablet_writer_add_batch(google::protobuf::RpcController* cntl_base,
                                                   const PTabletWriterAddBatchRequest* request,
                                                   PTabletWriterAddBatchResult* response,
                                                   google::protobuf::Closure* done) {
    _tablet_writer_add_batch(cntl_base, request, response, done);
}

void PInternalServiceImpl::tablet_writer_add_batch_by_http(
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

void PInternalServiceImpl::_tablet_writer_add_batch(google::protobuf::RpcController* cntl_base,
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

void PInternalServiceImpl::tablet_writer_cancel(google::protobuf::RpcController* controller,
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

Status PInternalServiceImpl::_exec_plan_fragment(const std::string& ser_request,
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

void PInternalServiceImpl::cancel_plan_fragment(google::protobuf::RpcController* cntl_base,
                                                const PCancelPlanFragmentRequest* request,
                                                PCancelPlanFragmentResult* result,
                                                google::protobuf::Closure* done) {
    auto span = telemetry::start_rpc_server_span("exec_plan_fragment_start", cntl_base);
    auto scope = OpentelemetryScope {span};
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

void PInternalServiceImpl::fetch_data(google::protobuf::RpcController* cntl_base,
                                      const PFetchDataRequest* request, PFetchDataResult* result,
                                      google::protobuf::Closure* done) {
    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
    GetResultBatchCtx* ctx = new GetResultBatchCtx(cntl, result, done);
    _exec_env->result_mgr()->fetch_data(request->finst_id(), ctx);
}

void PInternalServiceImpl::get_info(google::protobuf::RpcController* controller,
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

void PInternalServiceImpl::update_cache(google::protobuf::RpcController* controller,
                                        const PUpdateCacheRequest* request,
                                        PCacheResponse* response, google::protobuf::Closure* done) {
    brpc::ClosureGuard closure_guard(done);
    _exec_env->result_cache()->update(request, response);
}

void PInternalServiceImpl::fetch_cache(google::protobuf::RpcController* controller,
                                       const PFetchCacheRequest* request, PFetchCacheResult* result,
                                       google::protobuf::Closure* done) {
    brpc::ClosureGuard closure_guard(done);
    _exec_env->result_cache()->fetch(request, result);
}

void PInternalServiceImpl::clear_cache(google::protobuf::RpcController* controller,
                                       const PClearCacheRequest* request, PCacheResponse* response,
                                       google::protobuf::Closure* done) {
    brpc::ClosureGuard closure_guard(done);
    _exec_env->result_cache()->clear(request, response);
}

void PInternalServiceImpl::merge_filter(::google::protobuf::RpcController* controller,
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

void PInternalServiceImpl::apply_filter(::google::protobuf::RpcController* controller,
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

void PInternalServiceImpl::send_data(google::protobuf::RpcController* controller,
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

void PInternalServiceImpl::commit(google::protobuf::RpcController* controller,
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

void PInternalServiceImpl::rollback(google::protobuf::RpcController* controller,
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

void PInternalServiceImpl::fold_constant_expr(google::protobuf::RpcController* cntl_base,
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

Status PInternalServiceImpl::_fold_constant_expr(const std::string& ser_request,
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

void PInternalServiceImpl::transmit_block(google::protobuf::RpcController* cntl_base,
                                          const PTransmitDataParams* request,
                                          PTransmitDataResult* response,
                                          google::protobuf::Closure* done) {
    // TODO(zxy) delete in 1.2 version
    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
    attachment_transfer_request_block<PTransmitDataParams>(request, cntl);

    _transmit_block(cntl_base, request, response, done, Status::OK());
}

void PInternalServiceImpl::transmit_block_by_http(google::protobuf::RpcController* cntl_base,
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

void PInternalServiceImpl::_transmit_block(google::protobuf::RpcController* cntl_base,
                                           const PTransmitDataParams* request,
                                           PTransmitDataResult* response,
                                           google::protobuf::Closure* done,
                                           const Status& extract_st) {
    std::string query_id;
    TUniqueId finst_id;
    std::shared_ptr<MemTrackerLimiter> transmit_tracker;
    if (request->has_query_id()) {
        query_id = print_id(request->query_id());
        finst_id.__set_hi(request->finst_id().hi());
        finst_id.__set_lo(request->finst_id().lo());
        // In some cases, query mem tracker does not exist in BE when transmit block, will get null pointer.
        transmit_tracker = std::make_shared<MemTrackerLimiter>(
                -1, fmt::format("QueryTransmit#queryId={}", query_id),
                _exec_env->task_pool_mem_tracker_registry()->get_task_mem_tracker(query_id));
    } else {
        query_id = "unkown_transmit_block";
        transmit_tracker = std::make_shared<MemTrackerLimiter>(-1, "unkown_transmit_block");
    }
    SCOPED_ATTACH_TASK(transmit_tracker, ThreadContext::TaskType::QUERY, query_id, finst_id);
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

void PInternalServiceImpl::check_rpc_channel(google::protobuf::RpcController* controller,
                                             const PCheckRPCChannelRequest* request,
                                             PCheckRPCChannelResponse* response,
                                             google::protobuf::Closure* done) {
    brpc::ClosureGuard closure_guard(done);
    response->mutable_status()->set_status_code(0);
    if (request->data().size() != request->size()) {
        std::stringstream ss;
        ss << "data size not same, expected: " << request->size()
           << ", actual: " << request->data().size();
        response->mutable_status()->add_error_msgs(ss.str());
        response->mutable_status()->set_status_code(1);

    } else {
        Md5Digest digest;
        digest.update(static_cast<const void*>(request->data().c_str()), request->data().size());
        digest.digest();
        if (!iequal(digest.hex(), request->md5())) {
            std::stringstream ss;
            ss << "md5 not same, expected: " << request->md5() << ", actual: " << digest.hex();
            response->mutable_status()->add_error_msgs(ss.str());
            response->mutable_status()->set_status_code(1);
        }
    }
}

void PInternalServiceImpl::reset_rpc_channel(google::protobuf::RpcController* controller,
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

void PInternalServiceImpl::hand_shake(google::protobuf::RpcController* cntl_base,
                                      const PHandShakeRequest* request,
                                      PHandShakeResponse* response,
                                      google::protobuf::Closure* done) {
    brpc::ClosureGuard closure_guard(done);
    if (request->has_hello()) {
        response->set_hello(request->hello());
    }
    response->mutable_status()->set_status_code(0);
}

void PInternalServiceImpl::request_slave_tablet_pull_rowset(
        google::protobuf::RpcController* controller, const PTabletWriteSlaveRequest* request,
        PTabletWriteSlaveResult* response, google::protobuf::Closure* done) {
    brpc::ClosureGuard closure_guard(done);
    RowsetMetaPB rowset_meta_pb = request->rowset_meta();
    std::string rowset_path = request->rowset_path();
    google::protobuf::Map<int64, int64> segments_size = request->segments_size();
    std::string host = request->host();
    int64_t http_port = request->http_port();
    int64_t brpc_port = request->brpc_port();
    std::string token = request->token();
    int64_t node_id = request->node_id();
    _slave_replica_worker_pool.offer([=]() {
        TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(
                rowset_meta_pb.tablet_id(), rowset_meta_pb.tablet_schema_hash());
        if (tablet == nullptr) {
            LOG(WARNING) << "failed to pull rowset for slave replica. tablet ["
                         << rowset_meta_pb.tablet_id()
                         << "] is not exist. txn_id=" << rowset_meta_pb.txn_id();
            _response_pull_slave_rowset(host, brpc_port, rowset_meta_pb.txn_id(),
                                        rowset_meta_pb.tablet_id(), node_id, false);
            return;
        }

        RowsetMetaSharedPtr rowset_meta(new RowsetMeta());
        std::string rowset_meta_str;
        bool ret = rowset_meta_pb.SerializeToString(&rowset_meta_str);
        if (!ret) {
            LOG(WARNING) << "failed to pull rowset for slave replica. serialize rowset meta "
                            "failed. rowset_id="
                         << rowset_meta_pb.rowset_id()
                         << ", tablet_id=" << rowset_meta_pb.tablet_id()
                         << ", txn_id=" << rowset_meta_pb.txn_id();
            _response_pull_slave_rowset(host, brpc_port, rowset_meta_pb.txn_id(),
                                        rowset_meta_pb.tablet_id(), node_id, false);
            return;
        }
        bool parsed = rowset_meta->init(rowset_meta_str);
        if (!parsed) {
            LOG(WARNING) << "failed to pull rowset for slave replica. parse rowset meta string "
                            "failed. rowset_id="
                         << rowset_meta_pb.rowset_id()
                         << ", tablet_id=" << rowset_meta_pb.tablet_id()
                         << ", txn_id=" << rowset_meta_pb.txn_id();
            // return false will break meta iterator, return true to skip this error
            _response_pull_slave_rowset(host, brpc_port, rowset_meta->txn_id(),
                                        rowset_meta->tablet_id(), node_id, false);
            return;
        }
        RowsetId remote_rowset_id = rowset_meta->rowset_id();
        // change rowset id because it maybe same as other local rowset
        RowsetId new_rowset_id = StorageEngine::instance()->next_rowset_id();
        rowset_meta->set_rowset_id(new_rowset_id);
        rowset_meta->set_tablet_uid(tablet->tablet_uid());
        VLOG_CRITICAL << "succeed to init rowset meta for slave replica. rowset_id="
                      << rowset_meta->rowset_id() << ", tablet_id=" << rowset_meta->tablet_id()
                      << ", txn_id=" << rowset_meta->txn_id();

        for (auto& segment : segments_size) {
            uint64_t file_size = segment.second;
            uint64_t estimate_timeout = file_size / config::download_low_speed_limit_kbps / 1024;
            if (estimate_timeout < config::download_low_speed_time) {
                estimate_timeout = config::download_low_speed_time;
            }

            std::stringstream ss;
            ss << "http://" << host << ":" << http_port << "/api/_tablet/_download?token=" << token
               << "&file=" << rowset_path << "/" << remote_rowset_id << "_" << segment.first
               << ".dat";
            std::string remote_file_url = ss.str();
            ss.str("");
            ss << tablet->tablet_path() << "/" << rowset_meta->rowset_id() << "_" << segment.first
               << ".dat";
            std::string local_file_path = ss.str();

            auto download_cb = [remote_file_url, estimate_timeout, local_file_path,
                                file_size](HttpClient* client) {
                RETURN_IF_ERROR(client->init(remote_file_url));
                client->set_timeout_ms(estimate_timeout * 1000);
                RETURN_IF_ERROR(client->download(local_file_path));

                // Check file length
                uint64_t local_file_size = std::filesystem::file_size(local_file_path);
                if (local_file_size != file_size) {
                    LOG(WARNING)
                            << "failed to pull rowset for slave replica. download file length error"
                            << ", remote_path=" << remote_file_url << ", file_size=" << file_size
                            << ", local_file_size=" << local_file_size;
                    return Status::InternalError("downloaded file size is not equal");
                }
                chmod(local_file_path.c_str(), S_IRUSR | S_IWUSR);
                return Status::OK();
            };
            auto st = HttpClient::execute_with_retry(DOWNLOAD_FILE_MAX_RETRY, 1, download_cb);
            if (!st.ok()) {
                LOG(WARNING)
                        << "failed to pull rowset for slave replica. failed to download file. url="
                        << remote_file_url << ", local_path=" << local_file_path
                        << ", txn_id=" << rowset_meta->txn_id();
                _response_pull_slave_rowset(host, brpc_port, rowset_meta->txn_id(),
                                            rowset_meta->tablet_id(), node_id, false);
                return;
            }
            VLOG_CRITICAL << "succeed to download file for slave replica. url=" << remote_file_url
                          << ", local_path=" << local_file_path
                          << ", txn_id=" << rowset_meta->txn_id();
        }

        RowsetSharedPtr rowset;
        Status create_status = RowsetFactory::create_rowset(
                tablet->tablet_schema(), tablet->tablet_path(), rowset_meta, &rowset);
        if (!create_status) {
            LOG(WARNING) << "failed to create rowset from rowset meta for slave replica"
                         << ". rowset_id: " << rowset_meta->rowset_id()
                         << ", rowset_type: " << rowset_meta->rowset_type()
                         << ", rowset_state: " << rowset_meta->rowset_state()
                         << ", tablet_id=" << rowset_meta->tablet_id()
                         << ", txn_id=" << rowset_meta->txn_id();
            _response_pull_slave_rowset(host, brpc_port, rowset_meta->txn_id(),
                                        rowset_meta->tablet_id(), node_id, false);
            return;
        }
        if (rowset_meta->rowset_state() != RowsetStatePB::COMMITTED) {
            LOG(WARNING) << "could not commit txn for slave replica because master rowset state is "
                            "not committed, rowset_state="
                         << rowset_meta->rowset_state()
                         << ", tablet_id=" << rowset_meta->tablet_id()
                         << ", txn_id=" << rowset_meta->txn_id();
            _response_pull_slave_rowset(host, brpc_port, rowset_meta->txn_id(),
                                        rowset_meta->tablet_id(), node_id, false);
            return;
        }
        Status commit_txn_status = StorageEngine::instance()->txn_manager()->commit_txn(
                tablet->data_dir()->get_meta(), rowset_meta->partition_id(), rowset_meta->txn_id(),
                rowset_meta->tablet_id(), rowset_meta->tablet_schema_hash(), tablet->tablet_uid(),
                rowset_meta->load_id(), rowset, true);
        if (!commit_txn_status &&
            commit_txn_status !=
                    Status::OLAPInternalError(OLAP_ERR_PUSH_TRANSACTION_ALREADY_EXIST)) {
            LOG(WARNING) << "failed to add committed rowset for slave replica. rowset_id="
                         << rowset_meta->rowset_id() << ", tablet_id=" << rowset_meta->tablet_id()
                         << ", txn_id=" << rowset_meta->txn_id();
            _response_pull_slave_rowset(host, brpc_port, rowset_meta->txn_id(),
                                        rowset_meta->tablet_id(), node_id, false);
            return;
        }
        VLOG_CRITICAL << "succeed to pull rowset for slave replica. successfully to add committed "
                         "rowset: "
                      << rowset_meta->rowset_id()
                      << " to tablet, tablet_id=" << rowset_meta->tablet_id()
                      << ", schema_hash=" << rowset_meta->tablet_schema_hash()
                      << ", txn_id=" << rowset_meta->txn_id();
        _response_pull_slave_rowset(host, brpc_port, rowset_meta->txn_id(),
                                    rowset_meta->tablet_id(), node_id, true);
    });
    Status::OK().to_protobuf(response->mutable_status());
}

void PInternalServiceImpl::_response_pull_slave_rowset(const std::string& remote_host,
                                                       int64_t brpc_port, int64_t txn_id,
                                                       int64_t tablet_id, int64_t node_id,
                                                       bool is_succeed) {
    std::shared_ptr<PBackendService_Stub> stub =
            ExecEnv::GetInstance()->brpc_internal_client_cache()->get_client(remote_host,
                                                                             brpc_port);
    if (stub == nullptr) {
        LOG(WARNING) << "failed to response result of slave replica to master replica. get rpc "
                        "stub failed, master host="
                     << remote_host << ", port=" << brpc_port << ", tablet_id=" << tablet_id
                     << ", txn_id=" << txn_id;
        return;
    }

    PTabletWriteSlaveDoneRequest request;
    request.set_txn_id(txn_id);
    request.set_tablet_id(tablet_id);
    request.set_node_id(node_id);
    request.set_is_succeed(is_succeed);
    RefCountClosure<PTabletWriteSlaveDoneResult>* closure =
            new RefCountClosure<PTabletWriteSlaveDoneResult>();
    closure->ref();
    closure->ref();
    closure->cntl.set_timeout_ms(config::slave_replica_writer_rpc_timeout_sec * 1000);
    closure->cntl.ignore_eovercrowded();
    stub->response_slave_tablet_pull_rowset(&closure->cntl, &request, &closure->result, closure);

    closure->join();
    if (closure->cntl.Failed()) {
        if (!ExecEnv::GetInstance()->brpc_internal_client_cache()->available(stub, remote_host,
                                                                             brpc_port)) {
            ExecEnv::GetInstance()->brpc_internal_client_cache()->erase(
                    closure->cntl.remote_side());
        }
        LOG(WARNING) << "failed to response result of slave replica to master replica, error="
                     << berror(closure->cntl.ErrorCode())
                     << ", error_text=" << closure->cntl.ErrorText()
                     << ", master host: " << remote_host << ", tablet_id=" << tablet_id
                     << ", txn_id=" << txn_id;
    }

    if (closure->unref()) {
        delete closure;
    }
    closure = nullptr;
    VLOG_CRITICAL << "succeed to response the result of slave replica pull rowset to master "
                     "replica. master host: "
                  << remote_host << ". is_succeed=" << is_succeed << ", tablet_id=" << tablet_id
                  << ", slave server=" << node_id << ", txn_id=" << txn_id;
}

void PInternalServiceImpl::response_slave_tablet_pull_rowset(
        google::protobuf::RpcController* controller, const PTabletWriteSlaveDoneRequest* request,
        PTabletWriteSlaveDoneResult* response, google::protobuf::Closure* done) {
    brpc::ClosureGuard closure_guard(done);
    VLOG_CRITICAL
            << "receive the result of slave replica pull rowset from slave replica. slave server="
            << request->node_id() << ", is_succeed=" << request->is_succeed()
            << ", tablet_id=" << request->tablet_id() << ", txn_id=" << request->txn_id();
    StorageEngine::instance()->txn_manager()->finish_slave_tablet_pull_rowset(
            request->txn_id(), request->tablet_id(), request->node_id(), request->is_succeed());
    Status::OK().to_protobuf(response->mutable_status());
}

} // namespace doris
