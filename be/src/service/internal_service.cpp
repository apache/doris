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

#include <assert.h>
#include <brpc/closure_guard.h>
#include <brpc/controller.h>
#include <bthread/bthread.h>
#include <bthread/types.h>
#include <butil/errno.h>
#include <butil/iobuf.h>
#include <fcntl.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Status_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/internal_service.pb.h>
#include <gen_cpp/olap_file.pb.h>
#include <gen_cpp/segment_v2.pb.h>
#include <gen_cpp/types.pb.h>
#include <google/protobuf/stubs/callback.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/stat.h>

#include <algorithm>
#include <exception>
#include <filesystem>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/consts.h"
#include "common/exception.h"
#include "common/logging.h"
#include "common/signal_handler.h"
#include "common/status.h"
#include "gen_cpp/BackendService.h"
#include "gen_cpp/PaloInternalService_types.h"
#include "gen_cpp/internal_service.pb.h"
#include "gutil/integral_types.h"
#include "http/http_client.h"
#include "io/fs/local_file_system.h"
#include "io/fs/stream_load_pipe.h"
#include "io/io_common.h"
#include "olap/data_dir.h"
#include "olap/olap_common.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/rowset/segment_v2/column_reader.h"
#include "olap/rowset/segment_v2/common.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/segment_loader.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "olap/tablet_manager.h"
#include "olap/tablet_schema.h"
#include "olap/txn_manager.h"
#include "olap/utils.h"
#include "runtime/buffer_control_block.h"
#include "runtime/cache/result_cache.h"
#include "runtime/define_primitive_type.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/fold_constant_executor.h"
#include "runtime/fragment_mgr.h"
#include "runtime/load_channel_mgr.h"
#include "runtime/load_stream_mgr.h"
#include "runtime/result_buffer_mgr.h"
#include "runtime/routine_load/routine_load_task_executor.h"
#include "runtime/stream_load/new_load_stream_mgr.h"
#include "runtime/stream_load/stream_load_context.h"
#include "runtime/thread_context.h"
#include "runtime/types.h"
#include "service/point_query_executor.h"
#include "util/arrow/row_batch.h"
#include "util/async_io.h"
#include "util/brpc_client_cache.h"
#include "util/doris_metrics.h"
#include "util/md5.h"
#include "util/metrics.h"
#include "util/network_util.h"
#include "util/proto_util.h"
#include "util/ref_count_closure.h"
#include "util/runtime_profile.h"
#include "util/stopwatch.hpp"
#include "util/string_util.h"
#include "util/telemetry/brpc_carrier.h"
#include "util/telemetry/telemetry.h"
#include "util/thrift_util.h"
#include "util/time.h"
#include "util/uid_util.h"
#include "vec/columns/column.h"
#include "vec/columns/column_string.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/data_types/data_type.h"
#include "vec/exec/format/avro//avro_jni_reader.h"
#include "vec/exec/format/csv/csv_reader.h"
#include "vec/exec/format/generic_reader.h"
#include "vec/exec/format/json/new_json_reader.h"
#include "vec/exec/format/orc/vorc_reader.h"
#include "vec/exec/format/parquet/vparquet_reader.h"
#include "vec/jsonb/serialize.h"
#include "vec/runtime/vdata_stream_mgr.h"

namespace google {
namespace protobuf {
class RpcController;
} // namespace protobuf
} // namespace google

namespace doris {
using namespace ErrorCode;

const uint32_t DOWNLOAD_FILE_MAX_RETRY = 3;

DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(heavy_work_pool_queue_size, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(light_work_pool_queue_size, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(heavy_work_active_threads, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(light_work_active_threads, MetricUnit::NOUNIT);

DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(heavy_work_pool_max_queue_size, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(light_work_pool_max_queue_size, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(heavy_work_max_threads, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(light_work_max_threads, MetricUnit::NOUNIT);

bthread_key_t btls_key;

static void thread_context_deleter(void* d) {
    delete static_cast<ThreadContext*>(d);
}

template <typename T>
class NewHttpClosure : public ::google::protobuf::Closure {
public:
    NewHttpClosure(google::protobuf::Closure* done) : _done(done) {}
    NewHttpClosure(T* request, google::protobuf::Closure* done) : _request(request), _done(done) {}

    void Run() override {
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
concept CanCancel = requires(T* response) { response->mutable_status(); };

template <CanCancel T>
void offer_failed(T* response, google::protobuf::Closure* done, const FifoThreadPool& pool) {
    brpc::ClosureGuard closure_guard(done);
    response->mutable_status()->set_status_code(TStatusCode::CANCELLED);
    response->mutable_status()->add_error_msgs("fail to offer request to the work pool, pool=" +
                                               pool.get_info());
}

template <typename T>
void offer_failed(T* response, google::protobuf::Closure* done, const FifoThreadPool& pool) {
    brpc::ClosureGuard closure_guard(done);
    LOG(WARNING) << "fail to offer request to the work pool, pool=" << pool.get_info();
}

PInternalServiceImpl::PInternalServiceImpl(ExecEnv* exec_env)
        : _exec_env(exec_env),
          _heavy_work_pool(config::brpc_heavy_work_pool_threads != -1
                                   ? config::brpc_heavy_work_pool_threads
                                   : std::max(128, CpuInfo::num_cores() * 4),
                           config::brpc_heavy_work_pool_max_queue_size != -1
                                   ? config::brpc_heavy_work_pool_max_queue_size
                                   : std::max(10240, CpuInfo::num_cores() * 320),
                           "brpc_heavy"),
          _light_work_pool(config::brpc_light_work_pool_threads != -1
                                   ? config::brpc_light_work_pool_threads
                                   : std::max(128, CpuInfo::num_cores() * 4),
                           config::brpc_light_work_pool_max_queue_size != -1
                                   ? config::brpc_light_work_pool_max_queue_size
                                   : std::max(10240, CpuInfo::num_cores() * 320),
                           "brpc_light"),
          _load_stream_mgr(new LoadStreamMgr(
                  exec_env->store_paths().size() * config::flush_thread_num_per_store,
                  &_heavy_work_pool, &_light_work_pool)) {
    REGISTER_HOOK_METRIC(heavy_work_pool_queue_size,
                         [this]() { return _heavy_work_pool.get_queue_size(); });
    REGISTER_HOOK_METRIC(light_work_pool_queue_size,
                         [this]() { return _light_work_pool.get_queue_size(); });
    REGISTER_HOOK_METRIC(heavy_work_active_threads,
                         [this]() { return _heavy_work_pool.get_active_threads(); });
    REGISTER_HOOK_METRIC(light_work_active_threads,
                         [this]() { return _light_work_pool.get_active_threads(); });

    REGISTER_HOOK_METRIC(heavy_work_pool_max_queue_size,
                         []() { return config::brpc_heavy_work_pool_max_queue_size; });
    REGISTER_HOOK_METRIC(light_work_pool_max_queue_size,
                         []() { return config::brpc_light_work_pool_max_queue_size; });
    REGISTER_HOOK_METRIC(heavy_work_max_threads,
                         []() { return config::brpc_heavy_work_pool_threads; });
    REGISTER_HOOK_METRIC(light_work_max_threads,
                         []() { return config::brpc_light_work_pool_threads; });

    CHECK_EQ(0, bthread_key_create(&btls_key, thread_context_deleter));
    CHECK_EQ(0, bthread_key_create(&AsyncIO::btls_io_ctx_key, AsyncIO::io_ctx_key_deleter));
}

PInternalServiceImpl::~PInternalServiceImpl() {
    DEREGISTER_HOOK_METRIC(heavy_work_pool_queue_size);
    DEREGISTER_HOOK_METRIC(light_work_pool_queue_size);
    DEREGISTER_HOOK_METRIC(heavy_work_active_threads);
    DEREGISTER_HOOK_METRIC(light_work_active_threads);

    DEREGISTER_HOOK_METRIC(heavy_work_pool_max_queue_size);
    DEREGISTER_HOOK_METRIC(light_work_pool_max_queue_size);
    DEREGISTER_HOOK_METRIC(heavy_work_max_threads);
    DEREGISTER_HOOK_METRIC(light_work_max_threads);

    CHECK_EQ(0, bthread_key_delete(btls_key));
    CHECK_EQ(0, bthread_key_delete(AsyncIO::btls_io_ctx_key));
}

void PInternalServiceImpl::transmit_data(google::protobuf::RpcController* controller,
                                         const PTransmitDataParams* request,
                                         PTransmitDataResult* response,
                                         google::protobuf::Closure* done) {}

void PInternalServiceImpl::transmit_data_by_http(google::protobuf::RpcController* controller,
                                                 const PEmptyRequest* request,
                                                 PTransmitDataResult* response,
                                                 google::protobuf::Closure* done) {}

void PInternalServiceImpl::_transmit_data(google::protobuf::RpcController* controller,
                                          const PTransmitDataParams* request,
                                          PTransmitDataResult* response,
                                          google::protobuf::Closure* done,
                                          const Status& extract_st) {}

void PInternalServiceImpl::tablet_writer_open(google::protobuf::RpcController* controller,
                                              const PTabletWriterOpenRequest* request,
                                              PTabletWriterOpenResult* response,
                                              google::protobuf::Closure* done) {
    bool ret = _light_work_pool.try_offer([this, request, response, done]() {
        VLOG_RPC << "tablet writer open, id=" << request->id()
                 << ", index_id=" << request->index_id() << ", txn_id=" << request->txn_id();
        signal::set_signal_task_id(request->id());
        brpc::ClosureGuard closure_guard(done);
        auto st = _exec_env->load_channel_mgr()->open(*request);
        if (!st.ok()) {
            LOG(WARNING) << "load channel open failed, message=" << st << ", id=" << request->id()
                         << ", index_id=" << request->index_id()
                         << ", txn_id=" << request->txn_id();
        }
        st.to_protobuf(response->mutable_status());
    });
    if (!ret) {
        offer_failed(response, done, _light_work_pool);
        return;
    }
}

void PInternalServiceImpl::exec_plan_fragment(google::protobuf::RpcController* controller,
                                              const PExecPlanFragmentRequest* request,
                                              PExecPlanFragmentResult* response,
                                              google::protobuf::Closure* done) {
    bool ret = _light_work_pool.try_offer([this, controller, request, response, done]() {
        _exec_plan_fragment_in_pthread(controller, request, response, done);
    });
    if (!ret) {
        offer_failed(response, done, _light_work_pool);
        return;
    }
}

void PInternalServiceImpl::_exec_plan_fragment_in_pthread(
        google::protobuf::RpcController* controller, const PExecPlanFragmentRequest* request,
        PExecPlanFragmentResult* response, google::protobuf::Closure* done) {
    auto span = telemetry::start_rpc_server_span("exec_plan_fragment", controller);
    auto scope = OpentelemetryScope {span};
    brpc::ClosureGuard closure_guard(done);
    auto st = Status::OK();
    bool compact = request->has_compact() ? request->compact() : false;
    PFragmentRequestVersion version =
            request->has_version() ? request->version() : PFragmentRequestVersion::VERSION_1;
    try {
        st = _exec_plan_fragment_impl(request->request(), version, compact);
    } catch (const Exception& e) {
        st = e.to_status();
    } catch (...) {
        st = Status::Error(ErrorCode::INTERNAL_ERROR,
                           "_exec_plan_fragment_impl meet unknown error");
    }
    if (!st.ok()) {
        LOG(WARNING) << "exec plan fragment failed, errmsg=" << st;
    }
    st.to_protobuf(response->mutable_status());
}

void PInternalServiceImpl::exec_plan_fragment_prepare(google::protobuf::RpcController* controller,
                                                      const PExecPlanFragmentRequest* request,
                                                      PExecPlanFragmentResult* response,
                                                      google::protobuf::Closure* done) {
    bool ret = _light_work_pool.try_offer([this, controller, request, response, done]() {
        _exec_plan_fragment_in_pthread(controller, request, response, done);
    });
    if (!ret) {
        offer_failed(response, done, _light_work_pool);
        return;
    }
}

void PInternalServiceImpl::exec_plan_fragment_start(google::protobuf::RpcController* controller,
                                                    const PExecPlanFragmentStartRequest* request,
                                                    PExecPlanFragmentResult* result,
                                                    google::protobuf::Closure* done) {
    bool ret = _light_work_pool.try_offer([this, controller, request, result, done]() {
        auto span = telemetry::start_rpc_server_span("exec_plan_fragment_start", controller);
        auto scope = OpentelemetryScope {span};
        brpc::ClosureGuard closure_guard(done);
        auto st = _exec_env->fragment_mgr()->start_query_execution(request);
        st.to_protobuf(result->mutable_status());
    });
    if (!ret) {
        offer_failed(result, done, _light_work_pool);
        return;
    }
}

void PInternalServiceImpl::open_load_stream(google::protobuf::RpcController* controller,
                                            const POpenLoadStreamRequest* request,
                                            POpenLoadStreamResponse* response,
                                            google::protobuf::Closure* done) {
    bool ret = _light_work_pool.try_offer([this, controller, request, response, done]() {
        signal::set_signal_task_id(request->load_id());
        brpc::ClosureGuard done_guard(done);
        brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
        brpc::StreamOptions stream_options;

        LOG(INFO) << "open load stream, load_id = " << request->load_id()
                  << ", src_id = " << request->src_id();

        for (const auto& req : request->tablets()) {
            TabletManager* tablet_mgr = StorageEngine::instance()->tablet_manager();
            TabletSharedPtr tablet = tablet_mgr->get_tablet(req.tablet_id());
            if (tablet == nullptr) {
                auto st = Status::NotFound("Tablet {} not found", req.tablet_id());
                st.to_protobuf(response->mutable_status());
                cntl->SetFailed(st.to_string());
                return;
            }
            auto resp = response->add_tablet_schemas();
            resp->set_index_id(req.index_id());
            resp->set_enable_unique_key_merge_on_write(tablet->enable_unique_key_merge_on_write());
            tablet->tablet_schema()->to_schema_pb(resp->mutable_tablet_schema());
        }

        LoadStreamSharedPtr load_stream;
        auto st = _load_stream_mgr->open_load_stream(request, load_stream);
        if (!st.ok()) {
            st.to_protobuf(response->mutable_status());
            return;
        }

        stream_options.handler = load_stream.get();
        // TODO : set idle timeout
        // stream_options.idle_timeout_ms =

        StreamId streamid;
        if (brpc::StreamAccept(&streamid, *cntl, &stream_options) != 0) {
            st = Status::Cancelled("Fail to accept stream {}", streamid);
            st.to_protobuf(response->mutable_status());
            cntl->SetFailed(st.to_string());
            return;
        }

        load_stream->add_rpc_stream();
        VLOG_DEBUG << "get streamid =" << streamid;
        st.to_protobuf(response->mutable_status());
    });
    if (!ret) {
        offer_failed(response, done, _light_work_pool);
    }
}

void PInternalServiceImpl::tablet_writer_add_block(google::protobuf::RpcController* controller,
                                                   const PTabletWriterAddBlockRequest* request,
                                                   PTabletWriterAddBlockResult* response,
                                                   google::protobuf::Closure* done) {
    bool ret = _heavy_work_pool.try_offer([this, controller, request, response, done]() {
        _tablet_writer_add_block(controller, request, response, done);
    });
    if (!ret) {
        offer_failed(response, done, _heavy_work_pool);
        return;
    }
}

void PInternalServiceImpl::tablet_writer_add_block_by_http(
        google::protobuf::RpcController* controller, const ::doris::PEmptyRequest* request,
        PTabletWriterAddBlockResult* response, google::protobuf::Closure* done) {
    bool ret = _heavy_work_pool.try_offer([this, controller, response, done]() {
        PTabletWriterAddBlockRequest* new_request = new PTabletWriterAddBlockRequest();
        google::protobuf::Closure* new_done =
                new NewHttpClosure<PTabletWriterAddBlockRequest>(new_request, done);
        brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
        Status st = attachment_extract_request_contain_block<PTabletWriterAddBlockRequest>(
                new_request, cntl);
        if (st.ok()) {
            _tablet_writer_add_block(controller, new_request, response, new_done);
        } else {
            st.to_protobuf(response->mutable_status());
        }
    });
    if (!ret) {
        offer_failed(response, done, _heavy_work_pool);
        return;
    }
}

void PInternalServiceImpl::_tablet_writer_add_block(google::protobuf::RpcController* controller,
                                                    const PTabletWriterAddBlockRequest* request,
                                                    PTabletWriterAddBlockResult* response,
                                                    google::protobuf::Closure* done) {
    int64_t submit_task_time_ns = MonotonicNanos();
    bool ret = _heavy_work_pool.try_offer([request, response, done, submit_task_time_ns, this]() {
        int64_t wait_execution_time_ns = MonotonicNanos() - submit_task_time_ns;
        brpc::ClosureGuard closure_guard(done);
        int64_t execution_time_ns = 0;
        {
            SCOPED_RAW_TIMER(&execution_time_ns);
            signal::set_signal_task_id(request->id());
            auto st = _exec_env->load_channel_mgr()->add_batch(*request, response);
            if (!st.ok()) {
                LOG(WARNING) << "tablet writer add block failed, message=" << st
                             << ", id=" << request->id() << ", index_id=" << request->index_id()
                             << ", sender_id=" << request->sender_id()
                             << ", backend id=" << request->backend_id();
            }
            st.to_protobuf(response->mutable_status());
        }
        response->set_execution_time_us(execution_time_ns / NANOS_PER_MICRO);
        response->set_wait_execution_time_us(wait_execution_time_ns / NANOS_PER_MICRO);
    });
    if (!ret) {
        offer_failed(response, done, _heavy_work_pool);
        return;
    }
}

void PInternalServiceImpl::tablet_writer_cancel(google::protobuf::RpcController* controller,
                                                const PTabletWriterCancelRequest* request,
                                                PTabletWriterCancelResult* response,
                                                google::protobuf::Closure* done) {
    bool ret = _light_work_pool.try_offer([this, request, done]() {
        VLOG_RPC << "tablet writer cancel, id=" << request->id()
                 << ", index_id=" << request->index_id() << ", sender_id=" << request->sender_id();
        signal::set_signal_task_id(request->id());
        brpc::ClosureGuard closure_guard(done);
        auto st = _exec_env->load_channel_mgr()->cancel(*request);
        if (!st.ok()) {
            LOG(WARNING) << "tablet writer cancel failed, id=" << request->id()
                         << ", index_id=" << request->index_id()
                         << ", sender_id=" << request->sender_id();
        }
    });
    if (!ret) {
        offer_failed(response, done, _light_work_pool);
        return;
    }
}

Status PInternalServiceImpl::_exec_plan_fragment_impl(
        const std::string& ser_request, PFragmentRequestVersion version, bool compact,
        const std::function<void(RuntimeState*, Status*)>& cb) {
    // Sometimes the BE do not receive the first heartbeat message and it receives request from FE
    // If BE execute this fragment, it will core when it wants to get some property from master info.
    if (ExecEnv::GetInstance()->master_info() == nullptr) {
        return Status::InternalError(
                "Have not receive the first heartbeat message from master, not ready to provide "
                "service");
    }
    if (version == PFragmentRequestVersion::VERSION_1) {
        // VERSION_1 should be removed in v1.2
        TExecPlanFragmentParams t_request;
        {
            const uint8_t* buf = (const uint8_t*)ser_request.data();
            uint32_t len = ser_request.size();
            RETURN_IF_ERROR(deserialize_thrift_msg(buf, &len, compact, &t_request));
        }
        if (cb) {
            return _exec_env->fragment_mgr()->exec_plan_fragment(t_request, cb);
        } else {
            return _exec_env->fragment_mgr()->exec_plan_fragment(t_request);
        }
    } else if (version == PFragmentRequestVersion::VERSION_2) {
        TExecPlanFragmentParamsList t_request;
        {
            const uint8_t* buf = (const uint8_t*)ser_request.data();
            uint32_t len = ser_request.size();
            RETURN_IF_ERROR(deserialize_thrift_msg(buf, &len, compact, &t_request));
        }

        for (const TExecPlanFragmentParams& params : t_request.paramsList) {
            if (cb) {
                RETURN_IF_ERROR(_exec_env->fragment_mgr()->exec_plan_fragment(params, cb));
            } else {
                RETURN_IF_ERROR(_exec_env->fragment_mgr()->exec_plan_fragment(params));
            }
        }
        return Status::OK();
    } else if (version == PFragmentRequestVersion::VERSION_3) {
        TPipelineFragmentParamsList t_request;
        {
            const uint8_t* buf = (const uint8_t*)ser_request.data();
            uint32_t len = ser_request.size();
            RETURN_IF_ERROR(deserialize_thrift_msg(buf, &len, compact, &t_request));
        }

        for (const TPipelineFragmentParams& params : t_request.params_list) {
            if (cb) {
                RETURN_IF_ERROR(_exec_env->fragment_mgr()->exec_plan_fragment(params, cb));
            } else {
                RETURN_IF_ERROR(_exec_env->fragment_mgr()->exec_plan_fragment(params));
            }
        }
        return Status::OK();
    } else {
        return Status::InternalError("invalid version");
    }
}

void PInternalServiceImpl::cancel_plan_fragment(google::protobuf::RpcController* controller,
                                                const PCancelPlanFragmentRequest* request,
                                                PCancelPlanFragmentResult* result,
                                                google::protobuf::Closure* done) {
    bool ret = _light_work_pool.try_offer([this, controller, request, result, done]() {
        auto span = telemetry::start_rpc_server_span("exec_plan_fragment_start", controller);
        auto scope = OpentelemetryScope {span};
        brpc::ClosureGuard closure_guard(done);
        TUniqueId tid;
        tid.__set_hi(request->finst_id().hi());
        tid.__set_lo(request->finst_id().lo());
        signal::set_signal_task_id(tid);
        Status st = Status::OK();

        const bool has_cancel_reason = request->has_cancel_reason();
        LOG(INFO) << fmt::format("Cancel instance {}, reason: {}", print_id(tid),
                                 has_cancel_reason
                                         ? PPlanFragmentCancelReason_Name(request->cancel_reason())
                                         : "INTERNAL_ERROR");

        _exec_env->fragment_mgr()->cancel_instance(
                tid, has_cancel_reason ? request->cancel_reason()
                                       : PPlanFragmentCancelReason::INTERNAL_ERROR);

        // TODO: the logic seems useless, cancel only return Status::OK. remove it
        st.to_protobuf(result->mutable_status());
    });
    if (!ret) {
        offer_failed(result, done, _light_work_pool);
        return;
    }
}

void PInternalServiceImpl::fetch_data(google::protobuf::RpcController* controller,
                                      const PFetchDataRequest* request, PFetchDataResult* result,
                                      google::protobuf::Closure* done) {
    bool ret = _heavy_work_pool.try_offer([this, controller, request, result, done]() {
        brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
        GetResultBatchCtx* ctx = new GetResultBatchCtx(cntl, result, done);
        _exec_env->result_mgr()->fetch_data(request->finst_id(), ctx);
    });
    if (!ret) {
        offer_failed(result, done, _heavy_work_pool);
        return;
    }
}

void PInternalServiceImpl::fetch_table_schema(google::protobuf::RpcController* controller,
                                              const PFetchTableSchemaRequest* request,
                                              PFetchTableSchemaResult* result,
                                              google::protobuf::Closure* done) {
    bool ret = _heavy_work_pool.try_offer([request, result, done]() {
        VLOG_RPC << "fetch table schema";
        brpc::ClosureGuard closure_guard(done);
        TFileScanRange file_scan_range;
        Status st = Status::OK();
        {
            const uint8_t* buf = (const uint8_t*)(request->file_scan_range().data());
            uint32_t len = request->file_scan_range().size();
            st = deserialize_thrift_msg(buf, &len, false, &file_scan_range);
            if (!st.ok()) {
                LOG(WARNING) << "fetch table schema failed, errmsg=" << st;
                st.to_protobuf(result->mutable_status());
                return;
            }
        }
        if (file_scan_range.__isset.ranges == false) {
            st = Status::InternalError("can not get TFileRangeDesc.");
            st.to_protobuf(result->mutable_status());
            return;
        }
        if (file_scan_range.__isset.params == false) {
            st = Status::InternalError("can not get TFileScanRangeParams.");
            st.to_protobuf(result->mutable_status());
            return;
        }
        const TFileRangeDesc& range = file_scan_range.ranges.at(0);
        const TFileScanRangeParams& params = file_scan_range.params;

        // make sure profile is desctructed after reader cause PrefetchBufferedReader
        // might asynchronouslly access the profile
        std::unique_ptr<RuntimeProfile> profile =
                std::make_unique<RuntimeProfile>("FetchTableSchema");
        std::unique_ptr<vectorized::GenericReader> reader(nullptr);
        io::IOContext io_ctx;
        io::FileCacheStatistics file_cache_statis;
        io_ctx.file_cache_stats = &file_cache_statis;
        switch (params.format_type) {
        case TFileFormatType::FORMAT_CSV_PLAIN:
        case TFileFormatType::FORMAT_CSV_GZ:
        case TFileFormatType::FORMAT_CSV_BZ2:
        case TFileFormatType::FORMAT_CSV_LZ4FRAME:
        case TFileFormatType::FORMAT_CSV_LZ4BLOCK:
        case TFileFormatType::FORMAT_CSV_SNAPPYBLOCK:
        case TFileFormatType::FORMAT_CSV_LZOP:
        case TFileFormatType::FORMAT_CSV_DEFLATE: {
            // file_slots is no use
            std::vector<SlotDescriptor*> file_slots;
            reader = vectorized::CsvReader::create_unique(profile.get(), params, range, file_slots,
                                                          &io_ctx);
            break;
        }
        case TFileFormatType::FORMAT_PARQUET: {
            reader = vectorized::ParquetReader::create_unique(params, range, &io_ctx, nullptr);
            break;
        }
        case TFileFormatType::FORMAT_ORC: {
            reader = vectorized::OrcReader::create_unique(params, range, "", &io_ctx);
            break;
        }
        case TFileFormatType::FORMAT_JSON: {
            std::vector<SlotDescriptor*> file_slots;
            reader = vectorized::NewJsonReader::create_unique(profile.get(), params, range,
                                                              file_slots, &io_ctx);
            break;
        }
        case TFileFormatType::FORMAT_AVRO: {
            // file_slots is no use
            std::vector<SlotDescriptor*> file_slots;
            reader = vectorized::AvroJNIReader::create_unique(profile.get(), params, range,
                                                              file_slots);
            static_cast<void>(
                    ((vectorized::AvroJNIReader*)(reader.get()))->init_fetch_table_schema_reader());
            break;
        }
        default:
            st = Status::InternalError("Not supported file format in fetch table schema: {}",
                                       params.format_type);
            st.to_protobuf(result->mutable_status());
            return;
        }
        std::vector<std::string> col_names;
        std::vector<TypeDescriptor> col_types;
        st = reader->get_parsed_schema(&col_names, &col_types);
        if (!st.ok()) {
            LOG(WARNING) << "fetch table schema failed, errmsg=" << st;
            st.to_protobuf(result->mutable_status());
            return;
        }
        result->set_column_nums(col_names.size());
        for (size_t idx = 0; idx < col_names.size(); ++idx) {
            result->add_column_names(col_names[idx]);
        }
        for (size_t idx = 0; idx < col_types.size(); ++idx) {
            PTypeDesc* type_desc = result->add_column_types();
            col_types[idx].to_protobuf(type_desc);
        }
        st.to_protobuf(result->mutable_status());
    });
    if (!ret) {
        offer_failed(result, done, _heavy_work_pool);
        return;
    }
}

void PInternalServiceImpl::fetch_arrow_flight_schema(google::protobuf::RpcController* controller,
                                                     const PFetchArrowFlightSchemaRequest* request,
                                                     PFetchArrowFlightSchemaResult* result,
                                                     google::protobuf::Closure* done) {
    bool ret = _light_work_pool.try_offer([request, result, done]() {
        brpc::ClosureGuard closure_guard(done);
        RowDescriptor row_desc = ExecEnv::GetInstance()->result_mgr()->find_row_descriptor(
                UniqueId(request->finst_id()).to_thrift());
        if (row_desc.equals(RowDescriptor())) {
            auto st = Status::NotFound("not found row descriptor");
            st.to_protobuf(result->mutable_status());
            return;
        }

        std::shared_ptr<arrow::Schema> schema;
        auto st = convert_to_arrow_schema(row_desc, &schema);
        if (UNLIKELY(!st.ok())) {
            st.to_protobuf(result->mutable_status());
            return;
        }

        std::string schema_str;
        st = serialize_arrow_schema(row_desc, &schema, &schema_str);
        if (st.ok()) {
            result->set_schema(std::move(schema_str));
        }
        st.to_protobuf(result->mutable_status());
    });
    if (!ret) {
        offer_failed(result, done, _heavy_work_pool);
        return;
    }
}

Status PInternalServiceImpl::_tablet_fetch_data(const PTabletKeyLookupRequest* request,
                                                PTabletKeyLookupResponse* response) {
    PointQueryExecutor lookup_util;
    RETURN_IF_ERROR(lookup_util.init(request, response));
    RETURN_IF_ERROR(lookup_util.lookup_up());
    if (VLOG_DEBUG_IS_ON) {
        VLOG_DEBUG << lookup_util.print_profile();
    }
    LOG_EVERY_N(INFO, 500) << lookup_util.print_profile();
    return Status::OK();
}

void PInternalServiceImpl::tablet_fetch_data(google::protobuf::RpcController* controller,
                                             const PTabletKeyLookupRequest* request,
                                             PTabletKeyLookupResponse* response,
                                             google::protobuf::Closure* done) {
    bool ret = _light_work_pool.try_offer([this, controller, request, response, done]() {
        [[maybe_unused]] brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
        brpc::ClosureGuard guard(done);
        Status st = _tablet_fetch_data(request, response);
        st.to_protobuf(response->mutable_status());
    });
    if (!ret) {
        offer_failed(response, done, _light_work_pool);
        return;
    }
}

void PInternalServiceImpl::get_column_ids_by_tablet_ids(google::protobuf::RpcController* controller,
                                                        const PFetchColIdsRequest* request,
                                                        PFetchColIdsResponse* response,
                                                        google::protobuf::Closure* done) {
    bool ret = _light_work_pool.try_offer([this, controller, request, response, done]() {
        _get_column_ids_by_tablet_ids(controller, request, response, done);
    });
    if (!ret) {
        offer_failed(response, done, _light_work_pool);
        return;
    }
}

void PInternalServiceImpl::_get_column_ids_by_tablet_ids(
        google::protobuf::RpcController* controller, const PFetchColIdsRequest* request,
        PFetchColIdsResponse* response, google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    [[maybe_unused]] brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    TabletManager* tablet_mgr = StorageEngine::instance()->tablet_manager();
    const auto& params = request->params();
    for (const auto& param : params) {
        int64_t index_id = param.indexid();
        auto tablet_ids = param.tablet_ids();
        std::set<std::set<int32_t>> filter_set;
        for (const int64_t tablet_id : tablet_ids) {
            TabletSharedPtr tablet = tablet_mgr->get_tablet(tablet_id);
            if (tablet == nullptr) {
                std::stringstream ss;
                ss << "cannot get tablet by id:" << tablet_id;
                LOG(WARNING) << ss.str();
                response->mutable_status()->set_status_code(TStatusCode::ILLEGAL_STATE);
                response->mutable_status()->add_error_msgs(ss.str());
                return;
            }
            // check schema consistency, column ids should be the same
            const auto& columns = tablet->tablet_schema()->columns();
            std::set<int32_t> column_ids;
            for (const auto& col : columns) {
                column_ids.insert(col.unique_id());
            }
            filter_set.insert(column_ids);
        }
        if (filter_set.size() > 1) {
            // consistecy check failed
            std::stringstream ss;
            ss << "consistency check failed: index{" << index_id << "}"
               << "got inconsistent shema";
            LOG(WARNING) << ss.str();
            response->mutable_status()->set_status_code(TStatusCode::ILLEGAL_STATE);
            response->mutable_status()->add_error_msgs(ss.str());
            return;
        }
        // consistency check passed, use the first tablet to be the representative
        TabletSharedPtr tablet = tablet_mgr->get_tablet(tablet_ids[0]);
        const auto& columns = tablet->tablet_schema()->columns();
        auto entry = response->add_entries();
        entry->set_index_id(index_id);
        auto col_name_to_id = entry->mutable_col_name_to_id();
        for (const auto& column : columns) {
            (*col_name_to_id)[column.name()] = column.unique_id();
        }
    }
    response->mutable_status()->set_status_code(TStatusCode::OK);
}

void PInternalServiceImpl::report_stream_load_status(google::protobuf::RpcController* controller,
                                                     const PReportStreamLoadStatusRequest* request,
                                                     PReportStreamLoadStatusResponse* response,
                                                     google::protobuf::Closure* done) {
    TUniqueId load_id;
    load_id.__set_hi(request->load_id().hi());
    load_id.__set_lo(request->load_id().lo());
    Status st = Status::OK();
    auto stream_load_ctx = _exec_env->new_load_stream_mgr()->get(load_id);
    if (!stream_load_ctx) {
        st = Status::InternalError("unknown stream load id: {}", UniqueId(load_id).to_string());
    }
    stream_load_ctx->promise.set_value(st);
    st.to_protobuf(response->mutable_status());
}

void PInternalServiceImpl::get_info(google::protobuf::RpcController* controller,
                                    const PProxyRequest* request, PProxyResult* response,
                                    google::protobuf::Closure* done) {
    bool ret = _heavy_work_pool.try_offer([this, request, response, done]() {
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
                Status st = _exec_env->routine_load_task_executor()
                                    ->get_kafka_partition_offsets_for_times(
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
    });
    if (!ret) {
        offer_failed(response, done, _heavy_work_pool);
        return;
    }
}

void PInternalServiceImpl::update_cache(google::protobuf::RpcController* controller,
                                        const PUpdateCacheRequest* request,
                                        PCacheResponse* response, google::protobuf::Closure* done) {
    bool ret = _light_work_pool.try_offer([this, request, response, done]() {
        brpc::ClosureGuard closure_guard(done);
        _exec_env->result_cache()->update(request, response);
    });
    if (!ret) {
        offer_failed(response, done, _light_work_pool);
        return;
    }
}

void PInternalServiceImpl::fetch_cache(google::protobuf::RpcController* controller,
                                       const PFetchCacheRequest* request, PFetchCacheResult* result,
                                       google::protobuf::Closure* done) {
    bool ret = _heavy_work_pool.try_offer([this, request, result, done]() {
        brpc::ClosureGuard closure_guard(done);
        _exec_env->result_cache()->fetch(request, result);
    });
    if (!ret) {
        offer_failed(result, done, _heavy_work_pool);
        return;
    }
}

void PInternalServiceImpl::clear_cache(google::protobuf::RpcController* controller,
                                       const PClearCacheRequest* request, PCacheResponse* response,
                                       google::protobuf::Closure* done) {
    bool ret = _light_work_pool.try_offer([this, request, response, done]() {
        brpc::ClosureGuard closure_guard(done);
        _exec_env->result_cache()->clear(request, response);
    });
    if (!ret) {
        offer_failed(response, done, _light_work_pool);
        return;
    }
}

void PInternalServiceImpl::merge_filter(::google::protobuf::RpcController* controller,
                                        const ::doris::PMergeFilterRequest* request,
                                        ::doris::PMergeFilterResponse* response,
                                        ::google::protobuf::Closure* done) {
    bool ret = _light_work_pool.try_offer([this, controller, request, response, done]() {
        brpc::ClosureGuard closure_guard(done);
        auto attachment = static_cast<brpc::Controller*>(controller)->request_attachment();
        butil::IOBufAsZeroCopyInputStream zero_copy_input_stream(attachment);
        Status st = _exec_env->fragment_mgr()->merge_filter(request, &zero_copy_input_stream);
        if (!st.ok()) {
            LOG(WARNING) << "merge meet error" << st.to_string();
        }
        st.to_protobuf(response->mutable_status());
    });
    if (!ret) {
        offer_failed(response, done, _light_work_pool);
        return;
    }
}

void PInternalServiceImpl::apply_filter(::google::protobuf::RpcController* controller,
                                        const ::doris::PPublishFilterRequest* request,
                                        ::doris::PPublishFilterResponse* response,
                                        ::google::protobuf::Closure* done) {
    bool ret = _light_work_pool.try_offer([this, controller, request, response, done]() {
        brpc::ClosureGuard closure_guard(done);
        auto attachment = static_cast<brpc::Controller*>(controller)->request_attachment();
        butil::IOBufAsZeroCopyInputStream zero_copy_input_stream(attachment);
        UniqueId unique_id(request->query_id());
        VLOG_NOTICE << "rpc apply_filter recv";
        Status st = _exec_env->fragment_mgr()->apply_filter(request, &zero_copy_input_stream);
        if (!st.ok()) {
            LOG(WARNING) << "apply filter meet error: " << st.to_string();
        }
        st.to_protobuf(response->mutable_status());
    });
    if (!ret) {
        offer_failed(response, done, _light_work_pool);
        return;
    }
}

void PInternalServiceImpl::apply_filterv2(::google::protobuf::RpcController* controller,
                                          const ::doris::PPublishFilterRequestV2* request,
                                          ::doris::PPublishFilterResponse* response,
                                          ::google::protobuf::Closure* done) {
    bool ret = _light_work_pool.try_offer([this, controller, request, response, done]() {
        brpc::ClosureGuard closure_guard(done);
        auto attachment = static_cast<brpc::Controller*>(controller)->request_attachment();
        butil::IOBufAsZeroCopyInputStream zero_copy_input_stream(attachment);
        UniqueId unique_id(request->query_id());
        VLOG_NOTICE << "rpc apply_filterv2 recv";
        Status st = _exec_env->fragment_mgr()->apply_filterv2(request, &zero_copy_input_stream);
        if (!st.ok()) {
            LOG(WARNING) << "apply filter meet error: " << st.to_string();
        }
        st.to_protobuf(response->mutable_status());
    });
    if (!ret) {
        offer_failed(response, done, _light_work_pool);
        return;
    }
}

void PInternalServiceImpl::send_data(google::protobuf::RpcController* controller,
                                     const PSendDataRequest* request, PSendDataResult* response,
                                     google::protobuf::Closure* done) {
    bool ret = _heavy_work_pool.try_offer([this, request, response, done]() {
        brpc::ClosureGuard closure_guard(done);
        TUniqueId load_id;
        load_id.hi = request->load_id().hi();
        load_id.lo = request->load_id().lo();
        // On 1.2.3 we add load id to send data request and using load id to get pipe
        auto stream_load_ctx = _exec_env->new_load_stream_mgr()->get(load_id);
        if (stream_load_ctx == nullptr) {
            response->mutable_status()->set_status_code(1);
            response->mutable_status()->add_error_msgs("could not find stream load context");
        } else {
            auto pipe = stream_load_ctx->pipe;
            for (int i = 0; i < request->data_size(); ++i) {
                std::unique_ptr<PDataRow> row(new PDataRow());
                row->CopyFrom(request->data(i));
                Status s = pipe->append(std::move(row));
                if (!s.ok()) {
                    response->mutable_status()->set_status_code(1);
                    response->mutable_status()->add_error_msgs(s.to_string());
                    return;
                }
            }
            response->mutable_status()->set_status_code(0);
        }
    });
    if (!ret) {
        offer_failed(response, done, _heavy_work_pool);
        return;
    }
}

void PInternalServiceImpl::commit(google::protobuf::RpcController* controller,
                                  const PCommitRequest* request, PCommitResult* response,
                                  google::protobuf::Closure* done) {
    bool ret = _light_work_pool.try_offer([this, request, response, done]() {
        brpc::ClosureGuard closure_guard(done);
        TUniqueId load_id;
        load_id.hi = request->load_id().hi();
        load_id.lo = request->load_id().lo();

        auto stream_load_ctx = _exec_env->new_load_stream_mgr()->get(load_id);
        if (stream_load_ctx == nullptr) {
            response->mutable_status()->set_status_code(1);
            response->mutable_status()->add_error_msgs("could not find stream load context");
        } else {
            static_cast<void>(stream_load_ctx->pipe->finish());
            response->mutable_status()->set_status_code(0);
        }
    });
    if (!ret) {
        offer_failed(response, done, _light_work_pool);
        return;
    }
}

void PInternalServiceImpl::rollback(google::protobuf::RpcController* controller,
                                    const PRollbackRequest* request, PRollbackResult* response,
                                    google::protobuf::Closure* done) {
    bool ret = _light_work_pool.try_offer([this, request, response, done]() {
        brpc::ClosureGuard closure_guard(done);
        TUniqueId load_id;
        load_id.hi = request->load_id().hi();
        load_id.lo = request->load_id().lo();
        auto stream_load_ctx = _exec_env->new_load_stream_mgr()->get(load_id);
        if (stream_load_ctx == nullptr) {
            response->mutable_status()->set_status_code(1);
            response->mutable_status()->add_error_msgs("could not find stream load context");
        } else {
            stream_load_ctx->pipe->cancel("rollback");
            response->mutable_status()->set_status_code(0);
        }
    });
    if (!ret) {
        offer_failed(response, done, _light_work_pool);
        return;
    }
}

void PInternalServiceImpl::fold_constant_expr(google::protobuf::RpcController* controller,
                                              const PConstantExprRequest* request,
                                              PConstantExprResult* response,
                                              google::protobuf::Closure* done) {
    bool ret = _light_work_pool.try_offer([this, request, response, done]() {
        brpc::ClosureGuard closure_guard(done);
        Status st = Status::OK();
        st = _fold_constant_expr(request->request(), response);
        if (!st.ok()) {
            LOG(WARNING) << "exec fold constant expr failed, errmsg=" << st;
        }
        st.to_protobuf(response->mutable_status());
    });
    if (!ret) {
        offer_failed(response, done, _light_work_pool);
        return;
    }
}

Status PInternalServiceImpl::_fold_constant_expr(const std::string& ser_request,
                                                 PConstantExprResult* response) {
    TFoldConstantParams t_request;
    {
        const uint8_t* buf = (const uint8_t*)ser_request.data();
        uint32_t len = ser_request.size();
        RETURN_IF_ERROR(deserialize_thrift_msg(buf, &len, false, &t_request));
    }

    return FoldConstantExecutor().fold_constant_vexpr(t_request, response);
}

void PInternalServiceImpl::transmit_block(google::protobuf::RpcController* controller,
                                          const PTransmitDataParams* request,
                                          PTransmitDataResult* response,
                                          google::protobuf::Closure* done) {
    int64_t receive_time = GetCurrentTimeNanos();
    response->set_receive_time(receive_time);

    // under high concurrency, thread pool will have a lot of lock contention.
    // May offer failed to the thread pool, so that we should avoid using thread
    // pool here.
    _transmit_block(controller, request, response, done, Status::OK());
}

void PInternalServiceImpl::transmit_block_by_http(google::protobuf::RpcController* controller,
                                                  const PEmptyRequest* request,
                                                  PTransmitDataResult* response,
                                                  google::protobuf::Closure* done) {
    bool ret = _heavy_work_pool.try_offer([this, controller, response, done]() {
        PTransmitDataParams* new_request = new PTransmitDataParams();
        google::protobuf::Closure* new_done =
                new NewHttpClosure<PTransmitDataParams>(new_request, done);
        brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
        Status st =
                attachment_extract_request_contain_block<PTransmitDataParams>(new_request, cntl);
        _transmit_block(controller, new_request, response, new_done, st);
    });
    if (!ret) {
        offer_failed(response, done, _heavy_work_pool);
        return;
    }
}

void PInternalServiceImpl::_transmit_block(google::protobuf::RpcController* controller,
                                           const PTransmitDataParams* request,
                                           PTransmitDataResult* response,
                                           google::protobuf::Closure* done,
                                           const Status& extract_st) {
    std::string query_id;
    TUniqueId finst_id;
    if (request->has_query_id()) {
        query_id = print_id(request->query_id());
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
        if (!st.ok() && !st.is<END_OF_FILE>()) {
            LOG(WARNING) << "transmit_block failed, message=" << st
                         << ", fragment_instance_id=" << print_id(request->finst_id())
                         << ", node=" << request->node_id()
                         << ", from sender_id: " << request->sender_id()
                         << ", be_number: " << request->be_number()
                         << ", packet_seq: " << request->packet_seq();
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
    bool ret = _light_work_pool.try_offer([request, response, done]() {
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
            digest.update(static_cast<const void*>(request->data().c_str()),
                          request->data().size());
            digest.digest();
            if (!iequal(digest.hex(), request->md5())) {
                std::stringstream ss;
                ss << "md5 not same, expected: " << request->md5() << ", actual: " << digest.hex();
                response->mutable_status()->add_error_msgs(ss.str());
                response->mutable_status()->set_status_code(1);
            }
        }
    });
    if (!ret) {
        offer_failed(response, done, _light_work_pool);
        return;
    }
}

void PInternalServiceImpl::reset_rpc_channel(google::protobuf::RpcController* controller,
                                             const PResetRPCChannelRequest* request,
                                             PResetRPCChannelResponse* response,
                                             google::protobuf::Closure* done) {
    bool ret = _light_work_pool.try_offer([request, response, done]() {
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
    });
    if (!ret) {
        offer_failed(response, done, _light_work_pool);
        return;
    }
}

void PInternalServiceImpl::hand_shake(google::protobuf::RpcController* controller,
                                      const PHandShakeRequest* request,
                                      PHandShakeResponse* response,
                                      google::protobuf::Closure* done) {
    bool ret = _light_work_pool.try_offer([request, response, done]() {
        brpc::ClosureGuard closure_guard(done);
        if (request->has_hello()) {
            response->set_hello(request->hello());
        }
        response->mutable_status()->set_status_code(0);
    });
    if (!ret) {
        offer_failed(response, done, _light_work_pool);
        return;
    }
}

constexpr char HttpProtocol[] = "http://";
constexpr char DownloadApiPath[] = "/api/_tablet/_download?token=";
constexpr char FileParam[] = "&file=";
constexpr auto Permissions = S_IRUSR | S_IWUSR;

std::string construct_url(const std::string& host_port, const std::string& token,
                          const std::string& path) {
    return fmt::format("{}{}{}{}{}{}", HttpProtocol, host_port, DownloadApiPath, token, FileParam,
                       path);
}

std::string construct_file_path(const std::string& tablet_path, const std::string& rowset_id,
                                int64_t segment) {
    return fmt::format("{}/{}_{}.dat", tablet_path, rowset_id, segment);
}

static Status download_file_action(std::string& remote_file_url, std::string& local_file_path,
                                   uint64_t estimate_timeout, uint64_t file_size) {
    auto download_cb = [remote_file_url, estimate_timeout, local_file_path,
                        file_size](HttpClient* client) {
        RETURN_IF_ERROR(client->init(remote_file_url));
        client->set_timeout_ms(estimate_timeout * 1000);
        RETURN_IF_ERROR(client->download(local_file_path));

        if (file_size > 0) {
            // Check file length
            uint64_t local_file_size = std::filesystem::file_size(local_file_path);
            if (local_file_size != file_size) {
                LOG(WARNING) << "failed to pull rowset for slave replica. download file "
                                "length error"
                             << ", remote_path=" << remote_file_url << ", file_size=" << file_size
                             << ", local_file_size=" << local_file_size;
                return Status::InternalError("downloaded file size is not equal");
            }
        }
        chmod(local_file_path.c_str(), Permissions);
        return Status::OK();
    };
    return HttpClient::execute_with_retry(DOWNLOAD_FILE_MAX_RETRY, 1, download_cb);
}

void PInternalServiceImpl::request_slave_tablet_pull_rowset(
        google::protobuf::RpcController* controller, const PTabletWriteSlaveRequest* request,
        PTabletWriteSlaveResult* response, google::protobuf::Closure* done) {
    brpc::ClosureGuard closure_guard(done);
    RowsetMetaPB rowset_meta_pb = request->rowset_meta();
    std::string rowset_path = request->rowset_path();
    google::protobuf::Map<int64, int64> segments_size = request->segments_size();
    google::protobuf::Map<int64, PTabletWriteSlaveRequest_IndexSizeMap> indices_size =
            request->inverted_indices_size();
    std::string host = request->host();
    int64_t http_port = request->http_port();
    int64_t brpc_port = request->brpc_port();
    std::string token = request->token();
    int64_t node_id = request->node_id();
    bool ret = _heavy_work_pool.try_offer([rowset_meta_pb, host, brpc_port, node_id, segments_size,
                                           indices_size, http_port, token, rowset_path, this]() {
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

            std::string remote_file_path =
                    construct_file_path(rowset_path, remote_rowset_id.to_string(), segment.first);
            std::string remote_file_url =
                    construct_url(get_host_port(host, http_port), token, remote_file_path);

            std::string local_file_path = construct_file_path(
                    tablet->tablet_path(), rowset_meta->rowset_id().to_string(), segment.first);

            auto st = download_file_action(remote_file_url, local_file_path, estimate_timeout,
                                           file_size);
            if (!st.ok()) {
                LOG(WARNING) << "failed to pull rowset for slave replica. failed to download "
                                "file. url="
                             << remote_file_url << ", local_path=" << local_file_path
                             << ", txn_id=" << rowset_meta->txn_id();
                _response_pull_slave_rowset(host, brpc_port, rowset_meta->txn_id(),
                                            rowset_meta->tablet_id(), node_id, false);
                return;
            }
            VLOG_CRITICAL << "succeed to download file for slave replica. url=" << remote_file_url
                          << ", local_path=" << local_file_path
                          << ", txn_id=" << rowset_meta->txn_id();
            if (indices_size.find(segment.first) != indices_size.end()) {
                PTabletWriteSlaveRequest_IndexSizeMap segment_indices_size =
                        indices_size.at(segment.first);

                for (auto index_size : segment_indices_size.index_sizes()) {
                    auto index_id = index_size.indexid();
                    auto size = index_size.size();
                    std::string remote_inverted_index_file =
                            InvertedIndexDescriptor::get_index_file_name(remote_file_path,
                                                                         index_id);
                    std::string remote_inverted_index_file_url = construct_url(
                            get_host_port(host, http_port), token, remote_inverted_index_file);

                    std::string local_inverted_index_file =
                            InvertedIndexDescriptor::get_index_file_name(local_file_path, index_id);
                    st = download_file_action(remote_inverted_index_file_url,
                                              local_inverted_index_file, estimate_timeout, size);
                    if (!st.ok()) {
                        LOG(WARNING) << "failed to pull rowset for slave replica. failed to "
                                        "download "
                                        "file. url="
                                     << remote_inverted_index_file_url
                                     << ", local_path=" << local_inverted_index_file
                                     << ", txn_id=" << rowset_meta->txn_id();
                        _response_pull_slave_rowset(host, brpc_port, rowset_meta->txn_id(),
                                                    rowset_meta->tablet_id(), node_id, false);
                        return;
                    }
                    VLOG_CRITICAL
                            << "succeed to download inverted index file for slave replica. url="
                            << remote_inverted_index_file_url
                            << ", local_path=" << local_inverted_index_file
                            << ", txn_id=" << rowset_meta->txn_id();
                }
            }
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
                rowset_meta->tablet_id(), tablet->tablet_uid(), rowset_meta->load_id(), rowset,
                false);
        if (!commit_txn_status && !commit_txn_status.is<PUSH_TRANSACTION_ALREADY_EXIST>()) {
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
    if (!ret) {
        offer_failed(response, closure_guard.release(), _heavy_work_pool);
        return;
    }
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
    bool ret = _heavy_work_pool.try_offer([request, response, done]() {
        brpc::ClosureGuard closure_guard(done);
        VLOG_CRITICAL << "receive the result of slave replica pull rowset from slave replica. "
                         "slave server="
                      << request->node_id() << ", is_succeed=" << request->is_succeed()
                      << ", tablet_id=" << request->tablet_id() << ", txn_id=" << request->txn_id();
        StorageEngine::instance()->txn_manager()->finish_slave_tablet_pull_rowset(
                request->txn_id(), request->tablet_id(), request->node_id(), request->is_succeed());
        Status::OK().to_protobuf(response->mutable_status());
    });
    if (!ret) {
        offer_failed(response, done, _heavy_work_pool);
        return;
    }
}

template <typename Func>
auto scope_timer_run(Func fn, int64_t* cost) -> decltype(fn()) {
    MonotonicStopWatch watch;
    watch.start();
    auto res = fn();
    *cost += watch.elapsed_time() / 1000 / 1000;
    return res;
}

Status PInternalServiceImpl::_multi_get(const PMultiGetRequest& request,
                                        PMultiGetResponse* response) {
    OlapReaderStatistics stats;
    vectorized::Block result_block;
    int64_t acquire_tablet_ms = 0;
    int64_t acquire_rowsets_ms = 0;
    int64_t acquire_segments_ms = 0;
    int64_t lookup_row_data_ms = 0;

    // init desc
    TupleDescriptor desc(request.desc());
    std::vector<SlotDescriptor> slots;
    slots.reserve(request.slots().size());
    for (const auto& pslot : request.slots()) {
        slots.push_back(SlotDescriptor(pslot));
        desc.add_slot(&slots.back());
    }

    // init read schema
    TabletSchema full_read_schema;
    for (const ColumnPB& column_pb : request.column_desc()) {
        full_read_schema.append_column(TabletColumn(column_pb));
    }

    // read row by row
    for (size_t i = 0; i < request.row_locs_size(); ++i) {
        const auto& row_loc = request.row_locs(i);
        MonotonicStopWatch watch;
        watch.start();
        TabletSharedPtr tablet = scope_timer_run(
                [&]() {
                    return StorageEngine::instance()->tablet_manager()->get_tablet(
                            row_loc.tablet_id(), true /*include deleted*/);
                },
                &acquire_tablet_ms);
        RowsetId rowset_id;
        rowset_id.init(row_loc.rowset_id());
        if (!tablet) {
            continue;
        }
        // We ensured it's rowset is not released when init Tablet reader param, rowset->update_delayed_expired_timestamp();
        BetaRowsetSharedPtr rowset = std::static_pointer_cast<BetaRowset>(scope_timer_run(
                [&]() { return StorageEngine::instance()->get_quering_rowset(rowset_id); },
                &acquire_rowsets_ms));
        if (!rowset) {
            LOG(INFO) << "no such rowset " << rowset_id;
            continue;
        }
        size_t row_size = 0;
        Defer _defer([&]() {
            LOG_EVERY_N(INFO, 100)
                    << "multiget_data single_row, cost(us):" << watch.elapsed_time() / 1000
                    << ", row_size:" << row_size;
            *response->add_row_locs() = row_loc;
        });
        SegmentCacheHandle segment_cache;
        RETURN_IF_ERROR(scope_timer_run(
                [&]() {
                    return SegmentLoader::instance()->load_segments(rowset, &segment_cache, true);
                },
                &acquire_segments_ms));
        // find segment
        auto it = std::find_if(segment_cache.get_segments().begin(),
                               segment_cache.get_segments().end(),
                               [&row_loc](const segment_v2::SegmentSharedPtr& seg) {
                                   return seg->id() == row_loc.segment_id();
                               });
        if (it == segment_cache.get_segments().end()) {
            continue;
        }
        segment_v2::SegmentSharedPtr segment = *it;
        GlobalRowLoacation row_location(row_loc.tablet_id(), rowset->rowset_id(),
                                        row_loc.segment_id(), row_loc.ordinal_id());
        // fetch by row store, more effcient way
        if (request.fetch_row_store()) {
            CHECK(tablet->tablet_schema()->store_row_column());
            RowLocation loc(rowset_id, segment->id(), row_loc.ordinal_id());
            string* value = response->add_binary_row_data();
            RETURN_IF_ERROR(scope_timer_run(
                    [&]() {
                        return tablet->lookup_row_data({}, loc, rowset, &desc, stats, *value);
                    },
                    &lookup_row_data_ms));
            row_size = value->size();
            continue;
        }

        // fetch by column store
        if (result_block.is_empty_column()) {
            result_block = vectorized::Block(desc.slots(), request.row_locs().size());
        }
        for (int x = 0; x < desc.slots().size(); ++x) {
            int index = -1;
            if (desc.slots()[x]->col_unique_id() >= 0) {
                // light sc enabled
                index = full_read_schema.field_index(desc.slots()[x]->col_unique_id());
            } else {
                index = full_read_schema.field_index(desc.slots()[x]->col_name());
            }
            if (index < 0) {
                std::stringstream ss;
                ss << "field name is invalid. field=" << desc.slots()[x]->col_name()
                   << ", field_name_to_index=" << full_read_schema.get_all_field_names();
                return Status::InternalError(ss.str());
            }
            std::unique_ptr<segment_v2::ColumnIterator> column_iterator;
            vectorized::MutableColumnPtr column =
                    result_block.get_by_position(x).column->assume_mutable();
            RETURN_IF_ERROR(
                    segment->new_column_iterator(full_read_schema.column(index), &column_iterator));
            segment_v2::ColumnIteratorOptions opt {
                    .use_page_cache = !config::disable_storage_page_cache,
                    .file_reader = segment->file_reader().get(),
                    .stats = &stats,
                    .io_ctx = io::IOContext {.reader_type = ReaderType::READER_QUERY},
            };
            static_cast<void>(column_iterator->init(opt));
            std::vector<segment_v2::rowid_t> single_row_loc {
                    static_cast<segment_v2::rowid_t>(row_loc.ordinal_id())};
            RETURN_IF_ERROR(column_iterator->read_by_rowids(single_row_loc.data(), 1, column));
        }
    }
    // serialize block if not empty
    if (!result_block.is_empty_column()) {
        VLOG_DEBUG << "dump block:" << result_block.dump_data(0, 10)
                   << ", be_exec_version:" << request.be_exec_version();
        [[maybe_unused]] size_t compressed_size = 0;
        [[maybe_unused]] size_t uncompressed_size = 0;
        int be_exec_version = request.has_be_exec_version() ? request.be_exec_version() : 0;
        RETURN_IF_ERROR(result_block.serialize(be_exec_version, response->mutable_block(),
                                               &uncompressed_size, &compressed_size,
                                               segment_v2::CompressionTypePB::LZ4));
    }

    LOG(INFO) << "Query stats: "
              << fmt::format(
                         "hit_cached_pages:{}, total_pages_read:{}, compressed_bytes_read:{}, "
                         "io_latency:{}ns, "
                         "uncompressed_bytes_read:{},"
                         "acquire_tablet_ms:{}, acquire_rowsets_ms:{}, acquire_segments_ms:{}, "
                         "lookup_row_data_ms:{}",
                         stats.cached_pages_num, stats.total_pages_num, stats.compressed_bytes_read,
                         stats.io_ns, stats.uncompressed_bytes_read, acquire_tablet_ms,
                         acquire_rowsets_ms, acquire_segments_ms, lookup_row_data_ms);
    return Status::OK();
}

void PInternalServiceImpl::multiget_data(google::protobuf::RpcController* controller,
                                         const PMultiGetRequest* request,
                                         PMultiGetResponse* response,
                                         google::protobuf::Closure* done) {
    bool ret = _light_work_pool.try_offer([request, response, done, this]() {
        // multi get data by rowid
        MonotonicStopWatch watch;
        watch.start();
        brpc::ClosureGuard closure_guard(done);
        response->mutable_status()->set_status_code(0);
        Status st = _multi_get(*request, response);
        st.to_protobuf(response->mutable_status());
        LOG(INFO) << "multiget_data finished, cost(us):" << watch.elapsed_time() / 1000;
    });
    if (!ret) {
        offer_failed(response, done, _heavy_work_pool);
        return;
    }
}

void PInternalServiceImpl::get_tablet_rowset_versions(google::protobuf::RpcController* cntl_base,
                                                      const PGetTabletVersionsRequest* request,
                                                      PGetTabletVersionsResponse* response,
                                                      google::protobuf::Closure* done) {
    brpc::ClosureGuard closure_guard(done);
    VLOG_DEBUG << "receive get tablet versions request: " << request->DebugString();
    StorageEngine::instance()->get_tablet_rowset_versions(request, response);
}

void PInternalServiceImpl::glob(google::protobuf::RpcController* controller,
                                const PGlobRequest* request, PGlobResponse* response,
                                google::protobuf::Closure* done) {
    bool ret = _heavy_work_pool.try_offer([request, response, done]() {
        brpc::ClosureGuard closure_guard(done);
        std::vector<io::FileInfo> files;
        Status st = io::global_local_filesystem()->safe_glob(request->pattern(), &files);
        if (st.ok()) {
            for (auto& file : files) {
                PGlobResponse_PFileInfo* pfile = response->add_files();
                pfile->set_file(file.file_name);
                pfile->set_size(file.file_size);
            }
        }
        st.to_protobuf(response->mutable_status());
    });
    if (!ret) {
        offer_failed(response, done, _heavy_work_pool);
        return;
    }
}

void PInternalServiceImpl::group_commit_insert(google::protobuf::RpcController* controller,
                                               const PGroupCommitInsertRequest* request,
                                               PGroupCommitInsertResponse* response,
                                               google::protobuf::Closure* done) {
    TUniqueId load_id;
    load_id.__set_hi(request->load_id().hi());
    load_id.__set_lo(request->load_id().lo());
    bool ret = _light_work_pool.try_offer([this, request, response, done, load_id]() {
        brpc::ClosureGuard closure_guard(done);
        std::shared_ptr<StreamLoadContext> ctx = std::make_shared<StreamLoadContext>(_exec_env);
        auto pipe = std::make_shared<io::StreamLoadPipe>(
                io::kMaxPipeBufferedBytes /* max_buffered_bytes */, 64 * 1024 /* min_chunk_size */,
                -1 /* total_length */, true /* use_proto */);
        ctx->pipe = pipe;
        Status st = _exec_env->new_load_stream_mgr()->put(load_id, ctx);
        if (st.ok()) {
            doris::Mutex mutex;
            doris::ConditionVariable cv;
            bool handled = false;
            try {
                st = _exec_plan_fragment_impl(
                        request->exec_plan_fragment_request().request(),
                        request->exec_plan_fragment_request().version(),
                        request->exec_plan_fragment_request().compact(),
                        [&](RuntimeState* state, Status* status) {
                            response->set_label(state->import_label());
                            response->set_txn_id(state->wal_id());
                            response->set_loaded_rows(state->num_rows_load_success());
                            response->set_filtered_rows(state->num_rows_load_filtered());
                            st = *status;
                            std::unique_lock l(mutex);
                            handled = true;
                            cv.notify_one();
                        });
            } catch (const Exception& e) {
                st = e.to_status();
            } catch (...) {
                st = Status::Error(ErrorCode::INTERNAL_ERROR,
                                   "_exec_plan_fragment_impl meet unknown error");
            }
            if (!st.ok()) {
                LOG(WARNING) << "exec plan fragment failed, errmsg=" << st;
            } else {
                for (int i = 0; i < request->data().size(); ++i) {
                    std::unique_ptr<PDataRow> row(new PDataRow());
                    row->CopyFrom(request->data(i));
                    st = pipe->append(std::move(row));
                    if (!st.ok()) {
                        break;
                    }
                }
                if (st.ok()) {
                    static_cast<void>(pipe->finish());
                    std::unique_lock l(mutex);
                    if (!handled) {
                        cv.wait(l);
                    }
                }
            }
        }
        st.to_protobuf(response->mutable_status());
    });
    _exec_env->new_load_stream_mgr()->remove(load_id);
    if (!ret) {
        offer_failed(response, done, _light_work_pool);
        return;
    }
};

} // namespace doris
