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
#include <fmt/core.h>
#include <gen_cpp/DataSinks_types.h>
#include <gen_cpp/MasterService_types.h>
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
#include <vec/exec/vjdbc_connector.h>

#include <algorithm>
#include <exception>
#include <filesystem>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/consts.h"
#include "common/exception.h"
#include "common/logging.h"
#include "common/signal_handler.h"
#include "common/status.h"
#include "exec/rowid_fetcher.h"
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
#include "olap/olap_define.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/rowset/segment_v2/column_reader.h"
#include "olap/rowset/segment_v2/common.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/rowset/segment_v2/segment_iterator.h"
#include "olap/segment_loader.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "olap/tablet_fwd.h"
#include "olap/tablet_manager.h"
#include "olap/tablet_schema.h"
#include "olap/txn_manager.h"
#include "olap/utils.h"
#include "olap/wal/wal_manager.h"
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
#include "service/backend_options.h"
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
#include "util/thrift_util.h"
#include "util/time.h"
#include "util/uid_util.h"
#include "vec/columns/column.h"
#include "vec/columns/column_string.h"
#include "vec/common/schema_util.h"
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

PInternalService::PInternalService(ExecEnv* exec_env)
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
                           "brpc_light") {
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

    _exec_env->load_stream_mgr()->set_heavy_work_pool(&_heavy_work_pool);
    _exec_env->load_stream_mgr()->set_light_work_pool(&_light_work_pool);

    CHECK_EQ(0, bthread_key_create(&btls_key, thread_context_deleter));
    CHECK_EQ(0, bthread_key_create(&AsyncIO::btls_io_ctx_key, AsyncIO::io_ctx_key_deleter));
}

PInternalServiceImpl::PInternalServiceImpl(StorageEngine& engine, ExecEnv* exec_env)
        : PInternalService(exec_env), _engine(engine) {}

PInternalServiceImpl::~PInternalServiceImpl() = default;

PInternalService::~PInternalService() {
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

void PInternalService::transmit_data(google::protobuf::RpcController* controller,
                                     const PTransmitDataParams* request,
                                     PTransmitDataResult* response,
                                     google::protobuf::Closure* done) {}

void PInternalService::transmit_data_by_http(google::protobuf::RpcController* controller,
                                             const PEmptyRequest* request,
                                             PTransmitDataResult* response,
                                             google::protobuf::Closure* done) {}

void PInternalService::_transmit_data(google::protobuf::RpcController* controller,
                                      const PTransmitDataParams* request,
                                      PTransmitDataResult* response,
                                      google::protobuf::Closure* done, const Status& extract_st) {}

void PInternalService::tablet_writer_open(google::protobuf::RpcController* controller,
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

void PInternalService::exec_plan_fragment(google::protobuf::RpcController* controller,
                                          const PExecPlanFragmentRequest* request,
                                          PExecPlanFragmentResult* response,
                                          google::protobuf::Closure* done) {
    timeval tv {};
    gettimeofday(&tv, nullptr);
    response->set_received_time(tv.tv_sec * 1000LL + tv.tv_usec / 1000);
    bool ret = _light_work_pool.try_offer([this, controller, request, response, done]() {
        _exec_plan_fragment_in_pthread(controller, request, response, done);
    });
    if (!ret) {
        offer_failed(response, done, _light_work_pool);
        return;
    }
}

void PInternalService::_exec_plan_fragment_in_pthread(google::protobuf::RpcController* controller,
                                                      const PExecPlanFragmentRequest* request,
                                                      PExecPlanFragmentResult* response,
                                                      google::protobuf::Closure* done) {
    timeval tv1 {};
    gettimeofday(&tv1, nullptr);
    response->set_execution_time(tv1.tv_sec * 1000LL + tv1.tv_usec / 1000);
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
    timeval tv2 {};
    gettimeofday(&tv2, nullptr);
    response->set_execution_done_time(tv2.tv_sec * 1000LL + tv2.tv_usec / 1000);
}

void PInternalService::exec_plan_fragment_prepare(google::protobuf::RpcController* controller,
                                                  const PExecPlanFragmentRequest* request,
                                                  PExecPlanFragmentResult* response,
                                                  google::protobuf::Closure* done) {
    timeval tv {};
    gettimeofday(&tv, nullptr);
    response->set_received_time(tv.tv_sec * 1000LL + tv.tv_usec / 1000);
    bool ret = _light_work_pool.try_offer([this, controller, request, response, done]() {
        _exec_plan_fragment_in_pthread(controller, request, response, done);
    });
    if (!ret) {
        offer_failed(response, done, _light_work_pool);
        return;
    }
}

void PInternalService::exec_plan_fragment_start(google::protobuf::RpcController* /*controller*/,
                                                const PExecPlanFragmentStartRequest* request,
                                                PExecPlanFragmentResult* result,
                                                google::protobuf::Closure* done) {
    timeval tv {};
    gettimeofday(&tv, nullptr);
    result->set_received_time(tv.tv_sec * 1000LL + tv.tv_usec / 1000);
    bool ret = _light_work_pool.try_offer([this, request, result, done]() {
        timeval tv1 {};
        gettimeofday(&tv1, nullptr);
        result->set_execution_time(tv1.tv_sec * 1000LL + tv1.tv_usec / 1000);
        brpc::ClosureGuard closure_guard(done);
        auto st = _exec_env->fragment_mgr()->start_query_execution(request);
        st.to_protobuf(result->mutable_status());
        timeval tv2 {};
        gettimeofday(&tv2, nullptr);
        result->set_execution_done_time(tv2.tv_sec * 1000LL + tv2.tv_usec / 1000);
    });
    if (!ret) {
        offer_failed(result, done, _light_work_pool);
        return;
    }
}

void PInternalService::open_load_stream(google::protobuf::RpcController* controller,
                                        const POpenLoadStreamRequest* request,
                                        POpenLoadStreamResponse* response,
                                        google::protobuf::Closure* done) {
    bool ret = _light_work_pool.try_offer([this, controller, request, response, done]() {
        signal::set_signal_task_id(request->load_id());
        brpc::ClosureGuard done_guard(done);
        brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
        brpc::StreamOptions stream_options;

        LOG(INFO) << "open load stream, load_id=" << request->load_id()
                  << ", src_id=" << request->src_id();

        for (const auto& req : request->tablets()) {
            BaseTabletSPtr tablet;
            if (auto res = ExecEnv::get_tablet(req.tablet_id()); !res.has_value()) [[unlikely]] {
                auto st = std::move(res).error();
                st.to_protobuf(response->mutable_status());
                cntl->SetFailed(st.to_string());
                return;
            } else {
                tablet = std::move(res).value();
            }
            auto resp = response->add_tablet_schemas();
            resp->set_index_id(req.index_id());
            resp->set_enable_unique_key_merge_on_write(tablet->enable_unique_key_merge_on_write());
            tablet->tablet_schema()->to_schema_pb(resp->mutable_tablet_schema());
        }

        LoadStream* load_stream = nullptr;
        auto st = _exec_env->load_stream_mgr()->open_load_stream(request, load_stream);
        if (!st.ok()) {
            st.to_protobuf(response->mutable_status());
            return;
        }

        stream_options.handler = load_stream;
        stream_options.idle_timeout_ms = request->idle_timeout_ms();
        DBUG_EXECUTE_IF("PInternalServiceImpl.open_load_stream.set_idle_timeout",
                        { stream_options.idle_timeout_ms = 1; });

        StreamId streamid;
        if (brpc::StreamAccept(&streamid, *cntl, &stream_options) != 0) {
            st = Status::Cancelled("Fail to accept stream {}", streamid);
            st.to_protobuf(response->mutable_status());
            cntl->SetFailed(st.to_string());
            return;
        }

        VLOG_DEBUG << "get streamid =" << streamid;
        st.to_protobuf(response->mutable_status());
    });
    if (!ret) {
        offer_failed(response, done, _light_work_pool);
    }
}

void PInternalService::tablet_writer_add_block_by_http(google::protobuf::RpcController* controller,
                                                       const ::doris::PEmptyRequest* request,
                                                       PTabletWriterAddBlockResult* response,
                                                       google::protobuf::Closure* done) {
    PTabletWriterAddBlockRequest* new_request = new PTabletWriterAddBlockRequest();
    google::protobuf::Closure* new_done =
            new NewHttpClosure<PTabletWriterAddBlockRequest>(new_request, done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    Status st = attachment_extract_request_contain_block<PTabletWriterAddBlockRequest>(new_request,
                                                                                       cntl);
    if (st.ok()) {
        tablet_writer_add_block(controller, new_request, response, new_done);
    } else {
        st.to_protobuf(response->mutable_status());
    }
}

void PInternalService::tablet_writer_add_block(google::protobuf::RpcController* controller,
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

void PInternalService::tablet_writer_cancel(google::protobuf::RpcController* controller,
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

Status PInternalService::_exec_plan_fragment_impl(
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
            return _exec_env->fragment_mgr()->exec_plan_fragment(
                    t_request, QuerySource::INTERNAL_FRONTEND, cb);
        } else {
            return _exec_env->fragment_mgr()->exec_plan_fragment(t_request,
                                                                 QuerySource::INTERNAL_FRONTEND);
        }
    } else if (version == PFragmentRequestVersion::VERSION_2) {
        TExecPlanFragmentParamsList t_request;
        {
            const uint8_t* buf = (const uint8_t*)ser_request.data();
            uint32_t len = ser_request.size();
            RETURN_IF_ERROR(deserialize_thrift_msg(buf, &len, compact, &t_request));
        }
        const auto& fragment_list = t_request.paramsList;
        MonotonicStopWatch timer;
        timer.start();

        for (const TExecPlanFragmentParams& params : t_request.paramsList) {
            if (cb) {
                RETURN_IF_ERROR(_exec_env->fragment_mgr()->exec_plan_fragment(
                        params, QuerySource::INTERNAL_FRONTEND, cb));
            } else {
                RETURN_IF_ERROR(_exec_env->fragment_mgr()->exec_plan_fragment(
                        params, QuerySource::INTERNAL_FRONTEND));
            }
        }

        timer.stop();
        double cost_secs = static_cast<double>(timer.elapsed_time()) / 1000000000ULL;
        if (cost_secs > 5) {
            LOG_WARNING("Prepare {} fragments of query {} costs {} seconds, it costs too much",
                        fragment_list.size(), print_id(fragment_list.front().params.query_id),
                        cost_secs);
        }

        return Status::OK();
    } else if (version == PFragmentRequestVersion::VERSION_3) {
        TPipelineFragmentParamsList t_request;
        {
            const uint8_t* buf = (const uint8_t*)ser_request.data();
            uint32_t len = ser_request.size();
            RETURN_IF_ERROR(deserialize_thrift_msg(buf, &len, compact, &t_request));
        }

        const auto& fragment_list = t_request.params_list;
        if (fragment_list.empty()) {
            return Status::InternalError("Invalid TPipelineFragmentParamsList!");
        }
        MonotonicStopWatch timer;
        timer.start();
        for (const TPipelineFragmentParams& fragment : fragment_list) {
            if (cb) {
                RETURN_IF_ERROR(_exec_env->fragment_mgr()->exec_plan_fragment(
                        fragment, QuerySource::INTERNAL_FRONTEND, cb));
            } else {
                RETURN_IF_ERROR(_exec_env->fragment_mgr()->exec_plan_fragment(
                        fragment, QuerySource::INTERNAL_FRONTEND));
            }
        }
        timer.stop();
        double cost_secs = static_cast<double>(timer.elapsed_time()) / 1000000000ULL;
        if (cost_secs > 5) {
            LOG_WARNING("Prepare {} fragments of query {} costs {} seconds, it costs too much",
                        fragment_list.size(), print_id(fragment_list.front().query_id), cost_secs);
        }

        return Status::OK();
    } else {
        return Status::InternalError("invalid version");
    }
}

void PInternalService::cancel_plan_fragment(google::protobuf::RpcController* /*controller*/,
                                            const PCancelPlanFragmentRequest* request,
                                            PCancelPlanFragmentResult* result,
                                            google::protobuf::Closure* done) {
    bool ret = _light_work_pool.try_offer([this, request, result, done]() {
        brpc::ClosureGuard closure_guard(done);
        TUniqueId tid;
        tid.__set_hi(request->finst_id().hi());
        tid.__set_lo(request->finst_id().lo());
        signal::set_signal_task_id(tid);
        Status st = Status::OK();

        const bool has_cancel_reason = request->has_cancel_reason();
        const bool has_cancel_status = request->has_cancel_status();
        // During upgrade only LIMIT_REACH is used, other reason is changed to internal error
        Status actual_cancel_status = Status::OK();
        // Convert PPlanFragmentCancelReason to Status
        if (has_cancel_status) {
            // If fe set cancel status, then it is new FE now, should use cancel status.
            actual_cancel_status = Status::create(request->cancel_status());
        } else if (has_cancel_reason) {
            // If fe not set cancel status, but set cancel reason, should convert cancel reason
            // to cancel status here.
            if (request->cancel_reason() == PPlanFragmentCancelReason::LIMIT_REACH) {
                actual_cancel_status = Status::Error<ErrorCode::LIMIT_REACH>("limit reach");
            } else {
                // Use cancel reason as error message
                actual_cancel_status = Status::InternalError(
                        PPlanFragmentCancelReason_Name(request->cancel_reason()));
            }
        } else {
            actual_cancel_status = Status::InternalError("unknown error");
        }

        TUniqueId query_id;
        query_id.__set_hi(request->query_id().hi());
        query_id.__set_lo(request->query_id().lo());
        LOG(INFO) << fmt::format("Cancel query {}, reason: {}", print_id(query_id),
                                 actual_cancel_status.to_string());
        _exec_env->fragment_mgr()->cancel_query(query_id, actual_cancel_status);

        // TODO: the logic seems useless, cancel only return Status::OK. remove it
        st.to_protobuf(result->mutable_status());
    });
    if (!ret) {
        offer_failed(result, done, _light_work_pool);
        return;
    }
}

void PInternalService::fetch_data(google::protobuf::RpcController* controller,
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

void PInternalService::outfile_write_success(google::protobuf::RpcController* controller,
                                             const POutfileWriteSuccessRequest* request,
                                             POutfileWriteSuccessResult* result,
                                             google::protobuf::Closure* done) {
    bool ret = _heavy_work_pool.try_offer([request, result, done]() {
        VLOG_RPC << "outfile write success file";
        brpc::ClosureGuard closure_guard(done);
        TResultFileSink result_file_sink;
        Status st = Status::OK();
        {
            const uint8_t* buf = (const uint8_t*)(request->result_file_sink().data());
            uint32_t len = request->result_file_sink().size();
            st = deserialize_thrift_msg(buf, &len, false, &result_file_sink);
            if (!st.ok()) {
                LOG(WARNING) << "outfile write success file failed, errmsg = " << st;
                st.to_protobuf(result->mutable_status());
                return;
            }
        }

        TResultFileSinkOptions file_options = result_file_sink.file_options;
        std::stringstream ss;
        ss << file_options.file_path << file_options.success_file_name;
        std::string file_name = ss.str();
        if (result_file_sink.storage_backend_type == TStorageBackendType::LOCAL) {
            // For local file writer, the file_path is a local dir.
            // Here we do a simple security verification by checking whether the file exists.
            // Because the file path is currently arbitrarily specified by the user,
            // Doris is not responsible for ensuring the correctness of the path.
            // This is just to prevent overwriting the existing file.
            bool exists = true;
            st = io::global_local_filesystem()->exists(file_name, &exists);
            if (!st.ok()) {
                LOG(WARNING) << "outfile write success filefailed, errmsg = " << st;
                st.to_protobuf(result->mutable_status());
                return;
            }
            if (exists) {
                st = Status::InternalError("File already exists: {}", file_name);
            }
            if (!st.ok()) {
                LOG(WARNING) << "outfile write success file failed, errmsg = " << st;
                st.to_protobuf(result->mutable_status());
                return;
            }
        }

        auto&& res = FileFactory::create_file_writer(
                FileFactory::convert_storage_type(result_file_sink.storage_backend_type),
                ExecEnv::GetInstance(), file_options.broker_addresses,
                file_options.broker_properties, file_name,
                {
                        .write_file_cache = false,
                        .sync_file_data = false,
                });
        using T = std::decay_t<decltype(res)>;
        if (!res.has_value()) [[unlikely]] {
            st = std::forward<T>(res).error();
            st.to_protobuf(result->mutable_status());
            return;
        }

        std::unique_ptr<doris::io::FileWriter> _file_writer_impl = std::forward<T>(res).value();
        // must write somthing because s3 file writer can not writer empty file
        st = _file_writer_impl->append({"success"});
        if (!st.ok()) {
            LOG(WARNING) << "outfile write success filefailed, errmsg=" << st;
            st.to_protobuf(result->mutable_status());
            return;
        }
        st = _file_writer_impl->close();
        if (!st.ok()) {
            LOG(WARNING) << "outfile write success filefailed, errmsg=" << st;
            st.to_protobuf(result->mutable_status());
            return;
        }
    });
    if (!ret) {
        offer_failed(result, done, _heavy_work_pool);
        return;
    }
}

void PInternalService::fetch_table_schema(google::protobuf::RpcController* controller,
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

        std::shared_ptr<MemTrackerLimiter> mem_tracker = MemTrackerLimiter::create_shared(
                MemTrackerLimiter::Type::OTHER,
                fmt::format("InternalService::fetch_table_schema:{}#{}", params.format_type,
                            params.file_type));
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(mem_tracker);

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
            st = ((vectorized::AvroJNIReader*)(reader.get()))->init_fetch_table_schema_reader();
            break;
        }
        default:
            st = Status::InternalError("Not supported file format in fetch table schema: {}",
                                       params.format_type);
            st.to_protobuf(result->mutable_status());
            return;
        }
        if (!st.ok()) {
            LOG(WARNING) << "failed to init reader, errmsg=" << st;
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

void PInternalService::fetch_arrow_flight_schema(google::protobuf::RpcController* controller,
                                                 const PFetchArrowFlightSchemaRequest* request,
                                                 PFetchArrowFlightSchemaResult* result,
                                                 google::protobuf::Closure* done) {
    bool ret = _light_work_pool.try_offer([request, result, done]() {
        brpc::ClosureGuard closure_guard(done);
        std::shared_ptr<arrow::Schema> schema =
                ExecEnv::GetInstance()->result_mgr()->find_arrow_schema(
                        UniqueId(request->finst_id()).to_thrift());
        if (schema == nullptr) {
            LOG(INFO) << "FE not found arrow flight schema, maybe query has been canceled";
            auto st = Status::NotFound(
                    "FE not found arrow flight schema, maybe query has been canceled");
            st.to_protobuf(result->mutable_status());
            return;
        }

        std::string schema_str;
        auto st = serialize_arrow_schema(&schema, &schema_str);
        if (st.ok()) {
            result->set_schema(std::move(schema_str));
            if (config::public_access_ip != "") {
                result->set_be_arrow_flight_ip(config::public_access_ip);
            }
        }
        st.to_protobuf(result->mutable_status());
    });
    if (!ret) {
        offer_failed(result, done, _heavy_work_pool);
        return;
    }
}

Status PInternalService::_tablet_fetch_data(const PTabletKeyLookupRequest* request,
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

void PInternalService::tablet_fetch_data(google::protobuf::RpcController* controller,
                                         const PTabletKeyLookupRequest* request,
                                         PTabletKeyLookupResponse* response,
                                         google::protobuf::Closure* done) {
    bool ret = _light_work_pool.try_offer([this, controller, request, response, done]() {
        [[maybe_unused]] auto* cntl = static_cast<brpc::Controller*>(controller);
        brpc::ClosureGuard guard(done);
        Status st = _tablet_fetch_data(request, response);
        st.to_protobuf(response->mutable_status());
    });
    if (!ret) {
        offer_failed(response, done, _light_work_pool);
        return;
    }
}

void PInternalService::test_jdbc_connection(google::protobuf::RpcController* controller,
                                            const PJdbcTestConnectionRequest* request,
                                            PJdbcTestConnectionResult* result,
                                            google::protobuf::Closure* done) {
    bool ret = _heavy_work_pool.try_offer([request, result, done]() {
        VLOG_RPC << "test jdbc connection";
        brpc::ClosureGuard closure_guard(done);
        TTableDescriptor table_desc;
        vectorized::JdbcConnectorParam jdbc_param;
        Status st = Status::OK();
        {
            const uint8_t* buf = (const uint8_t*)request->jdbc_table().data();
            uint32_t len = request->jdbc_table().size();
            st = deserialize_thrift_msg(buf, &len, false, &table_desc);
            if (!st.ok()) {
                LOG(WARNING) << "test jdbc connection failed, errmsg=" << st;
                st.to_protobuf(result->mutable_status());
                return;
            }
        }
        TJdbcTable jdbc_table = (table_desc.jdbcTable);
        jdbc_param.catalog_id = jdbc_table.catalog_id;
        jdbc_param.driver_class = jdbc_table.jdbc_driver_class;
        jdbc_param.driver_path = jdbc_table.jdbc_driver_url;
        jdbc_param.driver_checksum = jdbc_table.jdbc_driver_checksum;
        jdbc_param.jdbc_url = jdbc_table.jdbc_url;
        jdbc_param.user = jdbc_table.jdbc_user;
        jdbc_param.passwd = jdbc_table.jdbc_password;
        jdbc_param.query_string = request->query_str();
        jdbc_param.table_type = static_cast<TOdbcTableType::type>(request->jdbc_table_type());
        jdbc_param.use_transaction = false;
        jdbc_param.connection_pool_min_size = jdbc_table.connection_pool_min_size;
        jdbc_param.connection_pool_max_size = jdbc_table.connection_pool_max_size;
        jdbc_param.connection_pool_max_life_time = jdbc_table.connection_pool_max_life_time;
        jdbc_param.connection_pool_max_wait_time = jdbc_table.connection_pool_max_wait_time;
        jdbc_param.connection_pool_keep_alive = jdbc_table.connection_pool_keep_alive;

        std::unique_ptr<vectorized::JdbcConnector> jdbc_connector;
        jdbc_connector.reset(new (std::nothrow) vectorized::JdbcConnector(jdbc_param));

        st = jdbc_connector->test_connection();
        st.to_protobuf(result->mutable_status());

        Status clean_st = jdbc_connector->clean_datasource();
        if (!clean_st.ok()) {
            LOG(WARNING) << "Failed to clean JDBC datasource: " << clean_st.msg();
        }
        Status close_st = jdbc_connector->close();
        if (!close_st.ok()) {
            LOG(WARNING) << "Failed to close JDBC connector: " << close_st.msg();
        }
    });

    if (!ret) {
        offer_failed(result, done, _heavy_work_pool);
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
    [[maybe_unused]] auto* cntl = static_cast<brpc::Controller*>(controller);
    TabletManager* tablet_mgr = _engine.tablet_manager();
    const auto& params = request->params();
    for (const auto& param : params) {
        int64_t index_id = param.indexid();
        const auto& tablet_ids = param.tablet_ids();
        std::set<std::set<int32_t>> filter_set;
        std::map<int32_t, const TabletColumn*> id_to_column;
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
                column_ids.insert(col->unique_id());
            }
            filter_set.insert(std::move(column_ids));

            if (id_to_column.empty()) {
                for (const auto& col : columns) {
                    id_to_column.insert(std::pair {col->unique_id(), col.get()});
                }
            } else {
                for (const auto& col : columns) {
                    auto it = id_to_column.find(col->unique_id());
                    if (it == id_to_column.end() || *(it->second) != *col) {
                        ColumnPB prev_col_pb;
                        ColumnPB curr_col_pb;
                        if (it != id_to_column.end()) {
                            it->second->to_schema_pb(&prev_col_pb);
                        }
                        col->to_schema_pb(&curr_col_pb);
                        std::stringstream ss;
                        ss << "consistency check failed: index{ " << index_id << " }"
                           << " got inconsistent schema, prev column: " << prev_col_pb.DebugString()
                           << " current column: " << curr_col_pb.DebugString();
                        LOG(WARNING) << ss.str();
                        response->mutable_status()->set_status_code(TStatusCode::ILLEGAL_STATE);
                        response->mutable_status()->add_error_msgs(ss.str());
                        return;
                    }
                }
            }
        }

        if (filter_set.size() > 1) {
            // consistecy check failed
            std::stringstream ss;
            ss << "consistency check failed: index{" << index_id << "}"
               << "got inconsistent schema";
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
            (*col_name_to_id)[column->name()] = column->unique_id();
        }
    }
    response->mutable_status()->set_status_code(TStatusCode::OK);
}

template <class RPCResponse>
struct AsyncRPCContext {
    RPCResponse response;
    brpc::Controller cntl;
    brpc::CallId cid;
};

void PInternalService::fetch_remote_tablet_schema(google::protobuf::RpcController* controller,
                                                  const PFetchRemoteSchemaRequest* request,
                                                  PFetchRemoteSchemaResponse* response,
                                                  google::protobuf::Closure* done) {
    bool ret = _heavy_work_pool.try_offer([request, response, done]() {
        brpc::ClosureGuard closure_guard(done);
        Status st = Status::OK();
        if (request->is_coordinator()) {
            // Spawn rpc request to none coordinator nodes, and finally merge them all
            PFetchRemoteSchemaRequest remote_request(*request);
            // set it none coordinator to get merged schema
            remote_request.set_is_coordinator(false);
            using PFetchRemoteTabletSchemaRpcContext = AsyncRPCContext<PFetchRemoteSchemaResponse>;
            std::vector<PFetchRemoteTabletSchemaRpcContext> rpc_contexts(
                    request->tablet_location_size());
            for (int i = 0; i < request->tablet_location_size(); ++i) {
                std::string host = request->tablet_location(i).host();
                int32_t brpc_port = request->tablet_location(i).brpc_port();
                std::shared_ptr<PBackendService_Stub> stub(
                        ExecEnv::GetInstance()->brpc_internal_client_cache()->get_client(
                                host, brpc_port));
                if (stub == nullptr) {
                    LOG(WARNING) << "Failed to init rpc to " << host << ":" << brpc_port;
                    st = Status::InternalError("Failed to init rpc to {}:{}", host, brpc_port);
                    continue;
                }
                rpc_contexts[i].cid = rpc_contexts[i].cntl.call_id();
                rpc_contexts[i].cntl.set_timeout_ms(config::fetch_remote_schema_rpc_timeout_ms);
                stub->fetch_remote_tablet_schema(&rpc_contexts[i].cntl, &remote_request,
                                                 &rpc_contexts[i].response, brpc::DoNothing());
            }
            std::vector<TabletSchemaSPtr> schemas;
            for (auto& rpc_context : rpc_contexts) {
                brpc::Join(rpc_context.cid);
                if (!st.ok()) {
                    // make sure all flying rpc request is joined
                    continue;
                }
                if (rpc_context.cntl.Failed()) {
                    LOG(WARNING) << "fetch_remote_tablet_schema rpc err:"
                                 << rpc_context.cntl.ErrorText();
                    ExecEnv::GetInstance()->brpc_internal_client_cache()->erase(
                            rpc_context.cntl.remote_side());
                    st = Status::InternalError("fetch_remote_tablet_schema rpc err: {}",
                                               rpc_context.cntl.ErrorText());
                }
                if (rpc_context.response.status().status_code() != 0) {
                    st = Status::create(rpc_context.response.status());
                }
                if (rpc_context.response.has_merged_schema()) {
                    TabletSchemaSPtr schema = std::make_shared<TabletSchema>();
                    schema->init_from_pb(rpc_context.response.merged_schema());
                    schemas.push_back(schema);
                }
            }
            if (!schemas.empty() && st.ok()) {
                // merge all
                TabletSchemaSPtr merged_schema;
                static_cast<void>(vectorized::schema_util::get_least_common_schema(schemas, nullptr,
                                                                                   merged_schema));
                VLOG_DEBUG << "dump schema:" << merged_schema->dump_structure();
                merged_schema->reserve_extracted_columns();
                merged_schema->to_schema_pb(response->mutable_merged_schema());
            }
            st.to_protobuf(response->mutable_status());
            return;
        } else {
            // This is not a coordinator, get it's tablet and merge schema
            std::vector<int64_t> target_tablets;
            for (int i = 0; i < request->tablet_location_size(); ++i) {
                const auto& location = request->tablet_location(i);
                auto backend = BackendOptions::get_local_backend();
                // If this is the target backend
                if (backend.host == location.host() && config::brpc_port == location.brpc_port()) {
                    target_tablets.assign(location.tablet_id().begin(), location.tablet_id().end());
                    break;
                }
            }
            if (!target_tablets.empty()) {
                std::vector<TabletSchemaSPtr> tablet_schemas;
                for (int64_t tablet_id : target_tablets) {
                    auto res = ExecEnv::get_tablet(tablet_id);
                    if (!res.has_value()) {
                        // just ignore
                        LOG(WARNING) << "tablet does not exist, tablet id is " << tablet_id;
                        continue;
                    }
                    tablet_schemas.push_back(res.value()->merged_tablet_schema());
                }
                if (!tablet_schemas.empty()) {
                    // merge all
                    TabletSchemaSPtr merged_schema;
                    static_cast<void>(vectorized::schema_util::get_least_common_schema(
                            tablet_schemas, nullptr, merged_schema));
                    merged_schema->to_schema_pb(response->mutable_merged_schema());
                    VLOG_DEBUG << "dump schema:" << merged_schema->dump_structure();
                }
            }
            st.to_protobuf(response->mutable_status());
        }
    });
    if (!ret) {
        offer_failed(response, done, _heavy_work_pool);
    }
}

void PInternalService::report_stream_load_status(google::protobuf::RpcController* controller,
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

void PInternalService::get_info(google::protobuf::RpcController* controller,
                                const PProxyRequest* request, PProxyResult* response,
                                google::protobuf::Closure* done) {
    bool ret = _heavy_work_pool.try_offer([this, request, response, done]() {
        brpc::ClosureGuard closure_guard(done);
        // PProxyRequest is defined in gensrc/proto/internal_service.proto
        // Currently it supports 2 kinds of requests:
        // 1. get all kafka partition ids for given topic
        // 2. get all kafka partition offsets for given topic and timestamp.
        int timeout_ms = request->has_timeout_secs() ? request->timeout_secs() * 1000 : 60 * 1000;
        if (request->has_kafka_meta_request()) {
            const PKafkaMetaProxyRequest& kafka_request = request->kafka_meta_request();
            if (!kafka_request.offset_flags().empty()) {
                std::vector<PIntegerPair> partition_offsets;
                Status st = _exec_env->routine_load_task_executor()
                                    ->get_kafka_real_offsets_for_partitions(
                                            request->kafka_meta_request(), &partition_offsets,
                                            timeout_ms);
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
            } else if (!kafka_request.partition_id_for_latest_offsets().empty()) {
                // get latest offsets for specified partition ids
                std::vector<PIntegerPair> partition_offsets;
                Status st = _exec_env->routine_load_task_executor()
                                    ->get_kafka_latest_offsets_for_partitions(
                                            request->kafka_meta_request(), &partition_offsets,
                                            timeout_ms);
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
                                            request->kafka_meta_request(), &partition_offsets,
                                            timeout_ms);
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

void PInternalService::update_cache(google::protobuf::RpcController* controller,
                                    const PUpdateCacheRequest* request, PCacheResponse* response,
                                    google::protobuf::Closure* done) {
    bool ret = _light_work_pool.try_offer([this, request, response, done]() {
        brpc::ClosureGuard closure_guard(done);
        _exec_env->result_cache()->update(request, response);
    });
    if (!ret) {
        offer_failed(response, done, _light_work_pool);
        return;
    }
}

void PInternalService::fetch_cache(google::protobuf::RpcController* controller,
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

void PInternalService::clear_cache(google::protobuf::RpcController* controller,
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

void PInternalService::merge_filter(::google::protobuf::RpcController* controller,
                                    const ::doris::PMergeFilterRequest* request,
                                    ::doris::PMergeFilterResponse* response,
                                    ::google::protobuf::Closure* done) {
    bool ret = _light_work_pool.try_offer([this, controller, request, response, done]() {
        brpc::ClosureGuard closure_guard(done);
        auto attachment = static_cast<brpc::Controller*>(controller)->request_attachment();
        butil::IOBufAsZeroCopyInputStream zero_copy_input_stream(attachment);
        Status st = _exec_env->fragment_mgr()->merge_filter(request, &zero_copy_input_stream);
        st.to_protobuf(response->mutable_status());
    });
    if (!ret) {
        offer_failed(response, done, _light_work_pool);
        return;
    }
}

void PInternalService::send_filter_size(::google::protobuf::RpcController* controller,
                                        const ::doris::PSendFilterSizeRequest* request,
                                        ::doris::PSendFilterSizeResponse* response,
                                        ::google::protobuf::Closure* done) {
    bool ret = _light_work_pool.try_offer([this, request, response, done]() {
        brpc::ClosureGuard closure_guard(done);
        Status st = _exec_env->fragment_mgr()->send_filter_size(request);
        st.to_protobuf(response->mutable_status());
    });
    if (!ret) {
        offer_failed(response, done, _light_work_pool);
        return;
    }
}

void PInternalService::sync_filter_size(::google::protobuf::RpcController* controller,
                                        const ::doris::PSyncFilterSizeRequest* request,
                                        ::doris::PSyncFilterSizeResponse* response,
                                        ::google::protobuf::Closure* done) {
    bool ret = _light_work_pool.try_offer([this, request, response, done]() {
        brpc::ClosureGuard closure_guard(done);
        Status st = _exec_env->fragment_mgr()->sync_filter_size(request);
        st.to_protobuf(response->mutable_status());
    });
    if (!ret) {
        offer_failed(response, done, _light_work_pool);
        return;
    }
}

void PInternalService::apply_filterv2(::google::protobuf::RpcController* controller,
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

void PInternalService::send_data(google::protobuf::RpcController* controller,
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

void PInternalService::commit(google::protobuf::RpcController* controller,
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

void PInternalService::rollback(google::protobuf::RpcController* controller,
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

void PInternalService::fold_constant_expr(google::protobuf::RpcController* controller,
                                          const PConstantExprRequest* request,
                                          PConstantExprResult* response,
                                          google::protobuf::Closure* done) {
    bool ret = _light_work_pool.try_offer([this, request, response, done]() {
        brpc::ClosureGuard closure_guard(done);
        TFoldConstantParams t_request;
        Status st = Status::OK();
        {
            const uint8_t* buf = (const uint8_t*)request->request().data();
            uint32_t len = request->request().size();
            st = deserialize_thrift_msg(buf, &len, false, &t_request);
        }
        if (!st.ok()) {
            LOG(WARNING) << "exec fold constant expr failed, errmsg=" << st
                         << " .and query_id_is: " << t_request.query_id;
        }
        st = _fold_constant_expr(request->request(), response);
        if (!st.ok()) {
            LOG(WARNING) << "exec fold constant expr failed, errmsg=" << st
                         << " .and query_id_is: " << t_request.query_id;
        }
        st.to_protobuf(response->mutable_status());
    });
    if (!ret) {
        offer_failed(response, done, _light_work_pool);
        return;
    }
}

Status PInternalService::_fold_constant_expr(const std::string& ser_request,
                                             PConstantExprResult* response) {
    TFoldConstantParams t_request;
    {
        const uint8_t* buf = (const uint8_t*)ser_request.data();
        uint32_t len = ser_request.size();
        RETURN_IF_ERROR(deserialize_thrift_msg(buf, &len, false, &t_request));
    }
    std::unique_ptr<FoldConstantExecutor> fold_executor = std::make_unique<FoldConstantExecutor>();
    RETURN_IF_ERROR_OR_CATCH_EXCEPTION(fold_executor->fold_constant_vexpr(t_request, response));
    return Status::OK();
}

void PInternalService::transmit_block(google::protobuf::RpcController* controller,
                                      const PTransmitDataParams* request,
                                      PTransmitDataResult* response,
                                      google::protobuf::Closure* done) {
    int64_t receive_time = GetCurrentTimeNanos();
    if (config::enable_bthread_transmit_block) {
        response->set_receive_time(receive_time);
        // under high concurrency, thread pool will have a lot of lock contention.
        // May offer failed to the thread pool, so that we should avoid using thread
        // pool here.
        _transmit_block(controller, request, response, done, Status::OK(), 0);
    } else {
        bool ret = _light_work_pool.try_offer([this, controller, request, response, done,
                                               receive_time]() {
            response->set_receive_time(receive_time);
            // Sometimes transmit block function is the last owner of PlanFragmentExecutor
            // It will release the object. And the object maybe a JNIContext.
            // JNIContext will hold some TLS object. It could not work correctly under bthread
            // Context. So that put the logic into pthread.
            // But this is rarely happens, so this config is disabled by default.
            _transmit_block(controller, request, response, done, Status::OK(),
                            GetCurrentTimeNanos() - receive_time);
        });
        if (!ret) {
            offer_failed(response, done, _light_work_pool);
            return;
        }
    }
}

void PInternalService::transmit_block_by_http(google::protobuf::RpcController* controller,
                                              const PEmptyRequest* request,
                                              PTransmitDataResult* response,
                                              google::protobuf::Closure* done) {
    int64_t receive_time = GetCurrentTimeNanos();
    bool ret = _heavy_work_pool.try_offer([this, controller, response, done, receive_time]() {
        PTransmitDataParams* new_request = new PTransmitDataParams();
        google::protobuf::Closure* new_done =
                new NewHttpClosure<PTransmitDataParams>(new_request, done);
        brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
        Status st =
                attachment_extract_request_contain_block<PTransmitDataParams>(new_request, cntl);
        _transmit_block(controller, new_request, response, new_done, st,
                        GetCurrentTimeNanos() - receive_time);
    });
    if (!ret) {
        offer_failed(response, done, _heavy_work_pool);
        return;
    }
}

void PInternalService::_transmit_block(google::protobuf::RpcController* controller,
                                       const PTransmitDataParams* request,
                                       PTransmitDataResult* response,
                                       google::protobuf::Closure* done, const Status& extract_st,
                                       const int64_t wait_for_worker) {
    if (request->has_query_id()) {
        VLOG_ROW << "transmit block: fragment_instance_id=" << print_id(request->finst_id())
                 << " query_id=" << print_id(request->query_id()) << " node=" << request->node_id();
    }

    // The response is accessed when done->Run is called in transmit_block(),
    // give response a default value to avoid null pointers in high concurrency.
    Status st;
    if (extract_st.ok()) {
        st = _exec_env->vstream_mgr()->transmit_block(request, &done, wait_for_worker);
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

void PInternalService::check_rpc_channel(google::protobuf::RpcController* controller,
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

void PInternalService::reset_rpc_channel(google::protobuf::RpcController* controller,
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

void PInternalService::hand_shake(google::protobuf::RpcController* controller,
                                  const PHandShakeRequest* request, PHandShakeResponse* response,
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

static std::string construct_url(const std::string& host_port, const std::string& token,
                                 const std::string& path) {
    return fmt::format("{}{}{}{}{}{}", HttpProtocol, host_port, DownloadApiPath, token, FileParam,
                       path);
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

        return io::global_local_filesystem()->permission(local_file_path,
                                                         io::LocalFileSystem::PERMS_OWNER_RW);
    };
    return HttpClient::execute_with_retry(DOWNLOAD_FILE_MAX_RETRY, 1, download_cb);
}

void PInternalServiceImpl::request_slave_tablet_pull_rowset(
        google::protobuf::RpcController* controller, const PTabletWriteSlaveRequest* request,
        PTabletWriteSlaveResult* response, google::protobuf::Closure* done) {
    brpc::ClosureGuard closure_guard(done);
    const RowsetMetaPB& rowset_meta_pb = request->rowset_meta();
    const std::string& rowset_path = request->rowset_path();
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
        TabletSharedPtr tablet = _engine.tablet_manager()->get_tablet(
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
        RowsetId new_rowset_id = _engine.next_rowset_id();
        auto pending_rs_guard = _engine.pending_local_rowsets().add(new_rowset_id);
        rowset_meta->set_rowset_id(new_rowset_id);
        rowset_meta->set_tablet_uid(tablet->tablet_uid());
        VLOG_CRITICAL << "succeed to init rowset meta for slave replica. rowset_id="
                      << rowset_meta->rowset_id() << ", tablet_id=" << rowset_meta->tablet_id()
                      << ", txn_id=" << rowset_meta->txn_id();

        auto tablet_scheme = rowset_meta->tablet_schema();
        for (const auto& segment : segments_size) {
            uint64_t file_size = segment.second;
            uint64_t estimate_timeout = file_size / config::download_low_speed_limit_kbps / 1024;
            if (estimate_timeout < config::download_low_speed_time) {
                estimate_timeout = config::download_low_speed_time;
            }

            std::string remote_file_path =
                    local_segment_path(rowset_path, remote_rowset_id.to_string(), segment.first);
            std::string remote_file_url =
                    construct_url(get_host_port(host, http_port), token, remote_file_path);

            std::string local_file_path = local_segment_path(
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
                    auto suffix_path = index_size.suffix_path();
                    std::string remote_inverted_index_file;
                    std::string local_inverted_index_file;
                    std::string remote_inverted_index_file_url;
                    if (tablet_scheme->get_inverted_index_storage_format() ==
                        InvertedIndexStorageFormatPB::V1) {
                        remote_inverted_index_file =
                                InvertedIndexDescriptor::get_index_file_path_v1(
                                        InvertedIndexDescriptor::get_index_file_path_prefix(
                                                remote_file_path),
                                        index_id, suffix_path);
                        remote_inverted_index_file_url = construct_url(
                                get_host_port(host, http_port), token, remote_inverted_index_file);

                        local_inverted_index_file = InvertedIndexDescriptor::get_index_file_path_v1(
                                InvertedIndexDescriptor::get_index_file_path_prefix(
                                        local_file_path),
                                index_id, suffix_path);
                    } else {
                        remote_inverted_index_file =
                                InvertedIndexDescriptor::get_index_file_path_v2(
                                        InvertedIndexDescriptor::get_index_file_path_prefix(
                                                remote_file_path));
                        remote_inverted_index_file_url = construct_url(
                                get_host_port(host, http_port), token, remote_inverted_index_file);

                        local_inverted_index_file = InvertedIndexDescriptor::get_index_file_path_v2(
                                InvertedIndexDescriptor::get_index_file_path_prefix(
                                        local_file_path));
                    }
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
        Status commit_txn_status = _engine.txn_manager()->commit_txn(
                tablet->data_dir()->get_meta(), rowset_meta->partition_id(), rowset_meta->txn_id(),
                rowset_meta->tablet_id(), tablet->tablet_uid(), rowset_meta->load_id(), rowset,
                std::move(pending_rs_guard), false);
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

    auto request = std::make_shared<PTabletWriteSlaveDoneRequest>();
    request->set_txn_id(txn_id);
    request->set_tablet_id(tablet_id);
    request->set_node_id(node_id);
    request->set_is_succeed(is_succeed);
    auto pull_rowset_callback = DummyBrpcCallback<PTabletWriteSlaveDoneResult>::create_shared();
    auto closure = AutoReleaseClosure<
            PTabletWriteSlaveDoneRequest,
            DummyBrpcCallback<PTabletWriteSlaveDoneResult>>::create_unique(request,
                                                                           pull_rowset_callback);
    closure->cntl_->set_timeout_ms(config::slave_replica_writer_rpc_timeout_sec * 1000);
    closure->cntl_->ignore_eovercrowded();
    stub->response_slave_tablet_pull_rowset(closure->cntl_.get(), closure->request_.get(),
                                            closure->response_.get(), closure.get());
    closure.release();

    pull_rowset_callback->join();
    if (pull_rowset_callback->cntl_->Failed()) {
        LOG(WARNING) << "failed to response result of slave replica to master replica, error="
                     << berror(pull_rowset_callback->cntl_->ErrorCode())
                     << ", error_text=" << pull_rowset_callback->cntl_->ErrorText()
                     << ", master host: " << remote_host << ", tablet_id=" << tablet_id
                     << ", txn_id=" << txn_id;
    }
    VLOG_CRITICAL << "succeed to response the result of slave replica pull rowset to master "
                     "replica. master host: "
                  << remote_host << ". is_succeed=" << is_succeed << ", tablet_id=" << tablet_id
                  << ", slave server=" << node_id << ", txn_id=" << txn_id;
}

void PInternalServiceImpl::response_slave_tablet_pull_rowset(
        google::protobuf::RpcController* controller, const PTabletWriteSlaveDoneRequest* request,
        PTabletWriteSlaveDoneResult* response, google::protobuf::Closure* done) {
    bool ret = _heavy_work_pool.try_offer([txn_mgr = _engine.txn_manager(), request, response,
                                           done]() {
        brpc::ClosureGuard closure_guard(done);
        VLOG_CRITICAL << "receive the result of slave replica pull rowset from slave replica. "
                         "slave server="
                      << request->node_id() << ", is_succeed=" << request->is_succeed()
                      << ", tablet_id=" << request->tablet_id() << ", txn_id=" << request->txn_id();
        txn_mgr->finish_slave_tablet_pull_rowset(request->txn_id(), request->tablet_id(),
                                                 request->node_id(), request->is_succeed());
        Status::OK().to_protobuf(response->mutable_status());
    });
    if (!ret) {
        offer_failed(response, done, _heavy_work_pool);
        return;
    }
}

void PInternalService::multiget_data(google::protobuf::RpcController* controller,
                                     const PMultiGetRequest* request, PMultiGetResponse* response,
                                     google::protobuf::Closure* done) {
    bool ret = _light_work_pool.try_offer([request, response, done]() {
        signal::set_signal_task_id(request->query_id());
        // multi get data by rowid
        MonotonicStopWatch watch;
        watch.start();
        brpc::ClosureGuard closure_guard(done);
        response->mutable_status()->set_status_code(0);
        SCOPED_ATTACH_TASK(ExecEnv::GetInstance()->rowid_storage_reader_tracker());
        Status st = RowIdStorageReader::read_by_rowids(*request, response);
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
    _engine.get_tablet_rowset_versions(request, response);
}

void PInternalService::glob(google::protobuf::RpcController* controller,
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

void PInternalService::group_commit_insert(google::protobuf::RpcController* controller,
                                           const PGroupCommitInsertRequest* request,
                                           PGroupCommitInsertResponse* response,
                                           google::protobuf::Closure* done) {
    TUniqueId load_id;
    load_id.__set_hi(request->load_id().hi());
    load_id.__set_lo(request->load_id().lo());
    std::shared_ptr<std::mutex> lock = std::make_shared<std::mutex>();
    std::shared_ptr<bool> is_done = std::make_shared<bool>(false);
    bool ret = _light_work_pool.try_offer([this, request, response, done, load_id, lock,
                                           is_done]() {
        brpc::ClosureGuard closure_guard(done);
        std::shared_ptr<StreamLoadContext> ctx = std::make_shared<StreamLoadContext>(_exec_env);
        auto pipe = std::make_shared<io::StreamLoadPipe>(
                io::kMaxPipeBufferedBytes /* max_buffered_bytes */, 64 * 1024 /* min_chunk_size */,
                -1 /* total_length */, true /* use_proto */);
        ctx->pipe = pipe;
        Status st = _exec_env->new_load_stream_mgr()->put(load_id, ctx);
        if (st.ok()) {
            try {
                st = _exec_plan_fragment_impl(
                        request->exec_plan_fragment_request().request(),
                        request->exec_plan_fragment_request().version(),
                        request->exec_plan_fragment_request().compact(),
                        [&, response, done, load_id, lock, is_done](RuntimeState* state,
                                                                    Status* status) {
                            std::lock_guard<std::mutex> lock1(*lock);
                            if (*is_done) {
                                return;
                            }
                            *is_done = true;
                            brpc::ClosureGuard cb_closure_guard(done);
                            response->set_label(state->import_label());
                            response->set_txn_id(state->wal_id());
                            response->set_loaded_rows(state->num_rows_load_success());
                            response->set_filtered_rows(state->num_rows_load_filtered());
                            status->to_protobuf(response->mutable_status());
                            if (!state->get_error_log_file_path().empty()) {
                                response->set_error_url(
                                        to_load_error_http_path(state->get_error_log_file_path()));
                            }
                            _exec_env->new_load_stream_mgr()->remove(load_id);
                        });
            } catch (const Exception& e) {
                st = e.to_status();
            } catch (...) {
                st = Status::Error(ErrorCode::INTERNAL_ERROR,
                                   "_exec_plan_fragment_impl meet unknown error");
            }
            if (!st.ok()) {
                LOG(WARNING) << "exec plan fragment failed, load_id=" << print_id(load_id)
                             << ", errmsg=" << st;
                std::lock_guard<std::mutex> lock1(*lock);
                if (*is_done) {
                    closure_guard.release();
                } else {
                    *is_done = true;
                    st.to_protobuf(response->mutable_status());
                    _exec_env->new_load_stream_mgr()->remove(load_id);
                }
            } else {
                closure_guard.release();
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
                }
            }
        }
    });
    if (!ret) {
        _exec_env->new_load_stream_mgr()->remove(load_id);
        offer_failed(response, done, _light_work_pool);
        return;
    }
};

void PInternalService::get_wal_queue_size(google::protobuf::RpcController* controller,
                                          const PGetWalQueueSizeRequest* request,
                                          PGetWalQueueSizeResponse* response,
                                          google::protobuf::Closure* done) {
    bool ret = _light_work_pool.try_offer([this, request, response, done]() {
        brpc::ClosureGuard closure_guard(done);
        Status st = Status::OK();
        auto table_id = request->table_id();
        auto count = _exec_env->wal_mgr()->get_wal_queue_size(table_id);
        response->set_size(count);
        response->mutable_status()->set_status_code(st.code());
    });
    if (!ret) {
        offer_failed(response, done, _light_work_pool);
    }
}

void PInternalService::get_be_resource(google::protobuf::RpcController* controller,
                                       const PGetBeResourceRequest* request,
                                       PGetBeResourceResponse* response,
                                       google::protobuf::Closure* done) {
    bool ret = _light_work_pool.try_offer([response, done]() {
        brpc::ClosureGuard closure_guard(done);
        int64_t mem_limit = MemInfo::mem_limit();
        int64_t mem_usage = PerfCounters::get_vm_rss();

        PGlobalResourceUsage* global_resource_usage = response->mutable_global_be_resource_usage();
        global_resource_usage->set_mem_limit(mem_limit);
        global_resource_usage->set_mem_usage(mem_usage);

        Status st = Status::OK();
        response->mutable_status()->set_status_code(st.code());
    });
    if (!ret) {
        offer_failed(response, done, _light_work_pool);
    }
}

} // namespace doris
