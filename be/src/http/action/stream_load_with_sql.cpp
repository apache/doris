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

#include "http/action/stream_load_with_sql.h"

#include <deque>
#include <future>
#include <sstream>

// use string iequal
#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/http.h>
#include <rapidjson/prettywriter.h>
#include <thrift/protocol/TDebugProtocol.h>

#include "common/consts.h"
#include "common/logging.h"
#include "common/status.h"
#include "common/utils.h"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/FrontendService_types.h"
#include "gen_cpp/HeartbeatService_types.h"
#include "http/http_channel.h"
#include "http/http_common.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_response.h"
#include "http/utils.h"
#include "io/fs/stream_load_pipe.h"
#include "olap/storage_engine.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "runtime/load_path_mgr.h"
#include "runtime/plan_fragment_executor.h"
#include "runtime/stream_load/new_load_stream_mgr.h"
#include "runtime/stream_load/stream_load_context.h"
#include "runtime/stream_load/stream_load_executor.h"
#include "runtime/stream_load/stream_load_recorder.h"
#include "util/byte_buffer.h"
#include "util/debug_util.h"
#include "util/doris_metrics.h"
#include "util/json_util.h"
#include "util/metrics.h"
#include "util/string_util.h"
#include "util/thrift_rpc_helper.h"
#include "util/time.h"
#include "util/uid_util.h"

namespace doris {
using namespace ErrorCode;

DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(streaming_load_with_sql_requests_total, MetricUnit::REQUESTS);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(streaming_load_with_sql_duration_ms, MetricUnit::MILLISECONDS);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(streaming_load_with_sql_current_processing,
                                   MetricUnit::REQUESTS);

#ifdef BE_TEST
TStreamLoadPutResult k_stream_load_put_result;
#endif

static bool is_format_support_streaming(TFileFormatType::type format) {
    switch (format) {
    case TFileFormatType::FORMAT_CSV_PLAIN:
    case TFileFormatType::FORMAT_CSV_BZ2:
    case TFileFormatType::FORMAT_CSV_DEFLATE:
    case TFileFormatType::FORMAT_CSV_GZ:
    case TFileFormatType::FORMAT_CSV_LZ4FRAME:
    case TFileFormatType::FORMAT_CSV_LZO:
    case TFileFormatType::FORMAT_CSV_LZOP:
    case TFileFormatType::FORMAT_JSON:
        return true;
    default:
        return false;
    }
}

StreamLoadWithSqlAction::StreamLoadWithSqlAction(ExecEnv* exec_env) : _exec_env(exec_env) {
    _stream_load_with_sql_entity =
            DorisMetrics::instance()->metric_registry()->register_entity("stream_load_with_sql");
    INT_COUNTER_METRIC_REGISTER(_stream_load_with_sql_entity,
                                streaming_load_with_sql_requests_total);
    INT_COUNTER_METRIC_REGISTER(_stream_load_with_sql_entity, streaming_load_with_sql_duration_ms);
    INT_GAUGE_METRIC_REGISTER(_stream_load_with_sql_entity,
                              streaming_load_with_sql_current_processing);
}

StreamLoadWithSqlAction::~StreamLoadWithSqlAction() {
    DorisMetrics::instance()->metric_registry()->deregister_entity(_stream_load_with_sql_entity);
}

void StreamLoadWithSqlAction::handle(HttpRequest* req) {
    StreamLoadContext* ctx = (StreamLoadContext*)req->handler_ctx();
    if (ctx == nullptr) {
        return;
    }

    // status already set to fail
    if (ctx->status.ok()) {
        ctx->status = _handle(ctx);
        if (!ctx->status.ok() && !ctx->status.is<PUBLISH_TIMEOUT>()) {
            LOG(WARNING) << "handle streaming load failed, id=" << ctx->id
                         << ", errmsg=" << ctx->status;
        }
    }
    ctx->load_cost_millis = UnixMillis() - ctx->start_millis;

    if (!ctx->status.ok() && !ctx->status.is<PUBLISH_TIMEOUT>()) {
        if (ctx->need_rollback) {
            _exec_env->stream_load_executor()->rollback_txn(ctx);
            ctx->need_rollback = false;
        }
        if (ctx->body_sink.get() != nullptr) {
            ctx->body_sink->cancel(ctx->status.to_string());
        }
    }

    // query stream load status
    // put request
    TStreamLoadWithLoadStatusRequest request;
    TStreamLoadWithLoadStatusResult result;
    request.__set_loadId(ctx->id.to_thrift());
    TNetworkAddress master_addr = _exec_env->master_info()->network_address;
    while (is_stream_load_put_success) {
        ThriftRpcHelper::rpc<FrontendServiceClient>(
                master_addr.hostname, master_addr.port,
                [&request, &result](FrontendServiceConnection& client) {
                    client->StreamLoadWithLoadStatus(result, request);
                });
        Status stream_load_status(result.status);
        if (!stream_load_status.ok()) {
            continue;
        }
        sleep(1);
        break;
    }
    auto str = std::string("Stream Load OK");
    // add new line at end
    str = str + '\n';
    if (!is_stream_load_put_success) {
        str = std::string("Stream Load is failed\n");
    }
    HttpChannel::send_reply(req, str);
#ifndef BE_TEST
    if (config::enable_stream_load_record) {
        str = ctx->prepare_stream_load_record(str);
        _save_stream_load_record(ctx, str);
    }
#endif
    // update statistics
    streaming_load_with_sql_requests_total->increment(1);
    streaming_load_with_sql_duration_ms->increment(ctx->load_cost_millis);
    streaming_load_with_sql_current_processing->increment(-1);
}

Status StreamLoadWithSqlAction::_handle(StreamLoadContext* ctx) {
    if (ctx->body_bytes > 0 && ctx->receive_bytes != ctx->body_bytes) {
        LOG(WARNING) << "recevie body don't equal with body bytes, body_bytes=" << ctx->body_bytes
                     << ", receive_bytes=" << ctx->receive_bytes << ", id=" << ctx->id;
        return Status::InternalError("receive body don't equal with body bytes");
    }

    RETURN_IF_ERROR(ctx->body_sink->finish());

    // wait stream load finish
    // RETURN_IF_ERROR(ctx->future.get());

    if (ctx->two_phase_commit) {
        int64_t pre_commit_start_time = MonotonicNanos();
        RETURN_IF_ERROR(_exec_env->stream_load_executor()->pre_commit_txn(ctx));
        ctx->pre_commit_txn_cost_nanos = MonotonicNanos() - pre_commit_start_time;
    } else {
        // If put file success we need commit this load
        int64_t commit_and_publish_start_time = MonotonicNanos();
        RETURN_IF_ERROR(_exec_env->stream_load_executor()->commit_txn(ctx));
        ctx->commit_and_publish_txn_cost_nanos = MonotonicNanos() - commit_and_publish_start_time;
    }
    return Status::OK();
}

int StreamLoadWithSqlAction::on_header(HttpRequest* req) {
    streaming_load_with_sql_current_processing->increment(1);

    StreamLoadContext* ctx = new StreamLoadContext(_exec_env);
    ctx->ref();
    req->set_handler_ctx(ctx);

    ctx->load_type = TLoadType::MANUL_LOAD;
    ctx->load_src_type = TLoadSourceType::RAW;

    ctx->db = req->param(HTTP_DB_KEY);
    ctx->table = req->param(HTTP_TABLE_KEY);
    ctx->label = req->header(HTTP_LABEL_KEY);
    if (ctx->label.empty()) {
        ctx->label = generate_uuid_string();
    }

    ctx->two_phase_commit = req->header(HTTP_TWO_PHASE_COMMIT) == "true" ? true : false;

    LOG(INFO) << "new income streaming load request." << ctx->brief() << ", db=" << ctx->db
              << ", tbl=" << ctx->table;

    auto st = _on_header(req, ctx);
    if (!st.ok()) {
        ctx->status = std::move(st);
        if (ctx->need_rollback) {
            _exec_env->stream_load_executor()->rollback_txn(ctx);
            ctx->need_rollback = false;
        }
        if (ctx->body_sink.get() != nullptr) {
            ctx->body_sink->cancel(ctx->status.to_string());
        }
        auto str = ctx->to_json();
        // add new line at end
        str = str + '\n';
        HttpChannel::send_reply(req, str);
        streaming_load_with_sql_current_processing->increment(-1);
#ifndef BE_TEST
        if (config::enable_stream_load_record) {
            str = ctx->prepare_stream_load_record(str);
            _save_stream_load_record(ctx, str);
        }
#endif
        return -1;
    }
    return 0;
}

Status StreamLoadWithSqlAction::_on_header(HttpRequest* http_req, StreamLoadContext* ctx) {
    // auth information
    if (!parse_basic_auth(*http_req, &ctx->auth)) {
        LOG(WARNING) << "parse basic authorization failed." << ctx->brief();
        return Status::InternalError("no valid Basic authorization");
    }
    // default csv
    ctx->format = TFileFormatType::FORMAT_CSV_PLAIN;

    if (ctx->format == TFileFormatType::FORMAT_UNKNOWN) {
        return Status::InternalError("unknown data format, format={}",
                                     http_req->header(HTTP_FORMAT_KEY));
    }

    // check content length
    ctx->body_bytes = 0;
    size_t csv_max_body_bytes = config::streaming_load_max_mb * 1024 * 1024;

#ifndef BE_TEST
    evhttp_connection_set_max_body_size(
            evhttp_request_get_connection(http_req->get_evhttp_request()), csv_max_body_bytes);
#endif

    // begin transaction
    int64_t begin_txn_start_time = MonotonicNanos();
    RETURN_IF_ERROR(_exec_env->stream_load_executor()->begin_txn(ctx));
    ctx->begin_txn_cost_nanos = MonotonicNanos() - begin_txn_start_time;

    // create stream load pipe
    auto pipe = std::make_shared<io::StreamLoadPipe>(kMaxPipeBufferedBytes /* max_buffered_bytes */,
                                                     64 * 1024 /* min_chunk_size */,
                                                     ctx->body_bytes /* total_length */);
    RETURN_IF_ERROR(_exec_env->new_load_stream_mgr()->put(ctx->id, pipe));
    ctx->body_sink = pipe;

    return Status::OK();
}

void StreamLoadWithSqlAction::on_chunk_data(HttpRequest* req) {
    StreamLoadContext* ctx = (StreamLoadContext*)req->handler_ctx();
    if (ctx == nullptr || !ctx->status.ok()) {
        return;
    }

    struct evhttp_request* ev_req = req->get_evhttp_request();
    auto evbuf = evhttp_request_get_input_buffer(ev_req);

    int64_t start_read_data_time = MonotonicNanos();
    const size_t buffer_max_size = 1 * 1024 * 1024;
    size_t buffer_size = 0;
    char* buffer = new char[buffer_max_size];
    bool is_put_buffer = false;
    while (evbuffer_get_length(evbuf) > 0) {
        auto bb = ByteBuffer::allocate(128 * 1024);
        auto remove_bytes = evbuffer_remove(evbuf, bb->ptr, bb->capacity);
        bb->pos = remove_bytes;
        bb->flip();
        auto st = ctx->body_sink->append(bb);
        if (!st.ok()) {
            LOG(WARNING) << "append body content failed. errmsg=" << st << ", " << ctx->brief();
            ctx->status = st;
            return;
        }
        ctx->receive_bytes += remove_bytes;
        if (ctx->receive_bytes <= buffer_max_size) {
            memcpy(buffer + buffer_size, bb->ptr, remove_bytes);
            buffer_size += remove_bytes;
        } else {
            _exec_env->new_load_stream_mgr()->put_buffer(ctx->id, buffer);
            is_put_buffer = true;
            _process_put(req, ctx);
        }
    }
    if (!is_put_buffer) {
        _exec_env->new_load_stream_mgr()->put_buffer(ctx->id, buffer);
        _process_put(req, ctx);
    }
    ctx->read_data_cost_nanos += (MonotonicNanos() - start_read_data_time);
}

void StreamLoadWithSqlAction::free_handler_ctx(void* param) {
    StreamLoadContext* ctx = (StreamLoadContext*)param;
    if (ctx == nullptr) {
        return;
    }
    // sender is gone, make receiver know it
    if (ctx->body_sink != nullptr) {
        ctx->body_sink->cancel("sender is gone");
    }
    if (ctx->unref()) {
        delete ctx;
    }
}

Status StreamLoadWithSqlAction::_process_put(HttpRequest* http_req, StreamLoadContext* ctx) {
    // Now we use stream
    ctx->use_streaming = is_format_support_streaming(ctx->format);

    // put request
    TStreamLoadPutRequest request;
    set_request_auth(&request, ctx->auth);
    request.db = ctx->db;
    request.tbl = ctx->table;
    request.txnId = ctx->txn_id;
    request.formatType = ctx->format;
    request.__set_version(version);
    request.__set_load_sql(http_req->header(HTTP_SQL));
    request.__set_loadId(ctx->id.to_thrift());
    if (_exec_env->master_info()->__isset.backend_id) {
        LOG(WARNING) << "_exec_env->master_info backend_id: "
                     << _exec_env->master_info()->backend_id;
        if (_exec_env->master_info()->backend_id < 1) {
            request.__set_backend_id(10046);
        } else {
            request.__set_backend_id(_exec_env->master_info()->backend_id);
        }
    } else {
        LOG(WARNING) << "_exec_env->master_info not set backend_id";
    }
    request.__set_backend_id(10046);
    request.__set_execMemLimit(2 * 1024 * 1024 * 1024L);
    request.fileType = TFileType::FILE_STREAM;
    request.__set_thrift_rpc_timeout_ms(20000);

#ifndef BE_TEST
    // plan this load
    TNetworkAddress master_addr = _exec_env->master_info()->network_address;
    int64_t stream_load_put_start_time = MonotonicNanos();
    RETURN_IF_ERROR(ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&request, ctx](FrontendServiceConnection& client) {
                client->streamLoadPut(ctx->put_result, request);
            }));
    ctx->stream_load_put_cost_nanos = MonotonicNanos() - stream_load_put_start_time;
#else
    ctx->put_result = k_stream_load_put_result;
#endif
    Status plan_status(ctx->put_result.status);
    if (!plan_status.ok()) {
        LOG(WARNING) << "plan streaming load failed. errmsg=" << plan_status << ctx->brief();
        return plan_status;
    }
    VLOG_NOTICE << "params is " << apache::thrift::ThriftDebugString(ctx->put_result.params);
    is_stream_load_put_success = true;
    return Status::OK();
}

void StreamLoadWithSqlAction::_save_stream_load_record(StreamLoadContext* ctx,
                                                       const std::string& str) {
    auto stream_load_recorder = StorageEngine::instance()->get_stream_load_recorder();
    if (stream_load_recorder != nullptr) {
        std::string key =
                std::to_string(ctx->start_millis + ctx->load_cost_millis) + "_" + ctx->label;
        auto st = stream_load_recorder->put(key, str);
        if (st.ok()) {
            LOG(INFO) << "put stream_load_record rocksdb successfully. label: " << ctx->label
                      << ", key: " << key;
        }
    } else {
        LOG(WARNING) << "put stream_load_record rocksdb failed. stream_load_recorder is null.";
    }
}

} // namespace doris
