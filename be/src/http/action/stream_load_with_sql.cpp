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

#include "common/config.h"
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
#include "runtime/export_task_mgr.h"
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

void StreamLoadWithSqlAction::_parse_format(const std::string& format_str,
                                            const std::string& compress_type_str,
                                            TFileFormatType::type* format_type,
                                            TFileCompressType::type* compress_type) {
    if (format_str.empty()) {
        _parse_format("CSV", compress_type_str, format_type, compress_type);
        return;
    }
    *compress_type = TFileCompressType::PLAIN;
    *format_type = TFileFormatType::FORMAT_UNKNOWN;
    if (iequal(format_str, "CSV")) {
        if (compress_type_str.empty()) {
            *format_type = TFileFormatType::FORMAT_CSV_PLAIN;
        } else if (iequal(compress_type_str, "GZ")) {
            *format_type = TFileFormatType::FORMAT_CSV_GZ;
            *compress_type = TFileCompressType::GZ;
        } else if (iequal(compress_type_str, "LZO")) {
            *format_type = TFileFormatType::FORMAT_CSV_LZO;
            *compress_type = TFileCompressType::LZO;
        } else if (iequal(compress_type_str, "BZ2")) {
            *format_type = TFileFormatType::FORMAT_CSV_BZ2;
            *compress_type = TFileCompressType::BZ2;
        } else if (iequal(compress_type_str, "LZ4")) {
            *format_type = TFileFormatType::FORMAT_CSV_LZ4FRAME;
            *compress_type = TFileCompressType::LZ4FRAME;
        } else if (iequal(compress_type_str, "LZOP")) {
            *format_type = TFileFormatType::FORMAT_CSV_LZOP;
            *compress_type = TFileCompressType::LZO;
        } else if (iequal(compress_type_str, "DEFLATE")) {
            *format_type = TFileFormatType::FORMAT_CSV_DEFLATE;
            *compress_type = TFileCompressType::DEFLATE;
        }
    } else if (iequal(format_str, "JSON")) {
        if (compress_type_str.empty()) {
            *format_type = TFileFormatType::FORMAT_JSON;
        }
    } else if (iequal(format_str, "PARQUET")) {
        *format_type = TFileFormatType::FORMAT_PARQUET;
    } else if (iequal(format_str, "ORC")) {
        *format_type = TFileFormatType::FORMAT_ORC;
    }
}

bool StreamLoadWithSqlAction::_is_format_support_streaming(TFileFormatType::type format) {
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

    if (!ctx->status.ok()) {
        auto str = std::string(ctx->to_json());
        // add new line at end
        str = str + '\n';
        HttpChannel::send_reply(req, str);
        return;
    }

    // query stream load status
    // put request
    TStreamLoadWithLoadStatusRequest request;
    TStreamLoadWithLoadStatusResult result;
    request.__set_loadId(ctx->id.to_thrift());
    TNetworkAddress master_addr = _exec_env->master_info()->network_address;
    while (true) {
        ThriftRpcHelper::rpc<FrontendServiceClient>(
                master_addr.hostname, master_addr.port,
                [&request, &result](FrontendServiceConnection& client) {
                    client->streamLoadWithLoadStatus(result, request);
                });
        Status stream_load_status(result.status);
        if (stream_load_status.ok()) {
            ctx->txn_id = result.txn_id;
            ctx->number_total_rows = result.total_rows;
            ctx->number_loaded_rows = result.loaded_rows;
            ctx->number_filtered_rows = result.filtered_rows;
            ctx->number_unselected_rows = result.unselected_rows;
            break;
        }
    }

    auto str = std::string(ctx->to_json());
    // add new line at end
    str = str + '\n';
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
    ctx->future.wait_for(std::chrono::seconds(1));
    if (!ctx->future.valid()) {
        return Status::TimedOut("stream load timeout");
    }
    RETURN_IF_ERROR(ctx->future.get());

    if (ctx->two_phase_commit) {
        int64_t pre_commit_start_time = MonotonicNanos();
        RETURN_IF_ERROR(_exec_env->stream_load_executor()->pre_commit_txn(ctx));
        ctx->pre_commit_txn_cost_nanos = MonotonicNanos() - pre_commit_start_time;
    } else {
        // If put file success we need commit this load
        int64_t commit_and_publish_start_time = MonotonicNanos();
        // RETURN_IF_ERROR(_exec_env->stream_load_executor()->commit_txn(ctx));
        ctx->commit_and_publish_txn_cost_nanos = MonotonicNanos() - commit_and_publish_start_time;
    }
    return ctx->status;
}

int StreamLoadWithSqlAction::on_header(HttpRequest* req) {
    streaming_load_with_sql_current_processing->increment(1);

    StreamLoadContext* ctx = new StreamLoadContext(_exec_env);
    ctx->ref();
    req->set_handler_ctx(ctx);

    ctx->load_type = TLoadType::MANUL_LOAD;
    ctx->load_src_type = TLoadSourceType::RAW;

    ctx->label = req->header(HTTP_LABEL_KEY);
    if (ctx->label.empty()) {
        ctx->label = generate_uuid_string();
    }

    ctx->two_phase_commit = req->header(HTTP_TWO_PHASE_COMMIT) == "true" ? true : false;

    LOG(INFO) << "new income streaming load request." << ctx->brief()
              << " sql : " << req->header(HTTP_SQL);

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
    // get format of this put
    if (!http_req->header(HTTP_COMPRESS_TYPE).empty() &&
        iequal(http_req->header(HTTP_FORMAT_KEY), "JSON")) {
        return Status::InternalError("compress data of JSON format is not supported.");
    }
    std::string format_str = http_req->header(HTTP_FORMAT_KEY);
    if (iequal(format_str, BeConsts::CSV_WITH_NAMES) ||
        iequal(format_str, BeConsts::CSV_WITH_NAMES_AND_TYPES)) {
        ctx->header_type = format_str;
        //treat as CSV
        format_str = BeConsts::CSV;
    }
    _parse_format(format_str, http_req->header(HTTP_COMPRESS_TYPE), &ctx->format,
                  &ctx->compress_type);
    if (ctx->format == TFileFormatType::FORMAT_UNKNOWN) {
        return Status::InternalError("unknown data format, format={}",
                                     http_req->header(HTTP_FORMAT_KEY));
    }

    // check content length
    ctx->body_bytes = 0;
    size_t csv_max_body_bytes = config::streaming_load_max_mb * 1024 * 1024;
    size_t json_max_body_bytes = config::streaming_load_json_max_mb * 1024 * 1024;
    bool read_json_by_line = false;
    if (!http_req->header(HTTP_READ_JSON_BY_LINE).empty()) {
        if (iequal(http_req->header(HTTP_READ_JSON_BY_LINE), "true")) {
            read_json_by_line = true;
        }
    }
    if (!http_req->header(HttpHeaders::CONTENT_LENGTH).empty()) {
        ctx->body_bytes = std::stol(http_req->header(HttpHeaders::CONTENT_LENGTH));
        // json max body size
        if ((ctx->format == TFileFormatType::FORMAT_JSON) &&
            (ctx->body_bytes > json_max_body_bytes) && !read_json_by_line) {
            return Status::InternalError(
                    "The size of this batch exceed the max size [{}]  of json type data "
                    " data [ {} ]. Split the file, or use 'read_json_by_line'",
                    json_max_body_bytes, ctx->body_bytes);
        }
        // csv max body size
        else if (ctx->body_bytes > csv_max_body_bytes) {
            LOG(WARNING) << "body exceed max size." << ctx->brief();
            return Status::InternalError("body exceed max size: {}, data: {}", csv_max_body_bytes,
                                         ctx->body_bytes);
        }
    } else {
#ifndef BE_TEST
        evhttp_connection_set_max_body_size(
                evhttp_request_get_connection(http_req->get_evhttp_request()), csv_max_body_bytes);
#endif
    }

    if (!http_req->header(HTTP_TIMEOUT).empty()) {
        try {
            ctx->timeout_second = std::stoi(http_req->header(HTTP_TIMEOUT));
        } catch (const std::invalid_argument& e) {
            return Status::InvalidArgument("Invalid timeout format");
        }
    }

    // create stream load pipe
    auto pipe = std::make_shared<io::StreamLoadPipe>(
            io::kMaxPipeBufferedBytes /* max_buffered_bytes */, 64 * 1024 /* min_chunk_size */,
            ctx->body_bytes /* total_length */);
    RETURN_IF_ERROR(_exec_env->new_load_stream_mgr()->put(ctx->id, pipe));
    ctx->future = _exec_env->new_load_stream_mgr()->get_future(ctx->id);
    ctx->body_sink = pipe;
    ctx->txn_id = 0;

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

    // Use buffer to store the first 1MB of stream data so that the schema can be parsed later
    // It is assumed that 1MB is sufficient here,
    // but later modifications may be needed to resolve different line lengths
    const size_t buffer_max_size = config::stream_tvf_buffer_size;
    size_t buffer_size = 0;
    char* buffer = new char[buffer_max_size];
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
            ctx->is_put_buffer = true;
            ctx->status = _process_put(req, ctx);
        }
    }
    if (!ctx->is_put_buffer && buffer_size) {
        _exec_env->new_load_stream_mgr()->put_buffer(ctx->id, buffer);
        ctx->is_put_buffer = true;
        ctx->status = _process_put(req, ctx);
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
    ctx->use_streaming = _is_format_support_streaming(ctx->format);
    if (!ctx->use_streaming) {
        return Status::InvalidArgument("Not support file format in stream load with sql");
    }

    // put request
    TStreamLoadPutRequest request;
    set_request_auth(&request, ctx->auth);
    request.txnId = ctx->txn_id;
    request.__set_version(version);
    request.__set_load_sql(http_req->header(HTTP_SQL));
    request.__set_loadId(ctx->id.to_thrift());
    request.__set_label(ctx->label);
    if (_exec_env->master_info()->__isset.backend_id) {
        request.__set_backend_id(_exec_env->master_info()->backend_id);
    } else {
        LOG(WARNING) << "_exec_env->master_info not set backend_id";
    }
    if (!http_req->header(HTTP_EXEC_MEM_LIMIT).empty()) {
        try {
            request.__set_execMemLimit(std::stoll(http_req->header(HTTP_EXEC_MEM_LIMIT)));
        } catch (const std::invalid_argument& e) {
            return Status::InvalidArgument("Invalid mem limit format");
        }
    } else {
        request.__set_execMemLimit(config::stream_load_exec_mem_limit);
    }
    request.fileType = TFileType::FILE_STREAM;
    if (ctx->timeout_second != -1) {
        request.__set_timeout(ctx->timeout_second);
    } else {
        request.__set_timeout(config::stream_load_timeout_second);
    }
    request.__set_thrift_rpc_timeout_ms(config::thrift_rpc_timeout_ms);

    // exec this load
    TNetworkAddress master_addr = _exec_env->master_info()->network_address;
    int64_t stream_load_put_start_time = MonotonicNanos();
    RETURN_IF_ERROR(ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&request, ctx](FrontendServiceConnection& client) {
                client->streamLoadPut(ctx->put_result, request);
            }));
    ctx->stream_load_put_cost_nanos = MonotonicNanos() - stream_load_put_start_time;
    Status plan_status(ctx->put_result.status);
    if (!plan_status.ok()) {
        LOG(WARNING) << "exec streaming load failed. errmsg=" << plan_status << ctx->brief();
        return plan_status;
    }
    ctx->is_stream_load_put_success = true;
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
