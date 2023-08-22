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

#include <cstddef>
#include <deque>
#include <future>
#include <shared_mutex>
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
#include "util/load_util.h"
#include "util/metrics.h"
#include "util/string_util.h"
#include "util/thrift_rpc_helper.h"
#include "util/time.h"
#include "util/uid_util.h"

// TODO The functions in this file need to be improved
namespace doris {
using namespace ErrorCode;

DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(streaming_load_with_sql_requests_total, MetricUnit::REQUESTS);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(streaming_load_with_sql_duration_ms, MetricUnit::MILLISECONDS);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(streaming_load_with_sql_current_processing,
                                   MetricUnit::REQUESTS);

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
    std::shared_ptr<StreamLoadContext> ctx =
            std::static_pointer_cast<StreamLoadContext>(req->handler_ctx());
    if (ctx == nullptr) {
        return;
    }

    // status already set to fail
    if (ctx->status.ok()) {
        ctx->status = _handle(req, ctx);
        if (!ctx->status.ok() && !ctx->status.is<PUBLISH_TIMEOUT>()) {
            LOG(WARNING) << "handle streaming load failed, id=" << ctx->id
                         << ", errmsg=" << ctx->status;
        }
    }
    ctx->load_cost_millis = UnixMillis() - ctx->start_millis;

    if (!ctx->status.ok() && !ctx->status.is<PUBLISH_TIMEOUT>()) {
        if (ctx->body_sink != nullptr) {
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
    ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&request, &result](FrontendServiceConnection& client) {
                client->streamLoadWithLoadStatus(result, request);
            });
    Status stream_load_status(Status::create(result.status));
    if (stream_load_status.ok()) {
        ctx->txn_id = result.txn_id;
        ctx->number_total_rows = result.total_rows;
        ctx->number_loaded_rows = result.loaded_rows;
        ctx->number_filtered_rows = result.filtered_rows;
        ctx->number_unselected_rows = result.unselected_rows;
    }

    auto str = std::string(ctx->to_json());
    // add new line at end
    str = str + '\n';
    HttpChannel::send_reply(req, str);
    if (config::enable_stream_load_record) {
        str = ctx->prepare_stream_load_record(str);
        _save_stream_load_record(ctx, str);
    }
    // update statistics
    streaming_load_with_sql_requests_total->increment(1);
    streaming_load_with_sql_duration_ms->increment(ctx->load_cost_millis);
    streaming_load_with_sql_current_processing->increment(-1);
}

Status StreamLoadWithSqlAction::_handle(HttpRequest* req, std::shared_ptr<StreamLoadContext> ctx) {
    if (ctx->body_bytes > 0 && ctx->receive_bytes != ctx->body_bytes) {
        LOG(WARNING) << "recevie body don't equal with body bytes, body_bytes=" << ctx->body_bytes
                     << ", receive_bytes=" << ctx->receive_bytes << ", id=" << ctx->id;
        return Status::InternalError("receive body don't equal with body bytes");
    }
    if (!ctx->use_streaming) {
        // if we use non-streaming, we need to close file first,
        // then execute_plan_fragment here
        // this will close file
        ctx->body_sink.reset();
        // TODO This function may not be placed here
        _process_put(req, ctx);
    } else {
        RETURN_IF_ERROR(ctx->body_sink->finish());
    }
    // TODO support parquet and orc
    RETURN_IF_ERROR(ctx->future.get());
    return ctx->status;
}

int StreamLoadWithSqlAction::on_header(HttpRequest* req) {
    streaming_load_with_sql_current_processing->increment(1);

    std::shared_ptr<StreamLoadContext> ctx = std::make_shared<StreamLoadContext>(_exec_env);
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
        if (ctx->body_sink != nullptr) {
            ctx->body_sink->cancel(ctx->status.to_string());
        }
        auto str = ctx->to_json();
        // add new line at end
        str = str + '\n';
        HttpChannel::send_reply(req, str);
        streaming_load_with_sql_current_processing->increment(-1);
        if (config::enable_stream_load_record) {
            str = ctx->prepare_stream_load_record(str);
            _save_stream_load_record(ctx, str);
        }
        return -1;
    }
    return 0;
}

// TODO The parameters of this function may need to be refactored because the parameters in HttpRequest are not sufficient.
Status StreamLoadWithSqlAction::_on_header(HttpRequest* http_req,
                                           std::shared_ptr<StreamLoadContext> ctx) {
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
    LoadUtil::parse_format(format_str, http_req->header(HTTP_COMPRESS_TYPE), &ctx->format,
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
        evhttp_connection_set_max_body_size(
                evhttp_request_get_connection(http_req->get_evhttp_request()), csv_max_body_bytes);
    }

    if (!http_req->header(HTTP_TIMEOUT).empty()) {
        try {
            ctx->timeout_second = std::stoi(http_req->header(HTTP_TIMEOUT));
        } catch (const std::invalid_argument& e) {
            return Status::InvalidArgument("Invalid timeout format, {}", e.what());
        }
    }

    ctx->use_streaming = LoadUtil::is_format_support_streaming(ctx->format);
    if (ctx->use_streaming) {
        // create stream load pipe for fetch schema
        auto pipe = std::make_shared<io::StreamLoadPipe>(
                io::kMaxPipeBufferedBytes /* max_buffered_bytes */, 64 * 1024 /* min_chunk_size */,
                ctx->body_bytes /* total_length */);
        ctx->body_sink = pipe;
        ctx->pipe = pipe;
    } else {
        // TODO here need _data_saved_path function and file_sink
    }
    RETURN_IF_ERROR(_exec_env->new_load_stream_mgr()->put(ctx->id, ctx));
    ctx->txn_id = 0;

    return Status::OK();
}

void StreamLoadWithSqlAction::on_chunk_data(HttpRequest* req) {
    std::shared_ptr<StreamLoadContext> ctx =
            std::static_pointer_cast<StreamLoadContext>(req->handler_ctx());
    if (ctx == nullptr || !ctx->status.ok()) {
        return;
    }

    struct evhttp_request* ev_req = req->get_evhttp_request();
    auto evbuf = evhttp_request_get_input_buffer(ev_req);

    int64_t start_read_data_time = MonotonicNanos();
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
    }
    ctx->read_data_cost_nanos += (MonotonicNanos() - start_read_data_time);
}

void StreamLoadWithSqlAction::free_handler_ctx(std::shared_ptr<void> param) {
    std::shared_ptr<StreamLoadContext> ctx = std::static_pointer_cast<StreamLoadContext>(param);
    if (ctx == nullptr) {
        return;
    }
    // sender is gone, make receiver know it
    if (ctx->body_sink != nullptr) {
        ctx->body_sink->cancel("sender is gone");
    }
    // remove stream load context from stream load manager and the resource will be released
    ctx->exec_env()->new_load_stream_mgr()->remove(ctx->id);
}

Status StreamLoadWithSqlAction::_process_put(HttpRequest* http_req,
                                             std::shared_ptr<StreamLoadContext> ctx) {
    // put request
    TStreamLoadPutRequest request;
    set_request_auth(&request, ctx->auth);
    request.txnId = ctx->txn_id;
    request.__set_version(1);
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
            return Status::InvalidArgument("Invalid mem limit format, {}", e.what());
        }
    }
    if (ctx->use_streaming) {
        request.fileType = TFileType::FILE_STREAM;
    } else {
        request.__isset.path = true;
        request.fileType = TFileType::FILE_LOCAL;
        request.__set_file_size(ctx->body_bytes);
    }
    if (ctx->timeout_second != -1) {
        request.__set_timeout(ctx->timeout_second);
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
    Status plan_status(Status::create(ctx->put_result.status));
    if (!plan_status.ok()) {
        LOG(WARNING) << "exec streaming load failed. errmsg=" << plan_status << ctx->brief();
        return plan_status;
    }
    // TODO perhaps the `execute_plan_fragment` function needs to be executed here
    return Status::OK();
}

Status StreamLoadWithSqlAction::_data_saved_path(HttpRequest* req, std::string* file_path) {
    std::string prefix;
    RETURN_IF_ERROR(
            _exec_env->load_path_mgr()->allocate_dir("stream_load_local_file", "", &prefix));
    timeval tv;
    gettimeofday(&tv, nullptr);
    struct tm tm;
    time_t cur_sec = tv.tv_sec;
    localtime_r(&cur_sec, &tm);
    char buf[64];
    strftime(buf, 64, "%Y%m%d%H%M%S", &tm);
    std::stringstream ss;
    ss << prefix << buf << "." << tv.tv_usec;
    *file_path = ss.str();
    return Status::OK();
}

void StreamLoadWithSqlAction::_save_stream_load_record(std::shared_ptr<StreamLoadContext> ctx,
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
