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

#include "http/action/stream_load.h"

#include <deque>
#include <future>
#include <sstream>

// use string iequal 
#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/http.h>
#include <rapidjson/prettywriter.h>
#include <thrift/protocol/TDebugProtocol.h>

#include "common/logging.h"
#include "exec/schema_scanner/frontend_helper.h"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/FrontendService_types.h"
#include "gen_cpp/HeartbeatService_types.h"
#include "http/http_channel.h"
#include "http/http_common.h"
#include "http/http_request.h"
#include "http/http_response.h"
#include "http/http_headers.h"
#include "http/utils.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "runtime/load_path_mgr.h"
#include "runtime/plan_fragment_executor.h"
#include "runtime/stream_load_pipe.h"
#include "runtime/load_stream_mgr.h"
#include "util/byte_buffer.h"
#include "util/debug_util.h"
#include "util/json_util.h"
#include "util/metrics.h"
#include "util/doris_metrics.h"
#include "util/time.h"
#include "util/uid_util.h"

namespace doris {

IntCounter k_streaming_load_requests_total;
IntCounter k_streaming_load_bytes;
IntCounter k_streaming_load_duration_ms;
static IntGauge k_streaming_load_current_processing;

#ifdef BE_TEST
TLoadTxnBeginResult k_stream_load_begin_result;
TLoadTxnCommitResult k_stream_load_commit_result;
TLoadTxnRollbackResult k_stream_load_rollback_result;
TStreamLoadPutResult k_stream_load_put_result;
Status k_stream_load_plan_status;
#endif

static TFileFormatType::type parse_format(const std::string& format_str) {
    if (boost::iequals(format_str, "CSV")) {
        return TFileFormatType::FORMAT_CSV_PLAIN;
    }
    return TFileFormatType::FORMAT_UNKNOWN;
}

static bool is_format_support_streaming(TFileFormatType::type format) {
    switch (format) {
    case TFileFormatType::FORMAT_CSV_PLAIN:
        return true;
    default:
        return false;
    }
}

// stream load context
struct StreamLoadContext {
    StreamLoadContext(StreamLoadAction* action_) : action(action_), _refs(0) {
        start_nanos = MonotonicNanos();
    }

    ~StreamLoadContext();

    StreamLoadAction* action;
    // id for each load
    UniqueId id;

    std::string db;
    std::string table;
    // load label, used to identify 
    std::string label;

    std::string user_ip;

    HttpAuthInfo auth;

    // only used to check if we receive whole body
    size_t body_bytes = 0;
    size_t receive_bytes = 0;

    int64_t txn_id = -1;

    bool need_rollback = false;
    // when use_streaming is true, we use stream_pipe to send source data,
    // otherwise we save source data to file first, then process it.
    bool use_streaming = false;
    TFileFormatType::type format = TFileFormatType::FORMAT_CSV_PLAIN;

    std::shared_ptr<MessageBodySink> body_sink;

    TStreamLoadPutResult put_result;
    double max_filter_ratio = 0.0;
    std::vector<TTabletCommitInfo> commit_infos;

    std::promise<Status> promise;
    std::future<Status> future = promise.get_future();

    Status status;

    int64_t number_loaded_rows = 0;
    int64_t number_filtered_rows = 0;
    int64_t start_nanos = 0;
    int64_t load_cost_nanos = 0;
    std::string error_url;

    std::string to_json() const;

    void ref() { _refs.fetch_add(1); }
    // If unref() returns true, this object should be delete
    bool unref() { return _refs.fetch_sub(1) == 1; }
private:
    std::atomic<int> _refs;
};

StreamLoadContext::~StreamLoadContext() {
    if (need_rollback) {
        action->rollback(this);
        need_rollback = false;
    }
}

std::string StreamLoadContext::to_json() const {
    rapidjson::StringBuffer s;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(s);

    writer.StartObject();
    // status
    writer.Key("Status");
    switch (status.code()) {
    case TStatusCode::OK:
        writer.String("Success");
        break;
    case TStatusCode::PUBLISH_TIMEOUT:
        writer.String("Publish Timeout");
        break;
    case TStatusCode::LABEL_ALREADY_EXISTS:
        writer.String("Label Already Exists");
        break;
    default:
        writer.String("Fail");
        break;
    }
    // msg
    writer.Key("Message");
    if (status.ok()) {
        writer.String("OK");
    } else {
        writer.String(status.get_error_msg().c_str());
    }
    // number_load_rows
    writer.Key("NumberLoadedRows");
    writer.Int64(number_loaded_rows);
    writer.Key("NumberFilteredRows");
    writer.Int64(number_filtered_rows);
    writer.Key("LoadBytes");
    writer.Int64(receive_bytes);
    writer.Key("LoadTimeMs");
    writer.Int64(load_cost_nanos / 1000000);
    if (!error_url.empty()) {
        writer.Key("ErrorURL");
        writer.String(error_url.c_str());
    }
    writer.EndObject();
    return s.GetString();
}

StreamLoadAction::StreamLoadAction(ExecEnv* exec_env) : _exec_env(exec_env) {
    DorisMetrics::metrics()->register_metric("streaming_load_requests_total",
                                            &k_streaming_load_requests_total);
    DorisMetrics::metrics()->register_metric("streaming_load_bytes",
                                            &k_streaming_load_bytes);
    DorisMetrics::metrics()->register_metric("streaming_load_duration_ms",
                                            &k_streaming_load_duration_ms);
    DorisMetrics::metrics()->register_metric("streaming_load_current_processing",
                                            &k_streaming_load_current_processing);
}

StreamLoadAction::~StreamLoadAction() {
}

void StreamLoadAction::handle(HttpRequest* req) {
    StreamLoadContext* ctx = (StreamLoadContext*) req->handler_ctx();
    if (ctx == nullptr) {
        return;
    }

    // status already set to fail
    if (ctx->status.ok()) {
        ctx->status = _handle(ctx);
        if (!ctx->status.ok()) {
            LOG(WARNING) << "handle streaming load failed, id=" << ctx->id
                << ", errmsg=" << ctx->status.get_error_msg();
        }
    }
    ctx->load_cost_nanos = MonotonicNanos() - ctx->start_nanos;

    if (!ctx->status.ok()) {
        if (ctx->need_rollback) {
            rollback(ctx);
            ctx->need_rollback = false;
        }
        if (ctx->body_sink.get() != nullptr) {
            ctx->body_sink->cancel();
        }
    }

    auto str = ctx->to_json();
    HttpChannel::send_reply(req, str);

    // update statstics
    k_streaming_load_requests_total.increment(1);
    k_streaming_load_duration_ms.increment(ctx->load_cost_nanos / 1000000);
    k_streaming_load_bytes.increment(ctx->receive_bytes);
    k_streaming_load_current_processing.increment(-1);
}

Status StreamLoadAction::_handle(StreamLoadContext* ctx) {
    if (ctx->body_bytes > 0 && ctx->receive_bytes != ctx->body_bytes) {
        LOG(WARNING) << "recevie body don't equal with body bytes, body_bytes="
            << ctx->body_bytes << ", receive_bytes=" << ctx->receive_bytes
            << ", id=" << ctx->id;
        return Status("receive body dont't equal with body bytes");
    }
    if (!ctx->use_streaming) {
        // if we use non-streaming, we need to close file first,
        // then _execute_plan_fragment here
        // this will close file
        ctx->body_sink.reset();
        RETURN_IF_ERROR(_execute_plan_fragment(ctx));
    } else {
        RETURN_IF_ERROR(ctx->body_sink->finish());
    }

    // wait stream load finish
    RETURN_IF_ERROR(ctx->future.get());

    // If put file succeess we need commit this load
    TLoadTxnCommitRequest request;
    set_http_auth(&request, ctx->auth);
    request.db = ctx->db;
    request.tbl = ctx->table;
    request.txnId = ctx->txn_id;
    request.sync = true;
    request.commitInfos = std::move(ctx->commit_infos);
    request.__isset.commitInfos = true;

    TNetworkAddress master_addr = _exec_env->master_info()->network_address;
    TLoadTxnCommitResult result;
#ifndef BE_TEST
    RETURN_IF_ERROR(FrontendHelper::rpc(
        master_addr.hostname, master_addr.port,
        [&request, &result] (FrontendServiceConnection& client) {
            client->loadTxnCommit(result, request);
        }, 10000));
#else
    result = k_stream_load_commit_result;
#endif
    // Return if this transaction is committed successful; otherwise, we need try to
    // rollback this transaction
    Status status(result.status);
    if (!status.ok()) {
        LOG(WARNING) << "commit transaction failed, id=" << ctx->id
            << ", errmsg=" << status.get_error_msg();
        return status;
    }
    // commit success, set need_rollback to false
    ctx->need_rollback = false;

    return Status::OK;
}

int StreamLoadAction::on_header(HttpRequest* req) {
    k_streaming_load_current_processing.increment(1);

    StreamLoadContext* ctx = new StreamLoadContext(this);
    ctx->ref();
    req->set_handler_ctx(ctx);

    ctx->db = req->param(HTTP_DB_KEY);
    ctx->table = req->param(HTTP_TABLE_KEY);
    ctx->label = req->header(HTTP_LABEL_KEY);
    if (ctx->label.empty()) {
        ctx->label = generate_uuid_string();
    }

    LOG(INFO) << "new income streaming load request, id=" << ctx->id
        << ", db=" << ctx->db << ", table=" << ctx->table << ", label=" << ctx->label;

    auto st = _on_header(req, ctx);
    if (!st.ok()) {
        ctx->status = st;
        if (ctx->need_rollback) {
            rollback(ctx);
            ctx->need_rollback = false;
        }
        if (ctx->body_sink.get() != nullptr) {
            ctx->body_sink->cancel();
        }
        auto str = ctx->to_json();
        HttpChannel::send_reply(req, str);
        k_streaming_load_current_processing.increment(-1);
        return -1;
    }
    return 0;
}

Status StreamLoadAction::_on_header(HttpRequest* http_req, StreamLoadContext* ctx) {
    // auth information
    if (!parse_basic_auth(*http_req, &ctx->auth)) {
        LOG(WARNING) << "parse basic authorization failed, id=" << ctx->id;
        return Status("no valid Basic authorization");
    }
    // check content length
    ctx->body_bytes = 0;
    size_t max_body_bytes = config::streaming_load_max_mb * 1024 * 1024;
    if (!http_req->header(HttpHeaders::CONTENT_LENGTH).empty()) {
        ctx->body_bytes = std::stol(http_req->header(HttpHeaders::CONTENT_LENGTH));
        if (ctx->body_bytes > max_body_bytes) {
            LOG(WARNING) << "body exceed max size, id=" << ctx->id;

            std::stringstream ss;
            ss << "body exceed max size, max_body_bytes=" << max_body_bytes;
            return Status(ss.str());
        }
    } else {
#ifndef BE_TEST
        evhttp_connection_set_max_body_size(
            evhttp_request_get_connection(http_req->get_evhttp_request()),
            max_body_bytes);
#endif
    }
    // get format of this put
    if (http_req->header(HTTP_FORMAT_KEY).empty()) {
        ctx->format = TFileFormatType::FORMAT_CSV_PLAIN;
    } else {
        ctx->format = parse_format(http_req->header(HTTP_FORMAT_KEY));
        if (ctx->format == TFileFormatType::FORMAT_UNKNOWN) {
            LOG(WARNING) << "unknown data format, id=" << ctx->id;
            std::stringstream ss;
            ss << "unknown data format, format=" << http_req->header(HTTP_FORMAT_KEY);
            return Status(ss.str());
        }
    }

    TNetworkAddress master_addr = _exec_env->master_info()->network_address;

    // begin transaction
    {
        TLoadTxnBeginRequest request;
        set_http_auth(&request, ctx->auth);
        request.db = ctx->db;
        request.tbl = ctx->table;
        request.label = ctx->label;

        TLoadTxnBeginResult result;
#ifndef BE_TEST
        RETURN_IF_ERROR(FrontendHelper::rpc(
                master_addr.hostname, master_addr.port,
                [&request, &result] (FrontendServiceConnection& client) {
                    client->loadTxnBegin(result, request);
                }));
#else
        result = k_stream_load_begin_result;
#endif
        Status status(result.status);
        if (!status.ok()) {
            LOG(WARNING) << "begin transaction failed, id=" << ctx->id
                << "errmsg=" << status.get_error_msg();
            return status;
        }
        ctx->txn_id = result.txnId;
        ctx->need_rollback = true;
    }

    // process put file
    return _process_put(http_req, ctx);
}

void StreamLoadAction::on_chunk_data(HttpRequest* req) {
    StreamLoadContext* ctx = (StreamLoadContext*)req->handler_ctx();
    if (ctx == nullptr || !ctx->status.ok()) {
        return;
    }

    struct evhttp_request* ev_req = req->get_evhttp_request();
    auto evbuf = evhttp_request_get_input_buffer(ev_req);

    while (evbuffer_get_length(evbuf) > 0) {
        auto bb = ByteBuffer::allocate(4096);
        auto remove_bytes = evbuffer_remove(evbuf, bb->ptr, bb->capacity);
        bb->pos = remove_bytes;
        bb->flip();
        auto st = ctx->body_sink->append(bb);
        if (!st.ok()) {
            LOG(WARNING) << "append body content failed, id=" << ctx->id
                << ", errmsg=" << st.get_error_msg();
            ctx->status = st;
            return;
        }
        ctx->receive_bytes += remove_bytes;
    }
}

void StreamLoadAction::free_handler_ctx(void* param) {
    StreamLoadContext* ctx = (StreamLoadContext*)param;
    if (ctx == nullptr) {
        return;
    }
    // sender is going, make receiver know it
    if (ctx->body_sink != nullptr) {
        ctx->body_sink->cancel();
    }
    if (ctx->unref()) {
        delete ctx;
    }
}

Status StreamLoadAction::_process_put(HttpRequest* http_req, StreamLoadContext* ctx) {
    // Now we use stream
    ctx->use_streaming = is_format_support_streaming(ctx->format);
    
    // put request
    TStreamLoadPutRequest request;
    set_http_auth(&request, ctx->auth);
    request.db = ctx->db;
    request.tbl = ctx->table;
    request.txnId = ctx->txn_id;
    request.formatType = ctx->format;
    request.__set_loadId(ctx->id.to_thrift());
    if (ctx->use_streaming) {
        auto pipe = std::make_shared<StreamLoadPipe>();
        RETURN_IF_ERROR(_exec_env->load_stream_mgr()->put(ctx->id, pipe));
        request.fileType = TFileType::FILE_STREAM;
        ctx->body_sink = pipe;
    } else {
        RETURN_IF_ERROR(_data_saved_path(http_req, &request.path));
        auto file_sink = std::make_shared<MessageBodyFileSink>(request.path);
        RETURN_IF_ERROR(file_sink->open());
        request.__isset.path = true;
        request.fileType = TFileType::FILE_LOCAL;
        ctx->body_sink = file_sink;
    }
    if (!http_req->header(HTTP_COLUMNS).empty()) {
        request.__set_columns(http_req->header(HTTP_COLUMNS));
    }
    if (!http_req->header(HTTP_WHERE).empty()) {
        request.__set_where(http_req->header(HTTP_WHERE));
    }
    if (!http_req->header(HTTP_COLUMN_SEPARATOR).empty()) {
        request.__set_columnSeparator(http_req->header(HTTP_COLUMN_SEPARATOR));
    }
    if (!http_req->header(HTTP_PARTITIONS).empty()) {
        request.__set_partitions(http_req->header(HTTP_PARTITIONS));
    }

    // plan this load
    TNetworkAddress master_addr = _exec_env->master_info()->network_address;
#ifndef BE_TEST
    if (!http_req->header(HTTP_MAX_FILTER_RATIO).empty()) {
        ctx->max_filter_ratio = strtod(http_req->header(HTTP_MAX_FILTER_RATIO).c_str(), nullptr);
    }

    RETURN_IF_ERROR(FrontendHelper::rpc(
                master_addr.hostname, master_addr.port,
            [&request, ctx] (FrontendServiceConnection& client) {
            client->streamLoadPut(ctx->put_result, request);
            }));
#else
    ctx->put_result = k_stream_load_put_result;
#endif
    Status plan_status(ctx->put_result.status);
    if (!plan_status.ok()) {
        LOG(WARNING) << "plan streaming load failed, id=" << ctx->id
            << ", txn_id=" << ctx->txn_id
            << ", errmsg=" << plan_status.get_error_msg();
        return plan_status;
    }
    VLOG(3) << "params is " << apache::thrift::ThriftDebugString(ctx->put_result.params);
    // if we not use streaming, we must download total content before we begin
    // to process this load
    if (!ctx->use_streaming) {
        return Status::OK;
    }
    return _execute_plan_fragment(ctx);
}

Status StreamLoadAction::_execute_plan_fragment(StreamLoadContext* ctx) {
    // submit this params
#ifndef BE_TEST
    ctx->ref();
    auto st = _exec_env->fragment_mgr()->exec_plan_fragment(
        ctx->put_result.params,
        [ctx] (PlanFragmentExecutor* executor) {
            ctx->commit_infos = std::move(executor->runtime_state()->tablet_commit_infos());
            Status status = executor->status();
            if (status.ok()) {
                ctx->number_loaded_rows = executor->runtime_state()->num_rows_load_success();
                ctx->number_filtered_rows = executor->runtime_state()->num_rows_load_filtered();
                int64_t num_total_rows =
                    ctx->number_loaded_rows + ctx->number_filtered_rows;
                if ((0.0 + ctx->number_filtered_rows) / num_total_rows > ctx->max_filter_ratio) {
                    status = Status("too many filtered rows");
                }
                if (ctx->number_filtered_rows > 0 &&
                    !executor->runtime_state()->get_error_log_file_path().empty()) {
                    ctx->error_url = to_load_error_http_path(
                        executor->runtime_state()->get_error_log_file_path());
                }
            } else {
                LOG(WARNING) << "fragment execute failed, load_id=" << ctx->id
                    << ", txn_id=" << ctx->txn_id
                    << ", query_id=" << UniqueId(ctx->put_result.params.params.query_id)
                    << ", errmsg=" << status.get_error_msg();
                // cancel body_sink, make sender known it
                if (ctx->body_sink != nullptr) {
                    ctx->body_sink->cancel();
                }
            }
            ctx->promise.set_value(status);
            if (ctx->unref()) {
                delete ctx;
            }
        });
    if (!st.ok()) {
        // no need to check unref's return value
        ctx->unref();
        return st;
    }
#else
    ctx->promise.set_value(k_stream_load_plan_status);
#endif
    return Status::OK;
}

void StreamLoadAction::rollback(StreamLoadContext* ctx) {
    TNetworkAddress master_addr = _exec_env->master_info()->network_address;
    TLoadTxnRollbackRequest request;
    set_http_auth(&request, ctx->auth);
    request.db = ctx->db;
    request.tbl = ctx->table;
    request.txnId = ctx->txn_id;
    request.__set_reason(ctx->status.get_error_msg());
    TLoadTxnRollbackResult result;
#ifndef BE_TEST
    auto rpc_st = FrontendHelper::rpc(
        master_addr.hostname, master_addr.port,
        [&request, &result] (FrontendServiceConnection& client) {
            client->loadTxnRollback(result, request);
        });
    if (!rpc_st.ok()) {
        LOG(WARNING) << "transaction rollback failed, id=" << ctx->id
            << ", txn_id=" << ctx->txn_id
            << ", errmsg=" << rpc_st.get_error_msg();
    }
#else
    result = k_stream_load_rollback_result;
#endif
}

Status StreamLoadAction::_data_saved_path(HttpRequest* req, std::string* file_path) {
    std::string prefix;
    RETURN_IF_ERROR(_exec_env->load_path_mgr()->allocate_dir(req->param(HTTP_DB_KEY), "", &prefix));
    timeval tv;
    gettimeofday(&tv, nullptr);
    struct tm tm;
    time_t cur_sec = tv.tv_sec;
    localtime_r(&cur_sec, &tm);
    char buf[64];
    strftime(buf, 64, "%Y%m%d%H%M%S", &tm);
    std::stringstream ss;
    ss << prefix << "/" << req->param(HTTP_TABLE_KEY) << "." << buf << "." << tv.tv_usec;
    *file_path = ss.str();
    return Status::OK;
}

}

