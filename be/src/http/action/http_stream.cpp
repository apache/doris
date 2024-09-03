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

#include "http/action/http_stream.h"

#include <cstddef>
#include <future>
#include <sstream>

// use string iequal
#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/http.h>
#include <rapidjson/prettywriter.h>
#include <thrift/protocol/TDebugProtocol.h>

#include "cloud/config.h"
#include "common/config.h"
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
#include "http/utils.h"
#include "io/fs/stream_load_pipe.h"
#include "olap/storage_engine.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "runtime/group_commit_mgr.h"
#include "runtime/load_path_mgr.h"
#include "runtime/stream_load/new_load_stream_mgr.h"
#include "runtime/stream_load/stream_load_context.h"
#include "runtime/stream_load/stream_load_executor.h"
#include "runtime/stream_load/stream_load_recorder.h"
#include "util/byte_buffer.h"
#include "util/doris_metrics.h"
#include "util/metrics.h"
#include "util/string_util.h"
#include "util/thrift_rpc_helper.h"
#include "util/time.h"
#include "util/uid_util.h"

namespace doris {
using namespace ErrorCode;

DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(http_stream_requests_total, MetricUnit::REQUESTS);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(http_stream_duration_ms, MetricUnit::MILLISECONDS);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(http_stream_current_processing, MetricUnit::REQUESTS);

HttpStreamAction::HttpStreamAction(ExecEnv* exec_env) : _exec_env(exec_env) {
    _http_stream_entity =
            DorisMetrics::instance()->metric_registry()->register_entity("http_stream");
    INT_COUNTER_METRIC_REGISTER(_http_stream_entity, http_stream_requests_total);
    INT_COUNTER_METRIC_REGISTER(_http_stream_entity, http_stream_duration_ms);
    INT_GAUGE_METRIC_REGISTER(_http_stream_entity, http_stream_current_processing);
}

HttpStreamAction::~HttpStreamAction() {
    DorisMetrics::instance()->metric_registry()->deregister_entity(_http_stream_entity);
}

void HttpStreamAction::handle(HttpRequest* req) {
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
    auto str = std::string(ctx->to_json());
    // add new line at end
    str = str + '\n';
    HttpChannel::send_reply(req, str);
    if (config::enable_stream_load_record) {
        str = ctx->prepare_stream_load_record(str);
        _save_stream_load_record(ctx, str);
    }
    // update statistics
    http_stream_requests_total->increment(1);
    http_stream_duration_ms->increment(ctx->load_cost_millis);
}

Status HttpStreamAction::_handle(HttpRequest* http_req, std::shared_ptr<StreamLoadContext> ctx) {
    if (ctx->body_bytes > 0 && ctx->receive_bytes != ctx->body_bytes) {
        LOG(WARNING) << "recevie body don't equal with body bytes, body_bytes=" << ctx->body_bytes
                     << ", receive_bytes=" << ctx->receive_bytes << ", id=" << ctx->id;
        return Status::Error<ErrorCode::NETWORK_ERROR>("receive body don't equal with body bytes");
    }
    RETURN_IF_ERROR(ctx->body_sink->finish());

    // wait stream load finish
    RETURN_IF_ERROR(ctx->future.get());

    if (ctx->group_commit) {
        LOG(INFO) << "skip commit because this is group commit, pipe_id=" << ctx->id.to_string();
        return Status::OK();
    }

    if (ctx->two_phase_commit) {
        int64_t pre_commit_start_time = MonotonicNanos();
        RETURN_IF_ERROR(_exec_env->stream_load_executor()->pre_commit_txn(ctx.get()));
        ctx->pre_commit_txn_cost_nanos = MonotonicNanos() - pre_commit_start_time;
    } else {
        // If put file success we need commit this load
        int64_t commit_and_publish_start_time = MonotonicNanos();
        RETURN_IF_ERROR(_exec_env->stream_load_executor()->commit_txn(ctx.get()));
        ctx->commit_and_publish_txn_cost_nanos = MonotonicNanos() - commit_and_publish_start_time;
    }
    return Status::OK();
}

int HttpStreamAction::on_header(HttpRequest* req) {
    http_stream_current_processing->increment(1);

    std::shared_ptr<StreamLoadContext> ctx = std::make_shared<StreamLoadContext>(_exec_env);
    req->set_handler_ctx(ctx);

    ctx->load_type = TLoadType::MANUL_LOAD;
    ctx->load_src_type = TLoadSourceType::RAW;
    ctx->two_phase_commit = req->header(HTTP_TWO_PHASE_COMMIT) == "true";
    Status st = _handle_group_commit(req, ctx);

    LOG(INFO) << "new income streaming load request." << ctx->brief()
              << " sql : " << req->header(HTTP_SQL) << ", group_commit=" << ctx->group_commit;
    if (st.ok()) {
        st = _on_header(req, ctx);
    }
    if (!st.ok()) {
        ctx->status = std::move(st);
        if (ctx->body_sink != nullptr) {
            ctx->body_sink->cancel(ctx->status.to_string());
        }
        auto str = ctx->to_json();
        // add new line at end
        str = str + '\n';
        HttpChannel::send_reply(req, str);
        if (config::enable_stream_load_record) {
            str = ctx->prepare_stream_load_record(str);
            _save_stream_load_record(ctx, str);
        }
        return -1;
    }
    return 0;
}

Status HttpStreamAction::_on_header(HttpRequest* http_req, std::shared_ptr<StreamLoadContext> ctx) {
    // auth information
    if (!parse_basic_auth(*http_req, &ctx->auth)) {
        LOG(WARNING) << "parse basic authorization failed." << ctx->brief();
        return Status::NotAuthorized("no valid Basic authorization");
    }

    // TODO(zs) : need Need to request an FE to obtain information such as format
    // check content length
    ctx->body_bytes = 0;
    size_t csv_max_body_bytes = config::streaming_load_max_mb * 1024 * 1024;
    if (!http_req->header(HttpHeaders::CONTENT_LENGTH).empty()) {
        ctx->body_bytes = std::stol(http_req->header(HttpHeaders::CONTENT_LENGTH));
        // csv max body size
        if (ctx->body_bytes > csv_max_body_bytes) {
            LOG(WARNING) << "body exceed max size." << ctx->brief();
            return Status::Error<ErrorCode::EXCEEDED_LIMIT>(
                    "body size {} exceed BE's conf `streaming_load_max_mb` {}. increase it if you "
                    "are sure this load is reasonable",
                    ctx->body_bytes, csv_max_body_bytes);
        }
    }

    auto pipe = std::make_shared<io::StreamLoadPipe>(
            io::kMaxPipeBufferedBytes /* max_buffered_bytes */, 64 * 1024 /* min_chunk_size */,
            ctx->body_bytes /* total_length */);
    ctx->body_sink = pipe;
    ctx->pipe = pipe;

    RETURN_IF_ERROR(_exec_env->new_load_stream_mgr()->put(ctx->id, ctx));

    // Here, transactions are set from fe's NativeInsertStmt.
    // TODO(zs) : How to support two_phase_commit

    return Status::OK();
}

void HttpStreamAction::on_chunk_data(HttpRequest* req) {
    std::shared_ptr<StreamLoadContext> ctx =
            std::static_pointer_cast<StreamLoadContext>(req->handler_ctx());
    if (ctx == nullptr || !ctx->status.ok()) {
        return;
    }
    if (!req->header(HTTP_WAL_ID_KY).empty()) {
        ctx->wal_id = std::stoll(req->header(HTTP_WAL_ID_KY));
    }
    struct evhttp_request* ev_req = req->get_evhttp_request();
    auto evbuf = evhttp_request_get_input_buffer(ev_req);

    SCOPED_ATTACH_TASK(ExecEnv::GetInstance()->stream_load_pipe_tracker());

    int64_t start_read_data_time = MonotonicNanos();
    Status st = ctx->allocate_schema_buffer();
    if (!st.ok()) {
        ctx->status = st;
        return;
    }
    while (evbuffer_get_length(evbuf) > 0) {
        ByteBufferPtr bb;
        st = ByteBuffer::allocate(128 * 1024, &bb);
        if (!st.ok()) {
            ctx->status = st;
            return;
        }
        auto remove_bytes = evbuffer_remove(evbuf, bb->ptr, bb->capacity);
        bb->pos = remove_bytes;
        bb->flip();
        st = ctx->body_sink->append(bb);
        // schema_buffer stores 1M of data for parsing column information
        // need to determine whether to cache for the first time
        if (ctx->is_read_schema) {
            if (ctx->schema_buffer()->pos + remove_bytes < config::stream_tvf_buffer_size) {
                ctx->schema_buffer()->put_bytes(bb->ptr, remove_bytes);
            } else {
                LOG(INFO) << "use a portion of data to request fe to obtain column information";
                ctx->is_read_schema = false;
                ctx->status = process_put(req, ctx);
            }
        }
        if (!st.ok()) {
            LOG(WARNING) << "append body content failed. errmsg=" << st << ", " << ctx->brief();
            ctx->status = st;
            return;
        }
        ctx->receive_bytes += remove_bytes;
    }
    // after all the data has been read and it has not reached 1M, it will execute here
    if (ctx->is_read_schema) {
        LOG(INFO) << "after all the data has been read and it has not reached 1M, it will execute "
                  << "here";
        ctx->is_read_schema = false;
        ctx->status = process_put(req, ctx);
    }
    ctx->read_data_cost_nanos += (MonotonicNanos() - start_read_data_time);
}

void HttpStreamAction::free_handler_ctx(std::shared_ptr<void> param) {
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
    http_stream_current_processing->increment(-1);
}

Status HttpStreamAction::process_put(HttpRequest* http_req,
                                     std::shared_ptr<StreamLoadContext> ctx) {
    TStreamLoadPutRequest request;
    if (http_req != nullptr) {
        request.__set_load_sql(http_req->header(HTTP_SQL));
        if (!http_req->header(HTTP_MEMTABLE_ON_SINKNODE).empty()) {
            bool value = iequal(http_req->header(HTTP_MEMTABLE_ON_SINKNODE), "true");
            request.__set_memtable_on_sink_node(value);
        }
    } else {
        request.__set_token(ctx->auth.token);
        request.__set_load_sql(ctx->sql_str);
        ctx->auth.token = "";
    }
    set_request_auth(&request, ctx->auth);
    request.__set_loadId(ctx->id.to_thrift());
    request.__set_label(ctx->label);
    if (ctx->group_commit) {
        if (!http_req->header(HTTP_GROUP_COMMIT).empty()) {
            request.__set_group_commit_mode(http_req->header(HTTP_GROUP_COMMIT));
        } else {
            // used for wait_internal_group_commit_finish
            request.__set_group_commit_mode("sync_mode");
        }
    }
    if (_exec_env->master_info()->__isset.backend_id) {
        request.__set_backend_id(_exec_env->master_info()->backend_id);
    } else {
        LOG(WARNING) << "_exec_env->master_info not set backend_id";
    }

    // plan this load
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
        LOG(WARNING) << "plan streaming load failed. errmsg=" << plan_status << ctx->brief();
        return plan_status;
    }
    if (config::is_cloud_mode() && ctx->two_phase_commit && ctx->is_mow_table()) {
        return Status::NotSupported("http stream 2pc is unsupported for mow table");
    }
    ctx->db = ctx->put_result.pipeline_params.db_name;
    ctx->table = ctx->put_result.pipeline_params.table_name;
    ctx->txn_id = ctx->put_result.pipeline_params.txn_conf.txn_id;
    ctx->label = ctx->put_result.pipeline_params.import_label;
    ctx->put_result.pipeline_params.__set_wal_id(ctx->wal_id);
    if (http_req != nullptr && http_req->header(HTTP_GROUP_COMMIT) == "async_mode") {
        // FIXME find a way to avoid chunked stream load write large WALs
        size_t content_length = 0;
        if (!http_req->header(HttpHeaders::CONTENT_LENGTH).empty()) {
            content_length = std::stol(http_req->header(HttpHeaders::CONTENT_LENGTH));
            if (ctx->format == TFileFormatType::FORMAT_CSV_GZ ||
                ctx->format == TFileFormatType::FORMAT_CSV_LZO ||
                ctx->format == TFileFormatType::FORMAT_CSV_BZ2 ||
                ctx->format == TFileFormatType::FORMAT_CSV_LZ4FRAME ||
                ctx->format == TFileFormatType::FORMAT_CSV_LZOP ||
                ctx->format == TFileFormatType::FORMAT_CSV_LZ4BLOCK ||
                ctx->format == TFileFormatType::FORMAT_CSV_SNAPPYBLOCK) {
                content_length *= 3;
            }
        }
        ctx->put_result.pipeline_params.__set_content_length(content_length);
    }

    return _exec_env->stream_load_executor()->execute_plan_fragment(ctx);
}

void HttpStreamAction::_save_stream_load_record(std::shared_ptr<StreamLoadContext> ctx,
                                                const std::string& str) {
    std::shared_ptr<StreamLoadRecorder> stream_load_recorder =
            ExecEnv::GetInstance()->storage_engine().get_stream_load_recorder();

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

Status HttpStreamAction::_handle_group_commit(HttpRequest* req,
                                              std::shared_ptr<StreamLoadContext> ctx) {
    std::string group_commit_mode = req->header(HTTP_GROUP_COMMIT);
    if (!group_commit_mode.empty() && !iequal(group_commit_mode, "sync_mode") &&
        !iequal(group_commit_mode, "async_mode") && !iequal(group_commit_mode, "off_mode")) {
        return Status::InvalidArgument(
                "group_commit can only be [async_mode, sync_mode, off_mode]");
    }
    if (config::wait_internal_group_commit_finish) {
        group_commit_mode = "sync_mode";
    }
    int64_t content_length = req->header(HttpHeaders::CONTENT_LENGTH).empty()
                                     ? 0
                                     : std::stoll(req->header(HttpHeaders::CONTENT_LENGTH));
    if (content_length < 0) {
        std::stringstream ss;
        ss << "This http load content length <0 (" << content_length
           << "), please check your content length.";
        LOG(WARNING) << ss.str();
        return Status::InvalidArgument(ss.str());
    }
    // allow chunked stream load in flink
    auto is_chunk =
            !req->header(HttpHeaders::TRANSFER_ENCODING).empty() &&
            req->header(HttpHeaders::TRANSFER_ENCODING).find("chunked") != std::string::npos;
    if (group_commit_mode.empty() || iequal(group_commit_mode, "off_mode") ||
        (content_length == 0 && !is_chunk)) {
        // off_mode and empty
        ctx->group_commit = false;
        return Status::OK();
    }
    if (is_chunk) {
        ctx->label = "";
    }

    auto partial_columns = !req->header(HTTP_PARTIAL_COLUMNS).empty() &&
                           iequal(req->header(HTTP_PARTIAL_COLUMNS), "true");
    auto temp_partitions = !req->header(HTTP_TEMP_PARTITIONS).empty();
    auto partitions = !req->header(HTTP_PARTITIONS).empty();
    if (!partial_columns && !partitions && !temp_partitions && !ctx->two_phase_commit) {
        if (!config::wait_internal_group_commit_finish && !ctx->label.empty()) {
            return Status::InvalidArgument("label and group_commit can't be set at the same time");
        }
        ctx->group_commit = true;
        if (iequal(group_commit_mode, "async_mode")) {
            if (!load_size_smaller_than_wal_limit(content_length)) {
                std::stringstream ss;
                ss << "There is no space for group commit http load async WAL. This http load "
                      "size is "
                   << content_length << ". WAL dir info: "
                   << ExecEnv::GetInstance()->wal_mgr()->get_wal_dirs_info_string();
                LOG(WARNING) << ss.str();
                return Status::Error<EXCEEDED_LIMIT>(ss.str());
            }
        }
    }
    return Status::OK();
}

} // namespace doris
