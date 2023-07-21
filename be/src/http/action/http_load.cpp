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

#include "http/action/http_load.h"

// use string iequal
#include <event2/buffer.h>
#include <event2/http.h>
#include <gen_cpp/FrontendService.h>
#include <gen_cpp/FrontendService_types.h>
#include <gen_cpp/HeartbeatService_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Types_types.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/time.h>
#include <thrift/protocol/TDebugProtocol.h>
#include <time.h>

#include <future>
#include <map>
#include <sstream>
#include <stdexcept>
#include <utility>

#include "common/config.h"
#include "common/consts.h"
#include "common/logging.h"
#include "common/status.h"
#include "common/utils.h"
#include "http/http_channel.h"
#include "http/http_common.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/utils.h"
#include "io/fs/stream_load_pipe.h"
#include "olap/storage_engine.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "runtime/load_path_mgr.h"
#include "runtime/message_body_sink.h"
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
#include "util/url_coding.h"

namespace doris {
using namespace ErrorCode;

DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(http_load_requests_total, MetricUnit::REQUESTS);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(http_load_duration_ms, MetricUnit::MILLISECONDS);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(http_load_current_processing, MetricUnit::REQUESTS);

void HttpLoadAction::_parse_format(const std::string& format_str,
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

bool HttpLoadAction::_is_format_support_streaming(TFileFormatType::type format) {
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

HttpLoadAction::HttpLoadAction(ExecEnv* exec_env) : _exec_env(exec_env) {
    _http_load_entity = DorisMetrics::instance()->metric_registry()->register_entity("http_load");
    INT_COUNTER_METRIC_REGISTER(_http_load_entity, http_load_requests_total);
    INT_COUNTER_METRIC_REGISTER(_http_load_entity, http_load_duration_ms);
    INT_GAUGE_METRIC_REGISTER(_http_load_entity, http_load_current_processing);
}

HttpLoadAction::~HttpLoadAction() {
    DorisMetrics::instance()->metric_registry()->deregister_entity(_http_load_entity);
}

void HttpLoadAction::handle(HttpRequest* req) {
    std::shared_ptr<StreamLoadContext> ctx =
            std::static_pointer_cast<StreamLoadContext>(req->handler_ctx());
    if (ctx == nullptr) {
        return;
    }

    THttpLoadPutParams params;
    // status already set to fail
    if (ctx->status.ok()) {
        ctx->status = _handle(params, ctx);
        if (!ctx->status.ok() && !ctx->status.is<PUBLISH_TIMEOUT>()) {
            LOG(WARNING) << "handle streaming load failed, id=" << ctx->id
                         << ", errmsg=" << ctx->status;
        }
    }
    ctx->load_cost_millis = UnixMillis() - ctx->start_millis;

    if (!ctx->status.ok() && !ctx->status.is<PUBLISH_TIMEOUT>()) {
        if (ctx->need_rollback) {
            _exec_env->stream_load_executor()->rollback_txn(ctx.get());
            ctx->need_rollback = false;
        }
        if (ctx->body_sink.get() != nullptr) {
            ctx->body_sink->cancel(ctx->status.to_string());
        }
    }

    auto str = ctx->to_json();
    // add new line at end
    str = str + '\n';
    HttpChannel::send_reply(req, str);
#ifndef BE_TEST
    if (config::enable_stream_load_record) {
        str = ctx->prepare_stream_load_record(str);
        _save_stream_load_record(ctx, str);
    }
#endif
}

Status HttpLoadAction::_handle(THttpLoadPutParams& params, std::shared_ptr<StreamLoadContext> ctx) {
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
        RETURN_IF_ERROR(_exec_env->stream_load_executor()->execute_plan_fragment(ctx));
    } else {
        RETURN_IF_ERROR(ctx->body_sink->finish());
    }
    // wait stream load finish
    RETURN_IF_ERROR(ctx->future.get());

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

int HttpLoadAction::on_header(HttpRequest* req) {
    http_load_current_processing->increment(1);

    std::shared_ptr<StreamLoadContext> ctx = std::make_shared<StreamLoadContext>(_exec_env);
    req->set_handler_ctx(ctx);

    // parse sql through rpc request fe
    THttpLoadPutRequest request;
    THttpLoadPutParams params;
    request.__set_loadId(ctx->id.to_thrift());
    request.__set_load_sql(req->header(HTTP_SQL));

    TNetworkAddress master_addr = _exec_env->master_info()->network_address;
    ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&request, &params](FrontendServiceConnection& client) {
                client->httpLoadPut(params, request);
            });

    ctx->load_type = TLoadType::MANUL_LOAD;
    ctx->load_src_type = TLoadSourceType::RAW;

    url_decode(params.db, &ctx->db);
    url_decode(params.table, &ctx->table);
    ctx->label = params.label;
    if (ctx->label.empty()) {
        ctx->label = generate_uuid_string();
    }

    ctx->two_phase_commit = params.two_phase_commit;

    LOG(INFO) << "new income http load request." << ctx->brief()
              << " sql : " << req->header(HTTP_SQL) << ", db=" << ctx->db << ", tbl=" << ctx->table;

    auto st = _on_header(req, params, ctx);
    if (!st.ok()) {
        ctx->status = std::move(st);
        if (ctx->need_rollback) {
            _exec_env->stream_load_executor()->rollback_txn(ctx.get());
            ctx->need_rollback = false;
        }
        if (ctx->body_sink.get() != nullptr) {
            ctx->body_sink->cancel(ctx->status.to_string());
        }
        auto str = ctx->to_json();
        // add new line at end
        str = str + '\n';
        HttpChannel::send_reply(req, str);
        http_load_current_processing->increment(-1);
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

Status HttpLoadAction::_on_header(HttpRequest* http_req, THttpLoadPutParams& params,
                                  std::shared_ptr<StreamLoadContext> ctx) {
    // auth information
    if (!parse_basic_auth(*http_req, &ctx->auth)) {
        LOG(WARNING) << "parse basic authorization failed." << ctx->brief();
        return Status::InternalError("no valid Basic authorization");
    }

    std::string format_str = params.format;
    if (iequal(format_str, BeConsts::CSV_WITH_NAMES) ||
        iequal(format_str, BeConsts::CSV_WITH_NAMES_AND_TYPES)) {
        ctx->header_type = format_str;
        //treat as CSV
        format_str = BeConsts::CSV;
    }
    _parse_format(format_str, params.compress_type, &ctx->format, &ctx->compress_type);
    if (ctx->format == TFileFormatType::FORMAT_UNKNOWN) {
        return Status::InternalError("unknown data format, format={}", params.format);
    }

    // check content length
    ctx->body_bytes = 0;
    size_t csv_max_body_bytes = config::streaming_load_max_mb * 1024 * 1024;
    size_t json_max_body_bytes = config::streaming_load_json_max_mb * 1024 * 1024;
    bool read_json_by_line = params.read_json_by_line;
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

    if (!params.timeout.empty()) {
        try {
            ctx->timeout_second = std::stoi(params.timeout);
        } catch (const std::invalid_argument& e) {
            return Status::InvalidArgument("Invalid timeout format, {}", e.what());
        }
    }
    if (!params.comment.empty()) {
        ctx->load_comment = params.comment;
    }
    // begin transaction
    int64_t begin_txn_start_time = MonotonicNanos();
    RETURN_IF_ERROR(_exec_env->stream_load_executor()->begin_txn(ctx.get()));
    ctx->begin_txn_cost_nanos = MonotonicNanos() - begin_txn_start_time;

    // process put file
    return _process_put(params, ctx);
}

void HttpLoadAction::on_chunk_data(HttpRequest* req) {
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

void HttpLoadAction::free_handler_ctx(std::shared_ptr<void> param) {
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

Status HttpLoadAction::_process_put(THttpLoadPutParams& params,
                                    std::shared_ptr<StreamLoadContext> ctx) {
    // Now we use stream
    ctx->use_streaming = _is_format_support_streaming(ctx->format);

    // put request
    TStreamLoadPutRequest request;
    set_request_auth(&request, ctx->auth);
    request.db = ctx->db;
    request.tbl = ctx->table;
    request.txnId = ctx->txn_id;
    request.formatType = ctx->format;
    request.__set_compress_type(ctx->compress_type);
    request.__set_header_type(ctx->header_type);
    request.__set_loadId(ctx->id.to_thrift());
    if (ctx->use_streaming) {
        auto pipe = std::make_shared<io::StreamLoadPipe>(
                io::kMaxPipeBufferedBytes /* max_buffered_bytes */, 64 * 1024 /* min_chunk_size */,
                ctx->body_bytes /* total_length */);
        request.fileType = TFileType::FILE_STREAM;
        ctx->body_sink = pipe;
        ctx->pipe = pipe;
        RETURN_IF_ERROR(_exec_env->new_load_stream_mgr()->put(ctx->id, ctx));
    } else {
        RETURN_IF_ERROR(_data_saved_path(params, &request.path));
        auto file_sink = std::make_shared<MessageBodyFileSink>(request.path);
        RETURN_IF_ERROR(file_sink->open());
        request.__isset.path = true;
        request.fileType = TFileType::FILE_LOCAL;
        request.__set_file_size(ctx->body_bytes);
        ctx->body_sink = file_sink;
    }
    // params setting
    if (!params.columns.empty()) {
        request.__set_columns(params.columns);
    }
    if (!params.where.empty()) {
        request.__set_where(params.where);
    }
    if (!params.column_separator.empty()) {
        request.__set_columnSeparator(params.column_separator);
    }
    if (!params.line_delimiter.empty()) {
        request.__set_line_delimiter(params.line_delimiter);
    }
    if (!params.partitions.empty()) {
        request.__set_partitions(params.partitions);
        request.__set_isTempPartition(false);
        if (!params.temporary_partitions.empty()) {
            return Status::InvalidArgument(
                    "Can not specify both partitions and temporary partitions");
        }
    }
    if (!params.temporary_partitions.empty()) {
        request.__set_partitions(params.temporary_partitions);
        request.__set_isTempPartition(true);
        if (!params.partitions.empty()) {
            return Status::InvalidArgument(
                    "Can not specify both partitions and temporary partitions");
        }
    }

    request.__set_negative(params.negative);
    request.__set_strictMode(params.strict_mode);

    if (!params.timeout.empty()) {
        request.__set_timezone(params.timeout);
    }

    if (!params.exec_mem_limit.empty()) {
        try {
            request.__set_execMemLimit(std::stoll(params.exec_mem_limit));
        } catch (const std::invalid_argument& e) {
            return Status::InvalidArgument("Invalid mem limit format, {}", e.what());
        }
    }
    if (!params.jsonpaths.empty()) {
        request.__set_jsonpaths(params.jsonpaths);
    }
    if (!params.json_root.empty()) {
        request.__set_json_root(params.json_root);
    }
    request.__set_strip_outer_array(params.strip_outer_array);
    request.__set_num_as_string(params.num_as_string);
    request.__set_fuzzy_parse(params.fuzzy_parse);
    request.__set_read_json_by_line(params.read_json_by_line);
    // TODO function_column.sequence_col

    request.__set_send_batch_parallelism(params.send_batch_parallelism);
    request.__set_load_to_single_tablet(params.load_to_single_tablet);

    if (ctx->timeout_second != -1) {
        request.__set_timeout(ctx->timeout_second);
    }
    request.__set_thrift_rpc_timeout_ms(config::thrift_rpc_timeout_ms);
    TMergeType::type merge_type = TMergeType::APPEND;
    StringCaseMap<TMergeType::type> merge_type_map = {{"APPEND", TMergeType::APPEND},
                                                      {"DELETE", TMergeType::DELETE},
                                                      {"MERGE", TMergeType::MERGE}};
    if (!params.merge_type.empty()) {
        std::string merge_type_str = params.merge_type;
        if (merge_type_map.find(merge_type_str) != merge_type_map.end()) {
            merge_type = merge_type_map.find(merge_type_str)->second;
        } else {
            return Status::InvalidArgument("Invalid merge type {}", merge_type_str);
        }
        if (merge_type == TMergeType::MERGE && params.delete_condition.empty()) {
            return Status::InvalidArgument("Excepted DELETE ON clause when merge type is MERGE.");
        } else if (merge_type != TMergeType::MERGE && !params.delete_condition.empty()) {
            return Status::InvalidArgument(
                    "Not support DELETE ON clause when merge type is not MERGE.");
        }
    }
    request.__set_merge_type(merge_type);
    if (!params.delete_condition.empty()) {
        request.__set_delete_condition(params.delete_condition);
    }
    ctx->max_filter_ratio = params.max_filter_ratio;
    request.__set_max_filter_ratio(ctx->max_filter_ratio);
    if (!params.hidden_columns.empty()) {
        request.__set_hidden_columns(params.hidden_columns);
    }
    request.__set_trim_double_quotes(params.trim_double_quotes);
    request.__set_skip_lines(params.skip_lines);
    request.__set_enable_profile(params.enable_profile);
    request.__set_partial_update(params.partial_columns);

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

#endif
    Status plan_status(ctx->put_result.status);
    if (!plan_status.ok()) {
        LOG(WARNING) << "plan http load failed. errmsg=" << plan_status << ctx->brief();
        return plan_status;
    }

    VLOG_NOTICE << "params is " << apache::thrift::ThriftDebugString(ctx->put_result.params);
    // if we not use streaming, we must download total content before we begin
    // to process this load
    if (!ctx->use_streaming) {
        return Status::OK();
    }

    return _exec_env->stream_load_executor()->execute_plan_fragment(ctx);
}

Status HttpLoadAction::_data_saved_path(THttpLoadPutParams& params, std::string* file_path) {
    std::string prefix;
    RETURN_IF_ERROR(_exec_env->load_path_mgr()->allocate_dir(params.db, "", &prefix));
    timeval tv;
    gettimeofday(&tv, nullptr);
    struct tm tm;
    time_t cur_sec = tv.tv_sec;
    localtime_r(&cur_sec, &tm);
    char buf[64];
    strftime(buf, 64, "%Y%m%d%H%M%S", &tm);
    std::stringstream ss;
    ss << prefix << "/" << params.table << "." << buf << "." << tv.tv_usec;
    *file_path = ss.str();
    return Status::OK();
}

void HttpLoadAction::_save_stream_load_record(std::shared_ptr<StreamLoadContext> ctx,
                                              const std::string& str) {
    auto stream_load_recorder = StorageEngine::instance()->get_stream_load_recorder();
    if (stream_load_recorder != nullptr) {
        std::string key =
                std::to_string(ctx->start_millis + ctx->load_cost_millis) + "_" + ctx->label;
        auto st = stream_load_recorder->put(key, str);
        if (st.ok()) {
            LOG(INFO) << "put http_load_record rocksdb successfully. label: " << ctx->label
                      << ", key: " << key;
        }
    } else {
        LOG(WARNING) << "put http_load_record rocksdb failed. stream_load_recorder is null.";
    }
}
} // namespace doris
