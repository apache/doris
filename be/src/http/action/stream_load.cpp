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

#include "common/consts.h"
#include "common/logging.h"
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
#include "olap/storage_engine.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "runtime/load_path_mgr.h"
#include "runtime/plan_fragment_executor.h"
#include "runtime/stream_load/load_stream_mgr.h"
#include "runtime/stream_load/stream_load_context.h"
#include "runtime/stream_load/stream_load_executor.h"
#include "runtime/stream_load/stream_load_pipe.h"
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

DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(streaming_load_requests_total, MetricUnit::REQUESTS);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(streaming_load_duration_ms, MetricUnit::MILLISECONDS);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(streaming_load_current_processing, MetricUnit::REQUESTS);

#ifdef BE_TEST
TStreamLoadPutResult k_stream_load_put_result;
#endif

static void parse_format(const std::string& format_str, const std::string& compress_type_str,
                         TFileFormatType::type* format_type,
                         TFileCompressType::type* compress_type) {
    if (format_str.empty()) {
        parse_format("CSV", compress_type_str, format_type, compress_type);
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
    return;
}

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

StreamLoadAction::StreamLoadAction(ExecEnv* exec_env) : _exec_env(exec_env) {
    _stream_load_entity =
            DorisMetrics::instance()->metric_registry()->register_entity("stream_load");
    INT_COUNTER_METRIC_REGISTER(_stream_load_entity, streaming_load_requests_total);
    INT_COUNTER_METRIC_REGISTER(_stream_load_entity, streaming_load_duration_ms);
    INT_GAUGE_METRIC_REGISTER(_stream_load_entity, streaming_load_current_processing);
}

StreamLoadAction::~StreamLoadAction() {
    DorisMetrics::instance()->metric_registry()->deregister_entity(_stream_load_entity);
}

void StreamLoadAction::handle(HttpRequest* req) {
    StreamLoadContext* ctx = (StreamLoadContext*)req->handler_ctx();
    if (ctx == nullptr) {
        return;
    }

    // status already set to fail
    if (ctx->status.ok()) {
        ctx->status = _handle(ctx);
        if (!ctx->status.ok() && ctx->status.code() != TStatusCode::PUBLISH_TIMEOUT) {
            LOG(WARNING) << "handle streaming load failed, id=" << ctx->id
                         << ", errmsg=" << ctx->status.get_error_msg();
        }
    }
    ctx->load_cost_millis = UnixMillis() - ctx->start_millis;

    if (!ctx->status.ok() && ctx->status.code() != TStatusCode::PUBLISH_TIMEOUT) {
        if (ctx->need_rollback) {
            _exec_env->stream_load_executor()->rollback_txn(ctx);
            ctx->need_rollback = false;
        }
        if (ctx->body_sink.get() != nullptr) {
            ctx->body_sink->cancel(ctx->status.get_error_msg());
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
    // update statstics
    streaming_load_requests_total->increment(1);
    streaming_load_duration_ms->increment(ctx->load_cost_millis);
    streaming_load_current_processing->increment(-1);
}

Status StreamLoadAction::_handle(StreamLoadContext* ctx) {
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

int StreamLoadAction::on_header(HttpRequest* req) {
    streaming_load_current_processing->increment(1);

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
        ctx->status = st;
        if (ctx->need_rollback) {
            _exec_env->stream_load_executor()->rollback_txn(ctx);
            ctx->need_rollback = false;
        }
        if (ctx->body_sink.get() != nullptr) {
            ctx->body_sink->cancel(st.get_error_msg());
        }
        auto str = ctx->to_json();
        // add new line at end
        str = str + '\n';
        HttpChannel::send_reply(req, str);
        streaming_load_current_processing->increment(-1);
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

Status StreamLoadAction::_on_header(HttpRequest* http_req, StreamLoadContext* ctx) {
    // auth information
    if (!parse_basic_auth(*http_req, &ctx->auth)) {
        LOG(WARNING) << "parse basic authorization failed." << ctx->brief();
        return Status::InternalError("no valid Basic authorization");
    }

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
    parse_format(format_str, http_req->header(HTTP_COMPRESS_TYPE), &ctx->format,
                 &ctx->compress_type);
    if (ctx->format == TFileFormatType::FORMAT_UNKNOWN) {
        return Status::InternalError("unknown data format, format={}",
                                     http_req->header(HTTP_FORMAT_KEY));
    }

    if (ctx->two_phase_commit && config::disable_stream_load_2pc) {
        return Status::InternalError("Two phase commit (2PC) for stream load was disabled");
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

    // begin transaction
    int64_t begin_txn_start_time = MonotonicNanos();
    RETURN_IF_ERROR(_exec_env->stream_load_executor()->begin_txn(ctx));
    ctx->begin_txn_cost_nanos = MonotonicNanos() - begin_txn_start_time;

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

    int64_t start_read_data_time = MonotonicNanos();
    while (evbuffer_get_length(evbuf) > 0) {
        auto bb = ByteBuffer::allocate(128 * 1024);
        auto remove_bytes = evbuffer_remove(evbuf, bb->ptr, bb->capacity);
        bb->pos = remove_bytes;
        bb->flip();
        auto st = ctx->body_sink->append(bb);
        if (!st.ok()) {
            LOG(WARNING) << "append body content failed. errmsg=" << st.get_error_msg() << ", "
                         << ctx->brief();
            ctx->status = st;
            return;
        }
        ctx->receive_bytes += remove_bytes;
    }
    ctx->read_data_cost_nanos += (MonotonicNanos() - start_read_data_time);
}

void StreamLoadAction::free_handler_ctx(void* param) {
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

Status StreamLoadAction::_process_put(HttpRequest* http_req, StreamLoadContext* ctx) {
    // Now we use stream
    ctx->use_streaming = is_format_support_streaming(ctx->format);

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
        auto pipe = std::make_shared<StreamLoadPipe>(kMaxPipeBufferedBytes /* max_buffered_bytes */,
                                                     64 * 1024 /* min_chunk_size */,
                                                     ctx->body_bytes /* total_length */);
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
    if (!http_req->header(HTTP_LINE_DELIMITER).empty()) {
        request.__set_line_delimiter(http_req->header(HTTP_LINE_DELIMITER));
    }
    if (!http_req->header(HTTP_PARTITIONS).empty()) {
        request.__set_partitions(http_req->header(HTTP_PARTITIONS));
        request.__set_isTempPartition(false);
        if (!http_req->header(HTTP_TEMP_PARTITIONS).empty()) {
            return Status::InvalidArgument(
                    "Can not specify both partitions and temporary partitions");
        }
    }
    if (!http_req->header(HTTP_TEMP_PARTITIONS).empty()) {
        request.__set_partitions(http_req->header(HTTP_TEMP_PARTITIONS));
        request.__set_isTempPartition(true);
        if (!http_req->header(HTTP_PARTITIONS).empty()) {
            return Status::InvalidArgument(
                    "Can not specify both partitions and temporary partitions");
        }
    }
    if (!http_req->header(HTTP_NEGATIVE).empty() && http_req->header(HTTP_NEGATIVE) == "true") {
        request.__set_negative(true);
    } else {
        request.__set_negative(false);
    }
    if (!http_req->header(HTTP_STRICT_MODE).empty()) {
        if (iequal(http_req->header(HTTP_STRICT_MODE), "false")) {
            request.__set_strictMode(false);
        } else if (iequal(http_req->header(HTTP_STRICT_MODE), "true")) {
            request.__set_strictMode(true);
        } else {
            return Status::InvalidArgument("Invalid strict mode format. Must be bool type");
        }
    }
    if (!http_req->header(HTTP_TIMEZONE).empty()) {
        request.__set_timezone(http_req->header(HTTP_TIMEZONE));
    }
    if (!http_req->header(HTTP_EXEC_MEM_LIMIT).empty()) {
        try {
            request.__set_execMemLimit(std::stoll(http_req->header(HTTP_EXEC_MEM_LIMIT)));
        } catch (const std::invalid_argument& e) {
            return Status::InvalidArgument("Invalid mem limit format");
        }
    }
    if (!http_req->header(HTTP_JSONPATHS).empty()) {
        request.__set_jsonpaths(http_req->header(HTTP_JSONPATHS));
    }
    if (!http_req->header(HTTP_JSONROOT).empty()) {
        request.__set_json_root(http_req->header(HTTP_JSONROOT));
    }
    if (!http_req->header(HTTP_STRIP_OUTER_ARRAY).empty()) {
        if (iequal(http_req->header(HTTP_STRIP_OUTER_ARRAY), "true")) {
            request.__set_strip_outer_array(true);
        } else {
            request.__set_strip_outer_array(false);
        }
    } else {
        request.__set_strip_outer_array(false);
    }
    if (!http_req->header(HTTP_NUM_AS_STRING).empty()) {
        if (iequal(http_req->header(HTTP_NUM_AS_STRING), "true")) {
            request.__set_num_as_string(true);
        } else {
            request.__set_num_as_string(false);
        }
    } else {
        request.__set_num_as_string(false);
    }
    if (!http_req->header(HTTP_FUZZY_PARSE).empty()) {
        if (iequal(http_req->header(HTTP_FUZZY_PARSE), "true")) {
            request.__set_fuzzy_parse(true);
        } else {
            request.__set_fuzzy_parse(false);
        }
    } else {
        request.__set_fuzzy_parse(false);
    }

    if (!http_req->header(HTTP_READ_JSON_BY_LINE).empty()) {
        if (iequal(http_req->header(HTTP_READ_JSON_BY_LINE), "true")) {
            request.__set_read_json_by_line(true);
        } else {
            request.__set_read_json_by_line(false);
        }
    } else {
        request.__set_read_json_by_line(false);
    }

    if (!http_req->header(HTTP_FUNCTION_COLUMN + "." + HTTP_SEQUENCE_COL).empty()) {
        request.__set_sequence_col(
                http_req->header(HTTP_FUNCTION_COLUMN + "." + HTTP_SEQUENCE_COL));
    }

    if (!http_req->header(HTTP_SEND_BATCH_PARALLELISM).empty()) {
        try {
            request.__set_send_batch_parallelism(
                    std::stoi(http_req->header(HTTP_SEND_BATCH_PARALLELISM)));
        } catch (const std::invalid_argument& e) {
            return Status::InvalidArgument("Invalid send_batch_parallelism format");
        }
    }

    if (!http_req->header(HTTP_LOAD_TO_SINGLE_TABLET).empty()) {
        if (iequal(http_req->header(HTTP_LOAD_TO_SINGLE_TABLET), "true")) {
            request.__set_load_to_single_tablet(true);
        } else {
            request.__set_load_to_single_tablet(false);
        }
    }

    if (ctx->timeout_second != -1) {
        request.__set_timeout(ctx->timeout_second);
    }
    request.__set_thrift_rpc_timeout_ms(config::thrift_rpc_timeout_ms);
    TMergeType::type merge_type = TMergeType::APPEND;
    StringCaseMap<TMergeType::type> merge_type_map = {{"APPEND", TMergeType::APPEND},
                                                      {"DELETE", TMergeType::DELETE},
                                                      {"MERGE", TMergeType::MERGE}};
    if (!http_req->header(HTTP_MERGE_TYPE).empty()) {
        std::string merge_type_str = http_req->header(HTTP_MERGE_TYPE);
        if (merge_type_map.find(merge_type_str) != merge_type_map.end()) {
            merge_type = merge_type_map.find(merge_type_str)->second;
        } else {
            return Status::InvalidArgument("Invalid merge type {}", merge_type_str);
        }
        if (merge_type == TMergeType::MERGE && http_req->header(HTTP_DELETE_CONDITION).empty()) {
            return Status::InvalidArgument("Excepted DELETE ON clause when merge type is MERGE.");
        } else if (merge_type != TMergeType::MERGE &&
                   !http_req->header(HTTP_DELETE_CONDITION).empty()) {
            return Status::InvalidArgument(
                    "Not support DELETE ON clause when merge type is not MERGE.");
        }
    }
    request.__set_merge_type(merge_type);
    if (!http_req->header(HTTP_DELETE_CONDITION).empty()) {
        request.__set_delete_condition(http_req->header(HTTP_DELETE_CONDITION));
    }

    if (!http_req->header(HTTP_MAX_FILTER_RATIO).empty()) {
        ctx->max_filter_ratio = strtod(http_req->header(HTTP_MAX_FILTER_RATIO).c_str(), nullptr);
        request.__set_max_filter_ratio(ctx->max_filter_ratio);
    }

    if (!http_req->header(HTTP_HIDDEN_COLUMNS).empty()) {
        request.__set_hidden_columns(http_req->header(HTTP_HIDDEN_COLUMNS));
    }

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
        LOG(WARNING) << "plan streaming load failed. errmsg=" << plan_status.get_error_msg()
                     << ctx->brief();
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
    return Status::OK();
}

void StreamLoadAction::_save_stream_load_record(StreamLoadContext* ctx, const std::string& str) {
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
