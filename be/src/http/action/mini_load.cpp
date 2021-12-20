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

#include "http/action/mini_load.h"

#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/http.h>
#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <thrift/protocol/TDebugProtocol.h>
#include <time.h>
#include <unistd.h>

#include <functional>
#include <mutex>
#include <sstream>
#include <string>

#include "agent/cgroups_mgr.h"
#include "common/status.h"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/FrontendService_types.h"
#include "gen_cpp/HeartbeatService_types.h"
#include "gen_cpp/MasterService_types.h"
#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_parser.h"
#include "http/http_request.h"
#include "http/http_response.h"
#include "http/http_status.h"
#include "http/utils.h"
#include "olap/file_helper.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "runtime/load_path_mgr.h"
#include "runtime/stream_load/stream_load_context.h"
#include "service/backend_options.h"
#include "util/file_utils.h"
#include "util/json_util.h"
#include "util/string_parser.hpp"
#include "util/string_util.h"
#include "util/thrift_rpc_helper.h"
#include "util/time.h"
#include "util/url_coding.h"

namespace doris {

// context used to handle mini-load in asynchronous mode
struct MiniLoadAsyncCtx {
    MiniLoadAsyncCtx(MiniLoadAction* handler_) : handler(handler_) {}
    ~MiniLoadAsyncCtx() {
        if (need_remove_handle) {
            handler->erase_handle(load_handle);
        }
        if (fd >= 0) {
            ::close(fd);
        }
    }

    MiniLoadAction* handler;

    // used to check duplicate
    LoadHandle load_handle;
    bool need_remove_handle = false;

    // file to save
    std::string file_path;
    int fd = -1;

    size_t body_bytes = 0;
    size_t bytes_written = 0;

    TLoadCheckRequest load_check_req;
};

struct MiniLoadCtx {
    MiniLoadCtx(bool is_streaming_) : is_streaming(is_streaming_) {}

    bool is_streaming = false;
    MiniLoadAsyncCtx* mini_load_async_ctx = nullptr;
    StreamLoadContext* stream_load_ctx = nullptr;
};

const std::string CLUSTER_KEY = "cluster";
const std::string DB_KEY = "db";
const std::string TABLE_KEY = "table";
const std::string LABEL_KEY = "label";
const std::string SUB_LABEL_KEY = "sub_label";
const std::string FILE_PATH_KEY = "file_path";
const std::string COLUMNS_KEY = "columns";
const std::string HLL_KEY = "hll";
const std::string COLUMN_SEPARATOR_KEY = "column_separator";
const std::string MAX_FILTER_RATIO_KEY = "max_filter_ratio";
const std::string STRICT_MODE_KEY = "strict_mode";
const std::string TIMEOUT_KEY = "timeout";
const char* k_100_continue = "100-continue";

MiniLoadAction::MiniLoadAction(ExecEnv* exec_env) : _exec_env(exec_env) {}

static bool is_name_valid(const std::string& name) {
    return !name.empty();
}

static Status check_request(HttpRequest* req) {
    auto& params = *req->params();

    // check params
    if (!is_name_valid(params[DB_KEY])) {
        return Status::InternalError("Database name is not valid.");
    }
    if (!is_name_valid(params[TABLE_KEY])) {
        return Status::InternalError("Table name is not valid.");
    }
    if (!is_name_valid(params[LABEL_KEY])) {
        return Status::InternalError("Label name is not valid.");
    }

    return Status::OK();
}

Status MiniLoadAction::data_saved_dir(const LoadHandle& desc, const std::string& table,
                                      std::string* file_path) {
    std::string prefix;
    RETURN_IF_ERROR(_exec_env->load_path_mgr()->allocate_dir(desc.db, desc.label, &prefix));
    timeval tv;
    gettimeofday(&tv, nullptr);
    struct tm tm;
    time_t cur_sec = tv.tv_sec;
    localtime_r(&cur_sec, &tm);
    char buf[64];
    strftime(buf, 64, "%Y%m%d%H%M%S", &tm);

    std::stringstream ss;
    ss << prefix << "/" << table << "." << desc.sub_label << "." << buf << "." << tv.tv_usec;
    *file_path = ss.str();
    return Status::OK();
}

Status MiniLoadAction::_load(HttpRequest* http_req, const std::string& file_path,
                             const std::string& user, const std::string& cluster,
                             int64_t file_size) {
    // Prepare request parameters.
    std::map<std::string, std::string> params(http_req->query_params().begin(),
                                              http_req->query_params().end());
    RETURN_IF_ERROR(_merge_header(http_req, &params));
    params.erase(LABEL_KEY);
    params.erase(SUB_LABEL_KEY);

    // put here to log master information
    const TNetworkAddress& master_address = _exec_env->master_info()->network_address;
    Status status;
    FrontendServiceConnection client(_exec_env->frontend_client_cache(), master_address,
                                     config::thrift_rpc_timeout_ms, &status);
    if (!status.ok()) {
        std::stringstream ss;
        ss << "Connect master failed, with address(" << master_address.hostname << ":"
           << master_address.port << ")";
        LOG(WARNING) << ss.str();
        return status;
    }
    TFeResult res;
    try {
        TMiniLoadRequest req;
        req.protocolVersion = FrontendServiceVersion::V1;
        req.__set_db(http_req->param(DB_KEY));
        if (!cluster.empty()) {
            req.__set_cluster(cluster);
        }
        req.__set_tbl(http_req->param(TABLE_KEY));
        req.__set_label(http_req->param(LABEL_KEY));
        req.__set_user(user);
        // Belong to a multi-load transaction
        if (!http_req->param(SUB_LABEL_KEY).empty()) {
            req.__set_subLabel(http_req->param(SUB_LABEL_KEY));
        }
        req.__set_properties(params);
        req.files.push_back(file_path);
        req.__isset.file_size = true;
        req.file_size.push_back(file_size);
        req.backend.__set_hostname(BackendOptions::get_localhost());
        req.backend.__set_port(config::be_port);

        req.__set_timestamp(GetCurrentTimeMicros());
        try {
            client->miniLoad(res, req);
        } catch (apache::thrift::transport::TTransportException& e) {
            LOG(WARNING) << "Retrying mini load from master(" << master_address.hostname << ":"
                         << master_address.port << ") because: " << e.what();
            status = client.reopen(config::thrift_rpc_timeout_ms);
            if (!status.ok()) {
                LOG(WARNING) << "Client reopen failed. with address(" << master_address.hostname
                             << ":" << master_address.port << ")";
                return status;
            }
            client->miniLoad(res, req);
        } catch (apache::thrift::TApplicationException& e) {
            LOG(WARNING) << "mini load request from master(" << master_address.hostname << ":"
                         << master_address.port << ") got unknown result: " << e.what();

            status = client.reopen(config::thrift_rpc_timeout_ms);
            if (!status.ok()) {
                LOG(WARNING) << "Client reopen failed. with address(" << master_address.hostname
                             << ":" << master_address.port << ")";
                return status;
            }
            client->miniLoad(res, req);
        }
    } catch (apache::thrift::TException& e) {
        // failed when retry.
        // reopen to disable this connection
        client.reopen(config::thrift_rpc_timeout_ms);
        std::stringstream ss;
        ss << "Request miniload from master(" << master_address.hostname << ":"
           << master_address.port << ") because: " << e.what();
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }

    return Status(res.status);
}

Status MiniLoadAction::_merge_header(HttpRequest* http_req,
                                     std::map<std::string, std::string>* params) {
    if (http_req == nullptr || params == nullptr) {
        return Status::OK();
    }
    if (!http_req->header(HTTP_FORMAT_KEY).empty()) {
        (*params)[HTTP_FORMAT_KEY] = http_req->header(HTTP_FORMAT_KEY);
    }
    if (!http_req->header(HTTP_COLUMNS).empty()) {
        (*params)[HTTP_COLUMNS] = http_req->header(HTTP_COLUMNS);
    }
    if (!http_req->header(HTTP_WHERE).empty()) {
        (*params)[HTTP_WHERE] = http_req->header(HTTP_WHERE);
    }
    if (!http_req->header(HTTP_COLUMN_SEPARATOR).empty()) {
        (*params)[HTTP_COLUMN_SEPARATOR] = http_req->header(HTTP_COLUMN_SEPARATOR);
    }
    if (!http_req->header(HTTP_PARTITIONS).empty()) {
        (*params)[HTTP_PARTITIONS] = http_req->header(HTTP_PARTITIONS);
        if (!http_req->header(HTTP_TEMP_PARTITIONS).empty()) {
            return Status::InvalidArgument(
                    "Can not specify both partitions and temporary partitions");
        }
    }
    if (!http_req->header(HTTP_TEMP_PARTITIONS).empty()) {
        (*params)[HTTP_TEMP_PARTITIONS] = http_req->header(HTTP_TEMP_PARTITIONS);
        if (!http_req->header(HTTP_PARTITIONS).empty()) {
            return Status::InvalidArgument(
                    "Can not specify both partitions and temporary partitions");
        }
    }
    if (!http_req->header(HTTP_NEGATIVE).empty() &&
        boost::iequals(http_req->header(HTTP_NEGATIVE), "true")) {
        (*params)[HTTP_NEGATIVE] = "true";
    } else {
        (*params)[HTTP_NEGATIVE] = "false";
    }
    if (!http_req->header(HTTP_STRICT_MODE).empty()) {
        if (boost::iequals(http_req->header(HTTP_STRICT_MODE), "false")) {
            (*params)[HTTP_STRICT_MODE] = "false";
        } else if (boost::iequals(http_req->header(HTTP_STRICT_MODE), "true")) {
            (*params)[HTTP_STRICT_MODE] = "true";
        } else {
            return Status::InvalidArgument("Invalid strict mode format. Must be bool type");
        }
    }
    if (!http_req->header(HTTP_TIMEZONE).empty()) {
        (*params)[HTTP_TIMEZONE] = http_req->header(HTTP_TIMEZONE);
    }
    if (!http_req->header(HTTP_EXEC_MEM_LIMIT).empty()) {
        (*params)[HTTP_EXEC_MEM_LIMIT] = http_req->header(HTTP_EXEC_MEM_LIMIT);
    }
    if (!http_req->header(HTTP_JSONPATHS).empty()) {
        (*params)[HTTP_JSONPATHS] = http_req->header(HTTP_JSONPATHS);
    }
    if (!http_req->header(HTTP_JSONROOT).empty()) {
        (*params)[HTTP_JSONROOT] = http_req->header(HTTP_JSONROOT);
    }
    if (!http_req->header(HTTP_STRIP_OUTER_ARRAY).empty()) {
        if (boost::iequals(http_req->header(HTTP_STRIP_OUTER_ARRAY), "true")) {
            (*params)[HTTP_STRIP_OUTER_ARRAY] = "true";
        } else {
            (*params)[HTTP_STRIP_OUTER_ARRAY] = "false";
        }
    } else {
        (*params)[HTTP_STRIP_OUTER_ARRAY] = "false";
    }
    if (!http_req->header(HTTP_FUZZY_PARSE).empty()) {
        if (boost::iequals(http_req->header(HTTP_FUZZY_PARSE), "true")) {
            (*params)[HTTP_FUZZY_PARSE] = "true";
        } else {
            (*params)[HTTP_FUZZY_PARSE] = "false";
        }
    } else {
        (*params)[HTTP_FUZZY_PARSE] = "false";
    }
    if (!http_req->header(HTTP_READ_JSON_BY_LINE).empty()) {
        if (boost::iequals(http_req->header(HTTP_READ_JSON_BY_LINE), "true")) {
            (*params)[HTTP_READ_JSON_BY_LINE] = "true";
        } else {
            (*params)[HTTP_READ_JSON_BY_LINE] = "false";
        }
    } else {
        (*params)[HTTP_READ_JSON_BY_LINE] = "false";
    }

    if (!http_req->header(HTTP_FUNCTION_COLUMN + "." + HTTP_SEQUENCE_COL).empty()) {
        (*params)[HTTP_FUNCTION_COLUMN + "." + HTTP_SEQUENCE_COL] =
                http_req->header(HTTP_FUNCTION_COLUMN + "." + HTTP_SEQUENCE_COL);
    }
    if (params->find(HTTP_MERGE_TYPE) == params->end()) {
        params->insert(std::make_pair(HTTP_MERGE_TYPE, "APPEND"));
    }
    StringCaseMap<TMergeType::type> merge_type_map = {{"APPEND", TMergeType::APPEND},
                                                      {"DELETE", TMergeType::DELETE},
                                                      {"MERGE", TMergeType::MERGE}};
    if (!http_req->header(HTTP_MERGE_TYPE).empty()) {
        std::string merge_type = http_req->header(HTTP_MERGE_TYPE);
        auto it = merge_type_map.find(merge_type);
        if (it != merge_type_map.end()) {
            (*params)[HTTP_MERGE_TYPE] = it->first;
        } else {
            return Status::InvalidArgument("Invalid merge type " + merge_type);
        }
    }
    if (!http_req->header(HTTP_DELETE_CONDITION).empty()) {
        if ((*params)[HTTP_MERGE_TYPE] == "MERGE") {
            (*params)[HTTP_DELETE_CONDITION] = http_req->header(HTTP_DELETE_CONDITION);
        } else {
            return Status::InvalidArgument("not support delete when merge type is " +
                                           (*params)[HTTP_MERGE_TYPE] + ".");
        }
    }
    return Status::OK();
}

static bool parse_auth(const std::string& auth, std::string* user, std::string* passwd,
                       std::string* cluster) {
    std::string decoded_auth;

    if (!base64_decode(auth, &decoded_auth)) {
        return false;
    }
    std::string::size_type pos = decoded_auth.find(':');
    if (pos == std::string::npos) {
        return false;
    }
    user->assign(decoded_auth.c_str(), pos);
    passwd->assign(decoded_auth.c_str() + pos + 1);
    const std::string::size_type cluster_pos = user->find('@');
    if (cluster_pos != std::string::npos) {
        cluster->assign(user->c_str(), cluster_pos + 1, pos - cluster_pos - 1);
        user->assign(user->c_str(), cluster_pos);
    }
    return true;
}

Status MiniLoadAction::check_auth(const HttpRequest* http_req,
                                  const TLoadCheckRequest& check_load_req) {
    // put here to log master information
    const TNetworkAddress& master_address = _exec_env->master_info()->network_address;
    Status status;
    FrontendServiceConnection client(_exec_env->frontend_client_cache(), master_address,
                                     config::thrift_rpc_timeout_ms, &status);
    if (!status.ok()) {
        std::stringstream ss;
        ss << "Connect master failed, with address(" << master_address.hostname << ":"
           << master_address.port << ")";
        LOG(WARNING) << ss.str();
        return status;
    }

    TFeResult res;
    try {
        try {
            client->loadCheck(res, check_load_req);
        } catch (apache::thrift::transport::TTransportException& e) {
            LOG(WARNING) << "Retrying mini load from master(" << master_address.hostname << ":"
                         << master_address.port << ") because: " << e.what();
            status = client.reopen(config::thrift_rpc_timeout_ms);
            if (!status.ok()) {
                LOG(WARNING) << "Client reopen failed. with address(" << master_address.hostname
                             << ":" << master_address.port << ")";
                return status;
            }
            client->loadCheck(res, check_load_req);
        } catch (apache::thrift::TApplicationException& e) {
            LOG(WARNING) << "load check request from master(" << master_address.hostname << ":"
                         << master_address.port << ") got unknown result: " << e.what();

            status = client.reopen(config::thrift_rpc_timeout_ms);
            if (!status.ok()) {
                LOG(WARNING) << "Client reopen failed. with address(" << master_address.hostname
                             << ":" << master_address.port << ")";
                return status;
            }
            client->loadCheck(res, check_load_req);
        }
    } catch (apache::thrift::TException& e) {
        // failed when retry.
        // reopen to disable this connection
        client.reopen(config::thrift_rpc_timeout_ms);
        std::stringstream ss;
        ss << "Request miniload from master(" << master_address.hostname << ":"
           << master_address.port << ") because: " << e.what();
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }

    return Status(res.status);
}

void MiniLoadAction::erase_handle(const LoadHandle& desc) {
    // remove
    std::lock_guard<std::mutex> l(_lock);
    _current_load.erase(desc);
}

int MiniLoadAction::on_header(HttpRequest* req) {
    // check authorization first, make client know what happened
    if (req->header(HttpHeaders::AUTHORIZATION).empty()) {
        HttpChannel::send_basic_challenge(req, "mini_load");
        return -1;
    }

    Status status;
    MiniLoadCtx* mini_load_ctx = new MiniLoadCtx(_is_streaming(req));
    req->set_handler_ctx(mini_load_ctx);
    if (((MiniLoadCtx*)req->handler_ctx())->is_streaming) {
        status = _on_new_header(req);
        StreamLoadContext* ctx = ((MiniLoadCtx*)req->handler_ctx())->stream_load_ctx;
        if (ctx != nullptr) {
            ctx->status = status;
        }
    } else {
        status = _on_header(req);
    }
    if (!status.ok()) {
        HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR, status.get_error_msg());
        return -1;
    }
    return 0;
}

bool MiniLoadAction::_is_streaming(HttpRequest* req) {
    // multi load must be non-streaming
    if (!req->param(SUB_LABEL_KEY).empty()) {
        return false;
    }

    TIsMethodSupportedRequest request;
    request.__set_function_name(_streaming_function_name);
    const TNetworkAddress& master_address = _exec_env->master_info()->network_address;
    TFeResult res;
    Status status = ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_address.hostname, master_address.port,
            [&request, &res](FrontendServiceConnection& client) {
                client->isMethodSupported(res, request);
            });
    if (!status.ok()) {
        std::stringstream ss;
        ss << "This mini load is not streaming because: " << status.get_error_msg()
           << " with address(" << master_address.hostname << ":" << master_address.port << ")";
        LOG(INFO) << ss.str();
        return false;
    }

    status = Status(res.status);
    if (!status.ok()) {
        std::stringstream ss;
        ss << "This streaming mini load is not be supportd because: " << status.get_error_msg()
           << " with address(" << master_address.hostname << ":" << master_address.port << ")";
        LOG(INFO) << ss.str();
        return false;
    }
    return true;
}

Status MiniLoadAction::_on_header(HttpRequest* req) {
    size_t body_bytes = 0;
    size_t max_body_bytes = config::streaming_load_max_mb * 1024 * 1024;
    if (!req->header(HttpHeaders::CONTENT_LENGTH).empty()) {
        body_bytes = std::stol(req->header(HttpHeaders::CONTENT_LENGTH));
        if (body_bytes > max_body_bytes) {
            std::stringstream ss;
            ss << "file size exceed max body size, max_body_bytes=" << max_body_bytes;
            return Status::InternalError(ss.str());
        }
    } else {
        evhttp_connection_set_max_body_size(
                evhttp_request_get_connection(req->get_evhttp_request()), max_body_bytes);
    }

    RETURN_IF_ERROR(check_request(req));

    std::unique_ptr<MiniLoadAsyncCtx> mini_load_async_ctx(new MiniLoadAsyncCtx(this));
    mini_load_async_ctx->body_bytes = body_bytes;
    mini_load_async_ctx->load_handle.db = req->param(DB_KEY);
    mini_load_async_ctx->load_handle.label = req->param(LABEL_KEY);
    mini_load_async_ctx->load_handle.sub_label = req->param(SUB_LABEL_KEY);

    // check if duplicate
    // Use this to prevent that two callback function write to one file
    // that file may be writen bad
    {
        std::lock_guard<std::mutex> l(_lock);
        if (_current_load.find(mini_load_async_ctx->load_handle) != _current_load.end()) {
            return Status::InternalError("Duplicate mini load request.");
        }
        _current_load.insert(mini_load_async_ctx->load_handle);
        mini_load_async_ctx->need_remove_handle = true;
    }
    // generate load check request
    RETURN_IF_ERROR(generate_check_load_req(req, &mini_load_async_ctx->load_check_req));

    // Check auth
    RETURN_IF_ERROR(check_auth(req, mini_load_async_ctx->load_check_req));

    // Receive data first, keep things easy.
    RETURN_IF_ERROR(data_saved_dir(mini_load_async_ctx->load_handle, req->param(TABLE_KEY),
                                   &mini_load_async_ctx->file_path));
    // destructor will close the file handle, not depend on DeferOp any more
    mini_load_async_ctx->fd =
            open(mini_load_async_ctx->file_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0660);
    if (mini_load_async_ctx->fd < 0) {
        char buf[64];
        LOG(WARNING) << "open file failed, path=" << mini_load_async_ctx->file_path
                     << ", errno=" << errno << ", errmsg=" << strerror_r(errno, buf, sizeof(buf));
        return Status::InternalError("open file failed");
    }

    ((MiniLoadCtx*)req->handler_ctx())->mini_load_async_ctx = mini_load_async_ctx.release();
    return Status::OK();
}

void MiniLoadAction::on_chunk_data(HttpRequest* http_req) {
    MiniLoadCtx* ctx = (MiniLoadCtx*)http_req->handler_ctx();
    if (ctx->is_streaming) {
        _on_new_chunk_data(http_req);
    } else {
        _on_chunk_data(http_req);
    }
}

void MiniLoadAction::_on_chunk_data(HttpRequest* http_req) {
    MiniLoadAsyncCtx* ctx = ((MiniLoadCtx*)http_req->handler_ctx())->mini_load_async_ctx;
    if (ctx == nullptr) {
        return;
    }

    struct evhttp_request* ev_req = http_req->get_evhttp_request();
    auto evbuf = evhttp_request_get_input_buffer(ev_req);

    char buf[4096];
    while (evbuffer_get_length(evbuf) > 0) {
        auto n = evbuffer_remove(evbuf, buf, sizeof(buf));
        while (n > 0) {
            auto res = write(ctx->fd, buf, n);
            if (res < 0) {
                char errbuf[64];
                LOG(WARNING) << "write file failed, path=" << ctx->file_path << ", errno=" << errno
                             << ", errmsg=" << strerror_r(errno, errbuf, sizeof(errbuf));
                HttpChannel::send_reply(http_req, HttpStatus::INTERNAL_SERVER_ERROR,
                                        "write file failed");
                delete ctx;
                http_req->set_handler_ctx(nullptr);
                return;
            }
            n -= res;
            ctx->bytes_written += res;
        }
    }
}

void MiniLoadAction::_on_new_chunk_data(HttpRequest* http_req) {
    StreamLoadContext* ctx = ((MiniLoadCtx*)http_req->handler_ctx())->stream_load_ctx;
    if (ctx == nullptr || !ctx->status.ok()) {
        return;
    }

    struct evhttp_request* ev_req = http_req->get_evhttp_request();
    auto evbuf = evhttp_request_get_input_buffer(ev_req);

    while (evbuffer_get_length(evbuf) > 0) {
        auto bb = ByteBuffer::allocate(4096);
        auto remove_bytes = evbuffer_remove(evbuf, bb->ptr, bb->capacity);
        bb->pos = remove_bytes;
        bb->flip();
        auto st = ctx->body_sink->append(bb);
        if (!st.ok()) {
            LOG(WARNING) << "append body content failed. errmsg=" << st.get_error_msg()
                         << ctx->brief();
            ctx->status = st;
            return;
        }
        ctx->receive_bytes += remove_bytes;
    }
}

void MiniLoadAction::free_handler_ctx(void* param) {
    MiniLoadCtx* ctx = (MiniLoadCtx*)param;
    if (ctx->is_streaming) {
        StreamLoadContext* streaming_ctx = ((MiniLoadCtx*)param)->stream_load_ctx;
        if (streaming_ctx != nullptr) {
            // sender is going, make receiver know it
            if (streaming_ctx->body_sink != nullptr) {
                LOG(WARNING) << "cancel stream load " << streaming_ctx->id.to_string()
                             << " because sender failed";
                streaming_ctx->body_sink->cancel("sender failed");
            }
            if (streaming_ctx->unref()) {
                delete streaming_ctx;
            }
        }
    } else {
        MiniLoadAsyncCtx* async_ctx = ((MiniLoadCtx*)param)->mini_load_async_ctx;
        delete async_ctx;
    }
    delete ctx;
}

void MiniLoadAction::handle(HttpRequest* http_req) {
    MiniLoadCtx* ctx = (MiniLoadCtx*)http_req->handler_ctx();
    if (ctx->is_streaming) {
        _new_handle(http_req);
    } else {
        _handle(http_req);
    }
}

void MiniLoadAction::_handle(HttpRequest* http_req) {
    MiniLoadAsyncCtx* ctx = ((MiniLoadCtx*)http_req->handler_ctx())->mini_load_async_ctx;
    if (ctx == nullptr) {
        // when ctx is nullptr, there must be error happened when on_chunk_data
        // and reply is sent, we just return with no operation
        LOG(WARNING) << "handler context is nullptr when MiniLoad callback execute, uri="
                     << http_req->uri();
        return;
    }
    if (ctx->body_bytes > 0 && ctx->bytes_written != ctx->body_bytes) {
        LOG(WARNING) << "bytes written is not equal with body size, uri=" << http_req->uri()
                     << ", body_bytes=" << ctx->body_bytes
                     << ", bytes_written=" << ctx->bytes_written;
        HttpChannel::send_reply(http_req, HttpStatus::INTERNAL_SERVER_ERROR,
                                "receipt size not equal with body size");
        return;
    }
    auto st = _load(http_req, ctx->file_path, ctx->load_check_req.user, ctx->load_check_req.cluster,
                    ctx->bytes_written);
    std::string str = to_json(st);
    HttpChannel::send_reply(http_req, str);
}

Status MiniLoadAction::generate_check_load_req(const HttpRequest* http_req,
                                               TLoadCheckRequest* check_load_req) {
    const char k_basic[] = "Basic ";
    const std::string& auth = http_req->header(HttpHeaders::AUTHORIZATION);
    if (auth.compare(0, sizeof(k_basic) - 1, k_basic, sizeof(k_basic) - 1) != 0) {
        return Status::InternalError("Not support Basic authorization.");
    }

    check_load_req->protocolVersion = FrontendServiceVersion::V1;
    // Skip "Basic "
    std::string str = auth.substr(sizeof(k_basic) - 1);
    std::string cluster;
    if (!parse_auth(str, &(check_load_req->user), &(check_load_req->passwd), &cluster)) {
        LOG(WARNING) << "parse auth string failed." << auth << " and str " << str;
        return Status::InternalError("Parse authorization failed.");
    }
    if (!cluster.empty()) {
        check_load_req->__set_cluster(cluster);
    }
    check_load_req->db = http_req->param(DB_KEY);
    check_load_req->__set_tbl(http_req->param(TABLE_KEY));
    if (http_req->param(SUB_LABEL_KEY).empty()) {
        check_load_req->__set_label(http_req->param(LABEL_KEY));
        check_load_req->__set_timestamp(GetCurrentTimeMicros());
    }

    if (http_req->remote_host() != nullptr) {
        std::string user_ip(http_req->remote_host());
        check_load_req->__set_user_ip(user_ip);
    }

    return Status::OK();
}

bool LoadHandleCmp::operator()(const LoadHandle& lhs, const LoadHandle& rhs) const {
    int ret = lhs.label.compare(rhs.label);
    if (ret < 0) {
        return true;
    } else if (ret > 0) {
        return false;
    }

    ret = lhs.sub_label.compare(rhs.sub_label);
    if (ret < 0) {
        return true;
    } else if (ret > 0) {
        return false;
    }

    ret = lhs.db.compare(rhs.db);
    if (ret < 0) {
        return true;
    }

    return false;
}

// fe will begin the txn and record the metadata of load
Status MiniLoadAction::_begin_mini_load(StreamLoadContext* ctx) {
    // prepare begin mini load request params
    TMiniLoadBeginRequest request;
    set_request_auth(&request, ctx->auth);
    request.db = ctx->db;
    request.tbl = ctx->table;
    request.label = ctx->label;
    if (!ctx->sub_label.empty()) {
        request.__set_sub_label(ctx->sub_label);
    }
    if (ctx->timeout_second != -1) {
        request.__set_timeout_second(ctx->timeout_second);
    }
    if (ctx->max_filter_ratio != 0.0) {
        request.__set_max_filter_ratio(ctx->max_filter_ratio);
    }
    request.__set_create_timestamp(UnixMillis());
    request.__set_request_id(ctx->id.to_thrift());
    // begin load by master
    const TNetworkAddress& master_addr = _exec_env->master_info()->network_address;
    TMiniLoadBeginResult res;
    RETURN_IF_ERROR(ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&request, &res](FrontendServiceConnection& client) {
                client->miniLoadBegin(res, request);
            }));
    Status begin_status(res.status);
    if (!begin_status.ok()) {
        LOG(INFO) << "failed to begin mini load " << ctx->label
                  << " with error msg:" << begin_status.get_error_msg();
        return begin_status;
    }
    ctx->txn_id = res.txn_id;
    // txn has been begun in fe
    ctx->need_rollback = true;
    LOG(INFO) << "load:" << ctx->label << " txn:" << res.txn_id << " has been begun in fe";
    return Status::OK();
}

Status MiniLoadAction::_process_put(HttpRequest* req, StreamLoadContext* ctx) {
    // prepare request parameters
    TStreamLoadPutRequest put_request;
    set_request_auth(&put_request, ctx->auth);
    put_request.db = ctx->db;
    put_request.tbl = ctx->table;
    put_request.txnId = ctx->txn_id;
    put_request.formatType = ctx->format;
    put_request.__set_loadId(ctx->id.to_thrift());
    put_request.fileType = TFileType::FILE_STREAM;
    std::map<std::string, std::string> params(req->query_params().begin(),
                                              req->query_params().end());
    /* merge params of columns and hll
     * for example:
     * input: columns=c1,tmp_c2,tmp_c3\&hll=hll_c2,tmp_c2:hll_c3,tmp_c3
     * output: columns=c1,tmp_c2,tmp_c3,hll_c2=hll_hash(tmp_c2),hll_c3=hll_hash(tmp_c3)
     */
    auto columns_it = params.find(COLUMNS_KEY);
    if (columns_it != params.end()) {
        std::string columns_value = columns_it->second;
        auto hll_it = params.find(HLL_KEY);
        if (hll_it != params.end()) {
            std::string hll_value = hll_it->second;
            if (hll_value.empty()) {
                return Status::InvalidArgument(
                        "Hll value could not be empty when hll key is exists!");
            }
            std::map<std::string, std::string> hll_map;
            RETURN_IF_ERROR(StringParser::split_string_to_map(hll_value, ":", ",", &hll_map));
            if (hll_map.empty()) {
                return Status::InvalidArgument("Hll value could not transform to hll expr: " +
                                               hll_value);
            }
            for (auto& hll_element : hll_map) {
                columns_value += "," + hll_element.first + "=hll_hash(" + hll_element.second + ")";
            }
        }
        put_request.__set_columns(columns_value);
    }
    auto column_separator_it = params.find(COLUMN_SEPARATOR_KEY);
    if (column_separator_it != params.end()) {
        put_request.__set_columnSeparator(column_separator_it->second);
    }
    if (ctx->timeout_second != -1) {
        put_request.__set_timeout(ctx->timeout_second);
    }
    auto strict_mode_it = params.find(STRICT_MODE_KEY);
    if (strict_mode_it != params.end()) {
        std::string strict_mode_value = strict_mode_it->second;
        if (boost::iequals(strict_mode_value, "false")) {
            put_request.__set_strictMode(false);
        } else if (boost::iequals(strict_mode_value, "true")) {
            put_request.__set_strictMode(true);
        } else {
            return Status::InvalidArgument("Invalid strict mode format. Must be bool type");
        }
    }

    // plan this load
    TNetworkAddress master_addr = _exec_env->master_info()->network_address;
    RETURN_IF_ERROR(ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&put_request, ctx](FrontendServiceConnection& client) {
                client->streamLoadPut(ctx->put_result, put_request);
            }));
    Status plan_status(ctx->put_result.status);
    if (!plan_status.ok()) {
        LOG(WARNING) << "plan streaming load failed. errmsg=" << plan_status.get_error_msg()
                     << ctx->brief();
        return plan_status;
    }
    VLOG_NOTICE << "params is " << apache::thrift::ThriftDebugString(ctx->put_result.params);
    return Status::OK();
}

// new on_header of mini load
Status MiniLoadAction::_on_new_header(HttpRequest* req) {
    size_t body_bytes = 0;
    size_t max_body_bytes = config::streaming_load_max_mb * 1024 * 1024;
    if (!req->header(HttpHeaders::CONTENT_LENGTH).empty()) {
        body_bytes = std::stol(req->header(HttpHeaders::CONTENT_LENGTH));
        if (body_bytes > max_body_bytes) {
            std::stringstream ss;
            ss << "file size exceed max body size, max_body_bytes=" << max_body_bytes;
            return Status::InvalidArgument(ss.str());
        }
    } else {
        evhttp_connection_set_max_body_size(
                evhttp_request_get_connection(req->get_evhttp_request()), max_body_bytes);
    }

    RETURN_IF_ERROR(check_request(req));

    StreamLoadContext* ctx = new StreamLoadContext(_exec_env);
    ctx->ref();
    ((MiniLoadCtx*)req->handler_ctx())->stream_load_ctx = ctx;

    // auth information
    if (!parse_basic_auth(*req, &ctx->auth)) {
        LOG(WARNING) << "parse basic authorization failed." << ctx->brief();
        return Status::InvalidArgument("no valid Basic authorization");
    }

    ctx->load_type = TLoadType::MINI_LOAD;
    ctx->load_src_type = TLoadSourceType::RAW;

    ctx->db = req->param(DB_KEY);
    ctx->table = req->param(TABLE_KEY);
    ctx->label = req->param(LABEL_KEY);
    if (!req->param(SUB_LABEL_KEY).empty()) {
        ctx->sub_label = req->param(SUB_LABEL_KEY);
    }
    ctx->format = TFileFormatType::FORMAT_CSV_PLAIN;
    std::map<std::string, std::string> params(req->query_params().begin(),
                                              req->query_params().end());
    auto max_filter_ratio_it = params.find(MAX_FILTER_RATIO_KEY);
    if (max_filter_ratio_it != params.end()) {
        ctx->max_filter_ratio = strtod(max_filter_ratio_it->second.c_str(), nullptr);
    }
    auto timeout_it = params.find(TIMEOUT_KEY);
    if (timeout_it != params.end()) {
        try {
            ctx->timeout_second = std::stoi(timeout_it->second);
        } catch (const std::invalid_argument& e) {
            return Status::InvalidArgument("Invalid timeout format");
        }
    }

    LOG(INFO) << "new income mini load request." << ctx->brief() << ", db: " << ctx->db
              << ", tbl: " << ctx->table;

    // record metadata in frontend
    RETURN_IF_ERROR(_begin_mini_load(ctx));

    // open sink
    auto pipe = std::make_shared<StreamLoadPipe>();
    RETURN_IF_ERROR(_exec_env->load_stream_mgr()->put(ctx->id, pipe));
    ctx->body_sink = pipe;

    // get plan from fe
    RETURN_IF_ERROR(_process_put(req, ctx));

    // execute plan
    return _exec_env->stream_load_executor()->execute_plan_fragment(ctx);
}

void MiniLoadAction::_new_handle(HttpRequest* req) {
    StreamLoadContext* ctx = ((MiniLoadCtx*)req->handler_ctx())->stream_load_ctx;
    DCHECK(ctx != nullptr);

    if (ctx->status.ok()) {
        ctx->status = _on_new_handle(ctx);
        if (!ctx->status.ok()) {
            LOG(WARNING) << "handle mini load failed, id=" << ctx->id
                         << ", errmsg=" << ctx->status.get_error_msg();
        }
    }

    // if failed to commit and status is not PUBLISH_TIMEOUT, rollback the txn.
    // PUBLISH_TIMEOUT is treated as OK in mini load, because user will use show load stmt
    // to see the final result.
    if (!ctx->status.ok() && ctx->status.code() != TStatusCode::PUBLISH_TIMEOUT) {
        if (ctx->need_rollback) {
            _exec_env->stream_load_executor()->rollback_txn(ctx);
            ctx->need_rollback = false;
        }
        if (ctx->body_sink.get() != nullptr) {
            ctx->body_sink->cancel(ctx->status.get_error_msg());
        }
    }

    std::string str = ctx->to_json_for_mini_load();
    str += '\n';
    HttpChannel::send_reply(req, str);
}

Status MiniLoadAction::_on_new_handle(StreamLoadContext* ctx) {
    if (ctx->body_bytes > 0 && ctx->receive_bytes != ctx->body_bytes) {
        LOG(WARNING) << "receive body don't equal with body bytes, body_bytes=" << ctx->body_bytes
                     << ", receive_bytes=" << ctx->receive_bytes << ", id=" << ctx->id;
        return Status::InternalError("receive body don't equal with body bytes");
    }

    // wait stream load sink finish
    RETURN_IF_ERROR(ctx->body_sink->finish());

    // wait stream load finish
    RETURN_IF_ERROR(ctx->future.get());

    // commit this load with mini load attachment
    RETURN_IF_ERROR(_exec_env->stream_load_executor()->commit_txn(ctx));

    return Status::OK();
}

} // namespace doris
