// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>

#include <string>
#include <sstream>
#include <mutex>

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>

#include <functional>

#include "agent/cgroups_mgr.h"
#include "common/status.h"
#include "http/http_request.h"
#include "http/http_status.h"
#include "http/http_headers.h"
#include "http/http_response.h"
#include "http/http_channel.h"
#include "http/http_parser.h"
#include "olap/file_helper.h"
#include "util/url_coding.h"
#include "util/file_utils.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "runtime/load_path_mgr.h"
#include "gen_cpp/MasterService_types.h"
#include "gen_cpp/HeartbeatService_types.h"
#include "gen_cpp/FrontendService.h"

namespace palo {

const std::string CLUSTER_KEY = "cluster";
const std::string DB_KEY = "db";
const std::string TABLE_KEY = "table";
const std::string LABEL_KEY = "label";
const std::string SUB_LABEL_KEY = "sub_label";
const std::string FILE_PATH_KEY = "file_path";
const char* k_100_continue = "100-continue";

MiniLoadAction::MiniLoadAction(ExecEnv* exec_env) :
        _exec_env(exec_env) {
}

static bool is_name_valid(const std::string& name) {
    return !name.empty();
}

// send response
static void send_response(const Status& status, HttpChannel *channel) {
    std::stringstream ss;
    ss << "{\n";
    if (status.ok()) {
        ss << "\t\"status\": \"Success\",\n";
        ss << "\t\"msg\": \"OK\"\n";
    } else {
        ss << "\t\"status\": \"Fail\",\n";
        ss << "\t\"msg\": \"" << status.get_error_msg() << "\"\n";
    }
    ss << "}\n";

    std::string str = ss.str();
    HttpResponse response(HttpStatus::OK, &str);

    channel->send_response(response);
}

// send error
static void send_100_continue(HttpChannel *channel) {
    static HttpResponse response(HttpStatus::CONTINUE);

    channel->send_response(response);
}

static Status check_request(HttpRequest* req) {
    std::map<std::string, std::string>& params = *req->params();

    // check params
    if (!is_name_valid(params[DB_KEY])) {
        return Status("Database name is not valid.");
    }
    if (!is_name_valid(params[TABLE_KEY])) {
        return Status("Table name is not valid.");
    }
    if (!is_name_valid(params[LABEL_KEY])) {
        return Status("Label name is not valid.");
    }

    return Status::OK;
}

// Receive 'Transfer-Encoding: chunked' data from client
// Params:
//  file_handler file descriptor and operation for output file
//  channel      used to receive client data
static Status save_chunked_data(FileHandler* file_handler, HttpChannel *channel) {
    const int64_t BUF_SIZE = 4096;
    char *buf = new char[BUF_SIZE];
    DeferOp free_buf(std::bind<void>(std::default_delete<char[]>(), buf));
    HttpChunkParseCtx ctx;
    const uint8_t* pos = (const uint8_t*)buf;
    const uint8_t* end = pos;
    int64_t total_bytes = 0;
    int64_t max_bytes = config::mini_load_max_mb * 1024 * 1024;
    // To get length of first read
    HttpParser::ParseState state = HttpParser::http_parse_chunked(&pos, end - pos, &ctx);
    while (state != HttpParser::PARSE_DONE && state != HttpParser::PARSE_ERROR) {
        if (pos == end) {
            int64_t need_length = std::min(BUF_SIZE, ctx.length);
            int64_t read_len = channel->read(buf, need_length);
            if (read_len != need_length) {
                char errmsg[64];
                LOG(INFO) << "read chunked data failed, need=" << need_length
                    << " and read=" << read_len << " and ctx=" << ctx
                    << ",syserr=" << strerror_r(errno, errmsg, 64);
                return Status("Failed when receiving http packet.");
            }
            pos = (const uint8_t*)buf;
            end = pos + read_len;
            total_bytes += read_len;
        }
        if (total_bytes > max_bytes) {
            return Status("File size exceed max size we can support.");
        }
        state = HttpParser::http_parse_chunked(&pos, end - pos, &ctx);
        switch (state) {
        case HttpParser::PARSE_AGAIN:
            // Do nothing
            break;
        case HttpParser::PARSE_OK: {
            // data received
            int64_t size = std::min(ctx.size, end - pos);
            OLAPStatus wr_status = file_handler->write(pos, size);
            if (wr_status != OLAP_SUCCESS) {
                char errmsg[64];
                LOG(WARNING) << "Write to file("
                        << FileUtils::path_of_fd(file_handler->fd()) << ") failed. "
                        << "need=" << size
                        << ",syserr=" << strerror_r(errno, errmsg, 64);
                return Status("Failed when saving uploaded data");
            }
            ctx.size -= size;
            ctx.length -= size;
            pos += size;
            break;
        }
        case HttpParser::PARSE_DONE:
            break;
        case HttpParser::PARSE_ERROR:
            break;
        default:
            // Impossible state, must be a bug, output all information
            LOG(WARNING) << "Unknown http parse state(" << state << "), pos=" << pos
                << ",end=" << end << ",ctx=" << ctx;
            state = HttpParser::PARSE_ERROR;
            break;
        }
    }

    if (state == HttpParser::PARSE_DONE) {
        LOG(INFO) << "Save file to path "
            << FileUtils::path_of_fd(file_handler->fd()) << " success.";
        return Status::OK;
    } else {
        return Status("Error happend when palo parse your http packet.");
    }
}

static Status save_to_file(FileHandler* file_handler, int64_t len, HttpChannel* channel) {
    const int64_t BUF_SIZE = 4096;
    char *buf = new char[BUF_SIZE];
    DeferOp free_buf(std::bind<void>(std::default_delete<char[]>(), buf));
    int64_t to_read = len;
    while (to_read > 0) {
        int64_t to_read_this_time = std::min(to_read, BUF_SIZE);
        int64_t read_this_time = channel->read(buf, to_read_this_time);
        if (to_read_this_time != read_this_time) {
            // what can i do??
            char errmsg[64];
            LOG(INFO) << "read chunked data failed, need=" << to_read_this_time
                << " and read=" << read_this_time
                << ",syserr=" << strerror_r(errno, errmsg, 64);
            return Status("Failed when receiving http packet.");
        }
        OLAPStatus wr_status = file_handler->write(buf, read_this_time);
        if (wr_status != OLAP_SUCCESS) {
            char errmsg[64];
            LOG(WARNING) << "Write to file("
                    << FileUtils::path_of_fd(file_handler->fd()) << ") failed. "
                    << "need=" << read_this_time
                    << ",syserr=" << strerror_r(errno, errmsg, 64);
            return Status("Failed when saving uploaded data");
        }
        // write will write all buf into file, so that write_len == read_this_time
        to_read -= read_this_time;
    }
    LOG(INFO) << "Save file to path " << FileUtils::path_of_fd(file_handler->fd()) << " success.";
    return Status::OK;
}

Status MiniLoadAction::data_saved_dir(const LoadHandle& desc,
                                      const std::string& table,
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
    ss << prefix << "/" << table << "." << desc.sub_label
        << "." << buf << "." << tv.tv_usec;
    *file_path = ss.str();
    return Status::OK;
}

// Receive data from client
// TODO(zc): support range in HTTP
Status MiniLoadAction::receive_data(const LoadHandle& desc, HttpRequest* req,
                    HttpChannel *channel, std::string* file_path) {

    // add tid to cgroup
    CgroupsMgr::apply_system_cgroup();
    RETURN_IF_ERROR(data_saved_dir(desc, req->param(TABLE_KEY), file_path));
    // download

    // destructor will close the file handle, not depend on DeferOp any more
    FileHandler file_handler;
    OLAPStatus open_status = file_handler.open_with_mode(file_path->c_str(),
                                                        O_WRONLY | O_CREAT | O_TRUNC,
                                                        0660);
    if (open_status != OLAP_SUCCESS) {
        // open failed
        char buf[64];
        LOG(ERROR) << "open file failed." << *file_path << strerror_r(errno, buf, 64);
        return Status("Internal Error");
    }

    // After all thing prepare thing, then send '100-continue' to client
    if (strcasecmp(req->header(HttpHeaders::EXPECT).c_str(), k_100_continue) == 0) {
        // send 100 continue;
        send_100_continue(channel);
    }

    // Check if chunk first according rfc2616
    if (!req->header(HttpHeaders::TRANSFER_ENCODING).empty()) {
        if (req->header(HttpHeaders::TRANSFER_ENCODING) != "chunked") {
            std::stringstream ss;
            ss << "Unknown " << HttpHeaders::TRANSFER_ENCODING << ": "
                << req->header(HttpHeaders::TRANSFER_ENCODING);
            return Status(ss.str());
        }
        return save_chunked_data(&file_handler, channel);
    } else if (!req->header(HttpHeaders::CONTENT_LENGTH).empty()) {
        int64_t len = std::stol(req->header(HttpHeaders::CONTENT_LENGTH));
        if (len > config::mini_load_max_mb * 1024 * 1024) {
            return Status("File size exceed max size we can support.");
        }
        return save_to_file(&file_handler, len, channel);
    } else {
        std::stringstream ss;
        ss << "There is no " << HttpHeaders::TRANSFER_ENCODING << " nor "
            << HttpHeaders::CONTENT_LENGTH << " in request headers, you need pass me one";
        return Status(ss.str());
    }

    return Status::OK;
}

Status MiniLoadAction::load(
        HttpRequest* http_req, const std::string& file_path) {
    // Prepare request parameters.
    std::map<std::string, std::string> params(
            http_req->query_params().begin(), http_req->query_params().end());
    params.erase(LABEL_KEY);
    params.erase(SUB_LABEL_KEY);

    // put here to log master information
    const TNetworkAddress& master_address = _exec_env->master_info()->network_address;
    Status status;
    FrontendServiceConnection client(
            _exec_env->frontend_client_cache(), master_address, 500, &status);
    if (!status.ok()) {
        std::stringstream ss;
        ss << "Connect master failed, with address("
            << master_address.hostname << ":" << master_address.port << ")";
        LOG(WARNING) << ss.str();
        return status;
    }
    TFeResult res;
    try {
        TMiniLoadRequest req;
        req.protocolVersion = FrontendServiceVersion::V1;
        req.__set_db(http_req->param(DB_KEY));
        if (!_cluster.empty()) {
            req.__set_cluster(_cluster);
        }
        req.__set_tbl(http_req->param(TABLE_KEY));
        req.__set_label(http_req->param(LABEL_KEY));
        req.__set_user(_user);
        // Belong to a multi-load transaction
        if (!http_req->param(SUB_LABEL_KEY).empty()) {
            req.__set_subLabel(http_req->param(SUB_LABEL_KEY));
        }
        req.__set_properties(params);
        req.files.push_back(file_path);
        req.backend.__set_hostname(*_exec_env->local_ip());
        req.backend.__set_port(config::be_port);

        struct timeval tv;
        gettimeofday(&tv, NULL);
        req.__set_timestamp(tv.tv_sec * 1000 + tv.tv_usec / 1000);

        try {
            client->miniLoad(res, req);
        } catch (apache::thrift::transport::TTransportException& e) {
            LOG(WARNING) << "Retrying mini load from master("
                    << master_address.hostname << ":" << master_address.port
                    << ") because: " << e.what();
            status = client.reopen(500);
            if (!status.ok()) {
                LOG(WARNING) << "Client repoen failed. with address("
                    << master_address.hostname << ":" << master_address.port << ")";
                return status;
            }
            client->miniLoad(res, req);
        }
    } catch (apache::thrift::TException& e) {
        // failed when retry.
        std::stringstream ss;
        ss << "Request miniload from master("
            << master_address.hostname << ":" << master_address.port
            << ") because: " << e.what();
        LOG(WARNING) << ss.str();
        return Status(ss.str());
    }

    return Status(res.status);
}

static bool parse_auth(const std::string& auth, std::string* user, 
                           std::string* passwd, std::string* cluster) {
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
    std::string::size_type cluster_pos = decoded_auth.find('@');
    if (cluster_pos == std::string::npos) {
        cluster_pos = pos;
    } else {
        cluster->assign(decoded_auth.c_str(), cluster_pos + 1, (pos - cluster_pos - 1));
    }

    user->assign(decoded_auth.c_str(), cluster_pos);
    return true;
}

Status MiniLoadAction::check_auth(HttpRequest* http_req) {
    const char k_basic[] = "Basic ";
    const std::string& auth = http_req->header(HttpHeaders::AUTHORIZATION);
    if (auth.compare(0, sizeof(k_basic) - 1, k_basic, sizeof(k_basic) - 1) != 0) {
        return Status("Not support Basic authorization.");
    }
    // put here to log master information
    const TNetworkAddress& master_address = _exec_env->master_info()->network_address;
    Status status;
    FrontendServiceConnection client(
            _exec_env->frontend_client_cache(), master_address, 500, &status);
    if (!status.ok()) {
        std::stringstream ss;
        ss << "Connect master failed, with address("
            << master_address.hostname << ":" << master_address.port << ")";
        LOG(WARNING) << ss.str();
        return status;
    }

    TFeResult res;
    try {
        TLoadCheckRequest req;

        req.protocolVersion = FrontendServiceVersion::V1;
        // Skip "Basic "
        std::string str = auth.substr(sizeof(k_basic) - 1);
        std::string cluster;
        if (!parse_auth(str, &req.user, &req.passwd, &cluster)) {
            LOG(WARNING) << "parse auth string failed." << auth << " and str " << str;
            return Status("Parse authorization failed.");
        }
        if (!cluster.empty()) {
            req.__set_cluster(cluster);
        }
        req.db = http_req->param(DB_KEY);
        if (http_req->param(SUB_LABEL_KEY).empty()) {
            req.__set_label(http_req->param(LABEL_KEY));

            struct timeval tv;
            gettimeofday(&tv, NULL);
            req.__set_timestamp(tv.tv_sec * 1000 + tv.tv_usec / 1000);
        }

        try {
            client->loadCheck(res, req);
            _user.assign(req.user);
            _cluster.assign(cluster);
        } catch (apache::thrift::transport::TTransportException& e) {
            LOG(WARNING) << "Retrying mini load from master("
                    << master_address.hostname << ":" << master_address.port
                    << ") because: " << e.what();
            status = client.reopen(500);
            if (!status.ok()) {
                LOG(WARNING) << "Client repoen failed. with address("
                    << master_address.hostname << ":" << master_address.port << ")";
                return status;
            }
            client->loadCheck(res, req);
        }
    } catch (apache::thrift::TException& e) {
        // failed when retry.
        std::stringstream ss;
        ss << "Request miniload from master("
            << master_address.hostname << ":" << master_address.port
            << ") because: " << e.what();
        LOG(WARNING) << ss.str();
        return Status(ss.str());
    }

    return Status(res.status);
}

void MiniLoadAction::erase_handle(const LoadHandle& desc) {
    // remove
    std::lock_guard<std::mutex> l(_lock);
    _current_load.erase(desc);
}

void MiniLoadAction::handle(HttpRequest *req, HttpChannel *channel) {
    LOG(INFO) << "accept one request " << req->debug_string();

    // check authorization first, make client know what happend
    if (req->header(HttpHeaders::AUTHORIZATION).empty()) {
        channel->send_basic_challenge("mini_load");
        return;
    }
    Status status = check_request(req);
    if (!status.ok()) {
        send_response(status, channel);
        return;
    }
    LoadHandle desc;
    desc.db = req->param(DB_KEY);
    desc.label = req->param(LABEL_KEY);
    desc.sub_label = req->param(SUB_LABEL_KEY);

    // check if duplicate
    // Use this to prevent that two callback function write to one file
    // that file may be writen bad
    {
        std::lock_guard<std::mutex> l(_lock);
        if (_current_load.find(desc) != _current_load.end()) {
            // Already exists.
            status = Status("Duplicate mini load request.");
        } else {
            _current_load.insert(desc);
        }
    }
    // Send response without lock
    if (!status.ok()) {
        send_response(status, channel);
        return;
    }

    // Used to erase desc from map
    DeferOp erase(std::bind<void>(&MiniLoadAction::erase_handle, this, desc));

    // Check auth
    status = check_auth(req);
    if (!status.ok()) {
        send_response(status, channel);
        return;
    }

    // Receive data first, keep things easy.
    std::string file_path;
    status = receive_data(desc, req, channel, &file_path);
    if (!status.ok()) {
        send_response(status, channel);
        return;
    }
    status = load(req, file_path);
    send_response(status, channel);
}

bool LoadHandleCmp::operator() (const LoadHandle& lhs, const LoadHandle& rhs) const {
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

}
