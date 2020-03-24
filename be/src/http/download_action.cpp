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

#include "http/download_action.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include <string>
#include <sstream>

#include "agent/cgroups_mgr.h"
#include "env/env.h"
#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_response.h"
#include "http/http_status.h"
#include "runtime/exec_env.h"
#include "util/file_utils.h"
#include "util/filesystem_util.h"
#include "util/path_util.h"

namespace doris {

const std::string FILE_PARAMETER = "file";
const std::string DB_PARAMETER = "db";
const std::string LABEL_PARAMETER = "label";
const std::string TOKEN_PARAMETER = "token";

DownloadAction::DownloadAction(ExecEnv* exec_env, const std::vector<std::string>& allow_dirs) :
    _exec_env(exec_env),
    _download_type(NORMAL) {
    for (auto& dir : allow_dirs) {
        std::string p;
        WARN_IF_ERROR(FileUtils::canonicalize(dir, &p), "canonicalize path " + dir + " failed");
        _allow_paths.emplace_back(std::move(p));
    }
}

DownloadAction::DownloadAction(ExecEnv* exec_env, const std::string& error_log_root_dir) :
    _exec_env(exec_env),
    _download_type(ERROR_LOG) {
    WARN_IF_ERROR(FileUtils::canonicalize(error_log_root_dir, &_error_log_root_dir),
                  "canonicalize path " + error_log_root_dir + " failed");
}

void DownloadAction::handle_normal(
        HttpRequest *req,
        const std::string& file_param) {
    // check token
    Status status;
    if (config::enable_token_check) {
        status = check_token(req);
        if (!status.ok()) {
            std::string error_msg = status.get_error_msg();
            HttpChannel::send_reply(req, error_msg);
            return;
        }
    }

    status = check_path_is_allowed(file_param);
    if (!status.ok()) {
        std::string error_msg = status.get_error_msg();
        HttpChannel::send_reply(req, error_msg);
        return;
    }

    if (FileUtils::is_dir(file_param)) {
        do_dir_response(file_param, req);
    } else {
        do_file_response(file_param, req);
    }
}

void DownloadAction::handle_error_log(
        HttpRequest *req,
        const std::string& file_param) {
    const std::string absolute_path = _error_log_root_dir + "/" + file_param;

    Status status = check_log_path_is_allowed(absolute_path);
    if (!status.ok()) {
        std::string error_msg = status.get_error_msg();
        HttpChannel::send_reply(req, error_msg);
        return;
    }

    if (FileUtils::is_dir(absolute_path)) {
        std::string error_msg = "error log can only be file.";
        HttpChannel::send_reply(req, error_msg);
        return;
    }

    do_file_response(absolute_path, req);
}

void DownloadAction::handle(HttpRequest *req) {
    LOG(INFO) << "accept one download request " << req->debug_string();

    // add tid to cgroup in order to limit read bandwidth
    CgroupsMgr::apply_system_cgroup();

    // Get 'file' parameter, then assembly file absolute path
    const std::string& file_path = req->param(FILE_PARAMETER);
    if (file_path.empty()) {
        std::string error_msg = std::string(
                "parameter " + FILE_PARAMETER + " not specified in url.");
        HttpChannel::send_reply(req, error_msg);
        return;
    }

    if (_download_type == ERROR_LOG) {
        handle_error_log(req, file_path);
    } else if (_download_type == NORMAL) {
        handle_normal(req, file_path);
    }

    LOG(INFO) << "deal with download requesst finished! ";
}

void DownloadAction::do_dir_response(
        const std::string& dir_path, HttpRequest *req) {
    std::vector<std::string> files;
    Status status = FileUtils::list_files(Env::Default(), dir_path, &files);
    if (!status.ok()) {
        LOG(WARNING) << "Failed to scan dir. dir=" << dir_path;
        HttpChannel::send_error(req, HttpStatus::INTERNAL_SERVER_ERROR);
    }

    const std::string FILE_DELIMETER_IN_DIR_RESPONSE = "\n";

    std::stringstream result;
    for (const std::string& file_name : files) {
        result << file_name << FILE_DELIMETER_IN_DIR_RESPONSE;
    }

    std::string result_str = result.str();
    HttpChannel::send_reply(req, result_str);
    return;
}

void DownloadAction::do_file_response(const std::string& file_path, HttpRequest *req) {
    // read file content and send response
    int fd = open(file_path.c_str(), O_RDONLY);
    if (fd < 0) {
        LOG(WARNING) << "Failed to open file: " << file_path;
        HttpChannel::send_error(req, HttpStatus::NOT_FOUND);
        return;
    }
    struct stat st;
    auto res = fstat(fd, &st);
    if (res < 0) {
        close(fd);
        LOG(WARNING) << "Failed to open file: " << file_path;
        HttpChannel::send_error(req, HttpStatus::NOT_FOUND);
        return;
    }

    int64_t file_size = st.st_size;

    // TODO(lingbin): process "IF_MODIFIED_SINCE" header
    // TODO(lingbin): process "RANGE" header
    const std::string& range_header = req->header(HttpHeaders::RANGE);
    if (!range_header.empty()) {
        // analyse range header
    }

    req->add_output_header(HttpHeaders::CONTENT_TYPE, get_content_type(file_path).c_str());

    if (req->method() == HttpMethod::HEAD) {
        close(fd);
        req->add_output_header(HttpHeaders::CONTENT_LENGTH, std::to_string(file_size).c_str());
        HttpChannel::send_reply(req);
        return;
    }

    HttpChannel::send_file(req, fd, 0, file_size);
}

// Do a simple decision, only deal a few type
std::string DownloadAction::get_content_type(const std::string& file_name) {
    std::string file_ext = path_util::file_extension(file_name);
    LOG(INFO) << "file_name: " << file_name << "; file extension: [" << file_ext << "]";
    if (file_ext == std::string(".html")
            || file_ext == std::string(".htm")) {
        return std::string("text/html; charset=utf-8");
    } else if (file_ext == std::string(".js")) {
        return std::string("application/javascript; charset=utf-8");
    } else if (file_ext == std::string(".css")) {
        return std::string("text/css; charset=utf-8");
    } else if (file_ext == std::string(".txt")) {
        return std::string("text/plain; charset=utf-8");
    } else {
        return "text/plain; charset=utf-8";
    }
    return "";
}

Status DownloadAction::check_token(HttpRequest *req) {
    const std::string& token_str = req->param(TOKEN_PARAMETER);
    if (token_str.empty()) {
        return Status::InternalError("token is not specified.");
    }

    if (token_str != _exec_env->token()) {
        return Status::InternalError("invalid token.");
    }

    return Status::OK();
}

Status DownloadAction::check_path_is_allowed(const std::string& file_path) {
    DCHECK_EQ(_download_type, NORMAL);

    std::string canonical_file_path;
    RETURN_WITH_WARN_IF_ERROR(FileUtils::canonicalize(file_path, &canonical_file_path),
                              Status::InternalError("file path is invalid: " + file_path),
                              "file path is invalid: " + file_path);

    for (auto& allow_path : _allow_paths) {
        if (FileSystemUtil::contain_path(allow_path, canonical_file_path)) {
            return Status::OK();
        }
    }

    return Status::InternalError("file path is not allowed: " + canonical_file_path);
}

Status DownloadAction::check_log_path_is_allowed(const std::string& file_path) {
    DCHECK_EQ(_download_type, ERROR_LOG);

    std::string canonical_file_path;
    RETURN_WITH_WARN_IF_ERROR(FileUtils::canonicalize(file_path, &canonical_file_path),
                              Status::InternalError("file path is invalid: " + file_path),
                              "file path is invalid: " + file_path);

    if (FileSystemUtil::contain_path(_error_log_root_dir, canonical_file_path)) {
        return Status::OK();
    }

    return Status::InternalError("file path is not allowed: " + file_path);
}

} // end namespace doris

