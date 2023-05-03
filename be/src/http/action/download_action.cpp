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

#include "http/action/download_action.h"

#include <algorithm>
#include <memory>
#include <sstream>
#include <string>
#include <utility>

#include "common/config.h"
#include "common/logging.h"
#include "http/http_channel.h"
#include "http/http_request.h"
#include "http/utils.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"

namespace doris {

const std::string FILE_PARAMETER = "file";
const std::string DB_PARAMETER = "db";
const std::string LABEL_PARAMETER = "label";
const std::string TOKEN_PARAMETER = "token";

DownloadAction::DownloadAction(ExecEnv* exec_env, const std::vector<std::string>& allow_dirs)
        : _exec_env(exec_env), _download_type(NORMAL) {
    for (auto& dir : allow_dirs) {
        std::string p;
        Status st = io::global_local_filesystem()->canonicalize(dir, &p);
        if (!st.ok()) {
            continue;
        }
        _allow_paths.emplace_back(std::move(p));
    }
}

DownloadAction::DownloadAction(ExecEnv* exec_env, const std::string& error_log_root_dir)
        : _exec_env(exec_env), _download_type(ERROR_LOG) {
    io::global_local_filesystem()->canonicalize(error_log_root_dir, &_error_log_root_dir);
}

void DownloadAction::handle_normal(HttpRequest* req, const std::string& file_param) {
    // check token
    Status status;
    if (config::enable_token_check) {
        status = check_token(req);
        if (!status.ok()) {
            std::string error_msg = status.to_string();
            if (status.is_not_authorized()) {
                HttpChannel::send_reply(req, HttpStatus::UNAUTHORIZED, error_msg);
                return;
            } else {
                HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR, error_msg);
                return;
            }
        }
    }

    status = check_path_is_allowed(file_param);
    if (!status.ok()) {
        std::string error_msg = status.to_string();
        if (status.is_not_found() || status.is_io_error()) {
            HttpChannel::send_reply(req, HttpStatus::NOT_FOUND, error_msg);
            return;
        } else if (status.is_not_authorized()) {
            HttpChannel::send_reply(req, HttpStatus::UNAUTHORIZED, error_msg);
            return;
        } else {
            HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR, error_msg);
            return;
        }
    }

    bool is_dir = false;
    status = io::global_local_filesystem()->is_directory(file_param, &is_dir);
    if (!status.ok()) {
        HttpChannel::send_reply(req, status.to_string());
        return;
    }

    if (is_dir) {
        do_dir_response(file_param, req);
    } else {
        do_file_response(file_param, req);
    }
}

void DownloadAction::handle_error_log(HttpRequest* req, const std::string& file_param) {
    const std::string absolute_path = _error_log_root_dir + "/" + file_param;

    Status status = check_log_path_is_allowed(absolute_path);
    if (!status.ok()) {
        std::string error_msg = status.to_string();
        if (status.is_not_authorized()) {
            HttpChannel::send_reply(req, HttpStatus::UNAUTHORIZED, error_msg);
            return;
        } else {
            HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR, error_msg);
            return;
        }
    }

    bool is_dir = false;
    status = io::global_local_filesystem()->is_directory(absolute_path, &is_dir);
    if (!status.ok()) {
        std::string error_msg = status.to_string();
        if (status.is_not_found() || status.is_io_error()) {
            HttpChannel::send_reply(req, HttpStatus::NOT_FOUND, error_msg);
            return;
        } else if (status.is_not_authorized()) {
            HttpChannel::send_reply(req, HttpStatus::UNAUTHORIZED, error_msg);
            return;
        } else {
            HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR, error_msg);
            return;
        }
    }
    if (is_dir) {
        std::string error_msg = "error log can only be file.";
        HttpChannel::send_reply(req, error_msg);
        return;
    }

    do_file_response(absolute_path, req);
}

void DownloadAction::handle(HttpRequest* req) {
    VLOG_CRITICAL << "accept one download request " << req->debug_string();

    // Get 'file' parameter, then assembly file absolute path
    const std::string& file_path = req->param(FILE_PARAMETER);
    if (file_path.empty()) {
        std::string error_msg =
                std::string("parameter " + FILE_PARAMETER + " not specified in url.");
        HttpChannel::send_reply(req, error_msg);
        return;
    }

    if (_download_type == ERROR_LOG) {
        handle_error_log(req, file_path);
    } else if (_download_type == NORMAL) {
        handle_normal(req, file_path);
    }

    VLOG_CRITICAL << "deal with download request finished! ";
}

Status DownloadAction::check_token(HttpRequest* req) {
    const std::string& token_str = req->param(TOKEN_PARAMETER);
    if (token_str.empty()) {
        return Status::NotAuthorized("token is not specified.");
    }

    if (token_str != _exec_env->token()) {
        return Status::NotAuthorized("invalid token.");
    }

    return Status::OK();
}

Status DownloadAction::check_path_is_allowed(const std::string& file_path) {
    DCHECK_EQ(_download_type, NORMAL);

    std::string canonical_file_path;
    RETURN_IF_ERROR(io::global_local_filesystem()->canonicalize(file_path, &canonical_file_path));
    for (auto& allow_path : _allow_paths) {
        if (io::LocalFileSystem::contain_path(allow_path, canonical_file_path)) {
            return Status::OK();
        }
    }

    return Status::NotAuthorized("file path is not allowed: {}", canonical_file_path);
}

Status DownloadAction::check_log_path_is_allowed(const std::string& file_path) {
    DCHECK_EQ(_download_type, ERROR_LOG);

    std::string canonical_file_path;
    RETURN_IF_ERROR(io::global_local_filesystem()->canonicalize(file_path, &canonical_file_path));
    if (io::LocalFileSystem::contain_path(_error_log_root_dir, canonical_file_path)) {
        return Status::OK();
    }

    return Status::NotAuthorized("file path is not allowed: {}", file_path);
}

} // end namespace doris
