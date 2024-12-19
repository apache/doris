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

#include "http/action/batch_download_action.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "gutil/strings/split.h"
#include "http/http_channel.h"
#include "http/http_method.h"
#include "http/http_request.h"
#include "http/utils.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "util/security.h"

namespace doris {
namespace {
const std::string CHECK_PARAMETER = "check";
const std::string LIST_PARAMETER = "list";
const std::string DIR_PARAMETER = "dir";
const std::string TOKEN_PARAMETER = "token";
} // namespace

BatchDownloadAction::BatchDownloadAction(
        ExecEnv* exec_env, std::shared_ptr<bufferevent_rate_limit_group> rate_limit_group,
        const std::vector<std::string>& allow_dirs)
        : HttpHandlerWithAuth(exec_env), _rate_limit_group(std::move(rate_limit_group)) {
    for (const auto& dir : allow_dirs) {
        std::string p;
        Status st = io::global_local_filesystem()->canonicalize(dir, &p);
        if (!st.ok()) {
            continue;
        }
        _allow_paths.emplace_back(std::move(p));
    }
}

void BatchDownloadAction::handle(HttpRequest* req) {
    if (VLOG_CRITICAL_IS_ON) {
        VLOG_CRITICAL << "accept one batch download request " << req->debug_string();
    }

    if (req->param(CHECK_PARAMETER) == "true") {
        // For API support check
        HttpChannel::send_reply(req, "OK");
        return;
    }

    // Get 'dir' parameter, then assembly file absolute path
    const std::string& dir_path = req->param(DIR_PARAMETER);
    if (dir_path.empty()) {
        std::string error_msg =
                std::string("parameter " + DIR_PARAMETER + " not specified in url.");
        LOG(WARNING) << "handle batch download request: " << error_msg
                     << ", url: " << mask_token(req->uri());
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, error_msg);
        return;
    }

    if (dir_path.find("..") != std::string::npos) {
        std::string error_msg = "Not allowed to read relative path: " + dir_path;
        LOG(WARNING) << "handle batch download request: " << error_msg
                     << ", url: " << mask_token(req->uri());
        HttpChannel::send_reply(req, HttpStatus::FORBIDDEN, error_msg);
        return;
    }

    Status status;
    if (config::enable_token_check) {
        status = _check_token(req);
        if (!status.ok()) {
            std::string error_msg = status.to_string();
            if (status.is<ErrorCode::NOT_AUTHORIZED>()) {
                HttpChannel::send_reply(req, HttpStatus::UNAUTHORIZED, error_msg);
                return;
            } else {
                HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR, error_msg);
                return;
            }
        }
    }

    status = _check_path_is_allowed(dir_path);
    if (!status.ok()) {
        std::string error_msg = status.to_string();
        if (status.is<ErrorCode::NOT_FOUND>() || status.is<ErrorCode::IO_ERROR>()) {
            HttpChannel::send_reply(req, HttpStatus::NOT_FOUND, error_msg);
            return;
        } else if (status.is<ErrorCode::NOT_AUTHORIZED>()) {
            HttpChannel::send_reply(req, HttpStatus::UNAUTHORIZED, error_msg);
            return;
        } else {
            HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR, error_msg);
            return;
        }
    }

    bool is_dir = false;
    status = io::global_local_filesystem()->is_directory(dir_path, &is_dir);
    if (!status.ok()) {
        LOG(WARNING) << "handle batch download request: " << status.to_string()
                     << ", url: " << mask_token(req->uri());
        HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR, status.to_string());
        return;
    }

    if (!is_dir) {
        std::string error_msg = fmt::format("The requested path is not a directory: {}", dir_path);
        LOG(WARNING) << "handle batch download request: " << error_msg
                     << ", url: " << mask_token(req->uri());
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, error_msg);
        return;
    }

    _handle(req, dir_path);

    VLOG_CRITICAL << "deal with batch download request finished! ";
}

void BatchDownloadAction::_handle(HttpRequest* req, const std::string& dir_path) {
    bool is_list_request = req->param(LIST_PARAMETER) == "true";
    if (is_list_request) {
        // return the list of files in the specified directory
        bool is_acquire_filesize = true;
        do_dir_response(dir_path, req, is_acquire_filesize);
    } else {
        _handle_batch_download(req, dir_path);
    }
}

void BatchDownloadAction::_handle_batch_download(HttpRequest* req, const std::string& dir_path) {
    std::vector<std::string> files =
            strings::Split(req->get_request_body(), "\n", strings::SkipWhitespace());
    if (files.empty()) {
        std::string error_msg = "No file specified in request body.";
        LOG(WARNING) << "handle batch download request: " << error_msg
                     << ", url: " << mask_token(req->uri());
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, error_msg);
        return;
    }

    if (files.size() > 64) {
        std::string error_msg =
                "The number of files to download in a batch should be less than 64.";
        LOG(WARNING) << "handle batch download request: " << error_msg
                     << ", url: " << mask_token(req->uri());
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, error_msg);
        return;
    }

    for (const auto& file : files) {
        if (file.find('/') != std::string::npos) {
            std::string error_msg =
                    fmt::format("Not allowed to read relative path: {}, dir: {}", file, dir_path);
            LOG(WARNING) << "handle batch download request: " << error_msg
                         << ", url: " << mask_token(req->uri());
            HttpChannel::send_reply(req, HttpStatus::FORBIDDEN, error_msg);
            return;
        }
    }

    HttpChannel::send_files(req, dir_path, std::move(files));
}

Status BatchDownloadAction::_check_token(HttpRequest* req) {
    const std::string& token_str = req->param(TOKEN_PARAMETER);
    if (token_str.empty()) {
        LOG(WARNING) << "token is not specified in request. url: " << mask_token(req->uri());
        return Status::NotAuthorized("token is not specified.");
    }

    const std::string& local_token = _exec_env->token();
    if (token_str != local_token) {
        LOG(WARNING) << "invalid download token: " << mask_token(token_str)
                     << ", local token: " << mask_token(local_token)
                     << ", url: " << mask_token(req->uri());
        return Status::NotAuthorized("invalid token {}", mask_token(token_str));
    }

    return Status::OK();
}

Status BatchDownloadAction::_check_path_is_allowed(const std::string& file_path) {
    std::string canonical_file_path;
    RETURN_IF_ERROR(io::global_local_filesystem()->canonicalize(file_path, &canonical_file_path));
    for (auto& allow_path : _allow_paths) {
        if (io::LocalFileSystem::contain_path(allow_path, canonical_file_path)) {
            return Status::OK();
        }
    }

    return Status::NotAuthorized("file path is not allowed: {}", canonical_file_path);
}

} // end namespace doris
