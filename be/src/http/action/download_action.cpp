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

#include <sys/types.h>
#include <unistd.h>

#include <sstream>
#include <string>

#include "agent/cgroups_mgr.h"
#include "env/env.h"
#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_response.h"
#include "http/http_status.h"
#include "http/utils.h"
#include "runtime/exec_env.h"
#include "util/file_utils.h"
#include "util/filesystem_util.h"
#include "util/path_util.h"

namespace doris {

const std::string FILE_PARAMETER = "file";
const std::string DB_PARAMETER = "db";
const std::string LABEL_PARAMETER = "label";
const std::string TOKEN_PARAMETER = "token";

DownloadAction::DownloadAction(ExecEnv* exec_env, const std::vector<std::string>& allow_dirs)
        : _exec_env(exec_env), _download_type(NORMAL) {
    for (auto& dir : allow_dirs) {
        std::string p;
        WARN_IF_ERROR(FileUtils::canonicalize(dir, &p), "canonicalize path " + dir + " failed");
        _allow_paths.emplace_back(std::move(p));
    }
}

DownloadAction::DownloadAction(ExecEnv* exec_env, const std::string& error_log_root_dir)
        : _exec_env(exec_env), _download_type(ERROR_LOG) {
    WARN_IF_ERROR(FileUtils::canonicalize(error_log_root_dir, &_error_log_root_dir),
                  "canonicalize path " + error_log_root_dir + " failed");
}

void DownloadAction::handle_normal(HttpRequest* req, const std::string& file_param) {
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

void DownloadAction::handle_error_log(HttpRequest* req, const std::string& file_param) {
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

void DownloadAction::handle(HttpRequest* req) {
    LOG(INFO) << "accept one download request " << req->debug_string();

    // add tid to cgroup in order to limit read bandwidth
    CgroupsMgr::apply_system_cgroup();

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

    LOG(INFO) << "deal with download request finished! ";
}

Status DownloadAction::check_token(HttpRequest* req) {
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
