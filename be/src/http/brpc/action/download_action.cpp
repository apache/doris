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

#include "download_action.h"

#include <brpc/controller.h>
#include <brpc/http_method.h>
#include <unistd.h>

#include <sstream>

#include "agent/cgroups_mgr.h"
#include "http/http_headers.h"
#include "http/utils.h"
#include "util/file_utils.h"
#include "util/filesystem_util.h"

namespace doris {
const std::string FILE_PARAMETER = "file";
const std::string DB_PARAMETER = "db";
const std::string LABEL_PARAMETER = "label";
const std::string TOKEN_PARAMETER = "token";

DownloadHandler::DownloadHandler(ExecEnv* exec_env, const std::vector<std::string>& allow_dirs)
        : BaseHttpHandler("download", exec_env), _download_type(NORMAL) {
    for (auto& dir : allow_dirs) {
        std::string p;
        WARN_IF_ERROR(FileUtils::canonicalize(dir, &p), "canonicalize path " + dir + " failed");
        _allow_paths.emplace_back(std::move(p));
    }
}

DownloadHandler::DownloadHandler(ExecEnv* exec_env, const std::string& error_log_root_dir)
        : BaseHttpHandler("download", exec_env), _download_type(ERROR_LOG) {
    WARN_IF_ERROR(FileUtils::canonicalize(error_log_root_dir, &_error_log_root_dir),
                  "canonicalize path " + error_log_root_dir + " failed");
}

void DownloadHandler::handle_sync(brpc::Controller* cntl) {
    // add tid to cgroup in order to limit read bandwidth
    CgroupsMgr::apply_system_cgroup();

    // Get 'file' parameter, then assembly file absolute path
    const std::string& file_path = *get_param(cntl, FILE_PARAMETER);
    if (file_path.empty()) {
        std::string error_msg =
                std::string("parameter " + FILE_PARAMETER + " not specified in url.");
        on_succ(cntl, error_msg);
        return;
    }

    if (_download_type == ERROR_LOG) {
        handle_error_log(cntl, file_path);
    } else if (_download_type == NORMAL) {
        handle_normal(cntl, file_path);
    }

    VLOG_CRITICAL << "deal with download request finished! ";
}

bool DownloadHandler::support_method(brpc::HttpMethod method) const {
    return method == brpc::HTTP_METHOD_HEAD || method == brpc::HTTP_METHOD_GET;
}

void DownloadHandler::handle_normal(brpc::Controller* cntl, const std::string& file_param) {
    // check token
    Status status;
    if (config::enable_token_check) {
        status = check_token(cntl);
        if (!status.ok()) {
            on_succ(cntl, status.to_string());
            return;
        }
    }

    status = check_path_is_allowed(file_param);
    if (!status.ok()) {
        on_succ(cntl, status.to_string());
        return;
    }

    if (FileUtils::is_dir(file_param)) {
        _do_dir_response(file_param, cntl);
    } else {
        _do_file_response(file_param, cntl);
    }
}

void DownloadHandler::handle_error_log(brpc::Controller* cntl, const std::string& file_param) {
    const std::string absolute_path = _error_log_root_dir + "/" + file_param;

    Status status = check_log_path_is_allowed(absolute_path);
    if (!status.ok()) {
        std::string error_msg = status.to_string();
        on_succ(cntl, error_msg);
        return;
    }

    if (FileUtils::is_dir(absolute_path)) {
        std::string error_msg = "error log can only be file.";
        on_succ(cntl, error_msg);
        return;
    }

    _do_file_response(absolute_path, cntl);
}

void DownloadHandler::_do_file_response(const std::string& file_path, brpc::Controller* cntl) {
    if (file_path.find("..") != std::string::npos) {
        std::stringstream ss;
        ss << "Not allowed to read relative path: " << file_path;
        LOG(WARNING) << ss.str();
        on_fobidden(cntl, ss.str());
        return;
    }

    // read file content and send response
    int fd = open(file_path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::stringstream ss;
        ss << "Failed to open file: " << file_path;
        LOG(WARNING) << ss.str();
        on_not_found(cntl, ss.str());
        return;
    }
    struct stat st;
    auto res = fstat(fd, &st);
    if (res < 0) {
        close(fd);
        std::stringstream ss;
        ss << "Failed to open file: " << file_path;
        LOG(WARNING) << ss.str();
        on_not_found(cntl, ss.str());
        return;
    }

    int64_t file_size = st.st_size;

    // TODO(lingbin): process "IF_MODIFIED_SINCE" header
    // TODO(lingbin): process "RANGE" header
    const std::string& range_header = *get_header(cntl, HttpHeaders::RANGE);
    if (!range_header.empty()) {
        // analyse range header
    }
    cntl->http_response().AppendHeader(HttpHeaders::CONTENT_TYPE, get_content_type(file_path));
    if (cntl->http_request().method() == brpc::HTTP_METHOD_HEAD) {
        close(fd);
        cntl->http_response().AppendHeader(HttpHeaders::CONTENT_LENGTH,
                                           std::to_string(file_size).c_str());
        on_succ(cntl, "");
        return;
    }
    std::unique_ptr<FileArgs> args(new FileArgs);
    args->fd = fd;
    args->pa = cntl->CreateProgressiveAttachment();
    bthread_t th;
    bthread_start_background(&th, nullptr, send_file, args.release());
}

void DownloadHandler::_do_dir_response(const std::string& dir_path, brpc::Controller* cntl) {
    std::vector<std::string> files;
    Status status = FileUtils::list_files(Env::Default(), dir_path, &files);
    if (!status.ok()) {
        std::stringstream ss;
        ss << "Failed to scan dir. dir=" << dir_path;
        LOG(WARNING) << ss.str();
        on_error(cntl, ss.str());
    }

    const std::string FILE_DELIMITER_IN_DIR_RESPONSE = "\n";

    std::stringstream result;
    for (const std::string& file_name : files) {
        result << file_name << FILE_DELIMITER_IN_DIR_RESPONSE;
    }

    std::string result_str = result.str();
    on_succ(cntl, result_str);
}

void* DownloadHandler::send_file(void* raw_args) {
    std::unique_ptr<FileArgs> args(static_cast<FileArgs*>(raw_args));
    if (!args->pa) {
        LOG(WARNING) << "ProgressiveAttachment is NULL";
        return nullptr;
    }
    do {
        char buf[2048];
        int read_size = read(args->fd, buf, sizeof(buf));
        if (read_size < 0) {
            const std::string err_msg = "upload failed";
            LOG(WARNING) << err_msg;
            break;
        }
        if (read_size == 0) {
            break;
        }
        args->pa->Write(buf, read_size);
        //sleep a while to send another part.
        bthread_usleep(10000);
    } while (true);
    close(args->fd);
    return nullptr;
}

void DownloadHandler::setup(ExecEnv* env, HandlerDispatcher* dispatcher) {
    std::vector<std::string> allow_paths;
    for (auto& path : env->store_paths()) {
        allow_paths.emplace_back(path.path);
    }
    dispatcher->add_handler(new DownloadHandler(env, allow_paths));
}

Status DownloadHandler::check_token(brpc::Controller* cntl) {
    const std::string& token_str = *get_param(cntl, TOKEN_PARAMETER);
    if (token_str.empty()) {
        return Status::InternalError("token is not specified.");
    }

    if (token_str != get_exec_env()->token()) {
        return Status::InternalError("invalid token.");
    }
    return Status::OK();
}

Status DownloadHandler::check_path_is_allowed(const std::string& file_path) {
    DCHECK_EQ(_download_type, NORMAL);

    std::string canonical_file_path;
    RETURN_WITH_WARN_IF_ERROR(FileUtils::canonicalize(file_path, &canonical_file_path),
                              Status::InternalError("file path is invalid: {}", file_path),
                              "file path is invalid: " + file_path);

    for (auto& allow_path : _allow_paths) {
        if (FileSystemUtil::contain_path(allow_path, canonical_file_path)) {
            return Status::OK();
        }
    }

    return Status::InternalError("file path is not allowed: {}", canonical_file_path);
}

Status DownloadHandler::check_log_path_is_allowed(const std::string& file_path) {
    DCHECK_EQ(_download_type, ERROR_LOG);

    std::string canonical_file_path;
    RETURN_WITH_WARN_IF_ERROR(FileUtils::canonicalize(file_path, &canonical_file_path),
                              Status::InternalError("file path is invalid: {}", file_path),
                              "file path is invalid: " + file_path);

    if (FileSystemUtil::contain_path(_error_log_root_dir, canonical_file_path)) {
        return Status::OK();
    }

    return Status::InternalError("file path is not allowed: {}", file_path);
}

} // namespace doris