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
#pragma once
#include <brpc/controller.h>

#include "exec/scan_node.h"
#include "http/brpc/brpc_http_handler.h"
#include "http/brpc/handler_dispatcher.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"

namespace doris {

// A simple handler that serves incoming HTTP requests of file-download to send their respective HTTP responses.
//
// TODO(lingbin): implements two useful header ('If-Modified-Since' and 'RANGE') to reduce
// transmission consumption.
// We use parameter named 'file' to specify the static resource path, it is an absolute path.
class DownloadHandler : public BaseHttpHandler {
public:
    DownloadHandler(ExecEnv* exec_env, const std::vector<std::string>& allow_dirs);

    // for load error
    DownloadHandler(ExecEnv* exec_env, const std::string& error_log_root_dir);

    ~DownloadHandler() override = default;

    static void* send_file(void* raw_args);

    static void setup(ExecEnv* env, HandlerDispatcher* dispatcher);

protected:
    void handle_sync(brpc::Controller* cntl) override;

    bool support_method(brpc::HttpMethod method) const override;

private:
    enum DOWNLOAD_TYPE {
        NORMAL = 1,
        ERROR_LOG = 2,
    };

    struct FileArgs {
        butil::intrusive_ptr<brpc::ProgressiveAttachment> pa;
        int fd;
    };

    Status check_token(brpc::Controller* cntl);
    Status check_path_is_allowed(const std::string& path);
    Status check_log_path_is_allowed(const std::string& file_path);

    void handle_normal(brpc::Controller* cntl, const std::string& file_param);
    void handle_error_log(brpc::Controller* cntl, const std::string& file_param);

    void _do_file_response(const std::string& file_path, brpc::Controller* cntl);
    void _do_dir_response(const std::string& dir_path, brpc::Controller* cntl);

    DOWNLOAD_TYPE _download_type;

    std::vector<std::string> _allow_paths;
    std::string _error_log_root_dir;
};

} // namespace doris