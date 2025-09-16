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

#include <string>
#include <vector>

#include "common/status.h"
#include "http/http_handler.h"
#include "http/http_handler_with_auth.h"
#include "util/threadpool.h"

struct bufferevent_rate_limit_group;

namespace doris {

class ExecEnv;
class HttpRequest;

// A simple handler that serves incoming HTTP requests of file-download to send their respective HTTP responses.
//
// TODO(lingbin): implements two useful header ('If-Modified-Since' and 'RANGE') to reduce
// transmission consumption.
// We use parameter named 'file' to specify the static resource path, it is an absolute path.
class DownloadAction : public HttpHandlerWithAuth {
public:
    DownloadAction(ExecEnv* exec_env,
                   std::shared_ptr<bufferevent_rate_limit_group> rate_limit_group,
                   const std::vector<std::string>& allow_dirs, int32_t num_workers = 0);

    // for load error
    DownloadAction(ExecEnv* exec_env, const std::string& error_log_root_dir);

    virtual ~DownloadAction() {}

    void handle(HttpRequest* req) override;

private:
    enum DOWNLOAD_TYPE {
        NORMAL = 1,
        ERROR_LOG = 2,
    };

    Status check_token(HttpRequest* req);
    Status check_path_is_allowed(const std::string& path);
    Status check_log_path_is_allowed(const std::string& file_path);

    void handle_normal(HttpRequest* req, const std::string& file_param);
    void handle_error_log(HttpRequest* req, const std::string& file_param);
    void _handle(HttpRequest* req);

    DOWNLOAD_TYPE _download_type;

    std::vector<std::string> _allow_paths;
    std::string _error_log_root_dir;
    int32_t _num_workers;
    std::unique_ptr<ThreadPool> _download_workers;

    std::shared_ptr<bufferevent_rate_limit_group> _rate_limit_group;
}; // end class DownloadAction

} // end namespace doris
