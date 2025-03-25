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

// A simple handler that serves incoming HTTP requests of batching file-download to send their
// respective HTTP responses.
//
// We use parameter named 'dir' to specify the static resource path, it is an absolute path.
//
// In HEAD request, then this handler will return the list of files in the specified directory.
//
// In GET request, the file names to download are specified in the request body as a list of strings,
// separated by '\n'. To avoid cost resource, the maximum number of files to download in a batch is 64.
class BatchDownloadAction : public HttpHandlerWithAuth {
public:
    BatchDownloadAction(ExecEnv* exec_env,
                        std::shared_ptr<bufferevent_rate_limit_group> rate_limit_group,
                        const std::vector<std::string>& allow_dirs);

    ~BatchDownloadAction() override = default;

    void handle(HttpRequest* req) override;

private:
    Status _check_token(HttpRequest* req);
    Status _check_path_is_allowed(const std::string& path);

    void _handle(HttpRequest* req, const std::string& dir_path);
    void _handle_batch_download(HttpRequest* req, const std::string& dir_path);

    std::vector<std::string> _allow_paths;
    std::shared_ptr<bufferevent_rate_limit_group> _rate_limit_group;
};

} // end namespace doris
