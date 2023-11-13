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

#include <memory>

#include "common/object_pool.h"
#include "common/status.h"

struct bufferevent_rate_limit_group;

namespace doris {

class ExecEnv;
class EvHttpServer;
class WebPageHandler;

// HTTP service for Doris BE
class HttpService {
public:
    HttpService(ExecEnv* env, int port, int num_threads);
    ~HttpService();

    Status start();
    void stop();

    // get real port
    int get_real_port() const;

private:
    ExecEnv* _env;
    ObjectPool _pool;

    std::unique_ptr<EvHttpServer> _ev_http_server;
    std::unique_ptr<WebPageHandler> _web_page_handler;

    std::shared_ptr<bufferevent_rate_limit_group> _rate_limit_group {nullptr};

    bool stopped = false;
};

} // namespace doris
