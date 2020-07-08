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
#include <thread>
#include <vector>

#include "common/status.h"
#include "util/path_trie.hpp"
#include "http/http_method.h"

namespace doris {

class HttpHandler;
class HttpRequest;

class EvHttpServer {
public:
    EvHttpServer(int port, int num_workers = 1);
    EvHttpServer(const std::string& host, int port, int num_workers = 1);
    ~EvHttpServer();

    // register handler for an a path-method pair
    bool register_handler(
        const HttpMethod& method, const std::string& path, HttpHandler* handler);

    void register_static_file_handler(HttpHandler* handler);

    Status start();
    void stop();
    void join();

    // callback 
    int on_header(struct evhttp_request* ev_req);

    // get real port
    int get_real_port() {
        return _real_port;
    }

private:
    Status _bind();
    HttpHandler* _find_handler(HttpRequest* req);

private:
    // input param
    std::string _host;
    int _port;
    int _num_workers;
    // used for unittest, set port to 0, os will choose a free port;
    int _real_port;

    int _server_fd = -1;
    std::vector<std::thread> _workers;

    pthread_rwlock_t _rw_lock;

    PathTrie<HttpHandler*> _get_handlers;
    HttpHandler* _static_file_handler = nullptr;
    PathTrie<HttpHandler*> _put_handlers;
    PathTrie<HttpHandler*> _post_handlers;
    PathTrie<HttpHandler*> _delete_handlers;
    PathTrie<HttpHandler*> _head_handlers;
    PathTrie<HttpHandler*> _options_handlers;
};

}
