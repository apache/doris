// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

#ifndef BDG_PALO_BE_SRC_COMMON_UTIL_WEBSERVER_H
#define BDG_PALO_BE_SRC_COMMON_UTIL_WEBSERVER_H

#include <pthread.h>

#include <string>
#include <map>
#include <boost/function.hpp>
#include <boost/thread/mutex.hpp>

#include "common/status.h"
#include "http/http_method.h"
#include "http/http_request.h"
#include "http/http_channel.h"
#include "http/http_handler.h"
#include "http/mongoose.h"
#include "util/path_trie.hpp"

namespace palo {

// Wrapper class for the Mongoose web server library. 
// Clients may register request handler with its path and method
class Webserver {
public:
    // Uses _config::webserver{port, interface}
    Webserver();

    ~Webserver();

    // Starts a webserver on the port passed to the constructor. The webserver runs in a
    // separate thread, so this call is non-blocking.
    Status start();

    // Stops the webserver synchronously.
    void stop();

    // Register one handler to 'path'.
    bool register_handler(const HttpMethod& method, const std::string& path, 
                          HttpHandler* handler);

private:
    // Get handler to this request
    HttpHandler* get_handler(HttpRequest& req);

    // Static so that it can act as a function pointer, and then call the next method
    static void* mongoose_callback_static(
            enum mg_event event,
            struct mg_connection* connection);

    // Dispatch point for all incoming requests.
    void* mongoose_callback(struct mg_connection* connection);

    PathTrie<HttpHandler*> _get_handlers;
    PathTrie<HttpHandler*> _put_handlers;
    PathTrie<HttpHandler*> _post_handlers;
    PathTrie<HttpHandler*> _delete_handlers;
    PathTrie<HttpHandler*> _head_handlers;
    PathTrie<HttpHandler*> _options_handlers;

    const int _port;
    // If empty, webserver will bind to all interfaces.
    const std::string _interface;

    // Handle to Mongoose context; owned and freed by Mongoose internally
    struct mg_context* _context;

    pthread_rwlock_t _rw_lock;
};

}

#endif
