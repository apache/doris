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

#include <event2/http.h>

#include <deque>
#include <memory>
#include <mutex>
#include <string>

#include "common/status.h"
#include "http/http_handler.h"
#include "http/http_request.h"
#include "util/byte_buffer.h"

namespace doris {

// Context for storing stream load forward request information
class StreamLoadForwardContext {
public:
    StreamLoadForwardContext() = default;
    ~StreamLoadForwardContext() {
        struct evhttp_connection* conn_to_free = nullptr;
        {
            std::lock_guard<std::mutex> lock(_mutex);
            // forward request will be released by libevent automatically
            forward_req = nullptr;
            if (conn && !connection_closed) {
                // Only free connection if it wasn't already released by libevent
                conn_to_free = conn;
                conn = nullptr;
            }
        }
        // Free connection outside of mutex to avoid deadlock with connection close callback
        if (conn_to_free) {
            evhttp_connection_free(conn_to_free);
        }
    }

    void set_forward_request(evhttp_request* req) {
        std::lock_guard<std::mutex> lock(_mutex);
        forward_req = req;
    }

    evhttp_request* get_forward_request() {
        std::lock_guard<std::mutex> lock(_mutex);
        return forward_req;
    }

    void handle_connection_close() {
        std::lock_guard<std::mutex> lock(_mutex);
        connection_closed = true;
        // Connection has been released by libevent automatically, set pointer to null
        // to prevent double-free in destructor
        conn = nullptr;
    }

    bool is_connection_closed() const {
        std::lock_guard<std::mutex> lock(_mutex);
        return connection_closed;
    }

    struct evhttp_connection* conn {nullptr};
    // Original request reference, lifecycle managed by HTTP framework
    HttpRequest* original_req {nullptr};

    std::string target_host;
    int target_port {0};

    // Buffer for collecting response data
    std::string response_data;

    std::deque<ByteBufferPtr> request_data_chunks;
    size_t total_request_size = 0;

private:
    mutable std::mutex _mutex;
    struct evhttp_request* forward_req {nullptr};
    bool connection_closed {false};
};

// Stream Load request forward handler
// Forwards Stream Load requests to other BE nodes
// Supports streaming forward, maintains original request path format: /api/{db}/{table}/_stream_load_forward
class StreamLoadForwardHandler : public HttpHandler {
public:
    StreamLoadForwardHandler() = default;
    ~StreamLoadForwardHandler() override = default;

    void handle(HttpRequest* req) override;

    bool request_will_be_read_progressively() override { return true; }

    int on_header(HttpRequest* req) override;

    void on_chunk_data(HttpRequest* req) override;

private:
    Status init_forward_request(HttpRequest* req, const std::string& target_host, int target_port,
                                std::shared_ptr<StreamLoadForwardContext>& ctx);

    static void forward_request_done(struct evhttp_request* req, void* arg);
    static void forward_request_chunked_cb(struct evhttp_request* req, void* arg);
    static void forward_connection_close_cb(struct evhttp_connection* conn, void* arg);

    // Response helper functions
    static void send_complete_response(struct evhttp_request* req, StreamLoadForwardContext* ctx,
                                       int response_code);
    static void copy_response_headers(struct evkeyvalq* input_headers,
                                      struct evkeyvalq* output_headers);

    Status parse_forward_target(const std::string& forward_to, std::string& host, int& port);

    std::string build_forward_url(HttpRequest* req);

    void setup_forward_headers(HttpRequest* req, struct evhttp_request* forward_req,
                               const std::string& target_host, int target_port);
};

} // namespace doris
