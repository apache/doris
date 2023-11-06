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

#include "http/ev_http_server.h"

#include <arpa/inet.h>
#include <butil/endpoint.h>
#include <butil/fd_utility.h>
// IWYU pragma: no_include <bthread/errno.h>
#include <errno.h> // IWYU pragma: keep
#include <event2/event.h>
#include <event2/http.h>
#include <event2/http_struct.h>
#include <event2/thread.h>
#include <netinet/in.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <memory>
#include <sstream>

#include "common/logging.h"
#include "http/http_channel.h"
#include "http/http_handler.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_status.h"
#include "service/backend_options.h"
#include "util/threadpool.h"

struct event_base;
struct evhttp;

namespace doris {

static void on_chunked(struct evhttp_request* ev_req, void* param) {
    HttpRequest* request = (HttpRequest*)ev_req->on_free_cb_arg;
    request->handler()->on_chunk_data(request);
}

static void on_free(struct evhttp_request* ev_req, void* arg) {
    HttpRequest* request = (HttpRequest*)arg;
    delete request;
}

static void on_request(struct evhttp_request* ev_req, void* arg) {
    auto request = (HttpRequest*)ev_req->on_free_cb_arg;
    if (request == nullptr) {
        // In this case, request's on_header return -1
        return;
    }
    request->handler()->handle(request);
}

static int on_header(struct evhttp_request* ev_req, void* param) {
    EvHttpServer* server = (EvHttpServer*)ev_req->on_complete_cb_arg;
    return server->on_header(ev_req);
}

// param is pointer of EvHttpServer
static int on_connection(struct evhttp_request* req, void* param) {
    evhttp_request_set_header_cb(req, on_header);
    // only used on_complete_cb's argument
    evhttp_request_set_on_complete_cb(req, nullptr, param);
    return 0;
}

EvHttpServer::EvHttpServer(int port, int num_workers)
        : _port(port), _num_workers(num_workers), _real_port(0) {
    _host = BackendOptions::get_service_bind_address();

    evthread_use_pthreads();
    DCHECK_GT(_num_workers, 0);
    _event_bases.resize(_num_workers);
    for (int i = 0; i < _num_workers; ++i) {
        std::shared_ptr<event_base> base(event_base_new(),
                                         [](event_base* base) { event_base_free(base); });
        CHECK(base != nullptr) << "Couldn't create an event_base.";
        std::lock_guard lock(_event_bases_lock);
        _event_bases[i] = base;
    }
}

EvHttpServer::EvHttpServer(const std::string& host, int port, int num_workers)
        : _host(host), _port(port), _num_workers(num_workers), _real_port(0) {
    DCHECK_GT(_num_workers, 0);
}

EvHttpServer::~EvHttpServer() {
    if (_started) {
        stop();
    }
}

void EvHttpServer::start() {
    _started = true;
    // bind to
    auto s = _bind();
    CHECK(s.ok()) << s.to_string();
    static_cast<void>(ThreadPoolBuilder("EvHttpServer")
                              .set_min_threads(_num_workers)
                              .set_max_threads(_num_workers)
                              .build(&_workers));
    for (int i = 0; i < _num_workers; ++i) {
        auto status = _workers->submit_func([this, i]() {
            std::shared_ptr<event_base> base;
            {
                std::lock_guard lock(_event_bases_lock);
                base = _event_bases[i];
            }

            /* Create a new evhttp object to handle requests. */
            std::shared_ptr<evhttp> http(evhttp_new(base.get()),
                                         [](evhttp* http) { evhttp_free(http); });
            CHECK(http != nullptr) << "Couldn't create an evhttp.";

            auto res = evhttp_accept_socket(http.get(), _server_fd);
            CHECK(res >= 0) << "evhttp accept socket failed, res=" << res;

            evhttp_set_newreqcb(http.get(), on_connection, this);
            evhttp_set_gencb(http.get(), on_request, this);

            event_base_dispatch(base.get());
        });
        CHECK(status.ok());
    }
}

void EvHttpServer::stop() {
    {
        std::lock_guard<std::mutex> lock(_event_bases_lock);
        for (int i = 0; i < _num_workers; ++i) {
            event_base_loopbreak(_event_bases[i].get());
        }
        _event_bases.clear();
    }
    _workers->shutdown();
    close(_server_fd);
    _started = false;
}

void EvHttpServer::join() {}

Status EvHttpServer::_bind() {
    butil::EndPoint point;
    auto res = butil::str2endpoint(_host.c_str(), _port, &point);
    if (res < 0) {
        return Status::InternalError("convert address failed, host={}, port={}", _host, _port);
    }
    _server_fd = butil::tcp_listen(point);
    if (_server_fd < 0) {
        char buf[64];
        std::stringstream ss;
        ss << "tcp listen failed, errno=" << errno
           << ", errmsg=" << strerror_r(errno, buf, sizeof(buf));
        return Status::InternalError(ss.str());
    }
    if (_port == 0) {
        struct sockaddr_in addr;
        socklen_t socklen = sizeof(addr);
        const int rc = getsockname(_server_fd, (struct sockaddr*)&addr, &socklen);
        if (rc == 0) {
            _real_port = ntohs(addr.sin_port);
        }
    }
    res = butil::make_non_blocking(_server_fd);
    if (res < 0) {
        char buf[64];
        std::stringstream ss;
        ss << "make socket to non_blocking failed, errno=" << errno
           << ", errmsg=" << strerror_r(errno, buf, sizeof(buf));
        return Status::InternalError(ss.str());
    }
    return Status::OK();
}

bool EvHttpServer::register_handler(const HttpMethod& method, const std::string& path,
                                    HttpHandler* handler) {
    if (handler == nullptr) {
        LOG(WARNING) << "dummy handler for http method " << method << " with path " << path;
        return false;
    }

    bool result = true;
    std::lock_guard<std::mutex> lock(_handler_lock);
    PathTrie<HttpHandler*>* root = nullptr;
    switch (method) {
    case GET:
        root = &_get_handlers;
        break;
    case PUT:
        root = &_put_handlers;
        break;
    case POST:
        root = &_post_handlers;
        break;
    case DELETE:
        root = &_delete_handlers;
        break;
    case HEAD:
        root = &_head_handlers;
        break;
    case OPTIONS:
        root = &_options_handlers;
        break;
    default:
        LOG(WARNING) << "unknown HTTP method, method=" << method;
        result = false;
    }
    if (result) {
        result = root->insert(path, handler);
    }

    return result;
}

void EvHttpServer::register_static_file_handler(HttpHandler* handler) {
    DCHECK(handler != nullptr);
    DCHECK(_static_file_handler == nullptr);
    std::lock_guard<std::mutex> lock(_handler_lock);
    _static_file_handler = handler;
}

int EvHttpServer::on_header(struct evhttp_request* ev_req) {
    std::unique_ptr<HttpRequest> request(new HttpRequest(ev_req));
    auto res = request->init_from_evhttp();
    if (res < 0) {
        return -1;
    }
    auto handler = _find_handler(request.get());
    if (handler == nullptr) {
        evhttp_remove_header(evhttp_request_get_input_headers(ev_req), HttpHeaders::EXPECT);
        HttpChannel::send_reply(request.get(), HttpStatus::NOT_FOUND, "Not Found");
        return 0;
    }
    // set handler before call on_header, because handler_ctx will set in on_header
    request->set_handler(handler);
    res = handler->on_header(request.get());
    if (res < 0) {
        // reply has already sent by handler's on_header
        evhttp_remove_header(evhttp_request_get_input_headers(ev_req), HttpHeaders::EXPECT);
        return 0;
    }

    // If request body would be big(greater than 1GB),
    // it is better that request_will_be_read_progressively is set true,
    // this can make body read in chunk, not in total
    if (handler->request_will_be_read_progressively()) {
        evhttp_request_set_chunked_cb(ev_req, on_chunked);
    }

    evhttp_request_set_on_free_cb(ev_req, on_free, request.release());
    return 0;
}

HttpHandler* EvHttpServer::_find_handler(HttpRequest* req) {
    auto& path = req->raw_path();

    HttpHandler* handler = nullptr;

    std::lock_guard<std::mutex> lock(_handler_lock);
    switch (req->method()) {
    case GET:
        _get_handlers.retrieve(path, &handler, req->params());
        // Static file handler is a fallback handler
        if (handler == nullptr) {
            handler = _static_file_handler;
        }
        break;
    case PUT:
        _put_handlers.retrieve(path, &handler, req->params());
        break;
    case POST:
        _post_handlers.retrieve(path, &handler, req->params());
        break;
    case DELETE:
        _delete_handlers.retrieve(path, &handler, req->params());
        break;
    case HEAD:
        _head_handlers.retrieve(path, &handler, req->params());
        break;
    case OPTIONS:
        _options_handlers.retrieve(path, &handler, req->params());
        break;
    default:
        LOG(WARNING) << "unknown HTTP method, method=" << req->method();
        break;
    }
    return handler;
}

} // namespace doris
