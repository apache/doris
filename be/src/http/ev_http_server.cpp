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
#include <event2/bufferevent.h>
#include <event2/bufferevent_ssl.h>
#include <event2/event.h>
#include <event2/http.h>
#include <event2/http_struct.h>
#include <event2/thread.h>
#include <netinet/in.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <memory>
#include <sstream>
#include <thread>

#include "common/certificate_manager.h"
#include "common/config.h"
#include "common/logging.h"
#include "http/http_channel.h"
#include "http/http_handler.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_status.h"
#include "service/backend_options.h"
#include "util/threadpool.h"
// #include <event2/bufferevent_ssl.h>
#include <event2/listener.h>
#include <openssl/err.h>
#include <openssl/ssl.h>

#include <iostream>

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

static void init_ssl_library() {
    signal(SIGPIPE, SIG_IGN);

    SSL_library_init();
    SSL_load_error_strings();
    OpenSSL_add_all_algorithms();
}

/**
 * This callback is responsible for creating a new SSL connection
 * and wrapping it in an OpenSSL bufferevent.  This is the way
 * we implement an https server instead of a plain old http server.
 */
static struct bufferevent* bevcb(struct event_base* base, void* arg) {
    struct bufferevent* r;
    auto* ctx = (SSL_CTX*)arg;

    r = bufferevent_openssl_socket_new(base, -1, SSL_new(ctx), BUFFEREVENT_SSL_ACCEPTING,
                                       BEV_OPT_CLOSE_ON_FREE);
    return r;
}

EvHttpServer::EvHttpServer(int port, int num_workers)
        : _port(port), _num_workers(num_workers), _real_port(0) {
    _host = BackendOptions::get_service_bind_address();

    evthread_use_pthreads();
    DCHECK_GT(_num_workers, 0);
    _event_bases.resize(_num_workers);
    _evhttps.resize(_num_workers);
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

#define CHECK_OPENSSL_ERR(cond, msg)                                                            \
    if (!(cond)) {                                                                              \
        LOG(ERROR) << (msg) << ": " << ERR_error_string(ERR_get_error(), nullptr) << std::endl; \
        return nullptr;                                                                         \
    }

SSL_CTX* EvHttpServer::_create_ssl_context() {
    SSL_CTX* ssl_ctx = SSL_CTX_new(SSLv23_server_method());
    CHECK_OPENSSL_ERR(ssl_ctx, "SSL_CTX_new failed");

    SSL_CTX_set_options(ssl_ctx, SSL_OP_NO_SSLv3 | SSL_OP_NO_TLSv1 | SSL_OP_NO_TLSv1_1);

    std::unique_ptr<X509, decltype(&X509_free)> cert_res(
            CertificateManager::load_cert(config::tls_certificate_path), X509_free);
    if (!cert_res) {
        return nullptr;
    }
    std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)> key_res(
            CertificateManager::load_key(config::tls_private_key_path,
                                         config::tls_private_key_password),
            EVP_PKEY_free);
    if (!key_res) {
        return nullptr;
    }

    CHECK_OPENSSL_ERR(SSL_CTX_use_certificate(ssl_ctx, cert_res.get()) == 1,
                      "Load server cert failed");
    CHECK_OPENSSL_ERR(SSL_CTX_use_PrivateKey(ssl_ctx, key_res.get()) == 1,
                      "Load server key failed");
    CHECK_OPENSSL_ERR(SSL_CTX_check_private_key(ssl_ctx) == 1, "Check server key failed");

    if (config::tls_verify_mode == CertificateManager::verify_fail_if_no_peer_cert) {
        SSL_CTX_set_verify(ssl_ctx, SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT, nullptr);
    } else if (config::tls_verify_mode == CertificateManager::verify_peer) {
        SSL_CTX_set_verify(ssl_ctx, SSL_VERIFY_PEER, nullptr);
    } else if (config::tls_verify_mode == CertificateManager::verify_none) {
        SSL_CTX_set_verify(ssl_ctx, SSL_VERIFY_NONE, nullptr);
    } else {
        SSL_CTX_set_verify(ssl_ctx, SSL_VERIFY_NONE, nullptr);
        LOG(WARNING) << fmt::format(
                "unknown verify_mode: {}, only support: verify_fail_if_no_peer_cert, "
                "verify_peer, verify_none",
                config::tls_verify_mode);
    }

    std::unique_ptr<X509, decltype(&X509_free)> ca_res(
            CertificateManager::load_ca(config::tls_ca_certificate_path), X509_free);
    if (!ca_res) {
        return nullptr;
    }
    X509_STORE* store = SSL_CTX_get_cert_store(ssl_ctx);
    if (store == nullptr) {
        CHECK_OPENSSL_ERR(false, "Load CA certificate failed");
    }
    int add_ret = X509_STORE_add_cert(store, ca_res.get());
    CHECK_OPENSSL_ERR(add_ret == 1, "Load CA certificate failed");

    return ssl_ctx;
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
    if (config::enable_tls) {
        init_ssl_library();
        _ssl_ctx = _create_ssl_context();

        // Start certificate monitoring thread
        _cert_monitor_thread = std::thread([this] {
#if defined(__linux__)
            pthread_setname_np(pthread_self(), "libevent_cert_monitor");
#elif defined(__APPLE__)
            pthread_setname_np("libevent_cert_monitor");
#endif
            _check_cert_file_changes();
        });
    }
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

            // Store the evhttp object for SSL context updates
            {
                std::lock_guard lock(_evhttp_lock);
                _evhttps[i] = http;
            }

            auto res = evhttp_accept_socket(http.get(), _server_fd);
            CHECK(res >= 0) << "evhttp accept socket failed, res=" << res;

            if (config::enable_tls && _ssl_ctx != nullptr) {
                evhttp_set_bevcb(http.get(), bevcb, _ssl_ctx);
            }

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
    }
    {
        std::lock_guard<std::mutex> lock(_evhttp_lock);
        _evhttps.clear();
    }
    _workers->shutdown();
    _event_bases.clear();
    close(_server_fd);
    if (config::enable_tls) {
        _stop_cert_monitor = true;
        if (_cert_monitor_thread.joinable()) {
            _cert_monitor_thread.join();
        }
        SSL_CTX_free(_ssl_ctx);

        // Clean up all old SSL contexts
        {
            std::lock_guard<std::mutex> lock(_old_ssl_contexts_mutex);
            while (!_old_ssl_contexts.empty()) {
                SSL_CTX* old_ctx = _old_ssl_contexts.front();
                _old_ssl_contexts.pop();
                SSL_CTX_free(old_ctx);
            }
        }
    }
    _started = false;
}

void EvHttpServer::join() {}

bool EvHttpServer::_reload_cert() {
    if (!config::enable_tls || !_started) {
        LOG(WARNING) << "SSL not enabled or server not started, cannot reload SSL context";
        return false;
    }

    LOG(INFO) << "Reloading SSL context for EvHttpServer...";

    SSL_CTX* new_ssl_ctx = _create_ssl_context();
    if (new_ssl_ctx == nullptr) {
        LOG(WARNING) << "Failed to create new ssl context";
        return false;
    }
    SSL_CTX* old_ssl_ctx = _ssl_ctx;
    _ssl_ctx = new_ssl_ctx;

    {
        std::lock_guard lock(_evhttp_lock);
        for (int i = 0; i < _num_workers; ++i) {
            if (_evhttps[i]) {
                evhttp_set_bevcb(_evhttps[i].get(), bevcb, new_ssl_ctx);
                LOG(INFO) << "Updated SSL context for worker " << i;
            }
        }
    }

    // Store old SSL context for delayed release
    {
        std::lock_guard<std::mutex> lock(_old_ssl_contexts_mutex);
        if (old_ssl_ctx != nullptr) {
            _old_ssl_contexts.push(old_ssl_ctx);
        }
    }

    // Clean up old contexts periodically
    _cleanup_old_ssl_contexts();

    LOG(INFO) << "SSL context reloaded successfully for EvHttpServer";
    return true;
}

void EvHttpServer::_cleanup_old_ssl_contexts() {
    std::lock_guard<std::mutex> lock(_old_ssl_contexts_mutex);
    // Keep only the last 3 contexts to allow old connections to finish gracefully
    while (_old_ssl_contexts.size() > 3) {
        SSL_CTX* old_ctx = _old_ssl_contexts.front();
        _old_ssl_contexts.pop();
        SSL_CTX_free(old_ctx);
    }
}

void EvHttpServer::_check_cert_file_changes() {
    auto should_stop = [this]() { return _stop_cert_monitor.load(); };

    CertificateManager::CertFileMonitorState cert_state;
    CertificateManager::CertFileMonitorState key_state;
    CertificateManager::CertFileMonitorState ca_state;

    (void)CertificateManager::check_certificate_file(config::tls_certificate_path, &cert_state,
                                                     "certificate file", should_stop);
    (void)CertificateManager::check_certificate_file(config::tls_private_key_path, &key_state,
                                                     "private key file", should_stop);
    (void)CertificateManager::check_certificate_file(config::tls_ca_certificate_path, &ca_state,
                                                     "CA certificate file", should_stop);

    while (!_stop_cert_monitor.load()) {
        std::this_thread::sleep_for(
                std::chrono::seconds(config::tls_cert_refresh_interval_seconds));
        if (_stop_cert_monitor.load()) {
            break;
        }

        // the check_num is use for weather we 'can' do reload
        // if ca change check_num |= 1 << 2
        // if key change check_num |= 1 << 1
        // if cert change check_num |= 1
        // then we can determine the current situation based on the value of check_num
        // if ca file changed, then cert and key also need change, the num is 7
        // if user don't want change key file, only ca and cert change ,the num is 5
        // if key changed, then cert also need change, the num is 3
        // if only cert changed, the num is 1
        // therefore, we can conclude that when check_num is odd, the certificate needs to be reloaded
        // this approach can eliminate some instances of user error (compare bool value)
        // it may also be used for finer-grained control in the future
        auto prev_cert_state = cert_state;
        auto prev_key_state = key_state;
        auto prev_ca_state = ca_state;
        int check_num = 0;

        if (CertificateManager::check_certificate_file(config::tls_certificate_path, &cert_state,
                                                       "certificate file", should_stop)) {
            check_num |= 1;
        }
        if (CertificateManager::check_certificate_file(config::tls_private_key_path, &key_state,
                                                       "private key file", should_stop)) {
            check_num |= 1 << 1;
        }
        if (CertificateManager::check_certificate_file(config::tls_ca_certificate_path, &ca_state,
                                                       "CA certificate file", should_stop)) {
            check_num |= 1 << 2;
        }

        if ((check_num & 1) == 1) {
            LOG(INFO) << "Certificate files changed, reloading SSL context...";
            if (!_reload_cert()) {
                cert_state = prev_cert_state;
                key_state = prev_key_state;
                ca_state = prev_ca_state;
                LOG(WARNING) << "SSL context reload failed, will retry on next check";
            }
        }
    }
}

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
