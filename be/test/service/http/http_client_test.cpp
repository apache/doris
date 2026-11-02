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

#include "service/http/http_client.h"

#include <absl/strings/str_split.h>
#include <fcntl.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <netinet/in.h>
#include <openssl/buffer.h>
#include <openssl/pem.h>
#include <openssl/rsa.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>
#include <poll.h>
#include <pthread.h>
#ifdef ADDRESS_SANITIZER
#include <sanitizer/allocator_interface.h>
#endif
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <boost/algorithm/string/predicate.hpp>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstring>
#include <filesystem>
#include <memory>
#include <mutex>
#include <thread>

#include "common/config.h"
#include "gtest/gtest_pred_impl.h"
#include "runtime/exec_env.h"
#include "service/backend_service.h"
#include "service/http/ev_http_server.h"
#include "service/http/http_channel.h"
#include "service/http/http_handler.h"
#include "service/http/http_headers.h"
#include "service/http/http_request.h"
#include "service/http/utils.h"
#include "service/http_service.h"
#include "testutil/http_utils.h"
#include "util/defer_op.h"
#include "util/md5.h"

namespace doris {

// GTest assertion macro expansion inflates the complexity of this two-transfer check.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST(HttpClientMetricsTest, reports_new_and_reused_connections_including_http_errors) {
    class MetricsHandler : public HttpHandler {
    public:
        void handle(HttpRequest* req) override {
            HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR, "error");
        }
    } handler;
    EvHttpServer server {0};
    ASSERT_TRUE(server.register_handler(GET, "/metrics", &handler));
    server.start();
    ASSERT_GT(server.get_real_port(), 0);
    const auto url = "http://127.0.0.1:" + std::to_string(server.get_real_port()) + "/metrics";
    HttpClient client;
    for (int attempt = 0; attempt < 2; ++attempt) {
        ASSERT_TRUE(client.init(url, false).ok());
        client.set_timeout_ms(5000);
        std::string response;
        ASSERT_TRUE(client.execute(&response).ok());
        EXPECT_EQ(client.get_http_status(), 500);
        EXPECT_EQ(response, "error");
        const auto metrics = client.get_transfer_metrics();
        EXPECT_EQ(metrics.new_connections, attempt == 0 ? 1 : 0);
        EXPECT_GE(metrics.dns_time_ns, 0);
        EXPECT_GE(metrics.connect_time_ns, 0);
        EXPECT_EQ(metrics.tls_time_ns, 0);
        EXPECT_GT(metrics.first_byte_time_ns, 0);
        EXPECT_GE(metrics.total_time_ns, metrics.first_byte_time_ns);
        if (attempt == 1) {
            EXPECT_EQ(metrics.connect_time_ns, 0);
        }
    }
}

TEST(HttpClientResetTest, ReleasesCopiedPayloadWithoutDestroyingHandle) {
    HttpClient client;
    client.reset();
    ASSERT_TRUE(client.init("http://127.0.0.1/").ok());
    auto* handle = client._curl;
    const std::string payload(4 * 1024 * 1024, 'x');
    client.set_payload(payload);
    client.set_content_type("application/json");
    client.set_abort_callback([] { return false; });
#ifdef ADDRESS_SANITIZER
    const auto retained_bytes = __sanitizer_get_current_allocated_bytes();
#endif
    client.reset();
    EXPECT_EQ(client._curl, handle);
    EXPECT_EQ(client._header_list, nullptr);
    EXPECT_FALSE(client._abort_callback);
    EXPECT_EQ(client._callback, nullptr);
#ifdef ADDRESS_SANITIZER
    // The original payload remains alive; only libcurl's request copy is released.
    EXPECT_LE(__sanitizer_get_current_allocated_bytes(), retained_bytes - payload.size());
#endif
    ASSERT_TRUE(client.init("http://127.0.0.1/").ok());
    EXPECT_EQ(client._curl, handle);
}

class HttpClientTlsTest : public testing::Test {
protected:
    void SetUp() override {
        {
            std::scoped_lock lock(*config::get_mutable_string_config_lock());
            _original_ca_paths = config::ca_cert_file_paths;
        }
        EVP_PKEY* raw_key = nullptr;
        generate_key(&raw_key);
        ASSERT_FALSE(HasFatalFailure());
        std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)> key(raw_key, EVP_PKEY_free);
        std::unique_ptr<X509, decltype(&X509_free)> certificate(X509_new(), X509_free);
        ASSERT_NE(certificate, nullptr);
        generate_certificate(certificate.get(), key.get());
        ASSERT_FALSE(HasFatalFailure());
        start_server_and_write_ca(certificate.get(), key.get());
        ASSERT_FALSE(HasFatalFailure());
    }

    static void generate_key(EVP_PKEY** key) {
        std::unique_ptr<EVP_PKEY_CTX, decltype(&EVP_PKEY_CTX_free)> key_context(
                EVP_PKEY_CTX_new_id(EVP_PKEY_RSA, nullptr), EVP_PKEY_CTX_free);
        ASSERT_NE(key_context, nullptr);
        ASSERT_EQ(EVP_PKEY_keygen_init(key_context.get()), 1);
        ASSERT_EQ(EVP_PKEY_CTX_set_rsa_keygen_bits(key_context.get(), 2048), 1);
        ASSERT_EQ(EVP_PKEY_keygen(key_context.get(), key), 1);
    }

    static void generate_certificate(X509* certificate, EVP_PKEY* key) {
        ASSERT_EQ(X509_set_version(certificate, 2), 1);
        ASSERT_EQ(ASN1_INTEGER_set(X509_get_serialNumber(certificate), 1), 1);
        ASSERT_NE(X509_gmtime_adj(X509_getm_notBefore(certificate), -60), nullptr);
        ASSERT_NE(X509_gmtime_adj(X509_getm_notAfter(certificate), 3600), nullptr);
        ASSERT_EQ(X509_set_pubkey(certificate, key), 1);
        auto* subject = X509_get_subject_name(certificate);
        ASSERT_EQ(X509_NAME_add_entry_by_txt(subject, "CN", MBSTRING_ASC,
                                             reinterpret_cast<const unsigned char*>("127.0.0.1"),
                                             -1, -1, 0),
                  1);
        ASSERT_EQ(X509_set_issuer_name(certificate, subject), 1);
        add_extension(certificate, NID_basic_constraints, "critical,CA:TRUE");
        add_extension(certificate, NID_subject_alt_name, "IP:127.0.0.1");
        ASSERT_FALSE(HasFatalFailure());
        ASSERT_GT(X509_sign(certificate, key, EVP_sha256()), 0);
    }

    void start_server_and_write_ca(X509* certificate, EVP_PKEY* key) {
        _ssl_context.reset(SSL_CTX_new(TLS_server_method()));
        ASSERT_NE(_ssl_context, nullptr);
        ASSERT_EQ(SSL_CTX_use_certificate(_ssl_context.get(), certificate), 1);
        ASSERT_EQ(SSL_CTX_use_PrivateKey(_ssl_context.get(), key), 1);
        std::unique_ptr<BIO, decltype(&BIO_free)> cert_bio(BIO_new(BIO_s_mem()), BIO_free);
        ASSERT_NE(cert_bio, nullptr);
        ASSERT_EQ(PEM_write_bio_X509(cert_bio.get(), certificate), 1);
        BUF_MEM* cert_pem = nullptr;
        BIO_get_mem_ptr(cert_bio.get(), &cert_pem);
        char ca_path[] = "/tmp/doris-http-client-ca-XXXXXX";
        const int fd = mkstemp(ca_path);
        ASSERT_GE(fd, 0);
        _ca_path = ca_path;
        Defer close_file {[fd] { close(fd); }};
        ASSERT_EQ(write(fd, cert_pem->data, cert_pem->length),
                  static_cast<ssize_t>(cert_pem->length));
        start_listener();
        ASSERT_FALSE(HasFatalFailure());
    }

    void start_listener() {
        _listen_fd = socket(AF_INET, SOCK_STREAM, 0);
        ASSERT_GE(_listen_fd, 0);
        const int flags = fcntl(_listen_fd, F_GETFL);
        ASSERT_GE(flags, 0);
        ASSERT_EQ(fcntl(_listen_fd, F_SETFL, flags | O_NONBLOCK), 0);
        sockaddr_in address {};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        ASSERT_EQ(bind(_listen_fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)), 0);
        ASSERT_EQ(listen(_listen_fd, 8), 0);
        socklen_t address_length = sizeof(address);
        ASSERT_EQ(getsockname(_listen_fd, reinterpret_cast<sockaddr*>(&address), &address_length),
                  0);
        _port = ntohs(address.sin_port);
        _server_thread = std::thread([this] { serve_https(); });
    }

    void serve_https() {
        sigset_t blocked_signals;
        sigemptyset(&blocked_signals);
        sigaddset(&blocked_signals, SIGPIPE);
        ASSERT_EQ(pthread_sigmask(SIG_BLOCK, &blocked_signals, nullptr), 0);
        Defer drain_sigpipe {[&blocked_signals] { drain_pending_sigpipe(blocked_signals); }};
        while (!_stopped) {
            _listener_ready = true;
            // Bound shutdown latency even when no client ever connects.
            pollfd listener {.fd = _listen_fd, .events = POLLIN, .revents = 0};
            const int ready = poll(&listener, 1, 100);
            if (ready == 0 || (ready < 0 && errno == EINTR)) {
                continue;
            }
            if (ready < 0 || _stopped) {
                return;
            }
            const int fd = accept(_listen_fd, nullptr, nullptr);
            if (fd < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
                    continue;
                }
                return;
            }
            {
                std::scoped_lock lock(_connection_mutex);
                _connection_fd = fd;
                if (_stopped) {
                    shutdown(fd, SHUT_RDWR);
                }
            }
            Defer close_connection {[this, fd] {
                std::scoped_lock lock(_connection_mutex);
                _connection_fd = -1;
                close(fd);
            }};
            respond_https(fd);
        }
    }

    static void drain_pending_sigpipe(const sigset_t& blocked_signals) {
        // sigtimedwait() is unavailable on Darwin; consume only pending SIGPIPE.
        sigset_t pending;
        while (sigpending(&pending) == 0 && sigismember(&pending, SIGPIPE) == 1) {
            int signal = 0;
            if (sigwait(&blocked_signals, &signal) != 0) {
                break;
            }
        }
    }

    void respond_https(int fd) {
        // Accepted sockets may inherit O_NONBLOCK on Darwin; TLS uses blocking I/O.
        const int flags = fcntl(fd, F_GETFL);
        ASSERT_GE(flags, 0);
        ASSERT_EQ(fcntl(fd, F_SETFL, flags & ~O_NONBLOCK), 0);
        const timeval timeout {.tv_sec = 5, .tv_usec = 0};
        ASSERT_EQ(setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)), 0);
        ASSERT_EQ(setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)), 0);
        std::unique_ptr<SSL, decltype(&SSL_free)> connection(SSL_new(_ssl_context.get()), SSL_free);
        ASSERT_NE(connection, nullptr);
        ASSERT_EQ(SSL_set_fd(connection.get(), fd), 1);
        if (SSL_accept(connection.get()) != 1) {
            return;
        }
        char request[4096];
        if (SSL_read(connection.get(), request, sizeof(request)) > 0) {
            constexpr std::string_view response =
                    "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK";
            SSL_write(connection.get(), response.data(), response.size());
        }
        SSL_shutdown(connection.get());
    }

    void TearDown() override {
        set_ca_paths(_original_ca_paths);
        if (!_ca_path.empty()) {
            EXPECT_EQ(unlink(_ca_path.c_str()), 0);
        }
        stop_server();
    }

    void stop_server() {
        _stopped = true;
        {
            std::scoped_lock lock(_connection_mutex);
            if (_connection_fd >= 0) {
                shutdown(_connection_fd, SHUT_RDWR);
            }
        }
        if (_listen_fd >= 0) {
            if (_server_thread.joinable()) {
                _server_thread.join();
            }
            close(_listen_fd);
            _listen_fd = -1;
        }
    }

    static void add_extension(X509* certificate, int nid, const char* value) {
        std::unique_ptr<X509_EXTENSION, decltype(&X509_EXTENSION_free)> extension(
                X509V3_EXT_conf_nid(nullptr, nullptr, nid, const_cast<char*>(value)),
                X509_EXTENSION_free);
        ASSERT_NE(extension, nullptr);
        ASSERT_EQ(X509_add_ext(certificate, extension.get(), -1), 1);
    }

    static void set_ca_paths(const std::string& paths) {
        std::scoped_lock lock(*config::get_mutable_string_config_lock());
        config::ca_cert_file_paths = paths;
    }

    std::string https_url(const std::string& host = "127.0.0.1") const {
        return "https://" + host + ":" + std::to_string(_port) + "/health";
    }

    std::unique_ptr<SSL_CTX, decltype(&SSL_CTX_free)> _ssl_context {nullptr, SSL_CTX_free};
    int _listen_fd = -1;
    uint16_t _port = 0;
    std::atomic<bool> _stopped {false};
    std::atomic<bool> _listener_ready {false};
    std::mutex _connection_mutex;
    int _connection_fd = -1;
    std::thread _server_thread;
    std::string _ca_path;
    std::string _original_ca_paths;
};

TEST_F(HttpClientTlsTest, tls_listener_stops_without_connections) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
    while (!_listener_ready && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::yield();
    }
    ASSERT_TRUE(_listener_ready);
    const auto started = std::chrono::steady_clock::now();
    stop_server();
    EXPECT_LT(std::chrono::steady_clock::now() - started, std::chrono::seconds(1));
    EXPECT_FALSE(_server_thread.joinable());
}

TEST_F(HttpClientTlsTest, tls_sigpipe_drain_consumes_only_pending_signals) {
    std::thread worker([] {
        sigset_t blocked_signals;
        sigemptyset(&blocked_signals);
        sigaddset(&blocked_signals, SIGPIPE);
        ASSERT_EQ(pthread_sigmask(SIG_BLOCK, &blocked_signals, nullptr), 0);
        ASSERT_EQ(pthread_kill(pthread_self(), SIGPIPE), 0);
        sigset_t pending;
        ASSERT_EQ(sigpending(&pending), 0);
        ASSERT_EQ(sigismember(&pending, SIGPIPE), 1);
        drain_pending_sigpipe(blocked_signals);
        ASSERT_EQ(sigpending(&pending), 0);
        EXPECT_EQ(sigismember(&pending, SIGPIPE), 0);
        drain_pending_sigpipe(blocked_signals);
    });
    worker.join();
}

TEST_F(HttpClientTlsTest, tls_configured_ca_survives_reinit) {
    set_ca_paths(_ca_path + ".missing;/tmp;" + _ca_path);
    HttpClient client;
    for (int request = 0; request < 2; ++request) {
        ASSERT_TRUE(client.init(https_url()).ok());
        client.set_timeout_ms(5000);
        std::string response;
        auto status = client.execute(&response);
        ASSERT_TRUE(status.ok()) << status;
        EXPECT_EQ(client.get_http_status(), 200);
        EXPECT_FALSE(response.empty());
    }
}

TEST_F(HttpClientTlsTest, reports_tls_transfer_metrics) {
    set_ca_paths(_ca_path);
    HttpClient client;
    ASSERT_TRUE(client.init(https_url()).ok());
    client.set_timeout_ms(5000);
    ASSERT_TRUE(client.execute().ok());
    const auto metrics = client.get_transfer_metrics();
    EXPECT_EQ(metrics.new_connections, 1);
    EXPECT_GE(metrics.dns_time_ns, 0);
    EXPECT_GE(metrics.connect_time_ns, 0);
    EXPECT_GT(metrics.tls_time_ns, 0);
    EXPECT_GE(metrics.first_byte_time_ns,
              metrics.dns_time_ns + metrics.connect_time_ns + metrics.tls_time_ns);
    EXPECT_GE(metrics.total_time_ns, metrics.first_byte_time_ns);
}

TEST_F(HttpClientTlsTest, tls_ca_path_probe_is_cached_and_configuration_invalidates_it) {
    const auto paths = _ca_path + ".missing;/tmp;" + _ca_path;
    EXPECT_EQ(HttpClient::_get_ca_certificate_path(paths), _ca_path);

    const auto moved_path = _ca_path + ".moved";
    ASSERT_EQ(rename(_ca_path.c_str(), moved_path.c_str()), 0);
    Defer restore_ca([&] { EXPECT_EQ(rename(moved_path.c_str(), _ca_path.c_str()), 0); });
    // An unchanged configuration reuses the selected path without filesystem probes.
    EXPECT_EQ(HttpClient::_get_ca_certificate_path(paths), _ca_path);
    EXPECT_TRUE(HttpClient::_get_ca_certificate_path(paths + ";").empty());
}

TEST_F(HttpClientTlsTest, tls_ca_configuration_updates_on_reused_client) {
    HttpClient client;
    set_ca_paths(_ca_path);
    ASSERT_TRUE(client.init(https_url()).ok());
    client.set_timeout_ms(5000);
    ASSERT_TRUE(client.execute().ok());

    set_ca_paths(_ca_path + ".missing");
    ASSERT_TRUE(client.init(https_url()).ok());
    client.set_timeout_ms(5000);
    EXPECT_FALSE(client.execute().ok());

    set_ca_paths(_ca_path);
    ASSERT_TRUE(client.init(https_url()).ok());
    client.set_timeout_ms(5000);
    EXPECT_TRUE(client.execute().ok());
}

TEST_F(HttpClientTlsTest, tls_configured_ca_follows_http_redirect) {
    class RedirectHandler : public HttpHandler {
    public:
        explicit RedirectHandler(std::string location) : _location(std::move(location)) {}
        void handle(HttpRequest* req) override {
            req->add_output_header("Location", _location.c_str());
            HttpChannel::send_reply(req, HttpStatus::FOUND, "");
        }

    private:
        std::string _location;
    } handler(https_url());
    EvHttpServer server {0};
    ASSERT_TRUE(server.register_handler(GET, "/redirect", &handler));
    server.start();
    ASSERT_GT(server.get_real_port(), 0);
    set_ca_paths(_ca_path);
    HttpClient client;
    ASSERT_TRUE(
            client.init("http://127.0.0.1:" + std::to_string(server.get_real_port()) + "/redirect")
                    .ok());
    client.set_timeout_ms(5000);
    auto status = client.execute();
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(client.get_http_status(), 200);
    ASSERT_TRUE(client.init(https_url()).ok());
    client.set_timeout_ms(5000);
    status = client.execute();
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(client.get_http_status(), 200);
}

TEST_F(HttpClientTlsTest, tls_rejects_untrusted_certificate) {
    for (const auto& paths : {std::string(), _ca_path + ".missing"}) {
        set_ca_paths(paths);
        HttpClient client;
        ASSERT_TRUE(client.init(https_url()).ok());
        client.set_timeout_ms(5000);
        EXPECT_FALSE(client.execute().ok());
        const auto code = client.get_last_curl_code();
        EXPECT_TRUE(code == CURLE_PEER_FAILED_VERIFICATION || code == CURLE_SSL_CACERT_BADFILE)
                << code;
    }
}

TEST_F(HttpClientTlsTest, tls_rejects_hostname_mismatch) {
    set_ca_paths(_ca_path);
    HttpClient client;
    ASSERT_TRUE(client.init(https_url("localhost")).ok());
    client.set_timeout_ms(5000);
    EXPECT_FALSE(client.execute().ok());
    EXPECT_EQ(client.get_last_curl_code(), CURLE_PEER_FAILED_VERIFICATION);
}

class HttpClientTestSimpleGetHandler : public HttpHandler {
public:
    void handle(HttpRequest* req) override {
        std::string user;
        std::string passwd;
        if (!parse_basic_auth(*req, &user, &passwd) || user != "test1") {
            HttpChannel::send_basic_challenge(req, "abc");
            return;
        }
        req->add_output_header(HttpHeaders::CONTENT_TYPE, "text/plain; version=0.0.4");
        bool is_acquire_md5 = !req->param("acquire_md5").empty();
        if (req->method() == HttpMethod::HEAD) {
            req->add_output_header(HttpHeaders::CONTENT_LENGTH, std::to_string(5).c_str());
            if (is_acquire_md5) {
                Md5Digest md5;
                md5.update("md5sum", 6);
                md5.digest();
                req->add_output_header(HttpHeaders::CONTENT_MD5, md5.hex().c_str());
            }
            HttpChannel::send_reply(req);
        } else {
            std::string response = "test1";
            HttpChannel::send_reply(req, response);
        }
    }
};

class HttpClientTestSimplePostHandler : public HttpHandler {
public:
    void handle(HttpRequest* req) override {
        std::string user;
        std::string passwd;
        if (!parse_basic_auth(*req, &user, &passwd) || user != "test1") {
            HttpChannel::send_basic_challenge(req, "abc");
            return;
        }
        if (req->method() == HttpMethod::POST) {
            std::string post_body = req->get_request_body();
            if (!post_body.empty()) {
                HttpChannel::send_reply(req, post_body);
            } else {
                HttpChannel::send_reply(req, "empty");
            }
        }
    }
};

class HttpNotFoundHandler : public HttpHandler {
public:
    void handle(HttpRequest* req) override {
        HttpChannel::send_reply(req, HttpStatus::NOT_FOUND, "file not exist.");
    }
};

class HttpDownloadFileHandler : public HttpHandler {
public:
    void handle(HttpRequest* req) override {
        do_file_response("/proc/self/exe", req, nullptr, true);
    }
};

class HttpBatchDownloadFileHandler : public HttpHandler {
public:
    void handle(HttpRequest* req) override {
        if (req->param("check") == "true") {
            HttpChannel::send_reply(req, "OK");
        } else if (req->param("list") == "true") {
            do_dir_response(req->param("dir"), req, true);
        } else {
            std::vector<std::string> acquire_files =
                    absl::StrSplit(req->get_request_body(), "\n", absl::SkipWhitespace());
            HttpChannel::send_files(req, req->param("dir"), acquire_files);
        }
    }
};

static EvHttpServer* s_server = nullptr;
static int real_port = 0;
static std::string hostname = "";
static std::string address = "";

static HttpClientTestSimpleGetHandler s_simple_get_handler;
static HttpClientTestSimplePostHandler s_simple_post_handler;
static HttpNotFoundHandler s_not_found_handler;
static HttpDownloadFileHandler s_download_file_handler;
static HttpBatchDownloadFileHandler s_batch_download_file_handler;

class HttpClientTest : public testing::Test {
public:
    HttpClientTest() {}
    ~HttpClientTest() override {}

    static void prepare_batch_download_files(const std::string& remote_dir) {
        std::string large_file = remote_dir + "/a_large_file";
        int fd = open(large_file.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
        ASSERT_TRUE(fd >= 0);
        std::string buf = "0123456789";
        for (int i = 0; i < 10; i++) {
            buf += buf;
        }
        for (int i = 0; i < 1024; i++) {
            ASSERT_TRUE(write(fd, buf.c_str(), buf.size()) > 0);
        }
        close(fd);

        // create some small files.
        for (int i = 0; i < 32; i++) {
            std::string small_file = remote_dir + "/small_file_" + std::to_string(i);
            fd = open(small_file.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
            ASSERT_TRUE(fd >= 0);
            ASSERT_TRUE(write(fd, buf.c_str(), buf.size()) > 0);
            close(fd);
        }

        // create a empty file
        std::string empty_file = remote_dir + "/empty_file";
        fd = open(empty_file.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
        ASSERT_TRUE(fd >= 0);
        close(fd);

        empty_file = remote_dir + "/zzzz";
        fd = open(empty_file.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
        ASSERT_TRUE(fd >= 0);
        close(fd);
    }

    static void SetUpTestCase() {
        s_server = new EvHttpServer(0);
        s_server->register_handler(GET, "/simple_get", &s_simple_get_handler);
        s_server->register_handler(HEAD, "/simple_get", &s_simple_get_handler);
        s_server->register_handler(POST, "/simple_post", &s_simple_post_handler);
        s_server->register_handler(GET, "/not_found", &s_not_found_handler);
        s_server->register_handler(HEAD, "/download_file", &s_download_file_handler);
        s_server->register_handler(HEAD, "/api/_tablet/_batch_download",
                                   &s_batch_download_file_handler);
        s_server->register_handler(GET, "/api/_tablet/_batch_download",
                                   &s_batch_download_file_handler);
        s_server->register_handler(POST, "/api/_tablet/_batch_download",
                                   &s_batch_download_file_handler);
        static_cast<void>(s_server->start());
        real_port = s_server->get_real_port();
        EXPECT_NE(0, real_port);
        address = "127.0.0.1:" + std::to_string(real_port);
        hostname = "http://" + address;
    }

    static void TearDownTestCase() { delete s_server; }
};

TEST_F(HttpClientTest, get_normal) {
    HttpClient client;
    auto st = client.init(hostname + "/simple_get");
    EXPECT_TRUE(st.ok());
    client.set_method(GET);
    client.set_basic_auth("test1", "");
    std::string response;
    st = client.execute(&response);
    EXPECT_TRUE(st.ok()) << st;
    EXPECT_STREQ("test1", response.c_str());

    // for head
    st = client.init(hostname + "/simple_get");
    EXPECT_TRUE(st.ok());
    client.set_method(HEAD);
    client.set_basic_auth("test1", "");
    st = client.execute();
    EXPECT_TRUE(st.ok());
    uint64_t len = 0;
    st = client.get_content_length(&len);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(5, len);
}

TEST_F(HttpClientTest, download) {
    HttpClient client;
    auto st = client.init(hostname + "/simple_get");
    EXPECT_TRUE(st.ok());
    client.set_basic_auth("test1", "");
    std::string local_file = ".http_client_test.dat";
    st = client.download(local_file);
    EXPECT_TRUE(st.ok());
    char buf[50];
    auto fp = fopen(local_file.c_str(), "r");
    auto size = fread(buf, 1, 50, fp);
    buf[size] = 0;
    EXPECT_STREQ("test1", buf);
    unlink(local_file.c_str());
}

TEST_F(HttpClientTest, get_failed) {
    HttpClient client;
    auto st = client.init(hostname + "/simple_get");
    EXPECT_TRUE(st.ok());
    client.set_method(GET);
    std::string response;
    st = client.execute(&response);
    EXPECT_FALSE(st.ok());
}

TEST_F(HttpClientTest, post_normal) {
    HttpClient client;
    auto st = client.init(hostname + "/simple_post");
    EXPECT_TRUE(st.ok());
    client.set_method(POST);
    client.set_basic_auth("test1", "");
    std::string response;
    std::string request_body = "simple post body query";
    st = client.execute_post_request(request_body, &response);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(response.length(), request_body.length());
    EXPECT_STREQ(response.c_str(), request_body.c_str());
}

TEST_F(HttpClientTest, post_failed) {
    HttpClient client;
    auto st = client.init(hostname + "/simple_pos");
    EXPECT_TRUE(st.ok());
    client.set_method(POST);
    client.set_basic_auth("test1", "");
    std::string response;
    std::string request_body = "simple post body query";
    st = client.execute_post_request(request_body, &response);
    EXPECT_FALSE(st.ok());
    std::string not_found = "404";
    EXPECT_TRUE(boost::algorithm::contains(st.to_string(), not_found));
}

TEST_F(HttpClientTest, not_found) {
    HttpClient client;
    std::string url = hostname + "/not_found";
    constexpr uint64_t kMaxTimeoutMs = 1000;

    auto get_cb = [&url](HttpClient* client) {
        std::string resp;
        RETURN_IF_ERROR(client->init(url));
        client->set_timeout_ms(kMaxTimeoutMs);
        return client->execute(&resp);
    };

    auto status = HttpClient::execute_with_retry(3, 1, get_cb);
    // libcurl is configured by CURLOPT_FAILONERROR
    EXPECT_FALSE(status.ok());
}

TEST_F(HttpClientTest, header_content_md5) {
    std::string url = hostname + "/simple_get";

    {
        // without md5
        HttpClient client;
        auto st = client.init(url);
        EXPECT_TRUE(st.ok());
        client.set_method(HEAD);
        client.set_basic_auth("test1", "");
        st = client.execute();
        EXPECT_TRUE(st.ok());
        uint64_t len = 0;
        st = client.get_content_length(&len);
        EXPECT_TRUE(st.ok());
        EXPECT_EQ(5, len);
        std::string md5;
        st = client.get_content_md5(&md5);
        EXPECT_TRUE(st.ok());
        EXPECT_TRUE(md5.empty());
    }

    {
        // with md5
        HttpClient client;
        auto st = client.init(url + "?acquire_md5=true");
        EXPECT_TRUE(st.ok());
        client.set_method(HEAD);
        client.set_basic_auth("test1", "");
        st = client.execute();
        EXPECT_TRUE(st.ok());
        uint64_t len = 0;
        st = client.get_content_length(&len);
        EXPECT_TRUE(st.ok());
        EXPECT_EQ(5, len);
        std::string md5_value;
        st = client.get_content_md5(&md5_value);
        EXPECT_TRUE(st.ok());

        Md5Digest md5;
        md5.update("md5sum", 6);
        md5.digest();
        EXPECT_EQ(md5_value, md5.hex());
    }
}

TEST_F(HttpClientTest, content_md5_uses_final_response_headers) {
    class RedirectHandler : public HttpHandler {
    public:
        void handle(HttpRequest* req) override {
            if (req->raw_path() == "/redirect") {
                req->add_output_header("Location", "/final");
                req->add_output_header(HttpHeaders::CONTENT_MD5, "redirect-md5");
                HttpChannel::send_reply(req, HttpStatus::FOUND, "");
            } else {
                req->add_output_header(HttpHeaders::CONTENT_MD5, "final-md5");
                HttpChannel::send_reply(req, "OK");
            }
        }
    } handler;
    EvHttpServer server {0};
    ASSERT_TRUE(server.register_handler(GET, "/redirect", &handler));
    ASSERT_TRUE(server.register_handler(GET, "/final", &handler));
    server.start();
    HttpClient client;
    ASSERT_TRUE(
            client.init("http://127.0.0.1:" + std::to_string(server.get_real_port()) + "/redirect")
                    .ok());
    ASSERT_TRUE(client.execute().ok());
    std::string header;
    ASSERT_TRUE(client.get_response_header(HttpHeaders::CONTENT_MD5, &header).ok());
    EXPECT_EQ(header, "final-md5");
    std::string md5;
    ASSERT_TRUE(client.get_content_md5(&md5).ok());
    EXPECT_EQ(md5, header);
    header = "previous value";
    ASSERT_TRUE(client.get_response_header("X-Missing-Header", &header).ok());
    EXPECT_TRUE(header.empty());
}

TEST_F(HttpClientTest, download_file_md5) {
    std::string url = hostname + "/download_file";
    HttpClient client;
    auto st = client.init(url);
    EXPECT_TRUE(st.ok());
    client.set_method(HEAD);
    client.set_basic_auth("test1", "");
    st = client.execute();
    EXPECT_TRUE(st.ok());

    std::string md5_value;
    st = client.get_content_md5(&md5_value);
    EXPECT_TRUE(st.ok());

    int fd = open("/proc/self/exe", O_RDONLY);
    ASSERT_TRUE(fd >= 0);
    struct stat stat;
    ASSERT_TRUE(fstat(fd, &stat) >= 0);

    int64_t file_size = stat.st_size;
    Md5Digest md5;
    void* buf = mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
    md5.update(buf, file_size);
    md5.digest();
    munmap(buf, file_size);

    EXPECT_EQ(md5_value, md5.hex());
    close(fd);
}

TEST_F(HttpClientTest, escape_url) {
    HttpClient client;
    client._curl = curl_easy_init();
    auto check_result = [&client](const auto& input_url, const auto& output_url) -> bool {
        std::string escaped_url;
        if (!client._escape_url(input_url, &escaped_url).ok()) {
            return false;
        }
        if (escaped_url != output_url) {
            return false;
        }
        return true;
    };
    std::string input_A = hostname + "/download_file?token=oxof&file_name=02x_0.dat";
    std::string output_A = hostname + "/download_file?token=oxof&file_name=02x_0.dat";
    ASSERT_TRUE(check_result(input_A, output_A));

    std::string input_B = hostname + "/download_file?";
    std::string output_B = hostname + "/download_file?";
    ASSERT_TRUE(check_result(input_B, output_B));

    std::string input_C = hostname + "/download_file";
    std::string output_C = hostname + "/download_file";
    ASSERT_TRUE(check_result(input_C, output_C));

    std::string input_D = hostname + "/download_file?&";
    std::string output_D = hostname + "/download_file?&";
    ASSERT_TRUE(check_result(input_D, output_D));

    std::string input_E = hostname + "/download_file?key=0x2E";
    std::string output_E = hostname + "/download_file?key=0x2E";
    ASSERT_TRUE(check_result(input_E, output_E));

    std::string input_F = hostname + "/download_file?key=0x2E&key=%";
    std::string output_F = hostname + "/download_file?key=0x2E&key=%25";
    ASSERT_TRUE(check_result(input_F, output_F));

    std::string input_G = hostname + "/download_file?key=0x2E&key=%2E#section";
    std::string output_G = hostname + "/download_file?key=0x2E&key=%252E#section";
    ASSERT_TRUE(check_result(input_G, output_G));
}

TEST_F(HttpClientTest, enable_http_auth) {
    std::string origin_hostname = hostname;
    Defer defer {[&origin_hostname]() {
        hostname = origin_hostname;
        config::enable_all_http_auth = false;
    }};

    hostname = doris::global_test_http_host;

    {
        config::enable_all_http_auth = false;
        std::string url = hostname + "/api/health";
        HttpClient client;
        auto st = client.init(url);
        EXPECT_TRUE(st.ok());
        client.set_method(GET);
        std::string response;
        st = client.execute(&response);
        EXPECT_TRUE(st.ok());
        std::cout << "response = " << response << "\n";
        EXPECT_TRUE(response.find("Server is not ready") != std::string::npos);
    }

    {
        config::enable_all_http_auth = true;
        std::string url = hostname + "/api/health";
        HttpClient client;
        auto st = client.init(url);
        EXPECT_TRUE(st.ok());
        client.set_method(GET);
        std::string response;
        st = client.execute(&response);
        std::cout << "st = " << st << "\n";
        std::cout << "response = " << response << "\n";
        std::cout << "st.msg() = " << st.msg() << "\n";
        EXPECT_TRUE(!st.ok()) << st;
        EXPECT_TRUE(st.msg().find("The requested URL returned error") != std::string::npos);
    }

    {
        config::enable_all_http_auth = true;
        std::string url = hostname + "/api/health";
        HttpClient client;
        auto st = client.init(url);
        EXPECT_TRUE(st.ok());
        client.set_method(GET);
        client.set_basic_auth("root", "");
        std::string response;
        st = client.execute(&response);
        EXPECT_TRUE(st.ok());
        std::cout << "response = " << response << "\n";
        EXPECT_TRUE(response.find("Server is not ready") != std::string::npos);
    }

    {
        config::enable_all_http_auth = true;
        std::string url = hostname + "/api/health";
        HttpClient client;
        auto st = client.init(url);
        EXPECT_TRUE(st.ok());
        client.set_method(GET);
        client.set_basic_auth("root", "errorpasswd");
        client.set_timeout_ms(200);
        std::string response;
        st = client.execute(&response);
        EXPECT_TRUE(!st.ok());
        std::cout << "response = " << response << "\n";
        std::cout << "st.msg() = " << st.msg() << "\n";
        EXPECT_TRUE(st.msg().find("403") != std::string::npos);
    }

    {
        config::enable_all_http_auth = false;
        std::string url = hostname + "/metrics";
        HttpClient client;
        auto st = client.init(url);
        EXPECT_TRUE(st.ok());
        client.set_method(GET);
        std::string response;
        st = client.execute(&response);
        EXPECT_TRUE(st.ok());
        std::cout << "response = " << response << "\n";
        EXPECT_TRUE(response.size() != 0);
    }

    {
        config::enable_all_http_auth = true;
        std::string url = hostname + "/metrics";
        HttpClient client;
        auto st = client.init(url);
        EXPECT_TRUE(st.ok());
        client.set_method(GET);
        std::string response;
        st = client.execute(&response);
        std::cout << "st = " << st << "\n";
        std::cout << "response = " << response << "\n";
        std::cout << "st.msg() = " << st.msg() << "\n";
        EXPECT_TRUE(!st.ok());
        EXPECT_TRUE(st.msg().find("The requested URL returned error") != std::string::npos);
    }
    {
        config::enable_all_http_auth = true;
        std::string url = hostname + "/metrics";
        HttpClient client;
        auto st = client.init(url);
        EXPECT_TRUE(st.ok());
        client.set_method(GET);
        client.set_basic_auth("root", "");
        std::string response;
        st = client.execute(&response);
        EXPECT_TRUE(st.ok());
        std::cout << "response = " << response << "\n";
        EXPECT_TRUE(response.size() != 0);
    }

    {
        config::enable_all_http_auth = true;
        std::string url = hostname + "/metrics";
        HttpClient client;
        auto st = client.init(url);
        EXPECT_TRUE(st.ok());
        client.set_method(GET);
        client.set_basic_auth("root", "errorpasswd");
        client.set_timeout_ms(200);
        std::string response;
        st = client.execute(&response);
        EXPECT_TRUE(!st.ok());
        std::cout << "response = " << response << "\n";
        std::cout << "st.msg() = " << st.msg() << "\n";
        EXPECT_TRUE(st.msg().find("403") != std::string::npos);
    }

    // valid token
    {
        config::enable_all_http_auth = true;
        std::string url = hostname + "/metrics";
        HttpClient client;
        auto st = client.init(url);
        EXPECT_TRUE(st.ok());
        client.set_method(GET);
        client.set_auth_token("valid_token");
        client.set_timeout_ms(200);
        std::string response;
        st = client.execute(&response);
        EXPECT_TRUE(st.ok()) << st;
    }

    // invalid token
    {
        config::enable_all_http_auth = true;
        std::string url = hostname + "/metrics";
        HttpClient client;
        auto st = client.init(url);
        EXPECT_TRUE(st.ok());
        client.set_method(GET);
        client.set_auth_token("invalid_token");
        client.set_timeout_ms(200);
        std::string response;
        st = client.execute(&response);
        EXPECT_TRUE(!st.ok()) << st;
    }

    {
        config::enable_all_http_auth = true;
        std::string url = hostname + "/api/glog/adjust";
        HttpClient client;
        auto st = client.init(url);
        EXPECT_TRUE(st.ok());
        client.set_method(POST);
        client.set_basic_auth("rootss", "");
        client.set_timeout_ms(200);
        std::string response;
        st = client.execute_post_request("level=1&module=xxx", &response);
        EXPECT_TRUE(!st.ok());
        std::cout << "response = " << response << "\n";
        std::cout << "st.msg() = " << st.msg() << "\n";
        EXPECT_TRUE(st.msg().find("403") != std::string::npos);
    }

    std::vector<std::string> check_get_list = {"/api/clear_cache/aa",
                                               "/api/running_pipeline_tasks",
                                               "/api/running_pipeline_tasks/223",
                                               "/api/query_pipeline_tasks/78902309190709864",
                                               "/api/be_process_thread_num",
                                               "/api/load_streams",
                                               "/api/show_config",
                                               "/api/shrink_mem"};

    for (auto endpoint : check_get_list) {
        std::cout << "endpint = " << endpoint << "\n";

        {
            config::enable_all_http_auth = true;
            std::string url = hostname + endpoint;
            HttpClient client;
            auto st = client.init(url);
            EXPECT_TRUE(st.ok());
            client.set_method(GET);
            std::string response;
            st = client.execute(&response);
            std::cout << "st = " << st << "\n";
            std::cout << "response = " << response << "\n";
            std::cout << "st.msg() = " << st.msg() << "\n";
            EXPECT_TRUE(!st.ok());
            EXPECT_TRUE(st.msg().find("The requested URL returned error") != std::string::npos);
        }

        {
            config::enable_all_http_auth = true;
            std::string url = hostname + endpoint;
            HttpClient client;
            auto st = client.init(url);
            EXPECT_TRUE(st.ok());
            client.set_method(GET);
            client.set_basic_auth("roxot", "errorpasswd");
            client.set_timeout_ms(200);
            std::string response;
            st = client.execute(&response);
            EXPECT_TRUE(!st.ok());
            std::cout << "response = " << response << "\n";
            std::cout << "st.msg() = " << st.msg() << "\n";
            EXPECT_TRUE(st.msg().find("403") != std::string::npos);
        }
    }
}

TEST_F(HttpClientTest, batch_download) {
    char temporary_directory[] = "/tmp/doris-http-batch-XXXXXX";
    ASSERT_NE(mkdtemp(temporary_directory), nullptr);
    std::string root_dir(temporary_directory);
    Defer cleanup([&root_dir] {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(root_dir).ok());
    });
    std::string remote_related_dir = root_dir + "/source";
    std::string local_dir = root_dir + "/target";
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(remote_related_dir).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(local_dir).ok());

    std::string remote_dir;
    EXPECT_TRUE(io::global_local_filesystem()->canonicalize(remote_related_dir, &remote_dir).ok());

    prepare_batch_download_files(remote_dir);
    ASSERT_FALSE(HasFatalFailure());

    // 1. check remote support batch download
    Status st = is_support_batch_download(address);
    EXPECT_TRUE(st.ok());

    // 2. list remote files
    std::vector<std::pair<std::string, size_t>> file_info_list;
    st = list_remote_files_v2(address, "token", remote_dir, &file_info_list);
    EXPECT_TRUE(st.ok());

    // 3. download files
    if (file_info_list.size() > 64) {
        file_info_list.resize(64);
    }

    // sort file info list by file name
    std::sort(file_info_list.begin(), file_info_list.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; });

    st = download_files_v2(address, "token", remote_dir, local_dir, file_info_list);
    EXPECT_TRUE(st.ok());
}

} // namespace doris
