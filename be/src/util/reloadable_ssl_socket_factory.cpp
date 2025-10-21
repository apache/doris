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

#include "util/reloadable_ssl_socket_factory.h"

#include <glog/logging.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <pthread.h>

#include <chrono>
#include <filesystem>
#include <system_error>
#include <thread>

#include "common/certificate_manager.h"
#include "common/config.h"

namespace doris {

ReloadableSSLSocketFactory::ReloadableSSLSocketFactory(
        apache::thrift::transport::SSLProtocol protocol)
        : TSSLSocketFactory(protocol), protocol_(protocol) {
    // Base class constructor has already created the initial SSL_CTX
    start_cert_monitoring();
}

ReloadableSSLSocketFactory::~ReloadableSSLSocketFactory() {
    stop_cert_monitoring();
}

bool ReloadableSSLSocketFactory::reload_ssl_context() {
    LOG(INFO) << "Reloading SSL context for ReloadableSSLSocketFactory";

    std::lock_guard<std::mutex> lock(ssl_ctx_mutex_);
    bool ok = create_and_swap_ssl_context();
    if (ok) {
        LOG(INFO) << "SSL context reloaded successfully";
    } else {
        LOG(WARNING) << "Failed to reload SSL context";
    }
    return ok;
}

bool ReloadableSSLSocketFactory::create_and_swap_ssl_context() {
    // create new SSL context
    std::shared_ptr<apache::thrift::transport::SSLContext> new_ctx =
            std::make_shared<apache::thrift::transport::SSLContext>(protocol_);

    SSL_CTX* ctx = new_ctx->get();
    if (!ctx) {
        LOG(ERROR) << "Failed to create new SSL_CTX";
        return false;
    }

    std::unique_ptr<X509, decltype(&X509_free)> cert_res(
            CertificateManager::load_cert(config::tls_certificate_path), X509_free);
    if (!cert_res) {
        return false;
    }
    std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)> key_res(
            CertificateManager::load_key(config::tls_private_key_path,
                                         config::tls_private_key_password),
            EVP_PKEY_free);
    if (!key_res) {
        return false;
    }
    // Load new certificate to new SSL_CTX
    if (SSL_CTX_use_certificate(ctx, cert_res.get()) != 1) {
        LOG(ERROR) << "Failed to load certificate: " << ERR_error_string(ERR_get_error(), nullptr);
        return false;
    }

    // Load new private key to new SSL_CTX
    if (SSL_CTX_use_PrivateKey(ctx, key_res.get()) != 1) {
        LOG(ERROR) << "Failed to load private key: " << ERR_error_string(ERR_get_error(), nullptr);
        return false;
    }

    // Verify private key matches certificate
    if (!SSL_CTX_check_private_key(ctx)) {
        LOG(ERROR) << "Private key does not match certificate";
        return false;
    }

    // Set verification mode
    if (config::tls_verify_mode == CertificateManager::verify_fail_if_no_peer_cert) {
        SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT, nullptr);
    } else if (config::tls_verify_mode == CertificateManager::verify_peer) {
        SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER, nullptr);
    } else if (config::tls_verify_mode == CertificateManager::verify_none) {
        SSL_CTX_set_verify(ctx, SSL_VERIFY_NONE, nullptr);
    }

    std::unique_ptr<X509, decltype(&X509_free)> ca_res(
            CertificateManager::load_ca(config::tls_ca_certificate_path), X509_free);
    if (!ca_res) {
        return false;
    }
    // Load CA certificate
    X509_STORE* store = SSL_CTX_get_cert_store(ctx);
    if (store == nullptr) {
        return false;
    }
    if (X509_STORE_add_cert(store, ca_res.get()) != 1) {
        LOG(WARNING) << "Load CA certificate failed";
        return false;
    }

    // Atomically replace SSL context - new connections will use new SSL_CTX, old connections continue using old one
    std::shared_ptr<apache::thrift::transport::SSLContext> old_ctx;
    {
        std::lock_guard<std::mutex> lock(_old_contexts_mutex);
        old_ctx = ctx_;
        ctx_.swap(new_ctx);
        if (old_ctx) {
            _old_contexts.push(old_ctx);
        }
    }

    // Clean up old contexts periodically
    cleanup_old_contexts();

    LOG(INFO) << "SSL context swapped successfully - new connections will use new certificate";
    return true;
}

void ReloadableSSLSocketFactory::start_cert_monitoring() {
    if (config::enable_tls) {
        _cert_monitor_thread = std::thread([this] {
#if defined(__linux__)
            pthread_setname_np(pthread_self(), "thrift_cert_monitor");
#elif defined(__APPLE__)
            pthread_setname_np("thrift_cert_monitor");
#endif
            check_cert_file_changes();
        });
    }
}

void ReloadableSSLSocketFactory::stop_cert_monitoring() {
    _stop_cert_monitor = true;
    if (_cert_monitor_thread.joinable()) {
        _cert_monitor_thread.join();
    }
}

void ReloadableSSLSocketFactory::check_cert_file_changes() {
    auto should_stop = [this]() { return _stop_cert_monitor.load(); };

    CertificateManager::CertFileMonitorState cert_state;
    CertificateManager::CertFileMonitorState key_state;
    CertificateManager::CertFileMonitorState ca_state;

    (void)CertificateManager::check_certificate_file(config::tls_certificate_path, &cert_state,
                                                     "certificate", should_stop);
    (void)CertificateManager::check_certificate_file(config::tls_private_key_path, &key_state,
                                                     "private key", should_stop);
    (void)CertificateManager::check_certificate_file(config::tls_ca_certificate_path, &ca_state,
                                                     "CA certificate", should_stop);

    while (!_stop_cert_monitor.load()) {
        std::this_thread::sleep_for(
                std::chrono::seconds(config::tls_cert_refresh_interval_seconds));

        if (_stop_cert_monitor.load()) {
            break;
        }

        // the check_num is used to decide whether we need to reload certificates
        // if ca change check_num |= 1 << 2
        // if key change check_num |= 1 << 1
        // if cert change check_num |= 1
        // therefore when check_num is odd, we need to reload
        auto prev_cert_state = cert_state;
        auto prev_key_state = key_state;
        auto prev_ca_state = ca_state;
        int check_num = 0;

        if (CertificateManager::check_certificate_file(config::tls_ca_certificate_path, &ca_state,
                                                       "CA certificate", should_stop)) {
            check_num |= 1 << 2;
        }
        if (CertificateManager::check_certificate_file(config::tls_private_key_path, &key_state,
                                                       "private key", should_stop)) {
            check_num |= 1 << 1;
        }
        if (CertificateManager::check_certificate_file(config::tls_certificate_path, &cert_state,
                                                       "certificate", should_stop)) {
            check_num |= 1;
        }

        if ((check_num & 1) == 1) {
            LOG(INFO) << "Certificate files changed, reloading SSL context...";
            if (!reload_ssl_context()) {
                cert_state = prev_cert_state;
                key_state = prev_key_state;
                ca_state = prev_ca_state;
                LOG(WARNING) << "SSL context reload failed, will retry on next check";
            }
        }
    }
}

void ReloadableSSLSocketFactory::cleanup_old_contexts() {
    std::lock_guard<std::mutex> lock(_old_contexts_mutex);
    // Keep only the last 3 contexts to allow old connections to finish gracefully
    while (_old_contexts.size() > 3) {
        _old_contexts.pop();
    }
}

} // namespace doris
