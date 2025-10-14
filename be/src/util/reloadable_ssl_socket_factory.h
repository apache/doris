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

#include <openssl/ssl.h>
#include <thrift/transport/TSSLSocket.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>

namespace doris {

// Thin wrapper around Thrift's TSSLSocketFactory that watches Doris TLS certificate
// files and hot-swaps a freshly built SSL_CTX when any of them change on disk.
class ReloadableSSLSocketFactory : public apache::thrift::transport::TSSLSocketFactory {
public:
    explicit ReloadableSSLSocketFactory(apache::thrift::transport::SSLProtocol protocol =
                                                apache::thrift::transport::SSLProtocol::TLSv1_2);

    ~ReloadableSSLSocketFactory() override;

private:
    bool reload_ssl_context();

    void start_cert_monitoring();
    void stop_cert_monitoring();

    bool create_and_swap_ssl_context();
    void check_cert_file_changes();
    void cleanup_old_contexts();

    std::mutex ssl_ctx_mutex_;
    apache::thrift::transport::SSLProtocol protocol_;

    // Certificate file monitoring
    std::thread _cert_monitor_thread;
    std::atomic<bool> _stop_cert_monitor {false};

    // Keep old SSL contexts for delayed release
    std::queue<std::shared_ptr<apache::thrift::transport::SSLContext>> _old_contexts;
    std::mutex _old_contexts_mutex;
};

} // namespace doris
