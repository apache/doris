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

#include <krb5.h>

#include <atomic>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <thread>

#include "common/kerberos/kerberos_config.h"
#include "common/status.h"

namespace doris::kerberos {

class KerberosTicketCache {
public:
    explicit KerberosTicketCache(const KerberosConfig& config);

    ~KerberosTicketCache();

    KerberosTicketCache(const KerberosTicketCache&) = delete;
    KerberosTicketCache& operator=(const KerberosTicketCache&) = delete;

    Status initialize();
    Status login();
    Status login_with_cache();

    Status write_ticket_cache();
    Status refresh_tickets();

    template <typename Func>
    auto do_as(Func&& func) -> decltype(func()) {
        std::lock_guard<std::mutex> lock(_mutex);

        if (_needs_refresh()) {
            RETURN_IF_ERROR(refresh_tickets());
        }

        return func();
    }

    void start_periodic_refresh();
    void stop_periodic_refresh();

private:
    Status _initialize_context();
    void _cleanup_context();
    Status _check_error(krb5_error_code code, const char* message);
    bool _needs_refresh() const;

    KerberosConfig _config;
    krb5_context _context {nullptr};
    krb5_ccache _ccache {nullptr};
    krb5_principal _principal {nullptr};

    std::unique_ptr<std::thread> _refresh_thread;
    std::mutex _mutex;
    std::atomic<bool> _should_stop_refresh {false};
};

} // namespace doris::kerberos
