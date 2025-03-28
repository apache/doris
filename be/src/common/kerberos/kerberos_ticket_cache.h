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
#include <vector>

#include "common/kerberos/kerberos_config.h"
#include "common/kerberos/krb5_interface.h"
#include "common/status.h"

namespace doris::kerberos {

// Structure to hold detailed information about a Kerberos ticket cache
struct KerberosTicketInfo {
    std::string principal;         // Client principal
    std::string keytab_path;       // Path to keytab file
    std::string service_principal; // Service principal this credential is for
    std::string cache_path;        // Path of ticket cache file
    std::string hash_code;         // the hash code from config
    int64_t start_time;            // Unix timestamp in seconds
    int64_t expiry_time;           // Unix timestamp in seconds
    int64_t auth_time;             // Unix timestamp in seconds
    long use_count;                // Reference count of the shared_ptr
    long refresh_interval_second;  // Refresh interval second
};

// Class responsible for managing Kerberos ticket cache, including initialization,
// authentication, and periodic ticket refresh
class KerberosTicketCache : public std::enable_shared_from_this<KerberosTicketCache> {
public:
    // Constructor that takes a Kerberos configuration and an optional KRB5 interface implementation
    explicit KerberosTicketCache(
            const KerberosConfig& config, const std::string& root_path,
            std::unique_ptr<Krb5Interface> krb5_interface = Krb5InterfaceFactory::create());

    virtual ~KerberosTicketCache();

    // Prevent copying of ticket cache instances
    KerberosTicketCache(const KerberosTicketCache&) = delete;
    KerberosTicketCache& operator=(const KerberosTicketCache&) = delete;

    // Initialize the ticket cache by setting up the cache path and Kerberos context
    // Logic: Creates cache directory if needed, initializes KRB5 context and principal
    virtual Status initialize();

    // Perform a fresh Kerberos login using the configured principal and keytab
    // Logic: Opens keytab, obtains new credentials, and stores them in the cache
    virtual Status login();

    // Attempt to login using existing cached credentials
    // Logic: Resolves the existing ticket cache without obtaining new credentials
    virtual Status login_with_cache();

    // Write the current credentials to the ticket cache file
    virtual Status write_ticket_cache();

    // Refresh Kerberos tickets if they're close to expiration or if forced
    // Logic: Checks if refresh is needed based on ticket expiration time,
    // performs a new login if necessary
    virtual Status refresh_tickets();

    // Start the background thread for periodic ticket refresh
    // Logic: Creates a thread that periodically checks and refreshes tickets
    virtual void start_periodic_refresh();

    // Stop the background ticket refresh thread
    virtual void stop_periodic_refresh();

    // Getters for configuration and cache path
    virtual const KerberosConfig& get_config() const { return _config; }
    virtual const std::string get_ticket_cache_path() const { return _ticket_cache_path; }

    // For testing purposes
    void set_refresh_thread_sleep_time(std::chrono::milliseconds sleep_time) {
        _refresh_thread_sleep_time = sleep_time;
    }

    // For testing purposes
    void set_ticket_cache_path(const std::string& mock_path) { _ticket_cache_path = mock_path; }

    // Get detailed information about all credentials in the current ticket cache
    virtual std::vector<KerberosTicketInfo> get_ticket_info();

    int64_t get_ticket_lifetime_sec() const { return _ticket_lifetime_sec; }

private:
    // Initialize the ticket cache file path using principal and keytab information
    Status _init_ticket_cache_path();
    // Initialize the Kerberos context and principal
    Status _initialize_context();
    // Clean up Kerberos resources
    void _cleanup_context();

private:
    // Kerberos configuration containing principal, keytab, and refresh settings
    KerberosConfig _config;
    // For testing purposes
    std::string _ccache_root_dir;
    // Path to the ticket cache file
    std::string _ticket_cache_path;
    // Kerberos context handle
    krb5_context _context {nullptr};
    // Credentials cache handle
    krb5_ccache _ccache {nullptr};
    // Principal handle
    krb5_principal _principal {nullptr};
    // Ticket lifetime in second
    int64_t _ticket_lifetime_sec;

    // Thread for periodic ticket refresh
    std::unique_ptr<std::thread> _refresh_thread;
    // Mutex for thread synchronization
    std::mutex _mutex;
    // Flag to control refresh thread execution
    std::atomic<bool> _should_stop_refresh {false};
    // Sleep time between refresh checks (in milliseconds)
    std::chrono::milliseconds _refresh_thread_sleep_time {5000};

    // Interface for KRB5 operations
    std::unique_ptr<Krb5Interface> _krb5_interface;
};

} // namespace doris::kerberos
