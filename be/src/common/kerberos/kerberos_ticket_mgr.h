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

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "cctz/time_zone.h"
#include "common/kerberos/kerberos_config.h"
#include "common/kerberos/kerberos_ticket_cache.h"
#include "common/status.h"

namespace doris {

namespace vectorized {
class Block;
}

namespace kerberos {

// Structure to hold a ticket cache instance and its last access time
struct KerberosTicketEntry {
    std::shared_ptr<KerberosTicketCache> cache;
    std::chrono::steady_clock::time_point last_access_time;
};

// Manager class responsible for maintaining multiple Kerberos ticket caches
// and handling their lifecycle, including creation, access, and cleanup
class KerberosTicketMgr {
public:
    // Constructor that takes the expiration time for unused ticket caches
    explicit KerberosTicketMgr(const std::string& root_path);

    // Get or create a ticket cache for the given Kerberos configuration
    // Logic: Checks if cache exists, if not creates new one, initializes it,
    // performs login, and starts periodic refresh
    Status get_or_set_ticket_cache(const KerberosConfig& config,
                                   std::shared_ptr<KerberosTicketCache>* ticket_cache);

    Status remove_ticket_cache(const std::string& principal, const std::string& keytab_path);

    // Get the ticket cache object. This is used by HdfsHandler to hold a reference
    std::shared_ptr<KerberosTicketCache> get_ticket_cache(const std::string& principal,
                                                          const std::string& keytab_path);

    // Get detailed information about all active Kerberos ticket caches
    std::vector<KerberosTicketInfo> get_krb_ticket_cache_info();

    // Set the cleanup interval for testing purpose
    void set_cleanup_interval(std::chrono::seconds interval) { _cleanup_interval = interval; }

    void get_ticket_cache_info_block(vectorized::Block* block, const cctz::time_zone& ctz);

    virtual ~KerberosTicketMgr();

protected:
    // Prevent copying of ticket manager instances
    KerberosTicketMgr(const KerberosTicketMgr&) = delete;
    KerberosTicketMgr& operator=(const KerberosTicketMgr&) = delete;

    // Factory method to create new ticket cache instances
    // Can be overridden in tests to provide mock implementations
    virtual std::shared_ptr<KerberosTicketCache> _make_new_ticket_cache(
            const KerberosConfig& config);

    // Start the cleanup thread
    void _start_cleanup_thread();
    // Stop the cleanup thread
    void _stop_cleanup_thread();
    // Cleanup thread function
    void _cleanup_loop();

protected:
    // The root dir of ticket caches
    std::string _root_path;
    // Map storing ticket caches, keyed by MD5 hash of principal and keytab path
    std::unordered_map<std::string, KerberosTicketEntry> _ticket_caches;
    // Mutex for thread-safe access to the ticket cache map
    std::mutex _mutex;

    // Cleanup thread related members
    std::atomic<bool> _should_stop_cleanup_thread {false};
    std::unique_ptr<std::thread> _cleanup_thread;
    std::chrono::seconds _cleanup_interval {3600}; // Default to 1 hour
};

} // namespace kerberos
} // namespace doris
