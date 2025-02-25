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

#include "common/kerberos/kerberos_ticket_mgr.h"

#include <chrono>
#include <iomanip>
#include <sstream>

#include "common/logging.h"
#include "exec/schema_scanner/schema_scanner_helper.h"
#include "service/backend_options.h"
#include "vec/core/block.h"

namespace doris::kerberos {

KerberosTicketMgr::KerberosTicketMgr(const std::string& root_path) {
    _root_path = root_path;
    _start_cleanup_thread();
}

KerberosTicketMgr::~KerberosTicketMgr() {
    _stop_cleanup_thread();
}

void KerberosTicketMgr::_start_cleanup_thread() {
    _cleanup_thread = std::make_unique<std::thread>(&KerberosTicketMgr::_cleanup_loop, this);
}

void KerberosTicketMgr::_stop_cleanup_thread() {
    if (_cleanup_thread) {
        _should_stop_cleanup_thread = true;
        _cleanup_thread->join();
        _cleanup_thread.reset();
    }
}

void KerberosTicketMgr::_cleanup_loop() {
#ifdef BE_TEST
    static constexpr int64_t CHECK_INTERVAL_SECONDS = 1; // For testing purpose
#else
    static constexpr int64_t CHECK_INTERVAL_SECONDS = 5; // Check stop flag every 5 seconds
#endif
    uint64_t last_cleanup_time = std::time(nullptr);

    while (!_should_stop_cleanup_thread) {
        uint64_t current_time = std::time(nullptr);

        // Only perform cleanup if enough time has passed
        if (current_time - last_cleanup_time >= _cleanup_interval.count()) {
            std::vector<std::string> keys_to_remove;
            {
                std::lock_guard<std::mutex> lock(_mutex);
                for (const auto& entry : _ticket_caches) {
                    // Check if this is the last reference to the ticket cache
                    // use_count() == 1 means it is only referenced in the _ticket_caches map
                    if (entry.second.cache.use_count() == 1) {
                        LOG(INFO) << "Found unused Kerberos ticket cache for principal: "
                                  << entry.second.cache->get_config().get_principal()
                                  << ", keytab: "
                                  << entry.second.cache->get_config().get_keytab_path();
                        keys_to_remove.push_back(entry.first);
                    }
                }

                // Remove entries under lock
                for (const auto& key : keys_to_remove) {
                    LOG(INFO) << "Removing unused Kerberos ticket cache for key: " << key;
                    _ticket_caches.erase(key);
                }
            }

            last_cleanup_time = current_time;
        }

        // Sleep for a short interval to check stop flag more frequently
        std::this_thread::sleep_for(std::chrono::seconds(CHECK_INTERVAL_SECONDS));
    }
}

Status KerberosTicketMgr::get_or_set_ticket_cache(
        const KerberosConfig& config, std::shared_ptr<KerberosTicketCache>* ticket_cache) {
    std::string key = config.get_hash_code();

    std::lock_guard<std::mutex> lock(_mutex);

    // Check if already exists
    auto it = _ticket_caches.find(key);
    if (it != _ticket_caches.end()) {
        *ticket_cache = it->second.cache;
        return Status::OK();
    }

    // Create new ticket cache
    auto new_ticket_cache = _make_new_ticket_cache(config);
    RETURN_IF_ERROR(new_ticket_cache->initialize());
    RETURN_IF_ERROR(new_ticket_cache->login());
    RETURN_IF_ERROR(new_ticket_cache->write_ticket_cache());
    new_ticket_cache->start_periodic_refresh();

    // Insert into _ticket_caches
    KerberosTicketEntry entry {.cache = new_ticket_cache,
                               .last_access_time = std::chrono::steady_clock::now()};
    auto [inserted_it, success] = _ticket_caches.emplace(key, std::move(entry));
    if (!success) {
        return Status::InternalError("Failed to insert ticket cache into map");
    }

    *ticket_cache = new_ticket_cache;
    LOG(INFO) << "create new kerberos ticket cache: "
              << inserted_it->second.cache->get_ticket_cache_path();
    return Status::OK();
}

std::shared_ptr<KerberosTicketCache> KerberosTicketMgr::get_ticket_cache(
        const std::string& principal, const std::string& keytab_path) {
    std::string key = KerberosConfig::get_hash_code(principal, keytab_path);

    std::lock_guard<std::mutex> lock(_mutex);
    auto it = _ticket_caches.find(key);
    if (it != _ticket_caches.end()) {
        return it->second.cache;
    }
    return nullptr;
}

std::shared_ptr<KerberosTicketCache> KerberosTicketMgr::_make_new_ticket_cache(
        const KerberosConfig& config) {
    return std::make_shared<KerberosTicketCache>(config, _root_path);
}

std::vector<KerberosTicketInfo> KerberosTicketMgr::get_krb_ticket_cache_info() {
    std::vector<KerberosTicketInfo> result;
    std::lock_guard<std::mutex> lock(_mutex);

    for (const auto& entry : _ticket_caches) {
        auto cache_info = entry.second.cache->get_ticket_info();
        result.insert(result.end(), cache_info.begin(), cache_info.end());
    }

    return result;
}

void KerberosTicketMgr::get_ticket_cache_info_block(vectorized::Block* block,
                                                    const cctz::time_zone& ctz) {
    TBackend be = BackendOptions::get_local_backend();
    int64_t be_id = be.id;
    std::string be_ip = be.host;
    std::vector<KerberosTicketInfo> infos = get_krb_ticket_cache_info();
    for (auto& info : infos) {
        SchemaScannerHelper::insert_int64_value(0, be_id, block);
        SchemaScannerHelper::insert_string_value(1, be_ip, block);
        SchemaScannerHelper::insert_string_value(2, info.principal, block);
        SchemaScannerHelper::insert_string_value(3, info.keytab_path, block);
        SchemaScannerHelper::insert_string_value(4, info.service_principal, block);
        SchemaScannerHelper::insert_string_value(5, info.cache_path, block);
        SchemaScannerHelper::insert_string_value(6, info.hash_code, block);
        SchemaScannerHelper::insert_datetime_value(7, info.start_time, ctz, block);
        SchemaScannerHelper::insert_datetime_value(8, info.expiry_time, ctz, block);
        SchemaScannerHelper::insert_datetime_value(9, info.auth_time, ctz, block);
        SchemaScannerHelper::insert_int64_value(10, info.use_count, block);
        SchemaScannerHelper::insert_int64_value(11, info.refresh_interval_second, block);
    }
}

} // namespace doris::kerberos
