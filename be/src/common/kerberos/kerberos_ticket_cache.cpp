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

#include "common/kerberos/kerberos_ticket_cache.h"

#include <chrono>
#include <filesystem>
#include <sstream>
#include <thread>

#include "common/config.h"
#include "util/time.h"

namespace doris::kerberos {

KerberosTicketCache::KerberosTicketCache(const KerberosConfig& config, const std::string& root_path,
                                         std::unique_ptr<Krb5Interface> krb5_interface)
        : _config(config),
          _ccache_root_dir(root_path),
          _krb5_interface(std::move(krb5_interface)) {}

KerberosTicketCache::~KerberosTicketCache() {
    stop_periodic_refresh();
    _cleanup_context();
    if (std::filesystem::exists(_ticket_cache_path)) {
        std::filesystem::remove(_ticket_cache_path);
    }
    LOG(INFO) << "destroy kerberos ticket cache " << _ticket_cache_path
              << " with principal: " << _config.get_principal();
}

Status KerberosTicketCache::initialize() {
    std::lock_guard<std::mutex> lock(_mutex);
    RETURN_IF_ERROR(_init_ticket_cache_path());
    Status st = _initialize_context();
    LOG(INFO) << "initialized kerberos ticket cache " << _ticket_cache_path
              << " with principal: " << _config.get_principal()
              << " and keytab: " << _config.get_keytab_path() << ": " << st.to_string();
    return st;
}

Status KerberosTicketCache::_init_ticket_cache_path() {
    std::string cache_file_md5 = "doris_krb_" + _config.get_hash_code();

    // The path should be with prefix "_ccache_root_dir"
    std::filesystem::path full_path = std::filesystem::path(_ccache_root_dir) / cache_file_md5;
    full_path = std::filesystem::weakly_canonical(full_path);
    std::filesystem::path parent_path = full_path.parent_path();

    LOG(INFO) << "try creating kerberos ticket path: " << full_path.string()
              << " for principal: " << _config.get_principal();
    try {
        if (!std::filesystem::exists(parent_path)) {
            // Create the parent dir if not exists
            std::filesystem::create_directories(parent_path);
        } else {
            // Delete the ticket cache file if exists
            if (std::filesystem::exists(full_path)) {
                std::filesystem::remove(full_path);
            }
        }

        _ticket_cache_path = full_path.string();
        return Status::OK();
    } catch (const std::filesystem::filesystem_error& e) {
        return Status::InternalError("Error when setting kerberos ticket cache file: {}, {}",
                                     full_path.native(), e.what());
    } catch (const std::exception& e) {
        return Status::InternalError("Exception when setting kerberos ticket cache file: {}, {}",
                                     full_path.native(), e.what());
    } catch (...) {
        return Status::InternalError("Unknown error when setting kerberos ticket cache file: {}",
                                     full_path.native());
    }
}

Status KerberosTicketCache::login() {
    std::lock_guard<std::mutex> lock(_mutex);

    krb5_keytab keytab = nullptr;
    krb5_ccache temp_ccache = nullptr;
    try {
        // Open the keytab file
        RETURN_IF_ERROR(
                _krb5_interface->kt_resolve(_context, _config.get_keytab_path().c_str(), &keytab));

        // init ccache
        RETURN_IF_ERROR(
                _krb5_interface->cc_resolve(_context, _ticket_cache_path.c_str(), &temp_ccache));

        // get init creds
        krb5_get_init_creds_opt* opts = nullptr;
        RETURN_IF_ERROR(_krb5_interface->get_init_creds_opt_alloc(_context, &opts));

        krb5_creds creds;
        Status status = _krb5_interface->get_init_creds_keytab(_context, &creds, _principal, keytab,
                                                               0,       // start time
                                                               nullptr, // TKT service name
                                                               opts);

        if (!status.ok()) {
            _krb5_interface->get_init_creds_opt_free(_context, opts);
            _krb5_interface->kt_close(_context, keytab);
            if (temp_ccache) {
                _krb5_interface->cc_close(_context, temp_ccache);
            }
            return status;
        }
        _ticket_lifetime_sec = static_cast<int64_t>(creds.times.endtime) -
                               static_cast<int64_t>(creds.times.starttime);

        // init ccache file
        status = _krb5_interface->cc_initialize(_context, temp_ccache, _principal);
        if (!status.ok()) {
            _krb5_interface->free_cred_contents(_context, &creds);
            _krb5_interface->get_init_creds_opt_free(_context, opts);
            _krb5_interface->kt_close(_context, keytab);
            _krb5_interface->cc_close(_context, temp_ccache);
            return status;
        }

        // save ccache
        status = _krb5_interface->cc_store_cred(_context, temp_ccache, &creds);
        if (!status.ok()) {
            _krb5_interface->free_cred_contents(_context, &creds);
            _krb5_interface->get_init_creds_opt_free(_context, opts);
            _krb5_interface->kt_close(_context, keytab);
            _krb5_interface->cc_close(_context, temp_ccache);
            return status;
        }

        // clean
        _krb5_interface->free_cred_contents(_context, &creds);
        _krb5_interface->get_init_creds_opt_free(_context, opts);
        _krb5_interface->kt_close(_context, keytab);

        // Only set _ccache if everything succeeded
        if (_ccache) {
            _krb5_interface->cc_close(_context, _ccache);
        }
        _ccache = temp_ccache;

    } catch (...) {
        if (keytab) {
            _krb5_interface->kt_close(_context, keytab);
        }
        if (temp_ccache) {
            _krb5_interface->cc_close(_context, temp_ccache);
        }
        return Status::InternalError("Failed to login with kerberos");
    }
    return Status::OK();
}

Status KerberosTicketCache::login_with_cache() {
    std::lock_guard<std::mutex> lock(_mutex);

    // Close existing ccache if any
    if (_ccache) {
        _krb5_interface->cc_close(_context, _ccache);
        _ccache = nullptr;
    }

    return _krb5_interface->cc_resolve(_context, _ticket_cache_path.c_str(), &_ccache);
}

Status KerberosTicketCache::write_ticket_cache() {
    std::lock_guard<std::mutex> lock(_mutex);

    if (!_ccache) {
        return Status::InternalError("No credentials cache available");
    }

    // MIT Kerberos automatically writes to the cache file
    // when using the FILE: cache type
    return Status::OK();
}

Status KerberosTicketCache::refresh_tickets() {
    try {
        return login();
    } catch (const std::exception& e) {
        std::stringstream ss;
        ss << "Failed to refresh tickets: " << e.what();
        return Status::InternalError(ss.str());
    }
}

void KerberosTicketCache::start_periodic_refresh() {
    _should_stop_refresh = false;
    _refresh_thread = std::make_unique<std::thread>([this]() {
        auto refresh_interval = std::chrono::milliseconds(
                static_cast<int>(_config.get_refresh_interval_second() * 1000));
        auto sleep_duration = _refresh_thread_sleep_time;
        std::chrono::milliseconds accumulated_time(0);
        while (!_should_stop_refresh) {
            std::this_thread::sleep_for(sleep_duration);
            accumulated_time += sleep_duration;
            if (accumulated_time >= refresh_interval) {
                accumulated_time = std::chrono::milliseconds(0); // Reset accumulated time
                Status st = refresh_tickets();
                if (!st.ok()) {
                    // ignore and continue
                    LOG(WARNING) << st.to_string();
                } else {
                    LOG(INFO) << "refresh kerberos ticket cache: " << _ticket_cache_path
                              << ", lifetime sec: " << _ticket_lifetime_sec;
                }
            }
        }
    });
}

void KerberosTicketCache::stop_periodic_refresh() {
    if (_refresh_thread) {
        _should_stop_refresh = true;
        _refresh_thread->join();
        _refresh_thread.reset();
    }
}

Status KerberosTicketCache::_initialize_context() {
    if (!_config.get_krb5_conf_path().empty()) {
        if (setenv("KRB5_CONFIG", _config.get_krb5_conf_path().c_str(), 1) != 0) {
            return Status::InvalidArgument("Failed to set KRB5_CONFIG environment variable");
        }
        LOG(INFO) << "Using custom krb5.conf: " << _config.get_krb5_conf_path();
    }

    RETURN_IF_ERROR(_krb5_interface->init_context(&_context));

    return _krb5_interface->parse_name(_context, _config.get_principal().c_str(), &_principal);
}

void KerberosTicketCache::_cleanup_context() {
    if (_principal) {
        _krb5_interface->free_principal(_context, _principal);
    }
    if (_ccache) {
        _krb5_interface->cc_close(_context, _ccache);
    }
    if (_context) {
        _krb5_interface->free_context(_context);
    }
}

std::vector<KerberosTicketInfo> KerberosTicketCache::get_ticket_info() {
    std::lock_guard<std::mutex> lock(_mutex);
    std::vector<KerberosTicketInfo> result;

    if (!_ccache || !_context) {
        // If no valid cache or context, return empty vector
        return result;
    }

    krb5_cc_cursor cursor;
    if (_krb5_interface->cc_start_seq_get(_context, _ccache, &cursor) != Status::OK()) {
        return result;
    }

    // Iterate through all credentials in the cache
    krb5_creds creds;
    while (_krb5_interface->cc_next_cred(_context, _ccache, &cursor, &creds) == Status::OK()) {
        KerberosTicketInfo info;
        info.principal = _config.get_principal();
        info.keytab_path = _config.get_keytab_path();
        info.start_time = static_cast<int64_t>(creds.times.starttime);
        info.expiry_time = static_cast<int64_t>(creds.times.endtime);
        info.auth_time = static_cast<int64_t>(creds.times.authtime);
        // minus 2,
        // one is shared_from_this(), the other is ref in _ticket_caches of KerberosTicketMgr
        info.use_count = shared_from_this().use_count() - 2;

        // Get the service principal name
        char* service_name = nullptr;
        if (_krb5_interface->unparse_name(_context, creds.server, &service_name) == Status::OK()) {
            info.service_principal = service_name;
            _krb5_interface->free_unparsed_name(_context, service_name);
        } else {
            info.service_principal = "Unparse Error";
        }
        info.cache_path = _ticket_cache_path;
        info.refresh_interval_second = _config.get_refresh_interval_second();
        info.hash_code = _config.get_hash_code();

        result.push_back(std::move(info));
        _krb5_interface->free_cred_contents(_context, &creds);
    }

    _krb5_interface->cc_end_seq_get(_context, _ccache, &cursor);
    return result;
}

} // namespace doris::kerberos
