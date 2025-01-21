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
#include <sstream>
#include <thread>

namespace doris::kerberos {

KerberosTicketCache::KerberosTicketCache(const KerberosConfig& config) : _config(config) {}

KerberosTicketCache::~KerberosTicketCache() {
    stop_periodic_refresh();
    _cleanup_context();
    LOG(INFO) << "destroy kerberos ticket cache with principal: " << _config.get_principal();
}

Status KerberosTicketCache::initialize() {
    std::lock_guard<std::mutex> lock(_mutex);
    Status st = _initialize_context();
    LOG(INFO) << "initialized kerberos ticket cache with principal: " << _config.get_principal()
              << ": " << st.to_string();
    return st;
}

Status KerberosTicketCache::login() {
    std::lock_guard<std::mutex> lock(_mutex);

    krb5_keytab keytab = nullptr;
    krb5_error_code code;

    try {
        // Open the keytab file
        code = krb5_kt_resolve(_context, _config.get_keytab_path().c_str(), &keytab);
        RETURN_IF_ERROR(_check_error(code, "Failed to resolve keytab"));

        // init ccache
        code = krb5_cc_resolve(_context, _config.get_cache_file_path().c_str(), &_ccache);
        RETURN_IF_ERROR(_check_error(code, "Failed to resolve credential cache"));

        // get init creds
        krb5_get_init_creds_opt* opts = nullptr;
        code = krb5_get_init_creds_opt_alloc(_context, &opts);
        RETURN_IF_ERROR(_check_error(code, "Failed to allocate get_init_creds_opt"));

        krb5_creds creds;
        code = krb5_get_init_creds_keytab(_context, &creds, _principal, keytab,
                                          0,       // start time
                                          nullptr, // TKT service name
                                          opts);
        RETURN_IF_ERROR(_check_error(code, "Failed to get initial credentials"));

        // init ccache file
        code = krb5_cc_initialize(_context, _ccache, _principal);
        RETURN_IF_ERROR(_check_error(code, "Failed to initialize credential cache"));

        // save ccache
        code = krb5_cc_store_cred(_context, _ccache, &creds);
        RETURN_IF_ERROR(_check_error(code, "Failed to store credentials"));

        // clean
        krb5_free_cred_contents(_context, &creds);
        krb5_get_init_creds_opt_free(_context, opts);
        krb5_kt_close(_context, keytab);
    } catch (...) {
        if (keytab) {
            krb5_kt_close(_context, keytab);
        }
        return Status::InternalError("Failed to login with kerberos");
    }
    return Status::OK();
}

Status KerberosTicketCache::login_with_cache() {
    std::lock_guard<std::mutex> lock(_mutex);
    krb5_error_code code =
            krb5_cc_resolve(_context, _config.get_cache_file_path().c_str(), &_ccache);
    return _check_error(code, "Failed to resolve credential cache");
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
    if (!_needs_refresh()) {
        return Status::OK();
    }

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
        auto sleep_duration = std::chrono::milliseconds(10);
        std::chrono::milliseconds accumulated_time(0);
        while (!_should_stop_refresh) {
            std::this_thread::sleep_for(sleep_duration);
            accumulated_time += sleep_duration;
            if (accumulated_time >= refresh_interval) {
                accumulated_time = std::chrono::milliseconds(0); // Reset accumulated time

                if (_needs_refresh()) {
                    Status st = refresh_tickets();
                    if (!st.ok()) {
                        // ignore and continue
                        LOG(WARNING) << st.to_string();
                    } else {
                        LOG(INFO) << "refresh kerberos ticket cache: "
                                  << _config.get_cache_file_path();
                    }
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
    krb5_error_code code;

    if (!_config.get_krb5_conf_path().empty()) {
        if (setenv("KRB5_CONFIG", _config.get_krb5_conf_path().c_str(), 1) != 0) {
            return Status::InvalidArgument("Failed to set KRB5_CONFIG environment variable");
        }
        LOG(INFO) << "Using custom krb5.conf: " << _config.get_krb5_conf_path();
    }

    code = krb5_init_context(&_context);
    RETURN_IF_ERROR(_check_error(code, "Failed to initialize krb5 context"));

    code = krb5_parse_name(_context, _config.get_principal().c_str(), &_principal);
    return _check_error(code, "Failed to parse principal name");
}

void KerberosTicketCache::_cleanup_context() {
    if (_principal) {
        krb5_free_principal(_context, _principal);
    }
    if (_ccache) {
        krb5_cc_close(_context, _ccache);
    }
    if (_context) {
        krb5_free_context(_context);
    }
}

Status KerberosTicketCache::_check_error(krb5_error_code code, const char* message) {
    if (code) {
        const char* err_message = krb5_get_error_message(_context, code);
        std::string full_message = std::string(message) + ": " + err_message;
        krb5_free_error_message(_context, err_message);
        return Status::InternalError(full_message);
    }
    return Status::OK();
}

bool KerberosTicketCache::_needs_refresh() const {
    if (!_ccache) {
        return true;
    }

    krb5_timestamp now;
    if (krb5_timeofday(_context, &now) != 0) {
        return true;
    }

    krb5_cc_cursor cursor;
    if (krb5_cc_start_seq_get(_context, _ccache, &cursor) != 0) {
        return true;
    }

    bool needs_refresh = true;
    krb5_creds creds;
    while (krb5_cc_next_cred(_context, _ccache, &cursor, &creds) == 0) {
        if (creds.times.endtime - now > _config.get_min_time_before_refresh_second()) {
            needs_refresh = false;
        }
        krb5_free_cred_contents(_context, &creds);
    }

    krb5_cc_end_seq_get(_context, _ccache, &cursor);
    return needs_refresh;
}

} // namespace doris::kerberos
