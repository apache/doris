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

#include <chrono>
#include <string>

#include "common/status.h"

namespace doris::kerberos {

// Configuration class for Kerberos authentication
class KerberosConfig {
public:
    // Constructor with default values for refresh intervals
    KerberosConfig();

    // Set the Kerberos principal and keytab file path
    void set_principal_and_keytab(const std::string& principal, const std::string& keytab) {
        _principal = principal;
        _keytab_path = keytab;
    }
    // Set the path to krb5.conf configuration file
    void set_krb5_conf_path(const std::string& path) { _krb5_conf_path = path; }
    // Set the interval for refreshing Kerberos tickets (in seconds)
    void set_refresh_interval(int32_t interval) { _refresh_interval_second = interval; }
    // Set the minimum time before refreshing tickets (in seconds)
    void set_min_time_before_refresh(int32_t time) { _min_time_before_refresh_second = time; }

    // Get the Kerberos principal name
    const std::string& get_principal() const { return _principal; }
    // Get the path to the keytab file
    const std::string& get_keytab_path() const { return _keytab_path; }
    // Get the path to krb5.conf configuration file
    const std::string& get_krb5_conf_path() const { return _krb5_conf_path; }
    // Get the ticket refresh interval in seconds
    int32_t get_refresh_interval_second() const { return _refresh_interval_second; }
    // Get the minimum time before refresh in seconds
    int32_t get_min_time_before_refresh_second() const { return _min_time_before_refresh_second; }

    std::string get_hash_code() const { return _get_hash_code(_principal, _keytab_path); }

    // Use principal and keytab to generate a hash code.
    static std::string get_hash_code(const std::string& principal, const std::string& keytab);

private:
    static std::string _get_hash_code(const std::string& principal, const std::string& keytab);

private:
    // Kerberos principal name (e.g., "user@REALM.COM")
    std::string _principal;
    // Path to the Kerberos keytab file
    std::string _keytab_path;
    // Path to the Kerberos configuration file (krb5.conf)
    std::string _krb5_conf_path;
    // Interval for refreshing Kerberos tickets (in seconds)
    int32_t _refresh_interval_second;
    // Minimum time before refreshing tickets (in seconds)
    int32_t _min_time_before_refresh_second;
};

} // namespace doris::kerberos
