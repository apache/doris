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

namespace doris::kerberos {

class KerberosConfig {
public:
    KerberosConfig();

    void set_keytab_path(const std::string& path) { _keytab_path = path; }
    void set_principal(const std::string& principal) { _principal = principal; }
    void set_cache_file_path(const std::string& path);
    void set_krb5_conf_path(const std::string& path) { _krb5_conf_path = path; }
    void set_refresh_interval(int32_t interval) { _refresh_interval_second = interval; }
    void set_min_time_before_refresh(int32_t time) { _min_time_before_refresh_second = time; }

    const std::string& get_keytab_path() const { return _keytab_path; }
    const std::string& get_principal() const { return _principal; }
    const std::string& get_cache_file_path() const { return _cache_file_path; }
    const std::string& get_krb5_conf_path() const { return _krb5_conf_path; }
    int32_t get_refresh_interval_second() const { return _refresh_interval_second; }
    int32_t get_min_time_before_refresh_second() const { return _min_time_before_refresh_second; }

private:
    std::string _keytab_path;
    std::string _principal;
    std::string _cache_file_path;
    std::string _krb5_conf_path;
    int32_t _refresh_interval_second;
    int32_t _min_time_before_refresh_second;
};

} // namespace doris::kerberos
