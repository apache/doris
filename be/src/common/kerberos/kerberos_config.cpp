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

#include "common/kerberos/kerberos_config.h"

#include <filesystem>

#include "common/config.h"
#include "util/md5.h"

namespace doris::kerberos {

KerberosConfig::KerberosConfig()
        : _refresh_interval_second(3600), _min_time_before_refresh_second(600) {}

std::string KerberosConfig::get_hash_code(const std::string& principal, const std::string& keytab) {
    return _get_hash_code(principal, keytab);
}

std::string KerberosConfig::_get_hash_code(const std::string& principal,
                                           const std::string& keytab) {
    // use md5(principal + keytab) as hash code
    // so that same (principal + keytab) will have same name.
    std::string combined = principal + keytab;
    Md5Digest digest;
    digest.update(combined.c_str(), combined.length());
    digest.digest();
    return digest.hex();
}

} // namespace doris::kerberos
