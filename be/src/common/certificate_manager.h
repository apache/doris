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

#include <openssl/evp.h>
#include <openssl/x509.h>

#include <filesystem>
#include <functional>
#include <string>
#include <string_view>

namespace doris {
class CertificateManager {
public:
    static constexpr std::string_view verify_none = "verify_none";
    static constexpr std::string_view verify_peer = "verify_peer";
    static constexpr std::string_view verify_fail_if_no_peer_cert = "verify_fail_if_no_peer_cert";

    struct CertFileMonitorState {
        std::filesystem::file_time_type write_time;
        bool has_value = false;
    };

    static X509* load_ca(std::filesystem::path path);
    static X509* load_cert(std::filesystem::path path);
    static EVP_PKEY* load_key(std::filesystem::path path, std::string passwd);
    static std::string load_key_string(std::filesystem::path path, std::string passwd);

    static bool check_certificate_file(const std::string& path, CertFileMonitorState* state,
                                       const char* label,
                                       const std::function<bool()>& should_stop = {});
};

} // namespace doris
