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

#include <butil/macros.h>

#include <string>
#include <vector>

#include "gen_cpp/Types_types.h"
#include "util/network_util.h"

namespace doris {

class CIDR;

class BackendOptions {
public:
    BackendOptions() = delete;
    static bool init();
    static const std::string& get_localhost();
    static std::string get_be_endpoint();
    static TBackend get_local_backend();
    static void set_backend_id(int64_t backend_id);
    static void set_localhost(const std::string& host);
    static bool is_bind_ipv6();
    static const char* get_service_bind_address();
    static const char* get_service_bind_address_without_bracket();
    static bool analyze_priority_cidrs(const std::string& priority_networks,
                                       std::vector<CIDR>* cidrs);
    static bool analyze_localhost(std::string& localhost, bool& bind_ipv6, std::vector<CIDR>* cidrs,
                                  std::vector<InetAddress>* hosts);

private:
    static bool is_in_prior_network(const std::string& ip);

    static std::string _s_localhost;
    static int64_t _s_backend_id;
    static std::vector<CIDR> _s_priority_cidrs;
    static bool _bind_ipv6;
};

} // namespace doris
