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

#include "service/backend_options.h"

#include <algorithm>
#include <ostream>

#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "gutil/strings/split.h"
#include "util/cidr.h"
#include "util/network_util.h"

namespace doris {

static const std::string PRIORITY_CIDR_SEPARATOR = ";";

std::string BackendOptions::_s_localhost;
std::vector<CIDR> BackendOptions::_s_priority_cidrs;
bool BackendOptions::_bind_ipv6 = false;
const char* _service_bind_address = "0.0.0.0";
int64_t BackendOptions::_s_backend_id = 0;

bool BackendOptions::init() {
    if (!analyze_priority_cidrs(config::priority_networks, &_s_priority_cidrs)) {
        return false;
    }
    std::vector<InetAddress> hosts;
    Status status = get_hosts(&hosts);

    if (!status.ok()) {
        LOG(FATAL) << status;
        return false;
    }

    if (hosts.empty()) {
        LOG(FATAL) << "failed to get host";
        return false;
    }
    if (!analyze_localhost(_s_localhost, _bind_ipv6, &_s_priority_cidrs, &hosts)) {
        return false;
    }
    if (_bind_ipv6) {
        _service_bind_address = "[::0]";
    }
    LOG(INFO) << "local host ip=" << _s_localhost;
    return true;
}

const std::string& BackendOptions::get_localhost() {
    return _s_localhost;
}

std::string BackendOptions::get_be_endpoint() {
    return _s_localhost + ":" + std::to_string(config::heartbeat_service_port);
}

TBackend BackendOptions::get_local_backend() {
    TBackend backend;
    backend.__set_host(_s_localhost);
    backend.__set_be_port(config::be_port);
    backend.__set_http_port(config::webserver_port);
    backend.__set_brpc_port(config::brpc_port);
    backend.__set_id(_s_backend_id);
    return backend;
}

void BackendOptions::set_backend_id(int64_t backend_id) {
    _s_backend_id = backend_id;
}

void BackendOptions::set_localhost(const std::string& host) {
    _s_localhost = host;
}

bool BackendOptions::is_bind_ipv6() {
    return _bind_ipv6;
}

const char* BackendOptions::get_service_bind_address() {
    return _service_bind_address;
}

const char* BackendOptions::get_service_bind_address_without_bracket() {
    if (_bind_ipv6) {
        return "::0";
    }
    return _service_bind_address;
}

bool BackendOptions::analyze_priority_cidrs(const std::string& priority_networks,
                                            std::vector<CIDR>* cidrs) {
    if (priority_networks.empty()) {
        return true;
    }
    LOG(INFO) << "priority cidrs: " << priority_networks;

    std::vector<std::string> cidr_strs = strings::Split(priority_networks, PRIORITY_CIDR_SEPARATOR);

    for (auto& cidr_str : cidr_strs) {
        CIDR cidr;
        if (!cidr.reset(cidr_str)) {
            LOG(FATAL) << "wrong cidr format. cidr_str=" << cidr_str;
            return false;
        }
        cidrs->push_back(cidr);
    }
    return true;
}

bool BackendOptions::analyze_localhost(std::string& localhost, bool& bind_ipv6,
                                       std::vector<CIDR>* cidrs, std::vector<InetAddress>* hosts) {
    auto addr_it = hosts->begin();
    if (!cidrs->empty()) {
        for (; addr_it != hosts->end(); ++addr_it) {
            VLOG_CRITICAL << "check ip=" << addr_it->get_host_address();
            // Whether to use IPV4 or IPV6, it's configured by CIDR format.
            // If both IPV4 and IPV6 are configured, the config order decides priority.
            if (is_in_prior_network(addr_it->get_host_address())) {
                localhost = addr_it->get_host_address();
                bind_ipv6 = addr_it->is_ipv6();
                break;
            }
            LOG(INFO) << "skip ip not belonged to priority networks: "
                      << addr_it->get_host_address();
        }
        if (localhost.empty()) {
            LOG(FATAL) << "fail to find one valid address, exit.";
            return false;
        }
    } else {
        std::string loopback;
        for (; addr_it != hosts->end(); ++addr_it) {
            if ((*addr_it).is_loopback()) {
                loopback = addr_it->get_host_address();
                _bind_ipv6 = addr_it->is_ipv6();
            } else if (!addr_it->is_ipv6()) {
                localhost = addr_it->get_host_address();
                _bind_ipv6 = addr_it->is_ipv6();
                break;
            }
        }
        if (localhost.empty()) {
            LOG(INFO) << "fail to find one valid non-loopback address, use loopback address.";
            localhost = loopback;
        }
    }
    return true;
}

bool BackendOptions::is_in_prior_network(const std::string& ip) {
    for (auto& cidr : _s_priority_cidrs) {
        CIDR _ip;
        _ip.reset(ip);
        if (cidr.contains(_ip)) {
            return true;
        }
    }
    return false;
}

} // namespace doris
