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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/util/network-util.h
// and modified by Doris

#pragma once

#include <gen_cpp/Types_types.h>
#include <sys/un.h>

#include <string>
#include <vector>

#include "common/status.h"

namespace doris {

class InetAddress {
public:
    InetAddress(std::string ip, sa_family_t family, bool is_loopback);
    bool is_loopback() const;
    std::string get_host_address() const;
    bool is_ipv6() const;

private:
    std::string _ip_addr;
    sa_family_t _family;
    bool _is_loopback;
};

bool is_valid_ip(const std::string& ip);

bool parse_endpoint(const std::string& endpoint, std::string* host, uint16_t* port);

// The `gai_err` out-parameter, when non-null, receives the raw getaddrinfo() return code
// (0 on success). Callers need it to tell an authoritative "no such host" (EAI_NONAME)
// apart from a transient resolver problem (EAI_AGAIN, EAI_SYSTEM, ...): the former means
// the host is really gone, the latter means DNS itself is unhealthy while the host is
// most likely still alive. DNSCache relies on that distinction so a resolver outage does
// not get mistaken for every backend disappearing at once.
// For the two-argument hostname_to_ip(), which falls back from IPv4 to IPv6, `gai_err`
// reports the code of the last attempt.
Status hostname_to_ip(const std::string& host, std::string& ip, int* gai_err = nullptr);

Status hostname_to_ipv4(const std::string& host, std::string& ip, int* gai_err = nullptr);

Status hostname_to_ipv6(const std::string& host, std::string& ip, int* gai_err = nullptr);

Status hostname_to_ip(const std::string& host, std::string& ip, bool ipv6, int* gai_err = nullptr);

// Finds the first non-localhost IP address in the given list. Returns
// true if such an address was found, false otherwise.
bool find_first_non_localhost(const std::vector<std::string>& addresses, std::string* addr);

// Sets the output argument to the system defined hostname.
// Returns OK if a hostname can be found, false otherwise.
Status get_hostname(std::string* hostname);

Status get_hosts(std::vector<InetAddress>* hosts);

// Utility method because Thrift does not supply useful constructors
TNetworkAddress make_network_address(const std::string& hostname, int port);

Status get_inet_interfaces(std::vector<std::string>* interfaces, bool include_ipv6 = false);

std::string get_host_port(const std::string& host, int port);

std::string get_brpc_http_url(const std::string& host, int port);

} // namespace doris
