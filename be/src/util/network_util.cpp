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
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/util/network-util.cc
// and modified by Doris

#include "util/network_util.h"

#include <arpa/inet.h>
// IWYU pragma: no_include <bits/local_lim.h>
// IWYU pragma: no_include <bthread/errno.h>
#include <errno.h> // IWYU pragma: keep
#include <fmt/format.h>
#include <gen_cpp/Types_types.h>
#include <ifaddrs.h>
#include <netdb.h>
#include <netinet/in.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <sstream>

#ifdef __APPLE__
#ifndef HOST_NAME_MAX
#define HOST_NAME_MAX MAXHOSTNAMELEN
#endif
#endif

namespace doris {

InetAddress::InetAddress(std::string ip, sa_family_t family, bool is_loopback)
        : _ip_addr(ip), _family(family), _is_loopback(is_loopback) {}

bool InetAddress::is_loopback() const {
    return _is_loopback;
}

std::string InetAddress::get_host_address() const {
    return _ip_addr;
}

bool InetAddress::is_ipv6() const {
    return _family == AF_INET6;
}

static const std::string LOCALHOST("127.0.0.1");

Status get_hostname(std::string* hostname) {
    char name[HOST_NAME_MAX];
    int ret = gethostname(name, HOST_NAME_MAX);

    if (ret != 0) {
        return Status::InternalError("Could not get hostname: errno: {}", errno);
    }

    *hostname = std::string(name);
    return Status::OK();
}

bool is_valid_ip(const std::string& ip) {
    unsigned char buf[sizeof(struct in6_addr)];
    return (inet_pton(AF_INET6, ip.data(), buf) > 0) || (inet_pton(AF_INET, ip.data(), buf) > 0);
}

bool parse_endpoint(const std::string& endpoint, std::string* host, uint16_t* port) {
    auto p = endpoint.find_last_of(':');
    if (p == std::string::npos || p + 1 == endpoint.size()) {
        return false;
    }

    const char* port_base = endpoint.c_str() + p + 1;
    char* end = nullptr;
    long value = strtol(port_base, &end, 10);
    if (port_base == end) {
        return false;
    } else if (*end) {
        while (std::isspace(*end)) {
            end++;
        }
        if (*end) {
            return false;
        }
    } else if (value < 0 || 65535 < value) {
        return false;
    }

    std::string::size_type i = 0;
    const char* host_base = endpoint.c_str();
    while (std::isspace(host_base[i])) {
        i++;
    }
    if (i < p && host_base[i] == '[' && host_base[p - 1] == ']') {
        i += 1;
        p -= 1;
    }
    if (i >= p) {
        return false;
    }
    *host = endpoint.substr(i, p - i);
    *port = value;
    return true;
}

Status hostname_to_ip(const std::string& host, std::string& ip) {
    auto start = std::chrono::high_resolution_clock::now();
    Status status = hostname_to_ipv4(host, ip);
    if (status.ok()) {
        return status;
    }
    status = hostname_to_ipv6(host, ip);

    auto current = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(current - start);
    if (duration.count() >= 500) {
        LOG(WARNING) << "hostname_to_ip cost to mush time, cost_time:" << duration.count()
                     << "ms hostname:" << host << " ip:" << ip;
    }
    return status;
}

Status hostname_to_ip(const std::string& host, std::string& ip, bool ipv6) {
    if (ipv6) {
        return hostname_to_ipv6(host, ip);
    } else {
        return hostname_to_ipv4(host, ip);
    }
}

Status hostname_to_ipv4(const std::string& host, std::string& ip) {
    addrinfo hints, *res;
    in_addr addr;

    memset(&hints, 0, sizeof(addrinfo));
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_family = AF_INET;
    int err = getaddrinfo(host.c_str(), NULL, &hints, &res);
    if (err != 0) {
        LOG(WARNING) << "failed to get ip from host: " << host << "err:" << gai_strerror(err);
        return Status::InternalError("failed to get ip from host: {}, err: {}", host,
                                     gai_strerror(err));
    }

    addr.s_addr = ((sockaddr_in*)(res->ai_addr))->sin_addr.s_addr;
    ip = inet_ntoa(addr);

    freeaddrinfo(res);
    return Status::OK();
}

Status hostname_to_ipv6(const std::string& host, std::string& ip) {
    char ipstr2[128];
    struct sockaddr_in6* sockaddr_ipv6;

    struct addrinfo *answer, hint;
    bzero(&hint, sizeof(hint));
    hint.ai_family = AF_INET6;
    hint.ai_socktype = SOCK_STREAM;

    int err = getaddrinfo(host.c_str(), NULL, &hint, &answer);
    if (err != 0) {
        LOG(WARNING) << "failed to get ip from host: " << host << "err:" << gai_strerror(err);
        return Status::InternalError("failed to get ip from host: {}, err: {}", host,
                                     gai_strerror(err));
    }

    sockaddr_ipv6 = reinterpret_cast<struct sockaddr_in6*>(answer->ai_addr);
    inet_ntop(AF_INET6, &sockaddr_ipv6->sin6_addr, ipstr2, sizeof(ipstr2));
    ip = ipstr2;
    fflush(NULL);
    freeaddrinfo(answer);
    return Status::OK();
}

bool find_first_non_localhost(const std::vector<std::string>& addresses, std::string* addr) {
    for (const std::string& candidate : addresses) {
        if (candidate != LOCALHOST) {
            *addr = candidate;
            return true;
        }
    }

    return false;
}

Status get_hosts(std::vector<InetAddress>* hosts) {
    ifaddrs* if_addrs = nullptr;
    if (getifaddrs(&if_addrs)) {
        std::stringstream ss;
        char buf[64];
        ss << "getifaddrs failed because " << strerror_r(errno, buf, sizeof(buf));
        return Status::InternalError(ss.str());
    }

    for (ifaddrs* if_addr = if_addrs; if_addr != nullptr; if_addr = if_addr->ifa_next) {
        if (!if_addr->ifa_addr) {
            continue;
        }
        auto addr = if_addr->ifa_addr;
        if (addr->sa_family == AF_INET) {
            // check legitimacy of IP4 Address
            char addr_buf[INET_ADDRSTRLEN];
            auto tmp_addr = &((struct sockaddr_in*)if_addr->ifa_addr)->sin_addr;
            inet_ntop(AF_INET, tmp_addr, addr_buf, INET_ADDRSTRLEN);
            // check is loopback Address
            in_addr_t s_addr = ((struct sockaddr_in*)addr)->sin_addr.s_addr;
            bool is_loopback = (ntohl(s_addr) & 0xFF000000) == 0x7F000000;
            hosts->emplace_back(std::string(addr_buf), AF_INET, is_loopback);
        } else if (addr->sa_family == AF_INET6) {
            // check legitimacy of IP6 Address
            auto tmp_addr = &((struct sockaddr_in6*)if_addr->ifa_addr)->sin6_addr;
            char addr_buf[INET6_ADDRSTRLEN];
            inet_ntop(AF_INET6, tmp_addr, addr_buf, sizeof(addr_buf));
            // check is loopback Address
            bool is_loopback = IN6_IS_ADDR_LOOPBACK(tmp_addr);
            hosts->emplace_back(std::string(addr_buf), AF_INET6, is_loopback);
        } else {
            continue;
        }
    }

    if (if_addrs != nullptr) {
        freeifaddrs(if_addrs);
    }

    return Status::OK();
}

TNetworkAddress make_network_address(const std::string& hostname, int port) {
    TNetworkAddress ret;
    ret.__set_hostname(hostname);
    ret.__set_port(port);
    return ret;
}

Status get_inet_interfaces(std::vector<std::string>* interfaces, bool include_ipv6) {
    ifaddrs* if_addrs = nullptr;
    if (getifaddrs(&if_addrs)) {
        std::stringstream ss;
        char buf[64];
        ss << "getifaddrs failed, errno:" << errno << ", message"
           << strerror_r(errno, buf, sizeof(buf));
        return Status::InternalError(ss.str());
    }

    for (ifaddrs* if_addr = if_addrs; if_addr != nullptr; if_addr = if_addr->ifa_next) {
        if (if_addr->ifa_addr == nullptr || if_addr->ifa_name == nullptr) {
            continue;
        }
        if (if_addr->ifa_addr->sa_family == AF_INET ||
            (include_ipv6 && if_addr->ifa_addr->sa_family == AF_INET6)) {
            interfaces->emplace_back(if_addr->ifa_name);
        }
    }
    if (if_addrs != nullptr) {
        freeifaddrs(if_addrs);
    }
    return Status::OK();
}

std::string get_host_port(const std::string& host, int port) {
    std::stringstream ss;
    if (host.find(':') == std::string::npos) {
        ss << host << ":" << port;
    } else {
        ss << "[" << host << "]"
           << ":" << port;
    }
    return ss.str();
}

std::string get_brpc_http_url(const std::string& host, int port) {
    if (host.find(':') != std::string::npos) {
        return fmt::format("list://[{}]:{}", host, port);
    } else {
        return fmt::format("http://{}:{}", host, port);
    }
}

} // namespace doris
