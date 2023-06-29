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

Status hostname_to_ip_addrs(const std::string& name, std::vector<std::string>* addresses) {
    addrinfo hints;
    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_family = AF_INET; // IPv4 addresses only
    hints.ai_socktype = SOCK_STREAM;

    struct addrinfo* addr_info;

    if (getaddrinfo(name.c_str(), nullptr, &hints, &addr_info) != 0) {
        return Status::InternalError("Could not find IPv4 address for: {}", name);
    }

    addrinfo* it = addr_info;

    while (it != nullptr) {
        char addr_buf[64];
        const char* result =
                inet_ntop(AF_INET, &((sockaddr_in*)it->ai_addr)->sin_addr, addr_buf, 64);

        if (result == nullptr) {
            freeaddrinfo(addr_info);
            return Status::InternalError("Could not convert IPv4 address for: {}", name);
        }

        // add address if not exists
        std::string address = std::string(addr_buf);
        if (std::find(addresses->begin(), addresses->end(), address) != addresses->end()) {
            LOG(WARNING) << "Repeated ip addresses has been found for host: " << name
                         << ", ip address:" << address
                         << ", please check your network configuration";
        } else {
            addresses->push_back(address);
        }
        it = it->ai_next;
    }

    freeaddrinfo(addr_info);
    return Status::OK();
}

bool is_valid_ip(const std::string& ip) {
    unsigned char buf[sizeof(struct in6_addr)];
    return (inet_pton(AF_INET6, ip.data(), buf) > 0) || (inet_pton(AF_INET, ip.data(), buf) > 0);
}

Status hostname_to_ip(const std::string& host, std::string& ip) {
    std::vector<std::string> addresses;
    Status status = hostname_to_ip_addrs(host, &addresses);
    if (!status.ok()) {
        LOG(WARNING) << "status of hostname_to_ip_addrs was not ok, err is " << status.to_string();
        return status;
    }
    if (addresses.size() != 1) {
        std::stringstream ss;
        std::copy(addresses.begin(), addresses.end(), std::ostream_iterator<std::string>(ss, ","));
        LOG(WARNING)
                << "the number of addresses could only be equal to 1, failed to get ip from host:"
                << host << ", addresses:" << ss.str();
        return Status::InternalError(
                "the number of addresses could only be equal to 1, failed to get ip from host: "
                "{}, addresses:{}",
                host, ss.str());
    }
    ip = addresses[0];
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
