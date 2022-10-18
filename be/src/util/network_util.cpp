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
#include <common/logging.h>
#include <ifaddrs.h>
#include <limits.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/types.h>

#include <sstream>

#ifdef __APPLE__
#ifndef HOST_NAME_MAX
#define HOST_NAME_MAX MAXHOSTNAMELEN
#endif
#endif

namespace doris {

InetAddress::InetAddress(struct sockaddr* addr) {
    this->addr = *(struct sockaddr_in*)addr;
}

bool InetAddress::is_address_v4() const {
    return addr.sin_family == AF_INET;
}

bool InetAddress::is_loopback_v4() {
    in_addr_t s_addr = addr.sin_addr.s_addr;
    return (ntohl(s_addr) & 0xFF000000) == 0x7F000000;
}

std::string InetAddress::get_host_address_v4() {
    char addr_buf[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &(addr.sin_addr), addr_buf, INET_ADDRSTRLEN);
    return std::string(addr_buf);
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

        addresses->push_back(std::string(addr_buf));
        it = it->ai_next;
    }

    freeaddrinfo(addr_info);
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

Status get_hosts_v4(std::vector<InetAddress>* hosts) {
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
        if (if_addr->ifa_addr->sa_family == AF_INET) { // check it is IP4
            // is a valid IP4 Address
            hosts->emplace_back(if_addr->ifa_addr);
        }
        //TODO: IPv6
        /*
        else if (if_addr->ifa_addr->sa_family == AF_INET6) { // check it is IP6
            // is a valid IP6 Address
            void* tmp_addr = &((struct sockaddr_in6 *)if_addr->ifa_addr)->sin6_addr;
            char addr_buf[INET6_ADDRSTRLEN];
            inet_ntop(AF_INET6, tmp_addr, addr_buf, sizeof(addr_buf));
            local_ip->assign(addr_buf);
            break;
        }
        */
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

} // namespace doris
