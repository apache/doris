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

#include "common/network_util.h"

#include <arpa/inet.h>
#include <butil/endpoint.h>
#include <butil/strings/string_split.h>
#include <ifaddrs.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <iterator>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/defer.h"
#include "common/logging.h"

namespace doris::cloud {

constexpr uint8_t kIPv4Bits = 32;
constexpr uint8_t kIPv6Bits = 128;

// Classless Inter-Domain Routing (IPv4/IPv6), ported from be/src/util/cidr.cpp.
class CIDR {
public:
    bool reset(const std::string& cidr_str) {
        auto slash = std::find(std::begin(cidr_str), std::end(cidr_str), '/');
        auto ip = (slash == std::end(cidr_str))
                          ? cidr_str
                          : cidr_str.substr(0, slash - std::begin(cidr_str));

        if (inet_pton(AF_INET, ip.c_str(), address_.data())) {
            family_ = AF_INET;
            netmask_len_ = kIPv4Bits;
        } else if (inet_pton(AF_INET6, ip.c_str(), address_.data())) {
            family_ = AF_INET6;
            netmask_len_ = kIPv6Bits;
        } else {
            LOG(WARNING) << "wrong CIDR IP format. network=" << cidr_str;
            return false;
        }

        if (slash == std::end(cidr_str)) {
            return true;
        }

        std::size_t pos;
        std::string suffix = std::string(slash + 1, std::end(cidr_str));
        int len;
        try {
            len = std::stoi(suffix, &pos);
        } catch (const std::exception& e) {
            LOG(WARNING) << "wrong CIDR format. network=" << cidr_str << ", reason=" << e.what();
            return false;
        }

        if (pos != suffix.size()) {
            LOG(WARNING) << "wrong CIDR format. network=" << cidr_str;
            return false;
        }

        if (len < 0 || len > netmask_len_) {
            LOG(WARNING) << "wrong CIDR mask format. network=" << cidr_str;
            return false;
        }
        netmask_len_ = len;
        return true;
    }

    bool contains(const CIDR& ip) const {
        if ((family_ != ip.family_) || (netmask_len_ > ip.netmask_len_)) {
            return false;
        }
        auto bytes = netmask_len_ / 8;
        auto cidr_begin = address_.cbegin();
        auto ip_begin = ip.address_.cbegin();
        if (!std::equal(cidr_begin, cidr_begin + bytes, ip_begin, ip_begin + bytes)) {
            return false;
        }
        if ((netmask_len_ % 8) == 0) {
            return true;
        }
        auto mask = (0xFF << (8 - (netmask_len_ % 8))) & 0xFF;
        return (address_[bytes] & mask) == (ip.address_[bytes] & mask);
    }

private:
    sa_family_t family_;
    std::array<uint8_t, 16> address_;
    uint8_t netmask_len_;
};

// Interface address (IPv4 or IPv6), ported from be/src/util/network_util.h.
class InetAddress {
public:
    InetAddress(std::string ip, sa_family_t family, bool is_loopback)
            : ip_(std::move(ip)), family_(family), is_loopback_(is_loopback) {}
    bool is_loopback() const { return is_loopback_; }
    const std::string& get_host_address() const { return ip_; }
    bool is_ipv6() const { return family_ == AF_INET6; }

private:
    std::string ip_;
    sa_family_t family_;
    bool is_loopback_;
};

static bool get_hosts(std::vector<InetAddress>* hosts) {
    ifaddrs* if_addrs = nullptr;
    if (getifaddrs(&if_addrs)) {
        char buf[64];
        LOG(FATAL) << "getifaddrs failed because " << strerror_r(errno, buf, sizeof(buf));
        return false;
    }

    for (ifaddrs* if_addr = if_addrs; if_addr != nullptr; if_addr = if_addr->ifa_next) {
        if (!if_addr->ifa_addr) {
            continue;
        }
        auto addr = if_addr->ifa_addr;
        if (addr->sa_family == AF_INET) { // check it is IP4
            // is a valid IP4 Address
            char addr_buf[INET_ADDRSTRLEN];
            auto tmp_addr = &((struct sockaddr_in*)addr)->sin_addr;
            inet_ntop(AF_INET, tmp_addr, addr_buf, INET_ADDRSTRLEN);
            in_addr_t s_addr = ((struct sockaddr_in*)addr)->sin_addr.s_addr;
            bool is_loopback = (ntohl(s_addr) & 0xFF000000) == 0x7F000000;
            hosts->emplace_back(addr_buf, AF_INET, is_loopback);
        } else if (addr->sa_family == AF_INET6) { // check it is IP6
            // is a valid IP6 Address
            auto tmp_addr = &((struct sockaddr_in6*)addr)->sin6_addr;
            char addr_buf[INET6_ADDRSTRLEN];
            inet_ntop(AF_INET6, tmp_addr, addr_buf, sizeof(addr_buf));
            bool is_loopback = IN6_IS_ADDR_LOOPBACK(tmp_addr);
            hosts->emplace_back(addr_buf, AF_INET6, is_loopback);
        }
    }

    if (if_addrs != nullptr) {
        freeifaddrs(if_addrs);
    }

    return true;
}

std::string get_local_ip(const std::string& priority_networks) {
    std::string localhost_str = butil::my_ip_cstr();
    DORIS_CLOUD_DEFER {
        // A loopback address (127.0.0.1 or ::1) makes ms/recycler exit, unless enabled.
        LOG(INFO) << "get the IP for ms is " << localhost_str;
        if (config::enable_loopback_address_for_ms ||
            (localhost_str != "127.0.0.1" && localhost_str != "::1"))
            return;
        LOG(WARNING) << "localhost IP is loopback address (" << localhost_str << "), "
                     << "there may be multiple NICs for use, "
                     << "please set priority_networks with a CIDR expression in doris_cloud.conf "
                     << "to choose a non-loopback address accordingly";
        LOG(WARNING) << "process will exit ...";
        exit(-1);
    };
    if (priority_networks == "") {
        LOG(INFO) << "use butil::my_ip_cstr(), local host ip=" << localhost_str;
        return localhost_str;
    }
    std::vector<CIDR> priority_cidrs;
    LOG(INFO) << "priority CIDRs: " << priority_networks;
    std::vector<std::string> cidr_strs;
    butil::SplitString(priority_networks, ';', &cidr_strs);
    for (auto& cidr_str : cidr_strs) {
        CIDR cidr;
        if (!cidr.reset(cidr_str)) {
            LOG(FATAL) << "wrong cidr format. cidr_str=" << cidr_str;
            return localhost_str;
        }
        priority_cidrs.push_back(cidr);
    }

    std::vector<InetAddress> hosts;
    if (!get_hosts(&hosts)) {
        LOG(FATAL) << "failed to getifaddrs";
        return localhost_str;
    }

    if (hosts.empty()) {
        LOG(FATAL) << "failed to get host";
        return localhost_str;
    }

    auto is_in_prior_network = [&priority_cidrs](const std::string& ip) {
        for (auto& cidr : priority_cidrs) {
            CIDR cidr_ip;
            cidr_ip.reset(ip);
            if (cidr.contains(cidr_ip)) {
                return true;
            }
        }
        return false;
    };

    std::string loopback;
    localhost_str = "";
    for (auto addr_it = hosts.begin(); addr_it != hosts.end(); ++addr_it) {
        const std::string& host_address = addr_it->get_host_address();
        VLOG_DEBUG << "check ip=" << host_address;
        // The CIDR family selects IPv4 or IPv6; with both configured,
        // the config order decides priority (same as BE BackendOptions).
        if (is_in_prior_network(host_address)) {
            localhost_str = host_address;
            break;
        }
        // Prefer the IPv4 loopback fallback: on dual-stack lo (127.0.0.1 + ::1)
        // the result must stay 127.0.0.1, as before IPv6 support.
        if (addr_it->is_loopback() && (loopback.empty() || !addr_it->is_ipv6())) {
            loopback = host_address;
        }
        // VLOG_DEBUG, not LOG(INFO): get_local_ip() runs in the periodic server
        // register thread, INFO here would spam the log on every cycle.
        VLOG_DEBUG << "skip ip not belonged to priority networks: " << host_address;
    }
    if (!localhost_str.empty()) {
        LOG(INFO) << "local host ip=" << localhost_str;
        return localhost_str;
    }

    if (!loopback.empty()) {
        localhost_str = loopback;
        LOG(WARNING) << "fail to find one valid non-loopback address, use loopback address: "
                     << localhost_str;
    } else {
        localhost_str = butil::my_ip_cstr();
        LOG(WARNING)
                << "fail to find valid address of priority cidrs in conf, use butil::my_ip_cstr(): "
                << localhost_str;
    }

    return localhost_str;
}

std::string get_service_bind_address(const std::string& priority_networks) {
    if (priority_networks.empty()) {
        return "0.0.0.0";
    }
    // Only IPv6 addresses contain ':'.
    const std::string ip = get_local_ip(priority_networks);
    return ip.find(':') != std::string::npos ? "[::0]" : "0.0.0.0";
}

std::string get_host_port(const std::string& host, int port) {
    if (host.find(':') == std::string::npos) {
        return host + ":" + std::to_string(port);
    }
    return "[" + host + "]:" + std::to_string(port);
}

} // namespace doris::cloud
