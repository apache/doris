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
#include <cstdint>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/defer.h"
#include "common/logging.h"

namespace doris::cloud {

class CIDR {
public:
    bool reset(const std::string& cidr_str) {
        address_.fill(0);

        // check if have mask
        std::string cidr_format_str = cidr_str;
        int32_t have_mask = cidr_str.find("/");
        std::string ip_str = have_mask == -1 ? cidr_str : cidr_str.substr(0, have_mask);
        if (inet_pton(AF_INET, ip_str.c_str(), address_.data())) {
            family_ = AF_INET;
            netmask_len_ = kIPv4Bits;
        } else if (inet_pton(AF_INET6, ip_str.c_str(), address_.data())) {
            family_ = AF_INET6;
            netmask_len_ = kIPv6Bits;
        } else {
            LOG(WARNING) << "wrong CIDR IP value. network=" << cidr_str;
            return false;
        }
        if (have_mask == -1) {
            cidr_format_str.assign(cidr_str + "/" + std::to_string(netmask_len_));
        }
        VLOG_DEBUG << "cidr format str: " << cidr_format_str;

        std::vector<std::string> cidr_items;
        butil::SplitString(cidr_format_str, '/', &cidr_items);
        if (cidr_items.size() != 2) {
            LOG(WARNING) << "wrong CIDR format. network=" << cidr_str;
            return false;
        }

        if (cidr_items[1].empty()) {
            LOG(WARNING) << "wrong CIDR mask format. network=" << cidr_str;
            return false;
        }

        char* endptr = nullptr;
        int32_t mask_length = strtol(cidr_items[1].c_str(), &endptr, 10);
        if (errno != 0 && mask_length == 0) {
            char errmsg[64];
            // Ignore unused return value
            auto ret = strerror_r(errno, errmsg, 64);
            LOG(WARNING) << "wrong CIDR mask format. network=" << cidr_str
                         << ", mask_length=" << mask_length << ", errno=" << errno
                         << ", errmsg=" << errmsg << ", strerror_r returns=" << ret;
            return false;
        }
        if (mask_length <= 0 || mask_length > netmask_len_) {
            LOG(WARNING) << "wrong CIDR mask format. network=" << cidr_str
                         << ", mask_length=" << mask_length;
            return false;
        }
        netmask_len_ = mask_length;
        return true;
    }
    bool contains(const std::string& ip) {
        CIDR ip_cidr;
        if (!ip_cidr.reset(ip)) {
            return false;
        }
        if ((family_ != ip_cidr.family_) || (netmask_len_ > ip_cidr.netmask_len_)) {
            return false;
        }
        auto bytes = netmask_len_ / 8;
        auto cidr_begin = address_.cbegin();
        auto ip_begin = ip_cidr.address_.cbegin();
        if (!std::equal(cidr_begin, cidr_begin + bytes, ip_begin, ip_begin + bytes)) {
            return false;
        }
        if ((netmask_len_ % 8) == 0) {
            return true;
        }
        auto mask = (0xFF << (8 - (netmask_len_ % 8))) & 0xFF;
        return (address_[bytes] & mask) == (ip_cidr.address_[bytes] & mask);
    }

private:
    static constexpr std::uint8_t kIPv4Bits = 32;
    static constexpr std::uint8_t kIPv6Bits = 128;

    sa_family_t family_ = AF_UNSPEC;
    std::array<std::uint8_t, 16> address_;
    int32_t netmask_len_ = 0;
};

class InetAddress {
public:
    InetAddress(std::string ip, sa_family_t family, bool is_loopback)
            : ip_addr_(std::move(ip)), family_(family), is_loopback_(is_loopback) {}
    bool is_loopback() const { return is_loopback_; }
    bool is_ipv6() const { return family_ == AF_INET6; }
    const std::string& get_host_address() const { return ip_addr_; }

private:
    std::string ip_addr_;
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
        if (addr->sa_family == AF_INET) {
            char addr_buf[INET_ADDRSTRLEN];
            auto tmp_addr = &((struct sockaddr_in*)addr)->sin_addr;
            inet_ntop(AF_INET, tmp_addr, addr_buf, INET_ADDRSTRLEN);
            in_addr_t s_addr = ((struct sockaddr_in*)addr)->sin_addr.s_addr;
            bool is_loopback = (ntohl(s_addr) & 0xFF000000) == 0x7F000000;
            hosts->emplace_back(std::string(addr_buf), AF_INET, is_loopback);
        } else if (addr->sa_family == AF_INET6) {
            char addr_buf[INET6_ADDRSTRLEN];
            auto tmp_addr = &((struct sockaddr_in6*)addr)->sin6_addr;
            inet_ntop(AF_INET6, tmp_addr, addr_buf, sizeof(addr_buf));
            bool is_loopback = IN6_IS_ADDR_LOOPBACK(tmp_addr);
            hosts->emplace_back(std::string(addr_buf), AF_INET6, is_loopback);
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
        // Check if ip is loopback, ms/recycler exit
        LOG(INFO) << "get the IP for ms is " << localhost_str;
        if (config::enable_loopback_address_for_ms ||
            (localhost_str != "127.0.0.1" && localhost_str != "::1")) {
            return;
        }
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
            if (cidr.contains(ip)) {
                return true;
            }
        }
        return false;
    };

    std::string loopback;
    std::string matched_loopback;
    localhost_str = "";
    for (auto addr_it = hosts.begin(); addr_it != hosts.end(); ++addr_it) {
        VLOG_DEBUG << "check ip=" << addr_it->get_host_address();
        if ((*addr_it).is_loopback()) {
            // Keep the IPv4 loopback as the fallback when both loopback families are present.
            // A matching loopback is only used if no non-loopback address matches.
            if (!addr_it->is_ipv6() || loopback.empty()) {
                loopback = addr_it->get_host_address();
            }
            if (matched_loopback.empty() && !priority_cidrs.empty() &&
                is_in_prior_network(addr_it->get_host_address())) {
                matched_loopback = addr_it->get_host_address();
            }
        } else if (!priority_cidrs.empty()) {
            if (is_in_prior_network(addr_it->get_host_address())) {
                localhost_str = addr_it->get_host_address();
                break;
            }
        } else {
            localhost_str = addr_it->get_host_address();
            break;
        }
    }
    if (!localhost_str.empty()) {
        LOG(INFO) << "local host ip=" << localhost_str;
        return localhost_str;
    }

    if (!matched_loopback.empty()) {
        localhost_str = matched_loopback;
    } else if (!loopback.empty()) {
        localhost_str = loopback;
    }
    if (!localhost_str.empty()) {
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
    std::stringstream ss;
    if (host.find(':') == std::string::npos) {
        ss << host << ":" << port;
    } else {
        ss << "[" << host << "]"
           << ":" << port;
    }
    return ss.str();
}

} // namespace doris::cloud
