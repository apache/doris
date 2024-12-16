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

#include <sstream>
#include <vector>

#include "common/logging.h"

namespace doris::cloud {

class CIDR {
public:
    CIDR() : address_(0), netmask_(0xffffffff) {}
    bool reset(const std::string& cidr_str) {
        address_ = 0;
        netmask_ = 0xffffffff;

        // check if have mask
        std::string cidr_format_str = cidr_str;
        int32_t have_mask = cidr_str.find("/");
        if (have_mask == -1) {
            cidr_format_str.assign(cidr_str + "/32");
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
        if (mask_length <= 0 || mask_length > 32) {
            LOG(WARNING) << "wrong CIDR mask format. network=" << cidr_str
                         << ", mask_length=" << mask_length;
            return false;
        }

        uint32_t address = 0;
        if (!ip_to_int(cidr_items[0], &address)) {
            LOG(WARNING) << "wrong CIDR IP value. network=" << cidr_str;
            return false;
        }
        address_ = address;

        netmask_ = 0xffffffff;
        netmask_ = netmask_ << (32 - mask_length);
        return true;
    }
    bool contains(const std::string& ip) {
        uint32_t ip_int = 0;
        if (!ip_to_int(ip, &ip_int)) {
            return false;
        }
        if ((address_ & netmask_) == (ip_int & netmask_)) {
            return true;
        }
        return false;
    }

private:
    bool ip_to_int(const std::string& ip_str, uint32_t* value) {
        struct in_addr addr;
        int flag = inet_aton(ip_str.c_str(), &addr);
        if (flag == 0) {
            return false;
        }
        *value = ntohl(addr.s_addr);
        return true;
    }

    uint32_t address_;
    uint32_t netmask_;
};

class InetAddress {
public:
    InetAddress(struct sockaddr* addr) { this->addr_ = *(struct sockaddr_in*)addr; }
    bool is_address_v4() const { return addr_.sin_family == AF_INET; }
    bool is_loopback_v4() const {
        in_addr_t s_addr = addr_.sin_addr.s_addr;
        return (ntohl(s_addr) & 0xFF000000) == 0x7F000000;
    }
    std::string get_host_address_v4() {
        char addr_buf[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &(addr_.sin_addr), addr_buf, INET_ADDRSTRLEN);
        return std::string(addr_buf);
    }

private:
    struct sockaddr_in addr_;
};

static bool get_hosts_v4(std::vector<InetAddress>* hosts) {
    ifaddrs* if_addrs = nullptr;
    if (getifaddrs(&if_addrs)) {
        std::stringstream ss;
        char buf[64];
        LOG(FATAL) << "getifaddrs failed because " << strerror_r(errno, buf, sizeof(buf));
        return false;
    }

    for (ifaddrs* if_addr = if_addrs; if_addr != nullptr; if_addr = if_addr->ifa_next) {
        if (!if_addr->ifa_addr) {
            continue;
        }
        if (if_addr->ifa_addr->sa_family == AF_INET) { // check it is IP4
            // is a valid IP4 Address
            hosts->emplace_back(if_addr->ifa_addr);
        }
    }

    if (if_addrs != nullptr) {
        freeifaddrs(if_addrs);
    }

    return true;
}

std::string get_local_ip(const std::string& priority_networks) {
    std::string localhost_str = butil::my_ip_cstr();
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
    if (!get_hosts_v4(&hosts)) {
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
    localhost_str = "";
    for (auto addr_it = hosts.begin(); addr_it != hosts.end(); ++addr_it) {
        if ((*addr_it).is_address_v4()) {
            VLOG_DEBUG << "check ip=" << addr_it->get_host_address_v4();
            if ((*addr_it).is_loopback_v4()) {
                loopback = addr_it->get_host_address_v4();
            } else if (!priority_cidrs.empty()) {
                if (is_in_prior_network(addr_it->get_host_address_v4())) {
                    localhost_str = addr_it->get_host_address_v4();

                    break;
                }
            } else {
                localhost_str = addr_it->get_host_address_v4();
                break;
            }
        }
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

} // namespace doris::cloud
