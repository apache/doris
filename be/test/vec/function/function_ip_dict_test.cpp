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

#include "function_ip_dict_test.h"

#include <memory>
#include <random>
#include <sstream>
#include <type_traits>
#include <vector>

#include "function_test_util.h"
#include "vec/columns/column_string.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_ipv4.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/ip_address_dictionary.h"

namespace doris::vectorized {

TEST(IpDictTest, TestIpv4) {
    std::vector<std::string> ips = {
            "192.168.0.0/16",     "192.168.1.0/24",  "192.168.1.128/25", "1:288:2080::/41",
            "10.0.0.0/8",         "10.1.0.0/16",     "172.16.0.0/12",    "172.16.1.0/24",
            "172.16.1.128/25",    "203.0.113.0/24",  "198.51.100.0/24",  "2001:db8::/32",
            "2001:db8:abcd::/48", "fc00::/7",        "fe80::/10",        "192.0.2.0/24",
            "198.18.0.0/15",      "198.51.100.0/24", "203.0.113.0/24",   "2400:cb00::/32",
            "192.168.2.0/24",     "192.168.3.0/24",  "10.2.0.0/16",      "172.17.0.0/16",
            "172.18.0.0/16",      "203.0.114.0/24",  "198.51.101.0/24",  "2001:db8:abcd:1::/64"};

    std::vector<std::string> ipv4_string = {
            "192.168.1.1", "10.1.1.1",   "172.16.1.1",    "198.51.100.1", "203.0.113.1",
            "192.0.2.1",   "198.18.1.1", "203.0.113.2",   "198.51.100.2", "192.168.0.1",
            "10.0.0.1",    "172.16.0.1", "192.168.1.129", "10.1.0.1",     "192.168.2.1",
            "192.168.3.1", "10.2.0.1",   "172.17.0.1",    "172.18.0.1",   "203.0.114.1",
            "198.51.101.1"};
    test_for_ip_type<DataTypeIPv4, true>(ips, ipv4_string);
}

TEST(IpDictTest, TestIpv6) {
    std::vector<std::string> ips = {"2001:db8::/32",
                                    "2001:db8:abcd::/48",
                                    "fc00::/7",
                                    "fe80::/10",
                                    "2400:cb00::/32",
                                    "2001:0db8:85a3::/64",
                                    "2001:0db8:85a3:0000:0000:8a2e:0370:7334/128",
                                    "2001:0db8:0000:0042:0000:8a2e:0370:7334/128",
                                    "2001:0db8:0000:0042:0000:8a2e:0370:7335/128",
                                    "2001:0db8:0000:0042:0000:8a2e:0370:7336/128",
                                    "2001:db8:abcd:1::/64",
                                    "2001:db8:abcd:2::/64",
                                    "fc00:1::/64",
                                    "fe80:1::/64"};

    std::vector<std::string> ipv6_string = {"2001:db8::1",
                                            "2001:db8:abcd::1",
                                            "fc00::1",
                                            "fe80::1",
                                            "2400:cb00::1",
                                            "2001:0db8:85a3::1",
                                            "2001:0db8:85a3:0000:0000:8a2e:0370:7334",
                                            "2001:0db8:0000:0042:0000:8a2e:0370:7334",
                                            "2001:0db8:0000:0042:0000:8a2e:0370:7335",
                                            "2001:0db8:0000:0042:0000:8a2e:0370:7336",
                                            "2001:db8:abcd:1::1",
                                            "2001:db8:abcd:2::1",
                                            "fc00:1::1",
                                            "fe80:1::1"};

    test_for_ip_type<DataTypeIPv6, true>(ips, ipv6_string);
}

std::string generate_random_ipv4() {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, 255);
    std::stringstream ss;
    ss << dis(gen) << '.' << dis(gen) << '.' << dis(gen) << '.' << dis(gen);
    return ss.str();
}

std::string generate_random_ipv6() {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, 65535);
    std::stringstream ss;
    for (int i = 0; i < 8; ++i) {
        ss << std::hex << dis(gen);
        if (i != 7) {
            ss << ':';
        }
    }
    return ss.str();
}

std::string generate_random_cidr_ipv4(std::unordered_set<std::string>& existing_cidrs) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis_prefix(1, 4); // 1到4之间的整数
    std::uniform_int_distribution<> dis(0, 255);
    std::string cidr;
    do {
        int prefix_length = dis_prefix(gen) * 8; // 8的倍数
        int mask = ~((1 << (32 - prefix_length)) - 1);
        int ip = (dis(gen) << 24) | (dis(gen) << 16) | (dis(gen) << 8) | dis(gen);
        ip &= mask;
        std::stringstream ss;
        ss << ((ip >> 24) & 0xFF) << '.' << ((ip >> 16) & 0xFF) << '.' << ((ip >> 8) & 0xFF) << '.'
           << (ip & 0xFF);
        cidr = ss.str() + "/" + std::to_string(prefix_length);
    } while (existing_cidrs.find(cidr) != existing_cidrs.end());
    existing_cidrs.insert(cidr);
    return cidr;
}

std::string generate_random_cidr_ipv6(std::unordered_set<std::string>& existing_cidrs) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis_prefix(1, 8); // 1到8之间的整数
    std::uniform_int_distribution<> dis(0, 65535);
    std::string cidr;
    do {
        int prefix_length = dis_prefix(gen) * 16; // 16的倍数
        std::stringstream ss;
        for (int i = 0; i < 8; ++i) {
            if (i * 16 < prefix_length) {
                ss << std::hex << dis(gen);
            } else {
                ss << "0";
            }
            if (i != 7) {
                ss << ':';
            }
        }
        cidr = ss.str() + "/" + std::to_string(prefix_length);
    } while (existing_cidrs.find(cidr) != existing_cidrs.end());
    existing_cidrs.insert(cidr);
    return cidr;
}

TEST(IpDictTest, RandomIpv4) {
    std::unordered_set<std::string> existing_cidrs;
    std::vector<std::string> ips;
    std::vector<std::string> ipv4_string;
    for (int i = 0; i < 1000; ++i) {
        ips.push_back(generate_random_cidr_ipv4(existing_cidrs));
        ipv4_string.push_back(generate_random_ipv4());
    }
    test_for_ip_type<DataTypeIPv4, false>(ips, ipv4_string);
}

TEST(IpDictTest, RandomIpv6) {
    std::unordered_set<std::string> existing_cidrs;
    std::vector<std::string> ips;
    std::vector<std::string> ipv6_string;
    for (int i = 0; i < 1000; ++i) {
        ips.push_back(generate_random_cidr_ipv6(existing_cidrs));
        ipv6_string.push_back(generate_random_ipv6());
    }
    test_for_ip_type<DataTypeIPv6, false>(ips, ipv6_string);
}

} // namespace doris::vectorized
