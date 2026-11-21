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

#include "common/config.h"
#include "gtest/gtest.h"

int main(int argc, char** argv) {
    doris::cloud::config::init(nullptr, true);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

TEST(NetWorkUtilTest, GetLocaHostTest) {
    auto priority_networks_bak = doris::cloud::config::priority_networks;
    doris::cloud::config::priority_networks = "";
    bool enable_loopback_address_for_ms_bak = doris::cloud::config::enable_loopback_address_for_ms;
    doris::cloud::config::enable_loopback_address_for_ms = true;
    // prepare an existed ip for test
    auto ip = doris::cloud::get_local_ip(doris::cloud::config::priority_networks);
    std::cout << "get ip: " << ip << " from butil::my_ip_cstr()" << std::endl;

    { // possible not match CIDR
        doris::cloud::config::priority_networks = "127.0.0.1/8";
        ASSERT_EQ(doris::cloud::get_local_ip(doris::cloud::config::priority_networks), "127.0.0.1");
    }

    { // not match CIDR
        doris::cloud::config::priority_networks = "1.2.3.4/8";
        ASSERT_EQ(doris::cloud::get_local_ip(doris::cloud::config::priority_networks), "127.0.0.1");
    }

    { // must match CIDR
        doris::cloud::config::priority_networks = ip + "/16";
        ASSERT_EQ(doris::cloud::get_local_ip(doris::cloud::config::priority_networks), ip);
    }
    doris::cloud::config::priority_networks = priority_networks_bak;
    doris::cloud::config::enable_loopback_address_for_ms = enable_loopback_address_for_ms_bak;
}

TEST(NetWorkUtilTest, GetIpv6LocalHostTest) {
    auto priority_networks_bak = doris::cloud::config::priority_networks;
    bool enable_loopback_address_for_ms_bak = doris::cloud::config::enable_loopback_address_for_ms;
    doris::cloud::config::enable_loopback_address_for_ms = true;

    doris::cloud::config::priority_networks = "::1/128";
    ASSERT_EQ(doris::cloud::get_local_ip(doris::cloud::config::priority_networks), "::1");
    ASSERT_EQ(doris::cloud::get_service_bind_address(doris::cloud::config::priority_networks),
              "[::0]");

    doris::cloud::config::priority_networks = "fd00:1234:5678::/48";
    ASSERT_EQ(doris::cloud::get_local_ip(doris::cloud::config::priority_networks), "127.0.0.1");

    doris::cloud::config::priority_networks = priority_networks_bak;
    doris::cloud::config::enable_loopback_address_for_ms = enable_loopback_address_for_ms_bak;
}

TEST(NetWorkUtilTest, GetServiceBindAddressTest) {
    bool enable_loopback_address_for_ms_bak = doris::cloud::config::enable_loopback_address_for_ms;
    doris::cloud::config::enable_loopback_address_for_ms = true;
    ASSERT_EQ(doris::cloud::get_service_bind_address(""), "0.0.0.0");
    ASSERT_EQ(doris::cloud::get_service_bind_address("127.0.0.1/8"), "0.0.0.0");
    ASSERT_EQ(doris::cloud::get_service_bind_address("::1/128"), "[::0]");
    doris::cloud::config::enable_loopback_address_for_ms = enable_loopback_address_for_ms_bak;
}

TEST(NetWorkUtilTest, PreferNonLoopbackAddressTest) {
    auto priority_networks_bak = doris::cloud::config::priority_networks;
    bool enable_loopback_address_for_ms_bak = doris::cloud::config::enable_loopback_address_for_ms;
    doris::cloud::config::enable_loopback_address_for_ms = true;
    doris::cloud::config::priority_networks = "";
    const auto host_ip = doris::cloud::get_local_ip(doris::cloud::config::priority_networks);

    doris::cloud::config::priority_networks = "127.0.0.0/8;" + host_ip + "/32";
    ASSERT_EQ(doris::cloud::get_local_ip(doris::cloud::config::priority_networks), host_ip);
    ASSERT_EQ(doris::cloud::get_service_bind_address(doris::cloud::config::priority_networks),
              "0.0.0.0");

    doris::cloud::config::priority_networks = "::1/128;" + host_ip + "/32";
    ASSERT_EQ(doris::cloud::get_local_ip(doris::cloud::config::priority_networks), host_ip);
    ASSERT_EQ(doris::cloud::get_service_bind_address(doris::cloud::config::priority_networks),
              "0.0.0.0");

    doris::cloud::config::priority_networks = priority_networks_bak;
    doris::cloud::config::enable_loopback_address_for_ms = enable_loopback_address_for_ms_bak;
}

TEST(NetWorkUtilTest, GetHostPortTest) {
    ASSERT_EQ(doris::cloud::get_host_port("10.0.0.1", 5000), "10.0.0.1:5000");
    ASSERT_EQ(doris::cloud::get_host_port("2001:db8::1", 5000), "[2001:db8::1]:5000");
    ASSERT_EQ(doris::cloud::get_host_port("ms-1", 5000), "ms-1:5000");
}
