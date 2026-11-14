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
    doris::cloud::config::enable_loopback_address_for_ms = enable_loopback_address_for_ms_bak;
}

TEST(NetWorkUtilTest, GetLocalHostIPv6Test) {
    bool enable_loopback_address_for_ms_bak = doris::cloud::config::enable_loopback_address_for_ms;
    doris::cloud::config::enable_loopback_address_for_ms = true;

    { // IPv6 loopback CIDR selects ::1
        doris::cloud::config::priority_networks = "::1/128";
        ASSERT_EQ(doris::cloud::get_local_ip(doris::cloud::config::priority_networks), "::1");
    }

    { // IPv6 CIDR not matching any interface falls back to the IPv4 loopback
        doris::cloud::config::priority_networks = "fd00:1234:5678::/48";
        ASSERT_EQ(doris::cloud::get_local_ip(doris::cloud::config::priority_networks), "127.0.0.1");
    }

    { // bind address follows the family of the selected address
        ASSERT_EQ(doris::cloud::get_service_bind_address(""), "0.0.0.0");
        ASSERT_EQ(doris::cloud::get_service_bind_address("127.0.0.1/8"), "0.0.0.0");
        ASSERT_EQ(doris::cloud::get_service_bind_address("::1/128"), "[::0]");
    }

    { // endpoint formatting brackets IPv6 literals only
        ASSERT_EQ(doris::cloud::get_host_port("127.0.0.1", 5000), "127.0.0.1:5000");
        ASSERT_EQ(doris::cloud::get_host_port("fd00::1", 5000), "[fd00::1]:5000");
    }

    doris::cloud::config::enable_loopback_address_for_ms = enable_loopback_address_for_ms_bak;
}
