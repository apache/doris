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

#include "service/backend_options.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include "gtest/gtest_pred_impl.h"
#include "util/cidr.h"

namespace doris {

class BackendOptionsTest : public testing::Test {
public:
    BackendOptionsTest() {}
    virtual ~BackendOptionsTest() {}
};

// only loopback
TEST_F(BackendOptionsTest, emptyCidr1) {
    std::vector<InetAddress> hosts;
    hosts.emplace_back(std::string("127.0.0.1"), AF_INET, true);

    std::vector<CIDR> cidrs;
    BackendOptions::analyze_priority_cidrs("", &cidrs);
    std::string localhost;
    bool bind_ipv6 = false;
    BackendOptions::analyze_localhost(localhost, bind_ipv6, &cidrs, &hosts);
    EXPECT_STREQ("127.0.0.1", localhost.c_str());
}

// priority not loopback
TEST_F(BackendOptionsTest, emptyCidr2) {
    std::vector<InetAddress> hosts;
    hosts.emplace_back(std::string("127.0.0.1"), AF_INET, true);
    hosts.emplace_back(std::string("10.10.10.10"), AF_INET, false);
    hosts.emplace_back(std::string("10.10.10.11"), AF_INET, false);

    std::vector<CIDR> cidrs;
    BackendOptions::analyze_priority_cidrs("", &cidrs);
    std::string localhost;
    bool bind_ipv6 = false;
    BackendOptions::analyze_localhost(localhost, bind_ipv6, &cidrs, &hosts);
    EXPECT_STREQ("10.10.10.10", localhost.c_str());
}

// not choose ipv6
TEST_F(BackendOptionsTest, emptyCidr3) {
    std::vector<InetAddress> hosts;
    hosts.emplace_back(std::string("127.0.0.1"), AF_INET, true);
    hosts.emplace_back(std::string("fe80::5054:ff:fec9:dee0"), AF_INET6, false);
    hosts.emplace_back(std::string("10.10.10.10"), AF_INET, false);
    hosts.emplace_back(std::string("10.10.10.11"), AF_INET, false);

    std::vector<CIDR> cidrs;
    BackendOptions::analyze_priority_cidrs("", &cidrs);
    std::string localhost;
    bool bind_ipv6 = false;
    BackendOptions::analyze_localhost(localhost, bind_ipv6, &cidrs, &hosts);
    EXPECT_STREQ("10.10.10.10", localhost.c_str());
}

} // namespace doris
