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

#include <gtest/gtest.h>

#include "vec/core/types.h"
#include "vec/functions/dictionary_factory.h"
#include "vec/functions/ip_address_dictionary.h"
#include "vec/runtime/ip_address_cidr.h"
#include "vec/runtime/ipv4_value.h"
#include "vec/runtime/ipv6_value.h"

namespace doris::vectorized {

IPv6 get_ipv6_from_ipv4(std::string ipv4) {
    IPv4Value ipv4_value;
    EXPECT_TRUE(ipv4_value.from_string(ipv4));
    return ipv4_to_ipv6(ipv4_value.value());
}

IPv6 get_ipv6_from_cidr(std::string ip_str) {
    auto cidr = parse_ip_with_cidr(ip_str);
    auto ipv6 = ipv4_to_ipv6(cidr._address.as_v4());
    return ipv6;
}

IPv6 get_ipv6_from_cidr_format(std::string ip_str) {
    auto cidr = parse_ip_with_cidr(ip_str);
    auto ipv6 = ipv4_to_ipv6(cidr._address.as_v4());
    return IPAddressDictionary::format_ipv6_cidr(reinterpret_cast<const UInt8*>(&ipv6),
                                                 cidr._prefix + 96);
}

TEST(DictionaryIpTest, test) {
    EXPECT_EQ(IPv6Value {get_ipv6_from_cidr("192.1.1.1/24")}.to_string(), "::ffff:192.1.1.1");
    EXPECT_EQ(IPv6Value {get_ipv6_from_cidr_format("192.1.1.1/24")}.to_string(),
              "::ffff:192.1.1.0");

    EXPECT_EQ(IPv6Value {get_ipv6_from_cidr("192.1.255.1/22")}.to_string(), "::ffff:192.1.255.1");
    EXPECT_EQ(IPv6Value {get_ipv6_from_cidr_format("192.1.255.1/22")}.to_string(),
              "::ffff:192.1.252.0");

    EXPECT_EQ(IPv6Value {get_ipv6_from_cidr("192.1.255.1/20")}.to_string(), "::ffff:192.1.255.1");
    EXPECT_EQ(IPv6Value {get_ipv6_from_cidr_format("192.1.255.1/20")}.to_string(),
              "::ffff:192.1.240.0");
}

TEST(DictionaryIpTest, testEQ) {
    EXPECT_EQ(get_ipv6_from_cidr("192.1.1.1/24"), get_ipv6_from_ipv4("192.1.1.1"));

    EXPECT_NE(get_ipv6_from_cidr_format("192.1.1.1/24"), get_ipv6_from_ipv4("192.1.1.1"));

    EXPECT_EQ(get_ipv6_from_cidr("192.1.255.1/22"), get_ipv6_from_ipv4("192.1.255.1"));

    EXPECT_NE(get_ipv6_from_cidr_format("192.1.255.1/22"), get_ipv6_from_ipv4("192.1.255.1"));

    EXPECT_EQ(get_ipv6_from_cidr("192.1.255.1/20"), get_ipv6_from_ipv4("192.1.255.1"));

    EXPECT_NE(get_ipv6_from_cidr_format("192.1.255.1/20"), get_ipv6_from_ipv4("192.1.255.1"));
}

} // namespace doris::vectorized
