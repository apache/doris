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

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <cstdint>
#include <iostream>

#include "gtest/gtest_pred_impl.h"
#include "vec/common/ipv6_to_binary.h"
#include "vec/runtime/ipv4_value.h"
#include "vec/runtime/ipv6_value.h"

namespace doris {

class IPv4Value;
class IPv6Value;

// util function
template <typename T>
static void print_bytes(T num) {
    auto* byte_ptr = reinterpret_cast<uint8_t*>(&num);

    std::cout << "low -> ";

    for (size_t i = 0; i < sizeof(T); ++i) {
        std::cout << std::hex << static_cast<int>(byte_ptr[i]) << " ";
    }

    std::cout << " -> high";
    std::cout << std::dec << std::endl;
}

TEST(IPValueTest, IPv4ValueTest) {
    const std::string ipv4_str1 = "192.168.103.254";
    const std::string ipv4_str2 = "193.168.103.255";
    vectorized::IPv4 ipv4_val1;
    vectorized::IPv4 ipv4_val2;
    ASSERT_TRUE(IPv4Value::from_string(ipv4_val1, ipv4_str1.c_str(), ipv4_str1.size()));
    ASSERT_TRUE(IPv4Value::from_string(ipv4_val2, ipv4_str2.c_str(), ipv4_str2.size()));
    ASSERT_TRUE(ipv4_val1 < ipv4_val2);
    print_bytes(ipv4_val1);
    print_bytes(ipv4_val2);
    std::string ipv4_format1 = IPv4Value::to_string(ipv4_val1);
    std::string ipv4_format2 = IPv4Value::to_string(ipv4_val2);
    ASSERT_EQ(ipv4_str1, ipv4_format1);
    ASSERT_EQ(ipv4_str2, ipv4_format2);
}

TEST(IPValueTest, IPv6ValueTest) {
    const std::string ipv6_str1 = "2001:418:0:5000::c2d";
    const std::string ipv6_str2 = "2001:428::205:171:200:230";
    vectorized::IPv6 ipv6_val1;
    vectorized::IPv6 ipv6_val2;
    ASSERT_TRUE(IPv6Value::from_string(ipv6_val1, ipv6_str1.c_str(), ipv6_str1.size()));
    ASSERT_TRUE(IPv6Value::from_string(ipv6_val2, ipv6_str2.c_str(), ipv6_str2.size()));
    ASSERT_TRUE(ipv6_val1 < ipv6_val2);
    print_bytes(ipv6_val1);
    print_bytes(ipv6_val2);
    std::string ipv6_format1 = IPv6Value::to_string(ipv6_val1);
    std::string ipv6_format2 = IPv6Value::to_string(ipv6_val2);
    ASSERT_EQ(ipv6_str1, ipv6_format1);
    ASSERT_EQ(ipv6_str2, ipv6_format2);
}

static void apply_cidr_mask(const char* __restrict src, char* __restrict dst_lower,
                            char* __restrict dst_upper, vectorized::UInt8 bits_to_keep) {
    const auto& mask = vectorized::get_cidr_mask_ipv6(bits_to_keep);

    for (int8_t i = IPV6_BINARY_LENGTH - 1; i >= 0; --i) {
        dst_lower[i] = src[i] & mask[i];
        dst_upper[i] = dst_lower[i] | ~mask[i];
    }
}

TEST(IPValueTest, IPv6CIDRTest) {
    const std::string ipv6_str1 = "2001:0db8:0000:85a3:0000:0000:ac1f:8001";
    const std::string ipv6_str2 = "2001:0db8:0000:85a3:ffff:ffff:ffff:ffff";
    vectorized::IPv6 ipv6_val1; // little-endian
    vectorized::IPv6 ipv6_val2; // little-endian
    ASSERT_TRUE(IPv6Value::from_string(ipv6_val1, ipv6_str1.c_str(), ipv6_str1.size()));
    ASSERT_TRUE(IPv6Value::from_string(ipv6_val2, ipv6_str2.c_str(), ipv6_str2.size()));
    vectorized::IPv6 min_range1, max_range1;
    vectorized::IPv6 min_range2, max_range2;
    apply_cidr_mask(reinterpret_cast<const char*>(&ipv6_val1), reinterpret_cast<char*>(&min_range1),
                    reinterpret_cast<char*>(&max_range1), 0);
    apply_cidr_mask(reinterpret_cast<const char*>(&ipv6_val2), reinterpret_cast<char*>(&min_range2),
                    reinterpret_cast<char*>(&max_range2), 32);
    print_bytes(min_range1);
    print_bytes(max_range1);
    print_bytes(min_range2);
    print_bytes(max_range2);
    std::string min_range_format1 = IPv6Value::to_string(min_range1);
    std::string max_range_format1 = IPv6Value::to_string(max_range1);
    std::string min_range_format2 = IPv6Value::to_string(min_range2);
    std::string max_range_format2 = IPv6Value::to_string(max_range2);
    ASSERT_EQ(min_range_format1, "::");
    ASSERT_EQ(max_range_format1, "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff");
    ASSERT_EQ(min_range_format2, "2001:db8::");
    ASSERT_EQ(max_range_format2, "2001:db8:ffff:ffff:ffff:ffff:ffff:ffff");
}

} // namespace doris