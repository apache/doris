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
#include <vector>

#include "function_test_util.h"
#include "runtime/primitive_type.h"
#include "vec/columns/column_string.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_ipv4.h"
#include "vec/functions/ip_address_dictionary.h"

namespace doris::vectorized {

TEST(IpDictTest, TestIpv4) {
    std::vector<std::string> ips = {
            "192.168.1.0/24",  "192.168.1.128/25",    "1:288:2080::/41",    "10.1.0.0/16",
            "172.16.0.0/12",   "172.16.1.0/24",       "172.16.1.128/25",    "203.0.113.0/24",
            "198.51.100.0/24", "2001:db8::/32",       "2001:db8:abcd::/48", "fc00::/7",
            "fe80::/10",       "192.0.2.0/24",        "172.18.0.0/16",      "203.0.114.0/24",
            "198.51.101.0/24", "2001:db8:abcd:1::/64"};

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

TEST(IpDictTest, String64) {
    auto input_key_column = ColumnString64::create();
    auto intput_key_data = std::make_shared<DataTypeString>();

    auto value_column = ColumnString64::create();
    auto value_type = std::make_shared<DataTypeString>();

    std::vector<std::string> ips = {
            "192.168.1.0/24",  "192.168.1.128/25",    "1:288:2080::/41",    "10.1.0.0/16",
            "172.16.0.0/12",   "172.16.1.0/24",       "172.16.1.128/25",    "203.0.113.0/24",
            "198.51.100.0/24", "2001:db8::/32",       "2001:db8:abcd::/48", "fc00::/7",
            "fe80::/10",       "192.0.2.0/24",        "172.18.0.0/16",      "203.0.114.0/24",
            "198.51.101.0/24", "2001:db8:abcd:1::/64"};

    for (const auto& ip : ips) {
        input_key_column->insert_value(ip);
        value_column->insert_value(ip);
    }

    auto ip_dict = create_ip_trie_dict_from_column(
            "ip dict", ColumnWithTypeAndName {input_key_column->clone(), intput_key_data, ""},
            ColumnsWithTypeAndName {
                    ColumnWithTypeAndName {value_column->clone(), value_type, "row"},
            });

    std::vector<std::string> ipv4_string = {
            "192.168.1.1", "10.1.1.1",   "172.16.1.1",    "198.51.100.1", "203.0.113.1",
            "192.0.2.1",   "198.18.1.1", "203.0.113.2",   "198.51.100.2", "192.168.0.1",
            "10.0.0.1",    "172.16.0.1", "192.168.1.129", "10.1.0.1",     "192.168.2.1",
            "192.168.3.1", "10.2.0.1",   "172.17.0.1",    "172.18.0.1",   "203.0.114.1",
            "198.51.101.1"};

    auto key_type = std::make_shared<DataTypeIPv4>();
    auto ipv_column = DataTypeIPv4::ColumnType::create();
    for (const auto& ip : ipv4_string) {
        IPv4 ipv4;
        EXPECT_TRUE(IPv4Value::from_string(ipv4, ip));
        ipv_column->insert_value(ipv4);
    }

    ColumnPtr key_column = ipv_column->clone();

    std::string attribute_name = "row";
    DataTypePtr attribute_type = value_type;
    auto result = ip_dict->get_column(attribute_name, attribute_type, key_column, key_type);

    std::cout << result->size() << std::endl;

    try {
        auto int_column = ColumnInt64::create();
        auto int_key_data = std::make_shared<DataTypeInt64>();
        auto ip_dict = create_ip_trie_dict_from_column(
                "ip dict", ColumnWithTypeAndName {int_column->clone(), int_key_data, ""},
                ColumnsWithTypeAndName {
                        ColumnWithTypeAndName {int_column->clone(), int_key_data, "row"},
                });
        ASSERT_TRUE(false);
    } catch (const Exception& e) {
        ASSERT_EQ(e.code(), ErrorCode::INVALID_ARGUMENT);
    }
}

} // namespace doris::vectorized
