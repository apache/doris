
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

#include <algorithm>
#include <string>
#include <vector>

#include "vec/common/assert_cast.h"
#include "vec/core/columns_with_type_and_name.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/dictionary.h"
#include "vec/functions/ip_address_dictionary.h"
#include "vec/runtime/ip_address_cidr.h"

namespace doris::vectorized {

struct IPRecord {
    IPAddressCIDR ip_with_cidr;
    size_t row;
    IPv6 to_ipv6() const {
        if (const auto* address = ip_with_cidr._address.as_v6()) {
            IPv6 ipv6;
            memcpy(reinterpret_cast<UInt8*>(&ipv6), address, sizeof(IPv6));
            return ipv6;
        }

        return ipv4_to_ipv6(ip_with_cidr._address.as_v4());
    }
    UInt8 prefix() const {
        if (ip_with_cidr._address.as_v6()) {
            return ip_with_cidr._prefix;
        }
        return ip_with_cidr._prefix + 96;
    }
};

class MockIPAddressDictionary : public IDictionary {
public:
    MockIPAddressDictionary(std::string name, std::vector<DictionaryAttribute> attributes)
            : IDictionary(std::move(name), std::move(attributes)) {}

    ColumnPtr get_column(const std::string& attribute_name, const DataTypePtr& attribute_type,
                         const ColumnPtr& key_column, const DataTypePtr& key_type) const override {
        MutableColumnPtr res_column = attribute_type->create_column();
        const auto& attribute = _values_data[attribute_index(attribute_name)];

        if (key_type->get_primitive_type() == TYPE_IPV6) {
            const auto* ipv6_column = assert_cast<const ColumnIPv6*>(key_column.get());

            std::visit(
                    [&](auto&& arg) {
                        using ValueDataType = std::decay_t<decltype(arg)>;

                        using AttributeRealColumnType = ValueDataType::OutputColumnType;

                        auto* res_real_column =
                                assert_cast<AttributeRealColumnType*>(res_column.get());
                        const auto* attributes_column = arg.get();

                        for (size_t i = 0; i < ipv6_column->size(); i++) {
                            IPv6 ipv6 = ipv6_column->get_element(i);
                            auto it = lookupIP(ipv6);
                            if (it == ip_not_found()) {
                                res_column->insert_default();
                            } else {
                                const auto idx = it->row;
                                res_real_column->insert_value(attributes_column->get_element(idx));
                            }
                        }
                    },
                    attribute);
        } else {
            const auto* ipv4_column = assert_cast<const ColumnIPv4*>(key_column.get());
            std::visit(
                    [&](auto&& arg) {
                        using ValueDataType = std::decay_t<decltype(arg)>;

                        using AttributeRealColumnType = ValueDataType::OutputColumnType;

                        auto* res_real_column =
                                assert_cast<AttributeRealColumnType*>(res_column.get());
                        const auto* attributes_column = arg.get();

                        for (size_t i = 0; i < ipv4_column->size(); i++) {
                            IPv4 ipv4 = ipv4_column->get_element(i);
                            IPv6 ipv6 = ipv4_to_ipv6(ipv4);
                            auto it = lookupIP(ipv6);
                            if (it == ip_not_found()) {
                                res_column->insert_default();
                            } else {
                                const auto idx = it->row;
                                res_real_column->insert_value(attributes_column->get_element(idx));
                            }
                        }
                    },
                    attribute);
        }

        return res_column;
    }

    static DictionaryPtr create_ip_trie_dict(const std::string& name, ColumnPtr& key_column,
                                             ColumnsWithTypeAndName& attribute_data) {
        std::vector<DictionaryAttribute> attributes;
        std::vector<ColumnPtr> attributes_column;
        for (const auto& att : attribute_data) {
            attributes.push_back({att.name, att.type});
            attributes_column.push_back(att.column);
        }
        auto dict = std::make_shared<MockIPAddressDictionary>(name, attributes);
        dict->load_data(key_column, attributes_column);
        return dict;
    }

    void load_data(ColumnPtr& key_column, std::vector<ColumnPtr>& attributes_column) {
        const auto* str_column = assert_cast<const ColumnString*>(key_column.get());
        for (size_t i = 0; i < str_column->size(); i++) {
            auto ip_str = str_column->get_element(i);
            ip_records.push_back(IPRecord {parse_ip_with_cidr(ip_str), i});
        }
        std::sort(ip_records.begin(), ip_records.end(),
                  [&](const IPRecord& a, const IPRecord& b) { return a.prefix() > b.prefix(); });

        load_values(attributes_column);
    }

    using RowIdxConstIter = std::vector<IPRecord>::const_iterator;
    RowIdxConstIter lookupIP(IPv6 target) const {
        for (auto it = ip_records.begin(); it != ip_records.end(); it++) {
            IPRecord ip = *it;
            auto ipv6 = ip.to_ipv6();
            if (match_ipv6_subnet(reinterpret_cast<const UInt8*>(&target),
                                  reinterpret_cast<const UInt8*>(&ipv6), ip.prefix())) {
                return it;
            }
        }
        return ip_not_found();
    }

    RowIdxConstIter ip_not_found() const { return ip_records.end(); }

    std::vector<IPRecord> ip_records;
};

inline DictionaryPtr create_mock_ip_trie_dict_from_column(const std::string& name,
                                                          ColumnWithTypeAndName key_data,
                                                          ColumnsWithTypeAndName attribute_data) {
    auto key_column = key_data.column;
    auto key_type = key_data.type;
    if (!is_string_type(key_type->get_primitive_type())) {
        throw doris::Exception(
                ErrorCode::INVALID_ARGUMENT,
                "IPAddressDictionary only support string in key , input key type is {} ",
                key_type->get_name());
    }

    for (auto col_type_name : attribute_data) {
        if (col_type_name.type->is_nullable() || col_type_name.column->is_nullable()) {
            throw doris::Exception(
                    ErrorCode::INVALID_ARGUMENT,
                    "IPAddressDictionary only support nullable attribute , input attribute is {} ",
                    col_type_name.type->get_name());
        }
    }

    DictionaryPtr dict =
            MockIPAddressDictionary::create_ip_trie_dict(name, key_column, attribute_data);
    return dict;
}

template <typename IPType, bool output>
void test_for_ip_type(std::vector<std::string> ips, std::vector<std::string> ip_string) {
    static_assert(std::is_same_v<IPType, DataTypeIPv4> || std::is_same_v<IPType, DataTypeIPv6>,
                  "IPType must be either DataTypeIPv4 or DataTypeIPv6");
    std::cout << "input data size\t" << ips.size() << "\t" << ip_string.size() << "\n";
    auto input_key_column = DataTypeString::ColumnType::create();
    auto intput_key_data = std::make_shared<DataTypeString>();

    auto value_column = DataTypeInt64::ColumnType::create();
    auto value_type = std::make_shared<DataTypeInt64>();

    for (int i = 0; i < ips.size(); i++) {
        input_key_column->insert_value(ips[i]);
        value_column->insert_value(i);
    }

    auto mock_ip_dict = create_mock_ip_trie_dict_from_column(
            "mock ip dict", ColumnWithTypeAndName {input_key_column->clone(), intput_key_data, ""},
            ColumnsWithTypeAndName {
                    ColumnWithTypeAndName {value_column->clone(), value_type, "row"},
            });
    auto ip_dict = create_ip_trie_dict_from_column(
            "ip dict", ColumnWithTypeAndName {input_key_column->clone(), intput_key_data, ""},
            ColumnsWithTypeAndName {
                    ColumnWithTypeAndName {value_column->clone(), value_type, "row"},
            });

    std::string attribute_name = "row";
    DataTypePtr attribute_type = value_type;

    {
        auto key_type = std::make_shared<IPType>();
        auto ipv_column = IPType::ColumnType::create();
        for (const auto& ip : ip_string) {
            if constexpr (std::is_same_v<IPType, DataTypeIPv4>) {
                IPv4 ipv4;
                EXPECT_TRUE(IPv4Value::from_string(ipv4, ip));
                ipv_column->insert_value(ipv4);
            } else {
                IPv6 ipv6;
                EXPECT_TRUE(IPv6Value::from_string(ipv6, ip));
                ipv_column->insert_value(ipv6);
            }
        }

        ColumnPtr key_column = ipv_column->clone();
        auto mock_result =
                mock_ip_dict->get_column(attribute_name, attribute_type, key_column, key_type);
        auto result = ip_dict->get_column(attribute_name, attribute_type, key_column, key_type);

        const auto* real_mock_result = assert_cast<const ColumnInt64*>(mock_result.get());
        const auto* real_result = assert_cast<const ColumnInt64*>(remove_nullable(result).get());
        for (int i = 0; i < ip_string.size(); i++) {
            if constexpr (output) {
                std::cout << ip_string[i] << "\t" << ips[real_mock_result->get_element(i)] << "\t"
                          << ips[real_result->get_element(i)] << "\n";
            }
            EXPECT_EQ(ips[real_mock_result->get_element(i)], ips[real_result->get_element(i)]);
        }
    }
}
} // namespace doris::vectorized
