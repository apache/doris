
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

#include <algorithm>
#include <cstdint>
#include <iomanip>
#include <string>
#include <vector>

#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/core/columns_with_type_and_name.h"
#include "vec/functions/dictionary.h"
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

    ColumnPtr getColumn(const std::string& attribute_name, const DataTypePtr& attribute_type,
                        const ColumnPtr& key_column, const DataTypePtr& key_type) const override {
        MutableColumnPtr res_column = attribute_type->create_column();
        const auto& attribute = _column_data[attribute_index(attribute_name)];

        if (WhichDataType {key_type}.is_ipv6()) {
            const auto* ipv6_column = assert_cast<const ColumnIPv6*>(key_column.get());

            std::visit(
                    [&](auto&& arg) {
                        using HashTableType = std::decay_t<decltype(arg)>;
                        using AttributeRealDataType = HashTableType::DataType;
                        using AttributeRealColumnType = AttributeRealDataType::ColumnType;

                        auto* res_real_column =
                                assert_cast<AttributeRealColumnType*>(res_column.get());
                        const auto& attributes_column = arg.column;

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
                        using HashTableType = std::decay_t<decltype(arg)>;
                        using AttributeRealDataType = HashTableType::DataType;
                        using AttributeRealColumnType = AttributeRealDataType::ColumnType;

                        auto* res_real_column =
                                assert_cast<AttributeRealColumnType*>(res_column.get());
                        const auto& attributes_column = arg.column;

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

        // load att column
        _column_data.resize(attributes_column.size());
        for (size_t i = 0; i < attributes_column.size(); i++) {
            const DataTypePtr att_type = _attributes[i].type;
            ColumnPtr column = attributes_column[i];
            bool valid = IDictionary::cast_type(att_type.get(), [&](const auto& type) {
                using AttributeRealDataType = std::decay_t<decltype(type)>;
                using AttributeRealColumnType = AttributeRealDataType::ColumnType;
                const auto* res_real_column =
                        typeid_cast<const AttributeRealColumnType*>(column.get());
                if (!res_real_column) {
                    return false;
                }
                auto& att = _column_data[i];
                ColumnWithType<AttributeRealDataType> column_with_type;
                column_with_type.column = AttributeRealColumnType::create(*res_real_column);
                att = column_with_type;
                return true;
            });
            if (!valid) {
                throw doris::Exception(
                        ErrorCode::INVALID_ARGUMENT,
                        "IPAddressDictionary({}) att type is : {} , but input column is : {}",
                        dict_name(), att_type->get_name(), column->get_name());
            }
        }
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
    std::vector<ColumnData> _column_data;
};

inline DictionaryPtr create_mock_ip_trie_dict_from_column(const std::string& name,
                                                          ColumnWithTypeAndName key_data,
                                                          ColumnsWithTypeAndName attribute_data) {
    auto key_column = key_data.column;
    auto key_type = key_data.type;
    if (!WhichDataType {key_type}.is_string()) {
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

} // namespace doris::vectorized
