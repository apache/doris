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

#include "vec/functions/ip_address_dictionary.h"

#include <algorithm>
#include <cassert>
#include <memory>
#include <ranges>
#include <vector>

#include "gutil/strings/split.h"
#include "vec/columns/column.h"
#include "vec/columns/column_string.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_number.h"
#include "vec/exprs/vlambda_function_call_expr.h"
#include "vec/functions/dictionary.h"
#include "vec/runtime/ip_address_cidr.h"
#include "vec/runtime/ipv4_value.h"

namespace doris::vectorized {

void map_ipv4_to_ipv6(IPv4 ipv4, UInt8* buf) {
    unaligned_store<UInt64>(buf, 0x0000FFFF00000000ULL | static_cast<UInt64>(ipv4));
    unaligned_store<UInt64>(buf + 8, 0);
}

IPv6 ipv4_to_ipv6(IPv4 ipv4) {
    IPv6 ipv6;
    map_ipv4_to_ipv6(ipv4, reinterpret_cast<UInt8*>(&ipv6));
    return ipv6;
}

ColumnPtr IPAddressDictionary::getColumn(const std::string& attribute_name,
                                         const DataTypePtr& attribute_type,
                                         const ColumnPtr& key_column,
                                         const DataTypePtr& key_type) const {
    if (!WhichDataType {key_type}.is_ip()) {
        throw doris::Exception(
                ErrorCode::INVALID_ARGUMENT,
                "IPAddressDictionary only support ip type key , input key type is {} ",
                key_type->get_name());
    }

    MutableColumnPtr res_column = attribute_type->create_column();
    const auto& attribute = _column_data[attribute_index(attribute_name)];

    if (WhichDataType {key_type}.is_ipv6()) {
        const auto* ipv6_column = assert_cast<const ColumnIPv6*>(key_column.get());

        std::visit(
                [&](auto&& arg) {
                    using HashTableType = std::decay_t<decltype(arg)>;
                    using AttributeRealDataType = HashTableType::DataType;
                    using AttributeRealColumnType = AttributeRealDataType::ColumnType;

                    auto* res_real_column = assert_cast<AttributeRealColumnType*>(res_column.get());
                    const auto& attributes_column = arg.column;

                    for (size_t i = 0; i < ipv6_column->size(); i++) {
                        IPv6 ipv6 = ipv6_column->get_element(i);
                        auto it = lookupIP(ipv6);
                        if (it == ip_not_found()) {
                            res_column->insert_default();
                        } else {
                            const auto idx = *it;
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

                    auto* res_real_column = assert_cast<AttributeRealColumnType*>(res_column.get());
                    const auto& attributes_column = arg.column;

                    for (size_t i = 0; i < ipv4_column->size(); i++) {
                        IPv4 ipv4 = ipv4_column->get_element(i);
                        IPv6 ipv6 = ipv4_to_ipv6(ipv4);
                        auto it = lookupIP(ipv6);
                        if (it == ip_not_found()) {
                            res_column->insert_default();
                        } else {
                            const auto idx = *it;
                            res_real_column->insert_value(attributes_column->get_element(idx));
                        }
                    }
                },
                attribute);
    }

    return res_column;
}

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

void IPAddressDictionary::load_data(ColumnPtr& key_column,
                                    std::vector<ColumnPtr>& attributes_column) {
    const auto* str_column = assert_cast<const ColumnString*>(key_column.get());

    std::vector<IPRecord> ip_records;

    // load key column
    for (size_t i = 0; i < str_column->size(); i++) {
        auto ip_str = str_column->get_element(i);
        ip_records.push_back(IPRecord {parse_ip_with_cidr(ip_str), i});
    }

    // load att column

    _column_data.resize(attributes_column.size());

    for (size_t i = 0; i < attributes_column.size(); i++) {
        const DataTypePtr att_type = _attributes[i].type;
        ColumnPtr column = attributes_column[i];
        bool valid = IDictionary::cast_type(att_type.get(), [&](const auto& type) {
            using AttributeRealDataType = std::decay_t<decltype(type)>;
            using AttributeRealColumnType = AttributeRealDataType::ColumnType;
            const auto* res_real_column = typeid_cast<const AttributeRealColumnType*>(column.get());
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

    // build ip trie

    // sort all ip
    std::sort(ip_records.begin(), ip_records.end(), [&](const IPRecord& a, const IPRecord& b) {
        if (a.to_ipv6() == b.to_ipv6()) {
            return a.prefix() < b.prefix();
        }
        return a.to_ipv6() < b.to_ipv6();
    });

    auto new_end = std::unique(ip_records.begin(), ip_records.end(),
                               [&](const IPRecord& a, const IPRecord& b) {
                                   return a.to_ipv6() == b.to_ipv6() && a.prefix() == b.prefix();
                               });
    ip_records.erase(new_end, ip_records.end());

    for (const auto& record : ip_records) {
        ip_column.push_back(record.to_ipv6());
        mask_column.push_back(record.prefix());
        row_idx.push_back(record.row);
    }

    parent_subnet.resize(ip_records.size());
    std::stack<size_t> subnets_stack;
    for (auto i = 0; i < ip_records.size(); i++) {
        parent_subnet[i] = i;
        while (!subnets_stack.empty()) {
            size_t pi = subnets_stack.top();

            auto cur_address_ip = ip_records[i].to_ipv6();
            const auto* cur_addi = reinterpret_cast<UInt8*>(&cur_address_ip);
            auto cur_subnet_ip = ip_records[pi].to_ipv6();
            const auto* cur_addip = reinterpret_cast<UInt8*>(&cur_subnet_ip);

            bool is_mask_smaller = ip_records[pi].prefix() < ip_records[i].prefix();
            if (is_mask_smaller &&
                match_ipv6_subnet(cur_addi, cur_addip, ip_records[pi].prefix())) {
                parent_subnet[i] = pi;
                break;
            }

            subnets_stack.pop();
        }
        subnets_stack.push(i);
    }
}

IPAddressDictionary::RowIdxConstIter IPAddressDictionary::lookupIP(IPv6 target) const {
    if (row_idx.empty()) {
        return ip_not_found();
    }

    auto comp = [&](IPv6 value, auto idx) -> bool { return value < ip_column[idx]; };

    auto range = std::ranges::views::iota(0ULL, row_idx.size());

    auto found_it = std::ranges::upper_bound(range, target, comp);

    if (found_it == range.begin()) {
        return ip_not_found();
    }

    --found_it;

    for (auto idx = *found_it;; idx = parent_subnet[idx]) {
        if (match_ipv6_subnet(reinterpret_cast<const UInt8*>(&target),
                              reinterpret_cast<const UInt8*>(&ip_column[idx]), mask_column[idx])) {
            return row_idx.begin() + idx;
        }
        if (idx == parent_subnet[idx]) {
            return ip_not_found();
        }
    }

    return ip_not_found();
}

} // namespace doris::vectorized