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
#include <ranges>
#include <vector>

#include "vec/columns/column.h"
#include "vec/columns/column_string.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/core/types.h"
#include "vec/runtime/ip_address_cidr.h"
#include "vec/runtime/ipv4_value.h"

namespace doris::vectorized {

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
    const auto& attribute = _attribute_data[attribute_index(attribute_name)];

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
    // load att column
    load_attributes(attributes_column);

    // Construct an IP trie

    // Step 1: Import the CIDR data.
    // Record the parsed CIDR and the corresponding row from the original data.
    const auto* str_column = assert_cast<const ColumnString*>(key_column.get());
    std::vector<IPRecord> ip_records;
    for (size_t i = 0; i < str_column->size(); i++) {
        auto ip_str = str_column->get_element(i);
        ip_records.push_back(IPRecord {parse_ip_with_cidr(ip_str), i});
    }

    // Step 2: Process IP data.

    // Step 2.1: Process all corresponding CIDRs as IPv6.
    // Sort them by {the value of IPv6, the prefix length of the CIDR in IPv6}.
    std::sort(ip_records.begin(), ip_records.end(), [&](const IPRecord& a, const IPRecord& b) {
        if (a.to_ipv6() == b.to_ipv6()) {
            return a.prefix() < b.prefix();
        }
        return a.to_ipv6() < b.to_ipv6();
    });

    // Step 2.2: Remove duplicate data.
    auto new_end = std::unique(ip_records.begin(), ip_records.end(),
                               [&](const IPRecord& a, const IPRecord& b) {
                                   return a.to_ipv6() == b.to_ipv6() && a.prefix() == b.prefix();
                               });
    ip_records.erase(new_end, ip_records.end());

    // Step 3: Process the data needed for the Trie.
    // You can treat ip_column, prefix_column, and origin_row_idx_column as a whole.
    // struct TrieNode {
    //     IPv6 ip;
    //     UInt8 prefix;
    //     size_t origin_row_idx;
    // };
    for (const auto& record : ip_records) {
        ip_column.push_back(record.to_ipv6());
        prefix_column.push_back(record.prefix());
        origin_row_idx_column.push_back(record.row);
    }

    // Step 4: Construct subnet relationships.
    // The CIDR at index i is a subnet of the CIDR at index parent_subnet[i], for example:
    // 192.168.0.0/24 [0]
    // ├── 192.168.0.0/25 [1]
    // │   ├── 192.168.0.0/26 [2]
    // │   └── 192.168.0.64/26 [3]
    // └── 192.168.0.128/25 [4]
    // parent_subnet[4] = 0
    // parent_subnet[3] = 1
    // parent_subnet[2] = 1
    // parent_subnet[1] = 0
    // parent_subnet[0] = 0 (itself)
    parent_subnet.resize(ip_records.size());
    std::stack<size_t> subnets_stack;
    for (auto i = 0; i < ip_records.size(); i++) {
        parent_subnet[i] = i;
        while (!subnets_stack.empty()) {
            size_t pi = subnets_stack.top();

            auto cur_address_ip = ip_records[i].to_ipv6();
            const auto* addr = reinterpret_cast<UInt8*>(&cur_address_ip);
            auto parent_subnet_ip = ip_records[pi].to_ipv6();
            const auto* parent_addr = reinterpret_cast<UInt8*>(&parent_subnet_ip);

            bool is_mask_smaller = ip_records[pi].prefix() < ip_records[i].prefix();
            if (is_mask_smaller && match_ipv6_subnet(addr, parent_addr, ip_records[pi].prefix())) {
                parent_subnet[i] = pi;
                break;
            }

            subnets_stack.pop();
        }
        subnets_stack.push(i);
    }
}

IPAddressDictionary::RowIdxConstIter IPAddressDictionary::lookupIP(IPv6 target) const {
    if (origin_row_idx_column.empty()) {
        return ip_not_found();
    }

    auto comp = [&](IPv6 value, auto idx) -> bool { return value < ip_column[idx]; };

    auto range = std::ranges::views::iota(0ULL, origin_row_idx_column.size());

    // Query Step 1: First, use binary search to find a CIDR that is close to the target.
    auto found_it = std::ranges::upper_bound(range, target, comp);

    if (found_it == range.begin()) {
        return ip_not_found();
    }

    --found_it;

    // Query Step 2: Based on the subnet relationships, find the first matching CIDR.
    for (auto idx = *found_it;; idx = parent_subnet[idx]) {
        if (match_ipv6_subnet(reinterpret_cast<const UInt8*>(&target),
                              reinterpret_cast<const UInt8*>(&ip_column[idx]),
                              prefix_column[idx])) {
            return origin_row_idx_column.begin() + idx;
        }
        if (idx == parent_subnet[idx]) {
            return ip_not_found();
        }
    }

    return ip_not_found();
}

} // namespace doris::vectorized