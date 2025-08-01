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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Dictionaries/IPAddressDictionary.cpp
// and modified by Doris

#include "vec/functions/ip_address_dictionary.h"

#include <algorithm>
#include <ranges>
#include <vector>

#include "common/exception.h"
#include "common/status.h"
#include "vec/columns/column.h"
#include "vec/columns/column_string.h"
#include "vec/common/assert_cast.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h" // IWYU pragma: keep
#include "vec/functions/dictionary.h"
#include "vec/runtime/ip_address_cidr.h"
#include "vec/runtime/ipv4_value.h"
#include "vec/utils/template_helpers.hpp"

namespace doris::vectorized {

IPAddressDictionary::~IPAddressDictionary() {
    if (_mem_tracker) {
        std::vector<IPv6> {}.swap(ip_column);
        std::vector<UInt8> {}.swap(prefix_column);
        std::vector<size_t> {}.swap(origin_row_idx_column);
        std::vector<size_t> {}.swap(parent_subnet);
    }
}

size_t IPAddressDictionary::allocated_bytes() const {
    auto vec_mem = [](const auto& vec) {
        return vec.capacity() * sizeof(typename std::decay_t<decltype(vec)>::value_type);
    };
    return IDictionary::allocated_bytes() + vec_mem(ip_column) + vec_mem(prefix_column) +
           vec_mem(origin_row_idx_column) + vec_mem(parent_subnet);
}

ColumnPtr IPAddressDictionary::get_column(const std::string& attribute_name,
                                          const DataTypePtr& attribute_type,
                                          const ColumnPtr& key_column,
                                          const DataTypePtr& key_type) const {
    if (have_nullable({attribute_type}) || have_nullable({key_type})) {
        throw doris::Exception(
                ErrorCode::INTERNAL_ERROR,
                "IPAddressDictionary get_column attribute_type or key_type must not nullable type");
    }
    if (key_type->get_primitive_type() != TYPE_IPV4 &&
        key_type->get_primitive_type() != TYPE_IPV6) {
        throw doris::Exception(
                ErrorCode::INTERNAL_ERROR,
                "IPAddressDictionary only support ip type key , input key type is {} ",
                key_type->get_name());
    }

    const auto rows = key_column->size();
    MutableColumnPtr res_column = attribute_type->create_column();
    ColumnUInt8::MutablePtr res_null = ColumnUInt8::create(rows, false);
    auto& res_null_map = res_null->get_data();
    const auto& value_data = _values_data[attribute_index(attribute_name)];

    if (key_type->get_primitive_type() == TYPE_IPV6) {
        // input key column without nullable
        const auto* ipv6_column = assert_cast<const ColumnIPv6*>(remove_nullable(key_column).get());
        // if input key column is nullable, will not be null
        const ColumnNullable* null_key =
                key_column->is_nullable() ? assert_cast<const ColumnNullable*>(key_column.get())
                                          : nullptr;
        std::visit(
                [&](auto&& arg, auto key_is_nullable, auto value_is_nullable) {
                    using ValueDataType = std::decay_t<decltype(arg)>;
                    using OutputColumnType = ValueDataType::OutputColumnType;
                    auto* res_real_column = assert_cast<OutputColumnType*>(res_column.get());
                    const auto* value_column = arg.get();
                    const auto* value_null_map = arg.get_null_map();
                    for (size_t i = 0; i < rows; i++) {
                        if constexpr (key_is_nullable) {
                            if (null_key->is_null_at(i)) {
                                // if input key is null, set the result column to null
                                res_real_column->insert_default();
                                res_null_map[i] = true;
                                continue;
                            }
                        }
                        auto it = look_up_IP(ipv6_column->get_element(i));
                        if (it == ip_not_found()) {
                            // if input key is not found, set the result column to null
                            res_real_column->insert_default();
                            res_null_map[i] = true;
                        } else {
                            const auto idx = *it;
                            set_value_data<value_is_nullable>(res_real_column, res_null_map[i],
                                                              value_column, value_null_map, idx);
                        }
                    }
                },
                value_data, make_bool_variant(null_key != nullptr),
                attribute_nullable_variant(attribute_index(attribute_name)));
    } else {
        // input key column without nullable
        const auto* ipv4_column = assert_cast<const ColumnIPv4*>(remove_nullable(key_column).get());
        // if input key column is nullable, will not be null
        const ColumnNullable* null_key =
                key_column->is_nullable() ? assert_cast<const ColumnNullable*>(key_column.get())
                                          : nullptr;
        std::visit(
                [&](auto&& arg, auto key_is_nullable, auto value_is_nullable) {
                    using ValueDataType = std::decay_t<decltype(arg)>;
                    using OutputColumnType = ValueDataType::OutputColumnType;
                    auto* res_real_column = assert_cast<OutputColumnType*>(res_column.get());
                    const auto* value_column = arg.get();
                    const auto* value_null_map = arg.get_null_map();
                    for (size_t i = 0; i < rows; i++) {
                        if constexpr (key_is_nullable) {
                            if (null_key->is_null_at(i)) {
                                // if input key is null, set the result column to null
                                res_real_column->insert_default();
                                res_null_map[i] = true;
                                continue;
                            }
                        }
                        auto it = look_up_IP(ipv4_to_ipv6(ipv4_column->get_element(i)));
                        if (it == ip_not_found()) {
                            // if input key is not found, set the result column to null
                            res_real_column->insert_default();
                            res_null_map[i] = true;
                        } else {
                            const auto idx = *it;
                            set_value_data<value_is_nullable>(res_real_column, res_null_map[i],
                                                              value_column, value_null_map, idx);
                        }
                    }
                },
                value_data, make_bool_variant(null_key != nullptr),
                attribute_nullable_variant(attribute_index(attribute_name)));
    }

    return ColumnNullable::create(std::move(res_column), std::move(res_null));
}

IPv6 IPAddressDictionary::format_ipv6_cidr(const uint8_t* addr, uint8_t prefix) {
    if (prefix > IPV6_BINARY_LENGTH * 8U) {
        prefix = IPV6_BINARY_LENGTH * 8U;
    }
    IPv6 ipv6 = 0;
    apply_cidr_mask(reinterpret_cast<const char*>(addr), reinterpret_cast<char*>(&ipv6), prefix);
    return ipv6;
}

struct IPRecord {
    IPAddressCIDR ip_with_cidr; // IP address with CIDR notation
    size_t row;                 // Row index in the original data

    // Convert the IP address to IPv6 format
    IPv6 to_ipv6() const {
        auto origin_ipv6 = _to_ipv6();
        return IPAddressDictionary::format_ipv6_cidr(reinterpret_cast<const UInt8*>(&origin_ipv6),
                                                     prefix());
    }

    // Get the prefix length of the IP address
    UInt8 prefix() const {
        if (ip_with_cidr._address.as_v6()) {
            return ip_with_cidr._prefix;
        }
        return ip_with_cidr._prefix + 96;
    }

private:
    IPv6 _to_ipv6() const {
        if (const auto* address = ip_with_cidr._address.as_v6()) {
            IPv6 ipv6;
            memcpy(reinterpret_cast<UInt8*>(&ipv6), address, sizeof(IPv6));
            return ipv6;
        }

        return ipv4_to_ipv6(ip_with_cidr._address.as_v4());
    }
};

void IPAddressDictionary::load_data(const ColumnPtr& key_column,
                                    const std::vector<ColumnPtr>& values_column) {
    // load att column
    load_values(values_column);

    // Construct an IP trie

    // Step 1: Import the CIDR data.
    // Record the parsed CIDR and the corresponding row from the original data.
    std::vector<IPRecord> ip_records;
    auto load_key_str = [&](const auto* str_column) {
        for (size_t i = 0; i < str_column->size(); i++) {
            auto ip_str = str_column->get_data_at(i);
            try {
                ip_records.push_back(IPRecord {parse_ip_with_cidr(ip_str), i});
            } catch (Exception& e) {
                // add data unqualified error tag to the error message
                throw Exception(e.code(), DICT_DATA_ERROR_TAG + e.message());
            }
        }
    };
    if (key_column->is_column_string64()) {
        load_key_str(assert_cast<const ColumnString64*>(key_column.get()));
    } else {
        load_key_str(assert_cast<const ColumnString*>(key_column.get()));
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

    if (ip_records.size() < key_column->size()) {
        throw doris::Exception(
                ErrorCode::INVALID_ARGUMENT,
                DICT_DATA_ERROR_TAG + "The CIDR has duplicate data in IpAddressDictionary");
    }

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
    // Use monotonic stack to build IP subnet relationships in trie structure
    // https://liuzhenglaichn.gitbook.io/algorithm/monotonic-stack
    // Note: The final structure may result in multiple trees rather than a single tree
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

IPAddressDictionary::RowIdxConstIter IPAddressDictionary::look_up_IP(const IPv6& target) const {
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
