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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Dictionaries/IPAddressDictionary.h
// and modified by Doris

#pragma once

#include <utility>
#include <vector>

#include "vec/columns/column.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/columns_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/functions/dictionary.h"

namespace doris::vectorized {
class IPAddressDictionary : public IDictionary {
public:
    IPAddressDictionary(std::string name, std::vector<DictionaryAttribute> attributes)
            : IDictionary(std::move(name), std::move(attributes)) {}

    ~IPAddressDictionary() override;

    ColumnPtr get_column(const std::string& attribute_name, const DataTypePtr& attribute_type,
                         const ColumnPtr& key_column, const DataTypePtr& key_type) const override;

    static DictionaryPtr create_ip_trie_dict(const std::string& name, const ColumnPtr& key_column,
                                             const ColumnsWithTypeAndName& values_data) {
        std::vector<DictionaryAttribute> attributes;
        std::vector<ColumnPtr> values_column;
        for (const auto& att : values_data) {
            attributes.push_back({att.name, att.type});
            values_column.push_back(att.column);
        }
        auto dict = std::make_shared<IPAddressDictionary>(name, attributes);
        dict->load_data(key_column, values_column);
        return dict;
    }
    /*
        format_ipv6_cidr is used to standardize CIDR.
        For example, 192.1.1.1/24 is not a valid CIDR. 
        It should ensure that all bits after the mask are set to 0, resulting in 192.1.1.0/24.
    */
    static IPv6 format_ipv6_cidr(const uint8_t* addr, uint8_t prefix);

    size_t allocated_bytes() const override;

private:
    using RowIdxConstIter = std::vector<size_t>::const_iterator;

    RowIdxConstIter ip_not_found() const { return origin_row_idx_column.end(); }

    RowIdxConstIter look_up_IP(const IPv6& target) const;

    void load_data(const ColumnPtr& key_column, const std::vector<ColumnPtr>& values_column);

    std::vector<IPv6> ip_column;

    std::vector<UInt8> prefix_column;

    std::vector<size_t> origin_row_idx_column;

    std::vector<size_t> parent_subnet;
};

inline DictionaryPtr create_ip_trie_dict_from_column(const std::string& name,
                                                     const ColumnWithTypeAndName& key_data,
                                                     const ColumnsWithTypeAndName& values_data) {
    auto key_column = key_data.column;
    auto key_type = key_data.type;
    if (!is_string_type(key_type->get_primitive_type())) {
        throw doris::Exception(
                ErrorCode::INVALID_ARGUMENT,
                DICT_DATA_ERROR_TAG +
                        "IPAddressDictionary only support string in key , input key type is {} ",
                key_type->get_name());
    }

    DictionaryPtr dict = IPAddressDictionary::create_ip_trie_dict(name, key_column, values_data);
    return dict;
}
} // namespace doris::vectorized
