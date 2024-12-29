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

#pragma once

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "vec/columns/column.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_ipv4.h"
#include "vec/data_types/data_type_ipv6.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_time_v2.h"
#include "vec/functions/cast_type_to_either.h"

namespace doris::vectorized {
struct DictionaryAttribute {
    const std::string name; // attribute name
    const DataTypePtr type; // should be a non-nullable type
};

class IDictionary {
public:
    IDictionary(std::string name, std::vector<DictionaryAttribute> attributes);
    virtual ~IDictionary() = default;
    std::string dict_name() const { return _dict_name; }

    virtual ColumnPtr getColumn(const std::string& attribute_name,
                                const DataTypePtr& attribute_type, const ColumnPtr& key_column,
                                const DataTypePtr& key_type) const = 0;

    bool has_attribute(const std::string& name) const;
    DataTypePtr get_attribute_type(const std::string& name) const;
    size_t attribute_index(const std::string& name) const;

    template <typename F>
    static bool cast_type(const IDataType* type, F&& f) {
        // The data types supported by cast_type must be consistent with the AttributeData below.
        return cast_type_to_either<DataTypeUInt8, DataTypeInt8, DataTypeInt16, DataTypeInt32,
                                   DataTypeInt64, DataTypeInt128, DataTypeFloat32, DataTypeFloat64,
                                   DataTypeIPv4, DataTypeIPv6, DataTypeString, DataTypeDateV2,
                                   DataTypeDateTimeV2, DataTypeDecimal<Decimal32>,
                                   DataTypeDecimal<Decimal64>, DataTypeDecimal<Decimal128V3>,
                                   DataTypeDecimal<Decimal256>>(type, std::forward<F>(f));
    }

protected:
    template <typename Type>
    struct ColumnWithType {
        using DataType = Type;
        DataType::ColumnType::Ptr column;
    };

    // `AttributeData` is a variant type. Use it with `std::visit` in the following way:
    // std::visit(
    //     [&](auto&& arg) {
    //         using HashTableType = std::decay_t<decltype(arg)>;
    //         using AttributeRealDataType = typename HashTableType::DataType;
    //         using AttributeRealColumnType = typename AttributeRealDataType::ColumnType;
    //     },
    //     AttributeData);
    using AttributeData =
            std::variant<ColumnWithType<DataTypeUInt8>, ColumnWithType<DataTypeInt8>,
                         ColumnWithType<DataTypeInt16>, ColumnWithType<DataTypeInt32>,
                         ColumnWithType<DataTypeInt64>, ColumnWithType<DataTypeInt128>,

                         ColumnWithType<DataTypeFloat32>, ColumnWithType<DataTypeFloat64>,

                         ColumnWithType<DataTypeIPv4>, ColumnWithType<DataTypeIPv6>,

                         ColumnWithType<DataTypeString>,

                         ColumnWithType<DataTypeDateV2>, ColumnWithType<DataTypeDateTimeV2>,

                         ColumnWithType<DataTypeDecimal<Decimal32>>,
                         ColumnWithType<DataTypeDecimal<Decimal64>>,
                         ColumnWithType<DataTypeDecimal<Decimal128V3>>,
                         ColumnWithType<DataTypeDecimal<Decimal256>>>;

    // load_attributes will remove nullable attributes.
    // Any nullable-related data needs to be handled by the subclass dictionary.
    void load_attributes(std::vector<ColumnPtr>& attributes_column);

    // _attribute_data is used to store the data of attribute columns.
    // Nullable columns are not stored here.
    std::vector<AttributeData> _attribute_data;
    const std::string _dict_name;
    std::vector<DictionaryAttribute> _attributes;
    // A mapping from attribute names to their corresponding indices.
    std::unordered_map<std::string, size_t> _name_to_attributes_index;
};

using DictionaryPtr = std::shared_ptr<IDictionary>;

} // namespace doris::vectorized
