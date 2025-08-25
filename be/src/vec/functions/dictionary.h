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
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "vec/columns/column.h"
#include "vec/columns/column_string.h"
#include "vec/common/assert_cast.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_date_or_datetime_v2.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_ipv4.h"
#include "vec/data_types/data_type_ipv6.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/cast_type_to_either.h"

namespace doris {
class MemTrackerLimiter;
}
class DictionaryFactory;
namespace doris::vectorized {
/*
 * Dictionary implementation in Doris that provides key-value mapping functionality
 * Currently only supports in-memory dictionary storage
 */

const static std::string DICT_DATA_ERROR_TAG = "[INVALID_DICT_MARK]";

struct DictionaryAttribute {
    const std::string name; // value name
    const DataTypePtr type; // value type
};

// Abstract base class IDictionary that only stores values. Keys are maintained by specific derived classes
// IDictionary serves as the foundation for dictionary implementations where:
// - Only values are stored at the base level
// - Key management is delegated to derived classes
// - Provides interface for dictionary operations
class IDictionary {
public:
    IDictionary(std::string name, std::vector<DictionaryAttribute> values);
    virtual ~IDictionary();
    std::string dict_name() const { return _dict_name; }

    // Returns the result column, throws an exception if there is an issue
    // attribute_type , key_type must be no nullable type
    virtual ColumnPtr get_column(const std::string& attribute_name,
                                 const DataTypePtr& attribute_type, const ColumnPtr& key_column,
                                 const DataTypePtr& key_type) const = 0;

    // Returns multiple result columns, throws an exception if there is an issue
    // The default implementation calls get_column. If a more performant implementation is needed, this method can be overridden
    virtual ColumnPtrs get_columns(const std::vector<std::string>& attribute_names,
                                   const DataTypes& attribute_types, const ColumnPtr& key_column,
                                   const DataTypePtr& key_type) const {
        ColumnPtrs columns;
        for (size_t i = 0; i < attribute_names.size(); ++i) {
            columns.push_back(
                    get_column(attribute_names[i], attribute_types[i], key_column, key_type));
        }
        return columns;
    }

    // Compared to get_column and get_columns, supports multiple key columns and multiple value columns
    // The default implementation only supports one key column, such as IPAddressDictionary, HashMapDictionary
    // If support for multiple key columns is needed, this method can be overridden
    virtual ColumnPtrs get_tuple_columns(const std::vector<std::string>& attribute_names,
                                         const DataTypes& attribute_types,
                                         const ColumnPtrs& key_columns,
                                         const DataTypes& key_types) const {
        if (key_types.size() != 1) {
            throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                                   "Dictionary {} does not support multiple key columns",
                                   dict_name());
        }
        return get_columns(attribute_names, attribute_types, key_columns[0], key_types[0]);
    }

    bool has_attribute(const std::string& name) const;

    // will return a non-nullable type
    DataTypePtr get_attribute_type(const std::string& name) const;
    size_t attribute_index(const std::string& name) const;

    bool attribute_is_nullable(size_t idx) const;

    std::variant<std::false_type, std::true_type> attribute_nullable_variant(size_t idx) const;

    template <typename F>
    static bool cast_type(const IDataType* type, F&& f) {
        // The data types supported by cast_type must be consistent with the AttributeData below.
        return cast_type_to_either<DataTypeUInt8, DataTypeInt8, DataTypeInt16, DataTypeInt32,
                                   DataTypeInt64, DataTypeInt128, DataTypeFloat32, DataTypeFloat64,
                                   DataTypeIPv4, DataTypeIPv6, DataTypeString, DataTypeDateV2,
                                   DataTypeDateTimeV2, DataTypeDecimal32, DataTypeDecimal64,
                                   DataTypeDecimal128, DataTypeDecimal256>(type,
                                                                           std::forward<F>(f));
    }

    virtual size_t allocated_bytes() const;

protected:
    friend class DictionaryFactory;

    // Only used to distinguish from DataTypeString, used for ColumnWithType
    struct DictDataTypeString64 {
        using ColumnType = ColumnString;
    };

    template <typename Type>
    struct ColumnWithType {
        // OutputColumnType is used as the result column type
        using OutputColumnType = Type::ColumnType;
        ColumnPtr column;
        ColumnPtr null_map;
        // RealColumnType is the real type of the column, as there may be ColumnString64, but the result column will not be ColumnString64
        using RealColumnType = std::conditional_t<std::is_same_v<DictDataTypeString64, Type>,
                                                  ColumnString64, OutputColumnType>;
        const RealColumnType* get() const {
            return assert_cast<const RealColumnType*, TypeCheckOnRelease::DISABLE>(column.get());
        }

        const ColumnUInt8* get_null_map() const {
            if (!null_map) {
                return nullptr;
            }
            return assert_cast<const ColumnUInt8*, TypeCheckOnRelease::DISABLE>(null_map.get());
        }
    };

    // res_real_column : result column (get_column result)
    // res_null : if value is null, will set res_null to true
    // value_column : corresponding value column, non-nullable
    // value_null_column : corresponding value null map, if the original value is non-nullable, it will be nullptr
    // value_idx : index in the value column
    template <bool value_is_nullable, typename ResultColumnType>
    ALWAYS_INLINE static void set_value_data(ResultColumnType* res_real_column, UInt8& res_null,
                                             const auto* value_column,
                                             const ColumnUInt8* value_null_column,
                                             const size_t& value_idx) {
        if constexpr (value_is_nullable) {
            // if the value is null, set the result column to null
            if (value_null_column->get_element(value_idx)) {
                res_null = true;
                res_real_column->insert_default();
                return;
            }
        }
        if constexpr (std::is_same_v<ResultColumnType, ColumnString>) {
            // If it is a string column, use get_data_at to avoid copying
            StringRef str_ref = value_column->get_data_at(value_idx);
            res_real_column->insert_data(str_ref.data, str_ref.size);
        } else {
            res_real_column->insert_value(value_column->get_element(value_idx));
        }
    }

    /// TODO: Add support for more data types ,such as Array, Map, etc.
    using ValueData =
            std::variant<ColumnWithType<DataTypeUInt8>, ColumnWithType<DataTypeInt8>,
                         ColumnWithType<DataTypeInt16>, ColumnWithType<DataTypeInt32>,
                         ColumnWithType<DataTypeInt64>, ColumnWithType<DataTypeInt128>,

                         ColumnWithType<DataTypeFloat32>, ColumnWithType<DataTypeFloat64>,

                         ColumnWithType<DataTypeIPv4>, ColumnWithType<DataTypeIPv6>,

                         ColumnWithType<DataTypeString>, ColumnWithType<DictDataTypeString64>,

                         ColumnWithType<DataTypeDateV2>, ColumnWithType<DataTypeDateTimeV2>,

                         ColumnWithType<DataTypeDecimal32>, ColumnWithType<DataTypeDecimal64>,
                         ColumnWithType<DataTypeDecimal128>, ColumnWithType<DataTypeDecimal256>>;

    void load_values(const std::vector<ColumnPtr>& values_column);

    // _value_data is used to store the data of value columns.
    std::vector<ValueData> _values_data;
    std::string _dict_name;
    std::vector<DictionaryAttribute> _attributes;
    // A mapping from attribute names to their corresponding indices.
    std::unordered_map<std::string, size_t> _name_to_attributes_index;

    // mem_tracker comes from DictionaryFactory. If _mem_tracker is nullptr, it means it is in UT.
    std::shared_ptr<MemTrackerLimiter> _mem_tracker;
};

using DictionaryPtr = std::shared_ptr<IDictionary>;

} // namespace doris::vectorized
