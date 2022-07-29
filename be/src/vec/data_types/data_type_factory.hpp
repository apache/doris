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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/DataTypes/DataTypeFactory.h
// and modified by Doris

#pragma once
#include <mutex>
#include <string>

#include "arrow/type.h"
#include "common/consts.h"
#include "gen_cpp/data.pb.h"
#include "olap/field.h"
#include "olap/tablet_schema.h"
#include "runtime/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_bitmap.h"
#include "vec/data_types/data_type_date.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_nothing.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {

class DataTypeFactory {
    using DataTypeMap = std::unordered_map<std::string, DataTypePtr>;
    using InvertedDataTypeMap = std::vector<std::pair<DataTypePtr, std::string>>;

public:
    static DataTypeFactory& instance() {
        static std::once_flag oc;
        static DataTypeFactory instance;
        std::call_once(oc, []() {
            std::unordered_map<std::string, DataTypePtr> base_type_map {
                    {"UInt8", std::make_shared<DataTypeUInt8>()},
                    {"UInt16", std::make_shared<DataTypeUInt16>()},
                    {"UInt32", std::make_shared<DataTypeUInt32>()},
                    {"UInt64", std::make_shared<DataTypeUInt64>()},
                    {"Int8", std::make_shared<DataTypeInt8>()},
                    {"Int16", std::make_shared<DataTypeInt16>()},
                    {"Int32", std::make_shared<DataTypeInt32>()},
                    {"Int64", std::make_shared<DataTypeInt64>()},
                    {"Int128", std::make_shared<DataTypeInt128>()},
                    {"Float32", std::make_shared<DataTypeFloat32>()},
                    {"Float64", std::make_shared<DataTypeFloat64>()},
                    {"Date", std::make_shared<DataTypeDate>()},
                    {"DateV2", std::make_shared<DataTypeDateV2>()},
                    {"DateTime", std::make_shared<DataTypeDateTime>()},
                    {"DateTimeV2", std::make_shared<DataTypeDateTimeV2>()},
                    {"String", std::make_shared<DataTypeString>()},
                    {"Decimal", std::make_shared<DataTypeDecimal<Decimal128>>(27, 9)},
                    {"Decimal32", std::make_shared<DataTypeDecimal<Decimal32>>(
                                          BeConsts::MAX_DECIMAL32_PRECISION, 0)},
                    {"Decimal64", std::make_shared<DataTypeDecimal<Decimal64>>(
                                          BeConsts::MAX_DECIMAL64_PRECISION, 0)},
                    {"Decimal128", std::make_shared<DataTypeDecimal<Decimal128>>(
                                           BeConsts::MAX_DECIMAL128_PRECISION, 0)},

            };
            for (auto const& [key, val] : base_type_map) {
                instance.register_data_type(key, val);
                instance.register_data_type("Array(" + key + ")",
                                            std::make_shared<vectorized::DataTypeArray>(val));
                instance.register_data_type(
                        "Array(Nullable(" + key + "))",
                        std::make_shared<vectorized::DataTypeArray>(
                                std::make_shared<vectorized::DataTypeNullable>(val)));
            }
        });
        return instance;
    }
    DataTypePtr get(const std::string& name) { return _data_type_map[name]; }
    const std::string& get(const DataTypePtr& data_type) const {
        auto type_ptr = data_type->is_nullable()
                                ? ((DataTypeNullable*)(data_type.get()))->get_nested_type()
                                : data_type;
        for (const auto& entity : _invert_data_type_map) {
            if (entity.first->equals(*type_ptr)) {
                return entity.second;
            }
            if (is_decimal(type_ptr) && type_ptr->get_type_id() == entity.first->get_type_id()) {
                return entity.second;
            }
        }
        return _empty_string;
    }

    DataTypePtr create_data_type(const doris::Field& col_desc);
    DataTypePtr create_data_type(const TabletColumn& col_desc, bool is_nullable = false);

    DataTypePtr create_data_type(const TypeDescriptor& col_desc, bool is_nullable = true);

    DataTypePtr create_data_type(const PColumnMeta& pcolumn);

    DataTypePtr create_data_type(const arrow::DataType* type, bool is_nullable);

    DataTypePtr create_data_type(const TTypeDesc& raw_type) {
        return create_data_type(TypeDescriptor::from_thrift(raw_type), raw_type.is_nullable);
    }

private:
    DataTypePtr _create_primitive_data_type(const FieldType& type, int precision, int scale) const;

    void register_data_type(const std::string& name, const DataTypePtr& data_type) {
        _data_type_map.emplace(name, data_type);
        _invert_data_type_map.emplace_back(data_type, name);
    }
    // TODO: Here is a little trick here, use bimap to replace map and vector
    DataTypeMap _data_type_map;
    InvertedDataTypeMap _invert_data_type_map;
    std::string _empty_string;
};
} // namespace doris::vectorized
