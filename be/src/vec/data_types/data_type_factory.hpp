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
#include <mutex>
#include <string>

#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_date.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_nothing.h"
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
        std::call_once(oc, [&]() {
            instance.regist_data_type("UInt8", DataTypePtr(std::make_shared<DataTypeUInt8>()));
            instance.regist_data_type("UInt16", DataTypePtr(std::make_shared<DataTypeUInt16>()));
            instance.regist_data_type("UInt32", DataTypePtr(std::make_shared<DataTypeUInt32>()));
            instance.regist_data_type("UInt64", DataTypePtr(std::make_shared<DataTypeUInt64>()));
            instance.regist_data_type("Int8", DataTypePtr(std::make_shared<DataTypeInt8>()));
            instance.regist_data_type("Int16", DataTypePtr(std::make_shared<DataTypeInt16>()));
            instance.regist_data_type("Int32", DataTypePtr(std::make_shared<DataTypeInt32>()));
            instance.regist_data_type("Int64", DataTypePtr(std::make_shared<DataTypeInt64>()));
            instance.regist_data_type("Int128", DataTypePtr(std::make_shared<DataTypeInt128>()));
            instance.regist_data_type("Float32", DataTypePtr(std::make_shared<DataTypeFloat32>()));
            instance.regist_data_type("Float64", DataTypePtr(std::make_shared<DataTypeFloat64>()));
            instance.regist_data_type("Date", DataTypePtr(std::make_shared<DataTypeDate>()));
            instance.regist_data_type("DateTime",
                                      DataTypePtr(std::make_shared<DataTypeDateTime>()));
            instance.regist_data_type("String", DataTypePtr(std::make_shared<DataTypeString>()));
            instance.regist_data_type("Decimal",
                    DataTypePtr(std::make_shared<DataTypeDecimal<Decimal128>>(27, 9)));
        });
        return instance;
    }
    DataTypePtr get(const std::string& name) { return _data_type_map[name]; }
    const std::string& get(const DataTypePtr& data_type) const {
        for (const auto& entity : _invert_data_type_map) {
            if (entity.first->equals(*data_type)) {
                return entity.second;
            }
        }
        return _empty_string;
    }

private:
    void regist_data_type(const std::string& name, const DataTypePtr& data_type) {
        _data_type_map.emplace(name, data_type);
        _invert_data_type_map.emplace_back(data_type, name);
    }
    // TODO: Here is a little trick here, use bimap to replace map and vector
    DataTypeMap _data_type_map;
    InvertedDataTypeMap _invert_data_type_map;
    std::string _empty_string;
};
} // namespace doris::vectorized
