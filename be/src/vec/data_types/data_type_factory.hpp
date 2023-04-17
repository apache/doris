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
#include <type_traits>

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
#include "vec/data_types/data_type_fixed_length_object.h"
#include "vec/data_types/data_type_hll.h"
#include "vec/data_types/data_type_jsonb.h"
#include "vec/data_types/data_type_map.h"
#include "vec/data_types/data_type_nothing.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_quantilestate.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_struct.h"

namespace doris::vectorized {

class DataTypeFactory {
    using DataTypeMap = std::unordered_map<std::string, DataTypePtr>;
    using InvertedDataTypeMap = std::vector<std::pair<DataTypePtr, std::string>>;

public:
    static DataTypeFactory& instance() {
        static DataTypeFactory instance;
        return instance;
    }

    DataTypePtr create_data_type(const doris::Field& col_desc);
    DataTypePtr create_data_type(const TypeIndex& type_index, bool is_nullable = false);
    DataTypePtr create_data_type(const TabletColumn& col_desc, bool is_nullable = false);

    DataTypePtr create_data_type(const TypeDescriptor& col_desc, bool is_nullable = true);

    DataTypePtr create_data_type(const PColumnMeta& pcolumn);

    DataTypePtr create_data_type(const arrow::DataType* type, bool is_nullable);

    DataTypePtr create_data_type(const TTypeDesc& raw_type) {
        return create_data_type(TypeDescriptor::from_thrift(raw_type), raw_type.is_nullable);
    }

    DataTypePtr create_data_type(const FieldType& type, int precision, int scale) {
        return _create_primitive_data_type(type, precision, scale);
    }

private:
    DataTypePtr _create_primitive_data_type(const FieldType& type, int precision, int scale) const;

    std::string _empty_string;
};
} // namespace doris::vectorized
