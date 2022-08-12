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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/DataTypes/DataTypeFactory.cpp
// and modified by Doris

#include "vec/data_types/data_type_factory.hpp"
#include "runtime/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_bitmap.h"
#include "vec/data_types/data_type_date.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_nothing.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {

DataTypePtr DataTypeFactory::create_data_type(const TypeDescriptor& col_desc, bool is_nullable) {
    DataTypePtr nested = nullptr;
    switch (col_desc.type) {
    case TYPE_BOOLEAN:
        nested = std::make_shared<vectorized::DataTypeUInt8>();
        break;
    case TYPE_TINYINT:
        nested = std::make_shared<vectorized::DataTypeInt8>();
        break;
    case TYPE_SMALLINT:
        nested = std::make_shared<vectorized::DataTypeInt16>();
        break;
    case TYPE_INT:
        nested = std::make_shared<vectorized::DataTypeInt32>();
        break;
    case TYPE_FLOAT:
        nested = std::make_shared<vectorized::DataTypeFloat32>();
        break;
    case TYPE_BIGINT:
        nested = std::make_shared<vectorized::DataTypeInt64>();
        break;
    case TYPE_LARGEINT:
        nested = std::make_shared<vectorized::DataTypeInt128>();
        break;
    case TYPE_DATE:
        nested = std::make_shared<vectorized::DataTypeDate>();
        break;
    case TYPE_DATETIME:
        nested = std::make_shared<vectorized::DataTypeDateTime>();
        break;
    case TYPE_TIME:
    case TYPE_DOUBLE:
        nested = std::make_shared<vectorized::DataTypeFloat64>();
        break;
    case TYPE_STRING:
    case TYPE_CHAR:
    case TYPE_VARCHAR:
        nested = std::make_shared<vectorized::DataTypeString>();
        break;
    case TYPE_HLL:
        nested = std::make_shared<vectorized::DataTypeHLL>();
        break;
    case TYPE_OBJECT:
        nested = std::make_shared<vectorized::DataTypeBitMap>();
        break;
    case TYPE_DECIMALV2:
        nested = std::make_shared<vectorized::DataTypeDecimal<vectorized::Decimal128>>(27, 9);
        break;
    // Just Mock A NULL Type in Vec Exec Engine
    case TYPE_NULL:
        nested = std::make_shared<vectorized::DataTypeUInt8>();
        break;
    case INVALID_TYPE:
    default:
        DCHECK(false) << "invalid PrimitiveType:" << (int)col_desc.type;
        break;
    }

    if (nested && is_nullable) {
        return std::make_shared<vectorized::DataTypeNullable>(nested);
    }
    return nested;
}


} // namespace doris::vectorized
