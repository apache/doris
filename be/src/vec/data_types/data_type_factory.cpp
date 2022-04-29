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

#include "vec/data_types/data_type_hll.h"

namespace doris::vectorized {

DataTypePtr DataTypeFactory::create_data_type(const doris::Field& col_desc) {
    DataTypePtr nested = nullptr;
    if (col_desc.type() == OLAP_FIELD_TYPE_ARRAY) {
        DCHECK(col_desc.get_sub_field_count() == 1);
        nested = std::make_shared<DataTypeArray>(create_data_type(*col_desc.get_sub_field(0)));
    } else {
        nested = _create_primitive_data_type(col_desc.type());
    }

    if (col_desc.is_nullable() && nested) {
        return std::make_shared<DataTypeNullable>(nested);
    }
    return nested;
}

DataTypePtr DataTypeFactory::create_data_type(const TabletColumn& col_desc, bool is_nullable) {
    DataTypePtr nested = nullptr;
    if (col_desc.type() == OLAP_FIELD_TYPE_ARRAY) {
        DCHECK(col_desc.get_subtype_count() == 1);
        nested = std::make_shared<DataTypeArray>(create_data_type(col_desc.get_sub_column(0)));
    } else {
        nested = _create_primitive_data_type(col_desc.type());
    }

    if ((is_nullable || col_desc.is_nullable()) && nested) {
        return std::make_shared<DataTypeNullable>(nested);
    }
    return nested;
}

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
    case TYPE_ARRAY:
        DCHECK(col_desc.children.size() == 1);
        nested =
                std::make_shared<vectorized::DataTypeArray>(create_data_type(col_desc.children[0]));
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

DataTypePtr DataTypeFactory::_create_primitive_data_type(const FieldType& type) const {
    DataTypePtr result = nullptr;
    switch (type) {
    case OLAP_FIELD_TYPE_BOOL:
        result = std::make_shared<vectorized::DataTypeUInt8>();
        break;
    case OLAP_FIELD_TYPE_TINYINT:
        result = std::make_shared<vectorized::DataTypeInt8>();
        break;
    case OLAP_FIELD_TYPE_SMALLINT:
        result = std::make_shared<vectorized::DataTypeInt16>();
        break;
    case OLAP_FIELD_TYPE_INT:
        result = std::make_shared<vectorized::DataTypeInt32>();
        break;
    case OLAP_FIELD_TYPE_FLOAT:
        result = std::make_shared<vectorized::DataTypeFloat32>();
        break;
    case OLAP_FIELD_TYPE_BIGINT:
        result = std::make_shared<vectorized::DataTypeInt64>();
        break;
    case OLAP_FIELD_TYPE_LARGEINT:
        result = std::make_shared<vectorized::DataTypeInt128>();
        break;
    case OLAP_FIELD_TYPE_DATE:
        result = std::make_shared<vectorized::DataTypeDate>();
        break;
    case OLAP_FIELD_TYPE_DATETIME:
        result = std::make_shared<vectorized::DataTypeDateTime>();
        break;
    case OLAP_FIELD_TYPE_DOUBLE:
        result = std::make_shared<vectorized::DataTypeFloat64>();
        break;
    case OLAP_FIELD_TYPE_CHAR:
    case OLAP_FIELD_TYPE_VARCHAR:
    case OLAP_FIELD_TYPE_STRING:
        result = std::make_shared<vectorized::DataTypeString>();
        break;
    case OLAP_FIELD_TYPE_HLL:
        result = std::make_shared<vectorized::DataTypeHLL>();
        break;
    case OLAP_FIELD_TYPE_OBJECT:
        result = std::make_shared<vectorized::DataTypeBitMap>();
        break;
    case OLAP_FIELD_TYPE_DECIMAL:
        result = std::make_shared<vectorized::DataTypeDecimal<vectorized::Decimal128>>(27, 9);
        break;
    default:
        DCHECK(false) << "Invalid FieldType:" << (int)type;
        result = nullptr;
        break;
    }
    return result;
}

DataTypePtr DataTypeFactory::create_data_type(const PColumnMeta& pcolumn) {
    DataTypePtr nested = nullptr;
    switch (pcolumn.type()) {
    case PGenericType::UINT8:
        nested = std::make_shared<DataTypeUInt8>();
        break;
    case PGenericType::UINT16:
        nested = std::make_shared<DataTypeUInt16>();
        break;
    case PGenericType::UINT32:
        nested = std::make_shared<DataTypeUInt32>();
        break;
    case PGenericType::UINT64:
        nested = std::make_shared<DataTypeUInt64>();
        break;
    case PGenericType::UINT128:
        nested = std::make_shared<DataTypeUInt128>();
        break;
    case PGenericType::INT8:
        nested = std::make_shared<DataTypeInt8>();
        break;
    case PGenericType::INT16:
        nested = std::make_shared<DataTypeInt16>();
        break;
    case PGenericType::INT32:
        nested = std::make_shared<DataTypeInt32>();
        break;
    case PGenericType::INT64:
        nested = std::make_shared<DataTypeInt64>();
        break;
    case PGenericType::INT128:
        nested = std::make_shared<DataTypeInt128>();
        break;
    case PGenericType::FLOAT:
        nested = std::make_shared<DataTypeFloat32>();
        break;
    case PGenericType::DOUBLE:
        nested = std::make_shared<DataTypeFloat64>();
        break;
    case PGenericType::STRING:
        nested = std::make_shared<DataTypeString>();
        break;
    case PGenericType::DATE:
        nested = std::make_shared<DataTypeDate>();
        break;
    case PGenericType::DATETIME:
        nested = std::make_shared<DataTypeDateTime>();
        break;
    case PGenericType::DECIMAL32:
        nested = std::make_shared<DataTypeDecimal<Decimal32>>(pcolumn.decimal_param().precision(),
                                                              pcolumn.decimal_param().scale());
        break;
    case PGenericType::DECIMAL64:
        nested = std::make_shared<DataTypeDecimal<Decimal64>>(pcolumn.decimal_param().precision(),
                                                              pcolumn.decimal_param().scale());
        break;
    case PGenericType::DECIMAL128:
        nested = std::make_shared<DataTypeDecimal<Decimal128>>(pcolumn.decimal_param().precision(),
                                                               pcolumn.decimal_param().scale());
        break;
    case PGenericType::BITMAP:
        nested = std::make_shared<DataTypeBitMap>();
        break;
    case PGenericType::HLL:
        nested = std::make_shared<DataTypeHLL>();
        break;
    case PGenericType::LIST:
        DCHECK(pcolumn.children_size() == 1);
        nested = std::make_shared<DataTypeArray>(create_data_type(pcolumn.children(0)));
        break;
    default: {
        LOG(FATAL) << fmt::format("Unknown data type: {}", pcolumn.type());
        return nullptr;
    }
    }

    if (nested && pcolumn.is_nullable() > 0) {
        return std::make_shared<vectorized::DataTypeNullable>(nested);
    }
    return nested;
}

} // namespace doris::vectorized
