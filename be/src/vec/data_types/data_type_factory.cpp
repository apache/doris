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

#include <arrow/type.h>
#include <fmt/format.h>
#include <gen_cpp/data.pb.h>
#include <gen_cpp/types.pb.h>
#include <glog/logging.h>
#include <stddef.h>
#include <stdint.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <memory>
#include <ostream>

#include "common/consts.h"
#include "common/exception.h"
#include "common/status.h"
#include "data_type_time.h"
#include "gen_cpp/segment_v2.pb.h"
#include "olap/field.h"
#include "olap/olap_common.h"
#include "runtime/define_primitive_type.h"
#include "vec/common/assert_cast.h"
#include "vec/common/uint128.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_agg_state.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_bitmap.h"
#include "vec/data_types/data_type_date.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_fixed_length_object.h"
#include "vec/data_types/data_type_hll.h"
#include "vec/data_types/data_type_ipv4.h"
#include "vec/data_types/data_type_ipv6.h"
#include "vec/data_types/data_type_jsonb.h"
#include "vec/data_types/data_type_map.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_object.h"
#include "vec/data_types/data_type_quantilestate.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_struct.h"
#include "vec/data_types/data_type_time_v2.h"

namespace doris::vectorized {

DataTypePtr DataTypeFactory::create_data_type(const doris::Field& col_desc) {
    return create_data_type(col_desc.get_desc(), col_desc.is_nullable());
}

DataTypePtr DataTypeFactory::create_data_type(const TabletColumn& col_desc, bool is_nullable) {
    DataTypePtr nested = nullptr;
    if (col_desc.type() == FieldType::OLAP_FIELD_TYPE_AGG_STATE) {
        DataTypes dataTypes;
        for (size_t i = 0; i < col_desc.get_subtype_count(); i++) {
            dataTypes.push_back(create_data_type(col_desc.get_sub_column(i)));
        }
        nested = std::make_shared<vectorized::DataTypeAggState>(
                dataTypes, col_desc.get_result_is_nullable(), col_desc.get_aggregation_name(),
                col_desc.get_be_exec_version());
    } else if (col_desc.type() == FieldType::OLAP_FIELD_TYPE_ARRAY) {
        DCHECK(col_desc.get_subtype_count() == 1);
        nested = std::make_shared<DataTypeArray>(create_data_type(col_desc.get_sub_column(0)));
    } else if (col_desc.type() == FieldType::OLAP_FIELD_TYPE_MAP) {
        DCHECK(col_desc.get_subtype_count() == 2);
        nested = std::make_shared<vectorized::DataTypeMap>(
                create_data_type(col_desc.get_sub_column(0)),
                create_data_type(col_desc.get_sub_column(1)));
    } else if (col_desc.type() == FieldType::OLAP_FIELD_TYPE_STRUCT) {
        DCHECK(col_desc.get_subtype_count() >= 1);
        size_t col_size = col_desc.get_subtype_count();
        DataTypes dataTypes;
        Strings names;
        dataTypes.reserve(col_size);
        names.reserve(col_size);
        for (size_t i = 0; i < col_size; i++) {
            dataTypes.push_back(create_data_type(col_desc.get_sub_column(i)));
            names.push_back(col_desc.get_sub_column(i).name());
        }
        nested = std::make_shared<DataTypeStruct>(dataTypes, names);
    } else {
        nested =
                _create_primitive_data_type(col_desc.type(), col_desc.precision(), col_desc.frac());
    }

    if ((is_nullable || col_desc.is_nullable()) && nested) {
        return make_nullable(nested);
    }
    return nested;
}

DataTypePtr DataTypeFactory::create_data_type(const TypeDescriptor& col_desc, bool is_nullable) {
    DataTypePtr nested = nullptr;
    DataTypes subTypes;
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
    case TYPE_IPV4:
        nested = std::make_shared<vectorized::DataTypeIPv4>();
        break;
    case TYPE_IPV6:
        nested = std::make_shared<vectorized::DataTypeIPv6>();
        break;
    case TYPE_DATE:
        nested = std::make_shared<vectorized::DataTypeDate>();
        break;
    case TYPE_DATEV2:
        nested = std::make_shared<vectorized::DataTypeDateV2>();
        break;
    case TYPE_DATETIMEV2:
        nested = vectorized::create_datetimev2(col_desc.scale);
        break;
    case TYPE_DATETIME:
        nested = std::make_shared<vectorized::DataTypeDateTime>();
        break;
    case TYPE_TIME:
    case TYPE_TIMEV2:
        nested = std::make_shared<vectorized::DataTypeTimeV2>(col_desc.scale);
        break;
    case TYPE_DOUBLE:
        nested = std::make_shared<vectorized::DataTypeFloat64>();
        break;
    case TYPE_VARIANT:
        nested = std::make_shared<vectorized::DataTypeObject>("", true);
        break;
    case TYPE_STRING:
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_BINARY:
    case TYPE_LAMBDA_FUNCTION:
        nested = std::make_shared<vectorized::DataTypeString>();
        break;
    case TYPE_AGG_STATE:
        for (size_t i = 0; i < col_desc.children.size(); i++) {
            subTypes.push_back(create_data_type(col_desc.children[i], col_desc.contains_nulls[i]));
        }
        nested = std::make_shared<vectorized::DataTypeAggState>(
                subTypes, col_desc.result_is_nullable, col_desc.function_name,
                col_desc.be_exec_version);
        break;
    case TYPE_JSONB:
        nested = std::make_shared<vectorized::DataTypeJsonb>();
        break;
    case TYPE_HLL:
        nested = std::make_shared<vectorized::DataTypeHLL>();
        break;
    case TYPE_OBJECT:
        nested = std::make_shared<vectorized::DataTypeBitMap>();
        break;
    case TYPE_DECIMALV2:
        nested = std::make_shared<vectorized::DataTypeDecimal<vectorized::Decimal128V2>>(
                27, 9, col_desc.precision, col_desc.scale);
        break;
    case TYPE_QUANTILE_STATE:
        nested = std::make_shared<vectorized::DataTypeQuantileState>();
        break;
    case TYPE_DECIMAL32:
    case TYPE_DECIMAL64:
    case TYPE_DECIMAL128I:
    case TYPE_DECIMAL256:
        nested = vectorized::create_decimal(col_desc.precision, col_desc.scale, false);
        break;
    // Just Mock A NULL Type in Vec Exec Engine
    case TYPE_NULL:
        nested = std::make_shared<vectorized::DataTypeUInt8>();
        const_cast<vectorized::DataTypeUInt8&>(
                reinterpret_cast<const vectorized::DataTypeUInt8&>(*nested))
                .set_null_literal(true);
        break;
    case TYPE_ARRAY:
        DCHECK(col_desc.children.size() == 1);
        nested = std::make_shared<vectorized::DataTypeArray>(
                create_data_type(col_desc.children[0], col_desc.contains_nulls[0]));
        break;
    case TYPE_MAP:
        DCHECK(col_desc.children.size() == 2);
        DCHECK_EQ(col_desc.contains_nulls.size(), 2);
        nested = std::make_shared<vectorized::DataTypeMap>(
                create_data_type(col_desc.children[0], col_desc.contains_nulls[0]),
                create_data_type(col_desc.children[1], col_desc.contains_nulls[1]));
        break;
    case TYPE_STRUCT: {
        DCHECK(col_desc.children.size() >= 1);
        size_t child_size = col_desc.children.size();
        DCHECK_EQ(col_desc.field_names.size(), child_size);
        DataTypes dataTypes;
        Strings names;
        dataTypes.reserve(child_size);
        names.reserve(child_size);
        for (size_t i = 0; i < child_size; i++) {
            dataTypes.push_back(create_data_type(col_desc.children[i], col_desc.contains_nulls[i]));
            names.push_back(col_desc.field_names[i]);
        }
        nested = std::make_shared<DataTypeStruct>(dataTypes, names);
        break;
    }
    case INVALID_TYPE:
    default:
        throw Exception(ErrorCode::INTERNAL_ERROR, "invalid PrimitiveType: {}", (int)col_desc.type);
        break;
    }

    if (nested && is_nullable) {
        return make_nullable(nested);
    }
    return nested;
}

DataTypePtr DataTypeFactory::create_data_type(const TypeIndex& type_index, bool is_nullable) {
    DataTypePtr nested = nullptr;
    switch (type_index) {
    case TypeIndex::UInt8:
        nested = std::make_shared<vectorized::DataTypeUInt8>();
        break;
    case TypeIndex::Int8:
        nested = std::make_shared<vectorized::DataTypeInt8>();
        break;
    case TypeIndex::UInt16:
        nested = std::make_shared<vectorized::DataTypeUInt16>();
        break;
    case TypeIndex::Int16:
        nested = std::make_shared<vectorized::DataTypeInt16>();
        break;
    case TypeIndex::UInt32:
        nested = std::make_shared<vectorized::DataTypeUInt32>();
        break;
    case TypeIndex::Int32:
        nested = std::make_shared<vectorized::DataTypeInt32>();
        break;
    case TypeIndex::UInt64:
        nested = std::make_shared<vectorized::DataTypeUInt64>();
        break;
    case TypeIndex::Int64:
        nested = std::make_shared<vectorized::DataTypeInt64>();
        break;
    case TypeIndex::Int128:
        nested = std::make_shared<vectorized::DataTypeInt128>();
        break;
    case TypeIndex::IPv4:
        nested = std::make_shared<vectorized::DataTypeIPv4>();
        break;
    case TypeIndex::IPv6:
        nested = std::make_shared<vectorized::DataTypeIPv6>();
        break;
    case TypeIndex::Float32:
        nested = std::make_shared<vectorized::DataTypeFloat32>();
        break;
    case TypeIndex::Float64:
        nested = std::make_shared<vectorized::DataTypeFloat64>();
        break;
    case TypeIndex::Date:
        nested = std::make_shared<vectorized::DataTypeDate>();
        break;
    case TypeIndex::DateV2:
        nested = std::make_shared<vectorized::DataTypeDateV2>();
        break;
    case TypeIndex::DateTimeV2:
        nested = std::make_shared<DataTypeDateTimeV2>();
        break;
    case TypeIndex::DateTime:
        nested = std::make_shared<vectorized::DataTypeDateTime>();
        break;
    case TypeIndex::String:
        nested = std::make_shared<vectorized::DataTypeString>();
        break;
    case TypeIndex::VARIANT:
        nested = std::make_shared<vectorized::DataTypeObject>("", true);
        break;
    case TypeIndex::Decimal32:
        nested = std::make_shared<DataTypeDecimal<Decimal32>>(BeConsts::MAX_DECIMAL32_PRECISION, 0);
        break;
    case TypeIndex::Decimal64:
        nested = std::make_shared<DataTypeDecimal<Decimal64>>(BeConsts::MAX_DECIMAL64_PRECISION, 0);
        break;
    case TypeIndex::Decimal128V2:
        nested = std::make_shared<DataTypeDecimal<Decimal128V2>>(BeConsts::MAX_DECIMALV2_PRECISION,
                                                                 0);
        break;
    case TypeIndex::Decimal128V3:
        nested = std::make_shared<DataTypeDecimal<Decimal128V3>>(BeConsts::MAX_DECIMAL128_PRECISION,
                                                                 0);
        break;
    case TypeIndex::Decimal256:
        nested = std::make_shared<DataTypeDecimal<Decimal256>>(BeConsts::MAX_DECIMAL256_PRECISION,
                                                               0);
        break;
    case TypeIndex::JSONB:
        nested = std::make_shared<vectorized::DataTypeJsonb>();
        break;
    case TypeIndex::BitMap:
        nested = std::make_shared<vectorized::DataTypeBitMap>();
        break;
    case TypeIndex::HLL:
        nested = std::make_shared<vectorized::DataTypeHLL>();
        break;
    case TypeIndex::QuantileState:
        nested = std::make_shared<vectorized::DataTypeQuantileState>();
        break;
    case TypeIndex::TimeV2:
    case TypeIndex::Time:
        nested = std::make_shared<vectorized::DataTypeTimeV2>();
        break;
    default:
        DCHECK(false) << "invalid typeindex:" << getTypeName(type_index);
        break;
    }

    if (nested && is_nullable) {
        return make_nullable(nested);
    }
    return nested;
}

DataTypePtr DataTypeFactory::_create_primitive_data_type(const FieldType& type, int precision,
                                                         int scale) const {
    DataTypePtr result = nullptr;
    switch (type) {
    case FieldType::OLAP_FIELD_TYPE_BOOL:
        result = std::make_shared<vectorized::DataTypeUInt8>();
        break;
    case FieldType::OLAP_FIELD_TYPE_TINYINT:
        result = std::make_shared<vectorized::DataTypeInt8>();
        break;
    case FieldType::OLAP_FIELD_TYPE_SMALLINT:
        result = std::make_shared<vectorized::DataTypeInt16>();
        break;
    case FieldType::OLAP_FIELD_TYPE_INT:
        result = std::make_shared<vectorized::DataTypeInt32>();
        break;
    case FieldType::OLAP_FIELD_TYPE_FLOAT:
        result = std::make_shared<vectorized::DataTypeFloat32>();
        break;
    case FieldType::OLAP_FIELD_TYPE_BIGINT:
        result = std::make_shared<vectorized::DataTypeInt64>();
        break;
    case FieldType::OLAP_FIELD_TYPE_LARGEINT:
        result = std::make_shared<vectorized::DataTypeInt128>();
        break;
    case FieldType::OLAP_FIELD_TYPE_IPV4:
        result = std::make_shared<vectorized::DataTypeIPv4>();
        break;
    case FieldType::OLAP_FIELD_TYPE_IPV6:
        result = std::make_shared<vectorized::DataTypeIPv6>();
        break;
    case FieldType::OLAP_FIELD_TYPE_DATE:
        result = std::make_shared<vectorized::DataTypeDate>();
        break;
    case FieldType::OLAP_FIELD_TYPE_DATEV2:
        result = std::make_shared<vectorized::DataTypeDateV2>();
        break;
    case FieldType::OLAP_FIELD_TYPE_DATETIMEV2:
        result = vectorized::create_datetimev2(scale);
        break;
    case FieldType::OLAP_FIELD_TYPE_DATETIME:
        result = std::make_shared<vectorized::DataTypeDateTime>();
        break;
    case FieldType::OLAP_FIELD_TYPE_DOUBLE:
        result = std::make_shared<vectorized::DataTypeFloat64>();
        break;
    case FieldType::OLAP_FIELD_TYPE_CHAR:
    case FieldType::OLAP_FIELD_TYPE_VARCHAR:
    case FieldType::OLAP_FIELD_TYPE_STRING:
        result = std::make_shared<vectorized::DataTypeString>();
        break;
    case FieldType::OLAP_FIELD_TYPE_VARIANT:
        result = std::make_shared<vectorized::DataTypeObject>("", true);
        break;
    case FieldType::OLAP_FIELD_TYPE_JSONB:
        result = std::make_shared<vectorized::DataTypeJsonb>();
        break;
    case FieldType::OLAP_FIELD_TYPE_HLL:
        result = std::make_shared<vectorized::DataTypeHLL>();
        break;
    case FieldType::OLAP_FIELD_TYPE_OBJECT:
        result = std::make_shared<vectorized::DataTypeBitMap>();
        break;
    case FieldType::OLAP_FIELD_TYPE_DECIMAL:
        result = std::make_shared<vectorized::DataTypeDecimal<vectorized::Decimal128V2>>(
                27, 9, precision, scale);
        break;
    case FieldType::OLAP_FIELD_TYPE_QUANTILE_STATE:
        result = std::make_shared<vectorized::DataTypeQuantileState>();
        break;
    case FieldType::OLAP_FIELD_TYPE_DECIMAL32:
    case FieldType::OLAP_FIELD_TYPE_DECIMAL64:
    case FieldType::OLAP_FIELD_TYPE_DECIMAL128I:
    case FieldType::OLAP_FIELD_TYPE_DECIMAL256:
        result = vectorized::create_decimal(precision, scale, false);
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
    case PGenericType::IPV4:
        nested = std::make_shared<DataTypeIPv4>();
        break;
    case PGenericType::IPV6:
        nested = std::make_shared<DataTypeIPv6>();
        break;
    case PGenericType::STRING:
        nested = std::make_shared<DataTypeString>();
        break;
    case PGenericType::VARIANT:
        nested = std::make_shared<DataTypeObject>("", true);
        break;
    case PGenericType::JSONB:
        nested = std::make_shared<DataTypeJsonb>();
        break;
    case PGenericType::DATE:
        nested = std::make_shared<DataTypeDate>();
        break;
    case PGenericType::DATEV2:
        nested = std::make_shared<DataTypeDateV2>();
        break;
    case PGenericType::DATETIMEV2:
        nested = std::make_shared<DataTypeDateTimeV2>(pcolumn.decimal_param().scale());
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
        nested = std::make_shared<DataTypeDecimal<Decimal128V2>>(
                pcolumn.decimal_param().precision(), pcolumn.decimal_param().scale());
        break;
    case PGenericType::DECIMAL128I:
        nested = std::make_shared<DataTypeDecimal<Decimal128V3>>(
                pcolumn.decimal_param().precision(), pcolumn.decimal_param().scale());
        break;
    case PGenericType::DECIMAL256:
        nested = std::make_shared<DataTypeDecimal<Decimal256>>(pcolumn.decimal_param().precision(),
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
    case PGenericType::FIXEDLENGTHOBJECT:
        nested = std::make_shared<DataTypeFixedLengthObject>();
        break;
    case PGenericType::MAP:
        DCHECK(pcolumn.children_size() == 2);
        // here to check pcolumn is list?
        nested = std::make_shared<vectorized::DataTypeMap>(create_data_type(pcolumn.children(0)),
                                                           create_data_type(pcolumn.children(1)));
        break;
    case PGenericType::STRUCT: {
        size_t col_size = pcolumn.children_size();
        DCHECK(col_size >= 1);
        DataTypes dataTypes;
        Strings names;
        dataTypes.reserve(col_size);
        names.reserve(col_size);
        for (size_t i = 0; i < col_size; i++) {
            dataTypes.push_back(create_data_type(pcolumn.children(i)));
            names.push_back(pcolumn.children(i).name());
        }
        nested = std::make_shared<DataTypeStruct>(dataTypes, names);
        break;
    }
    case PGenericType::QUANTILE_STATE: {
        nested = std::make_shared<DataTypeQuantileState>();
        break;
    }
    case PGenericType::TIME:
    case PGenericType::TIMEV2: {
        nested = std::make_shared<DataTypeTimeV2>(pcolumn.decimal_param().scale());
        break;
    }
    case PGenericType::AGG_STATE: {
        DataTypes sub_types;
        for (auto child : pcolumn.children()) {
            sub_types.push_back(create_data_type(child));
        }
        nested = std::make_shared<DataTypeAggState>(sub_types, pcolumn.result_is_nullable(),
                                                    pcolumn.function_name(),
                                                    pcolumn.be_exec_version());
        break;
    }
    default: {
        LOG(FATAL) << fmt::format("Unknown data type: {}", pcolumn.type());
        return nullptr;
    }
    }

    if (nested && pcolumn.is_nullable() > 0) {
        return make_nullable(nested);
    }
    return nested;
}

DataTypePtr DataTypeFactory::create_data_type(const segment_v2::ColumnMetaPB& pcolumn) {
    DataTypePtr nested = nullptr;
    if (pcolumn.type() == static_cast<int>(FieldType::OLAP_FIELD_TYPE_AGG_STATE)) {
        DataTypes data_types;
        for (auto child : pcolumn.children_columns()) {
            data_types.push_back(DataTypeFactory::instance().create_data_type(child));
        }
        nested = std::make_shared<vectorized::DataTypeAggState>(
                data_types, pcolumn.result_is_nullable(), pcolumn.function_name(),
                pcolumn.be_exec_version());
    } else if (pcolumn.type() == static_cast<int>(FieldType::OLAP_FIELD_TYPE_ARRAY)) {
        // Item subcolumn and length subcolumn, for sparse columns only subcolumn
        DCHECK_GE(pcolumn.children_columns().size(), 1) << pcolumn.DebugString();
        nested = std::make_shared<DataTypeArray>(create_data_type(pcolumn.children_columns(0)));
    } else if (pcolumn.type() == static_cast<int>(FieldType::OLAP_FIELD_TYPE_MAP)) {
        DCHECK_GE(pcolumn.children_columns().size(), 2) << pcolumn.DebugString();
        nested = std::make_shared<vectorized::DataTypeMap>(
                create_data_type(pcolumn.children_columns(0)),
                create_data_type(pcolumn.children_columns(1)));
    } else if (pcolumn.type() == static_cast<int>(FieldType::OLAP_FIELD_TYPE_STRUCT)) {
        DCHECK_GE(pcolumn.children_columns().size(), 1);
        size_t col_size = pcolumn.children_columns().size();
        DataTypes dataTypes(col_size);
        Strings names(col_size);
        for (size_t i = 0; i < col_size; i++) {
            dataTypes[i] = create_data_type(pcolumn.children_columns(i));
        }
        nested = std::make_shared<DataTypeStruct>(dataTypes, names);
    } else {
        // TODO add precision and frac
        nested = _create_primitive_data_type(static_cast<FieldType>(pcolumn.type()),
                                             pcolumn.precision(), pcolumn.frac());
    }

    if (pcolumn.is_nullable() && nested) {
        return make_nullable(nested);
    }
    return nested;
}

DataTypePtr DataTypeFactory::create_data_type(const arrow::DataType* type, bool is_nullable) {
    DataTypePtr nested = nullptr;
    switch (type->id()) {
    case ::arrow::Type::BOOL:
        nested = std::make_shared<vectorized::DataTypeUInt8>();
        break;
    case ::arrow::Type::INT8:
        nested = std::make_shared<vectorized::DataTypeInt8>();
        break;
    case ::arrow::Type::UINT8:
        nested = std::make_shared<vectorized::DataTypeUInt8>();
        break;
    case ::arrow::Type::INT16:
        nested = std::make_shared<vectorized::DataTypeInt16>();
        break;
    case ::arrow::Type::UINT16:
        nested = std::make_shared<vectorized::DataTypeUInt16>();
        break;
    case ::arrow::Type::INT32:
        nested = std::make_shared<vectorized::DataTypeInt32>();
        break;
    case ::arrow::Type::UINT32:
        nested = std::make_shared<vectorized::DataTypeUInt32>();
        break;
    case ::arrow::Type::INT64:
        nested = std::make_shared<vectorized::DataTypeInt64>();
        break;
    case ::arrow::Type::UINT64:
        nested = std::make_shared<vectorized::DataTypeUInt64>();
        break;
    case ::arrow::Type::HALF_FLOAT:
    case ::arrow::Type::FLOAT:
        nested = std::make_shared<vectorized::DataTypeFloat32>();
        break;
    case ::arrow::Type::DOUBLE:
        nested = std::make_shared<vectorized::DataTypeFloat64>();
        break;
    case ::arrow::Type::DATE32:
        nested = std::make_shared<vectorized::DataTypeDate>();
        break;
    case ::arrow::Type::DATE64:
    case ::arrow::Type::TIMESTAMP:
        nested = std::make_shared<vectorized::DataTypeDateTime>();
        break;
    case ::arrow::Type::BINARY:
    case ::arrow::Type::FIXED_SIZE_BINARY:
    case ::arrow::Type::STRING:
        nested = std::make_shared<vectorized::DataTypeString>();
        break;
    case ::arrow::Type::DECIMAL:
        nested = std::make_shared<vectorized::DataTypeDecimal<vectorized::Decimal128V2>>();
        break;
    case ::arrow::Type::LIST:
        DCHECK(type->num_fields() == 1);
        nested = std::make_shared<vectorized::DataTypeArray>(
                create_data_type(type->field(0)->type().get(), true));
        break;
    case ::arrow::Type::MAP:
        DCHECK(type->num_fields() == 2);
        nested = std::make_shared<vectorized::DataTypeMap>(
                create_data_type(type->field(0)->type().get(), true),
                create_data_type(type->field(1)->type().get(), true));
        break;
    case ::arrow::Type::STRUCT: {
        size_t field_num = type->num_fields();
        DCHECK(type->num_fields() >= 1);
        vectorized::DataTypes dataTypes;
        vectorized::Strings names;
        dataTypes.reserve(field_num);
        names.reserve(field_num);
        for (size_t i = 0; i < field_num; i++) {
            dataTypes.push_back(create_data_type(type->field(i)->type().get(), true));
            names.push_back(type->field(i)->name());
        }
        nested = std::make_shared<vectorized::DataTypeStruct>(dataTypes, names);
        break;
    }
    default:
        DCHECK(false) << "invalid arrow type:" << (int)(type->id());
        break;
    }

    if (nested && is_nullable) {
        return make_nullable(nested);
    }
    return nested;
}

} // namespace doris::vectorized
