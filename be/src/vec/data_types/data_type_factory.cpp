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
#include "vec/data_types/data_type_date_or_datetime_v2.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_fixed_length_object.h"
#include "vec/data_types/data_type_hll.h"
#include "vec/data_types/data_type_ipv4.h"
#include "vec/data_types/data_type_ipv6.h"
#include "vec/data_types/data_type_jsonb.h"
#include "vec/data_types/data_type_map.h"
#include "vec/data_types/data_type_nothing.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_quantilestate.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_struct.h"
#include "vec/data_types/data_type_variant.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"
DataTypePtr DataTypeFactory::create_data_type(const doris::Field& col_desc) {
    return create_data_type(col_desc.get_desc(), col_desc.is_nullable());
}

DataTypePtr DataTypeFactory::create_data_type(const TabletColumn& col_desc, bool is_nullable) {
    DataTypePtr nested = nullptr;
    if (col_desc.type() == FieldType::OLAP_FIELD_TYPE_AGG_STATE) {
        DataTypes dataTypes;
        for (UInt32 i = 0; i < col_desc.get_subtype_count(); i++) {
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
        for (UInt32 i = 0; i < col_size; i++) {
            dataTypes.push_back(create_data_type(col_desc.get_sub_column(i)));
            names.push_back(col_desc.get_sub_column(i).name());
        }
        nested = std::make_shared<DataTypeStruct>(dataTypes, names);
    } else if (col_desc.type() == FieldType::OLAP_FIELD_TYPE_VARIANT) {
        nested = std::make_shared<DataTypeVariant>(col_desc.variant_max_subcolumns_count());
    } else {
        nested =
                _create_primitive_data_type(col_desc.type(), col_desc.precision(), col_desc.frac());
    }

    if ((is_nullable || col_desc.is_nullable()) && nested) {
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
        result = std::make_shared<vectorized::DataTypeString>(-1, TYPE_CHAR);
        break;
    case FieldType::OLAP_FIELD_TYPE_VARCHAR:
        result = std::make_shared<vectorized::DataTypeString>(-1, TYPE_VARCHAR);
        break;
    case FieldType::OLAP_FIELD_TYPE_STRING:
        result = std::make_shared<vectorized::DataTypeString>(-1, TYPE_STRING);
        break;
    case FieldType::OLAP_FIELD_TYPE_VARIANT:
        result = std::make_shared<vectorized::DataTypeVariant>(0);
        break;
    case FieldType::OLAP_FIELD_TYPE_JSONB:
        result = std::make_shared<vectorized::DataTypeJsonb>();
        break;
    case FieldType::OLAP_FIELD_TYPE_HLL:
        result = std::make_shared<vectorized::DataTypeHLL>();
        break;
    case FieldType::OLAP_FIELD_TYPE_BITMAP:
        result = std::make_shared<vectorized::DataTypeBitMap>();
        break;
    case FieldType::OLAP_FIELD_TYPE_DECIMAL:
        result = std::make_shared<vectorized::DataTypeDecimalV2>(27, 9, precision, scale);
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
        nested = std::make_shared<DataTypeVariant>(pcolumn.variant_max_subcolumns_count());
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
        nested = std::make_shared<DataTypeDecimal32>(pcolumn.decimal_param().precision(),
                                                     pcolumn.decimal_param().scale());
        break;
    case PGenericType::DECIMAL64:
        nested = std::make_shared<DataTypeDecimal64>(pcolumn.decimal_param().precision(),
                                                     pcolumn.decimal_param().scale());
        break;
    case PGenericType::DECIMAL128:
        nested = std::make_shared<DataTypeDecimalV2>(pcolumn.decimal_param().precision(),
                                                     pcolumn.decimal_param().scale());
        break;
    case PGenericType::DECIMAL128I:
        nested = std::make_shared<DataTypeDecimal128>(pcolumn.decimal_param().precision(),
                                                      pcolumn.decimal_param().scale());
        break;
    case PGenericType::DECIMAL256:
        nested = std::make_shared<DataTypeDecimal256>(pcolumn.decimal_param().precision(),
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
        int col_size = pcolumn.children_size();
        DCHECK(col_size >= 1);
        DataTypes dataTypes;
        Strings names;
        dataTypes.reserve(col_size);
        names.reserve(col_size);
        for (int i = 0; i < col_size; i++) {
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
        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "Unknown data type: {}", pcolumn.type());
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
            auto type = DataTypeFactory::instance().create_data_type(child);
            // may have length column with OLAP_FIELD_TYPE_UNSIGNED_BIGINT, then type will be nullptr
            if (type) {
                data_types.push_back(type);
            }
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
        Int32 col_size = pcolumn.children_columns().size();
        DataTypes dataTypes(col_size);
        Strings names(col_size);
        for (Int32 i = 0; i < col_size; i++) {
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

DataTypePtr DataTypeFactory::create_data_type(const PrimitiveType primitive_type, bool is_nullable,
                                              int precision, int scale, int len) {
    DataTypePtr nested = nullptr;
    DataTypes subTypes;
    switch (primitive_type) {
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
        nested = vectorized::create_datetimev2(scale);
        break;
    case TYPE_DATETIME:
        nested = std::make_shared<vectorized::DataTypeDateTime>();
        break;
    case TYPE_TIMEV2:
        nested = std::make_shared<vectorized::DataTypeTimeV2>(scale);
        break;
    case TYPE_DOUBLE:
        nested = std::make_shared<vectorized::DataTypeFloat64>();
        break;
    case TYPE_VARIANT:
        nested = std::make_shared<vectorized::DataTypeVariant>(0);
        break;
    case TYPE_STRING:
    case TYPE_CHAR:
    case TYPE_VARCHAR:
        nested = std::make_shared<vectorized::DataTypeString>(len, primitive_type);
        break;
    case TYPE_BINARY:
    case TYPE_LAMBDA_FUNCTION:
        nested = std::make_shared<vectorized::DataTypeString>(len, TYPE_STRING);
        break;
    case TYPE_JSONB:
        nested = std::make_shared<vectorized::DataTypeJsonb>();
        break;
    case TYPE_HLL:
        nested = std::make_shared<vectorized::DataTypeHLL>();
        break;
    case TYPE_BITMAP:
        nested = std::make_shared<vectorized::DataTypeBitMap>();
        break;
    case TYPE_DECIMALV2:
        nested = std::make_shared<vectorized::DataTypeDecimalV2>(
                precision > 0 ? precision : 0, precision > 0 ? scale : 0, precision, scale);
        break;
    case TYPE_QUANTILE_STATE:
        nested = std::make_shared<vectorized::DataTypeQuantileState>();
        break;
    case TYPE_DECIMAL32:
    case TYPE_DECIMAL64:
    case TYPE_DECIMAL128I:
    case TYPE_DECIMAL256:
        nested = vectorized::create_decimal(precision, scale, false);
        break;
    // Just Mock A NULL Type in Vec Exec Engine
    case TYPE_NULL:
        nested = std::make_shared<vectorized::DataTypeUInt8>();
        const_cast<vectorized::DataTypeUInt8&>(
                reinterpret_cast<const vectorized::DataTypeUInt8&>(*nested))
                .set_null_literal(true);
        break;
    case TYPE_AGG_STATE:
    case TYPE_ARRAY:
    case TYPE_MAP:
    case TYPE_STRUCT: {
        throw Exception(ErrorCode::INTERNAL_ERROR, "Nested type should not reach here {}",
                        type_to_string(primitive_type));
        break;
    }
    default:
        throw Exception(ErrorCode::INTERNAL_ERROR, "invalid PrimitiveType: {}",
                        type_to_string(primitive_type));
        break;
    }

    if (nested && is_nullable) {
        return make_nullable(nested);
    }
    return nested;
}

DataTypePtr DataTypeFactory::create_data_type(const std::vector<TTypeNode>& types, int* idx,
                                              bool is_nullable) {
    DCHECK_GE(*idx, 0);
    DCHECK_LT(*idx, types.size());
    DataTypePtr nested = nullptr;
    const TTypeNode& node = types[*idx];
    switch (node.type) {
    case TTypeNodeType::SCALAR: {
        DCHECK(node.__isset.scalar_type);
        const TScalarType& scalar_type = node.scalar_type;
        if (scalar_type.type == TPrimitiveType::VARIANT) {
            DCHECK(scalar_type.variant_max_subcolumns_count >= 0)
                    << "count is: " << scalar_type.variant_max_subcolumns_count;
            return is_nullable ? make_nullable(std::make_shared<vectorized::DataTypeVariant>(
                                         scalar_type.variant_max_subcolumns_count))
                               : std::make_shared<vectorized::DataTypeVariant>(
                                         scalar_type.variant_max_subcolumns_count);
        }
        return create_data_type(thrift_to_type(scalar_type.type), is_nullable,
                                scalar_type.__isset.precision ? scalar_type.precision : 0,
                                scalar_type.__isset.scale ? scalar_type.scale : 0,
                                scalar_type.__isset.len ? scalar_type.len : -1);
    }
    case TTypeNodeType::ARRAY: {
        DCHECK(!node.__isset.scalar_type);
        DCHECK_LT(*idx, types.size() - 1);
        DCHECK(node.__isset.contains_nulls && node.contains_nulls.size() == 1)
                << node.__isset.contains_nulls
                << " size: " << (node.__isset.contains_nulls ? node.contains_nulls.size() : 0);
        ++(*idx);
        nested = std::make_shared<vectorized::DataTypeArray>(
                create_data_type(types, idx, node.contains_nulls[0]));
        break;
    }
    case TTypeNodeType::STRUCT: {
        DCHECK(!node.__isset.scalar_type);
        DCHECK_LT(*idx, types.size() - 1);
        DCHECK(!node.__isset.contains_nulls);
        DCHECK(node.__isset.struct_fields);
        DCHECK_GE(node.struct_fields.size(), 1);
        DataTypes data_types;
        Strings names;
        data_types.reserve(node.struct_fields.size());
        names.reserve(node.struct_fields.size());
        for (size_t i = 0; i < node.struct_fields.size(); i++) {
            ++(*idx);
            data_types.push_back(create_data_type(types, idx, node.struct_fields[i].contains_null));
            names.push_back(node.struct_fields[i].name);
        }
        nested = std::make_shared<DataTypeStruct>(data_types, names);
        break;
    }
    case TTypeNodeType::MAP: {
        DCHECK(!node.__isset.scalar_type);
        DCHECK_LT(*idx, types.size() - 2);
        DCHECK_EQ(node.contains_nulls.size(), 2);
        DataTypes data_types;
        data_types.reserve(2);
        for (size_t i = 0; i < 2; i++) {
            ++(*idx);
            data_types.push_back(create_data_type(types, idx, node.contains_nulls[i]));
        }
        nested = std::make_shared<vectorized::DataTypeMap>(data_types[0], data_types[1]);
        break;
    }
    default:
        throw Exception(ErrorCode::INTERNAL_ERROR, "invalid PrimitiveType: {}", (int)node.type);
        break;
    }
    if (nested && is_nullable) {
        return make_nullable(nested);
    }
    return nested;
}

DataTypePtr DataTypeFactory::create_data_type(
        const google::protobuf::RepeatedPtrField<PTypeNode>& types, int* idx, bool is_nullable) {
    DCHECK_GE(*idx, 0);
    DCHECK_LT(*idx, types.size());

    const PTypeNode& node = types.Get(*idx);
    DataTypePtr nested = nullptr;
    switch (node.type()) {
    case TTypeNodeType::SCALAR: {
        DCHECK(node.has_scalar_type());
        const PScalarType& scalar_type = node.scalar_type();
        // FIXME(gabriel): LoadChannel will set nested type as scalar type by DataType::to_protobuf
        auto primitive_type = thrift_to_type((TPrimitiveType::type)scalar_type.type());
        if (primitive_type == TYPE_ARRAY) {
            ++(*idx);
            nested = std::make_shared<vectorized::DataTypeArray>(create_data_type(
                    types, idx, node.has_contains_null() ? node.has_contains_null() : true));
        } else if (primitive_type == TYPE_MAP) {
            DataTypes data_types;
            data_types.resize(2, nullptr);
            for (size_t i = 0; i < 2; i++) {
                ++(*idx);
                data_types[i] = create_data_type(types, idx,
                                                 node.contains_nulls_size() > 1
                                                         ? node.contains_nulls(cast_set<int>(i))
                                                         : true);
            }
            nested = std::make_shared<vectorized::DataTypeMap>(data_types[0], data_types[1]);
        } else if (primitive_type == TYPE_STRUCT) {
            DataTypes data_types;
            Strings names;
            data_types.reserve(node.struct_fields_size());
            names.reserve(node.struct_fields_size());
            for (size_t i = 0; i < node.struct_fields_size(); i++) {
                const auto& field = node.struct_fields(cast_set<int>(i));
                ++(*idx);
                data_types.push_back(create_data_type(types, idx, field.contains_null()));
                names.push_back(field.name());
            }
            nested = std::make_shared<DataTypeStruct>(data_types, names);
        } else if (primitive_type == TYPE_AGG_STATE) {
            // Do nothing
            nested = std::make_shared<DataTypeAggState>();
        } else if (primitive_type == TYPE_VARIANT) {
            nested = std::make_shared<DataTypeVariant>(node.variant_max_subcolumns_count());
        } else {
            return create_data_type(primitive_type, is_nullable,
                                    scalar_type.has_precision() ? scalar_type.precision() : 0,
                                    scalar_type.has_scale() ? scalar_type.scale() : 0);
        }
        break;
    }
    case TTypeNodeType::ARRAY: {
        ++(*idx);
        nested = std::make_shared<vectorized::DataTypeArray>(create_data_type(
                types, idx, node.has_contains_null() ? node.has_contains_null() : true));
        break;
    }
    case TTypeNodeType::MAP: {
        DataTypes data_types;
        data_types.reserve(2);
        for (size_t i = 0; i < 2; i++) {
            ++(*idx);
            data_types.push_back(create_data_type(
                    types, idx,
                    node.contains_nulls_size() > 1 ? node.contains_nulls(cast_set<int>(i)) : true));
        }
        nested = std::make_shared<vectorized::DataTypeMap>(data_types[0], data_types[1]);
        break;
    }
    case TTypeNodeType::STRUCT: {
        DataTypes data_types;
        Strings names;
        data_types.reserve(node.struct_fields_size());
        names.reserve(node.struct_fields_size());
        for (size_t i = 0; i < node.struct_fields_size(); i++) {
            const auto& field = node.struct_fields(cast_set<int>(i));
            ++(*idx);
            data_types.push_back(create_data_type(types, idx, field.contains_null()));
            names.push_back(field.name());
        }
        nested = std::make_shared<DataTypeStruct>(data_types, names);
        break;
    }
    case TTypeNodeType::VARIANT: {
        nested = std::make_shared<DataTypeVariant>(node.variant_max_subcolumns_count());
        break;
    }
    default:
        throw Exception(ErrorCode::INTERNAL_ERROR, "invalid TTypeNodeType: {}", (int)node.type());
    }
    if (nested && is_nullable) {
        return make_nullable(nested);
    }
    return nested;
}

DataTypePtr DataTypeFactory::create_data_type(const TTypeDesc& t) {
    DataTypePtr nested = nullptr;

    auto is_agg_state = false;
    if (t.types[0].type == TTypeNodeType::SCALAR) {
        DCHECK(t.types[0].__isset.scalar_type);
        const TScalarType& scalar_type = t.types[0].scalar_type;
        is_agg_state = thrift_to_type(scalar_type.type) == TYPE_AGG_STATE;
    }
    if (is_agg_state) {
        DCHECK(t.__isset.sub_types);
        DataTypes subTypes;
        for (auto sub : t.sub_types) {
            subTypes.push_back(create_data_type(sub, sub.is_nullable));
        }
        DCHECK(t.__isset.result_is_nullable);
        DCHECK(t.__isset.function_name);
        nested = std::make_shared<vectorized::DataTypeAggState>(subTypes, t.result_is_nullable,
                                                                t.function_name, t.be_exec_version);
        return t.is_nullable ? make_nullable(nested) : nested;
    } else {
        int idx = 0;
        nested = create_data_type(t.types, &idx, t.is_nullable);
        DCHECK_EQ(idx, t.types.size() - 1);
        return nested;
    }
}

DataTypePtr DataTypeFactory::create_data_type(const TTypeDesc& t, bool is_nullable) {
    DataTypePtr nested = nullptr;

    auto is_agg_state = false;
    if (t.types[0].type == TTypeNodeType::SCALAR) {
        DCHECK(t.types[0].__isset.scalar_type);
        const TScalarType& scalar_type = t.types[0].scalar_type;
        is_agg_state = thrift_to_type(scalar_type.type) == TYPE_AGG_STATE;
    }
    if (is_agg_state) {
        DCHECK(t.__isset.sub_types);
        DataTypes subTypes;
        for (auto sub : t.sub_types) {
            subTypes.push_back(create_data_type(sub, sub.is_nullable));
        }
        DCHECK(t.__isset.result_is_nullable);
        DCHECK(t.__isset.function_name);
        nested = std::make_shared<vectorized::DataTypeAggState>(subTypes, t.result_is_nullable,
                                                                t.function_name, t.be_exec_version);
        return is_nullable ? make_nullable(nested) : nested;
    } else {
        int idx = 0;
        nested = create_data_type(t.types, &idx, is_nullable);
        DCHECK_EQ(idx, t.types.size() - 1);
        return nested;
    }
}

} // namespace doris::vectorized
