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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/DataTypes/IDataType.cpp
// and modified by Doris

#include "vec/data_types/data_type.h"

#include <fmt/format.h>

#include "common/logging.h"
#include "olap/olap_common.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/data_types/data_type_bitmap.h"
#include "vec/data_types/data_type_date.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_nothing.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_hll.h"

namespace doris::vectorized {

IDataType::IDataType() {}

IDataType::~IDataType() {}

String IDataType::get_name() const {
    return do_get_name();
}

String IDataType::do_get_name() const {
    return get_family_name();
}

void IDataType::update_avg_value_size_hint(const IColumn& column, double& avg_value_size_hint) {
    /// Update the average value size hint if amount of read rows isn't too small
    size_t column_size = column.size();
    if (column_size > 10) {
        double current_avg_value_size = static_cast<double>(column.byte_size()) / column_size;

        /// Heuristic is chosen so that avg_value_size_hint increases rapidly but decreases slowly.
        if (current_avg_value_size > avg_value_size_hint)
            avg_value_size_hint = std::min(1024., current_avg_value_size); /// avoid overestimation
        else if (current_avg_value_size * 2 < avg_value_size_hint)
            avg_value_size_hint = (current_avg_value_size + avg_value_size_hint * 3) / 4;
    }
}

ColumnPtr IDataType::create_column_const(size_t size, const Field& field) const {
    auto column = create_column();
    column->insert(field);
    return ColumnConst::create(std::move(column), size);
}

ColumnPtr IDataType::create_column_const_with_default_value(size_t size) const {
    return create_column_const(size, get_default());
}

DataTypePtr IDataType::promote_numeric_type() const {
    LOG(FATAL) << fmt::format("Data type {} can't be promoted.", get_name());
    return nullptr;
}

size_t IDataType::get_size_of_value_in_memory() const {
    LOG(FATAL) << fmt::format("Value of type {} in memory is not of fixed size.", get_name());
    return 0;
}

void IDataType::to_string(const IColumn& column, size_t row_num, BufferWritable& ostr) const {
    LOG(FATAL) << fmt::format("Data type {} to_string not implement.", get_name());
}

std::string IDataType::to_string(const IColumn& column, size_t row_num) const {
    LOG(FATAL) << fmt::format("Data type {} to_string not implement.", get_name());
    return "";
}

void IDataType::insert_default_into(IColumn& column) const {
    column.insert_default();
}

void IDataType::to_pb_column_meta(PColumnMeta* col_meta) const {
    col_meta->set_type(get_pdata_type(this));
}

PGenericType_TypeId IDataType::get_pdata_type(const IDataType* data_type) {
    switch (data_type->get_type_id()) {
    case TypeIndex::UInt8:
        return PGenericType::UINT8;
    case TypeIndex::UInt16:
        return PGenericType::UINT16;
    case TypeIndex::UInt32:
        return PGenericType::UINT32;
    case TypeIndex::UInt64:
        return PGenericType::UINT64;
    case TypeIndex::UInt128:
        return PGenericType::UINT128;
    case TypeIndex::Int8:
        return PGenericType::INT8;
    case TypeIndex::Int16:
        return PGenericType::INT16;
    case TypeIndex::Int32:
        return PGenericType::INT32;
    case TypeIndex::Int64:
        return PGenericType::INT64;
    case TypeIndex::Int128:
        return PGenericType::INT128;
    case TypeIndex::Float32:
        return PGenericType::FLOAT;
    case TypeIndex::Float64:
        return PGenericType::DOUBLE;
    case TypeIndex::Decimal32:
        return PGenericType::DECIMAL32;
    case TypeIndex::Decimal64:
        return PGenericType::DECIMAL64;
    case TypeIndex::Decimal128:
        return PGenericType::DECIMAL128;
    case TypeIndex::String:
        return PGenericType::STRING;
    case TypeIndex::Date:
        return PGenericType::DATE;
    case TypeIndex::DateTime:
        return PGenericType::DATETIME;
    case TypeIndex::BitMap:
        return PGenericType::BITMAP;
    case TypeIndex::HLL:
        return PGenericType::HLL;
    default:
        return PGenericType::UNKNOWN;
    }
}

DataTypePtr IDataType::from_thrift(const doris::PrimitiveType& type, const bool is_nullable){
    DataTypePtr result;
    switch (type) {
        case TYPE_BOOLEAN:
            result = std::make_shared<DataTypeUInt8>();
            break;
        case TYPE_TINYINT:
            result = std::make_shared<DataTypeInt8>();
            break;
        case TYPE_SMALLINT:
            result = std::make_shared<DataTypeInt16>();
            break;
        case TYPE_INT:
            result = std::make_shared<DataTypeInt32>();
            break;
        case TYPE_FLOAT:
            result = std::make_shared<DataTypeFloat32>();
            break;
        case TYPE_BIGINT:
            result = std::make_shared<DataTypeInt64>();
            break;
        case TYPE_LARGEINT:
            result = std::make_shared<DataTypeInt128>();
            break;
        case TYPE_DATE:
            result = std::make_shared<DataTypeDate>();
            break;
        case TYPE_DATETIME:
            result = std::make_shared<DataTypeDateTime>();
            break;
        case TYPE_TIME:
        case TYPE_DOUBLE:
            result = std::make_shared<DataTypeFloat64>();
            break;
        case TYPE_CHAR:
        case TYPE_VARCHAR:
        case TYPE_STRING:
            result = std::make_shared<DataTypeString>();
            break;
        case TYPE_HLL:
            result = std::make_shared<DataTypeHLL>();
            break;        
        case TYPE_OBJECT:
            result = std::make_shared<DataTypeBitMap>();
            break;
        case TYPE_DECIMALV2:
            result = std::make_shared<DataTypeDecimal<Decimal128>>(27, 9);
            break;
        case TYPE_NULL:
            result = std::make_shared<DataTypeNothing>();
            break;
        case INVALID_TYPE:
        default:
            DCHECK(false);
            result = nullptr;
            break;
    }
    if (is_nullable) {
        result = std::make_shared<DataTypeNullable>(result);
    }

    return result;
}

DataTypePtr IDataType::from_olap_engine(const doris::FieldType & type, const _Bool is_nullable) {
    DataTypePtr result;
    switch (type) {
        case OLAP_FIELD_TYPE_BOOL:
            result = std::make_shared<DataTypeUInt8>();
            break;
        case OLAP_FIELD_TYPE_TINYINT:
            result = std::make_shared<DataTypeInt8>();
            break;
        case OLAP_FIELD_TYPE_SMALLINT:
            result = std::make_shared<DataTypeInt16>();
            break;
        case OLAP_FIELD_TYPE_INT:
            result = std::make_shared<DataTypeInt32>();
            break;
        case OLAP_FIELD_TYPE_FLOAT:
            result = std::make_shared<DataTypeFloat32>();
            break;
        case OLAP_FIELD_TYPE_BIGINT:
            result = std::make_shared<DataTypeInt64>();
            break;
        case OLAP_FIELD_TYPE_LARGEINT:
            result = std::make_shared<DataTypeInt128>();
            break;
        case OLAP_FIELD_TYPE_DATE:
            result = std::make_shared<DataTypeDate>();
            break;
        case OLAP_FIELD_TYPE_DATETIME:
            result = std::make_shared<DataTypeDateTime>();
            break;
        case OLAP_FIELD_TYPE_DOUBLE:
            result = std::make_shared<DataTypeFloat64>();
            break;
        case OLAP_FIELD_TYPE_CHAR:
        case OLAP_FIELD_TYPE_VARCHAR:
        case OLAP_FIELD_TYPE_STRING:
            result = std::make_shared<DataTypeString>();
            break;
        case OLAP_FIELD_TYPE_HLL:
            result = std::make_shared<DataTypeHLL>();
            break;        
        case OLAP_FIELD_TYPE_OBJECT:
            result = std::make_shared<DataTypeBitMap>();
            break;
        case OLAP_FIELD_TYPE_DECIMAL:
            result = std::make_shared<DataTypeDecimal<Decimal128>>(27, 9);
            break;

        default:
            DCHECK(false) << "Invalid olap engine type";
            result = nullptr;
            break;
    }
    if (is_nullable) {
        result = std::make_shared<DataTypeNullable>(result);
    }

    return result;
}
} // namespace doris::vectorized
