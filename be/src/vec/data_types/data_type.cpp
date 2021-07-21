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

#include "vec/data_types/data_type.h"

#include <fmt/format.h>

#include "common/logging.h"
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
}

void IDataType::insert_default_into(IColumn& column) const {
    column.insert_default();
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
        case TYPE_HLL:
            result = std::make_shared<DataTypeString>();
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

    // For llvm complain
    return result;
}

} // namespace doris::vectorized
