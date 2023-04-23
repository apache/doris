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
#include <gen_cpp/data.pb.h>
#include <gen_cpp/types.pb.h>

#include <algorithm>
#include <utility>

#include "common/logging.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/core/field.h"

namespace doris {
namespace vectorized {
class BufferWritable;
class ReadBuffer;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

IDataType::IDataType() = default;

IDataType::~IDataType() = default;

String IDataType::get_name() const {
    return do_get_name();
}

String IDataType::do_get_name() const {
    return get_family_name();
}

void IDataType::update_avg_value_size_hint(const IColumn& column, double& avg_value_size_hint) {
    /// Update the average value size hint if amount of read rows isn't too small
    size_t row_size = column.size();
    if (row_size > 10) {
        double current_avg_value_size = static_cast<double>(column.byte_size()) / row_size;

        /// Heuristic is chosen so that avg_value_size_hint increases rapidly but decreases slowly.
        if (current_avg_value_size > avg_value_size_hint) {
            avg_value_size_hint = std::min(1024., current_avg_value_size); /// avoid overestimation
        } else if (current_avg_value_size * 2 < avg_value_size_hint) {
            avg_value_size_hint = (current_avg_value_size + avg_value_size_hint * 3) / 4;
        }
    }
}

ColumnPtr IDataType::create_column_const(size_t size, const Field& field) const {
    auto column = create_column();
    column->reserve(1);
    column->insert(field);
    return ColumnConst::create(std::move(column), size);
}

ColumnPtr IDataType::create_column_const_with_default_value(size_t size) const {
    return create_column_const(size, get_default());
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
Status IDataType::from_string(ReadBuffer& rb, IColumn* column) const {
    LOG(FATAL) << fmt::format("Data type {} from_string not implement.", get_name());
    return Status::OK();
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
    case TypeIndex::IPv4:
        return PGenericType::IPV4;
    case TypeIndex::IPv6:
        return PGenericType::IPV6;
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
    case TypeIndex::Decimal128I:
        return PGenericType::DECIMAL128I;
    case TypeIndex::Decimal256:
        return PGenericType::DECIMAL256;
    case TypeIndex::String:
        return PGenericType::STRING;
    case TypeIndex::Date:
        return PGenericType::DATE;
    case TypeIndex::DateV2:
        return PGenericType::DATEV2;
    case TypeIndex::DateTime:
        return PGenericType::DATETIME;
    case TypeIndex::VARIANT:
        return PGenericType::VARIANT;
    case TypeIndex::DateTimeV2:
        return PGenericType::DATETIMEV2;
    case TypeIndex::BitMap:
        return PGenericType::BITMAP;
    case TypeIndex::HLL:
        return PGenericType::HLL;
    case TypeIndex::QuantileState:
        return PGenericType::QUANTILE_STATE;
    case TypeIndex::Array:
        return PGenericType::LIST;
    case TypeIndex::Struct:
        return PGenericType::STRUCT;
    case TypeIndex::FixedLengthObject:
        return PGenericType::FIXEDLENGTHOBJECT;
    case TypeIndex::JSONB:
        return PGenericType::JSONB;
    case TypeIndex::GEOMETRY:
        return PGenericType::GEOMETRY;
    case TypeIndex::Map:
        return PGenericType::MAP;
    case TypeIndex::Time:
        return PGenericType::TIME;
    case TypeIndex::AggState:
        return PGenericType::AGG_STATE;
    case TypeIndex::TimeV2:
        return PGenericType::TIMEV2;
    default:
        LOG(FATAL) << fmt::format("could not mapping type {} to pb type", data_type->get_type_id());
        return PGenericType::UNKNOWN;
    }
}

} // namespace doris::vectorized
