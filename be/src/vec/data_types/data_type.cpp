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
#include "common/status.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/core/field.h"

namespace doris {
namespace vectorized {
class BufferWritable;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {
#include "common/compile_check_begin.h"

IDataType::IDataType() = default;

IDataType::~IDataType() = default;

String IDataType::get_name() const {
    return do_get_name();
}

String IDataType::do_get_name() const {
    return get_family_name();
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
    throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                           "Value of type {} in memory is not of fixed size.", get_name());
    return 0;
}

void IDataType::to_string(const IColumn& column, size_t row_num, BufferWritable& ostr) const {
    throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                           "Data type {} to_string ostr not implement.", get_name());
}

std::string IDataType::to_string(const IColumn& column, size_t row_num) const {
    throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                           "Data type {} to_string not implement.", get_name());
    return "";
}

void IDataType::to_string_batch(const IColumn& column, ColumnString& column_to) const {
    const auto size = column.size();
    column_to.reserve(size * 2);
    VectorBufferWriter write_buffer(column_to);
    for (size_t i = 0; i < size; ++i) {
        to_string(column, i, write_buffer);
        write_buffer.commit();
    }
}

void IDataType::to_pb_column_meta(PColumnMeta* col_meta) const {
    col_meta->set_type(get_pdata_type(this));
}

PGenericType_TypeId IDataType::get_pdata_type(const IDataType* data_type) {
    switch (data_type->get_primitive_type()) {
    case PrimitiveType::TYPE_BOOLEAN:
        return PGenericType::UINT8;
    case PrimitiveType::TYPE_TINYINT:
        return PGenericType::INT8;
    case PrimitiveType::TYPE_SMALLINT:
        return PGenericType::INT16;
    case PrimitiveType::TYPE_INT:
        return PGenericType::INT32;
    case PrimitiveType::TYPE_BIGINT:
        return PGenericType::INT64;
    case PrimitiveType::TYPE_LARGEINT:
        return PGenericType::INT128;
    case PrimitiveType::TYPE_IPV4:
        return PGenericType::IPV4;
    case PrimitiveType::TYPE_IPV6:
        return PGenericType::IPV6;
    case PrimitiveType::TYPE_FLOAT:
        return PGenericType::FLOAT;
    case PrimitiveType::TYPE_DOUBLE:
        return PGenericType::DOUBLE;
    case PrimitiveType::TYPE_DECIMAL32:
        return PGenericType::DECIMAL32;
    case PrimitiveType::TYPE_DECIMAL64:
        return PGenericType::DECIMAL64;
    case PrimitiveType::TYPE_DECIMALV2:
        return PGenericType::DECIMAL128;
    case PrimitiveType::TYPE_DECIMAL128I:
        return PGenericType::DECIMAL128I;
    case PrimitiveType::TYPE_DECIMAL256:
        return PGenericType::DECIMAL256;
    case PrimitiveType::TYPE_CHAR:
    case PrimitiveType::TYPE_VARCHAR:
    case PrimitiveType::TYPE_STRING:
        return PGenericType::STRING;
    case PrimitiveType::TYPE_DATE:
        return PGenericType::DATE;
    case PrimitiveType::TYPE_DATEV2:
        return PGenericType::DATEV2;
    case PrimitiveType::TYPE_DATETIME:
        return PGenericType::DATETIME;
    case PrimitiveType::TYPE_VARIANT:
        return PGenericType::VARIANT;
    case PrimitiveType::TYPE_DATETIMEV2:
        return PGenericType::DATETIMEV2;
    case PrimitiveType::TYPE_BITMAP:
        return PGenericType::BITMAP;
    case PrimitiveType::TYPE_HLL:
        return PGenericType::HLL;
    case PrimitiveType::TYPE_QUANTILE_STATE:
        return PGenericType::QUANTILE_STATE;
    case PrimitiveType::TYPE_ARRAY:
        return PGenericType::LIST;
    case PrimitiveType::TYPE_STRUCT:
        return PGenericType::STRUCT;
    case PrimitiveType::TYPE_JSONB:
        return PGenericType::JSONB;
    case PrimitiveType::TYPE_MAP:
        return PGenericType::MAP;
    case PrimitiveType::TYPE_TIME:
        return PGenericType::TIME;
    case PrimitiveType::TYPE_AGG_STATE:
        return PGenericType::AGG_STATE;
    case PrimitiveType::TYPE_TIMEV2:
        return PGenericType::TIMEV2;
    case PrimitiveType::TYPE_FIXED_LENGTH_OBJECT:
        return PGenericType::FIXEDLENGTHOBJECT;
    default:
        break;
    }
    throw doris::Exception(ErrorCode::INTERNAL_ERROR, "could not mapping type {} to pb type",
                           data_type->get_name());
    return PGenericType::UNKNOWN;
}

// write const_flag and row_num to buf
char* serialize_const_flag_and_row_num(const IColumn** column, char* buf,
                                       size_t* real_need_copy_num) {
    const auto* col = *column;
    // const flag
    bool is_const_column = is_column_const(*col);
    unaligned_store<bool>(buf, is_const_column);
    buf += sizeof(bool);

    // row num
    const auto row_num = col->size();
    unaligned_store<size_t>(buf, row_num);
    buf += sizeof(size_t);

    // real saved num
    *real_need_copy_num = is_const_column ? 1 : row_num;
    unaligned_store<size_t>(buf, *real_need_copy_num);
    buf += sizeof(size_t);

    if (is_const_column) {
        const auto& const_column = assert_cast<const vectorized::ColumnConst&>(*col);
        *column = &(const_column.get_data_column());
    }
    return buf;
}

const char* deserialize_const_flag_and_row_num(const char* buf, MutableColumnPtr* column,
                                               size_t* real_have_saved_num) {
    // const flag
    bool is_const_column = unaligned_load<bool>(buf);
    buf += sizeof(bool);
    // row num
    size_t row_num = unaligned_load<size_t>(buf);
    buf += sizeof(size_t);
    // real saved num
    *real_have_saved_num = unaligned_load<size_t>(buf);
    buf += sizeof(size_t*);

    if (is_const_column) {
        auto const_column = ColumnConst::create((*column)->get_ptr(), row_num, true);
        *column = const_column->get_ptr();
    }
    return buf;
}

FieldWithDataType IDataType::get_field_with_data_type(const IColumn& column, size_t row_num) const {
    return FieldWithDataType {.field = column[row_num],
                              .base_scalar_type_id = get_primitive_type()};
}

} // namespace doris::vectorized
