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

#include "core/column/variant_v2/column_variant_v2_typed_column.h"

#include "core/data_type/data_type_string.h"

namespace doris::column_variant_v2_internal {
namespace detail {

size_t format_int128(__int128 value, char* const output) noexcept {
    std::array<char, 39> reversed {};
    size_t digits = 0;
    unsigned __int128 remaining = unsigned_magnitude(value);
    do {
        reversed[digits++] = static_cast<char>('0' + remaining % 10);
        remaining /= 10;
    } while (remaining != 0);

    size_t position = 0;
    if (value < 0) {
        output[position++] = '-';
    }
    while (digits != 0) {
        output[position++] = reversed[--digits];
    }
    return position;
}

} // namespace detail

bool is_supported_typed_identity(PrimitiveType type) {
    switch (type) {
    case TYPE_BOOLEAN:
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
    case TYPE_LARGEINT:
    case TYPE_FLOAT:
    case TYPE_DOUBLE:
    case TYPE_DECIMALV2:
    case TYPE_DECIMAL32:
    case TYPE_DECIMAL64:
    case TYPE_DECIMAL128I:
    case TYPE_DATE:
    case TYPE_DATEV2:
    case TYPE_DATETIME:
    case TYPE_DATETIMEV2:
    case TYPE_TIMESTAMPTZ:
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_STRING:
    case TYPE_IPV4:
    case TYPE_IPV6:
        return true;
    default:
        return false;
    }
}

bool exact_typed_identity(const DataTypePtr& left, const DataTypePtr& right) {
    const PrimitiveType primitive = left->get_primitive_type();
    if (primitive != right->get_primitive_type()) {
        return false;
    }
    if (is_string_type(primitive)) {
        return assert_cast<const DataTypeString&>(*left).len() ==
               assert_cast<const DataTypeString&>(*right).len();
    }
    return left->equals(*right);
}

void validate_typed_decimal_scale(const IColumn& nested, PrimitiveType type, uint32_t scale) {
    uint32_t column_scale = scale;
    switch (type) {
    case TYPE_DECIMALV2:
        column_scale = assert_cast<const ColumnDecimal128V2&>(nested).get_scale();
        break;
    case TYPE_DECIMAL32:
        column_scale = assert_cast<const ColumnDecimal32&>(nested).get_scale();
        break;
    case TYPE_DECIMAL64:
        column_scale = assert_cast<const ColumnDecimal64&>(nested).get_scale();
        break;
    case TYPE_DECIMAL128I:
        column_scale = assert_cast<const ColumnDecimal128V3&>(nested).get_scale();
        break;
    default:
        return;
    }
    DORIS_CHECK_EQ(column_scale, scale) << "typed decimal scale does not match data type scale";
}

} // namespace doris::column_variant_v2_internal
