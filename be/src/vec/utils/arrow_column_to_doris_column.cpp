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

#include "vec/utils/arrow_column_to_doris_column.h"

#include <arrow/array/array_base.h>
#include <arrow/array/array_decimal.h>
#include <arrow/array/array_primitive.h>
#include <arrow/array/data.h>
#include <arrow/buffer.h>
#include <cctz/time_zone.h>
#include <fmt/format.h>
#include <glog/logging.h>
#include <stdint.h>

#include <memory>
#include <utility>
#include <vector>

#include "arrow/array/array_binary.h"
#include "arrow/array/array_nested.h"
#include "arrow/type.h"
#include "common/status.h"
#include "util/binary_cast.hpp"
#include "util/timezone_utils.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/runtime/vdatetime_value.h"

#define FOR_ARROW_TYPES(M)                            \
    M(::arrow::Type::BOOL, TYPE_BOOLEAN)              \
    M(::arrow::Type::INT8, TYPE_TINYINT)              \
    M(::arrow::Type::UINT8, TYPE_TINYINT)             \
    M(::arrow::Type::INT16, TYPE_SMALLINT)            \
    M(::arrow::Type::UINT16, TYPE_SMALLINT)           \
    M(::arrow::Type::INT32, TYPE_INT)                 \
    M(::arrow::Type::UINT32, TYPE_INT)                \
    M(::arrow::Type::INT64, TYPE_BIGINT)              \
    M(::arrow::Type::UINT64, TYPE_BIGINT)             \
    M(::arrow::Type::HALF_FLOAT, TYPE_FLOAT)          \
    M(::arrow::Type::FLOAT, TYPE_FLOAT)               \
    M(::arrow::Type::DOUBLE, TYPE_DOUBLE)             \
    M(::arrow::Type::BINARY, TYPE_VARCHAR)            \
    M(::arrow::Type::FIXED_SIZE_BINARY, TYPE_VARCHAR) \
    M(::arrow::Type::STRING, TYPE_VARCHAR)            \
    M(::arrow::Type::TIMESTAMP, TYPE_DATETIME)        \
    M(::arrow::Type::DATE32, TYPE_DATE)               \
    M(::arrow::Type::DATE64, TYPE_DATETIME)           \
    M(::arrow::Type::DECIMAL, TYPE_DECIMALV2)

namespace doris::vectorized {

PrimitiveType arrow_type_to_primitive_type(::arrow::Type::type type) {
    switch (type) {
        // TODO: convert arrow date type to datev2/datetimev2
#define DISPATCH(ARROW_TYPE, CPP_TYPE) \
    case ARROW_TYPE:                   \
        return CPP_TYPE;
        FOR_ARROW_TYPES(DISPATCH)
#undef DISPATCH
    default:
        break;
    }
    return INVALID_TYPE;
}

//// For convenient unit test. Not use this in formal code.
Status arrow_column_to_doris_column(const arrow::Array* arrow_column, size_t arrow_batch_cur_idx,
                                    ColumnPtr& doris_column, const DataTypePtr& type,
                                    size_t num_elements, const std::string& timezone) {
    cctz::time_zone ctz;
    TimezoneUtils::find_cctz_time_zone(timezone, ctz);
    return arrow_column_to_doris_column(arrow_column, arrow_batch_cur_idx, doris_column, type,
                                        num_elements, ctz);
}

Status arrow_column_to_doris_column(const arrow::Array* arrow_column, size_t arrow_batch_cur_idx,
                                    ColumnPtr& doris_column, const DataTypePtr& type,
                                    size_t num_elements, const cctz::time_zone& ctz) {
    RETURN_IF_ERROR(type->get_serde()->read_column_from_arrow(
            doris_column->assume_mutable_ref(), arrow_column, arrow_batch_cur_idx,
            arrow_batch_cur_idx + num_elements, ctz));
    return Status::OK();
}

} // namespace doris::vectorized
