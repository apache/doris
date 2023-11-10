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

#include "vec/exec/format/parquet/parquet_column_convert.h"

#include <cctz/time_zone.h>

#include "vec/columns/column_nullable.h"
namespace doris::vectorized {
namespace ParquetConvert {
const cctz::time_zone ConvertParams::utc0 = cctz::utc_time_zone();

ColumnPtr get_column(tparquet::Type::type parquet_physical_type, PrimitiveType show_type,
                     ColumnPtr& doris_column, DataTypePtr& doris_type, bool* need_convert) {
    ColumnPtr ans_column = doris_column;
    DataTypePtr tmp_data_type;

    switch (parquet_physical_type) {
    case tparquet::Type::type::BOOLEAN:
        tmp_data_type = std::make_shared<DataTypeUInt8>();
        break;
    case tparquet::Type::type::INT32:
        tmp_data_type = std::make_shared<DataTypeInt32>();
        break;
    case tparquet::Type::type::INT64:
        tmp_data_type = std::make_shared<DataTypeInt64>();
        break;
    case tparquet::Type::type::FLOAT:
        tmp_data_type = std::make_shared<DataTypeFloat32>();
        break;
    case tparquet::Type::type::DOUBLE:
        tmp_data_type = std::make_shared<DataTypeFloat64>();
        break;
    case tparquet::Type::type::BYTE_ARRAY:
    case tparquet::Type::type::FIXED_LEN_BYTE_ARRAY:
        tmp_data_type = std::make_shared<DataTypeString>();
        break;
    case tparquet::Type::type::INT96:
        tmp_data_type = std::make_shared<DataTypeInt8>();
        break;
    }

    if (tmp_data_type->get_type_id() == remove_nullable(doris_type)->get_type_id()) {
        if (tmp_data_type->get_type_id() == TypeIndex::String &&
            (show_type == PrimitiveType::TYPE_DECIMAL32 ||
             show_type == PrimitiveType::TYPE_DECIMAL64 ||
             show_type == PrimitiveType::TYPE_DECIMALV2 ||
             show_type == PrimitiveType::TYPE_DECIMAL128I)) {
            *need_convert = true;
            ans_column = tmp_data_type->create_column();
        } else {
            *need_convert = false;
        }
    } else {
        ans_column = tmp_data_type->create_column();
        *need_convert = true;
    }

    if (*need_convert && doris_type->is_nullable()) {
        auto doris_nullable_column = static_cast<const ColumnNullable*>(doris_column.get());
        ans_column = ColumnNullable::create(ans_column,
                                            doris_nullable_column->get_null_map_column_ptr());
    }
    return ans_column;
}

} // namespace ParquetConvert
} // namespace doris::vectorized
