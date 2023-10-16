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

namespace doris::vectorized {
namespace ParquetConvert {
const cctz::time_zone ConvertParams::utc0 = cctz::utc_time_zone();

Status convert_data_type_from_parquet(tparquet::Type::type parquet_type, PrimitiveType show_type,
                                      vectorized::DataTypePtr& ans_data_type, DataTypePtr& src_type,
                                      bool* need_convert) {
    if (is_complex_type(src_type)) {
        *need_convert = false;
        return Status::OK();
    }
    switch (parquet_type) {
    case tparquet::Type::type::BOOLEAN:
        ans_data_type = std::make_shared<DataTypeUInt8>();
        break;
    case tparquet::Type::type::INT32:
        ans_data_type = std::make_shared<DataTypeInt32>();
        break;
    case tparquet::Type::type::INT64:
        ans_data_type = std::make_shared<DataTypeInt64>();
        break;
    case tparquet::Type::type::FLOAT:
        ans_data_type = std::make_shared<DataTypeFloat32>();
        break;
    case tparquet::Type::type::DOUBLE:
        ans_data_type = std::make_shared<DataTypeFloat64>();
        break;
    case tparquet::Type::type::BYTE_ARRAY:
    case tparquet::Type::type::FIXED_LEN_BYTE_ARRAY:
        ans_data_type = std::make_shared<DataTypeString>();
        break;
    case tparquet::Type::type::INT96:
        ans_data_type = std::make_shared<DataTypeInt128>();
        break;
    default:
        return Status::IOError("Can't read parquet type : {}", parquet_type);
    }
    if (ans_data_type->get_type_id() == src_type->get_type_id()) {
        if (ans_data_type->get_type_id() == TypeIndex::String &&
            show_type == PrimitiveType::TYPE_DECIMAL64) {
            *need_convert = true;
            return Status::OK();
        }
        *need_convert = false;
        return Status::OK();
    }

    if (src_type->is_nullable()) {
        auto& nested_src_type =
                static_cast<const DataTypeNullable*>(src_type.get())->get_nested_type();
        auto sub = ans_data_type;
        ans_data_type = std::make_shared<DataTypeNullable>(ans_data_type);

        if (nested_src_type->get_type_id() == sub->get_type_id()) {
            if (sub->get_type_id() == TypeIndex::String &&
                show_type == PrimitiveType::TYPE_DECIMAL64) {
                *need_convert = true;
                return Status::OK();
            }
            *need_convert = false;
            return Status::OK();
        }
    }

    *need_convert = true;
    return Status::OK();
}

Status get_converter(std::shared_ptr<const IDataType> src_type, PrimitiveType show_type,
                     std::shared_ptr<const IDataType> dst_type,
                     std::unique_ptr<ColumnConvert>* converter, ConvertParams* convert_param) {
    if (src_type->is_nullable()) {
        auto src = reinterpret_cast<const DataTypeNullable*>(src_type.get())->get_nested_type();
        auto dst = reinterpret_cast<const DataTypeNullable*>(dst_type.get())->get_nested_type();

        return get_converter_impl<true>(src, show_type, dst, converter, convert_param);
    } else {
        return get_converter_impl<false>(src_type, show_type, dst_type, converter, convert_param);
    }
}
} // namespace ParquetConvert
} // namespace doris::vectorized
