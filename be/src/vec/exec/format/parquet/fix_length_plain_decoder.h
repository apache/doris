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

#pragma once

#include <gen_cpp/parquet_types.h>
#include <stddef.h>

#include "common/status.h"
#include "vec/data_types/data_type.h"
#include "vec/exec/format/parquet/decoder.h"

namespace doris {
namespace vectorized {
class ColumnSelectVector;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

class FixLengthPlainDecoder final : public Decoder {
public:
    FixLengthPlainDecoder(tparquet::Type::type physical_type) : _physical_type(physical_type) {};
    ~FixLengthPlainDecoder() override = default;

    Status decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                         ColumnSelectVector& select_vector, bool is_dict_filter) override;

    template <bool hasFilter>
    Status _decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                          ColumnSelectVector& select_vector, bool is_dict_filter);

    Status skip_values(size_t num_values) override;

protected:
    template <typename Numeric, typename PhysicalType, bool has_filter>
    Status _decode_numeric(MutableColumnPtr& doris_column, ColumnSelectVector& select_vector);

    template <typename CppType, typename ColumnType, bool has_filter>
    Status _decode_date(MutableColumnPtr& doris_column, ColumnSelectVector& select_vector);

    template <typename CppType, typename ColumnType, bool has_filter>
    Status _decode_datetime64(MutableColumnPtr& doris_column, ColumnSelectVector& select_vector);

    template <typename CppType, typename ColumnType, bool has_filter>
    Status _decode_datetime96(MutableColumnPtr& doris_column, ColumnSelectVector& select_vector);

    template <typename DecimalPrimitiveType, bool has_filter>
    Status _decode_binary_decimal(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                                  ColumnSelectVector& select_vector);

    template <typename DecimalPrimitiveType, typename DecimalPhysicalType, bool has_filter>
    Status _decode_primitive_decimal(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                                     ColumnSelectVector& select_vector);

    template <bool has_filter>
    Status _decode_string(MutableColumnPtr& doris_column, ColumnSelectVector& select_vector);

    tparquet::Type::type _physical_type;

private:
    template <typename DecimalPrimitiveType, bool has_filter, int fixed_type_length,
              typename ValueCopyType, DecimalScaleParams::ScaleType ScaleType>
    Status _decode_binary_decimal_internal(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                                           ColumnSelectVector& select_vector);
    template <typename DecimalPrimitiveType, typename DecimalPhysicalType, bool has_filter,
              int fixed_type_length, typename ValueCopyType,
              DecimalScaleParams::ScaleType ScaleType>
    Status _decode_primitive_decimal_internal(MutableColumnPtr& doris_column,
                                              DataTypePtr& data_type,
                                              ColumnSelectVector& select_vector);
};
} // namespace doris::vectorized
