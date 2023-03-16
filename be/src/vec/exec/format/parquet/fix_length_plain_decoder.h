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

#include "common/status.h"
#include "gen_cpp/parquet_types.h"
#include "util/slice.h"
#include "vec/columns/column.h"
#include "vec/data_types/data_type.h"
#include "vec/exec/format/parquet/decoder.h"
#include "vec/exec/format/parquet/parquet_common.h"

namespace doris::vectorized {

class FixLengthPlainDecoder final : public Decoder {
public:
    FixLengthPlainDecoder(tparquet::Type::type physical_type) : _physical_type(physical_type) {};
    ~FixLengthPlainDecoder() override = default;

    Status decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                         ColumnSelectVector& select_vector, bool is_dict_filter) override;

    Status skip_values(size_t num_values) override;

protected:
    template <typename Numeric>
    Status _decode_numeric(MutableColumnPtr& doris_column, ColumnSelectVector& select_vector);

    template <typename CppType, typename ColumnType>
    Status _decode_date(MutableColumnPtr& doris_column, ColumnSelectVector& select_vector);

    template <typename CppType, typename ColumnType>
    Status _decode_datetime64(MutableColumnPtr& doris_column, ColumnSelectVector& select_vector);

    template <typename CppType, typename ColumnType>
    Status _decode_datetime96(MutableColumnPtr& doris_column, ColumnSelectVector& select_vector);

    template <typename DecimalPrimitiveType>
    Status _decode_binary_decimal(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                                  ColumnSelectVector& select_vector);

    template <typename DecimalPrimitiveType, typename DecimalPhysicalType>
    Status _decode_primitive_decimal(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                                     ColumnSelectVector& select_vector);

    Status _decode_string(MutableColumnPtr& doris_column, ColumnSelectVector& select_vector);

    tparquet::Type::type _physical_type;
};
} // namespace doris::vectorized
