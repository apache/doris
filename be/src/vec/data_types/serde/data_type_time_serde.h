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
#include <gen_cpp/types.pb.h>
#include <glog/logging.h>

#include <cstdint>

#include "data_type_number_serde.h"
#include "vec/runtime/time_value.h"

namespace doris {
class JsonbOutStream;
#include "common/compile_check_begin.h"
namespace vectorized {
class DataTypeTimeV2SerDe : public DataTypeNumberSerDe<PrimitiveType::TYPE_TIMEV2> {
public:
    DataTypeTimeV2SerDe(int scale = 0, int nesting_level = 1)
            : DataTypeNumberSerDe<PrimitiveType::TYPE_TIMEV2>(nesting_level), _scale(scale) {};
    Status write_column_to_mysql(const IColumn& column, MysqlRowBuffer<true>& row_buffer,
                                 int64_t row_idx, bool col_const,
                                 const FormatOptions& options) const override;
    Status write_column_to_mysql(const IColumn& column, MysqlRowBuffer<false>& row_buffer,
                                 int64_t row_idx, bool col_const,
                                 const FormatOptions& options) const override;

    Status from_string(StringRef& str, IColumn& column,
                       const FormatOptions& options) const override;

    Status from_string_strict_mode(StringRef& str, IColumn& column,
                                   const FormatOptions& options) const override;

    Status from_string_batch(const ColumnString& str, ColumnNullable& column,
                             const FormatOptions& options) const final;

    Status from_string_strict_mode_batch(const ColumnString& str, IColumn& column,
                                         const FormatOptions& options,
                                         const NullMap::value_type* null_map = nullptr) const final;

    template <typename IntDataType>
    Status from_int_batch(const IntDataType::ColumnType& int_col, ColumnNullable& target_col) const;
    template <typename IntDataType>
    Status from_int_strict_mode_batch(const IntDataType::ColumnType& int_col,
                                      IColumn& target_col) const;

    template <typename FloatDataType>
    Status from_float_batch(const FloatDataType::ColumnType& float_col,
                            ColumnNullable& target_col) const;
    template <typename FloatDataType>
    Status from_float_strict_mode_batch(const FloatDataType::ColumnType& float_col,
                                        IColumn& target_col) const;

    template <typename DecimalDataType>
    Status from_decimal_batch(const DecimalDataType::ColumnType& decimal_col,
                              ColumnNullable& target_col) const;
    template <typename DecimalDataType>
    Status from_decimal_strict_mode_batch(const DecimalDataType::ColumnType& decimal_col,
                                          IColumn& target_col) const;

private:
    template <bool is_binary_format>
    Status _write_column_to_mysql(const IColumn& column, MysqlRowBuffer<is_binary_format>& result,
                                  int64_t row_idx, bool col_const,
                                  const FormatOptions& options) const;

    int _scale;
};
#include "common/compile_check_end.h"
} // namespace vectorized
} // namespace doris
