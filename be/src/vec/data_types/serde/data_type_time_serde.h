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
#include <stddef.h>
#include <stdint.h>

#include "data_type_number_serde.h"
#include "vec/core/types.h"

namespace doris {
class JsonbOutStream;

namespace vectorized {
class Arena;

class DataTypeTimeSerDe : public DataTypeNumberSerDe<Float64> {
public:
    DataTypeTimeSerDe(int nesting_level = 1) : DataTypeNumberSerDe<Float64>(nesting_level) {};

    Status write_column_to_mysql(const IColumn& column, MysqlRowBuffer<true>& row_buffer,
                                 int row_idx, bool col_const,
                                 const FormatOptions& options) const override;
    Status write_column_to_mysql(const IColumn& column, MysqlRowBuffer<false>& row_buffer,
                                 int row_idx, bool col_const,
                                 const FormatOptions& options) const override;

private:
    template <bool is_binary_format>
    Status _write_column_to_mysql(const IColumn& column, MysqlRowBuffer<is_binary_format>& result,
                                  int row_idx, bool col_const, const FormatOptions& options) const;
};
class DataTypeTimeV2SerDe : public DataTypeNumberSerDe<Float64> {
public:
    DataTypeTimeV2SerDe(int scale = 0, int nesting_level = 1)
            : DataTypeNumberSerDe<Float64>(nesting_level), scale(scale) {};
    Status write_column_to_mysql(const IColumn& column, MysqlRowBuffer<true>& row_buffer,
                                 int row_idx, bool col_const,
                                 const FormatOptions& options) const override;
    Status write_column_to_mysql(const IColumn& column, MysqlRowBuffer<false>& row_buffer,
                                 int row_idx, bool col_const,
                                 const FormatOptions& options) const override;

private:
    template <bool is_binary_format>
    Status _write_column_to_mysql(const IColumn& column, MysqlRowBuffer<is_binary_format>& result,
                                  int row_idx, bool col_const, const FormatOptions& options) const;
    int scale;
};
} // namespace vectorized
} // namespace doris
