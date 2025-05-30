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

namespace doris {
class JsonbOutStream;
#include "common/compile_check_begin.h"
namespace vectorized {
class DataTypeTimeV2SerDe : public DataTypeNumberSerDe<PrimitiveType::TYPE_TIMEV2> {
public:
    DataTypeTimeV2SerDe(int scale = 0, int nesting_level = 1)
            : DataTypeNumberSerDe<PrimitiveType::TYPE_TIMEV2>(nesting_level), scale(scale) {};
    Status write_column_to_mysql(const IColumn& column, MysqlRowBuffer<true>& row_buffer,
                                 int64_t row_idx, bool col_const,
                                 const FormatOptions& options) const override;
    Status write_column_to_mysql(const IColumn& column, MysqlRowBuffer<false>& row_buffer,
                                 int64_t row_idx, bool col_const,
                                 const FormatOptions& options) const override;

private:
    template <bool is_binary_format>
    Status _write_column_to_mysql(const IColumn& column, MysqlRowBuffer<is_binary_format>& result,
                                  int64_t row_idx, bool col_const,
                                  const FormatOptions& options) const;
    int scale;
};
#include "common/compile_check_end.h"
} // namespace vectorized
} // namespace doris
