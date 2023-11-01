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

#include "data_type_string_serde.h"
#include "util/jsonb_utils.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_string.h"
#include "vec/core/types.h"

namespace doris {
class JsonbOutStream;

namespace vectorized {
class Arena;

class DataTypeJsonbSerDe : public DataTypeStringSerDe {
    Status write_column_to_mysql(const IColumn& column, MysqlRowBuffer<true>& row_buffer,
                                 int row_idx, bool col_const) const override;
    Status write_column_to_mysql(const IColumn& column, MysqlRowBuffer<false>& row_buffer,
                                 int row_idx, bool col_const) const override;
    void write_column_to_arrow(const IColumn& column, const NullMap* null_map,
                               arrow::ArrayBuilder* array_builder, int start,
                               int end) const override;

    Status serialize_one_cell_to_json(const IColumn& column, int row_num, BufferWritable& bw,
                                      FormatOptions& options, int nesting_level = 1) const override;
    Status serialize_column_to_json(const IColumn& column, int start_idx, int end_idx,
                                    BufferWritable& bw, FormatOptions& options,
                                    int nesting_level = 1) const override;

    Status deserialize_one_cell_from_json(IColumn& column, Slice& slice,
                                          const FormatOptions& options,
                                          int nesting_level = 1) const override;

    Status deserialize_column_from_json_vector(IColumn& column, std::vector<Slice>& slices,
                                               int* num_deserialized, const FormatOptions& options,
                                               int nesting_level = 1) const override;

    Status write_column_to_orc(const std::string& timezone, const IColumn& column,
                               const NullMap* null_map, orc::ColumnVectorBatch* orc_col_batch,
                               int start, int end,
                               std::vector<StringRef>& buffer_list) const override;

private:
    template <bool is_binary_format>
    Status _write_column_to_mysql(const IColumn& column, MysqlRowBuffer<is_binary_format>& result,
                                  int row_idx, bool col_const) const;
};
} // namespace vectorized
} // namespace doris