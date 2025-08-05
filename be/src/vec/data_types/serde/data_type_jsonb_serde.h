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
#include "vec/columns/column_const.h"
#include "vec/columns/column_string.h"
#include "vec/core/types.h"

namespace doris {
class JsonbOutStream;

namespace vectorized {
class Arena;

class DataTypeJsonbSerDe : public DataTypeStringSerDe {
public:
    DataTypeJsonbSerDe(int nesting_level = 1) : DataTypeStringSerDe(nesting_level) {};

    std::string get_name() const override { return "JSONB"; }

    Status from_string(StringRef& str, IColumn& column,
                       const FormatOptions& options) const override;

    Status write_column_to_mysql(const IColumn& column, MysqlRowBuffer<true>& row_buffer,
                                 int64_t row_idx, bool col_const,
                                 const FormatOptions& options) const override;
    Status write_column_to_mysql(const IColumn& column, MysqlRowBuffer<false>& row_buffer,
                                 int64_t row_idx, bool col_const,
                                 const FormatOptions& options) const override;
    Status write_column_to_arrow(const IColumn& column, const NullMap* null_map,
                                 arrow::ArrayBuilder* array_builder, int64_t start, int64_t end,
                                 const cctz::time_zone& ctz) const override;

    Status serialize_one_cell_to_json(const IColumn& column, int64_t row_num, BufferWritable& bw,
                                      FormatOptions& options) const override;
    Status serialize_column_to_json(const IColumn& column, int64_t start_idx, int64_t end_idx,
                                    BufferWritable& bw, FormatOptions& options) const override;

    Status deserialize_one_cell_from_json(IColumn& column, Slice& slice,
                                          const FormatOptions& options) const override;

    Status deserialize_column_from_json_vector(IColumn& column, std::vector<Slice>& slices,
                                               uint64_t* num_deserialized,
                                               const FormatOptions& options) const override;

    Status write_column_to_orc(const std::string& timezone, const IColumn& column,
                               const NullMap* null_map, orc::ColumnVectorBatch* orc_col_batch,
                               int64_t start, int64_t end, vectorized::Arena& arena) const override;

    Status write_column_to_pb(const IColumn& column, PValues& result, int64_t start,
                              int64_t end) const override;

    Status read_column_from_pb(IColumn& column, const PValues& arg) const override;

    Status serialize_column_to_jsonb(const IColumn& from_column, int64_t row_num,
                                     JsonbWriter& writer) const override;

    Status deserialize_column_from_jsonb(IColumn& column, const JsonbValue* jsonb_value,
                                         CastParameters& castParms) const override;
    void write_one_cell_to_binary(const IColumn& src_column, ColumnString::Chars& chars,
                                  int64_t row_num) const override;

private:
    template <bool is_binary_format>
    Status _write_column_to_mysql(const IColumn& column, MysqlRowBuffer<is_binary_format>& result,
                                  int64_t row_idx, bool col_const,
                                  const FormatOptions& options) const;
};

void convert_jsonb_to_rapidjson(const JsonbValue& val, rapidjson::Value& target,
                                rapidjson::Document::AllocatorType& allocator);
} // namespace vectorized
} // namespace doris
