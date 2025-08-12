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

#include <glog/logging.h>

#include <cstdint>

#include "common/status.h"
#include "data_type_serde.h"

namespace doris {
class PValues;
struct JsonbValue;

namespace vectorized {
class IColumn;
class Arena;
#include "common/compile_check_begin.h"

class DataTypeStructSerDe : public DataTypeSerDe {
public:
    DataTypeStructSerDe(const DataTypeSerDeSPtrs& _elem_serdes_ptrs, const Strings names,
                        int nesting_level = 1)
            : DataTypeSerDe(nesting_level),
              elem_serdes_ptrs(_elem_serdes_ptrs),
              elem_names(names) {}

    std::string get_name() const override;

    Status from_string(StringRef& str, IColumn& column,
                       const FormatOptions& options) const override;

    Status from_string_strict_mode(StringRef& str, IColumn& column,
                                   const FormatOptions& options) const override;

    Status serialize_one_cell_to_json(const IColumn& column, int64_t row_num, BufferWritable& bw,
                                      FormatOptions& options) const override;

    Status serialize_column_to_json(const IColumn& column, int64_t start_idx, int64_t end_idx,
                                    BufferWritable& bw, FormatOptions& options) const override;

    Status deserialize_one_cell_from_json(IColumn& column, Slice& slice,
                                          const FormatOptions& options) const override;

    Status deserialize_column_from_json_vector(IColumn& column, std::vector<Slice>& slices,
                                               uint64_t* num_deserialized,
                                               const FormatOptions& options) const override;

    Status deserialize_one_cell_from_hive_text(
            IColumn& column, Slice& slice, const FormatOptions& options,
            int hive_text_complex_type_delimiter_level = 1) const override;
    Status deserialize_column_from_hive_text_vector(
            IColumn& column, std::vector<Slice>& slices, uint64_t* num_deserialized,
            const FormatOptions& options,
            int hive_text_complex_type_delimiter_level = 1) const override;
    Status serialize_one_cell_to_hive_text(
            const IColumn& column, int64_t row_num, BufferWritable& bw, FormatOptions& options,
            int hive_text_complex_type_delimiter_level = 1) const override;

    Status write_column_to_pb(const IColumn& column, PValues& result, int64_t start,
                              int64_t end) const override;
    Status read_column_from_pb(IColumn& column, const PValues& arg) const override;
    void write_one_cell_to_jsonb(const IColumn& column, JsonbWriter& result, Arena& mem_pool,
                                 int32_t col_id, int64_t row_num) const override;

    void read_one_cell_from_jsonb(IColumn& column, const JsonbValue* arg) const override;

    Status write_column_to_arrow(const IColumn& column, const NullMap* null_map,
                                 arrow::ArrayBuilder* array_builder, int64_t start, int64_t end,
                                 const cctz::time_zone& ctz) const override;
    Status read_column_from_arrow(IColumn& column, const arrow::Array* arrow_array, int64_t start,
                                  int64_t end, const cctz::time_zone& ctz) const override;

    Status write_column_to_mysql(const IColumn& column, MysqlRowBuffer<true>& row_buffer,
                                 int64_t row_idx, bool col_const,
                                 const FormatOptions& options) const override;
    Status write_column_to_mysql(const IColumn& column, MysqlRowBuffer<false>& row_buffer,
                                 int64_t row_idx, bool col_const,
                                 const FormatOptions& options) const override;

    Status write_column_to_orc(const std::string& timezone, const IColumn& column,
                               const NullMap* null_map, orc::ColumnVectorBatch* orc_col_batch,
                               int64_t start, int64_t end, vectorized::Arena& arena) const override;

    Status serialize_column_to_jsonb(const IColumn& from_column, int64_t row_num,
                                     JsonbWriter& writer) const override;

    Status deserialize_column_from_jsonb(IColumn& column, const JsonbValue* jsonb_value,
                                         CastParameters& castParms) const override;

    void set_return_object_as_string(bool value) override {
        DataTypeSerDe::set_return_object_as_string(value);
        for (auto& serde : elem_serdes_ptrs) {
            serde->set_return_object_as_string(value);
        }
    }

    DataTypeSerDeSPtrs get_nested_serdes() const override { return elem_serdes_ptrs; }

private:
    template <bool is_binary_format>
    Status _write_column_to_mysql(const IColumn& column, bool return_object_data_as_binary,
                                  std::vector<MysqlRowBuffer<is_binary_format>>& result,
                                  int64_t row_idx, int start, int end, bool col_const,
                                  const FormatOptions& options) const;
    template <bool is_binary_format>
    Status _write_column_to_mysql(const IColumn& column, MysqlRowBuffer<is_binary_format>& result,
                                  int64_t row_idx, bool col_const,
                                  const FormatOptions& options) const;

    template <bool is_strict_mode>
    Status _from_string(StringRef& str, IColumn& column, const FormatOptions& options) const;

    DataTypeSerDeSPtrs elem_serdes_ptrs;
    Strings elem_names;
};
#include "common/compile_check_end.h"
} // namespace vectorized
} // namespace doris
