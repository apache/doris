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

#include "core/custom_allocator.h"
#include "core/data_type_serde/data_type_serde.h"

namespace doris {

// FILE exposes the same six fields at every output boundary.
// PB/Arrow/ORC readers decode children; callers validate_file_column at the complete-value
// boundary after restoring all ancestor null maps. Child serdes cannot see those masks.
class DataTypeFileSerDe final : public DataTypeSerDe {
public:
    explicit DataTypeFileSerDe(int nesting_level = 1);
    std::string get_name() const override { return "File"; }

    Status from_string(StringRef& str, IColumn& column,
                       const FormatOptions& options) const override;
    void to_string(const IColumn& column, size_t row_num, BufferWritable& bw,
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
    Status serialize_column_to_jsonb(const IColumn& column, int64_t row_num,
                                     JsonbWriter& writer) const override;
    Status deserialize_column_from_jsonb(IColumn& column, const JsonbValue* value,
                                         CastParameters& cast_parameters) const override;

    bool write_column_to_mysql_text(const IColumn& column, BufferWritable& bw, int64_t row_idx,
                                    const FormatOptions& options) const override;
    Status write_column_to_mysql_binary(const IColumn& column, MysqlRowBinaryBuffer& row_buffer,
                                        int64_t row_idx, bool col_const,
                                        const FormatOptions& options) const override;
    Status write_column_to_pb(const IColumn& column, PValues& result, int64_t start,
                              int64_t end) const override;
    Status read_column_from_pb(IColumn& column, const PValues& arg) const override;
    void write_one_cell_to_jsonb(const IColumn& column, JsonbWriter& writer, Arena& arena,
                                 int32_t col_id, int64_t row_num,
                                 const FormatOptions& options) const override;
    void read_one_cell_from_jsonb(IColumn& column, const JsonbValue* arg) const override;

    Status write_column_to_arrow(const IColumn& column, const NullMap* null_map,
                                 arrow::ArrayBuilder* builder, int64_t start, int64_t end,
                                 const cctz::time_zone& ctz) const override;
    Status read_column_from_arrow(IColumn& column, const arrow::Array* array, int64_t start,
                                  int64_t end, const cctz::time_zone& ctz) const override;
    Status write_column_to_orc(const std::string& timezone, const IColumn& column,
                               const NullMap* null_map, orc::ColumnVectorBatch* batch,
                               int64_t start, int64_t end, Arena& arena,
                               const FormatOptions& options) const override;
    Status read_column_from_orc(IColumn& column, const OrcDecodedColumnView& view) const override;

    DataTypeSerDeSPtrs get_nested_serdes() const override { return _element_serdes; }
    void set_return_object_as_string(bool value) override;

private:
    Status _read_json(StringRef str, IColumn& column) const;
    static Status _decode_inline(StringRef encoded, DorisVector<char>& scratch, Field& value);
    static Status _checked_inline_size(size_t encoded_size, size_t padding, uint32_t* decoded_size);
    void _write_json(const IColumn& column, int64_t row, BufferWritable& bw) const;

    DataTypeSerDeSPtrs _element_serdes;
    Strings _names;
};

} // namespace doris
