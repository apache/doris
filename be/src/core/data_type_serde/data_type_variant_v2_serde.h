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

#include "core/data_type_serde/data_type_serde.h"

namespace doris {

// Direct SerDe used when the logical Variant type carries a ColumnVariantV2 physical column.
class DataTypeVariantV2SerDe final : public DataTypeSerDe {
public:
    explicit DataTypeVariantV2SerDe(int nesting_level = 1);

    static int64_t get_uncompressed_serialized_bytes(const IColumn& column, int be_exec_version);
    static char* serialize(const IColumn& column, char* buf, int be_exec_version);
    static const char* deserialize(const char* buf, MutableColumnPtr* column, int be_exec_version);

    std::string get_name() const override;

    Status serialize_one_cell_to_json(const IColumn& column, int64_t row_num, BufferWritable& bw,
                                      FormatOptions& options) const override;
    Status serialize_column_to_json(const IColumn& column, int64_t start_idx, int64_t end_idx,
                                    BufferWritable& bw, FormatOptions& options) const override;
    Status deserialize_one_cell_from_json(IColumn& column, Slice& slice,
                                          const FormatOptions& options) const override;
    Status deserialize_one_cell_from_csv(IColumn& column, Slice& slice,
                                         const FormatOptions& options) const override;
    Status deserialize_column_from_json_vector(IColumn& column, std::vector<Slice>& slices,
                                               uint64_t* num_deserialized,
                                               const FormatOptions& options) const override;

    Status write_column_to_pb(const IColumn& column, PValues& result, int64_t start,
                              int64_t end) const override;
    Status read_column_from_pb(IColumn& column, const PValues& arg) const override;

    void write_one_cell_to_jsonb(const IColumn& column, JsonbWriter& result, Arena& mem_pool,
                                 int32_t col_id, int64_t row_num,
                                 const FormatOptions& options) const override;
    void read_one_cell_from_jsonb(IColumn& column, const JsonbValue* arg) const override;

    Status write_column_to_arrow(const IColumn& column, const NullMap* null_map,
                                 arrow::ArrayBuilder* array_builder, int64_t start, int64_t end,
                                 const cctz::time_zone& ctz) const override;
    Status read_column_from_arrow(IColumn& column, const arrow::Array* arrow_array, int64_t start,
                                  int64_t end, const cctz::time_zone& ctz) const override;
    Status write_column_to_mysql_binary(const IColumn& column, MysqlRowBinaryBuffer& row_buffer,
                                        int64_t row_idx, bool col_const,
                                        const FormatOptions& options) const override;
    Status write_column_to_orc(const std::string& timezone, const IColumn& column,
                               const NullMap* null_map, orc::ColumnVectorBatch* orc_col_batch,
                               int64_t start, int64_t end, Arena& arena,
                               const FormatOptions& options) const override;

    void to_string(const IColumn& column, size_t row_num, BufferWritable& bw,
                   const FormatOptions& options) const override;
};

} // namespace doris
#include <cstring>
#include <limits>

#include "common/exception.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "exprs/function/parse/variant_string_parse.h"

namespace doris::data_type_variant_v2_serde_internal {

using column_variant_v2_internal::ForcedNulls;
using column_variant_v2_internal::visit_variant_values;

inline ForcedNulls forced_nulls(const NullMap* null_map) {
    return null_map == nullptr ? ForcedNulls {} : ForcedNulls {null_map->data(), null_map->size()};
}

inline size_t checked_row(int64_t row) {
    if (row < 0) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant SerDe row {} is negative", row);
    }
    return static_cast<size_t>(row);
}

struct CountingWriter {
    void write(const char*, size_t size) {
        if (size > std::numeric_limits<size_t>::max() - count) {
            throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant JSON output is too large");
        }
        count += size;
    }
    size_t count = 0;
};

struct FixedWriter {
    void write(const char* data, size_t size) {
        DCHECK_LE(written, capacity);
        DCHECK_LE(size, capacity - written);
        memcpy(destination + written, data, size);
        written += size;
    }
    char* destination;
    size_t capacity;
    size_t written = 0;
};

template <typename Writer>
void write_json_value(VariantRef value, Writer& writer,
                      const DataTypeSerDe::FormatOptions& options) {
    VariantJsonFormatOptions json_options {.timezone = options.timezone};
    to_json(value, writer, json_options);
}

} // namespace doris::data_type_variant_v2_serde_internal
