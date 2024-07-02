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
#include <stdint.h>

#include <ostream>

#include "common/status.h"
#include "data_type_serde.h"
#include "util/jsonb_writer.h"

namespace doris {
class PValues;
class JsonbValue;

namespace vectorized {
class IColumn;
class Arena;

class DataTypeStructSerDe : public DataTypeSerDe {
public:
    static bool next_slot_from_string(ReadBuffer& rb, StringRef& output, bool& is_name,
                                      bool& has_quota) {
        StringRef element(rb.position(), 0);
        has_quota = false;
        is_name = false;
        if (rb.eof()) {
            return false;
        }

        // ltrim
        while (!rb.eof() && isspace(*rb.position())) {
            ++rb.position();
            element.data = rb.position();
        }

        // parse string
        if (*rb.position() == '"' || *rb.position() == '\'') {
            const char str_sep = *rb.position();
            size_t str_len = 1;
            // search until next '"' or '\''
            while (str_len < rb.count() && *(rb.position() + str_len) != str_sep) {
                if (*(rb.position() + str_len) == '\\' && str_len + 1 < rb.count()) {
                    ++str_len;
                }
                ++str_len;
            }
            // invalid string
            if (str_len >= rb.count()) {
                rb.position() = rb.end();
                return false;
            }
            has_quota = true;
            rb.position() += str_len + 1;
            element.size += str_len + 1;
        }

        // parse element until separator ':' or ',' or end '}'
        while (!rb.eof() && (*rb.position() != ':') && (*rb.position() != ',') &&
               (rb.count() != 1 || *rb.position() != '}')) {
            if (has_quota && !isspace(*rb.position())) {
                return false;
            }
            ++rb.position();
            ++element.size;
        }
        // invalid element
        if (rb.eof()) {
            return false;
        }

        if (*rb.position() == ':') {
            is_name = true;
        }

        // adjust read buffer position to first char of next element
        ++rb.position();

        // rtrim
        while (element.size > 0 && isspace(element.data[element.size - 1])) {
            --element.size;
        }

        // trim '"' and '\'' for string
        if (element.size >= 2 && (element.data[0] == '"' || element.data[0] == '\'') &&
            element.data[0] == element.data[element.size - 1]) {
            ++element.data;
            element.size -= 2;
        }
        output = element;
        return true;
    }

    DataTypeStructSerDe(const DataTypeSerDeSPtrs& _elem_serdes_ptrs, const Strings names,
                        int nesting_level = 1)
            : DataTypeSerDe(nesting_level),
              elem_serdes_ptrs(_elem_serdes_ptrs),
              elem_names(names) {}

    Status serialize_one_cell_to_json(const IColumn& column, int row_num, BufferWritable& bw,
                                      FormatOptions& options) const override;

    Status serialize_column_to_json(const IColumn& column, int start_idx, int end_idx,
                                    BufferWritable& bw, FormatOptions& options) const override;

    Status deserialize_one_cell_from_json(IColumn& column, Slice& slice,
                                          const FormatOptions& options) const override;

    Status deserialize_column_from_json_vector(IColumn& column, std::vector<Slice>& slices,
                                               int* num_deserialized,
                                               const FormatOptions& options) const override;

    Status deserialize_one_cell_from_hive_text(
            IColumn& column, Slice& slice, const FormatOptions& options,
            int hive_text_complex_type_delimiter_level = 1) const override;
    Status deserialize_column_from_hive_text_vector(
            IColumn& column, std::vector<Slice>& slices, int* num_deserialized,
            const FormatOptions& options,
            int hive_text_complex_type_delimiter_level = 1) const override;
    void serialize_one_cell_to_hive_text(
            const IColumn& column, int row_num, BufferWritable& bw, FormatOptions& options,
            int hive_text_complex_type_delimiter_level = 1) const override;

    Status write_column_to_pb(const IColumn& column, PValues& result, int start,
                              int end) const override;
    Status read_column_from_pb(IColumn& column, const PValues& arg) const override;
    void write_one_cell_to_jsonb(const IColumn& column, JsonbWriter& result, Arena* mem_pool,
                                 int32_t col_id, int row_num) const override;

    void read_one_cell_from_jsonb(IColumn& column, const JsonbValue* arg) const override;

    void write_column_to_arrow(const IColumn& column, const NullMap* null_map,
                               arrow::ArrayBuilder* array_builder, int start, int end,
                               const cctz::time_zone& ctz) const override;
    void read_column_from_arrow(IColumn& column, const arrow::Array* arrow_array, int start,
                                int end, const cctz::time_zone& ctz) const override;

    Status write_column_to_mysql(const IColumn& column, MysqlRowBuffer<true>& row_buffer,
                                 int row_idx, bool col_const,
                                 const FormatOptions& options) const override;
    Status write_column_to_mysql(const IColumn& column, MysqlRowBuffer<false>& row_buffer,
                                 int row_idx, bool col_const,
                                 const FormatOptions& options) const override;

    Status write_column_to_orc(const std::string& timezone, const IColumn& column,
                               const NullMap* null_map, orc::ColumnVectorBatch* orc_col_batch,
                               int start, int end,
                               std::vector<StringRef>& buffer_list) const override;

    void set_return_object_as_string(bool value) override {
        DataTypeSerDe::set_return_object_as_string(value);
        for (auto& serde : elem_serdes_ptrs) {
            serde->set_return_object_as_string(value);
        }
    }

private:
    std::optional<size_t> try_get_position_by_name(const String& name) const;

    template <bool is_binary_format>
    Status _write_column_to_mysql(const IColumn& column, bool return_object_data_as_binary,
                                  std::vector<MysqlRowBuffer<is_binary_format>>& result,
                                  int row_idx, int start, int end, bool col_const,
                                  const FormatOptions& options) const;
    template <bool is_binary_format>
    Status _write_column_to_mysql(const IColumn& column, MysqlRowBuffer<is_binary_format>& result,
                                  int row_idx, bool col_const, const FormatOptions& options) const;

    DataTypeSerDeSPtrs elem_serdes_ptrs;
    Strings elem_names;
};
} // namespace vectorized
} // namespace doris
