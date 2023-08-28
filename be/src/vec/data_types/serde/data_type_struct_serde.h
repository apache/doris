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
    DataTypeStructSerDe(const DataTypeSerDeSPtrs& _elemSerDeSPtrs)
            : elemSerDeSPtrs(_elemSerDeSPtrs) {}

    void serialize_one_cell_to_text(const IColumn& column, int row_num, BufferWritable& bw,
                                    FormatOptions& options) const override {
        throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                               "serialize_one_cell_to_text with type " + column.get_name());
    }

    void serialize_column_to_text(const IColumn& column, int start_idx, int end_idx,
                                  BufferWritable& bw, FormatOptions& options) const override {
        throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                               "serialize_column_to_text with type " + column.get_name());
    }

    Status deserialize_one_cell_from_text(IColumn& column, Slice& slice,
                                          const FormatOptions& options) const override {
        throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                               "deserialize_one_cell_from_text with type " + column.get_name());
    }

    Status deserialize_column_from_text_vector(IColumn& column, std::vector<Slice>& slices,
                                               int* num_deserialized,
                                               const FormatOptions& options) const override {
        throw doris::Exception(
                ErrorCode::NOT_IMPLEMENTED_ERROR,
                "deserialize_column_from_text_vector with type " + column.get_name());
    }
    Status write_column_to_pb(const IColumn& column, PValues& result, int start,
                              int end) const override {
        throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                               "write_column_to_pb with type " + column.get_name());
    }
    Status read_column_from_pb(IColumn& column, const PValues& arg) const override {
        throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                               "read_column_from_pb with type " + column.get_name());
    }
    void write_one_cell_to_jsonb(const IColumn& column, JsonbWriter& result, Arena* mem_pool,
                                 int32_t col_id, int row_num) const override;

    void read_one_cell_from_jsonb(IColumn& column, const JsonbValue* arg) const override;

    void write_column_to_arrow(const IColumn& column, const NullMap* null_map,
                               arrow::ArrayBuilder* array_builder, int start,
                               int end) const override;
    void read_column_from_arrow(IColumn& column, const arrow::Array* arrow_array, int start,
                                int end, const cctz::time_zone& ctz) const override;

    Status write_column_to_mysql(const IColumn& column, MysqlRowBuffer<true>& row_buffer,
                                 int row_idx, bool col_const) const override;
    Status write_column_to_mysql(const IColumn& column, MysqlRowBuffer<false>& row_buffer,
                                 int row_idx, bool col_const) const override;

    void set_return_object_as_string(bool value) override {
        DataTypeSerDe::set_return_object_as_string(value);
        for (auto& serde : elemSerDeSPtrs) {
            serde->set_return_object_as_string(value);
        }
    }

private:
    template <bool is_binary_format>
    Status _write_column_to_mysql(const IColumn& column, bool return_object_data_as_binary,
                                  std::vector<MysqlRowBuffer<is_binary_format>>& result,
                                  int row_idx, int start, int end, bool col_const) const;
    template <bool is_binary_format>
    Status _write_column_to_mysql(const IColumn& column, MysqlRowBuffer<is_binary_format>& result,
                                  int row_idx, bool col_const) const;

    DataTypeSerDeSPtrs elemSerDeSPtrs;
};
} // namespace vectorized
} // namespace doris
