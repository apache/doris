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
#include <stddef.h>
#include <stdint.h>

#include "common/status.h"
#include "data_type_serde.h"
#include "util/jsonb_document.h"
#include "util/jsonb_writer.h"
#include "util/quantile_state.h"
#include "util/slice.h"
#include "vec/columns/column.h"
#include "vec/columns/column_complex.h"
#include "vec/columns/column_const.h"
#include "vec/common/arena.h"
#include "vec/common/string_ref.h"

namespace doris {

namespace vectorized {

class DataTypeQuantileStateSerDe : public DataTypeSerDe {
public:
    Status serialize_one_cell_to_json(const IColumn& column, int row_num, BufferWritable& bw,
                                      FormatOptions& options,
                                      int nesting_level = 1) const override {
        return Status::NotSupported("serialize_one_cell_to_json with type [{}]", column.get_name());
    }

    Status serialize_column_to_json(const IColumn& column, int start_idx, int end_idx,
                                    BufferWritable& bw, FormatOptions& options,
                                    int nesting_level = 1) const override {
        return Status::NotSupported("serialize_column_to_json with type [{}]", column.get_name());
    }
    Status deserialize_one_cell_from_json(IColumn& column, Slice& slice,
                                          const FormatOptions& options,
                                          int nesting_level = 1) const override {
        return Status::NotSupported("deserialize_one_cell_from_text with type " +
                                    column.get_name());
    }

    Status deserialize_column_from_json_vector(IColumn& column, std::vector<Slice>& slices,
                                               int* num_deserialized, const FormatOptions& options,
                                               int nesting_level = 1) const override {
        return Status::NotSupported("deserialize_column_from_text_vector with type " +
                                    column.get_name());
    }

    Status write_column_to_pb(const IColumn& column, PValues& result, int start,
                              int end) const override {
        result.mutable_bytes_value()->Reserve(end - start);
        for (size_t row_num = start; row_num < end; ++row_num) {
            StringRef data = column.get_data_at(row_num);
            result.add_bytes_value(data.to_string());
        }
        return Status::OK();
    }
    Status read_column_from_pb(IColumn& column, const PValues& arg) const override {
        column.reserve(arg.bytes_value_size());
        for (int i = 0; i < arg.bytes_value_size(); ++i) {
            column.insert_data(arg.bytes_value(i).c_str(), arg.bytes_value(i).size());
        }
        return Status::OK();
    }

    void write_one_cell_to_jsonb(const IColumn& column, JsonbWriter& result, Arena* mem_pool,
                                 int32_t col_id, int row_num) const override {
        auto& col = reinterpret_cast<const ColumnQuantileState&>(column);
        auto& val = const_cast<QuantileState&>(col.get_element(row_num));
        size_t actual_size = val.get_serialized_size();
        auto ptr = mem_pool->alloc(actual_size);
        result.writeKey(col_id);
        result.writeStartBinary();
        result.writeBinary(reinterpret_cast<const char*>(ptr), actual_size);
        result.writeEndBinary();
    }

    void read_one_cell_from_jsonb(IColumn& column, const JsonbValue* arg) const override {
        auto& col = reinterpret_cast<ColumnQuantileState&>(column);
        auto blob = static_cast<const JsonbBlobVal*>(arg);
        QuantileState val;
        val.deserialize(Slice(blob->getBlob()));
        col.insert_value(val);
    }
    void write_column_to_arrow(const IColumn& column, const NullMap* null_map,
                               arrow::ArrayBuilder* array_builder, int start,
                               int end) const override {
        throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                               "write_column_to_arrow with type " + column.get_name());
    }
    void read_column_from_arrow(IColumn& column, const arrow::Array* arrow_array, int start,
                                int end, const cctz::time_zone& ctz) const override {
        throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                               "read_column_from_arrow with type " + column.get_name());
    }

    Status write_column_to_mysql(const IColumn& column, MysqlRowBuffer<true>& row_buffer,
                                 int row_idx, bool col_const) const override {
        return _write_column_to_mysql(column, row_buffer, row_idx, col_const);
    }
    Status write_column_to_mysql(const IColumn& column, MysqlRowBuffer<false>& row_buffer,
                                 int row_idx, bool col_const) const override {
        return _write_column_to_mysql(column, row_buffer, row_idx, col_const);
    }

    Status write_column_to_orc(const IColumn& column, const NullMap* null_map,
                               orc::ColumnVectorBatch* orc_col_batch, int start, int end,
                               std::vector<StringRef>& buffer_list) const override {
        return Status::NotSupported("write_column_to_orc with type [{}]", column.get_name());
    }

private:
    template <bool is_binary_format>
    Status _write_column_to_mysql(const IColumn& column, MysqlRowBuffer<is_binary_format>& result,
                                  int row_idx, bool col_const) const;
};

// QuantileState is binary data which is not shown by mysql
template <bool is_binary_format>
Status DataTypeQuantileStateSerDe::_write_column_to_mysql(const IColumn& column,
                                                          MysqlRowBuffer<is_binary_format>& result,
                                                          int row_idx, bool col_const) const {
    auto& data_column = reinterpret_cast<const ColumnQuantileState&>(column);

    if (_return_object_as_string) {
        const auto col_index = index_check_const(row_idx, col_const);
        auto& quantile_value = const_cast<QuantileState&>(data_column.get_element(col_index));
        size_t size = quantile_value.get_serialized_size();
        std::unique_ptr<char[]> buf = std::make_unique<char[]>(size);
        quantile_value.serialize((uint8_t*)buf.get());
        if (0 != result.push_string(buf.get(), size)) {
            return Status::InternalError("pack mysql buffer failed.");
        }
    } else {
        if (0 != result.push_null()) {
            return Status::InternalError("pack mysql buffer failed.");
        }
    }
    return Status::OK();
}

} // namespace vectorized
} // namespace doris
