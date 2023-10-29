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

#include "data_type_hll_serde.h"

#include <gen_cpp/types.pb.h>
#include <stddef.h>
#include <stdint.h>

#include <string>

#include "arrow/array/builder_binary.h"
#include "olap/hll.h"
#include "util/jsonb_document.h"
#include "util/slice.h"
#include "vec/columns/column_complex.h"
#include "vec/columns/column_const.h"
#include "vec/common/arena.h"
#include "vec/common/assert_cast.h"
#include "vec/data_types/serde/data_type_nullable_serde.h"

namespace doris {

namespace vectorized {
class IColumn;

Status DataTypeHLLSerDe::serialize_column_to_json(const IColumn& column, int start_idx, int end_idx,
                                                  BufferWritable& bw, FormatOptions& options,
                                                  int nesting_level) const {
    SERIALIZE_COLUMN_TO_JSON();
}

Status DataTypeHLLSerDe::serialize_one_cell_to_json(const IColumn& column, int row_num,
                                                    BufferWritable& bw, FormatOptions& options,
                                                    int nesting_level) const {
    if (!options._output_object_data) {
        /**
         * For null values in ordinary types, we use \N to represent them;
         * for null values in nested types, we use null to represent them, just like the json format.
         */
        if (nesting_level >= 2) {
            bw.write(DataTypeNullableSerDe::NULL_IN_CSV_FOR_NESTED_TYPE.c_str(), 4);
        } else {
            bw.write(DataTypeNullableSerDe::NULL_IN_CSV_FOR_ORDINARY_TYPE.c_str(), 2);
        }
        return Status::OK();
    }
    auto col_row = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = col_row.first;
    row_num = col_row.second;
    auto& data = const_cast<HyperLogLog&>(assert_cast<const ColumnHLL&>(*ptr).get_element(row_num));
    std::unique_ptr<char[]> buf = std::make_unique<char[]>(data.max_serialized_size());
    size_t size = data.serialize((uint8*)buf.get());
    bw.write(buf.get(), size);
    return Status::OK();
}

Status DataTypeHLLSerDe::deserialize_column_from_json_vector(IColumn& column,
                                                             std::vector<Slice>& slices,
                                                             int* num_deserialized,
                                                             const FormatOptions& options,
                                                             int nesting_level) const {
    DESERIALIZE_COLUMN_FROM_JSON_VECTOR();
    return Status::OK();
}

Status DataTypeHLLSerDe::deserialize_one_cell_from_json(IColumn& column, Slice& slice,
                                                        const FormatOptions& options,
                                                        int nesting_level) const {
    auto& data_column = assert_cast<ColumnHLL&>(column);

    HyperLogLog hyper_log_log(slice);
    data_column.insert_value(hyper_log_log);
    return Status::OK();
}

Status DataTypeHLLSerDe::write_column_to_pb(const IColumn& column, PValues& result, int start,
                                            int end) const {
    auto ptype = result.mutable_type();
    ptype->set_id(PGenericType::HLL);
    auto& data_column = assert_cast<const ColumnHLL&>(column);
    int row_count = end - start;
    result.mutable_bytes_value()->Reserve(row_count);
    for (size_t row_num = start; row_num < end; ++row_num) {
        auto& value = const_cast<HyperLogLog&>(data_column.get_element(row_num));
        std::string memory_buffer(value.max_serialized_size(), '0');
        value.serialize((uint8_t*)memory_buffer.data());
        result.add_bytes_value(memory_buffer.data(), memory_buffer.size());
    }
    return Status::OK();
}
Status DataTypeHLLSerDe::read_column_from_pb(IColumn& column, const PValues& arg) const {
    auto& col = reinterpret_cast<ColumnHLL&>(column);
    for (int i = 0; i < arg.bytes_value_size(); ++i) {
        HyperLogLog value;
        value.deserialize(Slice(arg.bytes_value(i)));
        col.insert_value(value);
    }
    return Status::OK();
}

void DataTypeHLLSerDe::write_one_cell_to_jsonb(const IColumn& column, JsonbWriter& result,
                                               Arena* mem_pool, int32_t col_id, int row_num) const {
    result.writeKey(col_id);
    auto& data_column = assert_cast<const ColumnHLL&>(column);
    auto& hll_value = const_cast<HyperLogLog&>(data_column.get_element(row_num));
    auto size = hll_value.max_serialized_size();
    auto ptr = reinterpret_cast<char*>(mem_pool->alloc(size));
    size_t actual_size = hll_value.serialize((uint8_t*)ptr);
    result.writeStartBinary();
    result.writeBinary(reinterpret_cast<const char*>(ptr), actual_size);
    result.writeEndBinary();
}
void DataTypeHLLSerDe::read_one_cell_from_jsonb(IColumn& column, const JsonbValue* arg) const {
    auto& col = reinterpret_cast<ColumnHLL&>(column);
    auto blob = static_cast<const JsonbBlobVal*>(arg);
    HyperLogLog hyper_log_log(Slice(blob->getBlob()));
    col.insert_value(hyper_log_log);
}

void DataTypeHLLSerDe::write_column_to_arrow(const IColumn& column, const NullMap* null_map,
                                             arrow::ArrayBuilder* array_builder, int start,
                                             int end) const {
    const auto& col = assert_cast<const ColumnHLL&>(column);
    auto& builder = assert_cast<arrow::StringBuilder&>(*array_builder);
    for (size_t string_i = start; string_i < end; ++string_i) {
        if (null_map && (*null_map)[string_i]) {
            checkArrowStatus(builder.AppendNull(), column.get_name(),
                             array_builder->type()->name());
        } else {
            auto& hll_value = const_cast<HyperLogLog&>(col.get_element(string_i));
            std::string memory_buffer(hll_value.max_serialized_size(), '0');
            hll_value.serialize((uint8_t*)memory_buffer.data());
            checkArrowStatus(
                    builder.Append(memory_buffer.data(), static_cast<int>(memory_buffer.size())),
                    column.get_name(), array_builder->type()->name());
        }
    }
}

template <bool is_binary_format>
Status DataTypeHLLSerDe::_write_column_to_mysql(const IColumn& column,
                                                MysqlRowBuffer<is_binary_format>& result,
                                                int row_idx, bool col_const) const {
    auto& data_column = assert_cast<const ColumnHLL&>(column);
    if (_return_object_as_string) {
        const auto col_index = index_check_const(row_idx, col_const);
        HyperLogLog hyperLogLog = data_column.get_element(col_index);
        size_t size = hyperLogLog.max_serialized_size();
        std::unique_ptr<char[]> buf = std::make_unique<char[]>(size);
        hyperLogLog.serialize((uint8*)buf.get());
        if (UNLIKELY(0 != result.push_string(buf.get(), size))) {
            return Status::InternalError("pack mysql buffer failed.");
        }
    } else {
        if (UNLIKELY(0 != result.push_null())) {
            return Status::InternalError("pack mysql buffer failed.");
        }
    }
    return Status::OK();
}

Status DataTypeHLLSerDe::write_column_to_mysql(const IColumn& column,
                                               MysqlRowBuffer<true>& row_buffer, int row_idx,
                                               bool col_const) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const);
}

Status DataTypeHLLSerDe::write_column_to_mysql(const IColumn& column,
                                               MysqlRowBuffer<false>& row_buffer, int row_idx,
                                               bool col_const) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const);
}

Status DataTypeHLLSerDe::write_column_to_orc(const std::string& timezone, const IColumn& column,
                                             const NullMap* null_map,
                                             orc::ColumnVectorBatch* orc_col_batch, int start,
                                             int end, std::vector<StringRef>& buffer_list) const {
    auto& col_data = assert_cast<const ColumnHLL&>(column);
    orc::StringVectorBatch* cur_batch = dynamic_cast<orc::StringVectorBatch*>(orc_col_batch);

    for (size_t row_id = start; row_id < end; row_id++) {
        if (cur_batch->notNull[row_id] == 1) {
            const auto& ele = col_data.get_data_at(row_id);
            cur_batch->data[row_id] = const_cast<char*>(ele.data);
            cur_batch->length[row_id] = ele.size;
        }
    }

    cur_batch->numElements = end - start;
    return Status::OK();
}

} // namespace vectorized
} // namespace doris