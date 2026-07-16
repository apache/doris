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

#include "core/data_type_serde/data_type_varbinary_serde.h"

#include <cstring>

#include "core/column/column_varbinary.h"
#include "core/data_type_serde/parquet_decode_source.h"
#include "core/data_type_serde/arrow_validation.h"

namespace doris {
namespace {

class VarbinaryParquetConsumer final : public ParquetFixedValueConsumer,
                                       public ParquetBinaryValueConsumer {
public:
    explicit VarbinaryParquetConsumer(IColumn& column)
            : _column(assert_cast<ColumnVarbinary&>(column)) {}

    Status consume(const uint8_t* values, size_t num_values, size_t value_width) override {
        for (size_t row = 0; row < num_values; ++row) {
            _column.insert_data(reinterpret_cast<const char*>(values + row * value_width),
                                value_width);
        }
        return Status::OK();
    }

    Status consume(const StringRef* values, size_t num_values) override {
        for (size_t row = 0; row < num_values; ++row) {
            _column.insert_data(values[row].data, values[row].size);
        }
        return Status::OK();
    }

    Status consume_plain_byte_array(const char* encoded_data, const uint32_t* payload_offsets,
                                    const uint32_t* value_offsets, size_t num_values,
                                    const std::vector<ParquetSelectionRange>&) override {
        for (size_t row = 0; row < num_values; ++row) {
            _column.insert_data(encoded_data + payload_offsets[row],
                                value_offsets[row + 1] - value_offsets[row]);
        }
        return Status::OK();
    }

private:
    ColumnVarbinary& _column;
};

} // namespace

Status DataTypeVarbinarySerDe::read_parquet_dictionary(IColumn& column, ParquetDecodeSource& source,
                                                       const ParquetDecodeContext& context) const {
    VarbinaryParquetConsumer consumer(column);
    return source.decode_dictionary(consumer, consumer);
}

Status DataTypeVarbinarySerDe::read_column_from_parquet(IColumn& column,
                                                        ParquetDecodeSource& source,
                                                        const ParquetDecodeContext& context,
                                                        size_t num_values,
                                                        ParquetMaterializationState& state) const {
    VarbinaryParquetConsumer consumer(column);
    if (context.encoding != ParquetValueEncoding::DICTIONARY) {
        if (context.physical_type == ParquetPhysicalType::BYTE_ARRAY) {
            return source.decode_binary_values(num_values, consumer);
        }
        if (context.physical_type == ParquetPhysicalType::FIXED_LEN_BYTE_ARRAY) {
            return source.decode_fixed_values(num_values, consumer);
        }
        return Status::NotSupported("Unsupported Parquet physical type for VARBINARY");
    }
    if (state.dictionary_generation != source.dictionary_generation()) {
        state.typed_dictionary = column.clone_empty();
        RETURN_IF_ERROR(read_parquet_dictionary(*state.typed_dictionary, source, context));
        DORIS_CHECK_EQ(state.typed_dictionary->size(), source.dictionary_size());
        state.dictionary_generation = source.dictionary_generation();
    }
    return state.materialize_dictionary(column, source, num_values);
}

void DataTypeVarbinarySerDe::write_one_cell_to_jsonb(const IColumn& column, JsonbWriter& result,
                                                     Arena& arena, int32_t col_id, int64_t row_num,
                                                     const FormatOptions& options) const {
    throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                           "Data type {} read_one_cell_from_jsonb ostr not implement.",
                           column.get_name());
}

void DataTypeVarbinarySerDe::read_one_cell_from_jsonb(IColumn& column,
                                                      const JsonbValue* arg) const {
    throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                           "Data type {} read_one_cell_from_jsonb ostr not implement.",
                           column.get_name());
}

Status DataTypeVarbinarySerDe::write_column_to_mysql_binary(const IColumn& column,
                                                            MysqlRowBinaryBuffer& result,
                                                            int64_t row_idx, bool col_const,
                                                            const FormatOptions& options) const {
    auto col_index = index_check_const(row_idx, col_const);
    const auto& data = assert_cast<const ColumnVarbinary&>(column).get_data()[col_index];

    if (0 != result.push_string(data.data(), data.size())) {
        return Status::InternalError("pack mysql buffer failed.");
    }

    return Status::OK();
}

Status DataTypeVarbinarySerDe::write_column_to_arrow(const IColumn& column, const NullMap* null_map,
                                                     arrow::ArrayBuilder* array_builder,
                                                     int64_t start, int64_t end,
                                                     const cctz::time_zone& ctz) const {
    auto lambda_function = [&](auto& builder) -> Status {
        const auto& varbinary_column_data = assert_cast<const ColumnVarbinary&>(column).get_data();
        for (size_t i = start; i < end; ++i) {
            if (null_map && (*null_map)[i]) {
                RETURN_IF_ERROR(checkArrowStatus(builder.AppendNull(), column, builder));
                continue;
            }
            const auto& string_view = varbinary_column_data[i];
            RETURN_IF_ERROR(checkArrowStatus(builder.Append(string_view.data(), string_view.size()),
                                             column, builder));
        }
        return Status::OK();
    };
    if (array_builder->type()->id() == arrow::Type::BINARY) {
        auto& builder = assert_cast<arrow::BinaryBuilder&>(*array_builder);
        return lambda_function(builder);
    } else if (array_builder->type()->id() == arrow::Type::LARGE_BINARY) {
        auto& builder = assert_cast<arrow::LargeBinaryBuilder&>(*array_builder);
        const auto& varbinary_column_data = assert_cast<const ColumnVarbinary&>(column).get_data();
        for (size_t i = start; i < end; ++i) {
            if (null_map && (*null_map)[i]) {
                RETURN_IF_ERROR(checkArrowStatus(builder.AppendNull(), column, builder));
                continue;
            }
            const auto& string_view = varbinary_column_data[i];
            RETURN_IF_ERROR(checkArrowStatus(
                    builder.Append(reinterpret_cast<const uint8_t*>(string_view.data()),
                                   cast_set<int64_t, size_t, false>(string_view.size())),
                    column, builder));
        }
        return Status::OK();
    } else if (array_builder->type()->id() == arrow::Type::STRING) {
        auto& builder = assert_cast<arrow::StringBuilder&>(*array_builder);
        return lambda_function(builder);
    } else if (array_builder->type()->id() == arrow::Type::FIXED_SIZE_BINARY) {
        auto& builder = assert_cast<arrow::FixedSizeBinaryBuilder&>(*array_builder);
        const int byte_width =
                static_cast<const arrow::FixedSizeBinaryType&>(*array_builder->type()).byte_width();
        const auto& varbinary_column_data = assert_cast<const ColumnVarbinary&>(column).get_data();
        for (size_t i = start; i < end; ++i) {
            if (null_map && (*null_map)[i]) {
                RETURN_IF_ERROR(checkArrowStatus(builder.AppendNull(), column, builder));
                continue;
            }
            const auto& string_view = varbinary_column_data[i];
            if (string_view.size() != byte_width) {
                return Status::InvalidArgument("Fixed size binary column expects {} bytes, got {}",
                                               byte_width, string_view.size());
            }
            RETURN_IF_ERROR(checkArrowStatus(
                    builder.Append(reinterpret_cast<const uint8_t*>(string_view.data())), column,
                    builder));
        }
        return Status::OK();
    } else {
        return Status::InvalidArgument("Unsupported arrow type for varbinary column: {}",
                                       array_builder->type()->name());
    }
    return Status::OK();
}

Status DataTypeVarbinarySerDe::read_column_from_arrow(IColumn& column,
                                                      const arrow::Array* arrow_array,
                                                      int64_t start, int64_t end,
                                                      const cctz::time_zone& ctz) const {
    auto& varbinary_column = assert_cast<ColumnVarbinary&>(column);
    if (arrow_array->type_id() == arrow::Type::STRING ||
        arrow_array->type_id() == arrow::Type::BINARY) {
        const auto* concrete_array = assert_cast<const arrow::BinaryArray*>(arrow_array);
        if (config::enable_arrow_input_validation) {
            check_arrow_array_range(*concrete_array, start, end);
            check_arrow_binary_offsets_buffer(*concrete_array);
        }
        const auto& buffer = concrete_array->value_data();
        const uint8_t* offsets_data = concrete_array->value_offsets()->data();
        constexpr size_t offset_size = sizeof(int32_t);

        for (int64_t offset_i = start; offset_i < end; ++offset_i) {
            if (concrete_array->IsNull(offset_i)) {
                varbinary_column.insert_default();
                continue;
            }
            int32_t start_offset = 0;
            int32_t end_offset = 0;
            memcpy(&start_offset, offsets_data + offset_i * offset_size, offset_size);
            memcpy(&end_offset, offsets_data + (offset_i + 1) * offset_size, offset_size);
            const auto length = end_offset - start_offset;
            varbinary_column.insert_data(
                    reinterpret_cast<const char*>(buffer->data() + start_offset), length);
        }
    } else if (arrow_array->type_id() == arrow::Type::FIXED_SIZE_BINARY) {
        const auto* concrete_array = assert_cast<const arrow::FixedSizeBinaryArray*>(arrow_array);
        if (config::enable_arrow_input_validation) {
            check_arrow_array_range(*concrete_array, start, end);
            check_arrow_fixed_width_buffer(*concrete_array,
                                           static_cast<size_t>(concrete_array->byte_width()));
        }
        const auto width = concrete_array->byte_width();
        for (int64_t offset_i = start; offset_i < end; ++offset_i) {
            if (concrete_array->IsNull(offset_i)) {
                varbinary_column.insert_default();
            } else {
                varbinary_column.insert_data(
                        reinterpret_cast<const char*>(concrete_array->GetValue(offset_i)), width);
            }
        }
    } else if (arrow_array->type_id() == arrow::Type::LARGE_STRING ||
               arrow_array->type_id() == arrow::Type::LARGE_BINARY) {
        const auto* concrete_array = assert_cast<const arrow::LargeBinaryArray*>(arrow_array);
        if (config::enable_arrow_input_validation) {
            check_arrow_array_range(*concrete_array, start, end);
            check_arrow_binary_offsets_buffer(*concrete_array);
        }
        const auto& buffer = concrete_array->value_data();
        for (int64_t offset_i = start; offset_i < end; ++offset_i) {
            if (concrete_array->IsNull(offset_i)) {
                varbinary_column.insert_default();
                continue;
            }
            const auto value_offset = concrete_array->value_offset(offset_i);
            const auto value_length = concrete_array->value_length(offset_i);
            varbinary_column.insert_data(
                    reinterpret_cast<const char*>(buffer->data() + value_offset), value_length);
        }
    } else {
        return Status::InvalidArgument("Unsupported Arrow type for VARBINARY column: {}",
                                       arrow_array->type()->name());
    }
    return Status::OK();
}

Status DataTypeVarbinarySerDe::write_column_to_orc(const std::string& timezone,
                                                   const IColumn& column, const NullMap* null_map,
                                                   orc::ColumnVectorBatch* orc_col_batch,
                                                   int64_t start, int64_t end, Arena& arena,
                                                   const FormatOptions& options) const {
    auto* cur_batch = dynamic_cast<orc::StringVectorBatch*>(orc_col_batch);
    const auto& varbinary_column_data = assert_cast<const ColumnVarbinary&>(column).get_data();

    for (auto row_id = start; row_id < end; row_id++) {
        cur_batch->data[row_id] = const_cast<char*>(varbinary_column_data[row_id].data());
        cur_batch->length[row_id] = varbinary_column_data[row_id].size();
    }

    cur_batch->numElements = end - start;
    return Status::OK();
}

Status DataTypeVarbinarySerDe::serialize_one_cell_to_json(const IColumn& column, int64_t row_num,
                                                          BufferWritable& bw,
                                                          FormatOptions& options) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;
    const auto& value = assert_cast<const ColumnVarbinary&>(*ptr).get_data_at(row_num);
    bw.write(value.data, value.size);
    return Status::OK();
}

Status DataTypeVarbinarySerDe::deserialize_one_cell_from_json(IColumn& column, Slice& slice,
                                                              const FormatOptions& options) const {
    assert_cast<ColumnVarbinary&>(column).insert_data(slice.data, slice.size);
    return Status::OK();
}

void DataTypeVarbinarySerDe::to_string(const IColumn& column, size_t row_num, BufferWritable& bw,
                                       const FormatOptions& options) const {
    const auto& value = assert_cast<const ColumnVarbinary&>(column).get_data()[row_num];
    if (_nesting_level >= 2) { // in complex type, need to dump as hex string by hand
        const auto& hex_str = value.dump_hex();
        bw.write(hex_str.data(), hex_str.size());
    } else { // mysql protocol will be handle as hex binary data directly
        bw.write(value.data(), value.size());
    }
}

} // namespace doris
