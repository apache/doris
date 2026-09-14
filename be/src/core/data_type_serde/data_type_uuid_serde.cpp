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

#include "core/data_type_serde/data_type_uuid_serde.h"

#include <arrow/builder.h>
#include <arrow/extension/uuid.h>

#include <cstring>
#include <utility>

#include "common/config.h"
#include "core/column/column_const.h"
#include "core/data_type_serde/arrow_validation.h"
#include "core/data_type_serde/orc_serde_utils.h"
#include "core/data_type_serde/parquet_decode_source.h"
#include "core/value/uuid_value.h"
#include "util/jsonb_writer.h"

namespace doris {

namespace {

class UUIDParquetConsumer final : public ParquetFixedValueConsumer,
                                  public ParquetBinaryValueConsumer {
public:
    explicit UUIDParquetConsumer(IColumn& column) : _column(assert_cast<ColumnUUID&>(column)) {}

    Status consume(const uint8_t* values, size_t num_values, size_t value_width) override {
        if (value_width != UUIDValue::BINARY_LENGTH) {
            return Status::Corruption("Parquet UUID requires 16 bytes, got {}", value_width);
        }
        auto& data = _column.get_data();
        const auto offset = data.size();
        data.resize(offset + num_values);
        for (size_t row = 0; row < num_values; ++row) {
            data[offset + row] = UUIDValue::from_big_endian(values + row * value_width);
        }
        return Status::OK();
    }

    Status consume(const StringRef* values, size_t num_values) override {
        return Status::Corruption("Parquet UUID requires FIXED_LEN_BYTE_ARRAY(16)");
    }

private:
    ColumnUUID& _column;
};

Status validate_parquet_uuid(const ParquetDecodeContext& context) {
    if (context.physical_type != ParquetPhysicalType::FIXED_LEN_BYTE_ARRAY ||
        std::cmp_not_equal(context.type_length, UUIDValue::BINARY_LENGTH)) {
        return Status::Corruption("Parquet UUID requires FIXED_LEN_BYTE_ARRAY(16)");
    }
    return Status::OK();
}

} // namespace

Status DataTypeUUIDSerDe::read_parquet_dictionary(IColumn& column, ParquetDecodeSource& source,
                                                  const ParquetDecodeContext& context) const {
    RETURN_IF_ERROR(validate_parquet_uuid(context));
    UUIDParquetConsumer consumer(column);
    return source.decode_dictionary(consumer, consumer);
}

Status DataTypeUUIDSerDe::read_column_from_parquet(IColumn& column, ParquetDecodeSource& source,
                                                   const ParquetDecodeContext& context,
                                                   size_t num_values,
                                                   ParquetMaterializationState& state) const {
    RETURN_IF_ERROR(validate_parquet_uuid(context));
    if (context.encoding != ParquetValueEncoding::DICTIONARY) {
        UUIDParquetConsumer consumer(column);
        return source.decode_fixed_values(num_values, consumer);
    }
    if (state.dictionary_generation != source.dictionary_generation()) {
        state.typed_dictionary = column.clone_empty();
        RETURN_IF_ERROR(read_parquet_dictionary(*state.typed_dictionary, source, context));
        DORIS_CHECK_EQ(state.typed_dictionary->size(), source.dictionary_size());
        state.dictionary_generation = source.dictionary_generation();
    }
    return state.materialize_dictionary(column, source, num_values);
}

Status DataTypeUUIDSerDe::write_column_to_mysql_binary(const IColumn& column,
                                                       MysqlRowBinaryBuffer& result,
                                                       int64_t row_idx, bool col_const,
                                                       const FormatOptions& options) const {
    const auto& data = assert_cast<const ColumnUUID&>(column).get_data();
    const auto col_index = index_check_const(row_idx, col_const);
    const auto uuid = UUIDValue::to_string(data[col_index]);
    if (UNLIKELY(result.push_string(uuid.data(), uuid.size()) != 0)) {
        return Status::InternalError("pack mysql buffer failed.");
    }
    return Status::OK();
}

void DataTypeUUIDSerDe::read_one_cell_from_jsonb(IColumn& column, const JsonbValue* arg) const {
    const auto* value = arg->unpack<JsonbBinaryVal>();
    column.deserialize_and_insert_from_arena(value->getBlob());
}

void DataTypeUUIDSerDe::write_one_cell_to_jsonb(const IColumn& column,
                                                JsonbWriterT<JsonbOutStream>& result, Arena& arena,
                                                int col_id, int64_t row_num,
                                                const FormatOptions& options) const {
    result.writeKey(cast_set<JsonbKeyValue::keyid_type>(col_id));
    const char* begin = nullptr;
    StringRef value = column.serialize_value_into_arena(row_num, arena, begin);
    result.writeStartBinary();
    result.writeBinary(value.data, value.size);
    result.writeEndBinary();
}

Status DataTypeUUIDSerDe::serialize_one_cell_to_json(const IColumn& column, int64_t row_num,
                                                     BufferWritable& bw,
                                                     FormatOptions& options) const {
    if (_nesting_level > 1) {
        bw.write('"');
    }
    RETURN_IF_ERROR(serialize_one_cell_to_hive_text(column, row_num, bw, options));
    if (_nesting_level > 1) {
        bw.write('"');
    }
    return Status::OK();
}

Status DataTypeUUIDSerDe::serialize_one_cell_to_hive_text(
        const IColumn& column, int64_t row_num, BufferWritable& bw, FormatOptions& options,
        int hive_text_complex_type_delimiter_level) const {
    // Hive text has no JSON quotes, including UUID elements inside complex values.
    auto [column_ptr, real_row_num] = check_column_const_set_readability(column, row_num);
    const auto value = assert_cast<const ColumnUUID&>(*column_ptr).get_element(real_row_num);
    const auto uuid = UUIDValue::to_string(value);
    bw.write(uuid.data(), uuid.size());
    return Status::OK();
}

Status DataTypeUUIDSerDe::deserialize_one_cell_from_json(IColumn& column, Slice& slice,
                                                         const FormatOptions& options) const {
    if (_nesting_level > 1) {
        slice.trim_quote();
    }
    return deserialize_one_cell_from_hive_text(column, slice, options);
}

Status DataTypeUUIDSerDe::deserialize_one_cell_from_hive_text(
        IColumn& column, Slice& slice, const FormatOptions& options,
        int hive_text_complex_type_delimiter_level) const {
    StringRef input(slice.data, slice.size);
    return from_string(input, column, options);
}

Status DataTypeUUIDSerDe::deserialize_column_from_hive_text_vector(
        IColumn& column, std::vector<Slice>& slices, uint64_t* num_deserialized,
        const FormatOptions& options, int hive_text_complex_type_delimiter_level) const {
    DESERIALIZE_COLUMN_FROM_HIVE_TEXT_VECTOR();
    return Status::OK();
}

Status DataTypeUUIDSerDe::write_column_to_pb(const IColumn& column, PValues& result, int64_t start,
                                             int64_t end) const {
    const auto& column_data = assert_cast<const ColumnUUID&>(column);
    result.mutable_bytes_value()->Reserve(cast_set<int>(end - start));
    result.mutable_type()->set_id(PGenericType::UUID);
    for (auto i = start; i < end; ++i) {
        const auto& value = column_data.get_data_at(i);
        result.add_bytes_value(value.data, value.size);
    }
    return Status::OK();
}

Status DataTypeUUIDSerDe::read_column_from_pb(IColumn& column, const PValues& arg) const {
    for (const auto& value : arg.bytes_value()) {
        if (value.size() != sizeof(UUIDValueType)) {
            return Status::InvalidArgument("invalid UUID binary length: {}", value.size());
        }
    }

    auto& data = assert_cast<ColumnUUID&>(column).get_data();
    const auto old_size = column.size();
    data.resize(old_size + arg.bytes_value_size());
    for (int i = 0; i < arg.bytes_value_size(); ++i) {
        memcpy(&data[old_size + i], arg.bytes_value(i).data(), sizeof(UUIDValueType));
    }
    return Status::OK();
}

Status DataTypeUUIDSerDe::write_column_to_arrow(const IColumn& column, const NullMap* null_map,
                                                arrow::ArrayBuilder* array_builder, int64_t start,
                                                int64_t end, const cctz::time_zone& ctz) const {
    const auto& data = assert_cast<const ColumnUUID&>(column).get_data();
    auto& builder = assert_cast<arrow::FixedSizeBinaryBuilder&>(*array_builder);
    DORIS_CHECK_EQ(builder.byte_width(), UUIDValue::BINARY_LENGTH);
    for (int64_t i = start; i < end; ++i) {
        if (null_map && (*null_map)[i]) {
            RETURN_IF_ERROR(checkArrowStatus(builder.AppendNull(), column, *array_builder));
        } else {
            const auto uuid = UUIDValue::to_big_endian(data[i]);
            RETURN_IF_ERROR(checkArrowStatus(builder.Append(uuid.data()), column, *array_builder));
        }
    }
    return Status::OK();
}

Status DataTypeUUIDSerDe::read_column_from_arrow(IColumn& column, const arrow::Array* arrow_array,
                                                 int64_t start, int64_t end,
                                                 const cctz::time_zone& ctz) const {
    if (config::enable_arrow_input_validation) {
        arrow_validation_detail::check_arrow_length_and_offset(*arrow_array);
        if (UNLIKELY(start < 0 || end < start || end > arrow_array->length())) {
            arrow_validation_detail::throw_invalid_arrow(
                    *arrow_array, "read range is invalid: start={}, end={}, length={}", start, end,
                    arrow_array->length());
        }
    }
    if (arrow_array->type_id() == arrow::Type::EXTENSION) {
        if (!arrow_array->type()->Equals(arrow::extension::uuid())) {
            return Status::InvalidArgument("expected Arrow UUID, got {}",
                                           arrow_array->type()->ToString());
        }
        arrow_array = static_cast<const arrow::ExtensionArray*>(arrow_array)->storage().get();
    }
    if (!arrow_array->type()->Equals(arrow::fixed_size_binary(UUIDValue::BINARY_LENGTH))) {
        return Status::InvalidArgument("expected 16-byte Arrow UUID storage, got {}",
                                       arrow_array->type()->ToString());
    }
    if (config::enable_arrow_input_validation) {
        check_arrow_fixed_width_buffer(*arrow_array, UUIDValue::BINARY_LENGTH);
    }
    auto& data = assert_cast<ColumnUUID&>(column).get_data();
    const auto* array = assert_cast<const arrow::FixedSizeBinaryArray*>(arrow_array);

    for (auto i = start; i < end; ++i) {
        if (array->IsNull(i)) {
            data.emplace_back(0);
            continue;
        }
        data.emplace_back(UUIDValue::from_big_endian(array->GetValue(i)));
    }
    return Status::OK();
}

Status DataTypeUUIDSerDe::write_column_to_orc(const std::string& timezone, const IColumn& column,
                                              const NullMap* null_map,
                                              orc::ColumnVectorBatch* orc_col_batch, int64_t start,
                                              int64_t end, Arena& arena,
                                              const FormatOptions& options) const {
    const auto& data = assert_cast<const ColumnUUID&>(column).get_data();
    auto* batch = assert_cast<orc::StringVectorBatch*>(orc_col_batch);
    char* output = arena.alloc((end - start) * UUIDValue::BINARY_LENGTH);
    for (int64_t row_id = start; row_id < end; ++row_id) {
        if (batch->notNull[row_id] == 1) {
            const auto bytes = UUIDValue::to_big_endian(data[row_id]);
            memcpy(output, bytes.data(), bytes.size());
            batch->data[row_id] = output;
            batch->length[row_id] = bytes.size();
            output += bytes.size();
        }
    }
    batch->numElements = end - start;
    return Status::OK();
}

Status DataTypeUUIDSerDe::read_column_from_orc(IColumn& column,
                                               const OrcDecodedColumnView& view) const {
    DORIS_CHECK(view.file_type->getKind() == orc::BINARY);
    const auto& batch = static_cast<const orc::StringVectorBatch&>(*view.batch);
    auto& data = assert_cast<ColumnUUID&>(column).get_data();
    const auto rows = orc_serde_utils::orc_decode_row_count(view.rows, view.selected_rows);
    for (size_t row = 0; row < rows; ++row) {
        const auto source_row = orc_serde_utils::orc_source_row_at(row, view.selected_rows);
        if (orc_serde_utils::orc_row_is_null(batch, source_row)) {
            data.emplace_back(0);
            continue;
        }
        if (std::cmp_not_equal(batch.length[source_row], UUIDValue::BINARY_LENGTH)) {
            return Status::Corruption("invalid ORC UUID binary length: {}",
                                      batch.length[source_row]);
        }
        data.emplace_back(UUIDValue::from_big_endian(
                reinterpret_cast<const uint8_t*>(batch.data[source_row])));
    }
    return Status::OK();
}

Status DataTypeUUIDSerDe::from_string_batch(const ColumnString& str, ColumnNullable& column,
                                            const FormatOptions& options) const {
    const auto size = str.size();
    column.resize(size);
    auto& values = assert_cast<ColumnUUID&>(column.get_nested_column()).get_data();
    auto& null_map = column.get_null_map_data();
    for (size_t i = 0; i < size; ++i) {
        const auto input = str.get_data_at(i);
        null_map[i] = !UUIDValue::from_string(values[i], input.data, input.size);
    }
    return Status::OK();
}

Status DataTypeUUIDSerDe::from_string_strict_mode_batch(const ColumnString& str, IColumn& column,
                                                        const FormatOptions& options,
                                                        const NullMap::value_type* null_map) const {
    const auto size = str.size();
    column.resize(size);
    auto& values = assert_cast<ColumnUUID&>(column).get_data();
    for (size_t i = 0; i < size; ++i) {
        if (null_map && null_map[i]) {
            continue;
        }
        const auto input = str.get_data_at(i);
        if (!UUIDValue::from_string(values[i], input.data, input.size)) {
            return Status::InvalidArgument("parse uuid failed, string: '{}'", input.to_string());
        }
    }
    return Status::OK();
}

Status DataTypeUUIDSerDe::from_string(StringRef& str, IColumn& column,
                                      const FormatOptions& options) const {
    UUIDValueType value;
    if (!UUIDValue::from_string(value, str.data, str.size)) {
        return Status::InvalidArgument("parse uuid failed, string: '{}'", str.to_string());
    }
    assert_cast<ColumnUUID&>(column).insert_value(value);
    return Status::OK();
}

Status DataTypeUUIDSerDe::from_olap_string(const std::string& str, Field& field,
                                           const FormatOptions& options) const {
    UUIDValueType value;
    if (!UUIDValue::from_string(value, str)) {
        return Status::InvalidArgument("parse uuid failed, string: '{}'", str);
    }
    field = Field::create_field<TYPE_UUID>(value);
    return Status::OK();
}

Status DataTypeUUIDSerDe::from_string_strict_mode(StringRef& str, IColumn& column,
                                                  const FormatOptions& options) const {
    return from_string(str, column, options);
}

std::string DataTypeUUIDSerDe::to_olap_string(const Field& field) const {
    return UUIDValue::to_string(field.get<TYPE_UUID>());
}

} // namespace doris
