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

#include "core/data_type_serde/data_type_string_serde.h"

#include <algorithm>
#include <array>
#include <cstring>
#include <limits>

#include "common/config.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/define_primitive_type.h"
#include "core/data_type_serde/arrow_validation.h"
#include "core/data_type_serde/decoded_column_view.h"
#include "core/data_type_serde/parquet_decode_source.h"
#include "util/jsonb_document_cast.h"
#include "util/jsonb_utils.h"
#include "util/jsonb_writer.h"

namespace doris {
namespace {

template <typename ColumnType>
Status read_string_decoded_values(IColumn& column, const DecodedColumnView& view) {
    if (view.binary_values == nullptr && decoded_column_view_has_non_null_value(view)) {
        return Status::Corruption("Decoded binary values are null for {}", column.get_name());
    }
    auto& string_column = assert_cast<ColumnType&>(column);
    for (int64_t row = 0; row < view.row_count; ++row) {
        if (decoded_column_view_row_is_null(view, row)) {
            string_column.insert_default();
            continue;
        }
        const auto& value = (*view.binary_values)[row];
        if (value.data == nullptr && value.size > 0) {
            if (decoded_column_view_can_null_on_conversion_failure(view)) {
                decoded_column_view_insert_null_on_conversion_failure(column, view, row);
                continue;
            }
            return Status::Corruption("Decoded string binary value is null for {} at row {}",
                                      column.get_name(), row);
        }
        string_column.insert_data(value.data, value.size);
    }
    return Status::OK();
}

template <typename ColumnType>
class StringParquetConsumer final : public ParquetFixedValueConsumer,
                                    public ParquetBinaryValueConsumer {
public:
    explicit StringParquetConsumer(IColumn& column) : _column(assert_cast<ColumnType&>(column)) {}

    Status consume(const uint8_t* values, size_t num_values, size_t value_width) override {
        if constexpr (requires(ColumnType& column) {
                          column.insert_many_fixed_length_data(static_cast<const char*>(nullptr),
                                                               size_t(), size_t());
                      }) {
            // FIXED_LEN_BYTE_ARRAY is already a dense byte span. Copy it once and synthesize
            // offsets; StringRef batches add a second row loop and hundreds of tiny memcpy calls.
            _column.insert_many_fixed_length_data(reinterpret_cast<const char*>(values),
                                                  value_width, num_values);
        } else {
            static constexpr size_t BATCH_SIZE = 256;
            std::array<StringRef, BATCH_SIZE> refs;
            size_t offset = 0;
            while (offset < num_values) {
                const size_t batch_size = std::min(BATCH_SIZE, num_values - offset);
                for (size_t row = 0; row < batch_size; ++row) {
                    refs[row] = StringRef(
                            reinterpret_cast<const char*>(values + (offset + row) * value_width),
                            value_width);
                }
                _column.insert_many_strings(refs.data(), batch_size);
                offset += batch_size;
            }
        }
        return Status::OK();
    }

    Status consume(const StringRef* values, size_t num_values) override {
        _column.insert_many_strings(values, num_values);
        return Status::OK();
    }

    Status consume_plain_byte_array(
            const char* encoded_data, const uint32_t* payload_offsets,
            const uint32_t* value_offsets, size_t num_values,
            const std::vector<ParquetSelectionRange>& value_spans) override {
        if constexpr (requires(ColumnType& column) {
                          column.insert_many_parquet_plain_byte_arrays(
                                  encoded_data, payload_offsets, value_offsets, num_values,
                                  value_spans);
                      }) {
            _column.insert_many_parquet_plain_byte_arrays(encoded_data, payload_offsets,
                                                          value_offsets, num_values, value_spans);
            return Status::OK();
        }
        return ParquetBinaryValueConsumer::consume_plain_byte_array(
                encoded_data, payload_offsets, value_offsets, num_values, value_spans);
    }

private:
    ColumnType& _column;
};

} // namespace

namespace {

int hex_value(char c) {
    if (c >= '0' && c <= '9') {
        return c - '0';
    }
    if (c >= 'a' && c <= 'f') {
        return c - 'a' + 10;
    }
    if (c >= 'A' && c <= 'F') {
        return c - 'A' + 10;
    }
    return -1;
}

Status parse_uuid_to_bytes(StringRef uuid, std::array<uint8_t, 16>* bytes) {
    if (uuid.size != 32 && uuid.size != 36) {
        return Status::InvalidArgument("Invalid UUID string length: {}", uuid.size);
    }

    int hex_count = 0;
    int high_nibble = -1;
    int byte_index = 0;
    for (size_t i = 0; i < uuid.size; ++i) {
        char c = uuid.data[i];
        if (uuid.size == 36 && (i == 8 || i == 13 || i == 18 || i == 23)) {
            if (c != '-') {
                return Status::InvalidArgument("Invalid UUID string format");
            }
            continue;
        }
        if (c == '-') {
            return Status::InvalidArgument("Invalid UUID string format");
        }

        int value = hex_value(c);
        if (value < 0) {
            return Status::InvalidArgument("Invalid UUID string format");
        }
        if (hex_count % 2 == 0) {
            high_nibble = value;
        } else {
            (*bytes)[byte_index++] = static_cast<uint8_t>((high_nibble << 4) | value);
        }
        ++hex_count;
    }

    if (hex_count != 32 || byte_index != 16) {
        return Status::InvalidArgument("Invalid UUID string format");
    }
    return Status::OK();
}

Status append_fixed_size_binary(arrow::FixedSizeBinaryBuilder& builder, const IColumn& column,
                                StringRef string_ref, int byte_width, bool pad_char_value,
                                bool convert_uuid_string) {
    if (convert_uuid_string && byte_width == 16 &&
        (string_ref.size == 32 || string_ref.size == 36)) {
        std::array<uint8_t, 16> bytes;
        RETURN_IF_ERROR(parse_uuid_to_bytes(string_ref, &bytes));
        return checkArrowStatus(builder.Append(bytes.data()), column, builder);
    }

    if (string_ref.size == byte_width) {
        return checkArrowStatus(builder.Append(reinterpret_cast<const uint8_t*>(string_ref.data)),
                                column, builder);
    }

    if (pad_char_value && string_ref.size < byte_width) {
        std::string padded_value(byte_width, '\0');
        std::memcpy(padded_value.data(), string_ref.data, string_ref.size);
        return checkArrowStatus(
                builder.Append(reinterpret_cast<const uint8_t*>(padded_value.data())), column,
                builder);
    }

    return Status::InvalidArgument("Fixed size binary column expects {} bytes, got {}", byte_width,
                                   string_ref.size);
}

} // namespace

template <typename ColumnType>
Status DataTypeStringSerDeBase<ColumnType>::serialize_column_to_json(const IColumn& column,
                                                                     int64_t start_idx,
                                                                     int64_t end_idx,
                                                                     BufferWritable& bw,
                                                                     FormatOptions& options) const {
    SERIALIZE_COLUMN_TO_JSON();
}

template <typename ColumnType>
Status DataTypeStringSerDeBase<ColumnType>::serialize_one_cell_to_json(
        const IColumn& column, int64_t row_num, BufferWritable& bw, FormatOptions& options) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;
    const auto& value = assert_cast<const ColumnType&>(*ptr).get_data_at(row_num);

    if (_nesting_level > 1) {
        bw.write('"');
    }
    if constexpr (std::is_same_v<ColumnType, ColumnString>) {
        if (options.escape_char != 0) {
            // we should make deal with some special characters in json str if we have escape_char
            StringRef str_ref = value;
            write_with_escaped_char_to_json(str_ref, bw);
        } else {
            bw.write(value.data, value.size);
        }
    } else {
        bw.write(value.data, value.size);
    }
    if (_nesting_level > 1) {
        bw.write('"');
    }

    return Status::OK();
}

template <typename ColumnType>
Status DataTypeStringSerDeBase<ColumnType>::serialize_one_cell_to_hive_text(
        const IColumn& column, int64_t row_num, BufferWritable& bw, FormatOptions& options,
        int hive_text_complex_type_delimiter_level) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;
    const auto& value = assert_cast<const ColumnType&>(*ptr).get_data_at(row_num);
    if constexpr (std::is_same_v<ColumnType, ColumnString>) {
        if (options.escape_char != 0) {
            StringRef str_ref = value;
            write_with_escaped_char_to_hive_text(str_ref, bw, options.escape_char,
                                                 options.need_escape);
        } else {
            bw.write(value.data, value.size);
        }
    } else {
        bw.write(value.data, value.size);
    }
    return Status::OK();
}

template <typename ColumnType>
Status DataTypeStringSerDeBase<ColumnType>::deserialize_one_cell_from_json(
        IColumn& column, Slice& slice, const FormatOptions& options) const {
    /*
         * For strings in the json complex type, we remove double quotes by default.
         *
         * Because when querying complex types, such as selecting complexColumn from table,
         * we will add double quotes to the strings in the complex type.
         *
         * For the map<string,int> column, insert { "abc" : 1, "hello",2 }.
         * If you do not remove the double quotes, it will display {""abc"":1,""hello"": 2 },
         * remove the double quotes to display { "abc" : 1, "hello",2 }.
         *
         */
    if (_nesting_level >= 2) {
        slice.trim_quote();
    }
    if (options.escape_char != 0) {
        escape_string(slice.data, &slice.size, options.escape_char);
    }
    assert_cast<ColumnType&>(column).insert_data(slice.data, slice.size);
    return Status::OK();
}

template <typename ColumnType>
Status DataTypeStringSerDeBase<ColumnType>::deserialize_one_cell_from_csv(
        IColumn& column, Slice& slice, const FormatOptions& options) const {
    if (options.escape_char != 0) {
        escape_string_for_csv(slice.data, &slice.size, options.escape_char, options.quote_char);
    }
    assert_cast<ColumnType&>(column).insert_data(slice.data, slice.size);
    return Status::OK();
}

template <typename ColumnType>
Status DataTypeStringSerDeBase<ColumnType>::deserialize_one_cell_from_hive_text(
        IColumn& column, Slice& slice, const FormatOptions& options,
        int hive_text_complex_type_delimiter_level) const {
    if (options.escape_char != 0) {
        escape_string(slice.data, &slice.size, options.escape_char);
    }
    assert_cast<ColumnType&>(column).insert_data(slice.data, slice.size);
    return Status::OK();
}

template <typename ColumnType>
Status DataTypeStringSerDeBase<ColumnType>::deserialize_column_from_json_vector(
        IColumn& column, std::vector<Slice>& slices, uint64_t* num_deserialized,
        const FormatOptions& options) const {
    DESERIALIZE_COLUMN_FROM_JSON_VECTOR()
    return Status::OK();
}

template <typename ColumnType>
Status DataTypeStringSerDeBase<ColumnType>::write_column_to_pb(const IColumn& column,
                                                               PValues& result, int64_t start,
                                                               int64_t end) const {
    result.mutable_string_value()->Reserve(cast_set<int>(end - start));
    auto* ptype = result.mutable_type();
    ptype->set_id(PGenericType::STRING);
    for (size_t row_num = start; row_num < end; ++row_num) {
        StringRef data = column.get_data_at(row_num);
        result.add_string_value(data.to_string());
    }
    return Status::OK();
}

template <typename ColumnType>
Status DataTypeStringSerDeBase<ColumnType>::deserialize_column_from_fixed_json(
        IColumn& column, Slice& slice, uint64_t rows, uint64_t* num_deserialized,
        const FormatOptions& options) const {
    if (rows < 1) [[unlikely]] {
        return Status::OK();
    }
    Status st = deserialize_one_cell_from_json(column, slice, options);
    if (!st.ok()) {
        return st;
    }

    DataTypeStringSerDeBase::insert_column_last_value_multiple_times(column, rows - 1);
    *num_deserialized = rows;
    return Status::OK();
}

template <typename ColumnType>
void DataTypeStringSerDeBase<ColumnType>::insert_column_last_value_multiple_times(
        IColumn& column, uint64_t times) const {
    if (times < 1) [[unlikely]] {
        return;
    }
    auto& col = static_cast<ColumnString&>(column);
    auto sz = col.size();

    StringRef ref = col.get_data_at(sz - 1);
    String str(ref.data, ref.size);
    std::vector<StringRef> refs(times, {str.data(), str.size()});

    col.insert_many_strings(refs.data(), refs.size());
}

template <typename ColumnType>
Status DataTypeStringSerDeBase<ColumnType>::read_column_from_pb(IColumn& column,
                                                                const PValues& arg) const {
    auto& column_dest = assert_cast<ColumnType&>(column);
    column_dest.reserve(column_dest.size() + arg.string_value_size());
    for (int i = 0; i < arg.string_value_size(); ++i) {
        column_dest.insert_data(arg.string_value(i).c_str(), arg.string_value(i).size());
    }
    return Status::OK();
}

template <typename ColumnType>
void DataTypeStringSerDeBase<ColumnType>::write_one_cell_to_jsonb(
        const IColumn& column, JsonbWriter& result, Arena& mem_pool, int32_t col_id,
        int64_t row_num, const FormatOptions& options) const {
    result.writeKey(cast_set<JsonbKeyValue::keyid_type>(col_id));
    const auto& data_ref = column.get_data_at(row_num);
    result.writeStartBinary();
    result.writeBinary(reinterpret_cast<const char*>(data_ref.data), data_ref.size);
    result.writeEndBinary();
}

template <typename ColumnType>
void DataTypeStringSerDeBase<ColumnType>::read_one_cell_from_jsonb(IColumn& column,
                                                                   const JsonbValue* arg) const {
    assert(arg->isBinary());
    const auto* blob = arg->unpack<JsonbBinaryVal>();
    assert_cast<ColumnType&>(column).insert_data(blob->getBlob(), blob->getBlobLen());
}

template <typename ColumnType>
template <typename BuilderType>
Status DataTypeStringSerDeBase<ColumnType>::write_column_to_arrow_impl(const IColumn& column,
                                                                       const NullMap* null_map,
                                                                       BuilderType& builder,
                                                                       int64_t start,
                                                                       int64_t end) const {
    const auto& string_column = assert_cast<const ColumnType&>(column);
    for (size_t string_i = start; string_i < end; ++string_i) {
        if (null_map && (*null_map)[string_i]) {
            RETURN_IF_ERROR(checkArrowStatus(builder.AppendNull(), column, builder));
            continue;
        }
        auto string_ref = string_column.get_data_at(string_i);
        RETURN_IF_ERROR(checkArrowStatus(
                builder.Append(string_ref.data, cast_set<int, size_t, false>(string_ref.size)),
                column, builder));
    }
    return Status::OK();
}

template <typename ColumnType>
Status DataTypeStringSerDeBase<ColumnType>::write_column_to_arrow(
        const IColumn& column, const NullMap* null_map, arrow::ArrayBuilder* array_builder,
        int64_t start, int64_t end, const cctz::time_zone& ctz) const {
    if (array_builder->type()->id() == arrow::Type::LARGE_STRING) {
        auto& builder = assert_cast<arrow::LargeStringBuilder&>(*array_builder);
        return write_column_to_arrow_impl(column, null_map, builder, start, end);
    } else if (array_builder->type()->id() == arrow::Type::LARGE_BINARY) {
        auto& builder = assert_cast<arrow::LargeBinaryBuilder&>(*array_builder);
        const auto& string_column = assert_cast<const ColumnType&>(column);
        for (size_t string_i = start; string_i < end; ++string_i) {
            if (null_map && (*null_map)[string_i]) {
                RETURN_IF_ERROR(checkArrowStatus(builder.AppendNull(), column, builder));
                continue;
            }
            auto string_ref = string_column.get_data_at(string_i);
            RETURN_IF_ERROR(checkArrowStatus(
                    builder.Append(reinterpret_cast<const uint8_t*>(string_ref.data),
                                   cast_set<int64_t, size_t, false>(string_ref.size)),
                    column, builder));
        }
        return Status::OK();
    } else if (array_builder->type()->id() == arrow::Type::STRING) {
        auto& builder = assert_cast<arrow::StringBuilder&>(*array_builder);
        return write_column_to_arrow_impl(column, null_map, builder, start, end);
    } else if (array_builder->type()->id() == arrow::Type::BINARY) {
        auto& builder = assert_cast<arrow::BinaryBuilder&>(*array_builder);
        return write_column_to_arrow_impl(column, null_map, builder, start, end);
    } else if (array_builder->type()->id() == arrow::Type::FIXED_SIZE_BINARY) {
        auto& builder = assert_cast<arrow::FixedSizeBinaryBuilder&>(*array_builder);
        const int byte_width =
                static_cast<const arrow::FixedSizeBinaryType&>(*array_builder->type()).byte_width();
        const auto& string_column = assert_cast<const ColumnType&>(column);
        for (size_t string_i = start; string_i < end; ++string_i) {
            if (null_map && (*null_map)[string_i]) {
                RETURN_IF_ERROR(checkArrowStatus(builder.AppendNull(), column, builder));
                continue;
            }
            auto string_ref = string_column.get_data_at(string_i);
            RETURN_IF_ERROR(append_fixed_size_binary(builder, column, string_ref, byte_width,
                                                     _type == TYPE_CHAR, _type != TYPE_CHAR));
        }
        return Status::OK();
    } else {
        return Status::InvalidArgument("Unsupported arrow type for string column: {}",
                                       array_builder->type()->name());
    }
}

template <typename ColumnType>
Status DataTypeStringSerDeBase<ColumnType>::read_column_from_arrow(
        IColumn& column, const arrow::Array* arrow_array, int64_t start, int64_t end,
        const cctz::time_zone& ctz) const {
    if (arrow_array->type_id() == arrow::Type::STRING ||
        arrow_array->type_id() == arrow::Type::BINARY) {
        const auto* concrete_array = dynamic_cast<const arrow::BinaryArray*>(arrow_array);
        if (config::enable_arrow_input_validation) {
            check_arrow_array_range(*concrete_array, start, end);
            check_arrow_binary_offsets_buffer(*concrete_array);
        }
        std::shared_ptr<arrow::Buffer> buffer = concrete_array->value_data();
        const size_t buffer_size = buffer ? static_cast<size_t>(buffer->size()) : 0;
        const uint8_t* offsets_data = concrete_array->value_offsets()->data();
        const size_t offset_size = sizeof(int32_t);

        for (auto offset_i = start; offset_i < end; ++offset_i) {
            if (!concrete_array->IsNull(offset_i)) {
                int32_t start_offset = 0;
                int32_t end_offset = 0;
                memcpy(&start_offset, offsets_data + offset_i * offset_size, offset_size);
                memcpy(&end_offset, offsets_data + (offset_i + 1) * offset_size, offset_size);

                int32_t length = end_offset - start_offset;
                if (config::enable_arrow_input_validation) {
                    check_arrow_value_range(*concrete_array, start_offset, length, buffer_size);
                }
                // insert_data() does not read the input pointer when length is zero.
                const auto* raw_data = reinterpret_cast<const char*>(buffer->data() + start_offset);
                assert_cast<ColumnType&>(column).insert_data(raw_data, length);
            } else {
                assert_cast<ColumnType&>(column).insert_default();
            }
        }
    } else if (arrow_array->type_id() == arrow::Type::FIXED_SIZE_BINARY) {
        const auto* concrete_array = dynamic_cast<const arrow::FixedSizeBinaryArray*>(arrow_array);
        if (config::enable_arrow_input_validation) {
            check_arrow_array_range(*concrete_array, start, end);
            check_arrow_fixed_width_buffer(*concrete_array,
                                           static_cast<size_t>(concrete_array->byte_width()));
        }
        uint32_t width = concrete_array->byte_width();

        for (auto offset_i = start; offset_i < end; ++offset_i) {
            if (!concrete_array->IsNull(offset_i)) {
                const auto* raw_data = concrete_array->GetValue(offset_i);
                assert_cast<ColumnType&>(column).insert_data((char*)raw_data, width);
            } else {
                assert_cast<ColumnType&>(column).insert_default();
            }
        }
    } else if (arrow_array->type_id() == arrow::Type::LARGE_STRING ||
               arrow_array->type_id() == arrow::Type::LARGE_BINARY) {
        const auto* concrete_array = dynamic_cast<const arrow::LargeBinaryArray*>(arrow_array);
        if (config::enable_arrow_input_validation) {
            check_arrow_array_range(*concrete_array, start, end);
            check_arrow_binary_offsets_buffer(*concrete_array);
        }
        std::shared_ptr<arrow::Buffer> buffer = concrete_array->value_data();
        const size_t buffer_size = buffer ? static_cast<size_t>(buffer->size()) : 0;

        for (auto offset_i = start; offset_i < end; ++offset_i) {
            if (!concrete_array->IsNull(offset_i)) {
                const auto value_offset = concrete_array->value_offset(offset_i);
                const auto value_length = concrete_array->value_length(offset_i);
                if (config::enable_arrow_input_validation) {
                    check_arrow_value_range(*concrete_array, value_offset, value_length,
                                            buffer_size);
                }
                // insert_data() does not read the input pointer when length is zero.
                const auto* raw_data = reinterpret_cast<const char*>(buffer->data() + value_offset);
                assert_cast<ColumnType&>(column).insert_data(raw_data, value_length);
            } else {
                assert_cast<ColumnType&>(column).insert_default();
            }
        }
    } else {
        return Status::InvalidArgument("Unsupported arrow type for string column: {}",
                                       arrow_array->type_id());
    }
    return Status::OK();
}

template <typename ColumnType>
Status DataTypeStringSerDeBase<ColumnType>::read_column_from_decoded_values(
        IColumn& column, const DecodedColumnView& view) const {
    if (view.value_kind != DecodedValueKind::BINARY &&
        view.value_kind != DecodedValueKind::FIXED_BINARY) {
        return decoded_column_view_handle_conversion_failure(
                column, view,
                Status::NotSupported("Unsupported decoded values for {} from source kind {}",
                                     get_name(), static_cast<int>(view.value_kind)));
    }
    return read_string_decoded_values<ColumnType>(column, view);
}

template <typename ColumnType>
Status DataTypeStringSerDeBase<ColumnType>::read_parquet_dictionary(
        IColumn& column, ParquetDecodeSource& source, const ParquetDecodeContext& context) const {
    StringParquetConsumer<ColumnType> consumer(column);
    return source.decode_dictionary(consumer, consumer);
}

template <typename ColumnType>
Status DataTypeStringSerDeBase<ColumnType>::read_column_from_parquet(
        IColumn& column, ParquetDecodeSource& source, const ParquetDecodeContext& context,
        size_t num_values, ParquetMaterializationState& state) const {
    if (context.dictionary_index_only) {
        if (context.encoding != ParquetValueEncoding::DICTIONARY) {
            return Status::IOError("Dictionary filter requested for a non-dictionary page");
        }
        RETURN_IF_ERROR(source.decode_dictionary_indices(num_values, &state.dictionary_indices));
        auto& indices = assert_cast<ColumnInt32&>(column).get_data();
        const size_t old_size = indices.size();
        indices.resize(old_size + num_values);
        for (size_t row = 0; row < num_values; ++row) {
            if (UNLIKELY(state.dictionary_indices[row] >
                         static_cast<uint32_t>(std::numeric_limits<int32_t>::max()))) {
                indices.resize(old_size);
                return Status::Corruption("Parquet dictionary index {} exceeds INT32",
                                          state.dictionary_indices[row]);
            }
            indices[old_size + row] = static_cast<int32_t>(state.dictionary_indices[row]);
        }
        return Status::OK();
    }
    StringParquetConsumer<ColumnType> consumer(column);
    if (context.encoding != ParquetValueEncoding::DICTIONARY) {
        if (context.physical_type == ParquetPhysicalType::BYTE_ARRAY) {
            return source.decode_binary_values(num_values, consumer);
        }
        if (context.physical_type == ParquetPhysicalType::FIXED_LEN_BYTE_ARRAY) {
            return source.decode_fixed_values(num_values, consumer);
        }
        return Status::NotSupported("Unsupported Parquet physical type {} for string SerDe",
                                    static_cast<int>(context.physical_type));
    }

    if (state.dictionary_generation != source.dictionary_generation()) {
        state.typed_dictionary = column.clone_empty();
        RETURN_IF_ERROR(read_parquet_dictionary(*state.typed_dictionary, source, context));
        DORIS_CHECK_EQ(state.typed_dictionary->size(), source.dictionary_size());
        state.dictionary_generation = source.dictionary_generation();
    }
    return state.materialize_dictionary(column, source, num_values);
}

template <typename ColumnType>
Status DataTypeStringSerDeBase<ColumnType>::write_column_to_orc(
        const std::string& timezone, const IColumn& column, const NullMap* null_map,
        orc::ColumnVectorBatch* orc_col_batch, int64_t start, int64_t end, Arena& arena,
        const FormatOptions& options) const {
    auto* cur_batch = dynamic_cast<orc::StringVectorBatch*>(orc_col_batch);

    for (auto row_id = start; row_id < end; row_id++) {
        const auto& ele = assert_cast<const ColumnType&>(column).get_data_at(row_id);
        cur_batch->data[row_id] = const_cast<char*>(ele.data);
        cur_batch->length[row_id] = ele.size;
    }

    cur_batch->numElements = end - start;
    return Status::OK();
}

template <typename ColumnType>
Status DataTypeStringSerDeBase<ColumnType>::serialize_column_to_jsonb(const IColumn& from_column,
                                                                      int64_t row_num,
                                                                      JsonbWriter& writer) const {
    if constexpr (!std::is_same_v<ColumnType, ColumnString>) {
        return Status::NotSupported(
                "DataTypeStringSerDeBase only supports ColumnString for serialize_column_to_jsonb");
    }
    const auto& data = assert_cast<const ColumnString&>(from_column).get_data_at(row_num);

    // start writing string
    if (!writer.writeStartString()) {
        return Status::InternalError("writeStartString failed");
    }

    // write string
    if (data.size > 0) {
        if (writer.writeString(data.data, data.size) == 0) {
            return Status::InternalError("writeString failed");
        }
    }

    // end writing string
    if (!writer.writeEndString()) {
        return Status::InternalError("writeEndString failed");
    }

    return Status::OK();
}

template <typename ColumnType>
Status DataTypeStringSerDeBase<ColumnType>::deserialize_column_from_jsonb(
        IColumn& column, const JsonbValue* jsonb_value, CastParameters& castParms) const {
    if constexpr (!std::is_same_v<ColumnType, ColumnString>) {
        return Status::NotSupported(
                "DataTypeStringSerDeBase only supports ColumnString for "
                "deserialize_column_from_jsonb");
    }
    auto& col_str = assert_cast<ColumnString&>(column);
    std::string str;
    if (jsonb_value->isString()) {
        const auto* blob = jsonb_value->unpack<JsonbBinaryVal>();
        str.assign(blob->getBlob(), blob->getBlobLen());
    } else {
        str = JsonbToJson {}.to_json_string(jsonb_value);
    }
    col_str.insert_value(str);

    return Status::OK();
}

template <typename ColumnType>
Status DataTypeStringSerDeBase<ColumnType>::deserialize_column_from_jsonb_vector(
        ColumnNullable& column_to, const ColumnString& col_from_json,
        CastParameters& castParms) const {
    if constexpr (!std::is_same_v<ColumnType, ColumnString>) {
        return Status::NotSupported(
                "DataTypeStringSerDeBase only supports ColumnString for "
                "deserialize_column_from_jsonb_vector");
    }

    const size_t size = col_from_json.size();

    auto& null_map = column_to.get_null_map_data();
    auto& col_str = assert_cast<ColumnString&>(column_to.get_nested_column());

    null_map.resize_fill(size, false);
    col_str.get_offsets().reserve(size);
    col_str.get_chars().reserve(col_from_json.get_chars().size());

    for (size_t i = 0; i < size; ++i) {
        const auto& val = col_from_json.get_data_at(i);
        auto* jsonb_value = handle_jsonb_value(val);
        if (!jsonb_value) {
            null_map[i] = true;
            col_str.insert_default();
            continue;
        }
        if (jsonb_value->isString()) {
            const auto* blob = jsonb_value->unpack<JsonbBinaryVal>();
            col_str.insert_data(blob->getBlob(), blob->getBlobLen());
        } else {
            col_str.insert_value(JsonbToJson {}.to_json_string(jsonb_value));
        }
    }
    return Status::OK();
}

template <typename ColumnType>
void DataTypeStringSerDeBase<ColumnType>::to_string(const IColumn& column, size_t row_num,
                                                    BufferWritable& bw,
                                                    const FormatOptions& options) const {
    if (_nesting_level > 1) {
        bw.write('"');
    }
    const auto& value =
            assert_cast<const ColumnType&, TypeCheckOnRelease::DISABLE>(column).get_data_at(
                    row_num);
    bw.write(value.data, value.size);
    if (_nesting_level > 1) {
        bw.write('"');
    }
}

// Serializes a STRING/VARCHAR/CHAR value to its OLAP string representation for ZoneMap storage.
// This is the inverse of from_olap_string(). Returns the raw string content directly.
template <typename ColumnType>
std::string DataTypeStringSerDeBase<ColumnType>::to_olap_string(const Field& field) const {
    return field.get<TYPE_STRING>();
}

template <typename ColumnType>
bool DataTypeStringSerDeBase<ColumnType>::write_column_to_presto_text(
        const IColumn& column, BufferWritable& bw, int64_t row_idx,
        const FormatOptions& options) const {
    const auto& value =
            assert_cast<const ColumnType&, TypeCheckOnRelease::DISABLE>(column).get_data_at(
                    row_idx);
    bw.write(value.data, value.size);
    return true;
}

template <typename ColumnType>
Status DataTypeStringSerDeBase<ColumnType>::from_string(StringRef& str, IColumn& column,
                                                        const FormatOptions& options) const {
    auto slice = str.to_slice();
    return deserialize_one_cell_from_json(column, slice, options);
}

// Deserializes a STRING/VARCHAR/CHAR value from its OLAP string representation
// (e.g. from ZoneMap protobuf). This is the inverse of to_olap_string().
template <typename ColumnType>
Status DataTypeStringSerDeBase<ColumnType>::from_olap_string(const std::string& str, Field& field,
                                                             const FormatOptions& options) const {
    // CHAR(N) writes through OlapColumnDataConvertorChar are zero-padded to
    // the declared schema length, so the serialized OLAP string carries
    // trailing '\0' bytes. strnlen() drops that padding to surface the
    // logical character content in the Field. VARCHAR / STRING never write
    // trailing '\0' through this path, so strnlen is a no-op for them.
    size_t len = strnlen(str.data(), str.size());
    field = Field::create_field<TYPE_STRING>(std::string(str.data(), len));
    return Status::OK();
}

template class DataTypeStringSerDeBase<ColumnString>;
template class DataTypeStringSerDeBase<ColumnString64>;
template class DataTypeStringSerDeBase<ColumnFixedLengthObject>;

} // namespace doris
