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

#include "core/data_type_serde/data_type_variant_serde.h"

#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_nested.h>

#include <cstdint>
#include <string>

#include "common/cast_set.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/column/column.h"
#include "core/column/column_variant.h"
#include "core/data_type_serde/data_type_serde.h"
#include "core/field.h"
#include "core/string_ref.h"
#include "core/types.h"
#include "core/value/jsonb_value.h"
#include "exec/common/variant_util.h"
#include "exprs/function/parse/variant_jsonb_parse.h"
#include "util/json/json_parser.h"
#include "util/jsonb_writer.h"

namespace doris {
namespace {

template <typename BuilderType>
Status write_variant_column_to_arrow_impl(const IColumn& column, const ColumnVariant& var,
                                          const NullMap* null_map, BuilderType& builder,
                                          int64_t start, int64_t end, const cctz::time_zone& ctz) {
    DataTypeSerDe::FormatOptions options;
    options.timezone = &ctz;
    for (int64_t i = start; i < end; ++i) {
        if (null_map && (*null_map)[cast_set<size_t>(i)]) {
            RETURN_IF_ERROR(checkArrowStatus(builder.AppendNull(), column, builder));
            continue;
        }

        std::string serialized_value;
        var.serialize_one_row_to_string(i, &serialized_value, options);
        const auto serialized_size =
                cast_set<typename BuilderType::offset_type>(serialized_value.size());
        RETURN_IF_ERROR(checkArrowStatus(builder.Append(serialized_value.data(), serialized_size),
                                         column, builder));
    }
    return Status::OK();
}

Status write_variant_column_to_arrow_struct(const IColumn& column, const ColumnVariant& var,
                                            const NullMap* null_map, arrow::StructBuilder& builder,
                                            int64_t start, int64_t end,
                                            const cctz::time_zone& ctz) {
    const auto struct_type = std::dynamic_pointer_cast<arrow::StructType>(builder.type());
    if (struct_type == nullptr || builder.num_fields() != 2 ||
        struct_type->field(0)->name() != "value" || struct_type->field(1)->name() != "metadata") {
        return Status::InvalidArgument(
                "Variant Arrow output requires "
                "struct<value: binary, metadata: binary>");
    }
    auto* value_builder = dynamic_cast<arrow::BinaryBuilder*>(builder.field_builder(0));
    auto* metadata_builder = dynamic_cast<arrow::BinaryBuilder*>(builder.field_builder(1));
    if (value_builder == nullptr || metadata_builder == nullptr) {
        return Status::InvalidArgument(
                "Variant Arrow output requires binary value and metadata children");
    }

    const auto* root = var.get_subcolumn(PathInData());
    const bool string_root =
            root != nullptr && is_string_type(root->get_least_common_base_type_id());
    JsonbToVariantEncoder encoder(
            VariantBatchBuilder::ReserveHint {.rows = cast_set<size_t>(end - start)});
    DataTypeSerDe::FormatOptions options;
    options.timezone = &ctz;
    for (int64_t row = start; row < end; ++row) {
        if (null_map != nullptr && (*null_map)[cast_set<size_t>(row)]) {
            encoder.add_null();
            continue;
        }

        std::string serialized_value;
        var.serialize_one_row_to_string(row, &serialized_value, options);
        if (string_root && !root->is_null_at(cast_set<size_t>(row))) {
            JsonbWriter writer;
            if (!writer.writeStartString() ||
                (serialized_value.size() != 0 &&
                 !writer.writeString(serialized_value.data(), serialized_value.size())) ||
                !writer.writeEndString()) {
                return Status::InternalError("Failed to encode legacy Variant string as JSONB");
            }
            encoder.add_jsonb({writer.getOutput()->getBuffer(),
                               static_cast<size_t>(writer.getOutput()->getSize())});
            continue;
        }

        JsonBinaryValue jsonb;
        RETURN_IF_ERROR(jsonb.from_json_string(serialized_value));
        encoder.add_jsonb({jsonb.value(), jsonb.size()});
    }

    VariantBatchBuilder batch = encoder.finish_batch();
    for (size_t row = 0; row < batch.num_rows(); ++row) {
        const size_t source_row = cast_set<size_t>(start) + row;
        if (null_map != nullptr && (*null_map)[source_row]) {
            RETURN_IF_ERROR(checkArrowStatus(builder.AppendNull(), column, builder));
            continue;
        }
        const VariantRef value = batch.value_at(row);
        RETURN_IF_ERROR(checkArrowStatus(builder.Append(), column, builder));
        RETURN_IF_ERROR(checkArrowStatus(
                value_builder->Append(value.value.data, cast_set<int32_t>(value.value.size)),
                column, *value_builder));
        RETURN_IF_ERROR(
                checkArrowStatus(metadata_builder->Append(value.metadata.data,
                                                          cast_set<int32_t>(value.metadata.size)),
                                 column, *metadata_builder));
    }
    return Status::OK();
}

} // namespace

Status DataTypeVariantSerDe::write_column_to_mysql_binary(const IColumn& column,
                                                          MysqlRowBinaryBuffer& row_buffer,
                                                          int64_t row_idx, bool col_const,
                                                          const FormatOptions& options) const {
    const auto& variant = assert_cast<const ColumnVariant&>(column);
    // Serialize hierarchy types to json format
    std::string buffer;
    variant.serialize_one_row_to_string(row_idx, &buffer, options);
    row_buffer.push_string(buffer.data(), buffer.size());
    return Status::OK();
}

Status DataTypeVariantSerDe::serialize_column_to_json(const IColumn& column, int64_t start_idx,
                                                      int64_t end_idx, BufferWritable& bw,
                                                      FormatOptions& options) const {
    SERIALIZE_COLUMN_TO_JSON();
}

void DataTypeVariantSerDe::write_one_cell_to_jsonb(const IColumn& column, JsonbWriter& result,
                                                   Arena& mem_pool, int32_t col_id, int64_t row_num,
                                                   const FormatOptions& options) const {
    const auto& variant = assert_cast<const ColumnVariant&>(column);
    result.writeKey(cast_set<JsonbKeyValue::keyid_type>(col_id));
    std::string value_str;
    variant.serialize_one_row_to_string(row_num, &value_str, options);
    JsonBinaryValue jsonb_value;
    // encode as jsonb
    bool succ = jsonb_value.from_json_string(value_str.data(), value_str.size()).ok();
    if (!succ) {
        // not a valid json insert raw text
        result.writeStartString();
        result.writeString(value_str.data(), value_str.size());
        result.writeEndString();
    } else {
        // write a json binary
        result.writeStartBinary();
        result.writeBinary(jsonb_value.value(), jsonb_value.size());
        result.writeEndBinary();
    }
}

void DataTypeVariantSerDe::read_one_cell_from_jsonb(IColumn& column, const JsonbValue* arg) const {
    auto& variant = assert_cast<ColumnVariant&>(column);
    Field field;
    if (arg->isBinary()) {
        const auto* blob = arg->unpack<JsonbBinaryVal>();
        field = Field::create_field<TYPE_JSONB>(JsonbField(blob->getBlob(), blob->getBlobLen()));
    } else if (arg->isString()) {
        // not a valid jsonb type, insert as string
        const auto* str = arg->unpack<JsonbStringVal>();
        field = Field::create_field<TYPE_STRING>(String(str->getBlob(), str->getBlobLen()));
    } else {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "Invalid jsonb type");
    }
    VariantMap object;
    object.try_emplace(PathInData(), FieldWithDataType(field));
    field = Field::create_field<TYPE_VARIANT>(std::move(object));
    variant.insert(field);
}

Status DataTypeVariantSerDe::serialize_one_cell_to_json(const IColumn& column, int64_t row_num,
                                                        BufferWritable& bw,
                                                        FormatOptions& options) const {
    const auto* var = assert_cast<const ColumnVariant*>(&column);
    var->serialize_one_row_to_string(row_num, bw, options);
    return Status::OK();
}

Status DataTypeVariantSerDe::deserialize_one_cell_from_json(IColumn& column, Slice& slice,
                                                            const FormatOptions& options) const {
    ParseConfig parse_config;
    parse_config.check_duplicate_json_path = config::variant_enable_duplicate_json_path_check;
    StringRef json_ref(slice.data, slice.size);
    RETURN_IF_CATCH_EXCEPTION(
            variant_util::parse_json_to_variant(column, json_ref, nullptr, parse_config));
    return Status::OK();
}

Status DataTypeVariantSerDe::deserialize_column_from_json_vector(
        IColumn& column, std::vector<Slice>& slices, uint64_t* num_deserialized,
        const FormatOptions& options) const {
    DESERIALIZE_COLUMN_FROM_JSON_VECTOR()
    return Status::OK();
}

Status DataTypeVariantSerDe::write_column_to_arrow(const IColumn& column, const NullMap* null_map,
                                                   arrow::ArrayBuilder* array_builder,
                                                   int64_t start, int64_t end,
                                                   const cctz::time_zone& ctz) const {
    const auto* var = assert_cast<const ColumnVariant*>(&column);
    if (array_builder->type()->id() == arrow::Type::LARGE_STRING) {
        auto& builder = assert_cast<arrow::LargeStringBuilder&>(*array_builder);
        return write_variant_column_to_arrow_impl(column, *var, null_map, builder, start, end, ctz);
    } else if (array_builder->type()->id() == arrow::Type::STRING) {
        auto& builder = assert_cast<arrow::StringBuilder&>(*array_builder);
        return write_variant_column_to_arrow_impl(column, *var, null_map, builder, start, end, ctz);
    } else if (array_builder->type()->id() == arrow::Type::STRUCT) {
        auto& builder = assert_cast<arrow::StructBuilder&>(*array_builder);
        RETURN_IF_CATCH_EXCEPTION(return write_variant_column_to_arrow_struct(
                column, *var, null_map, builder, start, end, ctz));
    } else {
        return Status::InvalidArgument("Unsupported arrow type for variant column: {}",
                                       array_builder->type()->name());
    }
}

void DataTypeVariantSerDe::to_string(const IColumn& column, size_t row_num, BufferWritable& bw,
                                     const FormatOptions& options) const {
    const auto& var = assert_cast<const ColumnVariant&>(column);
    var.serialize_one_row_to_string(row_num, bw, options);
}

Status DataTypeVariantSerDe::write_column_to_orc(const std::string& timezone, const IColumn& column,
                                                 const NullMap* null_map,
                                                 orc::ColumnVectorBatch* orc_col_batch,
                                                 int64_t start, int64_t end, Arena& arena,
                                                 const FormatOptions& options) const {
    const auto* var = assert_cast<const ColumnVariant*>(&column);
    orc::StringVectorBatch* cur_batch = dynamic_cast<orc::StringVectorBatch*>(orc_col_batch);
    // First pass: calculate total memory needed and collect serialized values
    std::vector<std::string> serialized_values;
    std::vector<size_t> valid_row_indices;
    size_t total_size = 0;
    for (size_t row_id = start; row_id < end; row_id++) {
        if (cur_batch->notNull[row_id] == 1) {
            // avoid move the string data, use emplace_back to construct in place
            serialized_values.emplace_back();
            var->serialize_one_row_to_string(row_id, &serialized_values.back(), options);
            size_t len = serialized_values.back().length();
            total_size += len;
            valid_row_indices.push_back(row_id);
        }
    }
    // Allocate continues memory based on calculated size
    char* ptr = arena.alloc(total_size);
    if (!ptr) {
        return Status::InternalError(
                "malloc memory {} error when write variant column data to orc file.", total_size);
    }
    // Second pass: copy data to allocated memory
    size_t offset = 0;
    for (size_t i = 0; i < serialized_values.size(); i++) {
        const auto& serialized_value = serialized_values[i];
        size_t row_id = valid_row_indices[i];
        size_t len = serialized_value.length();
        if (offset + len > total_size) {
            return Status::InternalError(
                    "Buffer overflow when writing column data "
                    "to ORC file. offset {} with len {} "
                    "exceed total_size {} . ",
                    offset, len, total_size);
        }
        memcpy(ptr + offset, serialized_value.data(), len);
        cur_batch->data[row_id] = ptr + offset;
        cur_batch->length[row_id] = len;
        offset += len;
    }
    cur_batch->numElements = end - start;
    return Status::OK();
}

} // namespace doris
