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

#include "data_type_object_serde.h"

#include <rapidjson/stringbuffer.h>

#include <cstdint>
#include <string>

#include "common/exception.h"
#include "common/status.h"
#include "util/jsonb_parser_simd.h"
#include "vec/columns/column.h"
#include "vec/columns/column_variant.h"
#include "vec/common/assert_cast.h"
#include "vec/common/schema_util.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/serde/data_type_serde.h"
#include "vec/json/json_parser.h"
#include "vec/json/parse2column.cpp"

namespace doris {

namespace vectorized {
#include "common/compile_check_begin.h"

template <bool is_binary_format>

Status DataTypeVariantSerDe::_write_column_to_mysql(const IColumn& column,
                                                    MysqlRowBuffer<is_binary_format>& row_buffer,
                                                    int64_t row_idx, bool col_const,
                                                    const FormatOptions& options) const {
    const auto& variant = assert_cast<const ColumnVariant&>(column);
    // Serialize hierarchy types to json format
    std::string buffer;
    variant.serialize_one_row_to_string(row_idx, &buffer);
    row_buffer.push_string(buffer.data(), buffer.size());
    return Status::OK();
}

Status DataTypeVariantSerDe::write_column_to_mysql(const IColumn& column,
                                                   MysqlRowBuffer<true>& row_buffer,
                                                   int64_t row_idx, bool col_const,
                                                   const FormatOptions& options) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const, options);
}

Status DataTypeVariantSerDe::write_column_to_mysql(const IColumn& column,
                                                   MysqlRowBuffer<false>& row_buffer,
                                                   int64_t row_idx, bool col_const,
                                                   const FormatOptions& options) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const, options);
}

Status DataTypeVariantSerDe::serialize_column_to_json(const IColumn& column, int64_t start_idx,
                                                      int64_t end_idx, BufferWritable& bw,
                                                      FormatOptions& options) const {
    SERIALIZE_COLUMN_TO_JSON();
}

void DataTypeVariantSerDe::write_one_cell_to_jsonb(const IColumn& column, JsonbWriter& result,
                                                   Arena* mem_pool, int32_t col_id,
                                                   int64_t row_num) const {
    const auto& variant = assert_cast<const ColumnVariant&>(column);
    result.writeKey(cast_set<JsonbKeyValue::keyid_type>(col_id));
    std::string value_str;
    variant.serialize_one_row_to_string(row_num, &value_str);
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
        const auto* blob = static_cast<const JsonbBlobVal*>(arg);
        field = Field::create_field<TYPE_JSONB>(JsonbField(blob->getBlob(), blob->getBlobLen()));
    } else if (arg->isString()) {
        // not a valid jsonb type, insert as string
        const auto* str = static_cast<const JsonbStringVal*>(arg);
        field = Field::create_field<TYPE_STRING>(String(str->getBlob(), str->getBlobLen()));
    } else {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "Invalid jsonb type");
    }
    variant.insert(field);
}

Status DataTypeVariantSerDe::serialize_one_cell_to_json(const IColumn& column, int64_t row_num,
                                                        BufferWritable& bw,
                                                        FormatOptions& options) const {
    const auto* var = check_and_get_column<ColumnVariant>(column);
    var->serialize_one_row_to_string(row_num, bw);
    return Status::OK();
}

Status DataTypeVariantSerDe::deserialize_one_cell_from_json(IColumn& column, Slice& slice,
                                                            const FormatOptions& options) const {
    vectorized::ParseConfig config;
    auto parser = parsers_pool.get([] { return new JsonParser(); });
    RETURN_IF_CATCH_EXCEPTION(
            parse_json_to_variant(column, slice.data, slice.size, parser.get(), config));
    return Status::OK();
}

Status DataTypeVariantSerDe::deserialize_column_from_json_vector(
        IColumn& column, std::vector<Slice>& slices, uint64_t* num_deserialized,
        const FormatOptions& options) const {
    DESERIALIZE_COLUMN_FROM_JSON_VECTOR()
    return Status::OK();
}

template <typename BuilderType>
Status write_column_to_arrow_impl(const IColumn& column, const NullMap* null_map,
                                  BuilderType& builder, int64_t start, int64_t end) const {
    const auto* var = check_and_get_column<ColumnVariant>(column);
    for (size_t i = start; i < end; ++i) {
        if (null_map && (*null_map)[i]) {
            RETURN_IF_ERROR(checkArrowStatus(builder.AppendNull(), column.get_name(),
                                             builder.type()->name()));
        } else {
            std::string serialized_value;
            var->serialize_one_row_to_string(i, &serialized_value);
            RETURN_IF_ERROR(
                    checkArrowStatus(builder.Append(serialized_value.data(),
                                                    static_cast<int>(serialized_value.size())),
                                     column.get_name(), builder.type()->name()));
        }
    }
    return Status::OK();
}

Status DataTypeVariantSerDe::write_column_to_arrow(const IColumn& column, const NullMap* null_map,
                                                   arrow::ArrayBuilder* array_builder,
                                                   int64_t start, int64_t end,
                                                   const cctz::time_zone& ctz) const {
    const auto* var = check_and_get_column<ColumnVariant>(column);
    if (array_builder->type()->id() == arrow::Type::LARGE_STRING) {
        auto& builder = assert_cast<arrow::LargeStringBuilder&>(*array_builder);
        return write_column_to_arrow_impl(column, null_map, builder, start, end);
    } else if (array_builder->type()->id() == arrow::Type::STRING) {
        auto& builder = assert_cast<arrow::StringBuilder&>(*array_builder);
        return write_column_to_arrow_impl(column, null_map, builder, start, end);
    } else {
        return Status::InvalidArgument("Unsupported arrow type for string column: {}",
                                       array_builder->type()->name());
    }
}

Status DataTypeVariantSerDe::write_column_to_orc(const std::string& timezone, const IColumn& column,
                                                 const NullMap* null_map,
                                                 orc::ColumnVectorBatch* orc_col_batch,
                                                 int64_t start, int64_t end,
                                                 std::vector<StringRef>& buffer_list) const {
    const auto* var = check_and_get_column<ColumnVariant>(column);
    auto* cur_batch = dynamic_cast<orc::StringVectorBatch*>(orc_col_batch);

    INIT_MEMORY_FOR_ORC_WRITER()

    for (size_t row_id = start; row_id < end; row_id++) {
        if (cur_batch->notNull[row_id] == 1) {
            auto serialized_value = std::make_unique<std::string>();
            var->serialize_one_row_to_string(row_id, serialized_value.get());
            auto len = serialized_value->length();

            REALLOC_MEMORY_FOR_ORC_WRITER()

            memcpy(const_cast<char*>(bufferRef.data) + offset, serialized_value->data(), len);
            cur_batch->data[row_id] = const_cast<char*>(bufferRef.data) + offset;
            cur_batch->length[row_id] = len;
            offset += len;
        }
    }

    cur_batch->numElements = end - start;
    return Status::OK();
}

} // namespace vectorized

} // namespace doris
