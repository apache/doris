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
#include "vec/columns/column.h"
#include "vec/columns/column_object.h"
#include "vec/common/assert_cast.h"
#include "vec/common/schema_util.h"
#include "vec/core/field.h"
#include "vec/core/types.h"

#ifdef __AVX2__
#include "util/jsonb_parser_simd.h"
#else
#include "util/jsonb_parser.h"
#endif

namespace doris {

namespace vectorized {

template <bool is_binary_format>
Status DataTypeObjectSerDe::_write_column_to_mysql(const IColumn& column,
                                                   MysqlRowBuffer<is_binary_format>& row_buffer,
                                                   int row_idx, bool col_const,
                                                   const FormatOptions& options) const {
    const auto& variant = assert_cast<const ColumnObject&>(column);
    if (!variant.is_finalized()) {
        const_cast<ColumnObject&>(variant).finalize();
    }
    RETURN_IF_ERROR(variant.sanitize());
    if (variant.is_scalar_variant()) {
        // Serialize scalar types, like int, string, array, faster path
        const auto& root = variant.get_subcolumn({});
        RETURN_IF_ERROR(root->get_least_common_type_serde()->write_column_to_mysql(
                root->get_finalized_column(), row_buffer, row_idx, col_const, options));
    } else {
        // Serialize hierarchy types to json format
        rapidjson::StringBuffer buffer;
        bool is_null = false;
        if (!variant.serialize_one_row_to_json_format(row_idx, &buffer, &is_null)) {
            return Status::InternalError("Invalid json format");
        }
        if (is_null) {
            row_buffer.push_null();
        } else {
            row_buffer.push_string(buffer.GetString(), buffer.GetLength());
        }
    }
    return Status::OK();
}

Status DataTypeObjectSerDe::write_column_to_mysql(const IColumn& column,
                                                  MysqlRowBuffer<true>& row_buffer, int row_idx,
                                                  bool col_const,
                                                  const FormatOptions& options) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const, options);
}

Status DataTypeObjectSerDe::write_column_to_mysql(const IColumn& column,
                                                  MysqlRowBuffer<false>& row_buffer, int row_idx,
                                                  bool col_const,
                                                  const FormatOptions& options) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const, options);
}

Status DataTypeObjectSerDe::serialize_column_to_json(const IColumn& column, int start_idx,
                                                     int end_idx, BufferWritable& bw,
                                                     FormatOptions& options) const {
    SERIALIZE_COLUMN_TO_JSON();
}

void DataTypeObjectSerDe::write_one_cell_to_jsonb(const IColumn& column, JsonbWriter& result,
                                                  Arena* mem_pool, int32_t col_id,
                                                  int row_num) const {
    const auto& variant = assert_cast<const ColumnObject&>(column);
    if (!variant.is_finalized()) {
        const_cast<ColumnObject&>(variant).finalize();
    }
    result.writeKey(col_id);
    std::string value_str;
    if (!variant.serialize_one_row_to_string(row_num, &value_str)) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "Failed to serialize variant {}",
                               variant.dump_structure());
    }
    JsonbParser json_parser;
    // encode as jsonb
    bool succ = json_parser.parse(value_str.data(), value_str.size());
    if (!succ) {
        // not a valid json insert raw text
        result.writeStartString();
        result.writeString(value_str.data(), value_str.size());
        result.writeEndString();
    } else {
        // write a json binary
        result.writeStartBinary();
        result.writeBinary(json_parser.getWriter().getOutput()->getBuffer(),
                           json_parser.getWriter().getOutput()->getSize());
        result.writeEndBinary();
    }
}

void DataTypeObjectSerDe::read_one_cell_from_jsonb(IColumn& column, const JsonbValue* arg) const {
    auto& variant = assert_cast<ColumnObject&>(column);
    Field field;
    if (arg->isBinary()) {
        const auto* blob = static_cast<const JsonbBlobVal*>(arg);
        field = JsonbField(blob->getBlob(), blob->getBlobLen());
    } else if (arg->isString()) {
        // not a valid jsonb type, insert as string
        const auto* str = static_cast<const JsonbStringVal*>(arg);
        field = Field(String(str->getBlob(), str->getBlobLen()));
    } else {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "Invalid jsonb type");
    }
    variant.insert(field);
}

Status DataTypeObjectSerDe::serialize_one_cell_to_json(const IColumn& column, int row_num,
                                                       BufferWritable& bw,
                                                       FormatOptions& options) const {
    const auto* var = check_and_get_column<ColumnObject>(column);
    if (!var->serialize_one_row_to_string(row_num, bw)) {
        return Status::InternalError("Failed to serialize variant {}", var->dump_structure());
    }
    return Status::OK();
}

void DataTypeObjectSerDe::write_column_to_arrow(const IColumn& column, const NullMap* null_map,
                                                arrow::ArrayBuilder* array_builder, int start,
                                                int end, const cctz::time_zone& ctz) const {
    const auto* var = check_and_get_column<ColumnObject>(column);
    auto& builder = assert_cast<arrow::StringBuilder&>(*array_builder);
    for (size_t i = start; i < end; ++i) {
        if (null_map && (*null_map)[i]) {
            checkArrowStatus(builder.AppendNull(), column.get_name(),
                             array_builder->type()->name());
        } else {
            std::string serialized_value;
            if (!var->serialize_one_row_to_string(i, &serialized_value)) {
                throw doris::Exception(ErrorCode::INTERNAL_ERROR, "Failed to serialize variant {}",
                                       var->dump_structure());
            }
            checkArrowStatus(builder.Append(serialized_value.data(),
                                            static_cast<int>(serialized_value.size())),
                             column.get_name(), array_builder->type()->name());
        }
    }
}

Status DataTypeObjectSerDe::write_one_cell_to_json(const IColumn& column, rapidjson::Value& result,
                                                   rapidjson::Document::AllocatorType& allocator,
                                                   Arena& mem_pool, int row_num) const {
    const auto& var = assert_cast<const ColumnObject&>(column);
    if (!var.is_finalized()) {
        var.assume_mutable()->finalize();
    }
    result.SetObject();
    // sort to make output stable, todo add a config
    auto subcolumns = schema_util::get_sorted_subcolumns(var.get_subcolumns());
    for (const auto& entry : subcolumns) {
        const auto& subcolumn = entry->data.get_finalized_column();
        const auto& subtype_serde = entry->data.get_least_common_type_serde();
        if (subcolumn.is_null_at(row_num)) {
            continue;
        }
        rapidjson::Value key;
        key.SetString(entry->path.get_path().data(), entry->path.get_path().size());
        rapidjson::Value val;
        RETURN_IF_ERROR(subtype_serde->write_one_cell_to_json(subcolumn, val, allocator, mem_pool,
                                                              row_num));
        if (val.IsNull() && entry->path.empty()) {
            // skip null value with empty key, indicate the null json value of root in variant map,
            // usally padding in nested arrays
            continue;
        }
        result.AddMember(key, val, allocator);
    }
    return Status::OK();
}

Status DataTypeObjectSerDe::write_column_to_orc(const std::string& timezone, const IColumn& column,
                                                const NullMap* null_map,
                                                orc::ColumnVectorBatch* orc_col_batch, int start,
                                                int end,
                                                std::vector<StringRef>& buffer_list) const {
    const auto* var = check_and_get_column<ColumnObject>(column);
    orc::StringVectorBatch* cur_batch = dynamic_cast<orc::StringVectorBatch*>(orc_col_batch);
    // First pass: calculate total memory needed and collect serialized values
    std::vector<std::string> serialized_values;
    std::vector<size_t> valid_row_indices;
    size_t total_size = 0;
    for (size_t row_id = start; row_id < end; row_id++) {
        if (cur_batch->notNull[row_id] == 1) {
            // avoid move the string data, use emplace_back to construct in place
            serialized_values.emplace_back();
            RETURN_IF_ERROR(var->serialize_one_row_to_string(row_id, &serialized_values.back()));
            size_t len = serialized_values.back().length();
            total_size += len;
            valid_row_indices.push_back(row_id);
        }
    }
    // Allocate continues memory based on calculated size
    char* ptr = (char*)malloc(total_size);
    if (!ptr) {
        return Status::InternalError(
                "malloc memory {} error when write variant column data to orc file.", total_size);
    }
    StringRef bufferRef;
    bufferRef.data = ptr;
    bufferRef.size = total_size;
    buffer_list.emplace_back(bufferRef);
    // Second pass: copy data to allocated memory
    size_t offset = 0;
    for (size_t i = 0; i < serialized_values.size(); i++) {
        const auto& serialized_value = serialized_values[i];
        size_t row_id = valid_row_indices[i];
        size_t len = serialized_value.length();
        if (offset + len > total_size) {
            return Status::InternalError(
                    "Buffer overflow when writing column data to ORC file. offset {} with len {} "
                    "exceed total_size {} . ",
                    offset, len, total_size);
        }
        memcpy(const_cast<char*>(bufferRef.data) + offset, serialized_value.data(), len);
        cur_batch->data[row_id] = const_cast<char*>(bufferRef.data) + offset;
        cur_batch->length[row_id] = len;
        offset += len;
    }
    cur_batch->numElements = end - start;
    return Status::OK();
}

} // namespace vectorized

} // namespace doris
