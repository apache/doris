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

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "common/cast_set.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/column/column_map.h"
#include "core/column/column_struct.h"
#include "core/column/column_variant.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/column/variant_v2/column_variant_v2_typed_column.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type_serde/data_type_serde.h"
#include "core/data_type_serde/data_type_variant_v2_serde.h"
#include "core/data_type_serde/variant_arrow_utils.h"
#include "core/field.h"
#include "core/string_ref.h"
#include "core/types.h"
#include "core/value/jsonb_value.h"
#include "exec/common/variant_util.h"
#include "exprs/function/parse/variant_jsonb_parse.h"
#include "exprs/function/parse/variant_string_parse.h"
#include "util/json/json_parser.h"
#include "util/jsonb_writer.h"

namespace doris {
namespace {

Status append_legacy_arrow_document(const ColumnVariant& column, size_t index,
                                    VariantBatchBuilder::Row& output,
                                    const DataTypeSerDe::FormatOptions& options, size_t depth);

// Legacy CAST accepts more root families than V2 CAST. Encode their structure here so
// Flight output does not reject valid roots or lose typed leaves through JSON reparsing.
Status append_legacy_arrow_value(const IColumn& column, const DataTypePtr& type, size_t index,
                                 VariantBatchBuilder::Row& output,
                                 const DataTypeSerDe::FormatOptions& options, size_t depth = 0) {
    if (depth > VARIANT_MAX_NESTING_DEPTH) {
        return Status::NotSupported(
                "Native Arrow Variant nesting exceeds {}; "
                "use enable_arrow_flight_sql_native_variant=false for UTF8 output",
                VARIANT_MAX_NESTING_DEPTH);
    }
    if (const auto* constant = check_and_get_column<ColumnConst>(column)) {
        return append_legacy_arrow_value(constant->get_data_column(), type, 0, output, options,
                                         depth);
    }
    if (const auto* nullable = check_and_get_column<ColumnNullable>(column)) {
        if (nullable->is_null_at(index)) {
            output.add_null();
            return Status::OK();
        }
        return append_legacy_arrow_value(nullable->get_nested_column(), remove_nullable(type),
                                         index, output, options, depth);
    }
    const auto primitive = type->get_primitive_type();
    if (is_supported_variant_typed_identity(primitive)) {
        dispatch_variant_typed_column(
                column, primitive, [&]<PrimitiveType Type>(const auto& scalar) {
                    with_variant_typed_scalar<Type>(
                            scalar, index, cast_set<uint8_t>(type->get_scale()),
                            [&](const VariantScalarRef& value) { output.add_scalar(value); });
                });
    } else if (primitive == TYPE_TIMEV2) {
        // TIMEV2 already stores microseconds; treating its physical double as a number loses its type.
        const double micros = assert_cast<const ColumnTimeV2&>(column).get_data()[index];
        // Parquet TIME is a time of day, whereas Doris TIME also represents signed durations.
        // Reject unrepresentable durations instead of wrapping them or emitting invalid TIME values.
        constexpr int64_t micros_per_day = 86400000000;
        if (!std::isfinite(micros) || micros < 0 || micros >= micros_per_day ||
            std::llround(micros) >= micros_per_day) {
            return Status::NotSupported(
                    "Native Arrow Variant TIMEV2 requires a time in [00:00:00, 24:00:00); "
                    "use enable_arrow_flight_sql_native_variant=false for UTF8 output");
        }
        output.add_time_ntz_micros(std::llround(micros));
    } else if (primitive == TYPE_VARBINARY) {
        // Binary leaves must retain arbitrary bytes, including NUL and non-UTF8 data.
        output.add_binary(column.get_data_at(index));
    } else if (primitive == TYPE_JSONB) {
        // JSONB leaves retain their own depth limit, but also consume the enclosing Variant depth.
        try {
            jsonb_to_variant(column.get_data_at(index), output, cast_set<uint32_t>(depth));
        } catch (const Exception& e) {
            if (e.code() != ErrorCode::INVALID_ARGUMENT) {
                return e.to_status();
            }
            return Status::NotSupported(
                    "Native Arrow Variant cannot encode JSONB leaf: {}; "
                    "use enable_arrow_flight_sql_native_variant=false for UTF8 output",
                    e.what());
        }
    } else if (primitive == TYPE_ARRAY) {
        const auto& array = assert_cast<const ColumnArray&>(column);
        const auto& array_type = assert_cast<const DataTypeArray&>(*type);
        auto scope = output.start_array();
        for (size_t element = array.offset_at(index); element < array.get_offsets()[index];
             ++element) {
            RETURN_IF_ERROR(append_legacy_arrow_value(array.get_data(),
                                                      array_type.get_nested_type(), element, output,
                                                      options, depth + 1));
        }
        scope.finish();
    } else if (primitive == TYPE_MAP) {
        const auto& map = assert_cast<const ColumnMap&>(column);
        const auto& map_type = assert_cast<const DataTypeMap&>(*type);
        auto scope = output.start_object();
        for (size_t element = map.get_offsets()[static_cast<ssize_t>(index) - 1];
             element < map.get_offsets()[index]; ++element) {
            // Variant object keys cannot distinguish SQL NULL from the literal string "null".
            if (map.get_keys().is_null_at(element)) {
                return Status::NotSupported(
                        "Native Arrow Variant cannot represent MAP with NULL keys; "
                        "use enable_arrow_flight_sql_native_variant=false for UTF8 output");
            }
            auto key = map_type.get_key_type()->to_string(map.get_keys(), element, options);
            scope.add_key({key.data(), key.size()});
            RETURN_IF_ERROR(append_legacy_arrow_value(map.get_values(), map_type.get_value_type(),
                                                      element, output, options, depth + 1));
        }
        scope.finish();
    } else if (primitive == TYPE_STRUCT) {
        const auto& structure = assert_cast<const ColumnStruct&>(column);
        const auto& struct_type = assert_cast<const DataTypeStruct&>(*type);
        auto scope = output.start_object();
        for (size_t field = 0; field < struct_type.get_elements().size(); ++field) {
            const auto& name = struct_type.get_element_names()[field];
            scope.add_key({name.data(), name.size()});
            RETURN_IF_ERROR(append_legacy_arrow_value(structure.get_column(field),
                                                      struct_type.get_element(field), index, output,
                                                      options, depth + 1));
        }
        scope.finish();
    } else if (primitive == TYPE_VARIANT) {
        if (const auto* legacy = check_and_get_column<ColumnVariant>(column)) {
            const bool visible = legacy->is_scalar_variant()
                                         ? !legacy->get_root()->is_null_at(index)
                                         : legacy->is_visible_root_value(index);
            if (visible) {
                return append_legacy_arrow_value(*legacy->get_root(), legacy->get_root_type(),
                                                 index, output, options, depth);
            }
            RETURN_IF_ERROR(append_legacy_arrow_document(*legacy, index, output, options, depth));
        } else {
            // Reuse the selected-value import: each leaf may share a large dictionary, and
            // its enclosing legacy containers must count toward the native depth limit.
            Status status = Status::OK();
            visit_variant_v2_values(
                    column, index, index + 1, {}, [&](size_t) { output.add_null(); },
                    [&](size_t, VariantRef value) {
                        status = append_flight_variant_value(value, output, depth);
                    });
            RETURN_IF_ERROR(status);
        }
    } else {
        return Status::NotSupported("Native Arrow Variant does not support {} roots",
                                    type->get_name());
    }
    return Status::OK();
}

// Assemble the same flattened document paths as legacy JSON output, but encode each
// leaf with its stored type. JSON reparsing loses decimal precision and date identity.
Status append_legacy_arrow_document(const ColumnVariant& column, size_t index,
                                    VariantBatchBuilder::Row& output,
                                    const DataTypeSerDe::FormatOptions& options, size_t depth) {
    struct Field {
        std::string path;
        ColumnPtr values;
        DataTypePtr type;
        size_t row;
    };
    std::vector<Field> fields;
    auto append_serialized = [&](const ColumnMap& map) {
        const auto& keys = assert_cast<const ColumnString&>(map.get_keys());
        const auto& values = assert_cast<const ColumnString&>(map.get_values());
        for (size_t i = map.get_offsets()[static_cast<ssize_t>(index) - 1];
             i < map.get_offsets()[index]; ++i) {
            ColumnVariant::Subcolumn subcolumn(0, true);
            subcolumn.deserialize_from_binary_column(&values, i);
            subcolumn.finalize();
            fields.push_back({keys.get_data_at(i).to_string(), subcolumn.get_finalized_column_ptr(),
                              subcolumn.get_least_common_type(), 0});
        }
    };
    const auto& snapshot = assert_cast<const ColumnMap&>(*column.get_doc_value_column());
    if (snapshot.get_offsets()[index] != 0) {
        // Document snapshots are authoritative, including empty rows after a populated snapshot.
        append_serialized(snapshot);
    } else {
        for (const auto& subcolumn : column.get_subcolumns()) {
            if (subcolumn->path.empty() || subcolumn->data.is_null_at(index) ||
                subcolumn->data.is_empty_nested(index)) {
                continue;
            }
            if (subcolumn->data.is_finalized()) {
                fields.push_back({subcolumn->path.get_path(),
                                  subcolumn->data.get_finalized_column_ptr(),
                                  subcolumn->data.get_least_common_type(), index});
            } else {
                // Scans may leave lazy defaults or multiple parts. Materialize only this row,
                // without mutating shared input or repeatedly copying an entire batch.
                auto value = subcolumn->data.cut(index, 1);
                value.finalize();
                fields.push_back({subcolumn->path.get_path(), value.get_finalized_column_ptr(),
                                  value.get_least_common_type(), 0});
            }
        }
        append_serialized(assert_cast<const ColumnMap&>(*column.get_sparse_column()));
    }
    std::sort(fields.begin(), fields.end(),
              [](const auto& a, const auto& b) { return a.path < b.path; });
    std::vector<std::string_view> prefix;
    std::vector<VariantBatchBuilder::Row::ObjectScope> objects;
    objects.push_back(output.start_object());
    for (const auto& field : fields) {
        std::vector<std::string_view> parts;
        std::string_view path(field.path);
        while (true) {
            const auto dot = path.find('.');
            parts.push_back(path.substr(0, dot));
            if (depth + parts.size() > VARIANT_MAX_NESTING_DEPTH) {
                return Status::NotSupported(
                        "Native Arrow Variant nesting exceeds {}; "
                        "use enable_arrow_flight_sql_native_variant=false for UTF8 output",
                        VARIANT_MAX_NESTING_DEPTH);
            }
            if (dot == std::string_view::npos) {
                break;
            }
            path.remove_prefix(dot + 1);
        }
        size_t common = 0;
        while (common < prefix.size() && common + 1 < parts.size() &&
               prefix[common] == parts[common]) {
            ++common;
        }
        while (prefix.size() > common) {
            objects.back().finish();
            objects.pop_back();
            prefix.pop_back();
        }
        for (size_t i = common; i + 1 < parts.size(); ++i) {
            objects.back().add_key({parts[i].data(), parts[i].size()});
            objects.push_back(output.start_object());
            prefix.push_back(parts[i]);
        }
        objects.back().add_key({parts.back().data(), parts.back().size()});
        RETURN_IF_ERROR(append_legacy_arrow_value(*field.values, field.type, field.row, output,
                                                  options, depth + parts.size()));
    }
    while (!objects.empty()) {
        objects.back().finish();
        objects.pop_back();
    }
    return Status::OK();
}

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

} // namespace

#include "common/compile_check_begin.h"

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
    const auto* var = check_and_get_column<ColumnVariant>(column);
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
    const auto* var = check_and_get_column<ColumnVariant>(column);
    if (array_builder->type()->id() == arrow::Type::STRUCT) {
        // Keep legacy scalar and document leaves in their original types.
        // The outer null map must remain SQL NULL on the wire.
        if (start < 0 || end < start || end > column.size() ||
            (null_map != nullptr && end > null_map->size())) {
            return Status::InvalidArgument("Invalid Variant Arrow row range [{}, {})", start, end);
        }
        // A legacy null root renders as {}, not Variant null, even in a scalar-only batch.
        if (var->is_scalar_variant() && !var->get_root()->has_null(start, end)) {
            auto scalar_type = remove_nullable(var->get_root_type());
            // Unsupported roots use the row path below, after applying the outer SQL null mask.
            if (is_supported_variant_typed_identity(scalar_type->get_primitive_type())) {
                // Avoid a JSON round trip that would turn exact decimal roots into doubles.
                auto typed =
                        ColumnVariantV2::create_typed(make_nullable(var->get_root()), scalar_type);
                return DataTypeVariantV2SerDe().write_column_to_arrow(
                        *typed, null_map, array_builder, start, end, ctz);
            }
        }
        const size_t rows = end - start;
        NullMap selected_nulls(rows, 0);
        NullMap root_mask(rows, 1);
        bool has_roots = false;
        bool has_documents = false;
        for (size_t row = 0; row < rows; ++row) {
            selected_nulls[row] = null_map != nullptr && (*null_map)[start + row];
            if (selected_nulls[row]) {
                continue;
            }
            const bool root_visible = var->is_scalar_variant()
                                              ? !var->get_root()->is_null_at(start + row)
                                              : var->is_visible_root_value(start + row);
            root_mask[row] = !root_visible;
            has_roots |= root_visible;
            has_documents |= !root_visible;
        }
        ColumnPtr roots;
        if (has_roots) {
            VariantBatchBuilder builder(VariantBatchBuilder::ReserveHint {.rows = rows});
            FormatOptions options;
            options.timezone = &ctz;
            for (size_t index = 0; index < rows; ++index) {
                auto row = builder.begin_row();
                if (root_mask[index]) {
                    row.add_null();
                } else {
                    RETURN_IF_ERROR(append_legacy_arrow_value(
                            *var->get_root(), var->get_root_type(), start + index, row, options));
                }
                row.finish();
            }
            auto values = builder.finish_batch();
            auto encoded = ColumnVariantV2::create();
            encoded->insert_encoded_batch(values);
            roots = std::move(encoded);
        }
        ColumnPtr documents;
        if (has_documents || !has_roots) {
            VariantBatchBuilder builder(VariantBatchBuilder::ReserveHint {.rows = rows});
            FormatOptions options;
            options.timezone = &ctz;
            for (size_t index = 0; index < rows; ++index) {
                auto row = builder.begin_row();
                if (root_mask[index] && !selected_nulls[index]) {
                    RETURN_IF_ERROR(
                            append_legacy_arrow_document(*var, start + index, row, options, 0));
                } else {
                    row.add_null();
                }
                row.finish();
            }
            auto values = builder.finish_batch();
            auto encoded = ColumnVariantV2::create();
            encoded->insert_encoded_batch(values);
            documents = std::move(encoded);
        }
        // Write contiguous root/document runs without building another copy of the encoded batch.
        for (size_t first = 0; first < rows;) {
            const bool use_root = static_cast<bool>(roots) && (!root_mask[first] || !documents);
            size_t last = first + 1;
            while (last < rows &&
                   use_root == (static_cast<bool>(roots) && (!root_mask[last] || !documents))) {
                ++last;
            }
            RETURN_IF_ERROR(DataTypeVariantV2SerDe().write_column_to_arrow(
                    *(use_root ? roots : documents), &selected_nulls, array_builder, first, last,
                    ctz));
            first = last;
        }
        return Status::OK();
    }
    if (array_builder->type()->id() == arrow::Type::LARGE_STRING) {
        auto& builder = assert_cast<arrow::LargeStringBuilder&>(*array_builder);
        return write_variant_column_to_arrow_impl(column, *var, null_map, builder, start, end, ctz);
    } else if (array_builder->type()->id() == arrow::Type::STRING) {
        auto& builder = assert_cast<arrow::StringBuilder&>(*array_builder);
        return write_variant_column_to_arrow_impl(column, *var, null_map, builder, start, end, ctz);
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
    const auto* var = check_and_get_column<ColumnVariant>(column);
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
                    "Buffer overflow when writing column data to ORC file. offset {} with len {} "
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
