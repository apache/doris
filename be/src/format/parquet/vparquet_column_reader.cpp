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

#include "format/parquet/vparquet_column_reader.h"

#include <cctz/time_zone.h>
#include <gen_cpp/parquet_types.h>
#include <rapidjson/document.h>
#include <sys/types.h>

#include <algorithm>
#include <deque>
#include <limits>
#include <map>
#include <set>
#include <string_view>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/status.h"
#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_struct.h"
#include "core/column/column_varbinary.h"
#include "core/column/column_variant.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_jsonb.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/data_type_variant.h"
#include "core/data_type/define_primitive_type.h"
#include "core/data_type_serde/data_type_serde.h"
#include "core/string_buffer.hpp"
#include "core/value/jsonb_value.h"
#include "core/value/timestamptz_value.h"
#include "core/value/vdatetime_value.h"
#include "exec/common/variant_util.h"
#include "format/parquet/level_decoder.h"
#include "format/parquet/parquet_variant_reader.h"
#include "format/parquet/schema_desc.h"
#include "format/parquet/vparquet_column_chunk_reader.h"
#include "io/fs/tracing_file_reader.h"
#include "runtime/runtime_profile.h"
#include "util/jsonb_document.h"

namespace doris {
static void fill_struct_null_map(FieldSchema* field, NullMap& null_map,
                                 const std::vector<level_t>& rep_levels,
                                 const std::vector<level_t>& def_levels) {
    size_t num_levels = def_levels.size();
    DCHECK_EQ(num_levels, rep_levels.size());
    size_t origin_size = null_map.size();
    null_map.resize(origin_size + num_levels);
    size_t pos = origin_size;
    for (size_t i = 0; i < num_levels; ++i) {
        // skip the levels affect its ancestor or its descendants
        if (def_levels[i] < field->repeated_parent_def_level ||
            rep_levels[i] > field->repetition_level) {
            continue;
        }
        if (def_levels[i] >= field->definition_level) {
            null_map[pos++] = 0;
        } else {
            null_map[pos++] = 1;
        }
    }
    null_map.resize(pos);
}

static void fill_array_offset(FieldSchema* field, ColumnArray::Offsets64& offsets_data,
                              NullMap* null_map_ptr, const std::vector<level_t>& rep_levels,
                              const std::vector<level_t>& def_levels) {
    size_t num_levels = rep_levels.size();
    DCHECK_EQ(num_levels, def_levels.size());
    size_t origin_size = offsets_data.size();
    offsets_data.resize(origin_size + num_levels);
    if (null_map_ptr != nullptr) {
        null_map_ptr->resize(origin_size + num_levels);
    }
    size_t offset_pos = origin_size - 1;
    for (size_t i = 0; i < num_levels; ++i) {
        // skip the levels affect its ancestor or its descendants
        if (def_levels[i] < field->repeated_parent_def_level ||
            rep_levels[i] > field->repetition_level) {
            continue;
        }
        if (rep_levels[i] == field->repetition_level) {
            offsets_data[offset_pos]++;
            continue;
        }
        offset_pos++;
        offsets_data[offset_pos] = offsets_data[offset_pos - 1];
        if (def_levels[i] >= field->definition_level) {
            offsets_data[offset_pos]++;
        }
        if (def_levels[i] >= field->definition_level - 1) {
            (*null_map_ptr)[offset_pos] = 0;
        } else {
            (*null_map_ptr)[offset_pos] = 1;
        }
    }
    offsets_data.resize(offset_pos + 1);
    if (null_map_ptr != nullptr) {
        null_map_ptr->resize(offset_pos + 1);
    }
}

static constexpr int64_t UNIX_EPOCH_DAYNR = 719528;
static constexpr int64_t MICROS_PER_SECOND = 1000000;

static int64_t variant_date_value(const VecDateTimeValue& value) {
    return value.daynr() - UNIX_EPOCH_DAYNR;
}

static int64_t variant_date_value(const DateV2Value<DateV2ValueType>& value) {
    return value.daynr() - UNIX_EPOCH_DAYNR;
}

static int64_t variant_datetime_value(const VecDateTimeValue& value) {
    int64_t timestamp = 0;
    value.unix_timestamp(&timestamp, cctz::utc_time_zone());
    return timestamp * MICROS_PER_SECOND;
}

static int64_t variant_datetime_value(const DateV2Value<DateTimeV2ValueType>& value) {
    int64_t timestamp = 0;
    value.unix_timestamp(&timestamp, cctz::utc_time_zone());
    return timestamp * MICROS_PER_SECOND + value.microsecond();
}

static int64_t variant_datetime_value(const TimestampTzValue& value) {
    int64_t timestamp = 0;
    value.unix_timestamp(&timestamp, cctz::utc_time_zone());
    return timestamp * MICROS_PER_SECOND + value.microsecond();
}

static int find_child_idx(const FieldSchema& field, std::string_view name) {
    for (int i = 0; i < field.children.size(); ++i) {
        if (field.children[i].lower_case_name == name) {
            return i;
        }
    }
    return -1;
}

static bool is_variant_wrapper_typed_value_child(const FieldSchema& field) {
    auto type = remove_nullable(field.data_type);
    return type->get_primitive_type() == TYPE_STRUCT || type->get_primitive_type() == TYPE_ARRAY;
}

static bool is_unannotated_variant_value_field(const FieldSchema& field) {
    // VARIANT residual value is raw binary; annotated strings named value are user fields.
    return field.lower_case_name == "value" && field.physical_type == tparquet::Type::BYTE_ARRAY &&
           !field.parquet_schema.__isset.logicalType &&
           !field.parquet_schema.__isset.converted_type;
}

static bool is_unannotated_variant_metadata_field(const FieldSchema& field) {
    return field.lower_case_name == "metadata" &&
           field.physical_type == tparquet::Type::BYTE_ARRAY &&
           !field.parquet_schema.__isset.logicalType &&
           !field.parquet_schema.__isset.converted_type;
}

static bool is_variant_wrapper_field(const FieldSchema& field,
                                     bool allow_scalar_typed_value_only_wrapper) {
    auto type = remove_nullable(field.data_type);
    if (type->get_primitive_type() != TYPE_STRUCT && type->get_primitive_type() != TYPE_VARIANT) {
        return false;
    }

    bool has_metadata = false;
    bool has_value = false;
    const FieldSchema* typed_value = nullptr;
    for (const auto& child : field.children) {
        if (child.lower_case_name == "metadata") {
            if (!is_unannotated_variant_metadata_field(child)) {
                return false;
            }
            has_metadata = true;
            continue;
        }
        if (child.lower_case_name == "value") {
            if (!is_unannotated_variant_value_field(child)) {
                return false;
            }
            has_value = true;
            continue;
        }
        if (child.lower_case_name == "typed_value") {
            typed_value = &child;
            continue;
        }
        return false;
    }
    if (has_metadata) {
        return type->get_primitive_type() == TYPE_VARIANT && (has_value || typed_value != nullptr);
    }
    if (has_value) {
        return typed_value != nullptr;
    }
    return typed_value != nullptr && (allow_scalar_typed_value_only_wrapper ||
                                      is_variant_wrapper_typed_value_child(*typed_value));
}

static bool is_value_only_variant_wrapper_candidate(const FieldSchema& field) {
    auto type = remove_nullable(field.data_type);
    if (type->get_primitive_type() != TYPE_STRUCT && type->get_primitive_type() != TYPE_VARIANT) {
        return false;
    }

    bool has_value = false;
    for (const auto& child : field.children) {
        if (is_unannotated_variant_value_field(child)) {
            has_value = true;
            continue;
        }
        return false;
    }
    return has_value;
}

static Status get_binary_field(const Field& field, std::string* value, bool* present) {
    if (field.is_null()) {
        *present = false;
        return Status::OK();
    }
    *present = true;
    switch (field.get_type()) {
    case TYPE_STRING:
        *value = field.get<TYPE_STRING>();
        return Status::OK();
    case TYPE_CHAR:
        *value = field.get<TYPE_CHAR>();
        return Status::OK();
    case TYPE_VARCHAR:
        *value = field.get<TYPE_VARCHAR>();
        return Status::OK();
    case TYPE_VARBINARY: {
        auto ref = field.get<TYPE_VARBINARY>().to_string_ref();
        value->assign(ref.data, ref.size);
        return Status::OK();
    }
    default:
        return Status::Corruption("Parquet VARIANT binary field has unexpected Doris type {}",
                                  field.get_type_name());
    }
}

static PathInData append_path(const PathInData& prefix, const PathInData& suffix) {
    if (prefix.empty()) {
        return suffix;
    }
    if (suffix.empty()) {
        return prefix;
    }
    PathInDataBuilder builder;
    builder.append(prefix.get_parts(), false);
    builder.append(suffix.get_parts(), false);
    return builder.build();
}

static Status make_jsonb_field(std::string_view json, FieldWithDataType* value) {
    JsonBinaryValue jsonb_value;
    RETURN_IF_ERROR(jsonb_value.from_json_string(json.data(), json.size()));
    value->field =
            Field::create_field<TYPE_JSONB>(JsonbField(jsonb_value.value(), jsonb_value.size()));
    value->base_scalar_type_id = TYPE_JSONB;
    value->num_dimensions = 0;
    value->precision = 0;
    value->scale = 0;
    return Status::OK();
}

static std::string make_null_array_json(size_t elements) {
    std::string json = "[";
    for (size_t i = 0; i < elements; ++i) {
        if (i != 0) {
            json.push_back(',');
        }
        json.append("null");
    }
    json.push_back(']');
    return json;
}

static Status make_empty_object_field(Field* field) {
    FieldWithDataType value;
    RETURN_IF_ERROR(make_jsonb_field("{}", &value));
    *field = std::move(value.field);
    return Status::OK();
}

static Status insert_jsonb_value(const PathInData& path, std::string_view json,
                                 VariantMap* values) {
    FieldWithDataType value;
    RETURN_IF_ERROR(make_jsonb_field(json, &value));
    (*values)[path] = std::move(value);
    return Status::OK();
}

static Status insert_empty_object_marker(const PathInData& path, VariantMap* values) {
    return insert_jsonb_value(path, "{}", values);
}

static bool is_empty_object_marker(const FieldWithDataType& value) {
    if (value.field.get_type() != TYPE_JSONB) {
        return false;
    }
    const auto& jsonb = value.field.get<TYPE_JSONB>();
    const JsonbDocument* document = nullptr;
    Status st =
            JsonbDocument::checkAndCreateDocument(jsonb.get_value(), jsonb.get_size(), &document);
    if (!st.ok() || document == nullptr || document->getValue() == nullptr ||
        !document->getValue()->isObject()) {
        return false;
    }
    return document->getValue()->unpack<ObjectVal>()->numElem() == 0;
}

static Status collect_empty_object_markers(const rapidjson::Value& value, PathInDataBuilder* path,
                                           VariantMap* values) {
    if (!value.IsObject()) {
        return Status::OK();
    }
    if (value.MemberCount() == 0) {
        return insert_empty_object_marker(path->build(), values);
    }
    for (auto it = value.MemberBegin(); it != value.MemberEnd(); ++it) {
        if (it->value.IsObject()) {
            path->append(std::string_view(it->name.GetString(), it->name.GetStringLength()), false);
            RETURN_IF_ERROR(collect_empty_object_markers(it->value, path, values));
            path->pop_back();
        }
    }
    return Status::OK();
}

static Status add_empty_object_markers_from_json(const std::string& json, const PathInData& prefix,
                                                 VariantMap* values) {
    if (json.find("{}") == std::string::npos) {
        return Status::OK();
    }
    rapidjson::Document document;
    document.Parse(json.data(), json.size());
    if (document.HasParseError()) {
        return Status::Corruption("Invalid Parquet VARIANT decoded JSON");
    }
    PathInDataBuilder path;
    path.append(prefix.get_parts(), false);
    return collect_empty_object_markers(document, &path, values);
}

static Status parse_json_to_variant_map(const std::string& json, const PathInData& prefix,
                                        VariantMap* values) {
    auto parsed_column = ColumnVariant::create(0, false);
    ParseConfig parse_config;
    StringRef json_ref(json.data(), json.size());
    RETURN_IF_CATCH_EXCEPTION(
            variant_util::parse_json_to_variant(*parsed_column, json_ref, nullptr, parse_config));
    Field parsed = (*parsed_column)[0];
    if (!parsed.is_null()) {
        auto& parsed_values = parsed.get<TYPE_VARIANT>();
        for (auto& [path, value] : parsed_values) {
            (*values)[append_path(prefix, path)] = std::move(value);
        }
    }
    RETURN_IF_ERROR(add_empty_object_markers_from_json(json, prefix, values));
    return Status::OK();
}

static Status variant_map_to_json(VariantMap values, std::string* json) {
    auto variant_column = ColumnVariant::create(0, false);
    RETURN_IF_CATCH_EXCEPTION(
            variant_column->insert(Field::create_field<TYPE_VARIANT>(std::move(values))));
    DataTypeSerDe::FormatOptions options;
    variant_column->serialize_one_row_to_string(0, json, options);
    return Status::OK();
}

static bool path_has_prefix(const PathInData& path, const PathInData& prefix) {
    const auto& parts = path.get_parts();
    const auto& prefix_parts = prefix.get_parts();
    if (parts.size() < prefix_parts.size()) {
        return false;
    }
    for (size_t i = 0; i < prefix_parts.size(); ++i) {
        if (parts[i] != prefix_parts[i]) {
            return false;
        }
    }
    return true;
}

static bool has_descendant_path(const VariantMap& values, const PathInData& prefix) {
    const size_t prefix_size = prefix.get_parts().size();
    return std::ranges::any_of(values, [&](const auto& entry) {
        const auto& path = entry.first;
        return path.get_parts().size() > prefix_size && path_has_prefix(path, prefix);
    });
}

static void erase_shadowed_empty_object_markers(VariantMap* values,
                                                const VariantMap& shadowing_values) {
    for (auto it = values->begin(); it != values->end();) {
        if (is_empty_object_marker(it->second) &&
            (has_descendant_path(*values, it->first) ||
             has_descendant_path(shadowing_values, it->first))) {
            it = values->erase(it);
            continue;
        }
        ++it;
    }
}

static void erase_shadowed_empty_object_markers(VariantMap* value_values,
                                                VariantMap* typed_values) {
    erase_shadowed_empty_object_markers(value_values, *typed_values);
    erase_shadowed_empty_object_markers(typed_values, *value_values);
}

static Status check_no_shredded_value_typed_duplicates(const VariantMap& value_values,
                                                       const VariantMap& typed_values,
                                                       const PathInData& prefix) {
    const size_t prefix_size = prefix.get_parts().size();
    for (const auto& value_entry : value_values) {
        const auto& value_path = value_entry.first;
        if (!path_has_prefix(value_path, prefix)) {
            continue;
        }
        if (value_path.get_parts().size() == prefix_size) {
            if (is_empty_object_marker(value_entry.second) &&
                !has_descendant_path(typed_values, value_path)) {
                continue;
            }
            if (!typed_values.empty()) {
                return Status::Corruption(
                        "Parquet VARIANT residual value conflicts with typed_value at path {}",
                        value_path.get_path());
            }
            continue;
        }
        for (const auto& typed_entry : typed_values) {
            const auto& typed_path = typed_entry.first;
            if (!path_has_prefix(typed_path, prefix)) {
                continue;
            }
            if (typed_path.get_parts().size() == prefix_size) {
                if (is_empty_object_marker(typed_entry.second) &&
                    !has_descendant_path(value_values, typed_path)) {
                    continue;
                }
                return Status::Corruption(
                        "Parquet VARIANT residual value and typed_value contain duplicate field {}",
                        value_path.get_parts()[prefix_size].key);
            }
            if (value_path.get_parts()[prefix_size] == typed_path.get_parts()[prefix_size]) {
                if (value_path == typed_path && is_empty_object_marker(value_entry.second) &&
                    is_empty_object_marker(typed_entry.second)) {
                    continue;
                }
                return Status::Corruption(
                        "Parquet VARIANT residual value and typed_value contain duplicate field {}",
                        value_path.get_parts()[prefix_size].key);
            }
        }
    }
    return Status::OK();
}

static bool has_direct_typed_parent_null(const std::vector<const NullMap*>& null_maps, size_t row) {
    return std::ranges::any_of(null_maps, [&](const NullMap* null_map) {
        DCHECK_LT(row, null_map->size());
        return (*null_map)[row];
    });
}

static void insert_direct_typed_leaf_range(const IColumn& column, size_t start, size_t rows,
                                           const std::vector<const NullMap*>& parent_null_maps,
                                           IColumn* variant_leaf) {
    auto& nullable_leaf = assert_cast<ColumnNullable&>(*variant_leaf);
    const IColumn* value_column = &column;
    const NullMap* leaf_null_map = nullptr;
    if (const auto* nullable_column = check_and_get_column<ColumnNullable>(&column)) {
        value_column = &nullable_column->get_nested_column();
        leaf_null_map = &nullable_column->get_null_map_data();
    }

    nullable_leaf.get_nested_column().insert_range_from(*value_column, start, rows);
    auto& null_map = nullable_leaf.get_null_map_data();
    null_map.reserve(null_map.size() + rows);
    for (size_t i = 0; i < rows; ++i) {
        const size_t row = start + i;
        const bool leaf_is_null = leaf_null_map != nullptr && (*leaf_null_map)[row];
        null_map.push_back(leaf_is_null || has_direct_typed_parent_null(parent_null_maps, row));
    }
}

static bool is_temporal_variant_leaf_type(PrimitiveType type) {
    switch (type) {
    case TYPE_TIMEV2:
    case TYPE_DATE:
    case TYPE_DATETIME:
    case TYPE_DATEV2:
    case TYPE_DATETIMEV2:
    case TYPE_TIMESTAMPTZ:
        return true;
    default:
        return false;
    }
}

static bool is_floating_point_variant_leaf_type(PrimitiveType type) {
    switch (type) {
    case TYPE_FLOAT:
    case TYPE_DOUBLE:
        return true;
    default:
        return false;
    }
}

static bool is_uuid_typed_value_field(const FieldSchema& field_schema);
static bool contains_uuid_typed_value_field(const FieldSchema& field_schema);

static DataTypePtr direct_variant_leaf_type(const DataTypePtr& data_type) {
    const auto& type = remove_nullable(data_type);
    if (is_temporal_variant_leaf_type(type->get_primitive_type())) {
        return std::make_shared<DataTypeInt64>();
    }
    return type;
}

static DataTypePtr direct_variant_leaf_type(const FieldSchema& field_schema) {
    const auto& type = remove_nullable(field_schema.data_type);
    if (is_uuid_typed_value_field(field_schema)) {
        return std::make_shared<DataTypeString>();
    }
    if (type->get_primitive_type() == TYPE_ARRAY) {
        DORIS_CHECK(!field_schema.children.empty());
        DataTypePtr nested_type = direct_variant_leaf_type(field_schema.children[0]);
        if (field_schema.children[0].data_type->is_nullable()) {
            nested_type = make_nullable(nested_type);
        }
        return std::make_shared<DataTypeArray>(nested_type);
    }
    return direct_variant_leaf_type(field_schema.data_type);
}

static bool contains_temporal_variant_leaf_type(const DataTypePtr& data_type) {
    const auto& type = remove_nullable(data_type);
    if (is_temporal_variant_leaf_type(type->get_primitive_type())) {
        return true;
    }
    if (type->get_primitive_type() == TYPE_ARRAY) {
        return contains_temporal_variant_leaf_type(
                assert_cast<const DataTypeArray*>(type.get())->get_nested_type());
    }
    return false;
}

static bool contains_floating_point_variant_leaf_type(const DataTypePtr& data_type) {
    const auto& type = remove_nullable(data_type);
    if (is_floating_point_variant_leaf_type(type->get_primitive_type())) {
        return true;
    }
    if (type->get_primitive_type() == TYPE_ARRAY) {
        return contains_floating_point_variant_leaf_type(
                assert_cast<const DataTypeArray*>(type.get())->get_nested_type());
    }
    return false;
}

static int64_t direct_temporal_variant_value(PrimitiveType type, const IColumn& column,
                                             size_t row) {
    switch (type) {
    case TYPE_TIMEV2:
        return static_cast<int64_t>(
                std::llround(assert_cast<const ColumnTimeV2&>(column).get_data()[row]));
    case TYPE_DATE:
        return variant_date_value(assert_cast<const ColumnDate&>(column).get_data()[row]);
    case TYPE_DATETIME:
        return variant_datetime_value(assert_cast<const ColumnDateTime&>(column).get_data()[row]);
    case TYPE_DATEV2:
        return variant_date_value(assert_cast<const ColumnDateV2&>(column).get_data()[row]);
    case TYPE_DATETIMEV2:
        return variant_datetime_value(assert_cast<const ColumnDateTimeV2&>(column).get_data()[row]);
    case TYPE_TIMESTAMPTZ:
        return variant_datetime_value(
                assert_cast<const ColumnTimeStampTz&>(column).get_data()[row]);
    default:
        DORIS_CHECK(false);
        return 0;
    }
}

static void insert_direct_typed_temporal_leaf_range(
        PrimitiveType type, const IColumn& column, size_t start, size_t rows,
        const std::vector<const NullMap*>& parent_null_maps, IColumn* variant_leaf) {
    auto& nullable_leaf = assert_cast<ColumnNullable&>(*variant_leaf);
    const IColumn* value_column = &column;
    const NullMap* leaf_null_map = nullptr;
    if (const auto* nullable_column = check_and_get_column<ColumnNullable>(&column)) {
        value_column = &nullable_column->get_nested_column();
        leaf_null_map = &nullable_column->get_null_map_data();
    }

    auto& data = assert_cast<ColumnInt64&>(nullable_leaf.get_nested_column()).get_data();
    data.reserve(data.size() + rows);
    auto& null_map = nullable_leaf.get_null_map_data();
    null_map.reserve(null_map.size() + rows);
    for (size_t i = 0; i < rows; ++i) {
        const size_t row = start + i;
        const bool leaf_is_null = leaf_null_map != nullptr && (*leaf_null_map)[row];
        const bool is_null = leaf_is_null || has_direct_typed_parent_null(parent_null_maps, row);
        if (is_null) {
            data.push_back(0);
            null_map.push_back(1);
            continue;
        }
        data.push_back(direct_temporal_variant_value(type, *value_column, row));
        null_map.push_back(0);
    }
}

static Status insert_direct_typed_uuid_leaf_range(
        const IColumn& column, size_t start, size_t rows,
        const std::vector<const NullMap*>& parent_null_maps, IColumn* variant_leaf) {
    auto& nullable_leaf = assert_cast<ColumnNullable&>(*variant_leaf);
    const IColumn* value_column = &column;
    const NullMap* leaf_null_map = nullptr;
    if (const auto* nullable_column = check_and_get_column<ColumnNullable>(&column)) {
        value_column = &nullable_column->get_nested_column();
        leaf_null_map = &nullable_column->get_null_map_data();
    }

    auto& data = assert_cast<ColumnString&>(nullable_leaf.get_nested_column());
    auto& null_map = nullable_leaf.get_null_map_data();
    null_map.reserve(null_map.size() + rows);
    for (size_t i = 0; i < rows; ++i) {
        const size_t row = start + i;
        const bool leaf_is_null = leaf_null_map != nullptr && (*leaf_null_map)[row];
        const bool is_null = leaf_is_null || has_direct_typed_parent_null(parent_null_maps, row);
        if (is_null) {
            data.insert_default();
            null_map.push_back(1);
            continue;
        }
        StringRef bytes = value_column->get_data_at(row);
        if (bytes.size != 16) {
            return Status::Corruption("Parquet VARIANT UUID typed_value has invalid length {}",
                                      bytes.size);
        }
        std::string uuid =
                parquet::format_variant_uuid(reinterpret_cast<const uint8_t*>(bytes.data));
        data.insert_data(uuid.data(), uuid.size());
        null_map.push_back(0);
    }
    return Status::OK();
}

static void append_json_string(std::string_view value, std::string* json) {
    auto column = ColumnString::create();
    VectorBufferWriter writer(*column);
    writer.write_json_string(value);
    writer.commit();
    json->append(column->get_data_at(0).data, column->get_data_at(0).size);
}

static bool is_column_selected(const FieldSchema& field_schema,
                               const std::set<uint64_t>& column_ids) {
    return column_ids.empty() || column_ids.find(field_schema.get_column_id()) != column_ids.end();
}

static bool has_selected_column(const FieldSchema& field_schema,
                                const std::set<uint64_t>& column_ids) {
    if (is_column_selected(field_schema, column_ids)) {
        return true;
    }
    return std::any_of(field_schema.children.begin(), field_schema.children.end(),
                       [&column_ids](const FieldSchema& child) {
                           return has_selected_column(child, column_ids);
                       });
}

static bool is_direct_variant_leaf_type(const DataTypePtr& data_type) {
    const auto& type = remove_nullable(data_type);
    switch (type->get_primitive_type()) {
    case TYPE_BOOLEAN:
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
    case TYPE_LARGEINT:
    case TYPE_DECIMALV2:
    case TYPE_DECIMAL32:
    case TYPE_DECIMAL64:
    case TYPE_DECIMAL128I:
    case TYPE_DECIMAL256:
    case TYPE_FLOAT:
    case TYPE_DOUBLE:
    case TYPE_STRING:
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_VARBINARY:
        return true;
    case TYPE_TIMEV2:
    case TYPE_DATE:
    case TYPE_DATETIME:
    case TYPE_DATEV2:
    case TYPE_DATETIMEV2:
    case TYPE_TIMESTAMPTZ:
        return true;
    case TYPE_ARRAY: {
        const auto* array_type = assert_cast<const DataTypeArray*>(type.get());
        return is_direct_variant_leaf_type(array_type->get_nested_type());
    }
    default:
        return false;
    }
}

static bool can_direct_read_typed_value(const FieldSchema& field_schema, bool allow_variant_wrapper,
                                        const std::set<uint64_t>& column_ids) {
    if (!has_selected_column(field_schema, column_ids)) {
        return true;
    }
    if (allow_variant_wrapper && is_variant_wrapper_field(field_schema, false)) {
        const int value_idx = find_child_idx(field_schema, "value");
        const int typed_value_idx = find_child_idx(field_schema, "typed_value");
        return (value_idx < 0 ||
                !has_selected_column(field_schema.children[value_idx], column_ids)) &&
               typed_value_idx >= 0 &&
               can_direct_read_typed_value(field_schema.children[typed_value_idx], false,
                                           column_ids);
    }

    const auto& type = remove_nullable(field_schema.data_type);
    if (type->get_primitive_type() == TYPE_STRUCT) {
        return std::all_of(field_schema.children.begin(), field_schema.children.end(),
                           [&column_ids](const FieldSchema& child) {
                               return can_direct_read_typed_value(child, true, column_ids);
                           });
    }
    return is_direct_variant_leaf_type(field_schema.data_type);
}

static bool has_selected_direct_typed_leaf(const FieldSchema& field_schema,
                                           bool allow_variant_wrapper,
                                           const std::set<uint64_t>& column_ids) {
    if (!has_selected_column(field_schema, column_ids)) {
        return false;
    }
    if (allow_variant_wrapper && is_variant_wrapper_field(field_schema, false)) {
        const int typed_value_idx = find_child_idx(field_schema, "typed_value");
        DCHECK_GE(typed_value_idx, 0);
        return has_selected_direct_typed_leaf(field_schema.children[typed_value_idx], false,
                                              column_ids);
    }

    const auto& type = remove_nullable(field_schema.data_type);
    if (type->get_primitive_type() == TYPE_STRUCT) {
        return std::any_of(field_schema.children.begin(), field_schema.children.end(),
                           [&column_ids](const FieldSchema& child) {
                               return has_selected_direct_typed_leaf(child, true, column_ids);
                           });
    }
    return is_direct_variant_leaf_type(field_schema.data_type);
}

static bool can_use_direct_typed_only_value(const FieldSchema& variant_field,
                                            const std::set<uint64_t>& column_ids) {
    const int value_idx = find_child_idx(variant_field, "value");
    const int typed_value_idx = find_child_idx(variant_field, "typed_value");
    return (value_idx < 0 || !has_selected_column(variant_field.children[value_idx], column_ids)) &&
           typed_value_idx >= 0 &&
           has_selected_direct_typed_leaf(variant_field.children[typed_value_idx], false,
                                          column_ids) &&
           can_direct_read_typed_value(variant_field.children[typed_value_idx], false, column_ids);
}

static DataTypePtr make_variant_struct_reader_type(const FieldSchema& field) {
    DataTypes child_types;
    Strings child_names;
    child_types.reserve(field.children.size());
    child_names.reserve(field.children.size());
    for (const auto& child : field.children) {
        child_types.push_back(make_nullable(child.data_type));
        child_names.push_back(child.name);
    }
    return std::make_shared<DataTypeStruct>(child_types, child_names);
}

static ColumnPtr make_variant_struct_read_column(const FieldSchema& field,
                                                 const DataTypePtr& variant_struct_type) {
    if (field.data_type->is_nullable()) {
        return make_nullable(variant_struct_type)->create_column();
    }
    return variant_struct_type->create_column();
}

static void fill_variant_field_info(FieldWithDataType* value) {
    FieldInfo info;
    variant_util::get_field_info(value->field, &info);
    DCHECK_LE(info.num_dimensions, std::numeric_limits<uint8_t>::max());
    value->base_scalar_type_id = info.scalar_type_id;
    value->num_dimensions = static_cast<uint8_t>(info.num_dimensions);
}

static void fill_variant_leaf_type_info(const DataTypePtr& data_type, FieldWithDataType* value) {
    auto leaf_type = remove_nullable(data_type);
    size_t num_dimensions = 0;
    while (leaf_type->get_primitive_type() == TYPE_ARRAY) {
        ++num_dimensions;
        leaf_type = remove_nullable(
                assert_cast<const DataTypeArray*>(leaf_type.get())->get_nested_type());
    }
    DCHECK_LE(num_dimensions, std::numeric_limits<uint8_t>::max());
    if (value->base_scalar_type_id == INVALID_TYPE) {
        value->base_scalar_type_id = leaf_type->get_primitive_type();
    }
    if (value->num_dimensions == 0 && num_dimensions > 0) {
        value->num_dimensions = static_cast<uint8_t>(num_dimensions);
    }
    if (is_decimal(leaf_type->get_primitive_type())) {
        value->precision = leaf_type->get_precision();
        value->scale = leaf_type->get_scale();
    }
}

static Status fill_floating_point_variant_field(const Field& field, FieldWithDataType* value) {
    value->field = field;
    fill_variant_field_info(value);
    return Status::OK();
}

static Status fill_floating_point_variant_field(PrimitiveType type, const Field& field,
                                                FieldWithDataType* value) {
    DORIS_CHECK(type == TYPE_FLOAT || type == TYPE_DOUBLE);
    return fill_floating_point_variant_field(field, value);
}

static bool is_uuid_typed_value_field(const FieldSchema& field_schema) {
    return field_schema.parquet_schema.__isset.logicalType &&
           field_schema.parquet_schema.logicalType.__isset.UUID;
}

static bool contains_uuid_typed_value_field(const FieldSchema& field_schema) {
    return is_uuid_typed_value_field(field_schema) ||
           std::any_of(
                   field_schema.children.begin(), field_schema.children.end(),
                   [](const FieldSchema& child) { return contains_uuid_typed_value_field(child); });
}

static Status uuid_field_to_string(const Field& field, std::string* uuid) {
    StringRef bytes;
    switch (field.get_type()) {
    case TYPE_STRING:
        bytes = StringRef(field.get<TYPE_STRING>());
        break;
    case TYPE_CHAR:
        bytes = StringRef(field.get<TYPE_CHAR>());
        break;
    case TYPE_VARCHAR:
        bytes = StringRef(field.get<TYPE_VARCHAR>());
        break;
    case TYPE_VARBINARY:
        bytes = field.get<TYPE_VARBINARY>().to_string_ref();
        break;
    default:
        return Status::Corruption("Parquet VARIANT UUID typed_value has unexpected Doris type {}",
                                  field.get_type_name());
    }
    if (bytes.size != 16) {
        return Status::Corruption("Parquet VARIANT UUID typed_value has invalid length {}",
                                  bytes.size);
    }
    *uuid = parquet::format_variant_uuid(reinterpret_cast<const uint8_t*>(bytes.data));
    return Status::OK();
}

static Status fill_uuid_variant_field(const Field& field, FieldWithDataType* value) {
    std::string uuid;
    RETURN_IF_ERROR(uuid_field_to_string(field, &uuid));
    value->field = Field::create_field<TYPE_STRING>(std::move(uuid));
    value->base_scalar_type_id = TYPE_STRING;
    return Status::OK();
}

static Status fill_temporal_variant_field(PrimitiveType type, const Field& field,
                                          FieldWithDataType* value) {
    switch (type) {
    case TYPE_TIMEV2:
        value->field = Field::create_field<TYPE_BIGINT>(
                static_cast<int64_t>(std::llround(field.get<TYPE_TIMEV2>())));
        value->base_scalar_type_id = TYPE_BIGINT;
        return Status::OK();
    case TYPE_DATE:
        value->field = Field::create_field<TYPE_BIGINT>(variant_date_value(field.get<TYPE_DATE>()));
        value->base_scalar_type_id = TYPE_BIGINT;
        return Status::OK();
    case TYPE_DATETIME:
        value->field = Field::create_field<TYPE_BIGINT>(
                variant_datetime_value(field.get<TYPE_DATETIME>()));
        value->base_scalar_type_id = TYPE_BIGINT;
        return Status::OK();
    case TYPE_DATEV2:
        value->field =
                Field::create_field<TYPE_BIGINT>(variant_date_value(field.get<TYPE_DATEV2>()));
        value->base_scalar_type_id = TYPE_BIGINT;
        return Status::OK();
    case TYPE_DATETIMEV2:
        value->field = Field::create_field<TYPE_BIGINT>(
                variant_datetime_value(field.get<TYPE_DATETIMEV2>()));
        value->base_scalar_type_id = TYPE_BIGINT;
        return Status::OK();
    case TYPE_TIMESTAMPTZ:
        value->field = Field::create_field<TYPE_BIGINT>(
                variant_datetime_value(field.get<TYPE_TIMESTAMPTZ>()));
        value->base_scalar_type_id = TYPE_BIGINT;
        return Status::OK();
    default:
        DORIS_CHECK(false);
        return Status::OK();
    }
}

static uint8_t direct_array_dimensions(const DataTypePtr& data_type) {
    uint8_t num_dimensions = 0;
    auto type = remove_nullable(data_type);
    while (type->get_primitive_type() == TYPE_ARRAY) {
        ++num_dimensions;
        type = remove_nullable(assert_cast<const DataTypeArray*>(type.get())->get_nested_type());
    }
    return num_dimensions;
}

static PrimitiveType direct_array_base_scalar_type(const FieldSchema& field_schema) {
    auto leaf_type = remove_nullable(direct_variant_leaf_type(field_schema));
    while (leaf_type->get_primitive_type() == TYPE_ARRAY) {
        leaf_type = remove_nullable(
                assert_cast<const DataTypeArray*>(leaf_type.get())->get_nested_type());
    }
    return leaf_type->get_primitive_type();
}

static Status convert_direct_array_value(const FieldSchema& field_schema, const Field& field,
                                         Field* converted) {
    if (field.is_null()) {
        *converted = Field();
        return Status::OK();
    }

    const auto& type = remove_nullable(field_schema.data_type);
    if (type->get_primitive_type() == TYPE_ARRAY) {
        if (field_schema.children.empty()) {
            return Status::Corruption("Parquet VARIANT array typed_value has no element schema");
        }
        Array converted_elements;
        const auto& elements = field.get<TYPE_ARRAY>();
        converted_elements.reserve(elements.size());
        for (const auto& element : elements) {
            Field converted_element;
            RETURN_IF_ERROR(convert_direct_array_value(field_schema.children[0], element,
                                                       &converted_element));
            converted_elements.push_back(std::move(converted_element));
        }
        *converted = Field::create_field<TYPE_ARRAY>(std::move(converted_elements));
        return Status::OK();
    }

    if (is_uuid_typed_value_field(field_schema)) {
        FieldWithDataType value;
        RETURN_IF_ERROR(fill_uuid_variant_field(field, &value));
        *converted = std::move(value.field);
        return Status::OK();
    }
    if (is_temporal_variant_leaf_type(type->get_primitive_type())) {
        FieldWithDataType value;
        RETURN_IF_ERROR(fill_temporal_variant_field(type->get_primitive_type(), field, &value));
        *converted = std::move(value.field);
        return Status::OK();
    }
    if (is_floating_point_variant_leaf_type(type->get_primitive_type())) {
        FieldWithDataType value;
        RETURN_IF_ERROR(
                fill_floating_point_variant_field(type->get_primitive_type(), field, &value));
        *converted = std::move(value.field);
        return Status::OK();
    }

    *converted = field;
    return Status::OK();
}

static Status insert_direct_typed_array_leaf_range(
        const FieldSchema& field_schema, const IColumn& column, size_t start, size_t rows,
        const std::vector<const NullMap*>& parent_null_maps, IColumn* variant_leaf) {
    auto& nullable_leaf = assert_cast<ColumnNullable&>(*variant_leaf);
    const IColumn* value_column = &column;
    const NullMap* leaf_null_map = nullptr;
    if (const auto* nullable_column = check_and_get_column<ColumnNullable>(&column)) {
        value_column = &nullable_column->get_nested_column();
        leaf_null_map = &nullable_column->get_null_map_data();
    }

    auto& data = nullable_leaf.get_nested_column();
    auto& null_map = nullable_leaf.get_null_map_data();
    null_map.reserve(null_map.size() + rows);
    for (size_t i = 0; i < rows; ++i) {
        const size_t row = start + i;
        const bool leaf_is_null = leaf_null_map != nullptr && (*leaf_null_map)[row];
        const bool is_null = leaf_is_null || has_direct_typed_parent_null(parent_null_maps, row);
        if (is_null) {
            data.insert_default();
            null_map.push_back(1);
            continue;
        }

        Field field;
        value_column->get(row, field);
        Field converted;
        RETURN_IF_ERROR(convert_direct_array_value(field_schema, field, &converted));
        data.insert(converted);
        null_map.push_back(0);
    }
    return Status::OK();
}

static Status fill_direct_array_variant_field(const FieldSchema& field_schema, const Field& field,
                                              FieldWithDataType* value, bool* present) {
    if (field.is_null()) {
        *present = false;
        return Status::OK();
    }
    *present = true;
    RETURN_IF_ERROR(convert_direct_array_value(field_schema, field, &value->field));
    value->base_scalar_type_id = direct_array_base_scalar_type(field_schema);
    value->num_dimensions = direct_array_dimensions(field_schema.data_type);
    return Status::OK();
}

static Status field_to_variant_field(const FieldSchema& field_schema, const Field& field,
                                     FieldWithDataType* value, bool* present) {
    if (field.is_null()) {
        *present = false;
        return Status::OK();
    }
    *present = true;
    if (is_uuid_typed_value_field(field_schema)) {
        return fill_uuid_variant_field(field, value);
    }
    const DataTypePtr& type = remove_nullable(field_schema.data_type);
    if (is_temporal_variant_leaf_type(type->get_primitive_type())) {
        return fill_temporal_variant_field(type->get_primitive_type(), field, value);
    }
    switch (type->get_primitive_type()) {
    case TYPE_BOOLEAN:
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
    case TYPE_LARGEINT:
    case TYPE_DECIMALV2:
    case TYPE_DECIMAL32:
    case TYPE_DECIMAL64:
    case TYPE_DECIMAL128I:
    case TYPE_DECIMAL256:
    case TYPE_STRING:
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_VARBINARY:
    case TYPE_ARRAY:
        value->field = field;
        fill_variant_field_info(value);
        fill_variant_leaf_type_info(type, value);
        return Status::OK();
    case TYPE_FLOAT:
    case TYPE_DOUBLE:
        return fill_floating_point_variant_field(field, value);
    default:
        return Status::Corruption("Unsupported Parquet VARIANT typed_value Doris type {}",
                                  type->get_name());
    }
}

static Status typed_value_to_json(const FieldSchema& typed_value_field, const Field& field,
                                  const std::string& metadata, std::string* json, bool* present);
static Status typed_map_to_variant_map(const FieldSchema& typed_value_field, const Field& field,
                                       const std::string& metadata, PathInDataBuilder* path,
                                       VariantMap* values, bool* present,
                                       std::deque<std::string>* string_values);

static Status serialize_field_to_json(const DataTypePtr& data_type, const Field& field,
                                      std::string* json) {
    MutableColumnPtr column = data_type->create_column();
    column->insert(field);

    auto json_column = ColumnString::create();
    VectorBufferWriter writer(*json_column);
    auto serde = data_type->get_serde();
    DataTypeSerDe::FormatOptions options;
    RETURN_IF_ERROR(serde->serialize_one_cell_to_json(*column, 0, writer, options));
    writer.commit();
    *json = json_column->get_data_at(0).to_string();
    return Status::OK();
}

static Status scalar_typed_value_to_json(const FieldSchema& field_schema, const Field& field,
                                         std::string* json, bool* present) {
    FieldWithDataType value;
    RETURN_IF_ERROR(field_to_variant_field(field_schema, field, &value, present));
    if (!*present) {
        return Status::OK();
    }
    if (value.field.is_null()) {
        *json = "null";
        return Status::OK();
    }
    if (!is_uuid_typed_value_field(field_schema) &&
        remove_nullable(field_schema.data_type)->get_primitive_type() == TYPE_VARBINARY) {
        return Status::NotSupported(
                "Parquet VARIANT binary typed_value cannot be serialized to JSON");
    }

    DataTypePtr json_type;
    if (value.base_scalar_type_id != PrimitiveType::INVALID_TYPE) {
        json_type = DataTypeFactory::instance().create_data_type(value.base_scalar_type_id, false,
                                                                 value.precision, value.scale);
    } else {
        json_type = remove_nullable(field_schema.data_type);
    }
    return serialize_field_to_json(json_type, value.field, json);
}

static Status resolve_variant_metadata(const FieldSchema& variant_field, const Struct& fields,
                                       const std::string* inherited_metadata, std::string* metadata,
                                       bool* has_metadata) {
    *has_metadata = false;
    if (inherited_metadata != nullptr) {
        *metadata = *inherited_metadata;
        *has_metadata = true;
    }

    const int metadata_idx = find_child_idx(variant_field, "metadata");
    if (metadata_idx >= 0) {
        bool metadata_present = false;
        RETURN_IF_ERROR(get_binary_field(fields[metadata_idx], metadata, &metadata_present));
        *has_metadata = metadata_present;
    }
    return Status::OK();
}

static Status variant_typed_value_to_json(const FieldSchema& variant_field, const Struct& fields,
                                          const std::string& metadata, std::string* typed_json,
                                          bool* typed_present) {
    *typed_present = false;
    const int typed_value_idx = find_child_idx(variant_field, "typed_value");
    if (typed_value_idx < 0) {
        return Status::OK();
    }
    return typed_value_to_json(variant_field.children[typed_value_idx], fields[typed_value_idx],
                               metadata, typed_json, typed_present);
}

static Status variant_residual_value_to_json(const FieldSchema& variant_field, const Struct& fields,
                                             const std::string& metadata, bool has_metadata,
                                             std::string* value_json, bool* value_present) {
    *value_present = false;
    const int value_idx = find_child_idx(variant_field, "value");
    if (value_idx < 0) {
        return Status::OK();
    }

    std::string value;
    RETURN_IF_ERROR(get_binary_field(fields[value_idx], &value, value_present));
    if (!*value_present) {
        return Status::OK();
    }
    if (!has_metadata) {
        return Status::Corruption("Parquet VARIANT value is present without metadata");
    }
    return parquet::decode_variant_to_json(StringRef(metadata.data(), metadata.size()),
                                           StringRef(value.data(), value.size()), value_json);
}

static Status merge_variant_value_and_typed_json(const std::string& value_json,
                                                 const std::string& typed_json, std::string* json) {
    VariantMap value_values;
    RETURN_IF_ERROR(parse_json_to_variant_map(value_json, PathInData(), &value_values));
    VariantMap typed_values;
    RETURN_IF_ERROR(parse_json_to_variant_map(typed_json, PathInData(), &typed_values));
    erase_shadowed_empty_object_markers(&value_values, &typed_values);
    auto root_value = value_values.find(PathInData());
    if (root_value != value_values.end() && !is_empty_object_marker(root_value->second)) {
        return Status::Corruption(
                "Parquet VARIANT has conflicting non-object value and typed_value");
    }
    RETURN_IF_ERROR(
            check_no_shredded_value_typed_duplicates(value_values, typed_values, PathInData()));
    value_values.merge(std::move(typed_values));
    return variant_map_to_json(std::move(value_values), json);
}

static Status variant_to_json(const FieldSchema& variant_field, const Field& field,
                              const std::string* inherited_metadata, std::string* json,
                              bool* present) {
    if (field.is_null()) {
        *present = false;
        return Status::OK();
    }

    const auto& fields = field.get<TYPE_STRUCT>();
    std::string metadata;
    bool has_metadata = false;
    RETURN_IF_ERROR(resolve_variant_metadata(variant_field, fields, inherited_metadata, &metadata,
                                             &has_metadata));

    std::string typed_json;
    bool typed_present = false;
    RETURN_IF_ERROR(variant_typed_value_to_json(variant_field, fields, metadata, &typed_json,
                                                &typed_present));

    std::string value_json;
    bool value_present = false;
    RETURN_IF_ERROR(variant_residual_value_to_json(variant_field, fields, metadata, has_metadata,
                                                   &value_json, &value_present));

    if (value_present && typed_present) {
        RETURN_IF_ERROR(merge_variant_value_and_typed_json(value_json, typed_json, json));
        *present = true;
        return Status::OK();
    }

    if (typed_present) {
        *json = std::move(typed_json);
        *present = true;
        return Status::OK();
    }
    if (value_present) {
        *json = std::move(value_json);
        *present = true;
        return Status::OK();
    }

    *present = false;
    return Status::OK();
}

static Status shredded_field_to_json(const FieldSchema& field_schema, const Field& field,
                                     const std::string& metadata, std::string* json, bool* present,
                                     bool allow_scalar_typed_value_only_wrapper) {
    if (is_variant_wrapper_field(field_schema, allow_scalar_typed_value_only_wrapper)) {
        return variant_to_json(field_schema, field, &metadata, json, present);
    }
    if (is_value_only_variant_wrapper_candidate(field_schema)) {
        Status st = variant_to_json(field_schema, field, &metadata, json, present);
        if (st.ok()) {
            return st;
        }
        if (!st.is<ErrorCode::CORRUPTION>()) {
            return st;
        }
    }
    return typed_value_to_json(field_schema, field, metadata, json, present);
}

static Status typed_array_to_json(const FieldSchema& typed_value_field, const Field& field,
                                  const std::string& metadata, std::string* json, bool* present) {
    if (field.is_null()) {
        *present = false;
        return Status::OK();
    }
    if (typed_value_field.children.empty()) {
        return Status::Corruption("Parquet VARIANT array typed_value has no element schema");
    }

    const auto& elements = field.get<TYPE_ARRAY>();
    const auto& element_schema = typed_value_field.children[0];
    json->clear();
    json->push_back('[');
    for (size_t i = 0; i < elements.size(); ++i) {
        if (i != 0) {
            json->push_back(',');
        }
        std::string element_json;
        bool element_present = false;
        RETURN_IF_ERROR(shredded_field_to_json(element_schema, elements[i], metadata, &element_json,
                                               &element_present, true));
        if (!element_present) {
            if (elements[i].is_null()) {
                json->append("null");
                continue;
            }
            return Status::Corruption("Parquet VARIANT array element is missing");
        }
        json->append(element_json);
    }
    json->push_back(']');
    *present = true;
    return Status::OK();
}

static Status typed_struct_to_json(const FieldSchema& typed_value_field, const Field& field,
                                   const std::string& metadata, std::string* json, bool* present) {
    if (field.is_null()) {
        *present = false;
        return Status::OK();
    }

    const auto& fields = field.get<TYPE_STRUCT>();
    json->clear();
    json->push_back('{');
    bool first = true;
    for (int i = 0; i < typed_value_field.children.size(); ++i) {
        std::string child_json;
        bool child_present = false;
        RETURN_IF_ERROR(shredded_field_to_json(typed_value_field.children[i], fields[i], metadata,
                                               &child_json, &child_present, false));
        if (!child_present) {
            continue;
        }
        if (!first) {
            json->push_back(',');
        }
        append_json_string(typed_value_field.children[i].name, json);
        json->push_back(':');
        json->append(child_json);
        first = false;
    }
    json->push_back('}');
    *present = true;
    return Status::OK();
}

static Status typed_value_to_json(const FieldSchema& typed_value_field, const Field& field,
                                  const std::string& metadata, std::string* json, bool* present) {
    const DataTypePtr& typed_type = remove_nullable(typed_value_field.data_type);
    switch (typed_type->get_primitive_type()) {
    case TYPE_STRUCT:
        return typed_struct_to_json(typed_value_field, field, metadata, json, present);
    case TYPE_ARRAY:
        return typed_array_to_json(typed_value_field, field, metadata, json, present);
    case TYPE_MAP: {
        VariantMap values;
        PathInDataBuilder path;
        std::deque<std::string> string_values;
        RETURN_IF_ERROR(typed_map_to_variant_map(typed_value_field, field, metadata, &path, &values,
                                                 present, &string_values));
        if (!*present) {
            return Status::OK();
        }
        return variant_map_to_json(std::move(values), json);
    }
    default:
        return scalar_typed_value_to_json(typed_value_field, field, json, present);
    }
}

static Status typed_value_to_variant_map(const FieldSchema& typed_value_field, const Field& field,
                                         const std::string& metadata, PathInDataBuilder* path,
                                         VariantMap* values, bool* present,
                                         std::deque<std::string>* string_values);

static Status variant_to_variant_map(const FieldSchema& variant_field, const Field& field,
                                     const std::string* inherited_metadata, PathInDataBuilder* path,
                                     VariantMap* values, bool* present,
                                     std::deque<std::string>* string_values) {
    if (field.is_null()) {
        *present = false;
        return Status::OK();
    }
    const auto& fields = field.get<TYPE_STRUCT>();
    const int metadata_idx = find_child_idx(variant_field, "metadata");
    const int value_idx = find_child_idx(variant_field, "value");
    const int typed_value_idx = find_child_idx(variant_field, "typed_value");

    std::string metadata;
    bool has_metadata = false;
    if (inherited_metadata != nullptr) {
        metadata = *inherited_metadata;
        has_metadata = true;
    }
    if (metadata_idx >= 0) {
        bool metadata_present = false;
        RETURN_IF_ERROR(get_binary_field(fields[metadata_idx], &metadata, &metadata_present));
        has_metadata = metadata_present;
    }

    VariantMap value_values;
    bool value_present = false;
    const PathInData current_path = path->build();
    if (value_idx >= 0) {
        std::string value;
        RETURN_IF_ERROR(get_binary_field(fields[value_idx], &value, &value_present));
        if (value_present) {
            if (!has_metadata) {
                return Status::Corruption("Parquet VARIANT value is present without metadata");
            }
            RETURN_IF_ERROR(parquet::decode_variant_to_variant_map(
                    StringRef(metadata.data(), metadata.size()),
                    StringRef(value.data(), value.size()), current_path, &value_values,
                    string_values));
        }
    }

    VariantMap typed_values;
    bool typed_present = false;
    if (typed_value_idx >= 0) {
        RETURN_IF_ERROR(typed_value_to_variant_map(variant_field.children[typed_value_idx],
                                                   fields[typed_value_idx], metadata, path,
                                                   &typed_values, &typed_present, string_values));
    }

    erase_shadowed_empty_object_markers(&value_values, &typed_values);
    auto current_value = value_values.find(current_path);
    if (value_present && typed_present && current_value != value_values.end() &&
        !is_empty_object_marker(current_value->second)) {
        return Status::Corruption(
                "Parquet VARIANT has conflicting non-object value and typed_value");
    }
    RETURN_IF_ERROR(
            check_no_shredded_value_typed_duplicates(value_values, typed_values, current_path));
    values->merge(std::move(value_values));
    values->merge(std::move(typed_values));
    *present = value_present || typed_present;
    return Status::OK();
}

static Status shredded_field_to_variant_map(const FieldSchema& field_schema, const Field& field,
                                            const std::string& metadata, PathInDataBuilder* path,
                                            VariantMap* values, bool* present,
                                            std::deque<std::string>* string_values) {
    if (is_variant_wrapper_field(field_schema, false)) {
        return variant_to_variant_map(field_schema, field, &metadata, path, values, present,
                                      string_values);
    }
    if (is_value_only_variant_wrapper_candidate(field_schema)) {
        Status st = variant_to_variant_map(field_schema, field, &metadata, path, values, present,
                                           string_values);
        if (st.ok()) {
            return st;
        }
        if (!st.is<ErrorCode::CORRUPTION>()) {
            return st;
        }
    }
    return typed_value_to_variant_map(field_schema, field, metadata, path, values, present,
                                      string_values);
}

static Status append_typed_field_to_variant_map(const FieldSchema& typed_value_field,
                                                const Field& field, PathInDataBuilder* path,
                                                VariantMap* values, bool* present) {
    FieldWithDataType value;
    RETURN_IF_ERROR(field_to_variant_field(typed_value_field, field, &value, present));
    if (*present) {
        (*values)[path->build()] = std::move(value);
    }
    return Status::OK();
}

static void move_variant_map_to_field(VariantMap&& element_values, FieldWithDataType* value) {
    if (element_values.size() == 1 && element_values.begin()->first.empty()) {
        *value = std::move(element_values.begin()->second);
        return;
    }
    value->field = Field::create_field<TYPE_VARIANT>(std::move(element_values));
    fill_variant_field_info(value);
}

static Status typed_array_to_variant_map(const FieldSchema& typed_value_field, const Field& field,
                                         const std::string& metadata, PathInDataBuilder* path,
                                         VariantMap* values, bool* present,
                                         std::deque<std::string>* string_values) {
    if ((contains_uuid_typed_value_field(typed_value_field) ||
         contains_temporal_variant_leaf_type(typed_value_field.data_type) ||
         contains_floating_point_variant_leaf_type(typed_value_field.data_type)) &&
        is_direct_variant_leaf_type(typed_value_field.data_type)) {
        FieldWithDataType value;
        RETURN_IF_ERROR(fill_direct_array_variant_field(typed_value_field, field, &value, present));
        if (*present) {
            (*values)[path->build()] = std::move(value);
        }
        return Status::OK();
    }
    if (is_direct_variant_leaf_type(typed_value_field.data_type)) {
        return append_typed_field_to_variant_map(typed_value_field, field, path, values, present);
    }

    if (field.is_null()) {
        *present = false;
        return Status::OK();
    }
    if (typed_value_field.children.empty()) {
        return Status::Corruption("Parquet VARIANT array typed_value has no element schema");
    }

    const auto& elements = field.get<TYPE_ARRAY>();
    const auto& element_schema = typed_value_field.children[0];
    Array array;
    array.reserve(elements.size());
    for (const auto& element : elements) {
        VariantMap element_values;
        bool element_present = false;
        PathInDataBuilder element_path;
        RETURN_IF_ERROR(shredded_field_to_variant_map(element_schema, element, metadata,
                                                      &element_path, &element_values,
                                                      &element_present, string_values));
        if (!element_present) {
            if (element.is_null()) {
                array.push_back(Field());
                continue;
            }
            return Status::Corruption("Parquet VARIANT array element is missing");
        }

        FieldWithDataType element_value;
        move_variant_map_to_field(std::move(element_values), &element_value);
        array.push_back(std::move(element_value.field));
    }

    FieldWithDataType value;
    const size_t elements_count = array.size();
    value.field = Field::create_field<TYPE_ARRAY>(std::move(array));
    fill_variant_field_info(&value);
    if (value.base_scalar_type_id == INVALID_TYPE) {
        RETURN_IF_ERROR(make_jsonb_field(make_null_array_json(elements_count), &value));
    }
    (*values)[path->build()] = std::move(value);
    *present = true;
    return Status::OK();
}

static Status typed_map_to_variant_map(const FieldSchema& typed_value_field, const Field& field,
                                       const std::string& metadata, PathInDataBuilder* path,
                                       VariantMap* values, bool* present,
                                       std::deque<std::string>* string_values) {
    if (field.is_null()) {
        *present = false;
        return Status::OK();
    }
    if (typed_value_field.children.size() != 2) {
        return Status::Corruption("Parquet VARIANT map typed_value has {} child fields",
                                  typed_value_field.children.size());
    }

    const auto& map = field.get<TYPE_MAP>();
    DORIS_CHECK(map.size() == 2);
    DORIS_CHECK(map[0].get_type() == TYPE_ARRAY);
    DORIS_CHECK(map[1].get_type() == TYPE_ARRAY);
    const auto& keys = map[0].get<TYPE_ARRAY>();
    const auto& value_fields = map[1].get<TYPE_ARRAY>();
    DORIS_CHECK(keys.size() == value_fields.size());

    if (keys.empty()) {
        RETURN_IF_ERROR(insert_empty_object_marker(path->build(), values));
        *present = true;
        return Status::OK();
    }

    std::set<std::string> object_keys;
    const FieldSchema& key_field = typed_value_field.children[0];
    const FieldSchema& value_field = typed_value_field.children[1];
    for (size_t i = 0; i < keys.size(); ++i) {
        std::string key;
        bool key_present = false;
        RETURN_IF_ERROR(get_binary_field(keys[i], &key, &key_present));
        if (!key_present) {
            return Status::Corruption("Parquet VARIANT map typed_value has null key {}",
                                      key_field.name);
        }
        if (!object_keys.insert(key).second) {
            return Status::Corruption("Parquet VARIANT map typed_value has duplicate key {}", key);
        }

        path->append(key, false);
        bool value_present = false;
        Status st = shredded_field_to_variant_map(value_field, value_fields[i], metadata, path,
                                                  values, &value_present, string_values);
        if (!st.ok()) {
            path->pop_back();
            return st;
        }
        if (!value_present) {
            (*values)[path->build()] = FieldWithDataType {.field = Field()};
        }
        path->pop_back();
    }
    *present = true;
    return Status::OK();
}

static Status typed_value_to_variant_map(const FieldSchema& typed_value_field, const Field& field,
                                         const std::string& metadata, PathInDataBuilder* path,
                                         VariantMap* values, bool* present,
                                         std::deque<std::string>* string_values) {
    if (field.is_null()) {
        *present = false;
        return Status::OK();
    }
    const DataTypePtr& typed_type = remove_nullable(typed_value_field.data_type);
    if (typed_type->get_primitive_type() == TYPE_STRUCT) {
        const auto& fields = field.get<TYPE_STRUCT>();
        *present = true;
        bool has_present_child = false;
        for (int i = 0; i < typed_value_field.children.size(); ++i) {
            path->append(typed_value_field.children[i].name, false);
            bool child_present = false;
            RETURN_IF_ERROR(shredded_field_to_variant_map(typed_value_field.children[i], fields[i],
                                                          metadata, path, values, &child_present,
                                                          string_values));
            has_present_child |= child_present;
            path->pop_back();
        }
        if (!has_present_child) {
            RETURN_IF_ERROR(insert_empty_object_marker(path->build(), values));
        }
        return Status::OK();
    }
    if (typed_type->get_primitive_type() == TYPE_ARRAY) {
        return typed_array_to_variant_map(typed_value_field, field, metadata, path, values, present,
                                          string_values);
    }
    if (typed_type->get_primitive_type() == TYPE_MAP) {
        return typed_map_to_variant_map(typed_value_field, field, metadata, path, values, present,
                                        string_values);
    }

    return append_typed_field_to_variant_map(typed_value_field, field, path, values, present);
}

static bool direct_typed_value_present_at(const FieldSchema& field_schema, const IColumn& column,
                                          size_t row, bool allow_variant_wrapper,
                                          const std::set<uint64_t>& column_ids,
                                          const std::vector<const NullMap*>& parent_null_maps) {
    if (!has_selected_column(field_schema, column_ids) ||
        has_direct_typed_parent_null(parent_null_maps, row)) {
        return false;
    }

    const IColumn* value_column = &column;
    if (const auto* nullable_column = check_and_get_column<ColumnNullable>(&column)) {
        const auto& null_map = nullable_column->get_null_map_data();
        DCHECK_LT(row, null_map.size());
        if (null_map[row]) {
            return false;
        }
        value_column = &nullable_column->get_nested_column();
    }

    if (allow_variant_wrapper && is_variant_wrapper_field(field_schema, false)) {
        const int typed_value_idx = find_child_idx(field_schema, "typed_value");
        DCHECK_GE(typed_value_idx, 0);
        const auto& typed_struct = assert_cast<const ColumnStruct&>(*value_column);
        return direct_typed_value_present_at(field_schema.children[typed_value_idx],
                                             typed_struct.get_column(typed_value_idx), row, false,
                                             column_ids, parent_null_maps);
    }

    return true;
}

static Status append_direct_typed_empty_object_markers(
        const FieldSchema& field_schema, const ColumnStruct& struct_column, size_t start,
        size_t rows, PathInDataBuilder* path, ColumnVariant* batch,
        const std::set<uint64_t>& column_ids, const std::vector<const NullMap*>& parent_null_maps) {
    DataTypePtr marker_type = make_nullable(std::make_shared<DataTypeJsonb>());
    MutableColumnPtr marker_column = marker_type->create_column();
    marker_column->insert_default();
    bool has_marker = false;

    const PathInData marker_path = path->build();
    Field empty_object;
    RETURN_IF_ERROR(make_empty_object_field(&empty_object));
    for (size_t i = 0; i < rows; ++i) {
        const size_t row = start + i;
        if (has_direct_typed_parent_null(parent_null_maps, row)) {
            marker_column->insert_default();
            has_marker |= marker_path.empty();
            continue;
        }

        bool has_present_child = false;
        for (int child_idx = 0; child_idx < field_schema.children.size(); ++child_idx) {
            if (direct_typed_value_present_at(field_schema.children[child_idx],
                                              struct_column.get_column(child_idx), row, true,
                                              column_ids, parent_null_maps)) {
                has_present_child = true;
                break;
            }
        }

        if (has_present_child) {
            marker_column->insert_default();
            continue;
        }
        marker_column->insert(empty_object);
        has_marker = true;
    }

    if (!has_marker) {
        return Status::OK();
    }
    if (!batch->add_sub_column(marker_path, std::move(marker_column), marker_type)) {
        return Status::Corruption("Failed to add Parquet VARIANT empty typed object marker {}",
                                  marker_path.get_path());
    }
    return Status::OK();
}

static Status append_direct_typed_column_to_batch(const FieldSchema& field_schema,
                                                  const IColumn& column, size_t start, size_t rows,
                                                  PathInDataBuilder* path, ColumnVariant* batch,
                                                  bool allow_variant_wrapper,
                                                  const std::set<uint64_t>& column_ids,
                                                  std::vector<const NullMap*> parent_null_maps) {
    if (!has_selected_column(field_schema, column_ids)) {
        return Status::OK();
    }

    const IColumn* value_column = &column;
    if (const auto* nullable_column = check_and_get_column<ColumnNullable>(&column)) {
        parent_null_maps.push_back(&nullable_column->get_null_map_data());
        value_column = &nullable_column->get_nested_column();
    }

    if (allow_variant_wrapper && is_variant_wrapper_field(field_schema, false)) {
        const int typed_value_idx = find_child_idx(field_schema, "typed_value");
        DCHECK_GE(typed_value_idx, 0);
        const auto& typed_struct = assert_cast<const ColumnStruct&>(*value_column);
        return append_direct_typed_column_to_batch(
                field_schema.children[typed_value_idx], typed_struct.get_column(typed_value_idx),
                start, rows, path, batch, false, column_ids, parent_null_maps);
    }

    const auto& type = remove_nullable(field_schema.data_type);
    if (type->get_primitive_type() == TYPE_STRUCT) {
        const auto& struct_column = assert_cast<const ColumnStruct&>(*value_column);
        for (int i = 0; i < field_schema.children.size(); ++i) {
            if (!has_selected_column(field_schema.children[i], column_ids)) {
                continue;
            }
            path->append(field_schema.children[i].name, false);
            RETURN_IF_ERROR(append_direct_typed_column_to_batch(
                    field_schema.children[i], struct_column.get_column(i), start, rows, path, batch,
                    true, column_ids, parent_null_maps));
            path->pop_back();
        }
        return append_direct_typed_empty_object_markers(field_schema, struct_column, start, rows,
                                                        path, batch, column_ids, parent_null_maps);
    }

    DataTypePtr variant_leaf_type = make_nullable(direct_variant_leaf_type(field_schema));
    MutableColumnPtr variant_leaf = variant_leaf_type->create_column();
    variant_leaf->insert_default();
    if (type->get_primitive_type() == TYPE_ARRAY &&
        (contains_uuid_typed_value_field(field_schema) ||
         contains_temporal_variant_leaf_type(field_schema.data_type) ||
         contains_floating_point_variant_leaf_type(field_schema.data_type))) {
        RETURN_IF_ERROR(insert_direct_typed_array_leaf_range(
                field_schema, *value_column, start, rows, parent_null_maps, variant_leaf.get()));
    } else if (is_uuid_typed_value_field(field_schema)) {
        RETURN_IF_ERROR(insert_direct_typed_uuid_leaf_range(*value_column, start, rows,
                                                            parent_null_maps, variant_leaf.get()));
    } else if (is_temporal_variant_leaf_type(type->get_primitive_type())) {
        insert_direct_typed_temporal_leaf_range(type->get_primitive_type(), *value_column, start,
                                                rows, parent_null_maps, variant_leaf.get());
    } else {
        insert_direct_typed_leaf_range(*value_column, start, rows, parent_null_maps,
                                       variant_leaf.get());
    }
    if (!batch->add_sub_column(path->build(), std::move(variant_leaf), variant_leaf_type)) {
        return Status::Corruption("Failed to add Parquet VARIANT typed subcolumn {}",
                                  path->build().get_path());
    }
    return Status::OK();
}

static Status append_variant_struct_rows_to_column(
        const FieldSchema& field_schema, const ColumnStruct& variant_struct_column,
        const NullMap* struct_null_map, size_t start, size_t rows,
        const std::set<uint64_t>& column_ids, ColumnPtr& doris_column,
        ParquetColumnReader::ColumnStatistics* variant_statistics) {
    DCHECK_LE(start + rows, variant_struct_column.size());

    MutableColumnPtr variant_column_ptr;
    NullMap* null_map_ptr = nullptr;
    auto mutable_column = doris_column->assume_mutable();
    if (doris_column->is_nullable()) {
        auto* nullable_column = assert_cast<ColumnNullable*>(mutable_column.get());
        variant_column_ptr = nullable_column->get_nested_column_ptr();
        null_map_ptr = &nullable_column->get_null_map_data();
    } else {
        if (field_schema.data_type->is_nullable()) {
            return Status::Corruption("Not nullable column has null values in parquet file");
        }
        variant_column_ptr = std::move(mutable_column);
    }
    auto* variant_column = assert_cast<ColumnVariant*>(variant_column_ptr.get());

    const int typed_value_idx = find_child_idx(field_schema, "typed_value");
    if (can_use_direct_typed_only_value(field_schema, column_ids)) {
        variant_statistics->variant_direct_typed_value_read_rows += static_cast<int64_t>(rows);
        MutableColumnPtr batch_variant_column =
                ColumnVariant::create(variant_column->max_subcolumns_count(),
                                      variant_column->enable_doc_mode(), rows + 1);
        auto* batch_variant = assert_cast<ColumnVariant*>(batch_variant_column.get());
        PathInDataBuilder path;
        RETURN_IF_ERROR(append_direct_typed_column_to_batch(
                field_schema.children[typed_value_idx],
                variant_struct_column.get_column(typed_value_idx), start, rows, &path,
                batch_variant, false, column_ids, {}));
        variant_column->insert_range_from(*batch_variant_column, 1, rows);
        if (null_map_ptr != nullptr) {
            for (size_t i = start; i < start + rows; ++i) {
                null_map_ptr->push_back(struct_null_map != nullptr && (*struct_null_map)[i]);
            }
        }
        return Status::OK();
    }

    variant_statistics->variant_rowwise_read_rows += static_cast<int64_t>(rows);
    for (size_t i = start; i < start + rows; ++i) {
        if (struct_null_map != nullptr && (*struct_null_map)[i]) {
            if (null_map_ptr == nullptr) {
                return Status::Corruption("Not nullable column has null values in parquet file");
            }
            variant_column->insert_default();
            null_map_ptr->push_back(1);
            continue;
        }
        VariantMap values;
        bool present = false;
        PathInDataBuilder path;
        std::deque<std::string> string_values;
        RETURN_IF_ERROR(variant_to_variant_map(field_schema, variant_struct_column[i], nullptr,
                                               &path, &values, &present, &string_values));
        if (!present) {
            values[PathInData()] = FieldWithDataType {.field = Field()};
        }
        RETURN_IF_CATCH_EXCEPTION(
                variant_column->insert(Field::create_field<TYPE_VARIANT>(std::move(values))));
        if (null_map_ptr != nullptr) {
            null_map_ptr->push_back(0);
        }
    }
    return Status::OK();
}

#ifdef BE_TEST
namespace parquet_variant_reader_test {
bool can_direct_read_typed_value_for_test(const FieldSchema& typed_value_field) {
    const std::set<uint64_t> column_ids;
    return can_direct_read_typed_value(typed_value_field, false, column_ids);
}

bool can_use_direct_typed_only_value_for_test(const FieldSchema& variant_field,
                                              const std::set<uint64_t>& column_ids) {
    return can_use_direct_typed_only_value(variant_field, column_ids);
}

Status append_direct_typed_column_to_batch_for_test(const FieldSchema& typed_value_field,
                                                    const IColumn& typed_value_column, size_t start,
                                                    size_t rows, ColumnVariant* batch) {
    PathInDataBuilder path;
    const std::set<uint64_t> column_ids;
    return append_direct_typed_column_to_batch(typed_value_field, typed_value_column, start, rows,
                                               &path, batch, false, column_ids, {});
}

Status read_variant_row_for_test(const FieldSchema& variant_field, const Field& field,
                                 bool output_nullable, Field* result, bool* sql_null) {
    if (field.is_null()) {
        if (!output_nullable) {
            return Status::Corruption("Not nullable column has null values in parquet file");
        }
        *sql_null = true;
        return Status::OK();
    }

    VariantMap values;
    bool present = false;
    PathInDataBuilder path;
    std::deque<std::string> string_values;
    RETURN_IF_ERROR(variant_to_variant_map(variant_field, field, nullptr, &path, &values, &present,
                                           &string_values));
    if (!present) {
        values[PathInData()] = FieldWithDataType {.field = Field()};
    }

    auto variant_column = ColumnVariant::create(0, false);
    RETURN_IF_CATCH_EXCEPTION(
            variant_column->insert(Field::create_field<TYPE_VARIANT>(std::move(values))));
    variant_column->get(0, *result);
    *sql_null = false;
    return Status::OK();
}

Status read_variant_rows_for_test(const FieldSchema& variant_field, const IColumn& struct_column,
                                  const std::set<uint64_t>& column_ids, ColumnPtr& doris_column,
                                  int64_t* direct_rows, int64_t* rowwise_rows) {
    const IColumn* struct_source = &struct_column;
    const NullMap* struct_null_map = nullptr;
    if (const auto* nullable_struct = check_and_get_column<ColumnNullable>(struct_source)) {
        struct_null_map = &nullable_struct->get_null_map_data();
        struct_source = &nullable_struct->get_nested_column();
    }
    const auto& variant_struct_column = assert_cast<const ColumnStruct&>(*struct_source);

    ParquetColumnReader::ColumnStatistics variant_statistics;
    RETURN_IF_ERROR(append_variant_struct_rows_to_column(
            variant_field, variant_struct_column, struct_null_map, 0, variant_struct_column.size(),
            column_ids, doris_column, &variant_statistics));
    *direct_rows = variant_statistics.variant_direct_typed_value_read_rows;
    *rowwise_rows = variant_statistics.variant_rowwise_read_rows;
    return Status::OK();
}

Status variant_to_json_for_test(const FieldSchema& variant_field, const Field& field,
                                const std::string& inherited_metadata, std::string* json,
                                bool* present) {
    return variant_to_json(variant_field, field, &inherited_metadata, json, present);
}

bool variant_struct_reader_type_is_nullable_for_test(const FieldSchema& variant_field) {
    return make_variant_struct_reader_type(variant_field)->is_nullable();
}

bool variant_struct_reader_column_is_nullable_for_test(const FieldSchema& variant_field) {
    auto variant_struct_type = make_variant_struct_reader_type(variant_field);
    return make_variant_struct_read_column(variant_field, variant_struct_type)->is_nullable();
}
} // namespace parquet_variant_reader_test
#endif

// Existing recursive factory keeps nested reader wiring and shared state in one dispatch point.
// NOLINTNEXTLINE(readability-function-cognitive-complexity,readability-function-size)
Status ParquetColumnReader::create(io::FileReaderSPtr file, FieldSchema* field,
                                   const tparquet::RowGroup& row_group, const RowRanges& row_ranges,
                                   const cctz::time_zone* ctz, io::IOContext* io_ctx,
                                   std::unique_ptr<ParquetColumnReader>& reader,
                                   size_t max_buf_size,
                                   std::unordered_map<int, tparquet::OffsetIndex>& col_offsets,
                                   RuntimeState* state, bool in_collection,
                                   const std::set<uint64_t>& column_ids,
                                   const std::set<uint64_t>& filter_column_ids) {
    size_t total_rows = row_group.num_rows;
    const auto field_primitive_type = remove_nullable(field->data_type)->get_primitive_type();
    if (field_primitive_type == TYPE_ARRAY) {
        const bool offset_only = !column_ids.empty() &&
                                 column_ids.contains(field->get_column_id()) &&
                                 !column_ids.contains(field->children[0].get_column_id());
        std::unique_ptr<ParquetColumnReader> element_reader;
        RETURN_IF_ERROR(create(file, field->children.data(), row_group, row_ranges, ctz, io_ctx,
                               element_reader, max_buf_size, col_offsets, state, true, column_ids,
                               filter_column_ids));
        auto array_reader = ArrayColumnReader::create_unique(row_ranges, total_rows, ctz, io_ctx);
        element_reader->set_column_in_nested();
        RETURN_IF_ERROR(array_reader->init(std::move(element_reader), field, offset_only));
        array_reader->_filter_column_ids = filter_column_ids;
        reader.reset(array_reader.release());
    } else if (field_primitive_type == TYPE_MAP) {
        std::unique_ptr<ParquetColumnReader> key_reader;
        std::unique_ptr<ParquetColumnReader> value_reader;

        if (column_ids.empty() ||
            column_ids.find(field->children[0].get_column_id()) != column_ids.end()) {
            // Create key reader
            RETURN_IF_ERROR(create(file, field->children.data(), row_group, row_ranges, ctz, io_ctx,
                                   key_reader, max_buf_size, col_offsets, state, true, column_ids,
                                   filter_column_ids));
        } else {
            auto skip_reader = std::make_unique<SkipReadingReader>(row_ranges, total_rows, ctz,
                                                                   io_ctx, field->children.data());
            key_reader = std::move(skip_reader);
        }

        if (column_ids.empty() ||
            column_ids.find(field->children[1].get_column_id()) != column_ids.end()) {
            // Create value reader
            RETURN_IF_ERROR(create(file, &field->children[1], row_group, row_ranges, ctz, io_ctx,
                                   value_reader, max_buf_size, col_offsets, state, true, column_ids,
                                   filter_column_ids));
        } else {
            auto skip_reader = std::make_unique<SkipReadingReader>(row_ranges, total_rows, ctz,
                                                                   io_ctx, &field->children[1]);
            value_reader = std::move(skip_reader);
        }

        auto map_reader = MapColumnReader::create_unique(row_ranges, total_rows, ctz, io_ctx);
        key_reader->set_column_in_nested();
        value_reader->set_column_in_nested();
        RETURN_IF_ERROR(map_reader->init(std::move(key_reader), std::move(value_reader), field));
        map_reader->_filter_column_ids = filter_column_ids;
        reader.reset(map_reader.release());
    } else if (field_primitive_type == TYPE_STRUCT) {
        std::unordered_map<std::string, std::unique_ptr<ParquetColumnReader>> child_readers;
        child_readers.reserve(field->children.size());
        int non_skip_reader_idx = -1;
        for (int i = 0; i < field->children.size(); ++i) {
            auto& child = field->children[i];
            std::unique_ptr<ParquetColumnReader> child_reader;
            if (column_ids.empty() || column_ids.find(child.get_column_id()) != column_ids.end()) {
                RETURN_IF_ERROR(create(file, &child, row_group, row_ranges, ctz, io_ctx,
                                       child_reader, max_buf_size, col_offsets, state,
                                       in_collection, column_ids, filter_column_ids));
                child_readers[child.name] = std::move(child_reader);
                // Record the first non-SkippingReader
                if (non_skip_reader_idx == -1) {
                    non_skip_reader_idx = i;
                }
            } else {
                auto skip_reader = std::make_unique<SkipReadingReader>(row_ranges, total_rows, ctz,
                                                                       io_ctx, &child);
                skip_reader->_filter_column_ids = filter_column_ids;
                child_readers[child.name] = std::move(skip_reader);
            }
            child_readers[child.name]->set_column_in_nested();
        }
        // If all children are SkipReadingReader, force the first child to call create
        if (non_skip_reader_idx == -1) {
            std::unique_ptr<ParquetColumnReader> child_reader;
            RETURN_IF_ERROR(create(file, field->children.data(), row_group, row_ranges, ctz, io_ctx,
                                   child_reader, max_buf_size, col_offsets, state, in_collection,
                                   column_ids, filter_column_ids));
            child_reader->set_column_in_nested();
            child_readers[field->children[0].name] = std::move(child_reader);
        }
        auto struct_reader = StructColumnReader::create_unique(row_ranges, total_rows, ctz, io_ctx);
        RETURN_IF_ERROR(struct_reader->init(std::move(child_readers), field));
        struct_reader->_filter_column_ids = filter_column_ids;
        reader.reset(struct_reader.release());
    } else if (field_primitive_type == TYPE_VARIANT) {
        auto variant_reader =
                VariantColumnReader::create_unique(row_ranges, total_rows, ctz, io_ctx);
        RETURN_IF_ERROR(variant_reader->init(file, field, row_group, max_buf_size, col_offsets,
                                             state, in_collection, column_ids, filter_column_ids));
        variant_reader->_filter_column_ids = filter_column_ids;
        reader.reset(variant_reader.release());
    } else {
        auto physical_index = field->physical_column_index;
        const tparquet::OffsetIndex* offset_index =
                col_offsets.find(physical_index) != col_offsets.end() ? &col_offsets[physical_index]
                                                                      : nullptr;

        const tparquet::ColumnChunk& chunk = row_group.columns[physical_index];
        if (in_collection) {
            if (offset_index == nullptr) {
                auto scalar_reader = ScalarColumnReader<true, false>::create_unique(
                        row_ranges, total_rows, chunk, offset_index, ctz, io_ctx);

                RETURN_IF_ERROR(scalar_reader->init(file, field, max_buf_size, state));
                scalar_reader->_filter_column_ids = filter_column_ids;
                reader.reset(scalar_reader.release());
            } else {
                auto scalar_reader = ScalarColumnReader<true, true>::create_unique(
                        row_ranges, total_rows, chunk, offset_index, ctz, io_ctx);

                RETURN_IF_ERROR(scalar_reader->init(file, field, max_buf_size, state));
                scalar_reader->_filter_column_ids = filter_column_ids;
                reader.reset(scalar_reader.release());
            }
        } else {
            if (offset_index == nullptr) {
                auto scalar_reader = ScalarColumnReader<false, false>::create_unique(
                        row_ranges, total_rows, chunk, offset_index, ctz, io_ctx);

                RETURN_IF_ERROR(scalar_reader->init(file, field, max_buf_size, state));
                scalar_reader->_filter_column_ids = filter_column_ids;
                reader.reset(scalar_reader.release());
            } else {
                auto scalar_reader = ScalarColumnReader<false, true>::create_unique(
                        row_ranges, total_rows, chunk, offset_index, ctz, io_ctx);

                RETURN_IF_ERROR(scalar_reader->init(file, field, max_buf_size, state));
                scalar_reader->_filter_column_ids = filter_column_ids;
                reader.reset(scalar_reader.release());
            }
        }
    }
    return Status::OK();
}

void ParquetColumnReader::_generate_read_ranges(RowRange page_row_range,
                                                RowRanges* result_ranges) const {
    result_ranges->add(page_row_range);
    RowRanges::ranges_intersection(*result_ranges, _row_ranges, result_ranges);
}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
Status ScalarColumnReader<IN_COLLECTION, OFFSET_INDEX>::init(io::FileReaderSPtr file,
                                                             FieldSchema* field,
                                                             size_t max_buf_size,
                                                             RuntimeState* state) {
    _field_schema = field;
    auto& chunk_meta = _chunk_meta.meta_data;
    int64_t chunk_start = has_dict_page(chunk_meta) ? chunk_meta.dictionary_page_offset
                                                    : chunk_meta.data_page_offset;
    size_t chunk_len = chunk_meta.total_compressed_size;
    size_t prefetch_buffer_size = std::min(chunk_len, max_buf_size);
    if ((typeid_cast<doris::io::TracingFileReader*>(file.get()) &&
         typeid_cast<io::MergeRangeFileReader*>(
                 ((doris::io::TracingFileReader*)(file.get()))->inner_reader().get())) ||
        typeid_cast<io::MergeRangeFileReader*>(file.get())) {
        // turn off prefetch data when using MergeRangeFileReader
        prefetch_buffer_size = 0;
    }
    _stream_reader = std::make_unique<io::BufferedFileStreamReader>(file, chunk_start, chunk_len,
                                                                    prefetch_buffer_size);
    ParquetPageReadContext ctx(
            (state == nullptr) ? true : state->query_options().enable_parquet_file_page_cache);

    _chunk_reader = std::make_unique<ColumnChunkReader<IN_COLLECTION, OFFSET_INDEX>>(
            _stream_reader.get(), &_chunk_meta, field, _offset_index, _total_rows, _io_ctx, ctx);
    RETURN_IF_ERROR(_chunk_reader->init());
    return Status::OK();
}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
Status ScalarColumnReader<IN_COLLECTION, OFFSET_INDEX>::_skip_values(size_t num_values) {
    if (num_values == 0) {
        return Status::OK();
    }
    if (_chunk_reader->max_def_level() > 0) {
        LevelDecoder& def_decoder = _chunk_reader->def_level_decoder();
        size_t skipped = 0;
        size_t null_size = 0;
        size_t nonnull_size = 0;
        while (skipped < num_values) {
            level_t def_level = -1;
            size_t loop_skip = def_decoder.get_next_run(&def_level, num_values - skipped);
            if (loop_skip == 0) {
                std::stringstream ss;
                const auto& bit_reader = def_decoder.rle_decoder().bit_reader();
                ss << "def_decoder buffer (hex): ";
                for (size_t i = 0; i < bit_reader.max_bytes(); ++i) {
                    ss << std::hex << std::setw(2) << std::setfill('0')
                       << static_cast<int>(bit_reader.buffer()[i]) << " ";
                }
                LOG(WARNING) << ss.str();
                return Status::InternalError("Failed to decode definition level.");
            }
            if (def_level < _field_schema->definition_level) {
                null_size += loop_skip;
            } else {
                nonnull_size += loop_skip;
            }
            skipped += loop_skip;
        }
        if (null_size > 0) {
            RETURN_IF_ERROR(_chunk_reader->skip_values(null_size, false));
        }
        if (nonnull_size > 0) {
            RETURN_IF_ERROR(_chunk_reader->skip_values(nonnull_size, true));
        }
    } else {
        RETURN_IF_ERROR(_chunk_reader->skip_values(num_values));
    }
    return Status::OK();
}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
Status ScalarColumnReader<IN_COLLECTION, OFFSET_INDEX>::_read_values(size_t num_values,
                                                                     ColumnPtr& doris_column,
                                                                     DataTypePtr& type,
                                                                     FilterMap& filter_map,
                                                                     bool is_dict_filter) {
    if (num_values == 0) {
        return Status::OK();
    }
    MutableColumnPtr data_column;
    std::vector<uint16_t> null_map;
    NullMap* map_data_column = nullptr;
    if (doris_column->is_nullable()) {
        SCOPED_RAW_TIMER(&_decode_null_map_time);
        // doris_column either originates from a mutable block in vparquet_group_reader
        // or is a newly created ColumnPtr, and therefore can be modified.
        auto* nullable_column =
                assert_cast<ColumnNullable*>(const_cast<IColumn*>(doris_column.get()));

        data_column = nullable_column->get_nested_column_ptr();
        map_data_column = &(nullable_column->get_null_map_data());
        if (_chunk_reader->max_def_level() > 0) {
            LevelDecoder& def_decoder = _chunk_reader->def_level_decoder();
            size_t has_read = 0;
            bool prev_is_null = true;
            while (has_read < num_values) {
                level_t def_level;
                size_t loop_read = def_decoder.get_next_run(&def_level, num_values - has_read);
                if (loop_read == 0) {
                    std::stringstream ss;
                    const auto& bit_reader = def_decoder.rle_decoder().bit_reader();
                    ss << "def_decoder buffer (hex): ";
                    for (size_t i = 0; i < bit_reader.max_bytes(); ++i) {
                        ss << std::hex << std::setw(2) << std::setfill('0')
                           << static_cast<int>(bit_reader.buffer()[i]) << " ";
                    }
                    LOG(WARNING) << ss.str();
                    return Status::InternalError("Failed to decode definition level.");
                }

                bool is_null = def_level < _field_schema->definition_level;
                if (!(prev_is_null ^ is_null)) {
                    null_map.emplace_back(0);
                }
                size_t remaining = loop_read;
                while (remaining > USHRT_MAX) {
                    null_map.emplace_back(USHRT_MAX);
                    null_map.emplace_back(0);
                    remaining -= USHRT_MAX;
                }
                null_map.emplace_back((u_short)remaining);
                prev_is_null = is_null;
                has_read += loop_read;
            }
        }
    } else {
        if (_chunk_reader->max_def_level() > 0) {
            return Status::Corruption("Not nullable column has null values in parquet file");
        }
        data_column = doris_column->assume_mutable();
    }
    if (null_map.empty()) {
        size_t remaining = num_values;
        while (remaining > USHRT_MAX) {
            null_map.emplace_back(USHRT_MAX);
            null_map.emplace_back(0);
            remaining -= USHRT_MAX;
        }
        null_map.emplace_back((u_short)remaining);
    }
    ColumnSelectVector select_vector;
    {
        SCOPED_RAW_TIMER(&_decode_null_map_time);
        RETURN_IF_ERROR(select_vector.init(null_map, num_values, map_data_column, &filter_map,
                                           _filter_map_index));
        _filter_map_index += num_values;
    }
    return _chunk_reader->decode_values(data_column, type, select_vector, is_dict_filter);
}

/**
 * Load the nested column data of complex type.
 * A row of complex type may be stored across two(or more) pages, and the parameter `align_rows` indicates that
 * whether the reader should read the remaining value of the last row in previous page.
 */
template <bool IN_COLLECTION, bool OFFSET_INDEX>
// Existing nested scalar reader is the central row/page alignment loop for complex values.
// NOLINTNEXTLINE(readability-function-cognitive-complexity,readability-function-size)
Status ScalarColumnReader<IN_COLLECTION, OFFSET_INDEX>::_read_nested_column(
        ColumnPtr& doris_column, DataTypePtr& type, FilterMap& filter_map, size_t batch_size,
        size_t* read_rows, bool* eof, bool is_dict_filter) {
    _rep_levels.clear();
    _def_levels.clear();

    // Handle nullable columns
    MutableColumnPtr data_column;
    NullMap* map_data_column = nullptr;
    if (doris_column->is_nullable()) {
        SCOPED_RAW_TIMER(&_decode_null_map_time);
        // doris_column either originates from a mutable block in vparquet_group_reader
        // or is a newly created ColumnPtr, and therefore can be modified.
        auto* nullable_column =
                const_cast<ColumnNullable*>(assert_cast<const ColumnNullable*>(doris_column.get()));
        data_column = nullable_column->get_nested_column_ptr();
        map_data_column = &(nullable_column->get_null_map_data());
    } else {
        if (_field_schema->data_type->is_nullable()) {
            return Status::Corruption("Not nullable column has null values in parquet file");
        }
        data_column = doris_column->assume_mutable();
    }

    std::vector<uint16_t> null_map;
    std::unordered_set<size_t> ancestor_null_indices;
    std::vector<uint8_t> nested_filter_map_data;

    auto read_and_fill_data = [&](size_t before_rep_level_sz, size_t filter_map_index) {
        RETURN_IF_ERROR(_chunk_reader->fill_def(_def_levels));
        std::unique_ptr<FilterMap> nested_filter_map = std::make_unique<FilterMap>();
        if (filter_map.has_filter()) {
            RETURN_IF_ERROR(gen_filter_map(filter_map, filter_map_index, before_rep_level_sz,
                                           _rep_levels.size(), nested_filter_map_data,
                                           &nested_filter_map));
        }

        null_map.clear();
        ancestor_null_indices.clear();
        RETURN_IF_ERROR(gen_nested_null_map(before_rep_level_sz, _rep_levels.size(), null_map,
                                            ancestor_null_indices));

        ColumnSelectVector select_vector;
        {
            SCOPED_RAW_TIMER(&_decode_null_map_time);
            RETURN_IF_ERROR(select_vector.init(
                    null_map,
                    _rep_levels.size() - before_rep_level_sz - ancestor_null_indices.size(),
                    map_data_column, nested_filter_map.get(), 0, &ancestor_null_indices));
        }

        RETURN_IF_ERROR(
                _chunk_reader->decode_values(data_column, type, select_vector, is_dict_filter));
        if (!ancestor_null_indices.empty()) {
            RETURN_IF_ERROR(_chunk_reader->skip_values(ancestor_null_indices.size(), false));
        }
        if (filter_map.has_filter()) {
            auto new_rep_sz = before_rep_level_sz;
            for (size_t idx = before_rep_level_sz; idx < _rep_levels.size(); idx++) {
                if (nested_filter_map_data[idx - before_rep_level_sz]) {
                    _rep_levels[new_rep_sz] = _rep_levels[idx];
                    _def_levels[new_rep_sz] = _def_levels[idx];
                    new_rep_sz++;
                }
            }
            _rep_levels.resize(new_rep_sz);
            _def_levels.resize(new_rep_sz);
        }
        return Status::OK();
    };

    while (_current_range_idx < _row_ranges.range_size()) {
        size_t left_row =
                std::max(_current_row_index, _row_ranges.get_range_from(_current_range_idx));
        size_t right_row = std::min(left_row + batch_size - *read_rows,
                                    (size_t)_row_ranges.get_range_to(_current_range_idx));
        _current_row_index = left_row;
        RETURN_IF_ERROR(_chunk_reader->seek_to_nested_row(left_row));
        size_t load_rows = 0;
        bool cross_page = false;
        size_t before_rep_level_sz = _rep_levels.size();
        RETURN_IF_ERROR(_chunk_reader->load_page_nested_rows(_rep_levels, right_row - left_row,
                                                             &load_rows, &cross_page));
        RETURN_IF_ERROR(read_and_fill_data(before_rep_level_sz, _filter_map_index));
        _filter_map_index += load_rows;
        while (cross_page) {
            before_rep_level_sz = _rep_levels.size();
            RETURN_IF_ERROR(_chunk_reader->load_cross_page_nested_row(_rep_levels, &cross_page));
            RETURN_IF_ERROR(read_and_fill_data(before_rep_level_sz, _filter_map_index - 1));
        }
        *read_rows += load_rows;
        _current_row_index += load_rows;
        _current_range_idx += (_current_row_index == _row_ranges.get_range_to(_current_range_idx));
        if (*read_rows == batch_size) {
            break;
        }
    }
    *eof = _current_range_idx == _row_ranges.range_size();
    return Status::OK();
}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
Status ScalarColumnReader<IN_COLLECTION, OFFSET_INDEX>::_read_and_skip_nested_levels(
        FilterMap& filter_map, size_t before_rep_level_sz, size_t filter_map_index,
        std::vector<uint8_t>& nested_filter_map_data) {
    RETURN_IF_ERROR(_chunk_reader->fill_def(_def_levels));
    RETURN_IF_ERROR(_chunk_reader->skip_nested_values(_def_levels, before_rep_level_sz,
                                                      _def_levels.size()));
    if (!filter_map.has_filter()) {
        return Status::OK();
    }

    std::unique_ptr<FilterMap> nested_filter_map = std::make_unique<FilterMap>();
    RETURN_IF_ERROR(gen_filter_map(filter_map, filter_map_index, before_rep_level_sz,
                                   _rep_levels.size(), nested_filter_map_data, &nested_filter_map));
    auto new_rep_sz = before_rep_level_sz;
    for (size_t idx = before_rep_level_sz; idx < _rep_levels.size(); idx++) {
        if (nested_filter_map_data[idx - before_rep_level_sz]) {
            _rep_levels[new_rep_sz] = _rep_levels[idx];
            _def_levels[new_rep_sz] = _def_levels[idx];
            new_rep_sz++;
        }
    }
    _rep_levels.resize(new_rep_sz);
    _def_levels.resize(new_rep_sz);
    return Status::OK();
}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
Status ScalarColumnReader<IN_COLLECTION, OFFSET_INDEX>::read_nested_levels(FilterMap& filter_map,
                                                                           size_t batch_size,
                                                                           size_t* read_rows,
                                                                           bool* eof) {
    _rep_levels.clear();
    _def_levels.clear();
    *read_rows = 0;

    std::vector<uint8_t> nested_filter_map_data;

    while (_current_range_idx < _row_ranges.range_size()) {
        size_t left_row =
                std::max(_current_row_index, _row_ranges.get_range_from(_current_range_idx));
        size_t right_row = std::min(left_row + batch_size - *read_rows,
                                    (size_t)_row_ranges.get_range_to(_current_range_idx));
        _current_row_index = left_row;
        RETURN_IF_ERROR(_chunk_reader->seek_to_nested_row(left_row));
        size_t load_rows = 0;
        bool cross_page = false;
        size_t before_rep_level_sz = _rep_levels.size();
        RETURN_IF_ERROR(_chunk_reader->load_page_nested_rows(_rep_levels, right_row - left_row,
                                                             &load_rows, &cross_page));
        RETURN_IF_ERROR(_read_and_skip_nested_levels(filter_map, before_rep_level_sz,
                                                     _filter_map_index, nested_filter_map_data));
        _filter_map_index += load_rows;
        while (cross_page) {
            before_rep_level_sz = _rep_levels.size();
            RETURN_IF_ERROR(_chunk_reader->load_cross_page_nested_row(_rep_levels, &cross_page));
            RETURN_IF_ERROR(_read_and_skip_nested_levels(filter_map, before_rep_level_sz,
                                                         _filter_map_index - 1,
                                                         nested_filter_map_data));
        }
        *read_rows += load_rows;
        _current_row_index += load_rows;
        _current_range_idx += (_current_row_index == _row_ranges.get_range_to(_current_range_idx));
        if (*read_rows == batch_size) {
            break;
        }
    }
    *eof = _current_range_idx == _row_ranges.range_size();
    return Status::OK();
}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
Status ScalarColumnReader<IN_COLLECTION, OFFSET_INDEX>::read_dict_values_to_column(
        MutableColumnPtr& doris_column, bool* has_dict) {
    bool loaded;
    RETURN_IF_ERROR(_try_load_dict_page(&loaded, has_dict));
    if (loaded && *has_dict) {
        return _chunk_reader->read_dict_values_to_column(doris_column);
    }
    return Status::OK();
}
template <bool IN_COLLECTION, bool OFFSET_INDEX>
Result<MutableColumnPtr>
ScalarColumnReader<IN_COLLECTION, OFFSET_INDEX>::convert_dict_column_to_string_column(
        const ColumnInt32* dict_column) {
    return _chunk_reader->convert_dict_column_to_string_column(dict_column);
}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
Status ScalarColumnReader<IN_COLLECTION, OFFSET_INDEX>::_try_load_dict_page(bool* loaded,
                                                                            bool* has_dict) {
    // _chunk_reader init will load first page header to check whether has dict page
    *loaded = true;
    *has_dict = _chunk_reader->has_dict();
    return Status::OK();
}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
// Existing scalar read path handles page iteration, filtering, and conversion in one dispatch loop.
// NOLINTNEXTLINE(readability-function-cognitive-complexity,readability-function-size)
Status ScalarColumnReader<IN_COLLECTION, OFFSET_INDEX>::read_column_data(
        ColumnPtr& doris_column, const DataTypePtr& type,
        const std::shared_ptr<TableSchemaChangeHelper::Node>& root_node, FilterMap& filter_map,
        size_t batch_size, size_t* read_rows, bool* eof, bool is_dict_filter,
        int64_t real_column_size) {
    if (_converter == nullptr) {
        _converter = parquet::PhysicalToLogicalConverter::get_converter(
                _field_schema, _field_schema->data_type, type, _ctz, is_dict_filter);
        if (!_converter->support()) {
            return Status::InternalError(
                    "The column type of '{}' is not supported: {}, is_dict_filter: {}, "
                    "src_logical_type: {}, dst_logical_type: {}",
                    _field_schema->name, _converter->get_error_msg(), is_dict_filter,
                    _field_schema->data_type->get_name(), type->get_name());
        }
    }
    // !FIXME: We should verify whether the get_physical_column logic is correct, why do we return a doris_column?
    ColumnPtr resolved_column =
            _converter->get_physical_column(_field_schema->physical_type, _field_schema->data_type,
                                            doris_column, type, is_dict_filter);
    DataTypePtr& resolved_type = _converter->get_physical_type();

    _def_levels.clear();
    _rep_levels.clear();
    *read_rows = 0;

    if (_in_nested) {
        RETURN_IF_ERROR(_read_nested_column(resolved_column, resolved_type, filter_map, batch_size,
                                            read_rows, eof, is_dict_filter));
        return _converter->convert(resolved_column, _field_schema->data_type, type, doris_column,
                                   is_dict_filter);
    }

    int64_t right_row = 0;
    if constexpr (OFFSET_INDEX == false) {
        RETURN_IF_ERROR(_chunk_reader->parse_page_header());
        right_row = _chunk_reader->page_end_row();
    } else {
        right_row = _chunk_reader->page_end_row();
    }

    do {
        // generate the row ranges that should be read
        RowRanges read_ranges;
        _generate_read_ranges(RowRange {_current_row_index, right_row}, &read_ranges);
        if (read_ranges.count() == 0) {
            // skip the whole page
            _current_row_index = right_row;
        } else {
            bool skip_whole_batch = false;
            // Determining whether to skip page or batch will increase the calculation time.
            // When the filtering effect is greater than 60%, it is possible to skip the page or batch.
            if (filter_map.has_filter() && filter_map.filter_ratio() > 0.6) {
                // lazy read
                size_t remaining_num_values = read_ranges.count();
                if (batch_size >= remaining_num_values &&
                    filter_map.can_filter_all(remaining_num_values, _filter_map_index)) {
                    // We can skip the whole page if the remaining values are filtered by predicate columns
                    _filter_map_index += remaining_num_values;
                    _current_row_index = right_row;
                    *read_rows = remaining_num_values;
                    break;
                }
                skip_whole_batch = batch_size <= remaining_num_values &&
                                   filter_map.can_filter_all(batch_size, _filter_map_index);
                if (skip_whole_batch) {
                    _filter_map_index += batch_size;
                }
            }
            // load page data to decode or skip values
            RETURN_IF_ERROR(_chunk_reader->parse_page_header());
            RETURN_IF_ERROR(_chunk_reader->load_page_data_idempotent());
            size_t has_read = 0;
            for (size_t idx = 0; idx < read_ranges.range_size(); idx++) {
                auto range = read_ranges.get_range(idx);
                // generate the skipped values
                size_t skip_values = range.from() - _current_row_index;
                RETURN_IF_ERROR(_skip_values(skip_values));
                _current_row_index += skip_values;
                // generate the read values
                size_t read_values =
                        std::min((size_t)(range.to() - range.from()), batch_size - has_read);
                if (skip_whole_batch) {
                    RETURN_IF_ERROR(_skip_values(read_values));
                } else {
                    RETURN_IF_ERROR(_read_values(read_values, resolved_column, resolved_type,
                                                 filter_map, is_dict_filter));
                }
                has_read += read_values;
                *read_rows += read_values;
                _current_row_index += read_values;
                if (has_read == batch_size) {
                    break;
                }
            }
        }
    } while (false);

    if (right_row == _current_row_index) {
        if (!_chunk_reader->has_next_page()) {
            *eof = true;
        } else {
            RETURN_IF_ERROR(_chunk_reader->next_page());
        }
    }

    {
        SCOPED_RAW_TIMER(&_convert_time);
        RETURN_IF_ERROR(_converter->convert(resolved_column, _field_schema->data_type, type,
                                            doris_column, is_dict_filter));
    }
    return Status::OK();
}

Status ArrayColumnReader::init(std::unique_ptr<ParquetColumnReader> element_reader,
                               FieldSchema* field, bool offset_only) {
    _field_schema = field;
    _element_reader = std::move(element_reader);
    _offset_only = offset_only;
    return Status::OK();
}

Status ArrayColumnReader::read_column_data(
        ColumnPtr& doris_column, const DataTypePtr& type,
        const std::shared_ptr<TableSchemaChangeHelper::Node>& root_node, FilterMap& filter_map,
        size_t batch_size, size_t* read_rows, bool* eof, bool is_dict_filter,
        int64_t real_column_size) {
    MutableColumnPtr data_column;
    NullMap* null_map_ptr = nullptr;
    if (doris_column->is_nullable()) {
        auto mutable_column = doris_column->assume_mutable();
        auto* nullable_column = assert_cast<ColumnNullable*>(mutable_column.get());
        null_map_ptr = &nullable_column->get_null_map_data();
        data_column = nullable_column->get_nested_column_ptr();
    } else {
        if (_field_schema->data_type->is_nullable()) {
            return Status::Corruption("Not nullable column has null values in parquet file");
        }
        data_column = doris_column->assume_mutable();
    }
    if (type->get_primitive_type() != PrimitiveType::TYPE_ARRAY) {
        return Status::Corruption(
                "Wrong data type for column '{}', expected Array type, actual type: {}.",
                _field_schema->name, type->get_name());
    }

    ColumnPtr& element_column = assert_cast<ColumnArray&>(*data_column).get_data_ptr();
    const DataTypePtr& element_type =
            (assert_cast<const DataTypeArray*>(remove_nullable(type).get()))->get_nested_type();
    if (_offset_only) {
        // Cardinality needs collection levels and offsets, but not element payloads.
        RETURN_IF_ERROR(
                _element_reader->read_nested_levels(filter_map, batch_size, read_rows, eof));
    } else {
        RETURN_IF_ERROR(_element_reader->read_column_data(
                element_column, element_type, root_node->get_element_node(), filter_map, batch_size,
                read_rows, eof, is_dict_filter));
    }
    if (*read_rows == 0) {
        return Status::OK();
    }

    ColumnArray::Offsets64& offsets_data = assert_cast<ColumnArray&>(*data_column).get_offsets();
    // fill offset and null map
    fill_array_offset(_field_schema, offsets_data, null_map_ptr, _element_reader->get_rep_level(),
                      _element_reader->get_def_level());
    if (_offset_only && offsets_data.back() > element_column->size()) {
        auto mutable_element_column = element_column->assume_mutable();
        mutable_element_column->insert_many_defaults(offsets_data.back() - element_column->size());
        element_column = std::move(mutable_element_column);
    }
    DCHECK_EQ(element_column->size(), offsets_data.back());
#ifndef NDEBUG
    doris_column->sanity_check();
#endif
    return Status::OK();
}

Status MapColumnReader::init(std::unique_ptr<ParquetColumnReader> key_reader,
                             std::unique_ptr<ParquetColumnReader> value_reader,
                             FieldSchema* field) {
    _field_schema = field;
    _key_reader = std::move(key_reader);
    _value_reader = std::move(value_reader);
    return Status::OK();
}

Status MapColumnReader::read_column_data(
        ColumnPtr& doris_column, const DataTypePtr& type,
        const std::shared_ptr<TableSchemaChangeHelper::Node>& root_node, FilterMap& filter_map,
        size_t batch_size, size_t* read_rows, bool* eof, bool is_dict_filter,
        int64_t real_column_size) {
    MutableColumnPtr data_column;
    NullMap* null_map_ptr = nullptr;
    if (doris_column->is_nullable()) {
        auto mutable_column = doris_column->assume_mutable();
        auto* nullable_column = assert_cast<ColumnNullable*>(mutable_column.get());
        null_map_ptr = &nullable_column->get_null_map_data();
        data_column = nullable_column->get_nested_column_ptr();
    } else {
        if (_field_schema->data_type->is_nullable()) {
            return Status::Corruption("Not nullable column has null values in parquet file");
        }
        data_column = doris_column->assume_mutable();
    }
    if (remove_nullable(type)->get_primitive_type() != PrimitiveType::TYPE_MAP) {
        return Status::Corruption(
                "Wrong data type for column '{}', expected Map type, actual type id {}.",
                _field_schema->name, type->get_name());
    }

    auto& map = assert_cast<ColumnMap&>(*data_column);
    const DataTypePtr& key_type =
            assert_cast<const DataTypeMap*>(remove_nullable(type).get())->get_key_type();
    const DataTypePtr& value_type =
            assert_cast<const DataTypeMap*>(remove_nullable(type).get())->get_value_type();
    ColumnPtr& key_column = map.get_keys_ptr();
    ColumnPtr& value_column = map.get_values_ptr();

    size_t key_rows = 0;
    size_t value_rows = 0;
    bool key_eof = false;
    bool value_eof = false;
    int64_t orig_col_column_size = key_column->size();

    RETURN_IF_ERROR(_key_reader->read_column_data(key_column, key_type, root_node->get_key_node(),
                                                  filter_map, batch_size, &key_rows, &key_eof,
                                                  is_dict_filter));

    while (value_rows < key_rows && !value_eof) {
        size_t loop_rows = 0;
        RETURN_IF_ERROR(_value_reader->read_column_data(
                value_column, value_type, root_node->get_value_node(), filter_map,
                key_rows - value_rows, &loop_rows, &value_eof, is_dict_filter,
                key_column->size() - orig_col_column_size));
        value_rows += loop_rows;
    }
    DCHECK_EQ(key_rows, value_rows);
    *read_rows = key_rows;
    *eof = key_eof;

    if (*read_rows == 0) {
        return Status::OK();
    }

    DCHECK_EQ(key_column->size(), value_column->size());
    // fill offset and null map
    fill_array_offset(_field_schema, map.get_offsets(), null_map_ptr, _key_reader->get_rep_level(),
                      _key_reader->get_def_level());
    DCHECK_EQ(key_column->size(), map.get_offsets().back());
#ifndef NDEBUG
    doris_column->sanity_check();
#endif
    return Status::OK();
}

Status StructColumnReader::init(
        std::unordered_map<std::string, std::unique_ptr<ParquetColumnReader>>&& child_readers,
        FieldSchema* field) {
    _field_schema = field;
    _child_readers = std::move(child_readers);
    return Status::OK();
}

Status StructColumnReader::read_nested_levels(FilterMap& filter_map, size_t batch_size,
                                              size_t* read_rows, bool* eof) {
    _read_column_names.clear();
    for (const auto& child : _field_schema->children) {
        auto it = _child_readers.find(child.name);
        if (it == _child_readers.end() ||
            dynamic_cast<SkipReadingReader*>(it->second.get()) != nullptr) {
            continue;
        }
        _read_column_names.emplace_back(child.name);
        return it->second->read_nested_levels(filter_map, batch_size, read_rows, eof);
    }
    return Status::Corruption("Cannot read struct '{}' levels without a reference column",
                              _field_schema->name);
}

// Existing struct reader coordinates child readers, missing columns, and selection state.
// NOLINTNEXTLINE(readability-function-cognitive-complexity,readability-function-size)
Status StructColumnReader::read_column_data(
        ColumnPtr& doris_column, const DataTypePtr& type,
        const std::shared_ptr<TableSchemaChangeHelper::Node>& root_node, FilterMap& filter_map,
        size_t batch_size, size_t* read_rows, bool* eof, bool is_dict_filter,
        int64_t real_column_size) {
    MutableColumnPtr data_column;
    NullMap* null_map_ptr = nullptr;
    if (doris_column->is_nullable()) {
        auto mutable_column = doris_column->assume_mutable();
        auto* nullable_column = assert_cast<ColumnNullable*>(mutable_column.get());
        null_map_ptr = &nullable_column->get_null_map_data();
        data_column = nullable_column->get_nested_column_ptr();
    } else {
        if (_field_schema->data_type->is_nullable()) {
            return Status::Corruption("Not nullable column has null values in parquet file");
        }
        data_column = doris_column->assume_mutable();
    }
    if (type->get_primitive_type() != PrimitiveType::TYPE_STRUCT) {
        return Status::Corruption(
                "Wrong data type for column '{}', expected Struct type, actual type id {}.",
                _field_schema->name, type->get_name());
    }

    auto& doris_struct = assert_cast<ColumnStruct&>(*data_column);
    const auto* doris_struct_type = assert_cast<const DataTypeStruct*>(remove_nullable(type).get());

    int64_t not_missing_column_id = -1;
    size_t not_missing_orig_column_size = 0;
    std::vector<size_t> missing_column_idxs {};
    std::vector<size_t> skip_reading_column_idxs {};

    _read_column_names.clear();

    for (size_t i = 0; i < doris_struct.tuple_size(); ++i) {
        ColumnPtr& doris_field = doris_struct.get_column_ptr(i);
        const auto& doris_type = doris_struct_type->get_element(i);
        const auto& doris_name = doris_struct_type->get_element_name(i);
        if (!root_node->children_column_exists(doris_name)) {
            missing_column_idxs.push_back(i);
            VLOG_DEBUG << "[ParquetReader] Missing column in schema: column_idx[" << i
                       << "], doris_name: " << doris_name << " (column not exists in root node)";
            continue;
        }
        auto file_name = root_node->children_file_column_name(doris_name);

        // Check if this is a SkipReadingReader - we should skip it when choosing reference column
        // because SkipReadingReader doesn't know the actual data size in nested context
        bool is_skip_reader =
                dynamic_cast<SkipReadingReader*>(_child_readers[file_name].get()) != nullptr;

        if (is_skip_reader) {
            // Store SkipReadingReader columns to fill them later based on reference column size
            skip_reading_column_idxs.push_back(i);
            continue;
        }

        // Only add non-SkipReadingReader columns to _read_column_names
        // This ensures get_rep_level() and get_def_level() return valid levels
        _read_column_names.emplace_back(file_name);

        size_t field_rows = 0;
        bool field_eof = false;
        if (not_missing_column_id == -1) {
            not_missing_column_id = i;
            not_missing_orig_column_size = doris_field->size();
            RETURN_IF_ERROR(_child_readers[file_name]->read_column_data(
                    doris_field, doris_type, root_node->get_children_node(doris_name), filter_map,
                    batch_size, &field_rows, &field_eof, is_dict_filter));
            *read_rows = field_rows;
            *eof = field_eof;
            /*
             * Considering the issue in the `_read_nested_column` function where data may span across pages, leading
             * to missing definition and repetition levels, when filling the null_map of the struct later, it is
             * crucial to use the definition and repetition levels from the first read column
             * (since `_read_nested_column` is not called repeatedly).
             *
             *  It is worth mentioning that, theoretically, any sub-column can be chosen to fill the null_map,
             *  and selecting the shortest one will offer better performance
             */
        } else {
            while (field_rows < *read_rows && !field_eof) {
                size_t loop_rows = 0;
                RETURN_IF_ERROR(_child_readers[file_name]->read_column_data(
                        doris_field, doris_type, root_node->get_children_node(doris_name),
                        filter_map, *read_rows - field_rows, &loop_rows, &field_eof,
                        is_dict_filter));
                field_rows += loop_rows;
            }
            DCHECK_EQ(*read_rows, field_rows);
            //            DCHECK_EQ(*eof, field_eof);
        }
    }

    int64_t missing_column_sz = -1;

    if (not_missing_column_id == -1) {
        // All queried columns are missing in the file (e.g., all added after schema change)
        // We need to pick a column from _field_schema children that exists in the file for RL/DL reference
        std::string reference_file_column_name;
        std::unique_ptr<ParquetColumnReader>* reference_reader = nullptr;

        for (const auto& child : _field_schema->children) {
            auto it = _child_readers.find(child.name);
            if (it != _child_readers.end()) {
                // Skip SkipReadingReader as they don't have valid RL/DL
                bool is_skip_reader = dynamic_cast<SkipReadingReader*>(it->second.get()) != nullptr;
                if (!is_skip_reader) {
                    reference_file_column_name = child.name;
                    reference_reader = &(it->second);
                    break;
                }
            }
        }

        if (reference_reader != nullptr) {
            // Read the reference column to get correct RL/DL information
            // TODO: Optimize by only reading RL/DL without actual data decoding

            // We need to find the FieldSchema for the reference column from _field_schema children
            FieldSchema* ref_field_schema = nullptr;
            for (auto& child : _field_schema->children) {
                if (child.name == reference_file_column_name) {
                    ref_field_schema = &child;
                    break;
                }
            }

            if (ref_field_schema == nullptr) {
                return Status::InternalError(
                        "Cannot find field schema for reference column '{}' in struct '{}'",
                        reference_file_column_name, _field_schema->name);
            }

            // Create a temporary column to hold the data (we'll use its size for missing_column_sz)
            ColumnPtr temp_column = ref_field_schema->data_type->create_column();
            auto temp_type = ref_field_schema->data_type;

            size_t field_rows = 0;
            bool field_eof = false;

            // Use ConstNode for the reference column instead of looking up from root_node.
            // The reference column is only used to get RL/DL information for determining the number
            // of elements in the struct. It may be a column that has been dropped from the table
            // schema (e.g., 'removed' field), but still exists in older parquet files.
            // Since we don't need schema mapping for this column (we just need its RL/DL levels),
            // using ConstNode is safe and avoids the issue where the reference column doesn't exist
            // in root_node (because it was dropped from table schema).
            auto ref_child_node = TableSchemaChangeHelper::ConstNode::get_instance();
            not_missing_orig_column_size = temp_column->size();

            RETURN_IF_ERROR((*reference_reader)
                                    ->read_column_data(temp_column, temp_type, ref_child_node,
                                                       filter_map, batch_size, &field_rows,
                                                       &field_eof, is_dict_filter));

            *read_rows = field_rows;
            *eof = field_eof;

            // Store this reference column name for get_rep_level/get_def_level to use
            _read_column_names.emplace_back(reference_file_column_name);

            missing_column_sz = temp_column->size() - not_missing_orig_column_size;
        } else {
            return Status::Corruption(
                    "Cannot read struct '{}': all queried columns are missing and no reference "
                    "column found in file",
                    _field_schema->name);
        }
    }

    //  This missing_column_sz is not *read_rows. Because read_rows returns the number of rows.
    //  For example: suppose we have a column array<struct<a:int,b:string>>,
    //  where b is a newly added column, that is, a missing column.
    //  There are two rows of data in this column,
    //      [{1,null},{2,null},{3,null}]
    //      [{4,null},{5,null}]
    //  When you first read subcolumn a, you read 5 data items and the value of *read_rows is 2.
    //  You should insert 5 records into subcolumn b instead of 2.
    if (missing_column_sz == -1) {
        missing_column_sz = doris_struct.get_column(not_missing_column_id).size() -
                            not_missing_orig_column_size;
    }

    // Fill SkipReadingReader columns with the correct amount of data based on reference column
    // Let SkipReadingReader handle the data filling through its read_column_data method
    for (auto idx : skip_reading_column_idxs) {
        auto& doris_field = doris_struct.get_column_ptr(idx);
        auto& doris_type = const_cast<DataTypePtr&>(doris_struct_type->get_element(idx));
        auto& doris_name = const_cast<String&>(doris_struct_type->get_element_name(idx));
        auto file_name = root_node->children_file_column_name(doris_name);

        size_t field_rows = 0;
        bool field_eof = false;
        RETURN_IF_ERROR(_child_readers[file_name]->read_column_data(
                doris_field, doris_type, root_node->get_children_node(doris_name), filter_map,
                missing_column_sz, &field_rows, &field_eof, is_dict_filter, missing_column_sz));
    }

    // Fill truly missing columns (not in root_node) with null or default value
    for (auto idx : missing_column_idxs) {
        auto& doris_field = doris_struct.get_column_ptr(idx);
        const auto& doris_type = doris_struct_type->get_element(idx);
        DCHECK(doris_type->is_nullable());
        auto mutable_column = doris_field->assume_mutable();
        auto* nullable_column = static_cast<ColumnNullable*>(mutable_column.get());
        nullable_column->insert_many_defaults(missing_column_sz);
    }

    if (null_map_ptr != nullptr) {
        fill_struct_null_map(_field_schema, *null_map_ptr, this->get_rep_level(),
                             this->get_def_level());
    }
#ifndef NDEBUG
    doris_column->sanity_check();
#endif
    return Status::OK();
}

Status VariantColumnReader::init(io::FileReaderSPtr file, FieldSchema* field,
                                 const tparquet::RowGroup& row_group, size_t max_buf_size,
                                 std::unordered_map<int, tparquet::OffsetIndex>& col_offsets,
                                 RuntimeState* state, bool in_collection,
                                 const std::set<uint64_t>& column_ids,
                                 const std::set<uint64_t>& filter_column_ids) {
    _field_schema = field;
    _column_ids = column_ids;
    _variant_struct_field = std::make_unique<FieldSchema>(*field);

    DataTypePtr variant_struct_type = make_variant_struct_reader_type(*field);
    _variant_struct_field->data_type = variant_struct_type;

    RETURN_IF_ERROR(ParquetColumnReader::create(file, _variant_struct_field.get(), row_group,
                                                _row_ranges, _ctz, _io_ctx, _struct_reader,
                                                max_buf_size, col_offsets, state, in_collection,
                                                column_ids, filter_column_ids));
    _struct_reader->set_column_in_nested();
    return Status::OK();
}

Status VariantColumnReader::read_column_data(
        ColumnPtr& doris_column, const DataTypePtr& type,
        const std::shared_ptr<TableSchemaChangeHelper::Node>& root_node, FilterMap& filter_map,
        size_t batch_size, size_t* read_rows, bool* eof, bool is_dict_filter,
        int64_t real_column_size) {
    (void)root_node;
    if (remove_nullable(type)->get_primitive_type() != PrimitiveType::TYPE_VARIANT) {
        return Status::Corruption(
                "Wrong data type for column '{}', expected Variant type, actual type: {}.",
                _field_schema->name, type->get_name());
    }

    const auto& variant_struct_type = _variant_struct_field->data_type;
    ColumnPtr struct_column = make_variant_struct_read_column(*_field_schema, variant_struct_type);
    const size_t old_struct_rows = struct_column->size();
    auto const_node = TableSchemaChangeHelper::ConstNode::get_instance();
    RETURN_IF_ERROR(_struct_reader->read_column_data(struct_column, variant_struct_type, const_node,
                                                     filter_map, batch_size, read_rows, eof,
                                                     is_dict_filter, real_column_size));

    const size_t new_struct_rows = struct_column->size() - old_struct_rows;
    if (new_struct_rows == 0) {
        return Status::OK();
    }

    const IColumn* variant_struct_source = struct_column.get();
    const NullMap* struct_null_map = nullptr;
    if (const auto* nullable_struct = check_and_get_column<ColumnNullable>(variant_struct_source)) {
        struct_null_map = &nullable_struct->get_null_map_data();
        variant_struct_source = &nullable_struct->get_nested_column();
    }
    const auto& variant_struct_column = assert_cast<const ColumnStruct&>(*variant_struct_source);

    RETURN_IF_ERROR(append_variant_struct_rows_to_column(
            *_field_schema, variant_struct_column, struct_null_map, old_struct_rows,
            new_struct_rows, _column_ids, doris_column, &_variant_statistics));
#ifndef NDEBUG
    doris_column->sanity_check();
#endif
    return Status::OK();
}

template class ScalarColumnReader<true, true>;
template class ScalarColumnReader<true, false>;
template class ScalarColumnReader<false, true>;
template class ScalarColumnReader<false, false>;

}; // namespace doris
