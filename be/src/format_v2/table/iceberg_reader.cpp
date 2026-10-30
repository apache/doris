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

#include "format_v2/table/iceberg_reader.h"

#include <gen_cpp/Exprs_types.h>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <algorithm>
#include <deque>
#include <memory>
#include <sstream>
#include <string_view>
#include <utility>

#include "common/cast_set.h"
#include "common/consts.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/data_type_time.h"
#include "core/data_type/data_type_timestamptz.h"
#include "core/data_type/define_primitive_type.h"
#include "core/field.h"
#include "core/value/time_value.h"
#include "exprs/vliteral.h"
#include "exprs/vslot_ref.h"
#include "format/table/deletion_vector_reader.h"
#include "format/table/iceberg_default_value.h"
#include "format_v2/expr/equality_delete_predicate.h"
#include "format_v2/expr/evolution_cast.h"
#include "format_v2/orc/orc_reader.h"
#include "format_v2/parquet/parquet_reader.h"
#include "format_v2/parquet/reader/column_reader.h"
#include "format_v2/table/schema_history_util.h"
#include "format_v2/table_reader.h"
#include "io/file_factory.h"
#include "util/debug_points.h"
#include "util/string_util.h"
#include "util/url_coding.h"

namespace doris::format::iceberg {

static constexpr const char* ROW_LINEAGE_ROW_ID = "_row_id";
static constexpr int32_t ROW_LINEAGE_ROW_ID_FIELD_ID = 2147483540;

namespace {

bool external_field_has_authoritative_name_mapping(const schema::external::TField& field) {
    if (field.__isset.name_mapping_is_authoritative && field.name_mapping_is_authoritative) {
        return true;
    }
    if (!field.__isset.nestedField) {
        return false;
    }
    if (field.nestedField.__isset.struct_field && field.nestedField.struct_field.__isset.fields) {
        return std::ranges::any_of(field.nestedField.struct_field.fields, [](const auto& child) {
            const auto* child_field = format::get_field_ptr(child);
            return child_field != nullptr &&
                   external_field_has_authoritative_name_mapping(*child_field);
        });
    }
    if (field.nestedField.__isset.array_field && field.nestedField.array_field.__isset.item_field) {
        const auto* item = format::get_field_ptr(field.nestedField.array_field.item_field);
        return item != nullptr && external_field_has_authoritative_name_mapping(*item);
    }
    if (field.nestedField.__isset.map_field) {
        const auto& map_field = field.nestedField.map_field;
        if (map_field.__isset.key_field) {
            const auto* key = format::get_field_ptr(map_field.key_field);
            if (key != nullptr && external_field_has_authoritative_name_mapping(*key)) {
                return true;
            }
        }
        if (map_field.__isset.value_field) {
            const auto* value = format::get_field_ptr(map_field.value_field);
            return value != nullptr && external_field_has_authoritative_name_mapping(*value);
        }
    }
    return false;
}

bool contains_variant_type(const DataTypePtr& input) {
    if (input == nullptr) {
        return false;
    }
    const auto type = remove_nullable(input);
    switch (type->get_primitive_type()) {
    case TYPE_VARIANT:
        return true;
    case TYPE_ARRAY:
        return contains_variant_type(assert_cast<const DataTypeArray&>(*type).get_nested_type());
    case TYPE_MAP: {
        const auto& map = assert_cast<const DataTypeMap&>(*type);
        return contains_variant_type(map.get_key_type()) ||
               contains_variant_type(map.get_value_type());
    }
    case TYPE_STRUCT:
        return std::ranges::any_of(assert_cast<const DataTypeStruct&>(*type).get_elements(),
                                   contains_variant_type);
    default:
        return false;
    }
}

bool mapping_reads_variant(const format::ColumnMapping& mapping) {
    if (!mapping.file_local_id.has_value()) {
        return false;
    }
    if (contains_variant_type(mapping.original_file_type)) {
        return true;
    }
    if (mapping.table_type != nullptr &&
        remove_nullable(mapping.table_type)->get_primitive_type() == TYPE_VARIANT) {
        return true;
    }
    return std::ranges::any_of(mapping.child_mappings, mapping_reads_variant);
}

const char* file_format_name(FileFormat format) {
    switch (format) {
    case FileFormat::PARQUET:
        return "PARQUET";
    case FileFormat::ORC:
        return "ORC";
    case FileFormat::CSV:
        return "CSV";
    case FileFormat::JSON:
        return "JSON";
    case FileFormat::TEXT:
        return "TEXT";
    case FileFormat::JNI:
        return "JNI";
    case FileFormat::ARROW:
        return "ARROW";
    case FileFormat::ES_HTTP:
        return "ES_HTTP";
    case FileFormat::WAL:
        return "WAL";
    }
    return "UNKNOWN";
}

} // namespace

bool IcebergTableReader::_scan_has_any_authoritative_name_mapping() const {
    if (schema_has_any_authoritative_name_mapping(_projected_columns)) {
        return true;
    }
    if (_scan_params == nullptr || !_scan_params->__isset.history_schema_info) {
        return false;
    }
    // Metadata-only scans have no projected data column carrying aliases. Consult the complete
    // schema so hidden equality-delete keys use the same authoritative mapping as visible fields.
    for (const auto& schema : _scan_params->history_schema_info) {
        if (!schema.__isset.root_field || !schema.root_field.__isset.fields) {
            continue;
        }
        if (std::ranges::any_of(schema.root_field.fields, [](const auto& field) {
                const auto* schema_field = format::get_field_ptr(field);
                return schema_field != nullptr &&
                       external_field_has_authoritative_name_mapping(*schema_field);
            })) {
            return true;
        }
    }
    return false;
}

Status IcebergTableReader::validate_variant_file_mappings(
        FileFormat format, const std::vector<format::ColumnMapping>& mappings) {
    if (format == FileFormat::PARQUET || !std::ranges::any_of(mappings, mapping_reads_variant)) {
        return Status::OK();
    }
    // Gate on a physical mapping, not the table schema: an older ORC/Avro file may legitimately
    // omit a Variant field added by schema evolution, in which case the mapper synthesizes NULL.
    return Status::NotSupported(
            "Iceberg Variant is supported only for Parquet files in FileScannerV2; file format {} "
            "(including ORC/Avro readers) is not supported",
            file_format_name(format));
}

Status IcebergTableReader::annotate_file_schema(
        std::vector<format::ColumnDefinition>* file_schema) {
    if (file_schema == nullptr || file_schema->empty()) {
        return Status::OK();
    }
    // Iceberg's positional fallback is a Parquet-reader-only compatibility path. Its ORC/Avro
    // readers instead derive a name mapping from the expected schema for id-less files, which is
    // exactly the current-name resurrection that v2 scan semantics deliberately rejects (#65784),
    // so ORC keeps its existing behavior and stays out of scope here.
    if (_format != FileFormat::PARQUET) {
        return Status::OK();
    }
    // Iceberg's `hasIds` contract is existential: a single real field id keeps the file on the id
    // path and, exactly as in Iceberg, skips both the name mapping and the positional fallback.
    if (schema_has_any_field_id(*file_schema)) {
        return Status::OK();
    }
    // An old-FE plan keeps the legacy name fallback (rolling upgrade), and an explicit table-level
    // name mapping stays authoritative even when it does not cover this file.
    if (!supports_iceberg_scan_semantics_v2(_scan_params) ||
        _scan_has_any_authoritative_name_mapping()) {
        return Status::OK();
    }
    // ParquetSchemaUtil.addFallbackIds: ordinals start at 1, are counted over the physical
    // top-level fields in file order, and are not propagated to nested fields. Reader-synthesized
    // carriers (row number / global row id) are not file fields and must not consume an ordinal.
    int32_t ordinal = 0;
    for (auto& field : *file_schema) {
        if (field.column_type != format::ColumnType::DATA_COLUMN) {
            continue;
        }
        ++ordinal;
        // ID-less Parquet fields carry their physical name as a STRING identifier
        // (ParquetReader::_fill_column_definition), so only a real integer field id may skip
        // stamping; overwriting the string identifier is what lets FieldIdMatcher resolve the file.
        if (field.has_identifier_field_id()) {
            continue;
        }
        field.identifier = Field::create_field<TYPE_INT>(ordinal);
    }
    _positional_id_fallback = true;
    return Status::OK();
}

Status IcebergTableReader::validate_file_mapping(const format::TableColumnMapper& mapper) const {
    const bool metadata_only_count = _push_down_agg_type == TPushAggOp::type::COUNT &&
                                     _push_down_count_columns.has_value() &&
                                     _push_down_count_columns->empty() &&
                                     _supports_aggregate_pushdown(TPushAggOp::type::COUNT);
    if (metadata_only_count) {
        // COUNT(*) may retain a physical carrier only when aggregate pushdown will bypass decoding;
        // delete/filter fallbacks must still validate that carrier against the selected file reader.
        return Status::OK();
    }
    return validate_variant_file_mappings(_format, mapper.mappings());
}

template <typename T>
static std::string join_values_for_debug(const std::vector<T>& values) {
    std::ostringstream out;
    out << "[";
    for (size_t idx = 0; idx < values.size(); ++idx) {
        if (idx > 0) {
            out << ", ";
        }
        out << values[idx];
    }
    out << "]";
    return out.str();
}

static bool is_projected_row_lineage_row_id(const format::ColumnDefinition& column) {
    // Iceberg row lineage columns can be bound by field id when a mapper has already been built,
    // but customize_file_scan_request() is also exercised directly by scan-request tests before the
    // mapper exists. In that path, inspect the projected table schema so row-position dependencies
    // are still added for `_row_id`.
    return column.name == ROW_LINEAGE_ROW_ID ||
           (column.has_identifier_field_id() &&
            column.get_identifier_field_id() == ROW_LINEAGE_ROW_ID_FIELD_ID);
}

static bool is_projected_iceberg_rowid(const format::ColumnDefinition& column) {
    return column.name == BeConsts::ICEBERG_ROWID_COL;
}

static bool is_projected_iceberg_metadata(const format::ColumnDefinition& column) {
    return iequal(column.name, BeConsts::ICEBERG_FILE_PATH_COL) ||
           iequal(column.name, BeConsts::ICEBERG_ROW_POSITION_COL);
}

static int iceberg_hex_value(char value) {
    if (value >= '0' && value <= '9') {
        return value - '0';
    }
    if (value >= 'a' && value <= 'f') {
        return value - 'a' + 10;
    }
    if (value >= 'A' && value <= 'F') {
        return value - 'A' + 10;
    }
    return -1;
}

static Status decode_iceberg_hex(std::string_view encoded, std::string* decoded) {
    DORIS_CHECK(decoded != nullptr);
    if ((encoded.size() & 1U) != 0) {
        return Status::InvalidArgument("Invalid odd-length Iceberg binary default");
    }
    decoded->resize(encoded.size() / 2);
    for (size_t index = 0; index < encoded.size(); index += 2) {
        const int high = iceberg_hex_value(encoded[index]);
        const int low = iceberg_hex_value(encoded[index + 1]);
        if (high < 0 || low < 0) {
            return Status::InvalidArgument("Invalid hexadecimal Iceberg binary default");
        }
        (*decoded)[index / 2] = static_cast<char>((high << 4) | low);
    }
    return Status::OK();
}

static Status decode_iceberg_json_binary(std::string_view encoded, std::string* decoded) {
    DORIS_CHECK(decoded != nullptr);
    const bool is_uuid = encoded.size() == 36 && encoded[8] == '-' && encoded[13] == '-' &&
                         encoded[18] == '-' && encoded[23] == '-';
    if (!is_uuid) {
        return decode_iceberg_hex(encoded, decoded);
    }

    std::string uuid_hex;
    uuid_hex.reserve(32);
    for (size_t index = 0; index < encoded.size(); ++index) {
        if (index != 8 && index != 13 && index != 18 && index != 23) {
            uuid_hex.push_back(encoded[index]);
        }
    }
    return decode_iceberg_hex(uuid_hex, decoded);
}

static std::string iceberg_json_scalar_text(const rapidjson::Value& value) {
    if (value.IsString()) {
        return {value.GetString(), value.GetStringLength()};
    }
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    value.Accept(writer);
    return {buffer.GetString(), buffer.GetSize()};
}

static void normalize_iceberg_json_timestamp(PrimitiveType primitive_type, std::string* value) {
    if (primitive_type != TYPE_DATETIME && primitive_type != TYPE_DATETIMEV2 &&
        primitive_type != TYPE_TIMESTAMPTZ && primitive_type != TYPE_TIMESTAMP_NS) {
        return;
    }
    if (const size_t separator = value->find('T'); separator != std::string::npos) {
        (*value)[separator] = ' ';
    }
    if (primitive_type == TYPE_TIMESTAMPTZ) {
        return;
    }
    if (value->ends_with('Z')) {
        value->pop_back();
        return;
    }
    const size_t time_start = value->find(' ');
    if (time_start == std::string::npos) {
        return;
    }
    const size_t offset = value->find_first_of("+-", time_start + 1);
    if (offset != std::string::npos) {
        value->erase(offset);
    }
}

static Status build_v2_null_default(const format::ColumnDefinition& field,
                                    const DataTypePtr& data_type, Field* result) {
    DORIS_CHECK(data_type != nullptr);
    DORIS_CHECK(result != nullptr);
    if (field.is_optional.has_value() && !*field.is_optional) {
        return Status::InvalidArgument("Required Iceberg field '{}' has a null default",
                                       field.name);
    }
    if (!data_type->is_nullable()) {
        return Status::InternalError(
                "Optional Iceberg field '{}' has a null default, but its Doris type '{}' is not "
                "nullable",
                field.name, data_type->get_name());
    }
    *result = Field();
    return Status::OK();
}

static const format::ColumnDefinition* find_v2_struct_child(const format::ColumnDefinition& field,
                                                            const std::string& name) {
    const auto exact_child = std::ranges::find_if(
            field.children, [&](const auto& candidate) { return iequal(candidate.name, name); });
    if (exact_child != field.children.end()) {
        return &*exact_child;
    }
    const auto aliased_child = std::ranges::find_if(field.children, [&](const auto& candidate) {
        return std::ranges::any_of(candidate.name_mapping,
                                   [&](const auto& alias) { return iequal(alias, name); });
    });
    return aliased_child == field.children.end() ? nullptr : &*aliased_child;
}

static Status build_v2_initial_default_field(const format::ColumnDefinition& field,
                                             const DataTypePtr& data_type,
                                             std::deque<std::string>* binary_storage,
                                             Field* result);

static Status build_v2_json_default_field(const format::ColumnDefinition& field,
                                          const DataTypePtr& data_type,
                                          const rapidjson::Value& json_value,
                                          std::deque<std::string>* binary_storage, Field* result);

static Status build_v2_json_struct_default(const format::ColumnDefinition& field,
                                           const DataTypePtr& value_type,
                                           const rapidjson::Value& json_value,
                                           std::deque<std::string>* binary_storage, Field* result) {
    if (!json_value.IsObject()) {
        return Status::InvalidArgument("Invalid Iceberg struct default for field '{}'", field.name);
    }

    const auto& struct_type = assert_cast<const DataTypeStruct&>(*value_type);
    Struct struct_value;
    struct_value.reserve(struct_type.get_elements().size());
    for (size_t index = 0; index < struct_type.get_elements().size(); ++index) {
        const auto* child = find_v2_struct_child(field, struct_type.get_element_name(index));
        if (child == nullptr || !child->has_identifier_field_id()) {
            return Status::InvalidArgument(
                    "Iceberg struct default for field '{}' has incomplete child metadata",
                    field.name);
        }

        const std::string child_id = std::to_string(child->get_identifier_field_id());
        const auto member = json_value.FindMember(child_id.c_str());
        Field child_value;
        if (member == json_value.MemberEnd()) {
            RETURN_IF_ERROR(build_v2_initial_default_field(*child, struct_type.get_element(index),
                                                           binary_storage, &child_value));
        } else {
            RETURN_IF_ERROR(build_v2_json_default_field(*child, struct_type.get_element(index),
                                                        member->value, binary_storage,
                                                        &child_value));
        }
        struct_value.push_back(std::move(child_value));
    }
    *result = Field::create_field<TYPE_STRUCT>(std::move(struct_value));
    return Status::OK();
}

// The child ColumnDefinition, recursively transported from the item TField, describes the element
// schema and its field-level default metadata. It cannot represent a particular list literal's
// length or per-position values, so the parent initial-default keeps those values in Iceberg's
// single-value JSON array.
static Status build_v2_json_array_default(const format::ColumnDefinition& field,
                                          const DataTypePtr& value_type,
                                          const rapidjson::Value& json_value,
                                          std::deque<std::string>* binary_storage, Field* result) {
    if (!json_value.IsArray() || field.children.size() != 1) {
        return Status::InvalidArgument("Invalid Iceberg list default for field '{}'", field.name);
    }

    const auto& array_type = assert_cast<const DataTypeArray&>(*value_type);
    Array array_value;
    array_value.reserve(json_value.Size());
    for (const auto& json_element : json_value.GetArray()) {
        Field element_value;
        RETURN_IF_ERROR(build_v2_json_default_field(field.children.front(),
                                                    array_type.get_nested_type(), json_element,
                                                    binary_storage, &element_value));
        array_value.push_back(std::move(element_value));
    }
    *result = Field::create_field<TYPE_ARRAY>(std::move(array_value));
    return Status::OK();
}

// The child ColumnDefinitions, recursively transported from the key/value TFields, describe entry
// schemas and field-level default metadata. They cannot represent the number, order, or concrete
// values of map entries, so the parent initial-default keeps the entries in Iceberg's single-value
// JSON key/value arrays.
static Status build_v2_json_map_default(const format::ColumnDefinition& field,
                                        const DataTypePtr& value_type,
                                        const rapidjson::Value& json_value,
                                        std::deque<std::string>* binary_storage, Field* result) {
    if (!json_value.IsObject() || !json_value.HasMember("keys") || !json_value["keys"].IsArray() ||
        !json_value.HasMember("values") || !json_value["values"].IsArray() ||
        field.children.size() != 2) {
        return Status::InvalidArgument("Invalid Iceberg map default for field '{}'", field.name);
    }
    const auto& keys = json_value["keys"];
    const auto& values = json_value["values"];
    if (keys.Size() != values.Size()) {
        return Status::InvalidArgument(
                "Iceberg map default for field '{}' has {} keys but {} values", field.name,
                keys.Size(), values.Size());
    }

    const auto& map_type = assert_cast<const DataTypeMap&>(*value_type);
    Array key_fields;
    Array value_fields;
    key_fields.reserve(keys.Size());
    value_fields.reserve(values.Size());
    for (rapidjson::SizeType index = 0; index < keys.Size(); ++index) {
        Field key_value;
        Field mapped_value;
        RETURN_IF_ERROR(build_v2_json_default_field(field.children[0], map_type.get_key_type(),
                                                    keys[index], binary_storage, &key_value));
        RETURN_IF_ERROR(build_v2_json_default_field(field.children[1], map_type.get_value_type(),
                                                    values[index], binary_storage, &mapped_value));
        key_fields.push_back(std::move(key_value));
        value_fields.push_back(std::move(mapped_value));
    }
    Map map_value;
    map_value.push_back(Field::create_field<TYPE_ARRAY>(std::move(key_fields)));
    map_value.push_back(Field::create_field<TYPE_ARRAY>(std::move(value_fields)));
    *result = Field::create_field<TYPE_MAP>(std::move(map_value));
    return Status::OK();
}

static Status build_v2_json_scalar_default(const format::ColumnDefinition& field,
                                           const DataTypePtr& value_type,
                                           const rapidjson::Value& json_value,
                                           std::deque<std::string>* binary_storage, Field* result) {
    const auto primitive_type = value_type->get_primitive_type();
    if (primitive_type == TYPE_VARIANT) {
        return Status::InvalidArgument(
                "Iceberg VARIANT initial-default for field '{}' must be NULL", field.name);
    }
    std::string serialized_value = iceberg_json_scalar_text(json_value);
    const bool binary_like =
            field.initial_default_value_is_base64 || primitive_type == TYPE_VARBINARY;
    if (binary_like) {
        if (!json_value.IsString()) {
            return Status::InvalidArgument(
                    "Iceberg binary default for field '{}' is not a JSON string", field.name);
        }
        binary_storage->emplace_back();
        RETURN_IF_ERROR(decode_iceberg_json_binary(serialized_value, &binary_storage->back()));
        if (primitive_type == TYPE_VARBINARY) {
            *result = Field::create_field<TYPE_VARBINARY>(StringView(binary_storage->back()));
        } else if (is_string_type(primitive_type)) {
            *result = Field::create_field<TYPE_STRING>(binary_storage->back());
        } else {
            return Status::InvalidArgument(
                    "Iceberg binary default for field '{}' has incompatible Doris type '{}'",
                    field.name, value_type->get_name());
        }
        return Status::OK();
    }

    if (is_string_type(primitive_type)) {
        if (!json_value.IsString()) {
            return Status::InvalidArgument("Iceberg string default for field '{}' is not a string",
                                           field.name);
        }
        *result = Field::create_field<TYPE_STRING>(std::move(serialized_value));
        return Status::OK();
    }
    normalize_iceberg_json_timestamp(primitive_type, &serialized_value);
    if (doris::iceberg::detail::parse_non_finite_default(primitive_type, serialized_value,
                                                         result)) {
        return Status::OK();
    }
    RETURN_IF_ERROR(value_type->get_serde()->from_fe_string(serialized_value, *result));
    return Status::OK();
}

static Status build_v2_json_default_field(const format::ColumnDefinition& field,
                                          const DataTypePtr& data_type,
                                          const rapidjson::Value& json_value,
                                          std::deque<std::string>* binary_storage, Field* result) {
    DORIS_CHECK(data_type != nullptr);
    DORIS_CHECK(binary_storage != nullptr);
    DORIS_CHECK(result != nullptr);
    if (json_value.IsNull()) {
        return build_v2_null_default(field, data_type, result);
    }

    const auto value_type = remove_nullable(data_type);
    switch (value_type->get_primitive_type()) {
    case TYPE_STRUCT:
        return build_v2_json_struct_default(field, value_type, json_value, binary_storage, result);
    case TYPE_ARRAY:
        return build_v2_json_array_default(field, value_type, json_value, binary_storage, result);
    case TYPE_MAP:
        return build_v2_json_map_default(field, value_type, json_value, binary_storage, result);
    default:
        return build_v2_json_scalar_default(field, value_type, json_value, binary_storage, result);
    }
}

static Status build_v2_initial_default_field(const format::ColumnDefinition& field,
                                             const DataTypePtr& data_type,
                                             std::deque<std::string>* binary_storage,
                                             Field* result) {
    DORIS_CHECK(data_type != nullptr);
    DORIS_CHECK(binary_storage != nullptr);
    DORIS_CHECK(result != nullptr);
    if (!field.initial_default_value.has_value()) {
        if (field.is_optional.has_value() && !*field.is_optional) {
            return Status::InvalidArgument(
                    "Required Iceberg field '{}' is missing from the data file and has no initial "
                    "default",
                    field.name);
        }
        return build_v2_null_default(field, data_type, result);
    }

    const auto value_type = remove_nullable(data_type);
    const auto primitive_type = value_type->get_primitive_type();
    if (primitive_type == TYPE_VARIANT) {
        return Status::InvalidArgument(
                "Iceberg VARIANT initial-default for field '{}' must be NULL", field.name);
    }
    if (is_complex_type(primitive_type)) {
        rapidjson::Document document;
        document.Parse<rapidjson::kParseFullPrecisionFlag>(field.initial_default_value->data(),
                                                           field.initial_default_value->size());
        if (document.HasParseError()) {
            return Status::InvalidArgument("Invalid Iceberg JSON initial default for field '{}'",
                                           field.name);
        }
        return build_v2_json_default_field(field, data_type, document, binary_storage, result);
    }

    if (field.initial_default_value_is_base64 || primitive_type == TYPE_VARBINARY) {
        binary_storage->emplace_back();
        if (!base64_decode(*field.initial_default_value, &binary_storage->back())) {
            return Status::InvalidArgument("Invalid Base64 Iceberg initial default for field {}",
                                           field.name);
        }
        if (primitive_type == TYPE_VARBINARY) {
            *result = Field::create_field<TYPE_VARBINARY>(StringView(binary_storage->back()));
        } else if (is_string_type(primitive_type)) {
            *result = Field::create_field<TYPE_STRING>(binary_storage->back());
        } else {
            return Status::InvalidArgument(
                    "Base64 Iceberg initial default has incompatible Doris type {} for field {}",
                    data_type->get_name(), field.name);
        }
        return Status::OK();
    }

    if (doris::iceberg::detail::parse_non_finite_default(primitive_type,
                                                         *field.initial_default_value, result)) {
        return Status::OK();
    }
    RETURN_IF_ERROR(value_type->get_serde()->from_fe_string(*field.initial_default_value, *result));
    return Status::OK();
}

static Status build_initial_default_literal(const format::ColumnDefinition& table_field,
                                            VExprSPtr* literal) {
    DORIS_CHECK(table_field.type != nullptr);
    DORIS_CHECK(table_field.initial_default_value.has_value());
    DORIS_CHECK(literal != nullptr);

    std::deque<std::string> binary_storage;
    Field initial_default;
    RETURN_IF_ERROR(build_v2_initial_default_field(table_field, table_field.type, &binary_storage,
                                                   &initial_default));
    // VLiteral inserts the Field into an owning column before binary_storage is destroyed.
    *literal = VLiteral::create_shared(table_field.type, initial_default);
    return Status::OK();
}

Status prepare_iceberg_initial_default_exprs(format::ColumnDefinition* column) {
    DORIS_CHECK(column != nullptr);
    if (column->initial_default_value.has_value()) {
        VExprSPtr literal;
        RETURN_IF_ERROR(build_initial_default_literal(*column, &literal));
        column->default_expr = VExprContext::create_shared(std::move(literal));
    }
    for (auto& child : column->children) {
        RETURN_IF_ERROR(prepare_iceberg_initial_default_exprs(&child));
    }
    return Status::OK();
}

static Status build_missing_equality_delete_key_expr(const format::ColumnDefinition& table_field,
                                                     const DataTypePtr& delete_key_type,
                                                     bool require_complete_metadata,
                                                     VExprSPtr* key_expr) {
    DORIS_CHECK(delete_key_type != nullptr);
    DORIS_CHECK(key_expr != nullptr);
    if (!table_field.initial_default_value.has_value()) {
        if (require_complete_metadata && !table_field.is_optional.has_value()) {
            return Status::InvalidArgument(
                    "Iceberg equality delete field '{}' is missing optionality metadata",
                    table_field.name);
        }
        if (table_field.is_optional.has_value() && !*table_field.is_optional) {
            return Status::InvalidArgument("Missing required field: {}", table_field.name);
        }
        // A newly added optional field without an initial default is logically NULL in older
        // files. EqualityDeletePredicate treats NULL == NULL as a match.
        *key_expr = VLiteral::create_shared(make_nullable(delete_key_type), Field());
        return Status::OK();
    }

    VExprSPtr literal;
    RETURN_IF_ERROR(build_initial_default_literal(table_field, &literal));
    if (table_field.type->equals(*delete_key_type)) {
        *key_expr = std::move(literal);
        return Status::OK();
    }
    auto cast_expr = EvolutionCast::create_shared(delete_key_type);
    cast_expr->add_child(literal);
    *key_expr = std::move(cast_expr);
    return Status::OK();
}

static bool find_equality_delete_column_path(const std::vector<format::ColumnDefinition>& fields,
                                             int32_t field_id,
                                             std::vector<const format::ColumnDefinition*>* path) {
    DORIS_CHECK(path != nullptr);
    for (const auto& field : fields) {
        path->push_back(&field);
        if (field.has_identifier_field_id() && field.get_identifier_field_id() == field_id) {
            return true;
        }
        if (find_equality_delete_column_path(field.children, field_id, path)) {
            return true;
        }
        path->pop_back();
    }
    return false;
}

class NestedStructFieldExpr final : public VExpr {
public:
    NestedStructFieldExpr(DataTypePtr data_type, std::vector<size_t> child_indexes,
                          std::string expr_name)
            : VExpr(std::move(data_type), false),
              _child_indexes(std::move(child_indexes)),
              _expr_name(std::move(expr_name)) {
        _node_type = TExprNodeType::FUNCTION_CALL;
    }

    Status prepare(RuntimeState* state, const RowDescriptor& row_desc,
                   VExprContext* context) override {
        RETURN_IF_ERROR_OR_PREPARED(VExpr::prepare(state, row_desc, context));
        _prepare_finished = true;
        return Status::OK();
    }

    Status open(RuntimeState* state, VExprContext* context,
                FunctionContext::FunctionStateScope scope) override {
        RETURN_IF_ERROR_OR_PREPARED(VExpr::open(state, context, scope));
        _open_finished = true;
        return Status::OK();
    }

    void close(VExprContext* context, FunctionContext::FunctionStateScope scope) override {
        VExpr::close(context, scope);
    }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        DORIS_CHECK(_children.size() == 1);
        ColumnPtr current;
        RETURN_IF_ERROR(
                _children.front()->execute_column(context, block, selector, count, current));
        current = current->convert_to_full_column_if_const();

        std::vector<ColumnPtr> ancestor_nullable_columns;
        std::vector<const NullMap*> ancestor_null_maps;
        for (const size_t child_index : _child_indexes) {
            if (const auto* nullable = check_and_get_column<ColumnNullable>(*current);
                nullable != nullptr) {
                ancestor_nullable_columns.push_back(current);
                ancestor_null_maps.push_back(&nullable->get_null_map_data());
                current = nullable->get_nested_column_ptr();
            }
            const auto* struct_column = check_and_get_column<ColumnStruct>(*current);
            DORIS_CHECK(struct_column != nullptr);
            DORIS_CHECK(child_index < struct_column->tuple_size());
            current = struct_column->get_column_ptr(child_index);
        }
        if (const auto* nullable = check_and_get_column<ColumnNullable>(*current);
            nullable != nullptr) {
            ancestor_nullable_columns.push_back(current);
            ancestor_null_maps.push_back(&nullable->get_null_map_data());
            current = nullable->get_nested_column_ptr();
        }
        if (ancestor_null_maps.empty()) {
            result_column = make_nullable(current);
            return Status::OK();
        }

        auto result = ColumnNullable::create(remove_nullable(_data_type)->create_column(),
                                             ColumnUInt8::create());
        auto& result_data = result->get_nested_column();
        auto& result_null_map = result->get_null_map_data();
        result_data.reserve(count);
        result_null_map.reserve(count);
        for (size_t row = 0; row < count; ++row) {
            const bool is_null =
                    std::ranges::any_of(ancestor_null_maps, [row](const NullMap* null_map) {
                        DORIS_CHECK(null_map != nullptr);
                        DORIS_CHECK(row < null_map->size());
                        return (*null_map)[row] != 0;
                    });
            if (is_null) {
                result_data.insert_default();
                result_null_map.push_back(1);
            } else {
                result_data.insert_from(*current, row);
                result_null_map.push_back(0);
            }
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

    Status clone_node(VExprSPtr* cloned_expr) const override {
        DORIS_CHECK(cloned_expr != nullptr);
        *cloned_expr = std::make_shared<NestedStructFieldExpr>(*this);
        return Status::OK();
    }

private:
    std::vector<size_t> _child_indexes;
    std::string _expr_name;
};

class AncestorNullDefaultExpr final : public VExpr {
public:
    AncestorNullDefaultExpr(DataTypePtr data_type, std::string expr_name)
            : VExpr(std::move(data_type), false), _expr_name(std::move(expr_name)) {
        _node_type = TExprNodeType::FUNCTION_CALL;
    }

    Status prepare(RuntimeState* state, const RowDescriptor& row_desc,
                   VExprContext* context) override {
        RETURN_IF_ERROR_OR_PREPARED(VExpr::prepare(state, row_desc, context));
        _prepare_finished = true;
        return Status::OK();
    }

    Status open(RuntimeState* state, VExprContext* context,
                FunctionContext::FunctionStateScope scope) override {
        DCHECK(_prepare_finished);
        // Opens the children, as EqualityDeletePredicate does. RETURN_IF_ERROR_OR_PREPARED would
        // return at once, since prepare() has set `_prepared`, leaving a default cast to the
        // comparison type, e.g. the TIME text `00:00` parsed as TIMEV2, unopened, so executing it
        // fails "Check failed: _open_finished" in Cast.
        RETURN_IF_ERROR(VExpr::open(state, context, scope));
        _open_finished = true;
        return Status::OK();
    }

    void close(VExprContext* context, FunctionContext::FunctionStateScope scope) override {
        VExpr::close(context, scope);
    }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        DORIS_CHECK(_children.size() == 2);
        ColumnPtr ancestor;
        RETURN_IF_ERROR(
                _children.front()->execute_column(context, block, selector, count, ancestor));
        ancestor = ancestor->convert_to_full_column_if_const();
        const NullMap* ancestor_null_map = nullptr;
        if (const auto* nullable = check_and_get_column<ColumnNullable>(*ancestor);
            nullable != nullptr) {
            ancestor_null_map = &nullable->get_null_map_data();
        }

        ColumnPtr default_value;
        RETURN_IF_ERROR(
                _children.back()->execute_column(context, block, selector, count, default_value));
        default_value = default_value->convert_to_full_column_if_const();
        const NullMap* default_null_map = nullptr;
        const IColumn* default_data = default_value.get();
        if (const auto* nullable = check_and_get_column<ColumnNullable>(*default_value);
            nullable != nullptr) {
            default_null_map = &nullable->get_null_map_data();
            default_data = &nullable->get_nested_column();
        }

        auto result = ColumnNullable::create(remove_nullable(_data_type)->create_column(),
                                             ColumnUInt8::create());
        auto& result_data = result->get_nested_column();
        auto& result_null_map = result->get_null_map_data();
        result_data.reserve(count);
        result_null_map.reserve(count);
        for (size_t row = 0; row < count; ++row) {
            const bool ancestor_is_null =
                    ancestor_null_map != nullptr && (*ancestor_null_map)[row] != 0;
            const bool default_is_null =
                    default_null_map != nullptr && (*default_null_map)[row] != 0;
            if (ancestor_is_null || default_is_null) {
                result_data.insert_default();
                result_null_map.push_back(1);
            } else {
                result_data.insert_from(*default_data, row);
                result_null_map.push_back(0);
            }
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

    Status clone_node(VExprSPtr* cloned_expr) const override {
        DORIS_CHECK(cloned_expr != nullptr);
        *cloned_expr = std::make_shared<AncestorNullDefaultExpr>(*this);
        return Status::OK();
    }

private:
    std::string _expr_name;
};

// Converts an Iceberg TIME read from ORC, microseconds from midnight as BIGINT, to TIMEV2, which
// holds microseconds as well (as a double, exactly): 3600000001 becomes 01:00:00.000001. An
// ordinary CAST(BIGINT AS TIME) reads the integer's decimal digits as HHMMSS instead, turning
// 3600 into 00:36:00. A value outside 00:00:00..23:59:59.999999 is no Iceberg TIME and fails, as
// the strict comparison casts do.
class IcebergTimeMicrosExpr final : public VExpr {
public:
    IcebergTimeMicrosExpr(DataTypePtr data_type, std::string expr_name)
            : VExpr(std::move(data_type), false), _expr_name(std::move(expr_name)) {
        _node_type = TExprNodeType::FUNCTION_CALL;
        DORIS_CHECK(remove_nullable(_data_type)->get_primitive_type() == TYPE_TIMEV2);
    }

    Status prepare(RuntimeState* state, const RowDescriptor& row_desc,
                   VExprContext* context) override {
        RETURN_IF_ERROR_OR_PREPARED(VExpr::prepare(state, row_desc, context));
        _prepare_finished = true;
        return Status::OK();
    }

    Status open(RuntimeState* state, VExprContext* context,
                FunctionContext::FunctionStateScope scope) override {
        DCHECK(_prepare_finished);
        // As in AncestorNullDefaultExpr: the child, e.g. a nested key's NestedStructFieldExpr,
        // must be opened, which RETURN_IF_ERROR_OR_PREPARED would skip after prepare().
        RETURN_IF_ERROR(VExpr::open(state, context, scope));
        _open_finished = true;
        return Status::OK();
    }

    void close(VExprContext* context, FunctionContext::FunctionStateScope scope) override {
        VExpr::close(context, scope);
    }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        DORIS_CHECK(_children.size() == 1);
        ColumnPtr input;
        RETURN_IF_ERROR(_children.front()->execute_column(context, block, selector, count, input));
        input = input->convert_to_full_column_if_const();
        DORIS_CHECK(input->size() == count);
        const NullMap* null_map = nullptr;
        const IColumn* micros_column = input.get();
        if (const auto* nullable = check_and_get_column<ColumnNullable>(*input);
            nullable != nullptr) {
            null_map = &nullable->get_null_map_data();
            micros_column = &nullable->get_nested_column();
        }
        DORIS_CHECK(_data_type->is_nullable() == (null_map != nullptr));
        const auto& micros = assert_cast<const ColumnInt64&>(*micros_column).get_data();
        auto times = ColumnTimeV2::create();
        auto& values = times->get_data();
        values.resize_fill(count, 0);
        for (size_t row = 0; row < count; ++row) {
            if (null_map != nullptr && (*null_map)[row] != 0) {
                continue;
            }
            if (micros[row] < 0 || micros[row] >= MICROS_PER_DAY) {
                return Status::InvalidArgument(
                        "Iceberg TIME value {} of {} is outside 00:00:00..23:59:59.999999",
                        micros[row], _expr_name);
            }
            values[row] = static_cast<double>(micros[row]);
        }
        if (null_map == nullptr) {
            result_column = std::move(times);
        } else {
            auto nulls = ColumnUInt8::create();
            nulls->get_data().assign(null_map->begin(), null_map->end());
            result_column = ColumnNullable::create(std::move(times), std::move(nulls));
        }
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

    Status clone_node(VExprSPtr* cloned_expr) const override {
        DORIS_CHECK(cloned_expr != nullptr);
        *cloned_expr = std::make_shared<IcebergTimeMicrosExpr>(*this);
        return Status::OK();
    }

private:
    static constexpr int64_t MICROS_PER_DAY = 24 * TimeValue::ONE_HOUR_MICROSECONDS;

    std::string _expr_name;
};

// The struct element of `parent` that holds `child` on an equality key's path: matched by field id
// when both carry one, otherwise by name.
static size_t equality_delete_child_index(const format::ColumnDefinition& parent,
                                          const format::ColumnDefinition& child) {
    const auto child_it = std::ranges::find_if(parent.children, [&child](const auto& candidate) {
        if (candidate.has_identifier_field_id() && child.has_identifier_field_id()) {
            return candidate.get_identifier_field_id() == child.get_identifier_field_id();
        }
        return candidate.name == child.name;
    });
    DORIS_CHECK(child_it != parent.children.end());
    return cast_set<size_t>(child_it - parent.children.begin());
}

static Status build_nested_equality_delete_key_expr(
        const std::vector<const format::ColumnDefinition*>& path, VExprSPtr root_expr,
        VExprSPtr* key_expr) {
    DORIS_CHECK(!path.empty());
    DORIS_CHECK(root_expr != nullptr);
    DORIS_CHECK(key_expr != nullptr);
    const auto* root = path.front();
    DORIS_CHECK(root != nullptr);
    DORIS_CHECK(root->type != nullptr);
    VExprSPtr result = std::move(root_expr);
    std::vector<size_t> child_indexes;
    std::string expr_name = root->name;
    for (size_t index = 1; index < path.size(); ++index) {
        const auto* parent = path[index - 1];
        const auto* child = path[index];
        DORIS_CHECK(parent != nullptr);
        DORIS_CHECK(child != nullptr);
        DORIS_CHECK(parent->type != nullptr);
        DORIS_CHECK(child->type != nullptr);
        if (remove_nullable(parent->type)->get_primitive_type() != TYPE_STRUCT) {
            return Status::NotSupported(
                    "Iceberg equality delete field {} has non-struct ancestor {}", child->name,
                    parent->name);
        }
        child_indexes.push_back(equality_delete_child_index(*parent, *child));
        expr_name += "." + child->name;
    }
    if (!child_indexes.empty()) {
        auto nested_field = std::make_shared<NestedStructFieldExpr>(
                make_nullable(path.back()->type), std::move(child_indexes), std::move(expr_name));
        nested_field->add_child(result);
        result = std::move(nested_field);
    }
    *key_expr = std::move(result);
    return Status::OK();
}

static Status build_equality_delete_key_expr(
        const std::vector<const format::ColumnDefinition*>& path, size_t block_position,
        VExprSPtr* key_expr) {
    DORIS_CHECK(!path.empty());
    const auto* root = path.front();
    DORIS_CHECK(root != nullptr);
    DORIS_CHECK(root->type != nullptr);
    VExprSPtr root_expr =
            VSlotRef::create_shared(cast_set<int>(block_position), cast_set<int>(block_position),
                                    -1, root->type, root->name);
    return build_nested_equality_delete_key_expr(path, std::move(root_expr), key_expr);
}

// Whether a missing key's default is NULL. `missing_path` runs from the missing root to the key,
// and the root's initial default is walked down it with the key expression's child matching. The
// root having a default is not enough: a missing `payload` defaulting to the struct `{}` takes
// each child's own default, so payload.k is NULL when `k` has none.
static Status equality_delete_default_leaf_is_null(
        const std::vector<const format::ColumnDefinition*>& missing_path, bool* is_null) {
    DORIS_CHECK(!missing_path.empty());
    DORIS_CHECK(is_null != nullptr);
    const auto* root = missing_path.front();
    DORIS_CHECK(root != nullptr);
    if (!root->initial_default_value.has_value()) {
        *is_null = true;
        return Status::OK();
    }
    std::deque<std::string> binary_storage;
    Field root_default;
    RETURN_IF_ERROR(
            build_v2_initial_default_field(*root, root->type, &binary_storage, &root_default));
    const Field* value = &root_default;
    for (size_t index = 1; index < missing_path.size() && !value->is_null(); ++index) {
        DORIS_CHECK(value->get_type() == TYPE_STRUCT);
        const auto& children = value->get<TYPE_STRUCT>();
        const size_t child_index =
                equality_delete_child_index(*missing_path[index - 1], *missing_path[index]);
        DORIS_CHECK(child_index < children.size());
        value = &children[child_index];
    }
    *is_null = value->is_null();
    return Status::OK();
}

static int equality_delete_integer_width(PrimitiveType type) {
    switch (type) {
    case TYPE_TINYINT:
        return 1;
    case TYPE_SMALLINT:
        return 2;
    case TYPE_INT:
        return 4;
    case TYPE_BIGINT:
        return 8;
    case TYPE_LARGEINT:
        return 16;
    default:
        return 0;
    }
}

// Whether every `from` value converts exactly to a `to` value that compares equal to the same
// Iceberg value. These are the Iceberg promotions (INT -> LONG, FLOAT -> DOUBLE, decimal
// precision, DATE -> TIMESTAMP and DATE -> TIMESTAMP_NS), a wider fractional scale of the same
// date-time kind, and STRING -> VARBINARY: an Iceberg BINARY key decodes as either one, depending
// on enable_mapping_varbinary, and both hold the same bytes. TIMESTAMP -> TIMESTAMP_NS is not an
// Iceberg promotion, but a delete can be prepared under a nanosecond schema that the table later
// abandons. Scaling microseconds by 1000 is exact, whereas rounding the tombstone
// 12:00:00.000000500 to microseconds would delete the data key 12:00:00.000000.
static bool is_equality_delete_widening(const DataTypePtr& from, const DataTypePtr& to) {
    const auto from_type = remove_nullable(from);
    const auto to_type = remove_nullable(to);
    if (from_type->equals(*to_type)) {
        return true;
    }
    const auto from_primitive = from_type->get_primitive_type();
    const auto to_primitive = to_type->get_primitive_type();
    if (const int from_width = equality_delete_integer_width(from_primitive); from_width > 0) {
        return equality_delete_integer_width(to_primitive) >= from_width;
    }
    switch (from_primitive) {
    case TYPE_FLOAT:
        return to_primitive == TYPE_DOUBLE;
    case TYPE_DECIMAL32:
    case TYPE_DECIMAL64:
    case TYPE_DECIMAL128I:
    case TYPE_DECIMAL256:
        return is_decimalv3(to_primitive) && to_type->get_scale() == from_type->get_scale() &&
               to_type->get_precision() >= from_type->get_precision();
    case TYPE_DATEV2:
        return to_primitive == TYPE_DATETIMEV2 || to_primitive == TYPE_TIMESTAMP_NS;
    case TYPE_DATETIMEV2:
        return (to_primitive == TYPE_DATETIMEV2 &&
                to_type->get_scale() >= from_type->get_scale()) ||
               to_primitive == TYPE_TIMESTAMP_NS;
    case TYPE_TIMESTAMPTZ:
    case TYPE_TIMEV2:
        return to_primitive == from_primitive && to_type->get_scale() >= from_type->get_scale();
    case TYPE_VARCHAR:
        return to_primitive == TYPE_STRING || to_primitive == TYPE_VARBINARY;
    case TYPE_STRING:
        return to_primitive == TYPE_VARBINARY;
    default:
        return false;
    }
}

// Iceberg equality is by value, so a key is compared in the wider of the delete file's and the
// data file's key types, and neither side is narrowed. One scan-wide table type cannot serve: after
// expireSnapshots().cleanExpiredMetadata(true) drops the schema of a schema-only INT -> LONG
// promotion, the table type of a dropped key is INT while its tombstones are LONG, and casting
// 4294967296 to INT yields NULL, which deletes the rows whose key is NULL. `table_type` is the
// key's type in the scan's schema metadata; it decides only how a timestamptz field read as
// DATETIMEV2 and a TIME field behind a STRING placeholder compare.
static Status equality_delete_comparison_type(int field_id, const DataTypePtr& delete_type,
                                              const DataTypePtr& data_type,
                                              const DataTypePtr& table_type,
                                              const std::string& data_file_path,
                                              DataTypePtr* comparison_type) {
    DORIS_CHECK(delete_type != nullptr);
    DORIS_CHECK(data_type != nullptr);
    DORIS_CHECK(table_type != nullptr);
    DORIS_CHECK(comparison_type != nullptr);
    if (is_equality_delete_widening(delete_type, data_type)) {
        *comparison_type = remove_nullable(data_type);
        return Status::OK();
    }
    if (is_equality_delete_widening(data_type, delete_type)) {
        *comparison_type = remove_nullable(delete_type);
        return Status::OK();
    }
    const auto delete_value_type = remove_nullable(delete_type);
    const auto data_value_type = remove_nullable(data_type);
    const auto delete_primitive = delete_value_type->get_primitive_type();
    const auto data_primitive = data_value_type->get_primitive_type();
    // Parquet INT96 carries no instant annotation, so a timestamptz key in a Spark or Hive file
    // that Iceberg imported reads as DATETIMEV2(6), while an adjusted-UTC tombstone of the same
    // field, as a Flink upsert writes it, reads as TIMESTAMPTZ(6). Neither widens to the other.
    // The projection converts that DATETIMEV2 to the table's TIMESTAMPTZ in the session time zone
    // and the comparison cast does the same, so a tombstone matches exactly the value a query
    // returns: in Asia/Shanghai the key 2024-01-01 08:00:00 compares as 2024-01-01 00:00:00 UTC.
    if (remove_nullable(table_type)->get_primitive_type() == TYPE_TIMESTAMPTZ &&
        ((delete_primitive == TYPE_DATETIMEV2 && data_primitive == TYPE_TIMESTAMPTZ) ||
         (delete_primitive == TYPE_TIMESTAMPTZ && data_primitive == TYPE_DATETIMEV2))) {
        *comparison_type = std::make_shared<DataTypeTimeStampTz>(
                std::max(delete_value_type->get_scale(), data_value_type->get_scale()));
        return Status::OK();
    }
    // The FE sends an Iceberg TIME, which Doris cannot represent, as a STRING placeholder, so only
    // the files type its values: microseconds from midnight in both formats, but ORC stores them as
    // a LONG annotated iceberg.long-type=TIME, which reads as BIGINT, while Parquet's TIME(MICROS)
    // reads as TIMEV2(6). Such a pair compares as TIMEV2(6), which holds microseconds too, so the
    // ORC key 3600000001 matches the Parquet tombstone 01:00:00.000001; cast_equality_delete_key
    // converts the BIGINT side without a SQL cast. Under any other table type the pair fails below.
    if (remove_nullable(table_type)->get_primitive_type() == TYPE_STRING &&
        ((delete_primitive == TYPE_BIGINT && data_primitive == TYPE_TIMEV2) ||
         (delete_primitive == TYPE_TIMEV2 && data_primitive == TYPE_BIGINT))) {
        *comparison_type = std::make_shared<DataTypeTimeV2>(TimeValue::MICROS_SCALE);
        return Status::OK();
    }
    // No promotion leads from one type to the other, so no third type compares them faithfully:
    // STRING tombstones cast to an INT key would delete the row with key 1 for the string "1".
    return Status::NotSupported(
            "Iceberg equality delete field id {} is {} in the delete file and {} for data file "
            "{}, and neither type converts exactly to the other",
            field_id, remove_nullable(delete_type)->get_name(),
            remove_nullable(data_type)->get_name(), data_file_path);
}

// The comparison type of a missing key whose non-NULL initial default sits behind the STRING
// placeholder of an Iceberg TIME (see equality_delete_comparison_type), or nullptr for any other
// table or tombstone type. The default is Iceberg's text of the value, `00:00` for midnight, with at
// most six fractional digits, so the strict comparison cast parses it into the tombstones' type
// exactly, and text that does not parse fails the scan; an ORC TIME tombstone, BIGINT, compares in
// TIMEV2(6) as a present key does. Compared as text, the default `00:00` never matched a tombstone
// printing 00:00:00.000000. A timestamptz_ns default is rejected by the caller instead.
static DataTypePtr equality_delete_placeholder_default_type(const DataTypePtr& table_type,
                                                            const DataTypePtr& delete_type) {
    if (remove_nullable(table_type)->get_primitive_type() != TYPE_STRING) {
        return nullptr;
    }
    switch (remove_nullable(delete_type)->get_primitive_type()) {
    case TYPE_TIMEV2:
        return remove_nullable(delete_type);
    case TYPE_BIGINT:
        return std::make_shared<DataTypeTimeV2>(TimeValue::MICROS_SCALE);
    default:
        return nullptr;
    }
}

// Makes `key_expr` yield `comparison_type`, keeping its nullability.
static VExprSPtr cast_equality_delete_key(VExprSPtr key_expr, const DataTypePtr& comparison_type) {
    DORIS_CHECK(key_expr != nullptr);
    DORIS_CHECK(comparison_type != nullptr);
    const auto& source_type = key_expr->data_type();
    if (remove_nullable(source_type)->equals(*remove_nullable(comparison_type))) {
        return key_expr;
    }
    const auto result_type = source_type->is_nullable() ? make_nullable(comparison_type)
                                                        : remove_nullable(comparison_type);
    // Only an ORC Iceberg TIME key compares a BIGINT as TIMEV2 (equality_delete_comparison_type),
    // and its value is microseconds, which CAST(BIGINT AS TIME) would read as HHMMSS digits.
    if (remove_nullable(source_type)->get_primitive_type() == TYPE_BIGINT &&
        remove_nullable(comparison_type)->get_primitive_type() == TYPE_TIMEV2) {
        auto time_expr =
                std::make_shared<IcebergTimeMicrosExpr>(result_type, key_expr->expr_name());
        time_expr->add_child(std::move(key_expr));
        return time_expr;
    }
    auto cast_expr = EvolutionCast::create_shared(result_type, false, true);
    cast_expr->add_child(std::move(key_expr));
    return cast_expr;
}

Status IcebergTableReader::annotate_projected_column(const TFileScanSlotInfo& slot_info,
                                                     format::ProjectedColumnBuildContext* context,
                                                     format::ColumnDefinition* column) const {
    RETURN_IF_ERROR(format::TableReader::annotate_projected_column(slot_info, context, column));
    DORIS_CHECK(context != nullptr);
    DORIS_CHECK(column != nullptr);
    if (!supports_iceberg_scan_semantics_v2(context->scan_params)) {
        return Status::OK();
    }
    if (!context->schema_column.has_value()) {
        return Status::OK();
    }

    auto& schema_column = *context->schema_column;
    RETURN_IF_ERROR(prepare_iceberg_initial_default_exprs(&schema_column));
    column->initial_default_value = schema_column.initial_default_value;
    column->initial_default_value_is_base64 = schema_column.initial_default_value_is_base64;
    column->is_optional = schema_column.is_optional;
    if (schema_column.default_expr != nullptr) {
        // The Iceberg typed literal is authoritative. In particular, this replaces FE's generic
        // string expression for Base64-transported UUID/BINARY/FIXED defaults.
        column->default_expr = schema_column.default_expr;
    } else if (schema_column.is_optional.has_value() && !*schema_column.is_optional) {
        // FE's generic external-column metadata currently treats Iceberg columns as nullable. Clear
        // that fallback so a physically missing required field is rejected by the Iceberg mapper.
        column->default_expr = nullptr;
    }
    return Status::OK();
}

static std::string iceberg_delete_file_debug_string(const TIcebergDeleteFileDesc& delete_file) {
    std::ostringstream out;
    out << "TIcebergDeleteFileDesc{path=" << (delete_file.__isset.path ? delete_file.path : "null")
        << ", content=" << (delete_file.__isset.content ? delete_file.content : -1)
        << ", file_format="
        << (delete_file.__isset.file_format ? static_cast<int>(delete_file.file_format) : -1)
        << ", position_lower_bound="
        << (delete_file.__isset.position_lower_bound ? delete_file.position_lower_bound : -1)
        << ", position_upper_bound="
        << (delete_file.__isset.position_upper_bound ? delete_file.position_upper_bound : -1)
        << ", field_ids="
        << (delete_file.__isset.field_ids ? join_values_for_debug(delete_file.field_ids) : "[]")
        << ", content_offset="
        << (delete_file.__isset.content_offset ? delete_file.content_offset : -1)
        << ", content_size_in_bytes="
        << (delete_file.__isset.content_size_in_bytes ? delete_file.content_size_in_bytes : -1)
        << "}";
    return out.str();
}

static std::string iceberg_delete_files_debug_string(
        const std::vector<TIcebergDeleteFileDesc>& delete_files) {
    std::ostringstream out;
    out << "[";
    for (size_t idx = 0; idx < delete_files.size(); ++idx) {
        if (idx > 0) {
            out << ", ";
        }
        out << iceberg_delete_file_debug_string(delete_files[idx]);
    }
    out << "]";
    return out.str();
}

static std::string iceberg_params_debug_string(const std::optional<TIcebergFileDesc>& params) {
    if (!params.has_value()) {
        return "null";
    }
    const auto& iceberg_params = *params;
    std::ostringstream out;
    out << "TIcebergFileDesc{format_version="
        << (iceberg_params.__isset.format_version ? iceberg_params.format_version : -1)
        << ", content=" << (iceberg_params.__isset.content ? iceberg_params.content : -1)
        << ", original_file_path="
        << (iceberg_params.__isset.original_file_path ? iceberg_params.original_file_path : "null")
        << ", row_count=" << (iceberg_params.__isset.row_count ? iceberg_params.row_count : -1)
        << ", partition_spec_id="
        << (iceberg_params.__isset.partition_spec_id ? iceberg_params.partition_spec_id : 0)
        << ", has_partition_data_json=" << iceberg_params.__isset.partition_data_json
        << ", first_row_id="
        << (iceberg_params.__isset.first_row_id ? iceberg_params.first_row_id : -1)
        << ", last_updated_sequence_number="
        << (iceberg_params.__isset.last_updated_sequence_number
                    ? iceberg_params.last_updated_sequence_number
                    : -1)
        << ", delete_file_count="
        << (iceberg_params.__isset.delete_files ? iceberg_params.delete_files.size() : 0)
        << ", delete_files="
        << (iceberg_params.__isset.delete_files
                    ? iceberg_delete_files_debug_string(iceberg_params.delete_files)
                    : "[]")
        << ", has_serialized_split=" << iceberg_params.__isset.serialized_split << "}";
    return out.str();
}

IcebergTableReader::PositionDeleteRowsCollector::PositionDeleteRowsCollector(
        PositionDeleteFile* rows_by_data_file)
        : _rows_by_data_file(rows_by_data_file) {
    DORIS_CHECK(_rows_by_data_file != nullptr);
}

Status IcebergTableReader::PositionDeleteRowsCollector::collect(const Block& block,
                                                                size_t read_rows) {
    if (read_rows == 0) {
        return Status::OK();
    }
    const auto& file_path_column_ptr =
            block.get_by_position(ICEBERG_FILE_PATH_BLOCK_POSITION).column;
    const auto& pos_column_ptr = block.get_by_position(ICEBERG_ROW_POS_BLOCK_POSITION).column;
    if (const auto* nullable_column = check_and_get_column<ColumnNullable>(*file_path_column_ptr);
        nullable_column != nullptr && nullable_column->has_null(0, read_rows)) {
        return Status::Corruption("Iceberg position delete column file_path contains null values");
    }
    if (const auto* nullable_column = check_and_get_column<ColumnNullable>(*pos_column_ptr);
        nullable_column != nullptr && nullable_column->has_null(0, read_rows)) {
        return Status::Corruption("Iceberg position delete column pos contains null values");
    }
    const auto& file_path_column =
            assert_cast<const ColumnString&>(*remove_nullable(file_path_column_ptr));
    const auto& pos_column = assert_cast<const ColumnInt64&>(*remove_nullable(pos_column_ptr));
    for (size_t row = 0; row < read_rows; ++row) {
        const auto file_path = file_path_column.get_data_at(row).to_string();
        (*_rows_by_data_file)[file_path].push_back(pos_column.get_element(row));
    }
    return Status::OK();
}

Status IcebergTableReader::prepare_split(const format::SplitReadOptions& options) {
    {
        SCOPED_TIMER(_profile.total_timer);
        SCOPED_TIMER(_profile.prepare_split_timer);
        _row_lineage_columns = {};
        _iceberg_params.reset();
        _delete_predicates_initialized = false;
        // Per-file diagnostic: the reader instance is reused across splits, so a previous ID-less
        // file must not leave this set for the next (possibly ID-bearing) split.
        _positional_id_fallback = false;
        _position_delete_rows_storage.clear();
        _equality_delete_filters.clear();
        _split_cache = options.cache;
        if (options.current_range.__isset.table_format_params &&
            options.current_range.table_format_params.__isset.iceberg_params) {
            const auto& iceberg_params = options.current_range.table_format_params.iceberg_params;
            _iceberg_params = iceberg_params;
            if (iceberg_params.__isset.first_row_id) {
                _row_lineage_columns.first_row_id = iceberg_params.first_row_id;
            }
            if (iceberg_params.__isset.last_updated_sequence_number) {
                _row_lineage_columns.last_updated_sequence_number =
                        iceberg_params.last_updated_sequence_number;
            }
        }
    }
    RETURN_IF_ERROR(TableReader::prepare_split(options));
    SCOPED_TIMER(_profile.total_timer);
    SCOPED_TIMER(_profile.prepare_split_timer);
    if (current_split_pruned()) {
        return Status::OK();
    }
    // Iceberg data files are immutable once referenced by a snapshot; updates create new data files
    // at new paths instead of overwriting existing files. This lets the Parquet V2 reader use page
    // cache when the scan range does not carry an mtime, without extending V1's path::0 behavior to
    // mutable Hive/local files.
    mark_current_data_file_immutable();
    if (_is_table_level_count_active()) {
        return Status::OK();
    }
    DBUG_EXECUTE_IF("IcebergTableReader.prepare_split.before_delete_file_scan",
                    DBUG_RUN_CALLBACK());
    RETURN_IF_ERROR(_init_delete_predicates(options.current_range.table_format_params));
    return Status::OK();
}

std::string IcebergTableReader::debug_string() const {
    size_t position_delete_file_count = 0;
    size_t equality_delete_file_count = 0;
    size_t deletion_vector_file_count = 0;
    if (_iceberg_params.has_value() && _iceberg_params->__isset.delete_files) {
        for (const auto& delete_file : _iceberg_params->delete_files) {
            if (!delete_file.__isset.content) {
                continue;
            }
            if (delete_file.content == POSITION_DELETE) {
                ++position_delete_file_count;
            } else if (delete_file.content == EQUALITY_DELETE) {
                ++equality_delete_file_count;
            } else if (delete_file.content == DELETION_VECTOR) {
                ++deletion_vector_file_count;
            }
        }
    }

    std::ostringstream equality_filters;
    equality_filters << "[";
    for (size_t idx = 0; idx < _equality_delete_filters.size(); ++idx) {
        if (idx > 0) {
            equality_filters << ", ";
        }
        const auto& filter = _equality_delete_filters[idx];
        const auto append_types = [&equality_filters](const std::vector<DataTypePtr>& types) {
            for (size_t type_idx = 0; type_idx < types.size(); ++type_idx) {
                if (type_idx > 0) {
                    equality_filters << ", ";
                }
                equality_filters << (types[type_idx] == nullptr ? "null"
                                                                : types[type_idx]->get_name());
            }
        };
        equality_filters << "EqualityDeleteFilter{field_ids="
                         << join_values_for_debug(filter.field_ids) << ", key_types=[";
        append_types(filter.key_types);
        equality_filters << "], delete_key_types=[";
        append_types(filter.delete_key_types);
        equality_filters << "], delete_block_rows=" << filter.delete_block.rows()
                         << ", delete_block_columns=" << filter.delete_block.columns() << "}";
    }
    equality_filters << "]";

    std::ostringstream out;
    out << "IcebergTableReader{base=" << format::TableReader::debug_string()
        << ", iceberg_params=" << iceberg_params_debug_string(_iceberg_params)
        << ", row_lineage_first_row_id=" << _row_lineage_columns.first_row_id
        << ", row_lineage_last_updated_sequence_number="
        << _row_lineage_columns.last_updated_sequence_number
        << ", need_row_lineage_row_id=" << _need_row_lineage_row_id()
        << ", need_iceberg_rowid=" << _need_iceberg_rowid()
        << ", row_position_block_position=" << _row_position_block_position
        << ", delete_predicates_initialized=" << _delete_predicates_initialized
        << ", position_delete_file_count=" << position_delete_file_count
        << ", equality_delete_file_count=" << equality_delete_file_count
        << ", deletion_vector_file_count=" << deletion_vector_file_count
        << ", position_delete_rows_storage_count=" << _position_delete_rows_storage.size()
        << ", equality_delete_filter_count=" << _equality_delete_filters.size()
        << ", positional_id_fallback=" << _positional_id_fallback
        << ", equality_delete_filters=" << equality_filters.str() << "}";
    return out.str();
}

Status IcebergTableReader::materialize_virtual_columns(Block* table_block) {
    for (size_t column_idx = 0; column_idx < _data_reader.column_mapper->mappings().size();
         ++column_idx) {
        const auto& mapping = _data_reader.column_mapper->mappings()[column_idx];
        switch (mapping.virtual_column_type) {
        case format::TableVirtualColumnType::ROW_ID:
            RETURN_IF_ERROR(_materialize_row_lineage_row_id(table_block, column_idx));
            break;
        case format::TableVirtualColumnType::LAST_UPDATED_SEQUENCE_NUMBER:
            RETURN_IF_ERROR(
                    _materialize_row_lineage_last_updated_sequence_number(table_block, column_idx));
            break;
        case format::TableVirtualColumnType::ICEBERG_ROWID:
            RETURN_IF_ERROR(_materialize_iceberg_rowid(table_block, column_idx));
            break;
        case format::TableVirtualColumnType::ICEBERG_FILE_PATH:
            RETURN_IF_ERROR(_materialize_iceberg_file_path(table_block, column_idx));
            break;
        case format::TableVirtualColumnType::ICEBERG_ROW_POSITION:
            RETURN_IF_ERROR(_materialize_iceberg_row_position(table_block, column_idx));
            break;
        case format::TableVirtualColumnType::PAIMON_FILE_PATH:
        case format::TableVirtualColumnType::PAIMON_ROW_POSITION:
            DORIS_CHECK(false);
            break;
        case format::TableVirtualColumnType::INVALID:
            break;
        }
    }
    return Status::OK();
}

Status IcebergTableReader::customize_file_scan_request(format::FileScanRequest* file_request) {
    RETURN_IF_ERROR(TableReader::customize_file_scan_request(file_request));
    if ((_row_lineage_columns.first_row_id >= 0 && _need_row_lineage_row_id()) ||
        _need_iceberg_rowid() || _need_iceberg_metadata()) {
        RETURN_IF_ERROR(_append_row_position_output_column(file_request));
    }
    RETURN_IF_ERROR(_append_equality_delete_predicates(file_request));
    return Status::OK();
}

bool IcebergTableReader::_supports_aggregate_pushdown(TPushAggOp::type agg_type) const {
    if (!TableReader::_supports_aggregate_pushdown(agg_type)) {
        return false;
    }
    return _equality_delete_filters.empty();
}

Status IcebergTableReader::_parse_deletion_vector_file(const TTableFormatFileDesc& t_desc,
                                                       DeleteFileDesc* desc,
                                                       bool* has_delete_file) {
    DORIS_CHECK(desc != nullptr);
    DORIS_CHECK(has_delete_file != nullptr);
    *has_delete_file = false;
    if (!t_desc.__isset.iceberg_params) {
        return Status::OK();
    }
    const auto& iceberg_params = t_desc.iceberg_params;
    if (!iceberg_params.__isset.format_version ||
        iceberg_params.format_version < MIN_SUPPORT_DELETE_FILES_VERSION ||
        !iceberg_params.__isset.delete_files || iceberg_params.delete_files.empty()) {
        return Status::OK();
    }

    const TIcebergDeleteFileDesc* deletion_vector = nullptr;
    for (const auto& delete_file : iceberg_params.delete_files) {
        if (!delete_file.__isset.content || delete_file.content != DELETION_VECTOR) {
            continue;
        }
        if (deletion_vector != nullptr) {
            return Status::DataQualityError("This iceberg data file has multiple DVs.");
        }
        deletion_vector = &delete_file;
    }
    if (deletion_vector == nullptr) {
        return Status::OK();
    }
    size_t bytes_read = 0;
    RETURN_IF_ERROR(validate_iceberg_deletion_vector_descriptor(*deletion_vector, bytes_read));

    const std::string data_file_path = iceberg_params.__isset.original_file_path
                                               ? iceberg_params.original_file_path
                                               : _data_file_path();
    desc->key = build_iceberg_deletion_vector_cache_key(data_file_path, *deletion_vector);
    desc->path = deletion_vector->path;
    desc->start_offset = deletion_vector->content_offset;
    desc->size = static_cast<int64_t>(bytes_read);
    desc->file_size = deletion_vector->__isset.file_size ? deletion_vector->file_size : -1;
    desc->format = DeleteFileDesc::Format::ICEBERG;
    *has_delete_file = true;
    return Status::OK();
}

Status IcebergTableReader::_init_delete_predicates(const TTableFormatFileDesc& t_desc) {
    if (!t_desc.__isset.iceberg_params || _delete_predicates_initialized) {
        _delete_predicates_initialized = true;
        return Status::OK();
    }
    const auto& iceberg_params = t_desc.iceberg_params;
    if (!iceberg_params.__isset.format_version ||
        iceberg_params.format_version < MIN_SUPPORT_DELETE_FILES_VERSION ||
        !iceberg_params.__isset.delete_files || iceberg_params.delete_files.empty()) {
        _delete_predicates_initialized = true;
        return Status::OK();
    }

    std::vector<TIcebergDeleteFileDesc> position_delete_files;
    std::vector<TIcebergDeleteFileDesc> equality_delete_files;
    for (const auto& delete_file : iceberg_params.delete_files) {
        if (!delete_file.__isset.content) {
            continue;
        }
        if (delete_file.content == POSITION_DELETE) {
            position_delete_files.push_back(delete_file);
        } else if (delete_file.content == EQUALITY_DELETE) {
            equality_delete_files.push_back(delete_file);
        }
    }
    // Per Iceberg scan planning, position delete files apply only when there is no deletion vector
    // for the data file. DVs and position deletes now intentionally use different in-memory
    // representations, so use the Roaring pointer as the DV sentinel.
    if (_deletion_vector != nullptr) {
        position_delete_files.clear();
    }
    // Initialize position and equality delete predicates. Position delete files contain row
    // positions of deleted rows, which can be directly added to `_delete_rows`. Equality delete
    // files contain values of deleted rows, which require reading the files and building
    // predicates for later filtering.
    if (!position_delete_files.empty()) {
        RETURN_IF_ERROR(_init_position_delete_rows(position_delete_files));
    }
    if (!equality_delete_files.empty()) {
        RETURN_IF_ERROR(_init_equality_delete_predicates(equality_delete_files));
    }

    _delete_predicates_initialized = true;
    return Status::OK();
}

std::shared_ptr<io::FileSystemProperties> IcebergTableReader::_delete_file_system_properties(
        const TFileScanRangeParams& scan_params) {
    auto system_properties = std::make_shared<io::FileSystemProperties>();
    system_properties->system_type =
            scan_params.__isset.file_type ? scan_params.file_type : TFileType::FILE_LOCAL;
    system_properties->properties = scan_params.properties;
    system_properties->azure_client_config = io::azure_client_config_of(scan_params);
    system_properties->hdfs_params = scan_params.hdfs_params;
    if (scan_params.__isset.broker_addresses) {
        system_properties->broker_addresses.assign(scan_params.broker_addresses.begin(),
                                                   scan_params.broker_addresses.end());
    }
    return system_properties;
}

std::unique_ptr<io::FileDescription> IcebergTableReader::_delete_file_description(
        const TFileRangeDesc& range) {
    auto file_description = std::make_unique<io::FileDescription>();
    file_description->path = range.path;
    file_description->file_size = range.__isset.file_size ? range.file_size : -1;
    file_description->range_start_offset = range.__isset.start_offset ? range.start_offset : 0;
    file_description->range_size = range.__isset.size ? range.size : -1;
    // Iceberg delete files follow the same immutable-file contract as data files: a snapshot
    // references a fixed object and later changes publish a new file rather than replacing it.
    file_description->is_immutable = true;
    if (range.__isset.fs_name) {
        file_description->fs_name = range.fs_name;
    }
    return file_description;
}

std::string IcebergTableReader::_data_file_path() const {
    if (_iceberg_params.has_value() && _iceberg_params->__isset.original_file_path) {
        return _iceberg_params->original_file_path;
    }
    DORIS_CHECK(_current_task != nullptr);
    DORIS_CHECK(_current_task->data_file != nullptr);
    return _current_task->data_file->path;
}

Status IcebergTableReader::_append_row_position_output_column(format::FileScanRequest* request) {
    const auto row_position_column_id = format::LocalColumnId(format::ROW_POSITION_COLUMN_ID);
    _append_file_scan_column(request, row_position_column_id, &request->non_predicate_columns);
    _row_position_block_position = request->local_positions.at(row_position_column_id).value();
    return Status::OK();
}

Status IcebergTableReader::_find_equality_delete_data_field(
        const EqualityDeleteFilter& filter, size_t key_idx,
        EqualityDeleteColumnPath* const data_path, bool* const complete_path) const {
    DORIS_CHECK(key_idx < filter.field_ids.size());
    DORIS_CHECK(key_idx < filter.field_names.size());
    DORIS_CHECK(data_path != nullptr);
    DORIS_CHECK(complete_path != nullptr);
    data_path->clear();
    *complete_path = false;

    auto schema_path =
            _find_table_column_identity_path_by_field_id(filter.field_ids[key_idx], true);
    std::vector<const format::ColumnDefinition*> table_path;
    if (schema_path.has_value()) {
        for (const auto& field : *schema_path) {
            table_path.push_back(&field);
        }
    } else {
        static_cast<void>(find_equality_delete_column_path(_projected_columns,
                                                           filter.field_ids[key_idx], &table_path));
    }
    if (table_path.empty() && mapping_mode() != format::TableColumnMappingMode::BY_NAME) {
        const int field_id = filter.field_ids[key_idx];
        *complete_path =
                find_equality_delete_column_path(_data_reader.file_schema, field_id, data_path);
        return Status::OK();
    }

    // Equality keys are hidden scan dependencies and need not appear in the query projection.
    // Reuse ColumnMapper's exact BY_NAME rules at every ancestor so a nested key keeps its
    // physical path, including historical aliases for ID-less files.
    std::optional<format::ColumnDefinition> legacy_table_field;
    if (table_path.empty() && !supports_iceberg_scan_semantics_v2(_scan_params)) {
        legacy_table_field.emplace();
        legacy_table_field->name = filter.field_names[key_idx];
        legacy_table_field->type = filter.key_types[key_idx];
        table_path.push_back(&*legacy_table_field);
    }
    if (table_path.empty()) {
        return Status::InvalidArgument(
                "Iceberg equality delete field id {} is absent from current and historical table "
                "schema metadata",
                filter.field_ids[key_idx]);
    }
    const std::vector<format::ColumnDefinition>* candidates = &_data_reader.file_schema;
    for (size_t index = 0; index < table_path.size(); ++index) {
        const auto* table_field = table_path[index];
        DORIS_CHECK(table_field != nullptr);
        const format::ColumnDefinition* data_field = nullptr;
        if (mapping_mode() == format::TableColumnMappingMode::BY_NAME) {
            data_field = format::find_column_by_name(*table_field, *candidates);
        } else {
            DORIS_CHECK(table_field->has_identifier_field_id());
            data_field = format::find_column_by_field_id(
                    *table_field, *candidates,
                    supports_iceberg_scan_semantics_v1(_scan_params) &&
                            _format == FileFormat::PARQUET);
        }
        if (data_field == nullptr && mapping_mode() == format::TableColumnMappingMode::BY_NAME &&
            index + 1 == table_path.size() && !table_field->has_name_mapping) {
            // Schema-history fallback can carry a post-snapshot leaf rename when the target
            // snapshot's parent has expired. Retry the delete file's original leaf name, but
            // never bypass an explicit authoritative Iceberg mapping.
            format::ColumnDefinition delete_file_field;
            delete_file_field.name = filter.field_names[key_idx];
            data_field = format::find_column_by_name(delete_file_field, *candidates);
        }
        if (data_field == nullptr) {
            return Status::OK();
        }
        data_path->push_back(data_field);
        candidates = &data_field->children;
    }
    *complete_path = true;
    return Status::OK();
}

Status IcebergTableReader::_find_equality_delete_table_field(
        const EqualityDeleteFilter& filter, size_t key_idx,
        format::ColumnDefinition* table_field) const {
    DORIS_CHECK(key_idx < filter.field_ids.size());
    DORIS_CHECK(key_idx < filter.field_names.size());
    DORIS_CHECK(table_field != nullptr);
    const int field_id = filter.field_ids[key_idx];
    auto resolved = _find_table_column_by_field_id(field_id, filter.key_types[key_idx], true);
    if (!resolved.has_value()) {
        const auto projected_field = std::ranges::find_if(
                _projected_columns, [field_id](const format::ColumnDefinition& field) {
                    return field.has_identifier_field_id() &&
                           field.get_identifier_field_id() == field_id;
                });
        if (projected_field != _projected_columns.end()) {
            // Older scan descriptors and focused unit tests may omit history_schema_info. Keep the
            // projected metadata as a compatibility fallback, but never require projection when
            // the complete current schema is available.
            resolved = *projected_field;
        }
    }
    if (!resolved.has_value() && !supports_iceberg_scan_semantics_v2(_scan_params)) {
        resolved = format::ColumnDefinition {
                .identifier = {},
                .name = filter.field_names[key_idx],
                .type = filter.key_types[key_idx],
        };
    }
    if (!resolved.has_value()) {
        return Status::InvalidArgument(
                "Iceberg equality delete field id {} is absent from current and historical table "
                "schema metadata",
                field_id);
    }
    *table_field = std::move(*resolved);
    return Status::OK();
}

std::string IcebergTableReader::_delete_file_cache_key(const char* prefix,
                                                       const std::string& path) const {
    DORIS_CHECK(prefix != nullptr);
    std::string fs_name;
    if (_current_task != nullptr && _current_task->data_file != nullptr) {
        fs_name = _current_task->data_file->fs_name;
    }
    // Delete descriptors can reuse the same path text in different filesystem namespaces. Encode
    // both variable-length strings so neither an fs/path boundary nor equality field-id suffixes
    // can be reinterpreted as path content; scan-level credentials/properties are shared here.
    std::ostringstream key;
    key << prefix << fs_name.size() << ':' << fs_name << ':' << path.size() << ':' << path;
    return key.str();
}

void IcebergTableReader::_append_equality_delete_row_count_carrier(
        format::FileScanRequest* request) {
    DORIS_CHECK(request != nullptr);
    // Columnar readers establish a filter batch's row count from predicate columns. If all
    // equality keys are missing, the predicate consists only of NULL literals and the filter block
    // would otherwise have zero rows. Use the virtual row-position column as the carrier instead
    // of an arbitrary physical column. For example, a data file may start with an unsupported
    // TIME_MILLIS leaf while the query projects only a supported `id`; selecting that TIME leaf as
    // a hidden carrier would make Parquet reject a column the query never requested. Row position
    // has one value per input row in both Parquet and ORC, is already used by delete predicates,
    // and is explicitly excluded from physical logical-type validation.
    _append_file_scan_column(request, format::LocalColumnId(format::ROW_POSITION_COLUMN_ID),
                             &request->predicate_columns);
}

Status IcebergTableReader::_build_missing_equality_delete_key_expr(
        const EqualityDeleteFilter& filter, size_t key_idx,
        const EqualityDeleteColumnPath& data_path, format::FileScanRequest* const request,
        VExprSPtr* const key_expr, DataTypePtr* const comparison_type) {
    DORIS_CHECK(request != nullptr);
    DORIS_CHECK(key_expr != nullptr);
    DORIS_CHECK(comparison_type != nullptr);
    auto table_path = _find_table_column_path_by_field_id(filter.field_ids[key_idx],
                                                          filter.key_types[key_idx], true);
    if (!table_path.has_value() || data_path.size() >= table_path->size()) {
        format::ColumnDefinition table_field;
        RETURN_IF_ERROR(_find_equality_delete_table_field(filter, key_idx, &table_field));
        table_path = std::vector<format::ColumnDefinition> {std::move(table_field)};
    }
    const size_t missing_index = data_path.size() < table_path->size() ? data_path.size() : 0;
    auto& missing_root = (*table_path)[missing_index];
    DORIS_CHECK(missing_root.type != nullptr);
    VExprSPtr missing_root_expr;
    RETURN_IF_ERROR(build_missing_equality_delete_key_expr(
            missing_root, missing_root.type, supports_iceberg_scan_semantics_v2(_scan_params),
            &missing_root_expr));
    std::vector<const format::ColumnDefinition*> missing_path;
    for (size_t path_index = missing_index; path_index < table_path->size(); ++path_index) {
        missing_path.push_back(&(*table_path)[path_index]);
    }
    VExprSPtr default_expr;
    RETURN_IF_ERROR(build_nested_equality_delete_key_expr(
            missing_path, std::move(missing_root_expr), &default_expr));
    const auto* table_leaf = missing_path.back();
    DORIS_CHECK(table_leaf != nullptr);
    DORIS_CHECK(table_leaf->type != nullptr);
    bool default_is_null = false;
    RETURN_IF_ERROR(equality_delete_default_leaf_is_null(missing_path, &default_is_null));
    if (default_is_null) {
        // A NULL default makes the key NULL in this file whatever its table type, so it is
        // compared in the delete file's type, to which the table type need not convert: a dropped
        // key whose retained schemas only typed it Iceberg `unknown` arrives as STRING, while its
        // tombstones are, for example, DATE.
        *comparison_type = remove_nullable(filter.delete_key_types[key_idx]);
    } else if (remove_nullable(table_leaf->type)->get_primitive_type() == TYPE_STRING &&
               remove_nullable(filter.delete_key_types[key_idx])->get_primitive_type() ==
                       TYPE_TIMESTAMPTZ) {
        // An Iceberg timestamptz_ns key behind the STRING placeholder. Its tombstones are read as
        // TIMESTAMPTZ(6), Parquet flooring and ORC rounding the nanoseconds, so no parse of the
        // nanosecond text default compares exactly: the default `1970-01-01 00:00:00.0000001+00:00`
        // would match an epoch tombstone and delete every row of this file, and the default epoch
        // a tombstone 100 ns after it.
        return Status::NotSupported(
                "Iceberg equality delete field id {} is a timestamptz_ns key that data file {} "
                "does not store, with a non-NULL initial default; the delete file's values are "
                "read as {} without their nanoseconds, so the default cannot be compared exactly",
                filter.field_ids[key_idx], _data_file_path(),
                remove_nullable(filter.delete_key_types[key_idx])->get_name());
    } else if (auto placeholder_type = equality_delete_placeholder_default_type(
                       table_leaf->type, filter.delete_key_types[key_idx]);
               placeholder_type != nullptr) {
        // A TIME `payload.k` added with the default `00:00` after the file was written: rows
        // under a NULL `payload` still get a NULL key from AncestorNullDefaultExpr below, the
        // others the parsed midnight, so a 00:00:00 tombstone deletes exactly those.
        *comparison_type = std::move(placeholder_type);
    } else {
        RETURN_IF_ERROR(equality_delete_comparison_type(
                filter.field_ids[key_idx], filter.delete_key_types[key_idx], table_leaf->type,
                table_leaf->type, _data_file_path(), comparison_type));
    }
    default_expr = cast_equality_delete_key(std::move(default_expr), *comparison_type);
    if (data_path.empty()) {
        *key_expr = std::move(default_expr);
        return Status::OK();
    }

    const auto* root = data_path.front();
    const auto field_column_id = format::LocalColumnId(root->file_local_id());
    _append_file_scan_column(request, field_column_id, &request->predicate_columns);
    const auto block_position = request->local_positions.at(field_column_id).value();
    VExprSPtr ancestor_expr;
    RETURN_IF_ERROR(build_equality_delete_key_expr(data_path, block_position, &ancestor_expr));
    auto combined_expr = std::make_shared<AncestorNullDefaultExpr>(
            make_nullable(*comparison_type), ancestor_expr->expr_name() + "." + table_leaf->name);
    combined_expr->add_child(ancestor_expr);
    combined_expr->add_child(default_expr);
    *key_expr = std::move(combined_expr);
    return Status::OK();
}

Status IcebergTableReader::_append_equality_delete_predicates(format::FileScanRequest* request) {
    DORIS_CHECK(request != nullptr);
    for (const auto& filter : _equality_delete_filters) {
        DCHECK_EQ(filter.field_ids.size(), filter.key_types.size());
        DCHECK_EQ(filter.field_ids.size(), filter.delete_key_types.size());
        // A delete file without rows deletes nothing, but EqualityDeletePredicate evaluates its
        // keys before it looks at the delete set, so the strict comparison casts would still run:
        // a DATE key 1000-01-01 compared with an empty TIMESTAMP_NS delete file fails the scan
        // with an overflow. It is skipped here rather than at load time, so the loaded filter
        // still disables aggregate pushdown as before.
        if (filter.delete_block.rows() == 0) {
            continue;
        }
        VExprSPtrs key_exprs;
        DataTypes comparison_types;
        bool has_missing_key = false;
        for (size_t idx = 0; idx < filter.field_ids.size(); ++idx) {
            EqualityDeleteColumnPath data_path;
            bool complete_path = false;
            RETURN_IF_ERROR(
                    _find_equality_delete_data_field(filter, idx, &data_path, &complete_path));
            VExprSPtr key_expr;
            DataTypePtr comparison_type;
            if (!complete_path) {
                RETURN_IF_ERROR(_build_missing_equality_delete_key_expr(
                        filter, idx, data_path, request, &key_expr, &comparison_type));
                has_missing_key = true;
            } else {
                const auto* root = data_path.front();
                const auto field_column_id = format::LocalColumnId(root->file_local_id());
                _append_file_scan_column(request, field_column_id, &request->predicate_columns);
                const auto block_position = request->local_positions.at(field_column_id).value();
                RETURN_IF_ERROR(
                        build_equality_delete_key_expr(data_path, block_position, &key_expr));
                RETURN_IF_ERROR(equality_delete_comparison_type(
                        filter.field_ids[idx], filter.delete_key_types[idx], key_expr->data_type(),
                        filter.key_types[idx], _data_file_path(), &comparison_type));
                key_expr = cast_equality_delete_key(std::move(key_expr), comparison_type);
            }
            key_exprs.push_back(std::move(key_expr));
            comparison_types.push_back(std::move(comparison_type));
        }
        // A data file can hold a key in a wider type than the delete file, e.g. a LONG written
        // after an INT -> LONG promotion against INT tombstones written before it. The tombstones
        // are then widened once per scanner and cached, instead of narrowing every data value.
        EqualityDeleteFilter converted;
        const bool needs_conversion = !std::ranges::equal(
                filter.delete_key_types, comparison_types,
                [](const DataTypePtr& delete_type, const DataTypePtr& comparison_type) {
                    return remove_nullable(delete_type)->equals(*comparison_type);
                });
        if (needs_conversion) {
            RETURN_IF_ERROR(_convert_equality_delete_filter(filter, comparison_types, &converted));
        }
        const auto& compared = needs_conversion ? converted : filter;
        auto delete_predicate = std::make_shared<EqualityDeletePredicate>(
                compared.delete_block, filter.field_ids, compared.hash_index);
        for (auto& key_expr : key_exprs) {
            delete_predicate->add_child(std::move(key_expr));
        }
        if (has_missing_key && request->predicate_columns.empty()) {
            _append_equality_delete_row_count_carrier(request);
        }
        request->delete_conjuncts.push_back(
                VExprContext::create_shared(std::move(delete_predicate)));
    }
    return Status::OK();
}

Status IcebergTableReader::_convert_equality_delete_filter(const EqualityDeleteFilter& filter,
                                                           const DataTypes& comparison_types,
                                                           EqualityDeleteFilter* result) {
    DORIS_CHECK(_split_cache != nullptr);
    DORIS_CHECK(result != nullptr);
    DORIS_CHECK(comparison_types.size() == filter.delete_key_types.size());
    std::ostringstream cache_key;
    cache_key << filter.cache_key << ":compare";
    for (const auto& type : comparison_types) {
        cache_key << ':' << type->get_name();
    }
    Status convert_status = Status::OK();
    bool cache_hit = false;
    auto* cached_filter = _split_cache->get<EqualityDeleteFilter>(
            cache_key.str(),
            [&]() -> EqualityDeleteFilter* {
                auto converted = std::make_unique<EqualityDeleteFilter>();
                convert_status = _build_converted_equality_delete_filter(filter, comparison_types,
                                                                         converted.get());
                if (!convert_status.ok()) {
                    return nullptr;
                }
                converted->cache_key = cache_key.str();
                return converted.release();
            },
            &cache_hit);
    RETURN_IF_ERROR(convert_status);
    DORIS_CHECK(cached_filter != nullptr);
    if (!cache_hit) {
        COUNTER_UPDATE(_profile.equality_delete_hash_index_memory,
                       static_cast<int64_t>(cached_filter->hash_index->memory_usage()));
    }
    *result = *cached_filter;
    return Status::OK();
}

Status IcebergTableReader::_build_converted_equality_delete_filter(
        const EqualityDeleteFilter& filter, const DataTypes& comparison_types,
        EqualityDeleteFilter* result) const {
    DORIS_CHECK(result != nullptr);
    result->field_ids = filter.field_ids;
    result->field_names = filter.field_names;
    result->key_types = filter.key_types;
    Block delete_block = filter.delete_block;
    RowDescriptor row_desc;
    for (size_t index = 0; index < comparison_types.size(); ++index) {
        const auto& entry = delete_block.get_by_position(index);
        VExprSPtr key_expr = VSlotRef::create_shared(cast_set<int>(index), cast_set<int>(index), -1,
                                                     entry.type, entry.name);
        auto context = VExprContext::create_shared(
                cast_equality_delete_key(std::move(key_expr), comparison_types[index]));
        RETURN_IF_ERROR(context->prepare(_runtime_state, row_desc));
        RETURN_IF_ERROR(context->open(_runtime_state));
        ColumnWithTypeAndName key;
        RETURN_IF_ERROR(context->execute(&delete_block, key));
        key.name = entry.name;
        result->delete_key_types.push_back(key.type);
        result->delete_block.insert(std::move(key));
    }
    result->hash_index = EqualityDeletePredicate::build_hash_index(result->delete_block);
    return Status::OK();
}

Status IcebergTableReader::_create_delete_file_reader(const TIcebergDeleteFileDesc& delete_file,
                                                      const TFileScanRangeParams& scan_params,
                                                      IcebergDeleteFileIOContext* delete_io_ctx,
                                                      std::unique_ptr<format::FileReader>* reader) {
    DORIS_CHECK(delete_io_ctx != nullptr);
    DORIS_CHECK(reader != nullptr);
    if (!delete_file.__isset.file_format) {
        return Status::InternalError("Iceberg delete file is missing file format");
    }
    if (delete_file.file_format != TFileFormatType::FORMAT_PARQUET &&
        delete_file.file_format != TFileFormatType::FORMAT_ORC) {
        return Status::NotSupported("Unsupported Iceberg delete file format {}",
                                    delete_file.file_format);
    }
    auto delete_range = build_iceberg_delete_file_range(
            delete_file.path, delete_file.__isset.file_size ? delete_file.file_size : -1);
    if (_current_task != nullptr && _current_task->data_file != nullptr &&
        !_current_task->data_file->fs_name.empty()) {
        delete_range.__set_fs_name(_current_task->data_file->fs_name);
    }
    auto system_properties = _delete_file_system_properties(scan_params);
    auto file_description = _delete_file_description(delete_range);
    std::shared_ptr<io::IOContext> io_ctx(&delete_io_ctx->io_ctx, [](io::IOContext*) {});
    const bool enable_mapping_timestamp_tz = scan_params.__isset.enable_mapping_timestamp_tz &&
                                             scan_params.enable_mapping_timestamp_tz;
    const bool enable_mapping_varbinary =
            scan_params.__isset.enable_mapping_varbinary && scan_params.enable_mapping_varbinary;
    if (delete_file.file_format == TFileFormatType::FORMAT_PARQUET) {
        // Delete and data files must parse raw binary fields with the same scan-level mapping.
        *reader = std::make_unique<format::parquet::ParquetReader>(
                system_properties, file_description, io_ctx, _scanner_profile, std::nullopt,
                enable_mapping_timestamp_tz, enable_mapping_varbinary, "", preserve_binary_uuid(),
                _parquet_profile_cache);
    } else {
        *reader = std::make_unique<format::orc::OrcReader>(
                system_properties, file_description, io_ctx, _scanner_profile, std::nullopt,
                enable_mapping_timestamp_tz, enable_mapping_varbinary);
    }
    RETURN_IF_ERROR((*reader)->init(_runtime_state));
    return Status::OK();
}

Status IcebergTableReader::_read_position_delete_file(const TIcebergDeleteFileDesc& delete_file,
                                                      const TFileScanRangeParams& scan_params,
                                                      IcebergDeleteFileIOContext* delete_io_ctx,
                                                      PositionDeleteRowsCollector* collector) {
    DORIS_CHECK(collector != nullptr);
    std::unique_ptr<format::FileReader> reader;
    RETURN_IF_ERROR(_create_delete_file_reader(delete_file, scan_params, delete_io_ctx, &reader));
    DORIS_CHECK(reader != nullptr);

    std::vector<format::ColumnDefinition> schema;
    RETURN_IF_ERROR(reader->get_schema(&schema));
    format::ColumnDefinition* file_path_field = nullptr;
    format::ColumnDefinition* pos_field = nullptr;
    for (auto& field : schema) {
        if (field.name == ICEBERG_FILE_PATH) {
            file_path_field = &field;
        } else if (field.name == ICEBERG_ROW_POS) {
            pos_field = &field;
        }
    }
    if (file_path_field == nullptr || pos_field == nullptr) {
        return Status::InternalError("Position delete file is missing required columns");
    }

    auto request = std::make_shared<format::FileScanRequest>();
    request->non_predicate_columns = {
            format::LocalColumnIndex::top_level(
                    format::LocalColumnId(file_path_field->file_local_id())),
            format::LocalColumnIndex::top_level(format::LocalColumnId(pos_field->file_local_id()))};
    request->local_positions = {
            {format::LocalColumnId(file_path_field->file_local_id()),
             format::LocalIndex(ICEBERG_FILE_PATH_BLOCK_POSITION)},
            {format::LocalColumnId(pos_field->file_local_id()),
             format::LocalIndex(ICEBERG_ROW_POS_BLOCK_POSITION)},
    };
    RETURN_IF_ERROR(reader->open(request));

    bool eof = false;
    auto build_position_delete_block = [](const format::ColumnDefinition& file_path_field,
                                          const format::ColumnDefinition& pos_field) -> Block {
        Block block;
        block.insert(
                {file_path_field.type->create_column(), file_path_field.type, ICEBERG_FILE_PATH});
        block.insert({pos_field.type->create_column(), pos_field.type, ICEBERG_ROW_POS});
        return block;
    };
    while (!eof) {
        Block block = build_position_delete_block(*file_path_field, *pos_field);
        size_t read_rows = 0;
        RETURN_IF_ERROR(reader->get_block(&block, &read_rows, &eof));
        RETURN_IF_ERROR(collector->collect(block, read_rows));
    }
    return reader->close();
}

Status IcebergTableReader::_init_position_delete_rows(
        const std::vector<TIcebergDeleteFileDesc>& delete_files) {
    DORIS_CHECK(_split_cache != nullptr);
    TFileScanRangeParams delete_scan_params =
            _scan_params == nullptr ? TFileScanRangeParams() : *_scan_params;
    format::DeleteRows position_delete_rows;
    IcebergDeleteFileIOContext delete_io_ctx(_runtime_state);
    for (const auto& delete_file : delete_files) {
        Status read_status = Status::OK();
        // A position delete file normally references many data files. Cache the complete
        // path-to-position map once; caching only the current data file would still rescan the
        // shared delete file for every subsequent split.
        auto* rows_by_data_file =
                _split_cache->get<PositionDeleteRowsCollector::PositionDeleteFile>(
                        _delete_file_cache_key("iceberg_v2_position_delete_", delete_file.path),
                        [&]() -> PositionDeleteRowsCollector::PositionDeleteFile* {
                            auto result = std::make_unique<
                                    PositionDeleteRowsCollector::PositionDeleteFile>();
                            PositionDeleteRowsCollector collector(result.get());
                            read_status = _read_position_delete_file(
                                    delete_file, delete_scan_params, &delete_io_ctx, &collector);
                            if (!read_status.ok()) {
                                return nullptr;
                            }
                            for (auto& [_, rows] : *result) {
                                std::ranges::sort(rows);
                            }
                            return result.release();
                        });
        RETURN_IF_ERROR(read_status);
        DORIS_CHECK(rows_by_data_file != nullptr);
        const auto rows_it = rows_by_data_file->find(_data_file_path());
        if (rows_it == rows_by_data_file->end()) {
            continue;
        }
        auto first = rows_it->second.begin();
        auto last = rows_it->second.end();
        // Bounds are inclusive Iceberg position statistics supplied by FE. Apply them after the
        // cached per-data-file vector is sorted so irrelevant positions are sliced without a scan.
        if (delete_file.__isset.position_lower_bound) {
            first = std::lower_bound(first, last, delete_file.position_lower_bound);
        }
        if (delete_file.__isset.position_upper_bound) {
            last = std::upper_bound(first, last, delete_file.position_upper_bound);
        }
        position_delete_rows.insert(position_delete_rows.end(), first, last);
    }
    if (position_delete_rows.empty()) {
        return Status::OK();
    }
    // Position delete files and deletion vectors both become row-position deletes for the
    // common TableReader DeletePredicate path. Keep the merged rows in a member vector because
    // DeletePredicate stores a reference to the vector used by _delete_rows.
    _position_delete_rows_storage.insert(_position_delete_rows_storage.end(),
                                         position_delete_rows.begin(), position_delete_rows.end());
    std::sort(_position_delete_rows_storage.begin(), _position_delete_rows_storage.end());
    _position_delete_rows_storage.erase(
            std::unique(_position_delete_rows_storage.begin(), _position_delete_rows_storage.end()),
            _position_delete_rows_storage.end());
    _delete_rows = &_position_delete_rows_storage;
    return Status::OK();
}

Status IcebergTableReader::_init_equality_delete_predicates(
        const std::vector<TIcebergDeleteFileDesc>& delete_files) {
    DORIS_CHECK(_split_cache != nullptr);
    TFileScanRangeParams delete_scan_params =
            _scan_params == nullptr ? TFileScanRangeParams() : *_scan_params;
    IcebergDeleteFileIOContext delete_io_ctx(_runtime_state);
    for (const auto& delete_file : delete_files) {
        RETURN_IF_ERROR(
                _read_equality_delete_file(delete_file, delete_scan_params, &delete_io_ctx));
    }
    return Status::OK();
}

Status IcebergTableReader::_resolve_equality_delete_fields(
        const TIcebergDeleteFileDesc& delete_file,
        const std::vector<format::ColumnDefinition>& schema,
        std::vector<EqualityDeleteColumnPath>* delete_paths, EqualityDeleteFilter* result) const {
    DORIS_CHECK(delete_paths != nullptr);
    DORIS_CHECK(result != nullptr);
    for (const auto field_id : delete_file.field_ids) {
        EqualityDeleteColumnPath path;
        if (!find_equality_delete_column_path(schema, field_id, &path)) {
            return Status::InternalError("Can not find field id {} in equality delete file {}",
                                         field_id, delete_file.path);
        }
        const auto* field = path.back();
        if (!field->children.empty()) {
            return Status::NotSupported(
                    "Iceberg equality delete does not support complex column {}", field->name);
        }
        // The delete block keeps the delete file's own key type. Each data file's predicate
        // compares a key in the wider of this type and its own (equality_delete_comparison_type):
        // narrowing to either side is lossy, as an INT overflow becomes NULL and NULL-safe
        // equality then matches a NULL key.
        const DataTypePtr delete_file_type =
                path.size() > 1 ? make_nullable(field->type) : field->type;
        DataTypePtr key_type = delete_file_type;
        if (auto table_field = _find_table_column_by_field_id(field_id, delete_file_type, true);
            table_field.has_value() && table_field->type != nullptr) {
            key_type = table_field->type;
        }
        delete_paths->push_back(std::move(path));
        result->field_ids.push_back(field_id);
        result->field_names.push_back(field->name);
        result->key_types.push_back(key_type);
        result->delete_key_types.push_back(delete_file_type);
    }
    return Status::OK();
}

Status IcebergTableReader::_load_equality_delete_file(const TIcebergDeleteFileDesc& delete_file,
                                                      const TFileScanRangeParams& scan_params,
                                                      IcebergDeleteFileIOContext* delete_io_ctx,
                                                      EqualityDeleteFilter* result) {
    DORIS_CHECK(result != nullptr);
    std::unique_ptr<format::FileReader> reader;
    RETURN_IF_ERROR(_create_delete_file_reader(delete_file, scan_params, delete_io_ctx, &reader));
    DORIS_CHECK(reader != nullptr);

    std::vector<format::ColumnDefinition> schema;
    RETURN_IF_ERROR(reader->get_schema(&schema));
    std::vector<EqualityDeleteColumnPath> delete_paths;
    RETURN_IF_ERROR(_resolve_equality_delete_fields(delete_file, schema, &delete_paths, result));

    auto request = std::make_shared<format::FileScanRequest>();
    format::FileScanRequestBuilder request_builder(request.get());
    for (const auto& path : delete_paths) {
        DORIS_CHECK(!path.empty());
        RETURN_IF_ERROR(request_builder.add_non_predicate_column(
                format::LocalColumnId(path.front()->file_local_id())));
    }
    Block file_block_template;
    std::vector<const format::ColumnDefinition*> roots(request->local_positions.size());
    for (const auto& path : delete_paths) {
        const auto* root = path.front();
        const auto position =
                request->local_positions.at(format::LocalColumnId(root->file_local_id()));
        roots[position.value()] = root;
    }
    for (const auto* root : roots) {
        DORIS_CHECK(root != nullptr);
        file_block_template.insert({root->type->create_column(), root->type, root->name});
    }

    std::vector<VExprContextSPtr> key_exprs;
    key_exprs.reserve(delete_paths.size());
    RowDescriptor row_desc;
    for (size_t index = 0; index < delete_paths.size(); ++index) {
        const auto& path = delete_paths[index];
        const auto root_column_id = format::LocalColumnId(path.front()->file_local_id());
        VExprSPtr key_expr;
        RETURN_IF_ERROR(build_equality_delete_key_expr(
                path, request->local_positions.at(root_column_id).value(), &key_expr));
        DORIS_CHECK(key_expr->data_type()->equals(*result->delete_key_types[index]));
        auto context = VExprContext::create_shared(std::move(key_expr));
        RETURN_IF_ERROR(context->prepare(_runtime_state, row_desc));
        RETURN_IF_ERROR(context->open(_runtime_state));
        key_exprs.push_back(std::move(context));
    }
    RETURN_IF_ERROR(reader->open(request));

    Block delete_block_template;
    for (size_t index = 0; index < delete_paths.size(); ++index) {
        const auto* field = delete_paths[index].back();
        const auto& key_type = result->delete_key_types[index];
        delete_block_template.insert({key_type->create_column(), key_type, field->name});
    }
    MutableBlock mutable_delete_block(delete_block_template.clone_empty());
    bool eof = false;
    while (!eof) {
        Block block = file_block_template.clone_empty();
        size_t read_rows = 0;
        RETURN_IF_ERROR(reader->get_block(&block, &read_rows, &eof));
        if (read_rows > 0) {
            Block key_block;
            for (const auto& context : key_exprs) {
                ColumnWithTypeAndName key;
                RETURN_IF_ERROR(context->execute(&block, key));
                key_block.insert(std::move(key));
            }
            RETURN_IF_ERROR(mutable_delete_block.merge(key_block));
        }
    }
    RETURN_IF_ERROR(reader->close());
    result->delete_block = mutable_delete_block.to_block();
    result->hash_index = EqualityDeletePredicate::build_hash_index(result->delete_block);
    return Status::OK();
}

Status IcebergTableReader::_read_equality_delete_file(const TIcebergDeleteFileDesc& delete_file,
                                                      const TFileScanRangeParams& scan_params,
                                                      IcebergDeleteFileIOContext* delete_io_ctx) {
    if (!delete_file.__isset.field_ids || delete_file.field_ids.empty()) {
        return Status::InternalError("Iceberg equality delete file is missing field ids");
    }
    std::ostringstream cache_key;
    cache_key << _delete_file_cache_key("iceberg_v2_equality_delete_", delete_file.path);
    if (scan_params.__isset.current_schema_id) {
        // The table-side key types depend on the current snapshot schema, so a cached filter must
        // not be reused across schemas.
        cache_key << ":schema=" << scan_params.current_schema_id;
    }
    cache_key << ':' << delete_file.field_ids.size();
    for (const auto field_id : delete_file.field_ids) {
        cache_key << ':' << field_id;
    }
    Status read_status = Status::OK();
    bool cache_hit = false;
    // Include the ordered equality ids in the key because the same physical delete file can be
    // projected with different key layouts. The cached block, key metadata, and contiguous index
    // are immutable after construction and therefore safe to share across split-local predicates.
    auto* cached_filter = _split_cache->get<EqualityDeleteFilter>(
            cache_key.str(),
            [&]() -> EqualityDeleteFilter* {
                auto result = std::make_unique<EqualityDeleteFilter>();
                read_status = _load_equality_delete_file(delete_file, scan_params, delete_io_ctx,
                                                         result.get());
                if (!read_status.ok()) {
                    return nullptr;
                }
                result->cache_key = cache_key.str();
                return result.release();
            },
            &cache_hit);
    RETURN_IF_ERROR(read_status);
    DORIS_CHECK(cached_filter != nullptr);
    COUNTER_UPDATE(cache_hit ? _profile.equality_delete_index_cache_hit_count
                             : _profile.equality_delete_index_cache_miss_count,
                   1);
    if (!cache_hit) {
        COUNTER_UPDATE(_profile.equality_delete_hash_index_memory,
                       static_cast<int64_t>(cached_filter->hash_index->memory_usage()));
    }
    _equality_delete_filters.push_back(*cached_filter);
    return Status::OK();
}

Status IcebergTableReader::_materialize_row_lineage_row_id(Block* table_block, size_t column_idx) {
    if (_row_lineage_columns.first_row_id < 0) {
        return Status::OK();
    }
    DORIS_CHECK(_row_position_block_position < _data_reader.block_template.columns());
    const auto& row_position_column = assert_cast<const ColumnInt64&>(
            *_data_reader.block_template.get_by_position(_row_position_block_position).column);
    DORIS_CHECK(row_position_column.size() == table_block->rows());
    auto column = IColumn::mutate(
            table_block->get_by_position(column_idx).column->convert_to_full_column_if_const());
    auto* nullable_column = assert_cast<ColumnNullable*>(column.get());
    auto& null_map = nullable_column->get_null_map_data();
    auto& data = assert_cast<ColumnInt64&>(*nullable_column->get_nested_column_ptr()).get_data();
    DORIS_CHECK(null_map.size() == row_position_column.size());
    DORIS_CHECK(data.size() == row_position_column.size());
    for (size_t row = 0; row < row_position_column.size(); ++row) {
        if (null_map[row]) {
            null_map[row] = 0;
            data[row] = _row_lineage_columns.first_row_id + row_position_column.get_element(row);
        }
    }
    table_block->replace_by_position(column_idx, std::move(column));
    return Status::OK();
}

Status IcebergTableReader::_materialize_iceberg_rowid(Block* table_block, size_t column_idx) {
    DORIS_CHECK(_row_position_block_position < _data_reader.block_template.columns());
    const auto& row_position_column = assert_cast<const ColumnInt64&>(
            *_data_reader.block_template.get_by_position(_row_position_block_position).column);
    DORIS_CHECK(row_position_column.size() == table_block->rows());

    const auto& type = table_block->get_by_position(column_idx).type;
    auto column = type->create_column();
    auto* nullable_column = check_and_get_column<ColumnNullable>(column.get());
    auto* struct_column = nullable_column != nullptr
                                  ? check_and_get_column<ColumnStruct>(
                                            nullable_column->get_nested_column_ptr().get())
                                  : check_and_get_column<ColumnStruct>(column.get());
    DORIS_CHECK(struct_column != nullptr);
    DORIS_CHECK(struct_column->tuple_size() >= 4);

    const auto rows = row_position_column.size();
    const auto file_path = _data_file_path();
    const int32_t partition_spec_id =
            _iceberg_params.has_value() && _iceberg_params->__isset.partition_spec_id
                    ? _iceberg_params->partition_spec_id
                    : 0;
    const std::string partition_data_json =
            _iceberg_params.has_value() && _iceberg_params->__isset.partition_data_json
                    ? _iceberg_params->partition_data_json
                    : "";

    auto& file_path_column = struct_column->get_column(0);
    auto& row_pos_column = struct_column->get_column(1);
    auto& spec_id_column = struct_column->get_column(2);
    auto& partition_data_column = struct_column->get_column(3);
    file_path_column.reserve(rows);
    row_pos_column.reserve(rows);
    spec_id_column.reserve(rows);
    partition_data_column.reserve(rows);
    for (size_t row = 0; row < rows; ++row) {
        file_path_column.insert_data(file_path.data(), file_path.size());
        const int64_t row_pos = row_position_column.get_element(row);
        row_pos_column.insert_data(reinterpret_cast<const char*>(&row_pos), sizeof(row_pos));
        spec_id_column.insert_data(reinterpret_cast<const char*>(&partition_spec_id),
                                   sizeof(partition_spec_id));
        partition_data_column.insert_data(partition_data_json.data(), partition_data_json.size());
    }
    if (nullable_column != nullptr) {
        nullable_column->get_null_map_data().resize_fill(rows, 0);
    }
    table_block->replace_by_position(column_idx, std::move(column));
    return Status::OK();
}

Status IcebergTableReader::_materialize_iceberg_file_path(Block* table_block, size_t column_idx) {
    DORIS_CHECK(_row_position_block_position < _data_reader.block_template.columns());
    const auto& row_position_column = assert_cast<const ColumnInt64&>(
            *_data_reader.block_template.get_by_position(_row_position_block_position).column);
    DORIS_CHECK(row_position_column.size() == table_block->rows());

    auto column = table_block->get_by_position(column_idx).type->create_column();
    auto* nullable_column = check_and_get_column<ColumnNullable>(*column);
    auto* string_column = nullable_column != nullptr
                                  ? check_and_get_column<ColumnString>(
                                            nullable_column->get_nested_column_ptr().get())
                                  : check_and_get_column<ColumnString>(column.get());
    DORIS_CHECK(string_column != nullptr);

    const auto file_path = _data_file_path();
    string_column->insert_data(file_path.data(), file_path.size());
    if (nullable_column != nullptr) {
        nullable_column->get_null_map_data().resize_fill(1, 0);
    }
    table_block->replace_by_position(
            column_idx, ColumnConst::create(std::move(column), row_position_column.size()));
    return Status::OK();
}

Status IcebergTableReader::_materialize_iceberg_row_position(Block* table_block,
                                                             size_t column_idx) {
    DORIS_CHECK(_row_position_block_position < _data_reader.block_template.columns());
    const auto& row_position_column = assert_cast<const ColumnInt64&>(
            *_data_reader.block_template.get_by_position(_row_position_block_position).column);
    DORIS_CHECK(row_position_column.size() == table_block->rows());

    auto column = table_block->get_by_position(column_idx).type->create_column();
    auto* nullable_column = check_and_get_column<ColumnNullable>(*column);
    auto* int_column = nullable_column != nullptr
                               ? check_and_get_column<ColumnInt64>(
                                         nullable_column->get_nested_column_ptr().get())
                               : check_and_get_column<ColumnInt64>(column.get());
    DORIS_CHECK(int_column != nullptr);

    int_column->get_data().assign(row_position_column.get_data().begin(),
                                  row_position_column.get_data().end());
    if (nullable_column != nullptr) {
        nullable_column->get_null_map_data().resize_fill(row_position_column.size(), 0);
    }
    table_block->replace_by_position(column_idx, std::move(column));
    return Status::OK();
}

Status IcebergTableReader::_materialize_row_lineage_last_updated_sequence_number(
        Block* table_block, size_t column_idx) {
    if (_row_lineage_columns.last_updated_sequence_number < 0) {
        return Status::OK();
    }
    auto column = IColumn::mutate(
            table_block->get_by_position(column_idx).column->convert_to_full_column_if_const());
    auto* nullable_column = assert_cast<ColumnNullable*>(column.get());
    auto& null_map = nullable_column->get_null_map_data();
    auto& data = assert_cast<ColumnInt64&>(*nullable_column->get_nested_column_ptr()).get_data();
    DORIS_CHECK(null_map.size() == table_block->rows());
    DORIS_CHECK(data.size() == table_block->rows());
    for (size_t row = 0; row < table_block->rows(); ++row) {
        if (null_map[row]) {
            null_map[row] = 0;
            data[row] = _row_lineage_columns.last_updated_sequence_number;
        }
    }
    table_block->replace_by_position(column_idx, std::move(column));
    return Status::OK();
}

bool IcebergTableReader::_need_row_lineage_row_id() const {
    if (_data_reader.column_mapper != nullptr) {
        for (const auto& mapping : _data_reader.column_mapper->mappings()) {
            if (mapping.virtual_column_type == format::TableVirtualColumnType::ROW_ID) {
                return true;
            }
        }
    }
    return std::ranges::any_of(_projected_columns, is_projected_row_lineage_row_id);
}

bool IcebergTableReader::_need_iceberg_rowid() const {
    if (_data_reader.column_mapper != nullptr) {
        for (const auto& mapping : _data_reader.column_mapper->mappings()) {
            if (mapping.virtual_column_type == format::TableVirtualColumnType::ICEBERG_ROWID) {
                return true;
            }
        }
    }
    return std::ranges::any_of(_projected_columns, is_projected_iceberg_rowid);
}

bool IcebergTableReader::_need_iceberg_metadata() const {
    if (_data_reader.column_mapper != nullptr) {
        for (const auto& mapping : _data_reader.column_mapper->mappings()) {
            if (mapping.virtual_column_type == format::TableVirtualColumnType::ICEBERG_FILE_PATH ||
                mapping.virtual_column_type ==
                        format::TableVirtualColumnType::ICEBERG_ROW_POSITION) {
                return true;
            }
        }
    }
    return std::ranges::any_of(_projected_columns, is_projected_iceberg_metadata);
}

} // namespace doris::format::iceberg
