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

#include "format_v2/table_reader.h"

#include <gen_cpp/ExternalTableSchema_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Types_types.h>

#include <algorithm>
#include <limits>
#include <memory>
#include <ranges>
#include <set>
#include <sstream>
#include <utility>
#include <vector>

#include "common/cast_set.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/primitive_type.h"
#include "exprs/vexpr_context.h"
#include "exprs/vslot_ref.h"
#include "format/table/deletion_vector_reader.h"
#include "format/table/iceberg_delete_file_reader_helper.h"
#include "format/table/iceberg_scan_semantics.h"
#include "format/table/paimon_reader.h"
#include "format_v2/column_mapper.h"
#include "format_v2/delimited_text/csv_reader.h"
#include "format_v2/delimited_text/text_reader.h"
#include "format_v2/json/json_reader.h"
#include "format_v2/native/native_reader.h"
#include "format_v2/orc/orc_reader.h"
#include "format_v2/parquet/parquet_reader.h"
#include "runtime/file_scan_profile.h"
#include "storage/segment/condition_cache.h"
#include "util/debug_points.h"
#include "util/string_util.h"

namespace doris::format {
namespace {

std::optional<uint64_t> build_predicate_snapshot_digest(const VExprContextSPtrs& conjuncts) {
    // Adaptive state must remain independent of the optional Condition Cache seed. A zero result
    // means an expression cannot provide a stable semantic digest, so sharing is disabled.
    uint64_t digest = 0xcbf29ce484222325ULL;
    for (const auto& conjunct : conjuncts) {
        digest = conjunct->get_digest(digest);
        if (digest == 0) {
            return std::nullopt;
        }
    }
    return digest;
}

void extend_format_split_id_range(PhysicalFileSplit* destination, const PhysicalFileSplit& source) {
    DORIS_CHECK(destination != nullptr);
    DORIS_CHECK(destination->format_split_id >= 0 && source.format_split_id >= 0);
    const int64_t destination_end = destination->format_split_id_end >= 0
                                            ? destination->format_split_id_end
                                            : destination->format_split_id;
    const int64_t source_end =
            source.format_split_id_end >= 0 ? source.format_split_id_end : source.format_split_id;
    DORIS_CHECK(destination_end < std::numeric_limits<int64_t>::max());
    DORIS_CHECK(source.format_split_id == destination_end + 1);
    destination->format_split_id_end = source_end;
}

std::vector<PhysicalFileSplit> coalesce_physical_splits(std::vector<PhysicalFileSplit> splits,
                                                        int64_t target_split_size) {
    if (target_split_size <= 0 || splits.size() < 2) {
        return splits;
    }

    const uint64_t target_size = static_cast<uint64_t>(target_split_size);
    std::vector<PhysicalFileSplit> merged;
    merged.reserve(splits.size());
    auto current = std::move(splits.front());
    for (size_t index = 1; index < splits.size(); ++index) {
        auto& next = splits[index];
        const bool valid_ranges = current.start_offset >= 0 && current.size >= 0 &&
                                  next.start_offset >= 0 && next.size >= 0;
        const uint64_t current_start =
                valid_ranges ? static_cast<uint64_t>(current.start_offset) : 0;
        const uint64_t current_end = valid_ranges ? current_start + current.size : 0;
        const uint64_t next_start = valid_ranges ? static_cast<uint64_t>(next.start_offset) : 0;
        const uint64_t next_end = valid_ranges ? next_start + next.size : 0;
        const int64_t current_id_end = current.format_split_id_end >= 0
                                               ? current.format_split_id_end
                                               : current.format_split_id;
        const bool consecutive_ids = current_id_end >= 0 &&
                                     current_id_end < std::numeric_limits<int64_t>::max() &&
                                     next.format_split_id == current_id_end + 1;
        // Coalescing changes only scheduling granularity. Exact format ids remain attached so byte
        // padding or skipped physical granules cannot change which rows a child owns.
        const bool fits_target = valid_ranges && current.file_context == next.file_context &&
                                 consecutive_ids && next_start >= current_start &&
                                 next_end >= current_end && next_end - current_start <= target_size;
        if (fits_target) {
            current.size = cast_set<int64_t>(next_end - current_start);
            extend_format_split_id_range(&current, next);
        } else {
            merged.push_back(std::move(current));
            current = std::move(next);
        }
    }
    merged.push_back(std::move(current));

    if (merged.size() > 1) {
        auto& previous = merged[merged.size() - 2];
        auto& tail = merged.back();
        const bool valid_ranges = previous.start_offset >= 0 && previous.size >= 0 &&
                                  tail.start_offset >= 0 && tail.size >= 0;
        const uint64_t previous_start =
                valid_ranges ? static_cast<uint64_t>(previous.start_offset) : 0;
        const uint64_t previous_end = valid_ranges ? previous_start + previous.size : 0;
        const uint64_t tail_start = valid_ranges ? static_cast<uint64_t>(tail.start_offset) : 0;
        const uint64_t tail_end = valid_ranges ? tail_start + tail.size : 0;
        const uint64_t combined_size = valid_ranges ? tail_end - previous_start : 0;
        const int64_t previous_id_end = previous.format_split_id_end >= 0
                                                ? previous.format_split_id_end
                                                : previous.format_split_id;
        const bool consecutive_ids = previous_id_end >= 0 &&
                                     previous_id_end < std::numeric_limits<int64_t>::max() &&
                                     tail.format_split_id == previous_id_end + 1;
        // Avoid a tiny final task only when the ownership envelopes touch. A pruned gap must not
        // inflate its predecessor far beyond the FE target.
        if (valid_ranges && previous.file_context == tail.file_context && consecutive_ids &&
            previous.size <= target_split_size && tail.size <= (target_split_size - 1) / 2 &&
            tail_start <= previous_end && tail_end >= previous_end &&
            combined_size <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
            previous.size = static_cast<int64_t>(combined_size);
            extend_format_split_id_range(&previous, tail);
            merged.pop_back();
        }
    }
    return merged;
}

template <typename T, typename Formatter>
std::string join_table_reader_debug_strings(const std::vector<T>& values, Formatter formatter) {
    std::ostringstream out;
    out << "[";
    for (size_t i = 0; i < values.size(); ++i) {
        if (i > 0) {
            out << ", ";
        }
        out << formatter(values[i]);
    }
    out << "]";
    return out.str();
}

std::string file_format_to_string(FileFormat format) {
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
    case FileFormat::NATIVE:
        return "NATIVE";
    case FileFormat::ARROW:
        return "ARROW";
    case FileFormat::WAL:
        return "WAL";
    case FileFormat::LANCE:
        return "LANCE";
    }
    return "UNKNOWN";
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

bool mapping_reads_variant(const ColumnMapping& mapping) {
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

std::string push_down_agg_to_string(TPushAggOp::type op) {
    switch (op) {
    case TPushAggOp::NONE:
        return "NONE";
    case TPushAggOp::COUNT:
        return "COUNT";
    case TPushAggOp::MINMAX:
        return "MINMAX";
    case TPushAggOp::MIX:
        return "MIX";
    case TPushAggOp::COUNT_ON_INDEX:
        return "COUNT_ON_INDEX";
    }
    return "UNKNOWN";
}

std::string current_file_debug_string(const std::unique_ptr<ScanTask>& task) {
    if (task == nullptr || task->data_file == nullptr) {
        return "null";
    }
    const auto& file = *task->data_file;
    std::ostringstream out;
    out << "FileDescription{path=" << file.path << ", file_size=" << file.file_size
        << ", range_start_offset=" << file.range_start_offset << ", range_size=" << file.range_size
        << ", mtime=" << file.mtime << ", fs_name=" << file.fs_name
        << ", is_immutable=" << file.is_immutable
        << ", file_cache_admission=" << file.file_cache_admission << "}";
    return out.str();
}

std::string partition_values_debug_string(const std::map<std::string, Field>& partition_values) {
    std::ostringstream out;
    out << "{";
    size_t idx = 0;
    for (const auto& [key, _] : partition_values) {
        if (idx++ > 0) {
            out << ", ";
        }
        out << key;
    }
    out << "}";
    return out.str();
}

const schema::external::TField* get_field_ptr(const schema::external::TFieldPtr& field_ptr) {
    if (!field_ptr.__isset.field_ptr || field_ptr.field_ptr == nullptr) {
        return nullptr;
    }
    return field_ptr.field_ptr.get();
}

const schema::external::TField* find_external_field_by_id(
        const schema::external::TStructField* root, int32_t field_id) {
    if (root == nullptr || !root->__isset.fields) {
        return nullptr;
    }
    for (const auto& field_ptr : root->fields) {
        const auto* field = get_field_ptr(field_ptr);
        if (field == nullptr) {
            continue;
        }
        if (field->__isset.id && field->id == field_id) {
            return field;
        }
        if (!field->__isset.nestedField) {
            continue;
        }
        if (field->nestedField.__isset.struct_field) {
            if (const auto* result =
                        find_external_field_by_id(&field->nestedField.struct_field, field_id);
                result != nullptr) {
                return result;
            }
        } else if (field->nestedField.__isset.array_field &&
                   field->nestedField.array_field.__isset.item_field) {
            const auto* child = get_field_ptr(field->nestedField.array_field.item_field);
            if (child != nullptr) {
                schema::external::TStructField child_root;
                child_root.__set_fields({field->nestedField.array_field.item_field});
                if (const auto* result = find_external_field_by_id(&child_root, field_id);
                    result != nullptr) {
                    return result;
                }
            }
        } else if (field->nestedField.__isset.map_field) {
            schema::external::TStructField child_root;
            std::vector<schema::external::TFieldPtr> children;
            if (field->nestedField.map_field.__isset.key_field) {
                children.push_back(field->nestedField.map_field.key_field);
            }
            if (field->nestedField.map_field.__isset.value_field) {
                children.push_back(field->nestedField.map_field.value_field);
            }
            child_root.__set_fields(children);
            if (const auto* result = find_external_field_by_id(&child_root, field_id);
                result != nullptr) {
                return result;
            }
        }
    }
    return nullptr;
}

bool find_external_field_path_by_id(const schema::external::TField* field, int32_t field_id,
                                    std::vector<const schema::external::TField*>* const path) {
    DORIS_CHECK(path != nullptr);
    DORIS_CHECK(field != nullptr);
    path->push_back(field);
    if (field->__isset.id && field->id == field_id) {
        return true;
    }
    if (field->__isset.nestedField && field->nestedField.__isset.struct_field &&
        field->nestedField.struct_field.__isset.fields) {
        for (const auto& child_ptr : field->nestedField.struct_field.fields) {
            const auto* child = get_field_ptr(child_ptr);
            if (child != nullptr && find_external_field_path_by_id(child, field_id, path)) {
                return true;
            }
        }
    }
    path->pop_back();
    return false;
}

std::optional<std::vector<const schema::external::TField*>> find_external_struct_field_path_by_id(
        const schema::external::TSchema& schema, int32_t field_id) {
    if (!schema.__isset.root_field || !schema.root_field.__isset.fields) {
        return std::nullopt;
    }
    std::vector<const schema::external::TField*> path;
    for (const auto& field_ptr : schema.root_field.fields) {
        const auto* field = get_field_ptr(field_ptr);
        if (field != nullptr && find_external_field_path_by_id(field, field_id, &path)) {
            return path;
        }
    }
    return std::nullopt;
}

bool find_column_identity_path_by_id(const std::vector<ColumnDefinition>& fields, int32_t field_id,
                                     std::vector<ColumnDefinition>* path) {
    DORIS_CHECK(path != nullptr);
    for (const auto& field : fields) {
        path->push_back(field);
        if (field.has_identifier_field_id() && field.get_identifier_field_id() == field_id) {
            return true;
        }
        if (find_column_identity_path_by_id(field.children, field_id, path)) {
            return true;
        }
        path->pop_back();
    }
    return false;
}

ColumnDefinition build_schema_identity_from_external_field(const schema::external::TField& field) {
    ColumnDefinition identity;
    if (field.__isset.id) {
        identity.identifier = Field::create_field<TYPE_INT>(field.id);
    }
    identity.name = field.__isset.name ? field.name : "";
    identity.name_mapping =
            field.__isset.name_mapping ? field.name_mapping : std::vector<std::string> {};
    identity.has_name_mapping =
            field.__isset.name_mapping_is_authoritative && field.name_mapping_is_authoritative;
    if (!field.__isset.nestedField) {
        return identity;
    }
    if (field.nestedField.__isset.struct_field && field.nestedField.struct_field.__isset.fields) {
        for (const auto& child_ptr : field.nestedField.struct_field.fields) {
            if (const auto* child = get_field_ptr(child_ptr); child != nullptr) {
                identity.children.push_back(build_schema_identity_from_external_field(*child));
            }
        }
    } else if (field.nestedField.__isset.array_field &&
               field.nestedField.array_field.__isset.item_field) {
        if (const auto* child = get_field_ptr(field.nestedField.array_field.item_field);
            child != nullptr) {
            identity.children.push_back(build_schema_identity_from_external_field(*child));
            identity.children.back().name = "element";
        }
    } else if (field.nestedField.__isset.map_field) {
        if (field.nestedField.map_field.__isset.key_field) {
            if (const auto* child = get_field_ptr(field.nestedField.map_field.key_field);
                child != nullptr) {
                identity.children.push_back(build_schema_identity_from_external_field(*child));
                identity.children.back().name = "key";
            }
        }
        if (field.nestedField.map_field.__isset.value_field) {
            if (const auto* child = get_field_ptr(field.nestedField.map_field.value_field);
                child != nullptr) {
                identity.children.push_back(build_schema_identity_from_external_field(*child));
                identity.children.back().name = "value";
            }
        }
    }
    return identity;
}

const ColumnDefinition* find_identity_child(const ColumnDefinition& projected_child,
                                            const ColumnDefinition& identity_parent) {
    const auto child_it = std::ranges::find_if(
            identity_parent.children, [&](const ColumnDefinition& identity_child) {
                if (projected_child.has_identifier_field_id() &&
                    identity_child.has_identifier_field_id()) {
                    return projected_child.get_identifier_field_id() ==
                           identity_child.get_identifier_field_id();
                }
                if (to_lower(projected_child.name) == to_lower(identity_child.name)) {
                    return true;
                }
                return std::ranges::any_of(
                        identity_child.name_mapping, [&](const std::string& alias) {
                            return to_lower(projected_child.name) == to_lower(alias);
                        });
            });
    return child_it == identity_parent.children.end() ? nullptr : &*child_it;
}

void attach_full_schema_identity(ColumnDefinition* projected, const ColumnDefinition& identity) {
    DORIS_CHECK(projected != nullptr);
    // Access-path children control materialization, but wrapper discovery needs sibling IDs that
    // were pruned from that projection. Keep the complete identity tree on a separate channel.
    projected->identity_children = identity.children;
    for (auto& projected_child : projected->children) {
        if (const auto* identity_child = find_identity_child(projected_child, identity);
            identity_child != nullptr) {
            attach_full_schema_identity(&projected_child, *identity_child);
        }
    }
}

void clear_initial_default_metadata(ColumnDefinition* column) {
    DORIS_CHECK(column != nullptr);
    column->initial_default_value.reset();
    column->initial_default_value_is_base64 = false;
    for (auto& child : column->children) {
        clear_initial_default_metadata(&child);
    }
}

bool external_field_matches_name(const schema::external::TField& field, const std::string& name) {
    if (field.__isset.name && to_lower(field.name) == to_lower(name)) {
        return true;
    }
    return field.__isset.name_mapping &&
           std::ranges::any_of(field.name_mapping, [&](const std::string& alias) {
               return to_lower(alias) == to_lower(name);
           });
}

DataTypePtr find_struct_child_type_by_external_field(const DataTypeStruct& struct_type,
                                                     const schema::external::TField& field,
                                                     bool prefer_current_name) {
    if (prefer_current_name && field.__isset.name) {
        for (size_t field_idx = 0; field_idx < struct_type.get_elements().size(); ++field_idx) {
            if (to_lower(field.name) == to_lower(struct_type.get_element_name(field_idx))) {
                return struct_type.get_element(field_idx);
            }
        }
    }
    for (size_t field_idx = 0; field_idx < struct_type.get_elements().size(); ++field_idx) {
        const auto& element_name = struct_type.get_element_name(field_idx);
        if (external_field_matches_name(field, element_name)) {
            return struct_type.get_element(field_idx);
        }
    }
    return nullptr;
}

DataTypePtr restore_current_primitive_type(const schema::external::TField& field,
                                           DataTypePtr fallback_type) {
    if (!field.__isset.type) {
        return fallback_type;
    }
    DORIS_CHECK(fallback_type != nullptr);
    const auto primitive_type = thrift_to_type(field.type.type);
    if (primitive_type == TYPE_VARIANT) {
        // TColumnType predates the execution-only variant_is_v2 marker. Reconstructing from its
        // primitive enum would silently replace an Iceberg compute-V2 carrier with legacy VARIANT.
        return fallback_type;
    }
    if (is_complex_type(primitive_type)) {
        return fallback_type;
    }
    // The delete file can expose an older physical type, but initial defaults belong to the
    // current table field. Restore that type from FE before parsing the default and let the table
    // reader apply the normal promotion cast to the delete-key type.
    return DataTypeFactory::instance().create_data_type(
            primitive_type, fallback_type->is_nullable(),
            field.type.__isset.precision ? field.type.precision : 0,
            field.type.__isset.scale ? field.type.scale : 0,
            field.type.__isset.len ? field.type.len : -1);
}

ColumnDefinition build_schema_column_metadata_from_external_field(
        const schema::external::TField& field, DataTypePtr type) {
    type = restore_current_primitive_type(field, std::move(type));
    return ColumnDefinition {
            .identifier = field.__isset.id ? Field::create_field<TYPE_INT>(field.id) : Field {},
            .name = field.__isset.name ? field.name : "",
            .name_mapping =
                    field.__isset.name_mapping ? field.name_mapping : std::vector<std::string> {},
            .has_name_mapping = field.__isset.name_mapping_is_authoritative &&
                                field.name_mapping_is_authoritative,
            .type = std::move(type),
            .children = {},
            .default_expr = nullptr,
            .initial_default_value = field.__isset.initial_default_value
                                             ? std::make_optional(field.initial_default_value)
                                             : std::nullopt,
            .initial_default_value_is_base64 = field.__isset.initial_default_value_is_base64 &&
                                               field.initial_default_value_is_base64,
            .is_optional = field.__isset.is_optional ? std::make_optional(field.is_optional)
                                                     : std::nullopt,
            .is_partition_key = false,
    };
}

// NOLINTNEXTLINE(readability-function-size): keep recursive Iceberg type reconstruction together.
ColumnDefinition build_schema_column_from_external_field(const schema::external::TField& field,
                                                         DataTypePtr type,
                                                         bool prefer_current_name) {
    auto column = build_schema_column_metadata_from_external_field(field, std::move(type));
    if (column.type == nullptr || !field.__isset.nestedField) {
        return column;
    }

    const auto nested_type = remove_nullable(column.type);
    switch (nested_type->get_primitive_type()) {
    case TYPE_STRUCT: {
        if (!field.nestedField.__isset.struct_field ||
            !field.nestedField.struct_field.__isset.fields) {
            return column;
        }
        const auto& struct_type = assert_cast<const DataTypeStruct&>(*nested_type);
        for (const auto& child_ptr : field.nestedField.struct_field.fields) {
            const auto* child_field = get_field_ptr(child_ptr);
            if (child_field == nullptr || !child_field->__isset.name) {
                continue;
            }
            auto child_type = find_struct_child_type_by_external_field(struct_type, *child_field,
                                                                       prefer_current_name);
            if (child_type == nullptr) {
                continue;
            }
            column.children.push_back(build_schema_column_from_external_field(
                    *child_field, child_type, prefer_current_name));
        }
        break;
    }
    case TYPE_ARRAY: {
        if (!field.nestedField.__isset.array_field ||
            !field.nestedField.array_field.__isset.item_field) {
            return column;
        }
        const auto* item_field = get_field_ptr(field.nestedField.array_field.item_field);
        if (item_field == nullptr) {
            return column;
        }
        const auto& array_type = assert_cast<const DataTypeArray&>(*nested_type);
        auto child = build_schema_column_from_external_field(
                *item_field, array_type.get_nested_type(), prefer_current_name);
        child.name = "element";
        if (child.has_identifier_name()) {
            child.identifier = Field::create_field<TYPE_STRING>(child.name);
        }
        column.children.push_back(std::move(child));
        break;
    }
    case TYPE_MAP: {
        if (!field.nestedField.__isset.map_field ||
            !field.nestedField.map_field.__isset.key_field ||
            !field.nestedField.map_field.__isset.value_field) {
            return column;
        }
        const auto& map_type = assert_cast<const DataTypeMap&>(*nested_type);
        const auto* key_field = get_field_ptr(field.nestedField.map_field.key_field);
        if (key_field != nullptr) {
            auto child = build_schema_column_from_external_field(
                    *key_field, map_type.get_key_type(), prefer_current_name);
            child.name = "key";
            if (child.has_identifier_name()) {
                child.identifier = Field::create_field<TYPE_STRING>(child.name);
            }
            column.children.push_back(std::move(child));
        }
        const auto* value_field = get_field_ptr(field.nestedField.map_field.value_field);
        if (value_field != nullptr) {
            auto child = build_schema_column_from_external_field(
                    *value_field, map_type.get_value_type(), prefer_current_name);
            child.name = "value";
            if (child.has_identifier_name()) {
                child.identifier = Field::create_field<TYPE_STRING>(child.name);
            }
            column.children.push_back(std::move(child));
        }
        break;
    }
    default:
        break;
    }
    return column;
}

const schema::external::TField* find_external_root_field(const TFileScanRangeParams* params,
                                                         const ColumnDefinition& column) {
    if (params == nullptr || !params->__isset.history_schema_info ||
        params->history_schema_info.empty()) {
        return nullptr;
    }
    const auto* schema = &params->history_schema_info.front();
    if (params->__isset.current_schema_id) {
        for (const auto& candidate_schema : params->history_schema_info) {
            if (candidate_schema.__isset.schema_id &&
                candidate_schema.schema_id == params->current_schema_id) {
                schema = &candidate_schema;
                break;
            }
        }
    }
    if (!schema->__isset.root_field || !schema->root_field.__isset.fields) {
        return nullptr;
    }
    if (!supports_iceberg_scan_semantics_v1(params)) {
        // Old BEs used one ordered current-name/alias pass. Preserve that result for old-FE plans
        // until the explicit scan-semantics marker makes exact-name precedence cluster-wide.
        for (const auto& field_ptr : schema->root_field.fields) {
            const auto* field = get_field_ptr(field_ptr);
            if (field != nullptr && external_field_matches_name(*field, column.name)) {
                return field;
            }
        }
        return nullptr;
    }
    // A reused name identifies the newly added field, not an older sibling that retained that
    // spelling as an alias. Exhaust exact current names before consulting historical aliases.
    for (const auto& field_ptr : schema->root_field.fields) {
        const auto* field = get_field_ptr(field_ptr);
        if (field != nullptr && field->__isset.name &&
            to_lower(field->name) == to_lower(column.name)) {
            return field;
        }
    }
    for (const auto& field_ptr : schema->root_field.fields) {
        const auto* field = get_field_ptr(field_ptr);
        if (field != nullptr && field->__isset.name_mapping &&
            std::ranges::any_of(field->name_mapping, [&](const std::string& alias) {
                return to_lower(alias) == to_lower(column.name);
            })) {
            return field;
        }
    }
    return nullptr;
}

std::string expr_context_debug_string(const VExprContextSPtr& context) {
    if (context == nullptr) {
        return "null";
    }
    const auto root = context->root();
    if (root == nullptr) {
        return "VExprContext{root=null}";
    }
    std::ostringstream out;
    out << "VExprContext{root_name=" << root->expr_name() << ", root_debug=" << root->debug_string()
        << "}";
    return out.str();
}

std::string table_filter_debug_string(const TableFilter& filter) {
    std::ostringstream out;
    out << "TableFilter{conjunct=" << expr_context_debug_string(filter.conjunct)
        << ", global_indices="
        << join_table_reader_debug_strings(
                   filter.global_indices,
                   [](GlobalIndex global_index) { return std::to_string(global_index.value()); })
        << "}";
    return out.str();
}

bool contains_runtime_filter(const VExprContextSPtrs& conjuncts) {
    return std::ranges::any_of(conjuncts, [](const auto& conjunct) {
        return conjunct != nullptr && conjunct->root() != nullptr &&
               conjunct->root()->is_rf_wrapper();
    });
}

void collect_global_indices(const VExprSPtr& expr, std::set<GlobalIndex>* global_indices) {
    if (expr == nullptr) {
        return;
    }
    if (expr->is_rf_wrapper()) {
        // RuntimeFilterExpr wraps a real predicate expression but its own thrift node can still
        // look like SLOT_REF. Collect indices from the wrapped predicate; do not cast the wrapper
        // itself to VSlotRef.
        collect_global_indices(expr->get_impl(), global_indices);
        return;
    }
    if (expr->is_slot_ref()) {
        const auto* slot_ref = assert_cast<const VSlotRef*>(expr.get());
        DORIS_CHECK(slot_ref->column_id() >= 0);
        global_indices->insert(GlobalIndex(cast_set<size_t>(slot_ref->column_id())));
    }
    for (const auto& child : expr->children()) {
        collect_global_indices(child, global_indices);
    }
}

Status build_table_filters_from_conjunct(const VExprContextSPtr& conjunct, RuntimeState* state,
                                         std::vector<TableFilter>* table_filters) {
    if (conjunct == nullptr) {
        return Status::OK();
    }
    std::set<GlobalIndex> global_indices;
    collect_global_indices(conjunct->root(), &global_indices);
    if (!global_indices.empty()) {
        TableFilter table_filter;
        VExprSPtr filter_root;
        RETURN_IF_ERROR(clone_table_expr_tree(conjunct->root(), &filter_root));
        table_filter.conjunct = VExprContext::create_shared(std::move(filter_root));
        for (const auto global_index : global_indices) {
            table_filter.global_indices.push_back(global_index);
        }
        table_filters->push_back(std::move(table_filter));
    }
    return Status::OK();
}

Status parse_deletion_vector(const char* buf, size_t buffer_size, DeleteFileDesc::Format format,
                             DeletionVector* deletion_vector) {
    DORIS_CHECK(buf != nullptr);
    DORIS_CHECK(deletion_vector != nullptr);
    DORIS_CHECK(format == DeleteFileDesc::Format::PAIMON ||
                format == DeleteFileDesc::Format::ICEBERG);

    if (format == DeleteFileDesc::Format::PAIMON) {
        RETURN_IF_ERROR(decode_paimon_deletion_vector_buffer(buf, buffer_size, deletion_vector));
        return Status::OK();
    }

    return decode_iceberg_deletion_vector_buffer(buf, buffer_size, deletion_vector);
}

} // namespace

std::shared_ptr<io::FileSystemProperties> create_system_properties(
        const TFileScanRangeParams* scan_params) {
    auto system_properties = std::make_shared<io::FileSystemProperties>();
    if (scan_params == nullptr || !scan_params->__isset.file_type) {
        system_properties->system_type = TFileType::FILE_LOCAL;
        return system_properties;
    }
    system_properties->system_type = scan_params->file_type;
    system_properties->properties = scan_params->properties;
    system_properties->hdfs_params = scan_params->hdfs_params;
    if (scan_params->__isset.broker_addresses) {
        system_properties->broker_addresses.assign(scan_params->broker_addresses.begin(),
                                                   scan_params->broker_addresses.end());
    }
    return system_properties;
}

std::string TableReader::debug_string() const {
    std::ostringstream out;
    out << "TableReader{format=" << file_format_to_string(_format)
        << ", push_down_agg_type=" << push_down_agg_to_string(_push_down_agg_type)
        << ", aggregate_pushdown_tried=" << _aggregate_pushdown_tried
        << ", has_current_reader=" << (_data_reader.reader != nullptr)
        << ", has_current_task=" << (_current_task != nullptr)
        << ", current_file=" << current_file_debug_string(_current_task)
        << ", has_delete_rows=" << (_delete_rows != nullptr)
        << ", delete_row_count=" << (_delete_rows == nullptr ? 0 : _delete_rows->size())
        << ", has_deletion_vector=" << (_deletion_vector != nullptr)
        << ", deletion_vector_cardinality="
        << (_deletion_vector == nullptr ? 0 : _deletion_vector->cardinality())
        << ", has_system_properties=" << (_system_properties != nullptr) << ", system_type="
        << (_system_properties == nullptr ? static_cast<int>(TFileType::FILE_LOCAL)
                                          : static_cast<int>(_system_properties->system_type))
        << ", has_scan_params=" << (_scan_params != nullptr)
        << ", has_io_ctx=" << (_io_ctx != nullptr)
        << ", has_runtime_state=" << (_runtime_state != nullptr)
        << ", has_scanner_profile=" << (_scanner_profile != nullptr)
        << ", mapper_options=" << _mapper_options.debug_string() << ", projected_columns="
        << join_table_reader_debug_strings(
                   _projected_columns,
                   [](const ColumnDefinition& column) { return column.debug_string(); })
        << ", partition_values=" << partition_values_debug_string(_partition_values)
        << ", table_filters="
        << join_table_reader_debug_strings(
                   _table_filters,
                   [](const TableFilter& filter) { return table_filter_debug_string(filter); })
        << ", conjunct_count=" << _conjuncts.size() << ", conjuncts="
        << join_table_reader_debug_strings(_conjuncts,
                                           [](const VExprContextSPtr& conjunct) {
                                               return expr_context_debug_string(conjunct);
                                           })
        << ", file_schema="
        << join_table_reader_debug_strings(
                   _data_reader.file_schema,
                   [](const ColumnDefinition& field) { return field.debug_string(); })
        << ", file_block_layout="
        << join_table_reader_debug_strings(
                   _data_reader.file_block_layout,
                   [](const FileBlockColumn& column) {
                       std::ostringstream column_out;
                       column_out << "FileBlockColumn{file_column_id=" << column.file_column_id
                                  << ", name=" << column.name << ", type="
                                  << (column.type == nullptr ? "null" : column.type->get_name())
                                  << "}";
                       return column_out.str();
                   })
        << ", block_template_columns=" << _data_reader.block_template.columns()
        << ", column_mapper="
        << (_data_reader.column_mapper == nullptr ? "null"
                                                  : _data_reader.column_mapper->debug_string())
        << "}";
    return out.str();
}

Status TableReader::annotate_projected_column(const TFileScanSlotInfo& slot_info,
                                              ProjectedColumnBuildContext* context,
                                              ColumnDefinition* column) const {
    (void)slot_info;
    DORIS_CHECK(context != nullptr);
    DORIS_CHECK(column != nullptr);
    context->schema_column.reset();
    const auto* schema_field = find_external_root_field(context->scan_params, *column);
    if (schema_field == nullptr) {
        return Status::OK();
    }
    const bool use_current_semantics = supports_iceberg_scan_semantics_v1(context->scan_params);
    context->schema_column = build_schema_column_from_external_field(*schema_field, column->type,
                                                                     use_current_semantics);
    if (!use_current_semantics) {
        // IDs and encoded defaults predate the result-changing semantics. Strip only the new
        // default channel so an old-FE plan keeps the same generic root/nested values on every BE.
        clear_initial_default_metadata(&*context->schema_column);
    }
    column->identifier = context->schema_column->identifier;
    column->name_mapping = context->schema_column->name_mapping;
    column->has_name_mapping = context->schema_column->has_name_mapping;
    // Projected roots already carry a generic FE default expression, but Iceberg binary defaults
    // need the raw Base64 marker so missing-file materialization can decode rather than copy text.
    column->initial_default_value = context->schema_column->initial_default_value;
    column->initial_default_value_is_base64 =
            context->schema_column->initial_default_value_is_base64;
    return Status::OK();
}

std::optional<ColumnDefinition> TableReader::_find_table_column_by_field_id(
        int32_t field_id, DataTypePtr type, bool include_historical_schemas) const {
    if (_scan_params == nullptr || !_scan_params->__isset.history_schema_info ||
        _scan_params->history_schema_info.empty()) {
        return std::nullopt;
    }
    const auto find_field = [field_id](const schema::external::TSchema& schema) {
        return schema.__isset.root_field ? find_external_field_by_id(&schema.root_field, field_id)
                                         : nullptr;
    };

    const auto* current_schema = &_scan_params->history_schema_info.front();
    if (_scan_params->__isset.current_schema_id) {
        for (const auto& candidate_schema : _scan_params->history_schema_info) {
            if (candidate_schema.__isset.schema_id &&
                candidate_schema.schema_id == _scan_params->current_schema_id) {
                current_schema = &candidate_schema;
                break;
            }
        }
    }
    if (const auto* field = find_field(*current_schema); field != nullptr) {
        return build_schema_column_from_external_field(
                *field, std::move(type), supports_iceberg_scan_semantics_v1(_scan_params));
    }
    if (const auto* split_schema = _split_schema(); split_schema != nullptr) {
        if (const auto* field = find_field(*split_schema); field != nullptr) {
            return build_schema_column_from_external_field(
                    *field, std::move(type), supports_iceberg_scan_semantics_v1(_scan_params));
        }
    }
    if (!include_historical_schemas) {
        return std::nullopt;
    }

    const schema::external::TSchema* latest_schema = nullptr;
    const schema::external::TField* latest_field = nullptr;
    for (const auto& candidate_schema : _scan_params->history_schema_info) {
        if (&candidate_schema == current_schema) {
            continue;
        }
        const auto* candidate_field = find_field(candidate_schema);
        if (candidate_field == nullptr) {
            continue;
        }
        if (latest_schema == nullptr || (candidate_schema.__isset.schema_id &&
                                         (!latest_schema->__isset.schema_id ||
                                          candidate_schema.schema_id > latest_schema->schema_id))) {
            latest_schema = &candidate_schema;
            latest_field = candidate_field;
        }
    }
    if (latest_field == nullptr) {
        return std::nullopt;
    }
    return build_schema_column_from_external_field(
            *latest_field, std::move(type), supports_iceberg_scan_semantics_v1(_scan_params));
}

std::optional<std::vector<ColumnDefinition>> TableReader::_find_table_column_path_by_field_id(
        int32_t field_id, DataTypePtr leaf_type, bool include_historical_schemas) const {
    if (_scan_params == nullptr || !_scan_params->__isset.history_schema_info ||
        _scan_params->history_schema_info.empty()) {
        return std::nullopt;
    }
    const auto build_path = [&](const schema::external::TSchema& schema)
            -> std::optional<std::vector<ColumnDefinition>> {
        auto external_path = find_external_struct_field_path_by_id(schema, field_id);
        if (!external_path.has_value()) {
            return std::nullopt;
        }

        std::vector<DataTypePtr> path_types(external_path->size());
        path_types.back() = leaf_type;
        for (size_t index = external_path->size(); index > 1; --index) {
            const auto* parent = (*external_path)[index - 2];
            const auto* child = (*external_path)[index - 1];
            DORIS_CHECK(parent != nullptr);
            DORIS_CHECK(child != nullptr);
            DORIS_CHECK(child->__isset.name);
            if (!parent->__isset.nestedField || !parent->nestedField.__isset.struct_field) {
                return std::nullopt;
            }
            DataTypePtr path_type = std::make_shared<DataTypeStruct>(
                    DataTypes {path_types[index - 1]}, Strings {child->name});
            if (parent->__isset.is_optional && parent->is_optional) {
                path_type = make_nullable(path_type);
            }
            path_types[index - 2] = std::move(path_type);
        }

        std::vector<ColumnDefinition> result;
        result.reserve(external_path->size());
        for (size_t index = 0; index < external_path->size(); ++index) {
            result.push_back(build_schema_column_metadata_from_external_field(
                    *(*external_path)[index], path_types[index]));
        }
        // Keep metadata hierarchy aligned with the synthetic exact-ID ancestor types.
        for (size_t index = result.size(); index > 1; --index) {
            result[index - 2].children.push_back(result[index - 1]);
        }
        return result;
    };

    const auto* current_schema = &_scan_params->history_schema_info.front();
    if (_scan_params->__isset.current_schema_id) {
        for (const auto& candidate_schema : _scan_params->history_schema_info) {
            if (candidate_schema.__isset.schema_id &&
                candidate_schema.schema_id == _scan_params->current_schema_id) {
                current_schema = &candidate_schema;
                break;
            }
        }
    }
    if (auto path = build_path(*current_schema); path.has_value()) {
        return path;
    }
    if (const auto* split_schema = _split_schema(); split_schema != nullptr) {
        if (auto path = build_path(*split_schema); path.has_value()) {
            return path;
        }
    }
    if (!include_historical_schemas) {
        return std::nullopt;
    }

    const schema::external::TSchema* latest_schema = nullptr;
    std::optional<std::vector<ColumnDefinition>> latest_path;
    for (const auto& candidate_schema : _scan_params->history_schema_info) {
        if (&candidate_schema == current_schema) {
            continue;
        }
        auto candidate_path = build_path(candidate_schema);
        if (!candidate_path.has_value()) {
            continue;
        }
        if (latest_schema == nullptr || (candidate_schema.__isset.schema_id &&
                                         (!latest_schema->__isset.schema_id ||
                                          candidate_schema.schema_id > latest_schema->schema_id))) {
            latest_schema = &candidate_schema;
            latest_path = std::move(candidate_path);
        }
    }
    return latest_path;
}

std::optional<std::vector<ColumnDefinition>>
TableReader::_find_table_column_identity_path_by_field_id(int32_t field_id,
                                                          bool include_historical_schemas) const {
    if (_scan_params == nullptr || !_scan_params->__isset.history_schema_info ||
        _scan_params->history_schema_info.empty()) {
        return std::nullopt;
    }
    const auto find_path = [field_id](const schema::external::TSchema& schema)
            -> std::optional<std::vector<ColumnDefinition>> {
        if (!schema.__isset.root_field || !schema.root_field.__isset.fields) {
            return std::nullopt;
        }
        std::vector<ColumnDefinition> roots;
        roots.reserve(schema.root_field.fields.size());
        for (const auto& field_ptr : schema.root_field.fields) {
            const auto* field = get_field_ptr(field_ptr);
            if (field != nullptr) {
                roots.push_back(build_schema_identity_from_external_field(*field));
            }
        }
        std::vector<ColumnDefinition> path;
        if (find_column_identity_path_by_id(roots, field_id, &path)) {
            return path;
        }
        return std::nullopt;
    };

    const auto* current_schema = &_scan_params->history_schema_info.front();
    if (_scan_params->__isset.current_schema_id) {
        for (const auto& candidate_schema : _scan_params->history_schema_info) {
            if (candidate_schema.__isset.schema_id &&
                candidate_schema.schema_id == _scan_params->current_schema_id) {
                current_schema = &candidate_schema;
                break;
            }
        }
    }
    if (auto path = find_path(*current_schema); path.has_value()) {
        return path;
    }
    if (const auto* split_schema = _split_schema(); split_schema != nullptr) {
        if (auto path = find_path(*split_schema); path.has_value()) {
            return path;
        }
    }
    if (!include_historical_schemas) {
        return std::nullopt;
    }

    const schema::external::TSchema* latest_schema = nullptr;
    std::optional<std::vector<ColumnDefinition>> latest_path;
    for (const auto& candidate_schema : _scan_params->history_schema_info) {
        if (&candidate_schema == current_schema) {
            continue;
        }
        auto candidate_path = find_path(candidate_schema);
        if (!candidate_path.has_value()) {
            continue;
        }
        if (latest_schema == nullptr || (candidate_schema.__isset.schema_id &&
                                         (!latest_schema->__isset.schema_id ||
                                          candidate_schema.schema_id > latest_schema->schema_id))) {
            latest_schema = &candidate_schema;
            latest_path = std::move(candidate_path);
        }
    }
    return latest_path;
}

Status TableReader::init(TableReadOptions&& options) {
    _scanner_profile = options.scanner_profile;
    if (_scanner_profile != nullptr) {
        const auto hierarchy = file_scan_profile::ensure_hierarchy(_scanner_profile);
        static const char* table_profile = file_scan_profile::TABLE_READER;
        static const char* file_reader_profile = file_scan_profile::FILE_READER;
        _profile.total_timer = hierarchy.table_reader;
        _profile.file_reader_total_timer = hierarchy.file_reader;
        _profile.init_timer =
                ADD_CHILD_TIMER_WITH_LEVEL(_scanner_profile, "InitTime", table_profile, 1);
        _profile.num_delete_files = ADD_CHILD_COUNTER_WITH_LEVEL(_scanner_profile, "NumDeleteFiles",
                                                                 TUnit::UNIT, table_profile, 1);
        _profile.num_delete_rows = ADD_CHILD_COUNTER_WITH_LEVEL(_scanner_profile, "NumDeleteRows",
                                                                TUnit::UNIT, table_profile, 1);
        _profile.parse_delete_file_time = ADD_CHILD_TIMER_WITH_LEVEL(
                _scanner_profile, "ParseDeleteFileTime", table_profile, 1);
        _profile.decoded_dv_cache_hit_count =
                ADD_CHILD_COUNTER_WITH_LEVEL(_scanner_profile, "DeletionVectorDecodedCacheHitCount",
                                             TUnit::UNIT, table_profile, 1);
        _profile.decoded_dv_cache_miss_count = ADD_CHILD_COUNTER_WITH_LEVEL(
                _scanner_profile, "DeletionVectorDecodedCacheMissCount", TUnit::UNIT, table_profile,
                1);
        _profile.dv_file_cache_hit_count = ADD_CHILD_COUNTER_WITH_LEVEL(
                _scanner_profile, "DeletionVectorFileCacheHitCount", TUnit::UNIT, table_profile, 1);
        _profile.dv_file_cache_miss_count =
                ADD_CHILD_COUNTER_WITH_LEVEL(_scanner_profile, "DeletionVectorFileCacheMissCount",
                                             TUnit::UNIT, table_profile, 1);
        _profile.dv_file_cache_peer_read_count = ADD_CHILD_COUNTER_WITH_LEVEL(
                _scanner_profile, "DeletionVectorFileCachePeerReadCount", TUnit::UNIT,
                table_profile, 1);
        _profile.exec_timer =
                ADD_CHILD_TIMER_WITH_LEVEL(_scanner_profile, "GetBlockTime", table_profile, 1);
        _profile.prepare_split_timer =
                ADD_CHILD_TIMER_WITH_LEVEL(_scanner_profile, "PrepareSplitTime", table_profile, 1);
        _profile.finalize_timer =
                ADD_CHILD_TIMER_WITH_LEVEL(_scanner_profile, "FinalizeBlockTime", table_profile, 1);
        _profile.create_reader_timer =
                ADD_CHILD_TIMER_WITH_LEVEL(_scanner_profile, "CreateReaderTime", table_profile, 1);
        _profile.pushdown_agg_timer =
                ADD_CHILD_TIMER_WITH_LEVEL(_scanner_profile, "PushDownAggTime", table_profile, 1);
        _profile.open_reader_timer =
                ADD_CHILD_TIMER_WITH_LEVEL(_scanner_profile, "OpenReaderTime", table_profile, 1);
        _profile.refresh_conjuncts_timer = ADD_CHILD_TIMER_WITH_LEVEL(
                _scanner_profile, "RefreshConjunctsTime", table_profile, 1);
        _profile.runtime_filter_partition_prune_timer = ADD_CHILD_TIMER_WITH_LEVEL(
                _scanner_profile, "FileScannerRuntimeFilterPartitionPruningTime", table_profile, 1);
        _profile.runtime_filter_partition_pruned_range_counter = ADD_CHILD_COUNTER_WITH_LEVEL(
                _scanner_profile, "RuntimeFilterPartitionPrunedRangeNum", TUnit::UNIT,
                table_profile, 1);
        _profile.close_timer =
                ADD_CHILD_TIMER_WITH_LEVEL(_scanner_profile, "CloseTime", table_profile, 1);
        // Lifecycle timer names remain globally unique because RuntimeProfile's visual hierarchy
        // does not namespace counters that share the same display parent.
        _profile.file_reader_init_timer = ADD_CHILD_TIMER_WITH_LEVEL(
                _scanner_profile, "FileReaderInitTime", file_reader_profile, 1);
        _profile.file_reader_schema_timer = ADD_CHILD_TIMER_WITH_LEVEL(
                _scanner_profile, "FileReaderGetSchemaTime", file_reader_profile, 1);
        _profile.file_reader_mapper_timer = ADD_CHILD_TIMER_WITH_LEVEL(
                _scanner_profile, "FileReaderCreateColumnMapperTime", file_reader_profile, 1);
        _profile.file_reader_open_timer = ADD_CHILD_TIMER_WITH_LEVEL(
                _scanner_profile, "FileReaderOpenTime", file_reader_profile, 1);
        _profile.file_reader_refresh_timer = ADD_CHILD_TIMER_WITH_LEVEL(
                _scanner_profile, "FileReaderRefreshScanRequestTime", file_reader_profile, 1);
        _profile.file_reader_get_block_timer = ADD_CHILD_TIMER_WITH_LEVEL(
                _scanner_profile, "FileReaderGetBlockTime", file_reader_profile, 1);
        _profile.file_reader_aggregate_timer = ADD_CHILD_TIMER_WITH_LEVEL(
                _scanner_profile, "FileReaderAggregatePushDownTime", file_reader_profile, 1);
        _profile.file_reader_close_timer = ADD_CHILD_TIMER_WITH_LEVEL(
                _scanner_profile, "FileReaderCloseTime", file_reader_profile, 1);
    }
    // Establish lifecycle timers before consuming options or constructing filesystem properties;
    // placing these scopes at the tail records only scope teardown and hides expensive init work.
    SCOPED_TIMER(_profile.total_timer);
    SCOPED_TIMER(_profile.init_timer);
    _scan_params = options.scan_params;
    _format = options.format;
    _io_ctx = options.io_ctx;
    _runtime_state = options.runtime_state;
    _file_slot_descs = options.file_slot_descs;
    _push_down_agg_type = options.push_down_agg_type;
    _push_down_count_columns = options.push_down_count_columns;
    _initial_condition_cache_digest = options.condition_cache_digest;
    _condition_cache_digest = _initial_condition_cache_digest;
    _projected_columns = std::move(options.projected_columns);
    if (supports_iceberg_scan_semantics_v1(_scan_params)) {
        for (auto& projected_column : _projected_columns) {
            const auto* schema_field = find_external_root_field(_scan_params, projected_column);
            if (schema_field != nullptr) {
                attach_full_schema_identity(
                        &projected_column,
                        build_schema_identity_from_external_field(*schema_field));
            }
        }
    }
    _system_properties = create_system_properties(_scan_params);
    _mapper_options.mode = TableColumnMappingMode::BY_NAME;
    _conjuncts = std::move(options.conjuncts);
    _predicate_snapshot_digest = build_predicate_snapshot_digest(_conjuncts);
    return Status::OK();
}

Status TableReader::validate_variant_file_mappings(FileFormat format,
                                                   const std::vector<ColumnMapping>& mappings) {
    if (format == FileFormat::PARQUET || !std::ranges::any_of(mappings, mapping_reads_variant)) {
        return Status::OK();
    }
    // Gate on a physical mapping, not the table schema: an older file may legitimately omit a
    // Variant field added by schema evolution, in which case the mapper synthesizes NULL.
    return Status::NotSupported(
            "External Variant is supported only for Parquet files in FileScannerV2; file format "
            "{} is not supported",
            file_format_to_string(format));
}

Status TableReader::validate_file_mapping(const TableColumnMapper& mapper) const {
    if (_push_down_agg_type == TPushAggOp::type::COUNT && _push_down_count_columns.has_value() &&
        _push_down_count_columns->empty()) {
        // COUNT(*) may retain an arbitrary minimum-width slot, but that carrier is never a
        // semantic physical read and must not trigger the Variant file-format capability gate.
        return Status::OK();
    }
    return validate_variant_file_mappings(_format, mapper.mappings());
}

Status TableReader::_build_table_filters_from_conjuncts() {
    _table_filters.clear();
    _constant_pruning_safe_filter_count = 0;
    bool in_safe_prefix = true;
    for (const auto& conjunct : _conjuncts) {
        DORIS_CHECK(conjunct != nullptr);
        DORIS_CHECK(conjunct->root() != nullptr);
        // `_table_filters` omits expressions without slot references, but such an expression still
        // occupies a position in the row-level conjunct order. Record how many localized filters
        // precede the first unsafe original conjunct so constant pruning cannot jump over a
        // slotless non-deterministic/error-preserving barrier. Unsafe predicates remain solely on
        // Scanner's original row-level path because localizing a clone would execute their state
        // twice with independent state.
        if (in_safe_prefix && !_is_safe_to_pre_execute(conjunct)) {
            in_safe_prefix = false;
        }
        const size_t first_new_filter = _table_filters.size();
        RETURN_IF_ERROR(
                build_table_filters_from_conjunct(conjunct, _runtime_state, &_table_filters));
        for (size_t filter_idx = first_new_filter; filter_idx < _table_filters.size();
             ++filter_idx) {
            _table_filters[filter_idx].metadata_pruning_safe = in_safe_prefix;
        }
        if (in_safe_prefix) {
            _constant_pruning_safe_filter_count = _table_filters.size();
        }
    }
    return Status::OK();
}

namespace {

bool same_scan_projection(const LocalColumnIndex& lhs, const LocalColumnIndex& rhs) {
    if (lhs.index != rhs.index || lhs.project_all_children != rhs.project_all_children ||
        lhs.children.size() != rhs.children.size()) {
        return false;
    }
    for (size_t index = 0; index < lhs.children.size(); ++index) {
        if (!same_scan_projection(lhs.children[index], rhs.children[index])) {
            return false;
        }
    }
    return true;
}

const LocalColumnIndex* find_scan_projection(const FileScanRequest& request,
                                             LocalColumnId column_id) {
    const auto find_by_id = [column_id](const std::vector<LocalColumnIndex>& projections) {
        return std::ranges::find_if(projections, [column_id](const LocalColumnIndex& projection) {
            return projection.column_id() == column_id;
        });
    };
    auto it = find_by_id(request.predicate_columns);
    if (it != request.predicate_columns.end()) {
        return &*it;
    }
    it = find_by_id(request.non_predicate_columns);
    return it == request.non_predicate_columns.end() ? nullptr : &*it;
}

bool same_physical_scan_layout(const FileScanRequest& lhs, const FileScanRequest& rhs) {
    if (lhs.local_positions != rhs.local_positions) {
        return false;
    }
    for (const auto& [column_id, _] : lhs.local_positions) {
        const auto* lhs_projection = find_scan_projection(lhs, column_id);
        const auto* rhs_projection = find_scan_projection(rhs, column_id);
        if (lhs_projection == nullptr || rhs_projection == nullptr ||
            !same_scan_projection(*lhs_projection, *rhs_projection)) {
            return false;
        }
    }
    return true;
}

} // namespace

Status TableReader::refresh_conjuncts(VExprContextSPtrs conjuncts,
                                      std::optional<uint64_t> condition_cache_digest,
                                      bool all_runtime_filters_applied) {
    SCOPED_TIMER(_profile.total_timer);
    SCOPED_TIMER(_profile.refresh_conjuncts_timer);
    _conjuncts = std::move(conjuncts);
    _predicate_snapshot_digest = build_predicate_snapshot_digest(_conjuncts);
    // A prepared footer result belongs to the prior immutable predicate snapshot. Discard it
    // before a refresh can make the planning reader resume ordinary row production.
    _metadata_aggregate_result.reset();
    if (all_runtime_filters_applied) {
        // A refresh can prove the last pending RF has arrived, but a later partial refresh must
        // never make the same split incomplete again.
        _all_runtime_filters_applied_for_split = true;
    }
    if (condition_cache_digest.has_value()) {
        // A runtime filter can arrive after a physical child is prepared but before its reader is
        // created. Keep that child's cache key tied to the same refreshed predicate snapshot.
        _condition_cache_digest = *condition_cache_digest;
        _condition_cache_digest_covers_current_split = true;
    }
    if (_data_reader.reader == nullptr) {
        // The split is prepared but its physical reader has not opened yet. open_reader() will use
        // this newest snapshot directly, so no pending request is needed.
        return Status::OK();
    }
    if (!_data_reader.reader->supports_scan_request_refresh()) {
        return Status::OK();
    }

    RETURN_IF_ERROR(_build_table_filters_from_conjuncts());
    // create_scan_request() rebuilds mapping projections in place. Build late predicates with an
    // isolated mapper so the active row group cannot observe an unprepared or incompatible mapper
    // before its physical request reaches the reader's safe activation boundary.
    auto refreshed_mapper = _data_reader.reader->create_column_mapper(_mapper_options);
    DORIS_CHECK(refreshed_mapper != nullptr);
    RETURN_IF_ERROR(refreshed_mapper->create_mapping(_projected_columns, _partition_values,
                                                     _data_reader.file_schema));
    auto refreshed_request = std::make_shared<FileScanRequest>();
    RETURN_IF_ERROR(refreshed_mapper->create_scan_request(
            _table_filters, _projected_columns, refreshed_request.get(), _runtime_state,
            _file_scan_request == nullptr ? nullptr : &_file_scan_request->local_positions));
    refreshed_request->predicate_snapshot_digest = _predicate_snapshot_digest;
    // A refresh does not prove that every future runtime filter has arrived. Keep carrier values
    // available whenever the split started with pending filters.
    if (_push_down_agg_type == TPushAggOp::type::COUNT && _push_down_count_columns.has_value() &&
        _push_down_count_columns->empty() && _all_runtime_filters_applied_for_split) {
        for (const auto& column : refreshed_request->non_predicate_columns) {
            if (!refreshed_request->is_residual_predicate_column(column.column_id())) {
                refreshed_request->count_star_placeholder_columns.push_back(column.column_id());
            }
        }
    }
    RETURN_IF_ERROR(customize_file_scan_request(refreshed_request.get()));
    if (_file_scan_request == nullptr ||
        !same_physical_scan_layout(*refreshed_request, *_file_scan_request)) {
        // A reader cannot reinterpret columns already materialized with another block layout.
        // Keep scanner-level filtering as the correctness fallback for hidden slots or nested
        // projections instead of switching an incompatible physical shape mid-file.
        return Status::OK();
    }
    RETURN_IF_ERROR(_open_local_filter_exprs(*refreshed_request));

    if (_condition_cache_ctx != nullptr && !_condition_cache_ctx->is_hit) {
        // Rows before and after a late RF were evaluated by different predicate snapshots. Such a
        // partial MISS bitmap must never be published under either snapshot's cache key.
        _condition_cache_split_invalid = _condition_cache_split_participating;
        _condition_cache = nullptr;
        _condition_cache_ctx = nullptr;
        _data_reader.reader->set_condition_cache_context(nullptr);
    }
    {
        SCOPED_TIMER(_profile.file_reader_total_timer);
        SCOPED_TIMER(_profile.file_reader_refresh_timer);
        RETURN_IF_ERROR(_data_reader.reader->queue_scan_request(refreshed_request));
    }
    _file_scan_request = std::move(refreshed_request);
    return Status::OK();
}

Status TableReader::_open_local_filter_exprs(const FileScanRequest& file_request) {
    RowDescriptor row_desc;
    for (const auto& conjunct : file_request.conjuncts) {
        RETURN_IF_ERROR(conjunct->prepare(_runtime_state, row_desc));
        RETURN_IF_ERROR(conjunct->open(_runtime_state));
    }
    for (const auto& delete_conjunct : file_request.delete_conjuncts) {
        RETURN_IF_ERROR(delete_conjunct->prepare(_runtime_state, row_desc));
        RETURN_IF_ERROR(delete_conjunct->open(_runtime_state));
    }
    return Status::OK();
}

bool TableReader::_should_enable_condition_cache(const FileScanRequest& file_request) const {
    if (_condition_cache_digest == 0 || _push_down_agg_type == TPushAggOp::type::COUNT ||
        _current_file_description == std::nullopt || _data_reader.reader == nullptr) {
        return false;
    }
    // Condition cache is populated by file readers after evaluating file-local row-level
    // conjuncts. Metadata pruning can skip row groups/pages, but it does not produce a per-row
    // survivor bitmap that can safely populate the cache.
    if (file_request.conjuncts.empty()) {
        return false;
    }
    // Delete files/deletion vectors are table-format state. They may change independently of the
    // data file path/mtime/size used by the external cache key, so caching their result can become
    // stale. Keep delete filtering enabled, but do not read or write condition cache.
    if (_delete_rows != nullptr || _deletion_vector != nullptr ||
        !file_request.delete_conjuncts.empty()) {
        return false;
    }
    // Only scanner-driven splits provide a digest rebuilt from the exact RF snapshot. Keep the
    // conservative behavior for standalone TableReader callers: their initial digest may describe
    // only static predicate P and must not store P AND RF under that key.
    return _condition_cache_digest_covers_current_split ||
           !contains_runtime_filter(file_request.conjuncts);
}

Status TableReader::_init_reader_condition_cache(const FileScanRequest& file_request) {
    _condition_cache = nullptr;
    _condition_cache_ctx = nullptr;
    if (!_should_enable_condition_cache(file_request)) {
        _condition_cache_split_invalid = _condition_cache_split_participating;
        return Status::OK();
    }

    auto* cache = segment_v2::ConditionCache::instance();
    if (cache == nullptr) {
        _condition_cache_split_invalid = _condition_cache_split_participating;
        return Status::OK();
    }
    const auto& file = *_current_file_description;
    const auto cache_start = _condition_cache_source_range.has_value()
                                     ? _condition_cache_source_range->first
                                     : file.range_start_offset;
    const auto cache_size = _condition_cache_source_range.has_value()
                                    ? _condition_cache_source_range->second
                                    : file.range_size;
    _condition_cache_key = segment_v2::ConditionCache::ExternalCacheKey(
            file.path, file.mtime, file.file_size, _condition_cache_digest, cache_start, cache_size,
            segment_v2::ConditionCache::ExternalCacheKey::BASE_GRANULE_AWARE_VERSION);
    _condition_cache_initialized = true;

    segment_v2::ConditionCacheHandle handle;
    const bool condition_cache_hit = cache->lookup(_condition_cache_key, &handle);
    if (condition_cache_hit) {
        _condition_cache = handle.get_filter_result();
        ++_condition_cache_hit_count;
    } else {
        const int64_t total_rows = _data_reader.reader->get_total_rows();
        if (total_rows <= 0) {
            return Status::OK();
        }
        // Add one guard granule for split ranges that start in the middle of a granule. A guard
        // false bit beyond the real range never overlaps real rows, but avoids boundary overflow
        // when a reader marks the last partial granule.
        const size_t num_granules = (total_rows + ConditionCacheContext::GRANULE_SIZE - 1) /
                                    ConditionCacheContext::GRANULE_SIZE;
        _condition_cache = std::make_shared<std::vector<bool>>(num_granules + 1, false);
    }

    if (_condition_cache != nullptr) {
        _condition_cache_ctx = std::make_shared<ConditionCacheContext>();
        _condition_cache_ctx->is_hit = condition_cache_hit;
        _condition_cache_ctx->filter_result = _condition_cache;
        _condition_cache_ctx->num_granules = _condition_cache->size();
        if (condition_cache_hit) {
            _condition_cache_ctx->base_granule = handle.get_base_granule();
        }
        _data_reader.reader->set_condition_cache_context(_condition_cache_ctx);
    }
    return Status::OK();
}

void TableReader::_finalize_reader_condition_cache() {
    if (_condition_cache_split_participating) {
        DORIS_CHECK(_condition_cache_split_context != nullptr);
        const bool cache_hit = _condition_cache_ctx != nullptr && _condition_cache_ctx->is_hit;
        const bool complete_miss = _condition_cache_initialized &&
                                   !_condition_cache_split_invalid && _current_reader_reached_eof;
        std::shared_ptr<std::vector<bool>> published_filter;
        int64_t published_base_granule = 0;
        {
            std::lock_guard lock(_condition_cache_split_context->lock);
            auto& split_context = *_condition_cache_split_context;
            if (!_condition_cache_initialized) {
                split_context.valid = false;
            } else {
                const auto encoded_key = _condition_cache_key.encode();
                if (!split_context.encoded_key.has_value()) {
                    split_context.encoded_key = encoded_key;
                } else if (*split_context.encoded_key != encoded_key) {
                    // Children can observe different late-RF snapshots. Never combine their
                    // partial bitmaps under either digest's source-level cache key.
                    split_context.valid = false;
                }
            }
            if (cache_hit) {
                split_context.cache_hit_seen = true;
            } else if (!complete_miss) {
                split_context.valid = false;
            } else if (_condition_cache_ctx != nullptr && _condition_cache != nullptr) {
                DORIS_CHECK(_condition_cache_ctx->num_granules <= _condition_cache->size());
                const int64_t local_base = _condition_cache_ctx->base_granule;
                const size_t local_size = _condition_cache_ctx->num_granules;
                if (split_context.merged_filter_result.empty()) {
                    split_context.base_granule = local_base;
                    split_context.merged_filter_result.assign(local_size, false);
                } else {
                    const int64_t merged_end =
                            split_context.base_granule +
                            cast_set<int64_t>(split_context.merged_filter_result.size());
                    const int64_t local_end = local_base + cast_set<int64_t>(local_size);
                    const int64_t combined_base = std::min(split_context.base_granule, local_base);
                    const int64_t combined_end = std::max(merged_end, local_end);
                    if (combined_base != split_context.base_granule || combined_end != merged_end) {
                        std::vector<bool> combined(cast_set<size_t>(combined_end - combined_base),
                                                   false);
                        for (size_t index = 0; index < split_context.merged_filter_result.size();
                             ++index) {
                            combined[cast_set<size_t>(split_context.base_granule - combined_base) +
                                     index] = split_context.merged_filter_result[index];
                        }
                        split_context.base_granule = combined_base;
                        split_context.merged_filter_result = std::move(combined);
                    }
                }
                for (size_t index = 0; index < local_size; ++index) {
                    const auto merged_index =
                            cast_set<size_t>(local_base - split_context.base_granule) + index;
                    split_context.merged_filter_result[merged_index] =
                            split_context.merged_filter_result[merged_index] ||
                            (*_condition_cache)[index];
                }
            }

            DORIS_CHECK(split_context.remaining_children > 0);
            --split_context.remaining_children;
            if (split_context.remaining_children == 0 && split_context.valid &&
                !split_context.cache_hit_seen && !split_context.merged_filter_result.empty()) {
                // A source-level entry is visible only after every physical child reaches EOF;
                // publishing an earlier partial MISS would let a sibling HIT skip valid rows.
                published_base_granule = split_context.base_granule;
                published_filter = std::make_shared<std::vector<bool>>(
                        std::move(split_context.merged_filter_result));
            }
        }
        if (published_filter != nullptr) {
            if (auto* cache = segment_v2::ConditionCache::instance(); cache != nullptr) {
                cache->insert(_condition_cache_key, std::move(published_filter),
                              published_base_granule);
            }
        }
        _condition_cache = nullptr;
        _condition_cache_ctx = nullptr;
        _condition_cache_split_context.reset();
        _condition_cache_split_participating = false;
        _condition_cache_initialized = false;
        return;
    }
    if (_condition_cache_ctx == nullptr || _condition_cache_ctx->is_hit) {
        _condition_cache = nullptr;
        _condition_cache_ctx = nullptr;
        return;
    }
    // LIMIT or scanner cancellation may close a reader before all selected row ranges are visited.
    // Unvisited granules remain false in a MISS bitmap, so inserting a partial bitmap would make a
    // later HIT skip valid rows. Only publish cache entries after the physical reader reaches EOF.
    if (!_current_reader_reached_eof) {
        _condition_cache = nullptr;
        _condition_cache_ctx = nullptr;
        return;
    }
    DORIS_CHECK(_condition_cache_ctx->num_granules <= _condition_cache->size());
    _condition_cache->resize(_condition_cache_ctx->num_granules);
    segment_v2::ConditionCache::instance()->insert(
            _condition_cache_key, std::move(_condition_cache), _condition_cache_ctx->base_granule);
    _condition_cache = nullptr;
    _condition_cache_ctx = nullptr;
}

Status TableReader::create_next_reader(bool* eos) {
    SCOPED_TIMER(_profile.create_reader_timer);
    DCHECK(_data_reader.reader == nullptr);
    if (_current_task == nullptr) {
        *eos = true;
        return Status::OK();
    }

    RETURN_IF_ERROR(create_file_reader(&_data_reader.reader));
    DORIS_CHECK(_data_reader.reader != nullptr);
    if (_batch_size > 0) {
        _data_reader.reader->set_batch_size(_batch_size);
    }
    Status st;
    {
        SCOPED_TIMER(_profile.file_reader_total_timer);
        SCOPED_TIMER(_profile.file_reader_init_timer);
        st = _data_reader.reader->init(_runtime_state);
    }
    if (!st.ok()) {
        if (_io_ctx != nullptr && _io_ctx->should_stop && st.is<ErrorCode::END_OF_FILE>()) {
            *eos = true;
            _data_reader.reader.reset();
            return Status::OK();
        }
        return st;
    }
    st = open_reader();
    if (!st.ok()) {
        if (_io_ctx != nullptr && _io_ctx->should_stop && st.is<ErrorCode::END_OF_FILE>()) {
            *eos = true;
            _data_reader.reader.reset();
            return Status::OK();
        }
        return st;
    }
    if (_data_reader.reader == nullptr) {
        *eos = _current_task == nullptr;
        return Status::OK();
    }
    *eos = false;
    return Status::OK();
}

Status TableReader::create_file_reader(std::unique_ptr<FileReader>* reader) {
    DORIS_CHECK(reader != nullptr);
    const bool enable_mapping_timestamp_tz = _scan_params != nullptr &&
                                             _scan_params->__isset.enable_mapping_timestamp_tz &&
                                             _scan_params->enable_mapping_timestamp_tz;
    const bool enable_mapping_varbinary = _scan_params != nullptr &&
                                          _scan_params->__isset.enable_mapping_varbinary &&
                                          _scan_params->enable_mapping_varbinary;
    if (_format == FileFormat::PARQUET) {
        // V2 must honor the scan contract directly; otherwise Hive STRING columns backed by an
        // unannotated BYTE_ARRAY are silently exposed as VARBINARY and predicate bytes no longer
        // match the table type.
        *reader = std::make_unique<format::parquet::ParquetReader>(
                _system_properties, _current_task->data_file, _io_ctx, _scanner_profile,
                _global_rowid_context, enable_mapping_timestamp_tz, enable_mapping_varbinary,
                _current_task->file_context, _current_task->format_split_id,
                _current_task->format_split_id_end);
        return Status::OK();
    }
    if (_format == FileFormat::ORC) {
        *reader = std::make_unique<format::orc::OrcReader>(
                _system_properties, _current_task->data_file, _io_ctx, _scanner_profile,
                _global_rowid_context, enable_mapping_timestamp_tz, _current_task->file_context);
        return Status::OK();
    }
    if (_format == FileFormat::CSV) {
        if (_file_slot_descs == nullptr) {
            return Status::InvalidArgument("CSV reader requires file slot descriptors");
        }
        // CSV has no embedded schema. TableReader owns table-level mapping, while CsvReader needs
        // only the physical file slots plus scan text parameters to build a file-local schema.
        // Non-file columns such as partitions/defaults/virtual row ids are intentionally excluded
        // from `_file_slot_descs` and are materialized during finalize_chunk().
        *reader = std::make_unique<format::csv::CsvReader>(
                _system_properties, _current_task->data_file, _io_ctx, _scanner_profile,
                _scan_params, *_file_slot_descs, _current_range_compress_type,
                _current_range_load_id);
        return Status::OK();
    }
    if (_format == FileFormat::TEXT) {
        if (_file_slot_descs == nullptr) {
            return Status::InvalidArgument("Text reader requires file slot descriptors");
        }
        // Text files have no embedded schema. As with CSV, TableReader handles table-level mapping
        // and only passes physical file slots to the v2 TextReader.
        *reader = std::make_unique<format::text::TextReader>(
                _system_properties, _current_task->data_file, _io_ctx, _scanner_profile,
                _scan_params, *_file_slot_descs, _current_range_compress_type,
                _current_range_load_id);
        return Status::OK();
    }
    if (_format == FileFormat::JSON) {
        if (_file_slot_descs == nullptr) {
            return Status::InvalidArgument("JSON reader requires file slot descriptors");
        }
        *reader = std::make_unique<format::json::JsonReader>(
                _system_properties, _current_task->data_file, _io_ctx, _scanner_profile,
                _scan_params, _current_file_range_desc, *_file_slot_descs,
                _current_range_compress_type, _current_range_load_id);
        return Status::OK();
    }
    if (_format == FileFormat::NATIVE) {
        *reader = std::make_unique<format::native::NativeReader>(
                _system_properties, _current_task->data_file, _io_ctx, _scanner_profile);
        return Status::OK();
    }
    return Status::NotSupported("TableReader does not support file format {}",
                                file_format_to_string(_format));
}

std::unique_ptr<io::FileDescription> create_file_description(const TFileRangeDesc& range) {
    auto file_description = std::make_unique<io::FileDescription>();
    file_description->path = range.path;
    file_description->file_size = range.__isset.file_size ? range.file_size : -1;
    file_description->mtime = range.__isset.modification_time ? range.modification_time : 0;
    file_description->range_start_offset = range.__isset.start_offset ? range.start_offset : 0;
    file_description->range_size = range.__isset.size ? range.size : -1;
    if (range.__isset.fs_name) {
        file_description->fs_name = range.fs_name;
    }
    if (range.__isset.file_cache_admission) {
        file_description->file_cache_admission = range.file_cache_admission;
    }
    return file_description;
}

Status TableReader::prepare_split(const SplitReadOptions& options) {
    SCOPED_TIMER(_profile.total_timer);
    SCOPED_TIMER(_profile.prepare_split_timer);
    _current_split_pruned = false;
    _all_runtime_filters_applied_for_split = options.all_runtime_filters_applied;
    _condition_cache_source_range = options.condition_cache_source_range;
    _condition_cache_split_context = options.condition_cache_split_context;
    _condition_cache_split_participating = _condition_cache_split_context != nullptr;
    _condition_cache_split_invalid = false;
    _condition_cache_initialized = false;
    _condition_cache_digest_covers_current_split = options.condition_cache_digest.has_value();
    if (options.condition_cache_digest.has_value()) {
        // The split snapshot may include RFs that arrived after TableReader::init(). Use the digest
        // computed from that exact snapshot. Example: an initial P digest must not be used to store
        // the bitmap for P AND late RF{7, 9}; the scanner supplies digest(P AND RF{7, 9}) here.
        _condition_cache_digest = *options.condition_cache_digest;
    } else {
        // An explicit scanner digest is split-scoped. Restore the init-time digest when a later
        // standalone split omits it instead of leaking the previous split's RF payload into its key.
        _condition_cache_digest = _initial_condition_cache_digest;
    }
    if (options.conjuncts.has_value()) {
        _conjuncts = *options.conjuncts;
        _predicate_snapshot_digest = build_predicate_snapshot_digest(_conjuncts);
    }
    // Update to current split format to handle ORC/PARQUET files in one table.
    _format = options.current_split_format;
    _partition_values = std::move(options.partition_values);
    _current_task.reset();
    _current_file_description.reset();
    _current_file_range_desc = options.current_range;
    _current_range_compress_type = options.current_range.__isset.compress_type
                                           ? options.current_range.compress_type
                                           : TFileCompressType::UNKNOWN;
    _current_range_load_id = options.current_range.__isset.load_id
                                     ? std::make_optional(options.current_range.load_id)
                                     : std::nullopt;
    _global_rowid_context = options.global_rowid_context;
    _delete_rows = nullptr;
    _deletion_vector = nullptr;
    _aggregate_pushdown_tried = false;
    _metadata_aggregate_result.reset();
    _remaining_table_level_count = -1;
    _remaining_file_level_count = -1;
    _current_split_uses_metadata_count = false;
    _current_reader_reached_eof = false;
    RETURN_IF_ERROR(_evaluate_partition_prune_conjuncts(options.partition_prune_conjuncts,
                                                        &_current_split_pruned));
    if (_current_split_pruned) {
        COUNTER_UPDATE(_profile.runtime_filter_partition_pruned_range_counter, 1);
        return Status::OK();
    }
    _current_task = std::make_unique<ScanTask>();
    _current_task->data_file = create_file_description(options.current_range);
    _current_task->file_context = options.file_context;
    _current_task->format_split_id = options.format_split_id;
    _current_task->format_split_id_end = options.format_split_id_end;
    _current_file_description = *_current_task->data_file;
    // A table-level row count is only equivalent to scanning the split when no row predicate is
    // active and no predicate can arrive later. The metadata path can return several batches for
    // one split; after its first synthetic batch there is no way to recover the real rows if a
    // runtime filter arrives before the next scheduler turn.
    // Table-level metadata only contains the number of rows; it cannot evaluate an expression or
    // the NULL state of a COUNT argument. Require the new FE's explicit empty argument list, which
    // means COUNT(*)/COUNT(1). A non-empty list means COUNT(col), while nullopt comes from an old FE
    // whose COUNT semantics are unknown during a BE-first rolling upgrade.
    if (_push_down_agg_type == TPushAggOp::type::COUNT && _push_down_count_columns.has_value() &&
        _push_down_count_columns->empty() && options.all_runtime_filters_applied &&
        _conjuncts.empty() && options.current_range.__isset.table_format_params &&
        options.current_range.table_format_params.__isset.table_level_row_count) {
        DORIS_CHECK(options.current_range.table_format_params.table_level_row_count >= -1);
        _remaining_table_level_count =
                options.current_range.table_format_params.table_level_row_count;
        _current_split_uses_metadata_count = _is_table_level_count_active();
    }
    if (_is_table_level_count_active()) {
        return Status::OK();
    }
    return _parse_delete_predicates(options);
}

Status TableReader::build_physical_splits(const FileScanSplit& source_split,
                                          std::vector<FileScanSplit>* splits, bool* was_split) {
    SCOPED_TIMER(_profile.total_timer);
    DORIS_CHECK(splits != nullptr);
    DORIS_CHECK(was_split != nullptr);
    splits->clear();
    *was_split = false;
    if ((_format != FileFormat::PARQUET && _format != FileFormat::ORC) || _current_split_pruned ||
        _current_split_uses_metadata_count || _current_task == nullptr) {
        return Status::OK();
    }
    SCOPED_TIMER(_profile.create_reader_timer);

    std::unique_ptr<FileReader> reader;
    RETURN_IF_ERROR(create_file_reader(&reader));
    DORIS_CHECK(reader != nullptr);
    auto close_planning_reader = [&]() {
        SCOPED_TIMER(_profile.file_reader_total_timer);
        SCOPED_TIMER(_profile.file_reader_close_timer);
        return reader->close();
    };
    Status init_status;
    {
        SCOPED_TIMER(_profile.file_reader_total_timer);
        SCOPED_TIMER(_profile.file_reader_init_timer);
        init_status = reader->init(_runtime_state);
    }
    if (!init_status.ok()) {
        // A failed init may still own partially opened resources. Close it through the same
        // lifecycle path while preserving the initialization error returned to the scanner.
        static_cast<void>(close_planning_reader());
        return init_status;
    }

    _data_reader.reader = std::move(reader);
    if (_batch_size > 0) {
        _data_reader.reader->set_batch_size(_batch_size);
    }
    const auto open_status = open_reader();
    if (!open_status.ok()) {
        if (_data_reader.reader != nullptr) {
            static_cast<void>(close_current_reader());
        }
        return open_status;
    }
    if (_data_reader.reader == nullptr) {
        // Constant pruning can close the eagerly opened planning reader. Publish an empty refined
        // source so scanners do not recreate and reopen the same already-rejected file.
        *was_split = true;
        return Status::OK();
    }

    if (_supports_aggregate_pushdown(_push_down_agg_type)) {
        FileAggregateRequest aggregate_request;
        const auto request_status =
                _build_file_aggregate_request(_push_down_agg_type, &aggregate_request);
        if (!request_status.ok()) {
            static_cast<void>(close_current_reader());
            return request_status;
        }
        FileAggregateResult aggregate_result;
        Status aggregate_status;
        {
            SCOPED_TIMER(_profile.file_reader_total_timer);
            SCOPED_TIMER(_profile.file_reader_aggregate_timer);
            aggregate_status = _data_reader.reader->get_metadata_aggregate_result(
                    aggregate_request, &aggregate_result);
        }
        if (aggregate_status.ok()) {
            // The planning reader already owns the exact pruned physical-granule set. Retaining it
            // avoids replacing one metadata-only aggregate with N children that repeat the
            // reduction.
            _metadata_aggregate_result = std::move(aggregate_result);
            return Status::OK();
        }
        if (!aggregate_status.is<ErrorCode::NOT_IMPLEMENTED_ERROR>()) {
            static_cast<void>(close_current_reader());
            return aggregate_status;
        }
    }

    std::vector<PhysicalFileSplit> physical_splits;
    Status status;
    {
        SCOPED_TIMER(_profile.file_reader_total_timer);
        status = _data_reader.reader->build_physical_splits(&physical_splits, was_split);
    }
    if (!status.ok()) {
        static_cast<void>(close_current_reader());
        return status;
    }
    const auto& source_range = source_split.source_identity_range();
    if (source_range.__isset.target_split_size && source_range.target_split_size > 0) {
        physical_splits = coalesce_physical_splits(std::move(physical_splits),
                                                   source_range.target_split_size);
    }
    if (!*was_split || physical_splits.size() == 1) {
        // Reuse the fully planned reader when refinement is unnecessary. Besides avoiding another
        // footer parse, this preserves the request snapshot whose metadata pruning selected the
        // single surviving physical granule.
        splits->clear();
        *was_split = false;
        return Status::OK();
    }
    // FileReader descriptors deliberately carry no scanner/table policy. Attach the FE source
    // identity and shared child coordination here so format readers cannot accidentally own range
    // progress, table metadata semantics, or Condition Cache publication.
    const auto shared_source_range = std::make_shared<TFileRangeDesc>(source_split.range);
    std::shared_ptr<ConditionCacheSplitContext> condition_cache_split_context;
    if (physical_splits.size() > 1) {
        condition_cache_split_context =
                std::make_shared<ConditionCacheSplitContext>(physical_splits.size());
    }
    splits->reserve(physical_splits.size());
    for (auto& physical_split : physical_splits) {
        FileScanSplit child;
        child.source_range = shared_source_range;
        child.start_offset = physical_split.start_offset;
        child.size = physical_split.size;
        // A source-level count is not valid for one generated physical child. Child readers can
        // still derive an exact count from shared format metadata when pushdown is eligible.
        child.clear_table_level_row_count = true;
        child.file_context = std::move(physical_split.file_context);
        child.condition_cache_split_context = condition_cache_split_context;
        child.format_split_id = physical_split.format_split_id;
        child.format_split_id_end = physical_split.format_split_id_end;
        splits->push_back(std::move(child));
    }
    return close_current_reader();
}

Status TableReader::_evaluate_partition_prune_conjuncts(const VExprContextSPtrs& conjuncts,
                                                        bool* can_filter_all) {
    DORIS_CHECK(can_filter_all != nullptr);
    SCOPED_TIMER(_profile.runtime_filter_partition_prune_timer);
    *can_filter_all = false;
    if (conjuncts.empty() || _partition_values.empty()) {
        return Status::OK();
    }

    VExprContextSPtrs partition_conjuncts;
    for (const auto& conjunct : conjuncts) {
        DORIS_CHECK(conjunct != nullptr);
        DORIS_CHECK(conjunct->root() != nullptr);
        // Keep only the safe prefix of the original conjunct order. If an unsafe conjunct is
        // skipped, a later predicate could prune the split before the unsafe one reaches its
        // normal row-level evaluation point.
        if (!_is_safe_to_pre_execute(conjunct)) {
            break;
        }
        std::set<GlobalIndex> global_indices;
        collect_global_indices(conjunct->root(), &global_indices);
        if (global_indices.empty()) {
            continue;
        }
        const bool partition_only = std::ranges::all_of(global_indices, [&](GlobalIndex index) {
            if (index.value() >= _projected_columns.size()) {
                return false;
            }
            const auto& column = _projected_columns[index.value()];
            // Identity-partition metadata is a split constant even when the same source column
            // must remain file-backed for data written under another evolved partition spec.
            return find_partition_value(column, _partition_values) != nullptr;
        });
        if (partition_only) {
            partition_conjuncts.push_back(conjunct);
        }
    }
    if (partition_conjuncts.empty()) {
        return Status::OK();
    }

    Block block;
    RETURN_IF_ERROR(_build_partition_prune_block(&block));
    RowDescriptor row_desc;
    for (const auto& conjunct : partition_conjuncts) {
        RETURN_IF_ERROR(conjunct->prepare(_runtime_state, row_desc));
        RETURN_IF_ERROR(conjunct->open(_runtime_state));
    }
    IColumn::Filter result_filter(block.rows(), 1);
    return VExprContext::execute_conjuncts(partition_conjuncts, nullptr, &block, &result_filter,
                                           can_filter_all);
}

bool TableReader::_is_safe_to_pre_execute(const VExprContextSPtr& conjunct) {
    DORIS_CHECK(conjunct != nullptr);
    DORIS_CHECK(conjunct->root() != nullptr);
    const auto root = conjunct->root();
    const auto impl = root->get_impl();
    const auto predicate = impl != nullptr ? impl : root;
    // Split pruning evaluates a predicate once before any file rows are read. Reordering
    // non-deterministic or error-preserving expressions can change their row-level semantics,
    // even when every referenced slot is a partition column or maps to a constant entry.
    return predicate->is_safe_to_execute_on_selected_rows();
}

Status TableReader::_build_partition_prune_block(Block* block) const {
    DORIS_CHECK(block != nullptr);
    DORIS_CHECK(!_projected_columns.empty());
    block->clear();
    for (const auto& column : _projected_columns) {
        DORIS_CHECK(column.type != nullptr);
        ColumnPtr value_column = column.type->create_column_const_with_default_value(1);
        const auto* partition_value = find_partition_value(column, _partition_values);
        if (partition_value != nullptr) {
            value_column = column.type->create_column_const(1, *partition_value);
        }
        block->insert({std::move(value_column), column.type, column.name});
    }
    return Status::OK();
}

Status TableReader::_parse_delete_predicates(const SplitReadOptions& options) {
    DeleteFileDesc desc {.fs_name = options.current_range.fs_name};
    bool has_delete_file = false;
    RETURN_IF_ERROR(_parse_deletion_vector_file(options.current_range.table_format_params, &desc,
                                                &has_delete_file));
    if (has_delete_file) {
        DORIS_CHECK(options.cache != nullptr);
        Status create_status = Status::OK();

        bool decoded_cache_hit = false;
        _deletion_vector = options.cache->get<DeletionVector>(
                desc.key,
                [&]() -> DeletionVector* {
                    auto deletion_vector = std::make_unique<DeletionVector>();

                    DeletionVectorReader dv_reader(_runtime_state, _scanner_profile, *_scan_params,
                                                   desc, _io_ctx.get());
                    create_status = dv_reader.open();
                    if (!create_status.ok()) [[unlikely]] {
                        return nullptr;
                    }

                    size_t bytes_read = desc.size;
                    std::vector<char> buffer(bytes_read);
                    DBUG_EXECUTE_IF("TableReader.parse_deletion_vector.io_error", {
                        create_status =
                                Status::IOError("injected format v2 deletion vector read failure");
                        return nullptr;
                    });
                    DBUG_EXECUTE_IF("TableReader.parse_deletion_vector.should_stop", {
                        create_status = Status::EndOfFile("stop read.");
                        return nullptr;
                    });
                    create_status =
                            dv_reader.read_at(desc.start_offset, {buffer.data(), bytes_read});
                    const auto& file_cache_stats = dv_reader.file_cache_statistics();
                    COUNTER_UPDATE(_profile.dv_file_cache_hit_count,
                                   file_cache_stats.num_local_io_total);
                    COUNTER_UPDATE(_profile.dv_file_cache_miss_count,
                                   file_cache_stats.num_remote_io_total);
                    COUNTER_UPDATE(_profile.dv_file_cache_peer_read_count,
                                   file_cache_stats.num_peer_io_total);
                    if (!create_status.ok()) [[unlikely]] {
                        return nullptr;
                    }

                    const char* buf = buffer.data();
                    SCOPED_TIMER(_profile.parse_delete_file_time);
                    create_status = parse_deletion_vector(buf, bytes_read, desc.format,
                                                          deletion_vector.get());
                    if (!create_status.ok()) [[unlikely]] {
                        return nullptr;
                    }
                    COUNTER_UPDATE(_profile.num_delete_rows, deletion_vector->cardinality());
                    return deletion_vector.release();
                },
                &decoded_cache_hit);
        RETURN_IF_ERROR(create_status);
        COUNTER_UPDATE(decoded_cache_hit ? _profile.decoded_dv_cache_hit_count
                                         : _profile.decoded_dv_cache_miss_count,
                       1);
    }

    return Status::OK();
}
} // namespace doris::format
