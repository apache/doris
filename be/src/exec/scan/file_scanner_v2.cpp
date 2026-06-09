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

#include "exec/scan/file_scanner_v2.h"

#include <fmt/format.h>
#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/ExternalTableSchema_types.h>
#include <gen_cpp/PlanNodes_types.h>

#include <algorithm>
#include <charconv>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include "common/cast_set.h"
#include "common/config.h"
#include "common/consts.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type_serde/data_type_serde.h"
#include "core/string_ref.h"
#include "exec/common/util.hpp"
#include "exec/operator/scan_operator.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "exprs/vslot_ref.h"
#include "format/format_common.h"
#include "format_v2/column_mapper.h"
#include "format_v2/expr/slot_ref.h"
#include "format_v2/table/hive_reader.h"
#include "format_v2/table/iceberg_reader.h"
#include "format_v2/table/paimon_reader.h"
#include "format_v2/table_reader.h"
#include "io/fs/file_meta_cache.h"
#include "io/io_common.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "service/backend_options.h"
#include "storage/id_manager.h"

namespace doris {
namespace {

std::string table_format_name(const TFileRangeDesc& range) {
    return range.__isset.table_format_params ? range.table_format_params.table_format_type
                                             : "NotSet";
}

TFileFormatType::type get_range_format_type(const TFileScanRangeParams& params,
                                            const TFileRangeDesc& range) {
    return range.__isset.format_type ? range.format_type : params.format_type;
}

bool is_supported_table_format(const TFileRangeDesc& range) {
    const auto table_format = table_format_name(range);
    return table_format == "NotSet" || table_format == "tvf" || table_format == "hive" ||
           table_format == "iceberg" || table_format == "paimon";
}

bool is_partition_slot(const TFileScanSlotInfo& slot_info, const std::string& column_name) {
    if (column_name.starts_with(BeConsts::GLOBAL_ROWID_COL)) {
        return false;
    }
    return slot_info.__isset.category ? slot_info.category == TColumnCategory::PARTITION_KEY
                                      : !slot_info.is_file_slot;
}

bool parse_non_negative_int(std::string_view value, int32_t* result) {
    DORIS_CHECK(result != nullptr);
    int32_t parsed = -1;
    const auto* begin = value.data();
    const auto* end = begin + value.size();
    const auto [ptr, ec] = std::from_chars(begin, end, parsed);
    if (ec != std::errc() || ptr != end || parsed < 0) {
        return false;
    }
    *result = parsed;
    return true;
}

std::string access_path_to_string(const std::vector<std::string>& path) {
    return fmt::format("{}", fmt::join(path, "."));
}

format::ColumnDefinition* find_or_add_child(format::ColumnDefinition* parent, int32_t id,
                                            std::string name, DataTypePtr type) {
    DORIS_CHECK(parent != nullptr);
    for (auto& child : parent->children) {
        if ((child.has_identifier_field_id() && child.get_identifier_field_id() == id) ||
            child.name == name) {
            return &child;
        }
    }
    parent->children.push_back({
            .identifier = Field::create_field<TYPE_INT>(id),
            .name = std::move(name),
            .type = std::move(type),
            .children = {},
            .default_expr = nullptr,
            .is_partition_key = false,
    });
    return &parent->children.back();
}

const schema::external::TField* get_field_ptr(const schema::external::TFieldPtr& field_ptr) {
    if (!field_ptr.__isset.field_ptr || field_ptr.field_ptr == nullptr) {
        return nullptr;
    }
    return field_ptr.field_ptr.get();
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

DataTypePtr find_struct_child_type_by_name(const DataTypeStruct& struct_type,
                                           const std::string& field_name) {
    for (size_t field_idx = 0; field_idx < struct_type.get_elements().size(); ++field_idx) {
        if (to_lower(struct_type.get_element_name(field_idx)) == to_lower(field_name)) {
            return struct_type.get_element(field_idx);
        }
    }
    return nullptr;
}

format::ColumnDefinition build_schema_column_from_external_field(
        const schema::external::TField& field, DataTypePtr type) {
    format::ColumnDefinition column {
            .identifier = field.__isset.id ? Field::create_field<TYPE_INT>(field.id) : Field {},
            .name = field.__isset.name ? field.name : "",
            .name_mapping =
                    field.__isset.name_mapping ? field.name_mapping : std::vector<std::string> {},
            .type = std::move(type),
            .children = {},
            .default_expr = nullptr,
            .is_partition_key = false,
    };
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
            auto child_type = find_struct_child_type_by_name(struct_type, child_field->name);
            if (child_type == nullptr) {
                continue;
            }
            column.children.push_back(
                    build_schema_column_from_external_field(*child_field, child_type));
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
        column.children.push_back(
                build_schema_column_from_external_field(*item_field, array_type.get_nested_type()));
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
            column.children.push_back(
                    build_schema_column_from_external_field(*key_field, map_type.get_key_type()));
        }
        const auto* value_field = get_field_ptr(field.nestedField.map_field.value_field);
        if (value_field != nullptr) {
            column.children.push_back(build_schema_column_from_external_field(
                    *value_field, map_type.get_value_type()));
        }
        break;
    }
    default:
        break;
    }
    return column;
}

const format::ColumnDefinition* find_schema_child_by_path(
        const format::ColumnDefinition* schema_column, const std::string& child_path) {
    if (schema_column == nullptr) {
        return nullptr;
    }
    int32_t parsed_field_id = -1;
    if (parse_non_negative_int(child_path, &parsed_field_id)) {
        const auto child_it = std::ranges::find_if(
                schema_column->children, [&](const format::ColumnDefinition& child) {
                    return child.has_identifier_field_id() &&
                           child.get_identifier_field_id() == parsed_field_id;
                });
        return child_it == schema_column->children.end() ? nullptr : &*child_it;
    }
    const auto child_it = std::ranges::find_if(schema_column->children, [&](const auto& child) {
        if (to_lower(child.name) == to_lower(child_path)) {
            return true;
        }
        return std::ranges::any_of(child.name_mapping, [&](const std::string& alias) {
            return to_lower(alias) == to_lower(child_path);
        });
    });
    return child_it == schema_column->children.end() ? nullptr : &*child_it;
}

int32_t schema_field_id(const format::ColumnDefinition* schema_column) {
    if (schema_column == nullptr || !schema_column->has_identifier_field_id()) {
        return -1;
    }
    return schema_column->get_identifier_field_id();
}

const schema::external::TField* find_external_root_field(const TFileScanRangeParams* params,
                                                         const format::ColumnDefinition& column) {
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
    for (const auto& field_ptr : schema->root_field.fields) {
        const auto* field = get_field_ptr(field_ptr);
        if (field == nullptr) {
            continue;
        }
        if (external_field_matches_name(*field, column.name)) {
            return field;
        }
    }
    return nullptr;
}

struct AccessPathNode {
    bool project_all = false;
    std::map<std::string, AccessPathNode> children;
};

void merge_access_path_node(AccessPathNode* dst, const AccessPathNode& src) {
    DORIS_CHECK(dst != nullptr);
    if (dst->project_all) {
        return;
    }
    if (src.project_all) {
        dst->project_all = true;
        dst->children.clear();
        return;
    }
    for (const auto& [path, child] : src.children) {
        merge_access_path_node(&dst->children[path], child);
    }
}

void insert_access_path(AccessPathNode* root, const std::vector<std::string>& path,
                        size_t path_idx) {
    DORIS_CHECK(root != nullptr);
    if (root->project_all) {
        return;
    }
    if (path_idx >= path.size()) {
        root->project_all = true;
        root->children.clear();
        return;
    }
    insert_access_path(&root->children[path[path_idx]], path, path_idx + 1);
}

Status build_nested_children_from_access_node(format::ColumnDefinition* column,
                                              const DataTypePtr& type, const AccessPathNode& node,
                                              const std::string& path,
                                              const format::ColumnDefinition* schema_column);

Status build_struct_children_from_access_node(format::ColumnDefinition* column,
                                              const DataTypeStruct& struct_type,
                                              const AccessPathNode& node, const std::string& path,
                                              const format::ColumnDefinition* schema_column) {
    DORIS_CHECK(column != nullptr);
    for (const auto& [child_path, child_node] : node.children) {
        // Currently we do not support accessing struct children by position (e.g. "col.0") because it can be ambiguous and error-prone when the struct schema evolves. We only support accessing struct children by name (e.g. "col.child"). If needed, we can consider adding support for position-based access in the future with careful design and consideration.
        if (child_path == "OFFSET" || child_path == "*" || child_path == "KEYS" ||
            child_path == "VALUES") {
            return Status::NotSupported("FileScannerV2 does not support access path {} for slot {}",
                                        path + "." + child_path, column->name);
        }

        // Try to find the child field in the schema column first. If not found, fallback to find the child field in the struct type by name (case-insensitive).
        const auto* schema_child = find_schema_child_by_path(schema_column, child_path);
        int32_t field_id = schema_field_id(schema_child);
        std::string field_name = schema_child == nullptr ? child_path : schema_child->name;
        DataTypePtr field_type = schema_child == nullptr ? nullptr : schema_child->type;
        if (field_id < 0 || field_type == nullptr) {
            for (size_t field_idx = 0; field_idx < struct_type.get_elements().size(); ++field_idx) {
                if (to_lower(struct_type.get_element_name(field_idx)) == to_lower(field_name)) {
                    field_id = cast_set<int32_t>(field_idx);
                    field_name = struct_type.get_element_name(field_idx);
                    field_type = struct_type.get_element(field_idx);
                    break;
                }
            }
        }

        if (field_id < 0 || field_type == nullptr) {
            return Status::NotSupported("FileScannerV2 does not support access path {} for slot {}",
                                        path + "." + child_path, column->name);
        }
        // TODO: For TVF Parquet files without field ids, this fallback uses the struct ordinal as
        // the table child identifier. BY_NAME mapping should instead keep a string identifier and
        // let TableColumnMapper resolve the file-local child id from the Parquet schema.
        auto* child = find_or_add_child(column, field_id, field_name, field_type);
        RETURN_IF_ERROR(build_nested_children_from_access_node(
                child, child->type, child_node, path + "." + child_path, schema_child));
    }
    return Status::OK();
}

Status build_map_children_from_access_node(format::ColumnDefinition* column,
                                           const DataTypeMap& map_type, const AccessPathNode& node,
                                           const std::string& path,
                                           const format::ColumnDefinition* schema_column) {
    DORIS_CHECK(column != nullptr);
    AccessPathNode key_node;
    AccessPathNode value_node;
    bool need_key = false;
    bool need_value = false;

    for (const auto& [child_path, child_node] : node.children) {
        if (child_path == "OFFSET") {
            return Status::NotSupported("FileScannerV2 does not support access path {} for slot {}",
                                        path + "." + child_path, column->name);
        }
        if (child_path == "KEYS") {
            need_key = true;
            merge_access_path_node(&key_node, child_node);
            continue;
        }
        if (child_path == "VALUES") {
            need_key = true;
            key_node.project_all = true;
            key_node.children.clear();
            need_value = true;
            merge_access_path_node(&value_node, child_node);
            continue;
        }
        if (child_path == "*") {
            need_key = true;
            key_node.project_all = true;
            key_node.children.clear();
            need_value = true;
            merge_access_path_node(&value_node, child_node);
            continue;
        }
        return Status::NotSupported("FileScannerV2 does not support access path {} for slot {}",
                                    path + "." + child_path, column->name);
    }
    if (need_key && !need_value) {
        // Keep value readable until the downstream map materialization path can construct a table
        // Map column from keys only.
        need_value = true;
        value_node.project_all = true;
        value_node.children.clear();
    }

    DataTypes entry_child_types;
    Strings entry_child_names;
    if (need_key) {
        entry_child_types.push_back(map_type.get_key_type());
        entry_child_names.push_back("key");
    }
    if (need_value) {
        entry_child_types.push_back(map_type.get_value_type());
        entry_child_names.push_back("value");
    }
    if (entry_child_types.empty()) {
        return Status::OK();
    }

    auto entry_type = std::make_shared<DataTypeStruct>(entry_child_types, entry_child_names);
    auto* entry_child = find_or_add_child(column, 0, "entries", entry_type);
    const auto* key_schema = schema_column != nullptr && !schema_column->children.empty()
                                     ? &schema_column->children[0]
                                     : nullptr;
    const auto* value_schema = schema_column != nullptr && schema_column->children.size() > 1
                                       ? &schema_column->children[1]
                                       : nullptr;
    if (need_key) {
        auto* key_child = find_or_add_child(entry_child, 0, "key", map_type.get_key_type());
        RETURN_IF_ERROR(build_nested_children_from_access_node(key_child, key_child->type, key_node,
                                                               path + ".KEYS", key_schema));
    }
    if (need_value) {
        auto* value_child = find_or_add_child(entry_child, 1, "value", map_type.get_value_type());
        RETURN_IF_ERROR(build_nested_children_from_access_node(
                value_child, value_child->type, value_node, path + ".VALUES", value_schema));
    }
    return Status::OK();
}

Status build_nested_children_from_access_node(format::ColumnDefinition* column,
                                              const DataTypePtr& type, const AccessPathNode& node,
                                              const std::string& path,
                                              const format::ColumnDefinition* schema_column) {
    DORIS_CHECK(column != nullptr);
    if (node.project_all || node.children.empty()) {
        // If project_all is true or there is no specific child path, we need to project all children of the complex type.
        return Status::OK();
    }

    const auto nested_type = remove_nullable(type);
    switch (nested_type->get_primitive_type()) {
    case TYPE_STRUCT:
        return build_struct_children_from_access_node(
                column, assert_cast<const DataTypeStruct&>(*nested_type), node, path,
                schema_column);
    case TYPE_ARRAY: {
        if (node.children.size() != 1 || !node.children.contains("*")) {
            return Status::NotSupported("FileScannerV2 does not support access path {} for slot {}",
                                        path, column->name);
        }
        const auto& array_type = assert_cast<const DataTypeArray&>(*nested_type);
        auto* child = find_or_add_child(column, 0, "element", array_type.get_nested_type());
        const auto* element_schema = schema_column != nullptr && !schema_column->children.empty()
                                             ? &schema_column->children[0]
                                             : nullptr;
        return build_nested_children_from_access_node(child, child->type, node.children.at("*"),
                                                      path + ".*", element_schema);
    }
    case TYPE_MAP:
        return build_map_children_from_access_node(
                column, assert_cast<const DataTypeMap&>(*nested_type), node, path, schema_column);
    default:
        return Status::NotSupported("FileScannerV2 does not support access path {} for slot {}",
                                    path, column->name);
    }
}

Status build_nested_children_from_access_paths(format::ColumnDefinition* column,
                                               const TColumnAccessPaths& access_paths,
                                               const format::ColumnDefinition* schema_column) {
    DORIS_CHECK(column != nullptr);
    if (!is_complex_type(remove_nullable(column->type)->get_primitive_type())) {
        return Status::OK();
    }

    AccessPathNode root;
    // Build tree for AccessPathNode.
    // For example, for access paths ["a.b", "a.c", "d"], the tree will be:
    // root
    // ├── a
    // │   ├── b
    // │   └── c
    // └── d
    for (const auto& access_path : access_paths) {
        // TODO: Support META access paths if needed. Currently FileScannerV2 only supports DATA access paths.
        if (access_path.type != TAccessPathType::DATA || !access_path.__isset.data_access_path) {
            return Status::NotSupported("FileScannerV2 only supports DATA access paths for slot {}",
                                        column->name);
        }
        const auto& path = access_path.data_access_path.path;
        if (path.empty()) {
            insert_access_path(&root, path, 0);
            continue;
        }
        int32_t top_level_id = -1;
        if (to_lower(path.front()) != to_lower(column->name) &&
            (!parse_non_negative_int(path.front(), &top_level_id) ||
             !column->has_identifier_field_id() ||
             top_level_id != column->get_identifier_field_id())) {
            return Status::NotSupported("FileScannerV2 access path {} does not match slot {}",
                                        access_path_to_string(path), column->name);
        }
        insert_access_path(&root, path, 1);
    }
    // Recursively build nested children for the column based on the AccessPathNode tree.
    return build_nested_children_from_access_node(column, column->type, root, column->name,
                                                  schema_column);
}

Status build_nested_children_from_access_paths(format::ColumnDefinition* column,
                                               const SlotDescriptor* slot_desc,
                                               const format::ColumnDefinition* schema_column) {
    DORIS_CHECK(column != nullptr);
    DORIS_CHECK(slot_desc != nullptr);
    return build_nested_children_from_access_paths(column, slot_desc->all_access_paths(),
                                                   schema_column);
}

Status rewrite_slot_refs_to_global_index(
        VExprSPtr* expr,
        const std::unordered_map<int32_t, format::GlobalIndex>& slot_id_to_global_index) {
    DORIS_CHECK(expr != nullptr);
    if (*expr == nullptr) {
        return Status::OK();
    }
    if ((*expr)->is_slot_ref()) {
        const auto* slot_ref = assert_cast<const VSlotRef*>(expr->get());
        const auto global_index_it = slot_id_to_global_index.find(slot_ref->slot_id());
        if (global_index_it == slot_id_to_global_index.end()) {
            DORIS_CHECK(slot_ref->slot_id() >= 0);
            const auto global_index = format::GlobalIndex(cast_set<size_t>(slot_ref->slot_id()));
            *expr = TableSlotRef::create_shared(cast_set<int>(global_index.value()),
                                                cast_set<int>(global_index.value()), -1,
                                                slot_ref->data_type(), slot_ref->column_name());
            RETURN_IF_ERROR(expr->get()->prepare(nullptr, RowDescriptor(), nullptr));
            return Status::OK();
        }
        const auto global_index = global_index_it->second;
        *expr = TableSlotRef::create_shared(cast_set<int>(global_index.value()),
                                            cast_set<int>(global_index.value()), -1,
                                            slot_ref->data_type(), slot_ref->column_name());
        RETURN_IF_ERROR(expr->get()->prepare(nullptr, RowDescriptor(), nullptr));
        return Status::OK();
    }
    auto children = (*expr)->children();
    for (auto& child : children) {
        if (child == nullptr) {
            continue;
        }
        RETURN_IF_ERROR(rewrite_slot_refs_to_global_index(&child, slot_id_to_global_index));
    }
    (*expr)->set_children(std::move(children));
    return Status::OK();
}

} // namespace

#ifdef BE_TEST
Status FileScannerV2::TEST_build_nested_children_from_access_paths(
        format::ColumnDefinition* column, const TColumnAccessPaths& access_paths) {
    return build_nested_children_from_access_paths(column, access_paths, nullptr);
}

Status FileScannerV2::TEST_build_nested_children_from_access_paths(
        format::ColumnDefinition* column, const TColumnAccessPaths& access_paths,
        const format::ColumnDefinition* schema_column) {
    return build_nested_children_from_access_paths(column, access_paths, schema_column);
}
#endif

// TODO: Only support parquet format now
bool FileScannerV2::is_supported(const TFileScanRangeParams& params, const TFileRangeDesc& range) {
    return get_range_format_type(params, range) == TFileFormatType::FORMAT_PARQUET &&
           is_supported_table_format(range);
}

FileScannerV2::FileScannerV2(RuntimeState* state, FileScanLocalState* local_state, int64_t limit,
                             std::shared_ptr<SplitSourceConnector> split_source,
                             RuntimeProfile* profile, ShardedKVCache* kv_cache,
                             const std::unordered_map<std::string, int>* colname_to_slot_id)
        : Scanner(state, local_state, limit, profile),
          _split_source(std::move(split_source)),
          _kv_cache(kv_cache) {
    (void)colname_to_slot_id;
    if (state->get_query_ctx() != nullptr &&
        state->get_query_ctx()->file_scan_range_params_map.count(local_state->parent_id()) > 0) {
        _params = &(state->get_query_ctx()->file_scan_range_params_map[local_state->parent_id()]);
    } else {
        _params = _split_source->get_params();
    }
}

Status FileScannerV2::init(RuntimeState* state, const VExprContextSPtrs& conjuncts) {
    RETURN_IF_ERROR(Scanner::init(state, conjuncts));
    _get_block_timer =
            ADD_TIMER_WITH_LEVEL(_local_state->scanner_profile(), "FileScannerV2GetBlockTime", 1);
    _file_counter =
            ADD_COUNTER_WITH_LEVEL(_local_state->scanner_profile(), "FileNumber", TUnit::UNIT, 1);
    _file_read_bytes_counter = ADD_COUNTER_WITH_LEVEL(_local_state->scanner_profile(),
                                                      "FileReadBytes", TUnit::BYTES, 1);
    _file_read_calls_counter = ADD_COUNTER_WITH_LEVEL(_local_state->scanner_profile(),
                                                      "FileReadCalls", TUnit::UNIT, 1);
    _file_read_time_counter =
            ADD_TIMER_WITH_LEVEL(_local_state->scanner_profile(), "FileReadTime", 1);
    _file_cache_statistics = std::make_unique<io::FileCacheStatistics>();
    _file_reader_stats = std::make_unique<io::FileReaderStats>();
    RETURN_IF_ERROR(_init_io_ctx());
    _io_ctx->file_cache_stats = _file_cache_statistics.get();
    _io_ctx->file_reader_stats = _file_reader_stats.get();
    _io_ctx->is_disposable = _state->query_options().disable_file_cache;
    return Status::OK();
}

Status FileScannerV2::_open_impl(RuntimeState* state) {
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(Scanner::_open_impl(state));
    RETURN_IF_ERROR(_split_source->get_next(&_first_scan_range, &_current_range));
    if (_first_scan_range) {
        RETURN_IF_ERROR(_init_expr_ctxes());
    }
    return Status::OK();
}

Status FileScannerV2::_get_block_impl(RuntimeState* state, Block* block, bool* eof) {
    while (true) {
        RETURN_IF_CANCELLED(state);
        if (_table_reader == nullptr) {
            RETURN_IF_ERROR(_prepare_next_split(eof));
            if (*eof) {
                return Status::OK();
            }
        }

        {
            SCOPED_TIMER(_get_block_timer);
            RETURN_IF_ERROR(_table_reader->get_block(block, eof));
        }
        if (*eof) {
            RETURN_IF_ERROR(_table_reader->close());
            _table_reader.reset();
            _state->update_num_finished_scan_range(1);
            *eof = false;
            continue;
        }
        return Status::OK();
    }
}

Status FileScannerV2::_prepare_next_split(bool* eos) {
    if (_table_reader != nullptr) {
        RETURN_IF_ERROR(_table_reader->close());
        _table_reader.reset();
        _state->update_num_finished_scan_range(1);
    }

    bool has_next = _first_scan_range;
    if (!_first_scan_range) {
        RETURN_IF_ERROR(_split_source->get_next(&has_next, &_current_range));
    }
    _first_scan_range = false;
    if (!has_next || _should_stop) {
        *eos = true;
        return Status::OK();
    }
    _current_range_path = _current_range.path;
    RETURN_IF_ERROR(_create_table_reader(_current_range));
    RETURN_IF_ERROR(_prepare_table_reader_split(_current_range));
    COUNTER_UPDATE(_file_counter, 1);
    *eos = false;
    return Status::OK();
}

Status FileScannerV2::_create_table_reader(const TFileRangeDesc& range) {
    const auto format_type = _get_current_format_type();
    format::FileFormat file_format;
    RETURN_IF_ERROR(_to_file_format(format_type, &file_format));
    RETURN_IF_ERROR(_create_table_reader_for_format(range));
    DORIS_CHECK(_table_reader != nullptr);

    format::TableColumnPredicates table_column_predicates;
    RETURN_IF_ERROR(_build_table_column_predicates(&table_column_predicates));
    VExprContextSPtrs table_conjuncts;
    RETURN_IF_ERROR(_build_table_conjuncts(&table_conjuncts));
    RETURN_IF_ERROR(_table_reader->init({
            .projected_columns = _projected_columns,
            .column_predicates = std::move(table_column_predicates),
            .conjuncts = std::move(table_conjuncts),
            .format = file_format,
            .scan_params = const_cast<TFileScanRangeParams*>(_params),
            .io_ctx = _io_ctx,
            .runtime_state = _state,
            .scanner_profile = _local_state->scanner_profile(),
            .allow_missing_columns = false, // TODO
            .push_down_agg_type = _local_state->get_push_down_agg_type(),
    }));
    return Status::OK();
}

Status FileScannerV2::_create_table_reader_for_format(const TFileRangeDesc& range) {
    const auto table_format = table_format_name(range);
    if (table_format == "NotSet" || table_format == "tvf") {
        _table_reader = std::make_unique<format::TableReader>();
    } else if (table_format == "hive") {
        _table_reader = hive::HiveReader::create_unique();
    } else if (table_format == "iceberg") {
        _table_reader = std::make_unique<iceberg::IcebergTableReader>();
    } else if (table_format == "paimon") {
        _table_reader = paimon::PaimonReader::create_unique();
    } else {
        return Status::NotSupported("FileScannerV2 does not support table format {}", table_format);
    }
    return Status::OK();
}

Status FileScannerV2::_prepare_table_reader_split(const TFileRangeDesc& range) {
    std::map<std::string, Field> partition_values;
    RETURN_IF_ERROR(_generate_partition_values(range, &partition_values));
    RETURN_IF_ERROR(_table_reader->prepare_split({
            .partition_values = std::move(partition_values),
            .cache = _kv_cache,
            .current_range = range,
            .global_rowid_context = _create_global_rowid_context(range),
    }));
    return Status::OK();
}

bool FileScannerV2::_should_enable_file_meta_cache() const {
    return ExecEnv::GetInstance()->file_meta_cache()->enabled() &&
           _split_source->num_scan_ranges() < config::max_external_file_meta_cache_num / 3;
}

std::optional<format::GlobalRowIdContext> FileScannerV2::_create_global_rowid_context(
        const TFileRangeDesc& range) const {
    if (!_need_global_rowid_column) {
        return std::nullopt;
    }
    auto& id_file_map = _state->get_id_file_map();
    DORIS_CHECK(id_file_map != nullptr);
    const auto file_id = id_file_map->get_file_mapping_id(
            std::make_shared<FileMapping>(_local_state->cast<FileScanLocalState>().parent_id(),
                                          range, _should_enable_file_meta_cache()));
    return format::GlobalRowIdContext {
            .version = IdManager::ID_VERSION,
            .backend_id = BackendOptions::get_backend_id(),
            .file_id = file_id,
    };
}

Status FileScannerV2::_generate_partition_values(
        const TFileRangeDesc& range, std::map<std::string, Field>* partition_values) const {
    DORIS_CHECK(partition_values != nullptr);
    partition_values->clear();
    if (!range.__isset.columns_from_path_keys || !range.__isset.columns_from_path) {
        return Status::OK();
    }
    DORIS_CHECK(range.columns_from_path_keys.size() == range.columns_from_path.size());
    for (size_t idx = 0; idx < range.columns_from_path_keys.size(); ++idx) {
        const auto& key = range.columns_from_path_keys[idx];
        const auto it = _partition_slot_descs.find(key);
        if (it == _partition_slot_descs.end()) {
            continue;
        }
        const auto& value = range.columns_from_path[idx];
        const bool is_null = range.__isset.columns_from_path_is_null &&
                             idx < range.columns_from_path_is_null.size() &&
                             range.columns_from_path_is_null[idx];
        Field field;
        DORIS_CHECK(it->second.slot_desc != nullptr);
        RETURN_IF_ERROR(_parse_partition_value(it->second.slot_desc, value, is_null, &field));
        partition_values->emplace(it->second.canonical_name, std::move(field));
    }
    return Status::OK();
}

Status FileScannerV2::_parse_partition_value(const SlotDescriptor* slot_desc,
                                             const std::string& value, bool is_null,
                                             Field* field) const {
    DORIS_CHECK(slot_desc != nullptr);
    DORIS_CHECK(field != nullptr);
    if (is_null) {
        *field = Field::create_field<TYPE_NULL>(Null());
        return Status::OK();
    }
    const auto data_type = remove_nullable(slot_desc->get_data_type_ptr());
    auto column = data_type->create_column();
    auto serde = data_type->get_serde();
    DataTypeSerDe::FormatOptions options;
    options.converted_from_string = true;
    StringRef ref(value.data(), value.size());
    RETURN_IF_ERROR(serde->from_string(ref, *column, options));
    DORIS_CHECK(column->size() == 1);
    *field = (*column)[0];
    return Status::OK();
}

Status FileScannerV2::_init_expr_ctxes() {
    _slot_id_to_desc.clear();
    _slot_id_to_global_index.clear();
    _partition_slot_descs.clear();
    for (const auto* slot_desc : _output_tuple_desc->slots()) {
        _slot_id_to_desc.emplace(slot_desc->id(), slot_desc);
    }
    RETURN_IF_ERROR(_build_projected_columns());
    return Status::OK();
}

Status FileScannerV2::_build_projected_columns() {
    _projected_columns.clear();
    _projected_columns.reserve(_params->required_slots.size());
    _need_global_rowid_column = false;

    for (size_t slot_idx = 0; slot_idx < _params->required_slots.size(); ++slot_idx) {
        const auto& slot_info = _params->required_slots[slot_idx];
        const auto it = _slot_id_to_desc.find(slot_info.slot_id);
        if (it == _slot_id_to_desc.end()) {
            return Status::InternalError("Unknown source slot descriptor, slot_id={}",
                                         slot_info.slot_id);
        }
        auto column = _build_table_column(it->second);
        if (column.name.starts_with(BeConsts::GLOBAL_ROWID_COL)) {
            _need_global_rowid_column = true;
        }
        RETURN_IF_ERROR(_build_default_expr(slot_info, &column.default_expr));
        std::optional<format::ColumnDefinition> schema_column;
        if (const auto* schema_field = find_external_root_field(_params, column);
            schema_field != nullptr) {
            // If the column has a matching root field in the schema, use the schema field to build the column's nested children.
            // NOTICE: The nested `schema_column` is completed without projection.
            schema_column = build_schema_column_from_external_field(*schema_field, column.type);
            column.identifier = schema_column->identifier;
            column.name_mapping = schema_column->name_mapping;
        }
        // Build the column's nested children based on the column's access paths and the schema column (if exists).
        // The access paths are generated from the slot's access path expressions which means a projected column can have a subset of the schema column's nested children.
        RETURN_IF_ERROR(build_nested_children_from_access_paths(
                &column, it->second, schema_column.has_value() ? &*schema_column : nullptr));
        if (is_partition_slot(slot_info, column.name)) {
            column.is_partition_key = true;
            _partition_slot_descs.emplace(
                    column.name,
                    PartitionSlotInfo {.slot_desc = it->second, .canonical_name = column.name});
            for (const auto& alias : column.name_mapping) {
                _partition_slot_descs.emplace(
                        alias,
                        PartitionSlotInfo {.slot_desc = it->second, .canonical_name = column.name});
            }
        }
        const auto global_index = format::GlobalIndex(slot_idx);
        _slot_id_to_global_index.emplace(slot_info.slot_id, global_index);
        _projected_columns.push_back(std::move(column));
    }
    return Status::OK();
}

Status FileScannerV2::_build_default_expr(const TFileScanSlotInfo& slot_info,
                                          VExprContextSPtr* ctx) const {
    DORIS_CHECK(ctx != nullptr);
    if (slot_info.__isset.default_value_expr && !slot_info.default_value_expr.nodes.empty()) {
        return VExpr::create_expr_tree(slot_info.default_value_expr, *ctx);
    }

    if (_params->__isset.default_value_of_src_slot) {
        const auto it = _params->default_value_of_src_slot.find(slot_info.slot_id);
        if (it != _params->default_value_of_src_slot.end() && !it->second.nodes.empty()) {
            return VExpr::create_expr_tree(it->second, *ctx);
        }
    }
    return Status::OK();
}

format::ColumnDefinition FileScannerV2::_build_table_column(const SlotDescriptor* slot_desc) {
    DORIS_CHECK(slot_desc != nullptr);
    format::ColumnDefinition column;
    // TODO(gabriel): why always BY_NAME here?
    column.identifier = Field::create_field<TYPE_STRING>(slot_desc->col_name());
    column.name = slot_desc->col_name();
    column.type = slot_desc->get_data_type_ptr();
    return column;
}

Status FileScannerV2::_build_table_column_predicates(
        format::TableColumnPredicates* predicates) const {
    DORIS_CHECK(predicates != nullptr);
    predicates->clear();
    const auto& slot_predicates = _local_state->cast<FileScanLocalState>()._slot_id_to_predicates;
    for (const auto& [slot_id, slot_predicate_list] : slot_predicates) {
        const auto it = _slot_id_to_desc.find(slot_id);
        if (it == _slot_id_to_desc.end()) {
            continue;
        }
        const auto global_index_it = _slot_id_to_global_index.find(slot_id);
        if (global_index_it == _slot_id_to_global_index.end()) {
            continue;
        }
        (*predicates)[global_index_it->second] = slot_predicate_list;
    }
    return Status::OK();
}

Status FileScannerV2::_build_table_conjuncts(VExprContextSPtrs* conjuncts) const {
    DORIS_CHECK(conjuncts != nullptr);
    conjuncts->clear();
    conjuncts->reserve(_conjuncts.size());
    for (const auto& conjunct : _conjuncts) {
        VExprSPtr root;
        RETURN_IF_ERROR(format::clone_table_expr_tree(conjunct->root(), &root));
        RETURN_IF_ERROR(rewrite_slot_refs_to_global_index(&root, _slot_id_to_global_index));
        conjuncts->push_back(VExprContext::create_shared(std::move(root)));
    }
    return Status::OK();
}

TFileFormatType::type FileScannerV2::_get_current_format_type() const {
    return get_range_format_type(*_params, _current_range);
}

Status FileScannerV2::_to_file_format(TFileFormatType::type format_type,
                                      format::FileFormat* file_format) {
    DORIS_CHECK(file_format != nullptr);
    switch (format_type) {
    case TFileFormatType::FORMAT_PARQUET:
        *file_format = format::FileFormat::PARQUET;
        return Status::OK();
    default:
        return Status::NotSupported("FileScannerV2 does not support file format {}",
                                    to_string(format_type));
    }
}

Status FileScannerV2::_init_io_ctx() {
    _io_ctx = std::make_shared<io::IOContext>();
    _io_ctx->query_id = &_state->query_id();
    return Status::OK();
}

Status FileScannerV2::close(RuntimeState* state) {
    if (!_try_close()) {
        return Status::OK();
    }
    if (_table_reader != nullptr) {
        RETURN_IF_ERROR(_table_reader->close());
        _table_reader.reset();
    }
    return Scanner::close(state);
}

void FileScannerV2::try_stop() {
    Scanner::try_stop();
    if (_io_ctx) {
        _io_ctx->should_stop = true;
    }
}

void FileScannerV2::update_realtime_counters() {
    if (_file_reader_stats == nullptr) {
        return;
    }
    const int64_t bytes_read = _file_reader_stats->read_bytes;
    COUNTER_SET(_file_read_bytes_counter, bytes_read);
    COUNTER_SET(_file_read_calls_counter, cast_set<int64_t>(_file_reader_stats->read_calls));
    COUNTER_SET(_file_read_time_counter, cast_set<int64_t>(_file_reader_stats->read_time_ns));
}

} // namespace doris
