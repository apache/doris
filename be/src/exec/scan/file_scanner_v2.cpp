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
#include "format/format_common.h"
#include "format/reader/table/hive_reader.h"
#include "format/reader/table/paimon_reader.h"
#include "format/reader/table_reader.h"
#include "format/table/iceberg_reader_v2.h"
#include "io/io_common.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"

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

bool is_partition_slot(const TFileScanSlotInfo& slot_info) {
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

reader::TableColumn* find_or_add_child(reader::TableColumn* parent, reader::ColumnId id,
                                       std::string name, DataTypePtr type) {
    DORIS_CHECK(parent != nullptr);
    for (auto& child : parent->children) {
        if (child.id == id || child.name == name) {
            return &child;
        }
    }
    parent->children.push_back({
            .id = id,
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

DataTypePtr find_struct_child_type_by_name(const DataTypeStruct& struct_type,
                                           const std::string& field_name) {
    for (size_t field_idx = 0; field_idx < struct_type.get_elements().size(); ++field_idx) {
        if (to_lower(struct_type.get_element_name(field_idx)) == to_lower(field_name)) {
            return struct_type.get_element(field_idx);
        }
    }
    return nullptr;
}

reader::TableColumn build_schema_column_from_external_field(const schema::external::TField& field,
                                                            DataTypePtr type) {
    reader::TableColumn column {
            .id = field.__isset.id ? field.id : -1,
            .name = field.__isset.name ? field.name : "",
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

const reader::TableColumn* find_schema_child_by_path(const reader::TableColumn* schema_column,
                                                     const std::string& child_path) {
    if (schema_column == nullptr) {
        return nullptr;
    }
    int32_t parsed_field_id = -1;
    if (parse_non_negative_int(child_path, &parsed_field_id)) {
        const auto child_it = std::ranges::find_if(
                schema_column->children,
                [&](const reader::TableColumn& child) { return child.id == parsed_field_id; });
        return child_it == schema_column->children.end() ? nullptr : &*child_it;
    }
    const auto child_it = std::ranges::find_if(schema_column->children, [&](const auto& child) {
        return to_lower(child.name) == to_lower(child_path);
    });
    return child_it == schema_column->children.end() ? nullptr : &*child_it;
}

const schema::external::TField* find_external_root_field(const TFileScanRangeParams* params,
                                                         const reader::TableColumn& column) {
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
        if (field->__isset.id && field->id == column.id) {
            return field;
        }
        if (field->__isset.name && to_lower(field->name) == to_lower(column.name)) {
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

Status build_nested_children_from_access_node(reader::TableColumn* column, const DataTypePtr& type,
                                              const AccessPathNode& node, const std::string& path,
                                              const reader::TableColumn* schema_column);

Status build_struct_children_from_access_node(reader::TableColumn* column,
                                              const DataTypeStruct& struct_type,
                                              const AccessPathNode& node, const std::string& path,
                                              const reader::TableColumn* schema_column) {
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
        int32_t field_id = schema_child == nullptr ? -1 : schema_child->id;
        std::string field_name = schema_child == nullptr ? child_path : schema_child->name;
        DataTypePtr field_type = schema_child == nullptr ? nullptr : schema_child->type;
        if (schema_child == nullptr) {
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
        auto* child = find_or_add_child(column, field_id, field_name, field_type);
        RETURN_IF_ERROR(build_nested_children_from_access_node(
                child, child->type, child_node, path + "." + child_path, schema_child));
    }
    return Status::OK();
}

Status build_map_children_from_access_node(reader::TableColumn* column, const DataTypeMap& map_type,
                                           const AccessPathNode& node, const std::string& path,
                                           const reader::TableColumn* schema_column) {
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

Status build_nested_children_from_access_node(reader::TableColumn* column, const DataTypePtr& type,
                                              const AccessPathNode& node, const std::string& path,
                                              const reader::TableColumn* schema_column) {
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

Status build_nested_children_from_access_paths(reader::TableColumn* column,
                                               const TColumnAccessPaths& access_paths,
                                               const reader::TableColumn* schema_column) {
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
            (!parse_non_negative_int(path.front(), &top_level_id) || top_level_id != column->id)) {
            return Status::NotSupported("FileScannerV2 access path {} does not match slot {}",
                                        access_path_to_string(path), column->name);
        }
        insert_access_path(&root, path, 1);
    }
    // Recursively build nested children for the column based on the AccessPathNode tree.
    return build_nested_children_from_access_node(column, column->type, root, column->name,
                                                  schema_column);
}

Status build_nested_children_from_access_paths(reader::TableColumn* column,
                                               const SlotDescriptor* slot_desc,
                                               const reader::TableColumn* schema_column) {
    DORIS_CHECK(column != nullptr);
    DORIS_CHECK(slot_desc != nullptr);
    return build_nested_children_from_access_paths(column, slot_desc->all_access_paths(),
                                                   schema_column);
}

} // namespace

#ifdef BE_TEST
Status FileScannerV2::TEST_build_nested_children_from_access_paths(
        reader::TableColumn* column, const TColumnAccessPaths& access_paths) {
    return build_nested_children_from_access_paths(column, access_paths, nullptr);
}

Status FileScannerV2::TEST_build_nested_children_from_access_paths(
        reader::TableColumn* column, const TColumnAccessPaths& access_paths,
        const reader::TableColumn* schema_column) {
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
    reader::FileFormat format;
    RETURN_IF_ERROR(_to_file_format(format_type, &format));
    RETURN_IF_ERROR(_create_table_reader_for_format(range));
    DORIS_CHECK(_table_reader != nullptr);

    reader::TableColumnPredicates table_column_predicates;
    RETURN_IF_ERROR(_build_table_column_predicates(&table_column_predicates));
    RETURN_IF_ERROR(_table_reader->init({
            .projected_columns = _projected_columns,
            .column_predicates = std::move(table_column_predicates),
            .conjuncts = _conjuncts,
            .format = format,
            .scan_params = const_cast<TFileScanRangeParams*>(_params),
            .io_ctx = _io_ctx,
            .runtime_state = _state,
            .scanner_profile = _local_state->scanner_profile(),
            .allow_missing_columns = false, // TODO
            .push_down_agg_type = _local_state->get_push_down_agg_type(),
            .profile = nullptr, // TODO
    }));
    return Status::OK();
}

Status FileScannerV2::_create_table_reader_for_format(const TFileRangeDesc& range) {
    const auto table_format = table_format_name(range);
    if (table_format == "NotSet" || table_format == "tvf") {
        _table_reader = std::make_unique<reader::TableReader>();
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
    }));
    return Status::OK();
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
        RETURN_IF_ERROR(_parse_partition_value(it->second, value, is_null, &field));
        partition_values->emplace(key, std::move(field));
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

    for (const auto& slot_info : _params->required_slots) {
        const auto it = _slot_id_to_desc.find(slot_info.slot_id);
        if (it == _slot_id_to_desc.end()) {
            return Status::InternalError("Unknown source slot descriptor, slot_id={}",
                                         slot_info.slot_id);
        }
        auto column = _build_table_column(it->second);
        RETURN_IF_ERROR(_build_default_expr(slot_info, &column.default_expr));
        std::optional<reader::TableColumn> schema_column;
        if (const auto* schema_field = find_external_root_field(_params, column);
            schema_field != nullptr) {
            // If the column has a matching root field in the schema, use the schema field to build the column's nested children.
            schema_column = build_schema_column_from_external_field(*schema_field, column.type);
        }
        RETURN_IF_ERROR(build_nested_children_from_access_paths(
                &column, it->second, schema_column.has_value() ? &*schema_column : nullptr));
        if (is_partition_slot(slot_info)) {
            column.is_partition_key = true;
            _partition_slot_descs.emplace(column.name, it->second);
        }
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

reader::TableColumn FileScannerV2::_build_table_column(const SlotDescriptor* slot_desc) {
    DORIS_CHECK(slot_desc != nullptr);
    reader::TableColumn column;
    column.id = slot_desc->col_unique_id();
    column.name = slot_desc->col_name();
    column.type = slot_desc->get_data_type_ptr();
    return column;
}

Status FileScannerV2::_build_table_column_predicates(
        reader::TableColumnPredicates* predicates) const {
    DORIS_CHECK(predicates != nullptr);
    predicates->clear();
    const auto& slot_predicates = _local_state->cast<FileScanLocalState>()._slot_id_to_predicates;
    for (const auto& [slot_id, slot_predicate_list] : slot_predicates) {
        const auto it = _slot_id_to_desc.find(slot_id);
        if (it == _slot_id_to_desc.end()) {
            continue;
        }
        (*predicates)[it->second->col_unique_id()] = {
                reader::TableColumn {.id = it->second->col_unique_id(),
                                     .name = it->second->col_name(),
                                     .type = it->second->get_data_type_ptr()},
                slot_predicate_list};
    }
    return Status::OK();
}

TFileFormatType::type FileScannerV2::_get_current_format_type() const {
    return get_range_format_type(*_params, _current_range);
}

Status FileScannerV2::_to_file_format(TFileFormatType::type format_type,
                                      reader::FileFormat* format) {
    DORIS_CHECK(format != nullptr);
    switch (format_type) {
    case TFileFormatType::FORMAT_PARQUET:
        *format = reader::FileFormat::PARQUET;
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
    _should_stop = true;
    if (_table_reader != nullptr) {
        static_cast<void>(_table_reader->close());
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
