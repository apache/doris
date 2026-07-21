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
#include "format/table/paimon_reader.h"
#include "format_v2/column_mapper.h"
#include "format_v2/delimited_text/csv_reader.h"
#include "format_v2/delimited_text/text_reader.h"
#include "format_v2/json/json_reader.h"
#include "format_v2/native/native_reader.h"
#include "format_v2/orc/orc_reader.h"
#include "format_v2/parquet/parquet_reader.h"
#include "storage/segment/condition_cache.h"
#include "util/debug_points.h"
#include "util/string_util.h"

namespace doris::format {
namespace {

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
    }
    return "UNKNOWN";
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
                                                     const schema::external::TField& field) {
    for (size_t field_idx = 0; field_idx < struct_type.get_elements().size(); ++field_idx) {
        if (external_field_matches_name(field, struct_type.get_element_name(field_idx))) {
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
    const auto primitive_type = thrift_to_type(field.type.type);
    if (is_complex_type(primitive_type)) {
        return fallback_type;
    }
    // The delete file can expose an older physical type, but initial defaults belong to the
    // current table field. Restore that type from FE before parsing the default and let the table
    // reader apply the normal promotion cast to the delete-key type.
    return DataTypeFactory::instance().create_data_type(
            primitive_type, false, field.type.__isset.precision ? field.type.precision : 0,
            field.type.__isset.scale ? field.type.scale : 0,
            field.type.__isset.len ? field.type.len : -1);
}

ColumnDefinition build_schema_column_from_external_field(const schema::external::TField& field,
                                                         DataTypePtr type) {
    type = restore_current_primitive_type(field, std::move(type));
    ColumnDefinition column {
            .identifier = field.__isset.id ? Field::create_field<TYPE_INT>(field.id) : Field {},
            .name = field.__isset.name ? field.name : "",
            .name_mapping =
                    field.__isset.name_mapping ? field.name_mapping : std::vector<std::string> {},
            .type = std::move(type),
            .children = {},
            .default_expr = nullptr,
            .initial_default_value = field.__isset.initial_default_value
                                             ? std::make_optional(field.initial_default_value)
                                             : std::nullopt,
            .initial_default_value_is_base64 = field.__isset.initial_default_value_is_base64 &&
                                               field.initial_default_value_is_base64,
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
            auto child_type = find_struct_child_type_by_external_field(struct_type, *child_field);
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
        auto child =
                build_schema_column_from_external_field(*item_field, array_type.get_nested_type());
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
            auto child =
                    build_schema_column_from_external_field(*key_field, map_type.get_key_type());
            child.name = "key";
            if (child.has_identifier_name()) {
                child.identifier = Field::create_field<TYPE_STRING>(child.name);
            }
            column.children.push_back(std::move(child));
        }
        const auto* value_field = get_field_ptr(field.nestedField.map_field.value_field);
        if (value_field != nullptr) {
            auto child = build_schema_column_from_external_field(*value_field,
                                                                 map_type.get_value_type());
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
    context->schema_column = build_schema_column_from_external_field(*schema_field, column->type);
    column->identifier = context->schema_column->identifier;
    column->name_mapping = context->schema_column->name_mapping;
    return Status::OK();
}

std::optional<ColumnDefinition> TableReader::_find_current_table_column_by_field_id(
        int32_t field_id, DataTypePtr type) const {
    if (_scan_params == nullptr || !_scan_params->__isset.history_schema_info ||
        _scan_params->history_schema_info.empty()) {
        return std::nullopt;
    }
    const auto* schema = &_scan_params->history_schema_info.front();
    if (_scan_params->__isset.current_schema_id) {
        for (const auto& candidate_schema : _scan_params->history_schema_info) {
            if (candidate_schema.__isset.schema_id &&
                candidate_schema.schema_id == _scan_params->current_schema_id) {
                schema = &candidate_schema;
                break;
            }
        }
    }
    if (!schema->__isset.root_field || !schema->root_field.__isset.fields) {
        return std::nullopt;
    }
    for (const auto& field_ptr : schema->root_field.fields) {
        const auto* field = get_field_ptr(field_ptr);
        if (field != nullptr && field->__isset.id && field->id == field_id) {
            return build_schema_column_from_external_field(*field, std::move(type));
        }
    }
    return std::nullopt;
}

Status TableReader::init(TableReadOptions&& options) {
    _scan_params = options.scan_params;
    _format = options.format;
    _io_ctx = options.io_ctx;
    _runtime_state = options.runtime_state;
    _scanner_profile = options.scanner_profile;
    _file_slot_descs = options.file_slot_descs;
    _push_down_agg_type = options.push_down_agg_type;
    _push_down_count_columns = options.push_down_count_columns;
    _initial_condition_cache_digest = options.condition_cache_digest;
    _condition_cache_digest = _initial_condition_cache_digest;
    _projected_columns = std::move(options.projected_columns);
    _system_properties = create_system_properties(_scan_params);
    _mapper_options.mode = TableColumnMappingMode::BY_NAME;
    _conjuncts = std::move(options.conjuncts);

    if (_scanner_profile != nullptr) {
        static const char* table_profile = "TableReader";
        ADD_TIMER_WITH_LEVEL(_scanner_profile, table_profile, 1);
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
        _profile.runtime_filter_partition_prune_timer = ADD_TIMER_WITH_LEVEL(
                _scanner_profile, "FileScannerRuntimeFilterPartitionPruningTime", 1);
        _profile.runtime_filter_partition_pruned_range_counter = ADD_COUNTER_WITH_LEVEL(
                _scanner_profile, "RuntimeFilterPartitionPrunedRangeNum", TUnit::UNIT, 1);
    }
    return Status::OK();
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
        // slotless non-deterministic/error-preserving barrier.
        if (in_safe_prefix && !_is_safe_to_pre_execute(conjunct)) {
            in_safe_prefix = false;
        }
        if (!in_safe_prefix) {
            continue;
        }
        RETURN_IF_ERROR(
                build_table_filters_from_conjunct(conjunct, _runtime_state, &_table_filters));
        _constant_pruning_safe_filter_count = _table_filters.size();
    }
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
        return Status::OK();
    }

    auto* cache = segment_v2::ConditionCache::instance();
    if (cache == nullptr) {
        return Status::OK();
    }
    const auto& file = *_current_file_description;
    _condition_cache_key = segment_v2::ConditionCache::ExternalCacheKey(
            file.path, file.mtime, file.file_size, _condition_cache_digest, file.range_start_offset,
            file.range_size,
            segment_v2::ConditionCache::ExternalCacheKey::BASE_GRANULE_AWARE_VERSION);

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
    Status st = _data_reader.reader->init(_runtime_state);
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
    const std::string hive_parquet_time_zone =
            _scan_params != nullptr && _scan_params->__isset.hive_parquet_time_zone
                    ? _scan_params->hive_parquet_time_zone
                    : "";
    if (_format == FileFormat::PARQUET) {
        *reader = std::make_unique<format::parquet::ParquetReader>(
                _system_properties, _current_task->data_file, _io_ctx, _scanner_profile,
                _global_rowid_context, enable_mapping_timestamp_tz, hive_parquet_time_zone);
        return Status::OK();
    }
    if (_format == FileFormat::ORC) {
        *reader = std::make_unique<format::orc::OrcReader>(
                _system_properties, _current_task->data_file, _io_ctx, _scanner_profile,
                _global_rowid_context, enable_mapping_timestamp_tz);
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
    SCOPED_TIMER(_profile.prepare_split_timer);
    _current_split_pruned = false;
    _all_runtime_filters_applied_for_split = options.all_runtime_filters_applied;
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
    _remaining_table_level_count = -1;
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
            return column.is_partition_key &&
                   find_partition_value(column, _partition_values) != nullptr;
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
        if (column.is_partition_key) {
            const auto* partition_value = find_partition_value(column, _partition_values);
            if (partition_value != nullptr) {
                value_column = column.type->create_column_const(1, *partition_value);
            }
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
