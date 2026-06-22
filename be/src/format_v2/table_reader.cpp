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
#include <cstring>
#include <set>
#include <sstream>
#include <stdexcept>
#include <utility>
#include <vector>

#include "common/cast_set.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_struct.h"
#include "exec/common/endian.h"
#include "exprs/vexpr_context.h"
#include "exprs/vslot_ref.h"
#include "format/table/deletion_vector_reader.h"
#include "format_v2/column_mapper.h"
#include "format_v2/parquet/parquet_reader.h"
#include "roaring/roaring64map.hh"
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
    case FileFormat::JNI:
        return "JNI";
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

DataTypePtr find_struct_child_type_by_name(const DataTypeStruct& struct_type,
                                           const std::string& field_name) {
    for (size_t field_idx = 0; field_idx < struct_type.get_elements().size(); ++field_idx) {
        if (to_lower(struct_type.get_element_name(field_idx)) == to_lower(field_name)) {
            return struct_type.get_element(field_idx);
        }
    }
    return nullptr;
}

ColumnDefinition build_schema_column_from_external_field(const schema::external::TField& field,
                                                         DataTypePtr type) {
    ColumnDefinition column {
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

std::string table_column_predicates_debug_string(const TableColumnPredicates& predicates) {
    std::ostringstream out;
    out << "{";
    size_t idx = 0;
    for (const auto& [global_index, column_predicates] : predicates) {
        if (idx++ > 0) {
            out << ", ";
        }
        out << global_index.value() << ":{predicate_count=" << column_predicates.size() << "}";
    }
    out << "}";
    return out.str();
}

void collect_global_indices(const VExprSPtr& expr, std::set<GlobalIndex>* global_indices) {
    if (expr == nullptr) {
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
                             DeleteRows* delete_rows) {
    DORIS_CHECK(buf != nullptr);
    DORIS_CHECK(delete_rows != nullptr);
    DORIS_CHECK(format == DeleteFileDesc::Format::PAIMON ||
                format == DeleteFileDesc::Format::ICEBERG);

    const size_t checksum_size = format == DeleteFileDesc::Format::ICEBERG ? 4 : 0;
    if (buffer_size < 8 + checksum_size) [[unlikely]] {
        return Status::DataQualityError("Deletion vector file size too small: {}", buffer_size);
    }

    auto total_length = BigEndian::Load32(buf);
    if (total_length + 4 + checksum_size != buffer_size) [[unlikely]] {
        return Status::DataQualityError("Deletion vector length mismatch, expected: {}, actual: {}",
                                        total_length + 4 + checksum_size, buffer_size);
    }

    const char* bitmap_buf = buf + 8;
    const size_t bitmap_size = buffer_size - 8 - checksum_size;
    if (format == DeleteFileDesc::Format::PAIMON) {
        // Paimon BitmapDeletionVector stores:
        //   [4-byte big-endian length][4-byte magic 0x5E43F2D0][32-bit roaring bitmap]
        // The length covers magic + bitmap, and does not include the leading length field.
        constexpr static char PAIMON_BITMAP_MAGIC[] = {'\x5E', '\x43', '\xF2', '\xD0'};
        if (memcmp(buf + sizeof(total_length), PAIMON_BITMAP_MAGIC, 4) != 0) [[unlikely]] {
            return Status::DataQualityError(
                    "Paimon deletion vector magic number mismatch, expected: {}, actual: {}",
                    BigEndian::Load32(PAIMON_BITMAP_MAGIC),
                    BigEndian::Load32(buf + sizeof(total_length)));
        }

        roaring::Roaring bitmap;
        try {
            bitmap = roaring::Roaring::readSafe(bitmap_buf, bitmap_size);
        } catch (const std::runtime_error& e) {
            return Status::DataQualityError("Decode roaring bitmap failed, {}", e.what());
        }

        delete_rows->reserve(bitmap.cardinality());
        for (auto it = bitmap.begin(); it != bitmap.end(); it++) {
            delete_rows->push_back(*it);
        }
        return Status::OK();
    }

    constexpr static char ICEBERG_DV_MAGIC[] = {'\xD1', '\xD3', '\x39', '\x64'};
    if (memcmp(buf + sizeof(total_length), ICEBERG_DV_MAGIC, 4) != 0) [[unlikely]] {
        return Status::DataQualityError(
                "Iceberg deletion vector magic number mismatch, expected: {}, actual: {}",
                BigEndian::Load32(ICEBERG_DV_MAGIC), BigEndian::Load32(buf + sizeof(total_length)));
    }

    roaring::Roaring64Map bitmap;
    try {
        bitmap = roaring::Roaring64Map::readSafe(bitmap_buf, bitmap_size);
    } catch (const std::runtime_error& e) {
        return Status::DataQualityError("Decode roaring bitmap failed, {}", e.what());
    }

    delete_rows->reserve(bitmap.cardinality());
    for (auto it = bitmap.begin(); it != bitmap.end(); it++) {
        delete_rows->push_back(cast_set<int64_t>(*it));
    }
    return Status::OK();
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
        << ", table_column_predicates="
        << table_column_predicates_debug_string(_table_column_predicates)
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
        << ", column_mapper=" << _data_reader.column_mapper.debug_string() << "}";
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

Status TableReader::init(TableReadOptions&& options) {
    _scan_params = options.scan_params;
    _format = options.format;
    _io_ctx = options.io_ctx;
    _runtime_state = options.runtime_state;
    _scanner_profile = options.scanner_profile;
    _push_down_agg_type = options.push_down_agg_type;
    _projected_columns = std::move(options.projected_columns);
    _system_properties = create_system_properties(_scan_params);
    _mapper_options.mode = TableColumnMappingMode::BY_NAME;
    _conjuncts = std::move(options.conjuncts);
    _table_column_predicates = std::move(options.column_predicates);

    if (_scanner_profile != nullptr) {
        static const char* table_profile = "TableReader";
        ADD_TIMER_WITH_LEVEL(_scanner_profile, table_profile, 1);
        _profile.num_delete_files = ADD_CHILD_COUNTER_WITH_LEVEL(_scanner_profile, "NumDeleteFiles",
                                                                 TUnit::UNIT, table_profile, 1);
        _profile.num_delete_rows = ADD_CHILD_COUNTER_WITH_LEVEL(_scanner_profile, "NumDeleteRows",
                                                                TUnit::UNIT, table_profile, 1);
        _profile.parse_delete_file_time = ADD_CHILD_TIMER_WITH_LEVEL(
                _scanner_profile, "ParseDeleteFileTime", table_profile, 1);
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
    }
    return Status::OK();
}

Status TableReader::_build_table_filters_from_conjuncts() {
    _table_filters.clear();
    for (const auto& conjunct : _conjuncts) {
        RETURN_IF_ERROR(
                build_table_filters_from_conjunct(conjunct, _runtime_state, &_table_filters));
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

Status TableReader::create_next_reader(bool* eos) {
    SCOPED_TIMER(_profile.create_reader_timer);
    DCHECK(_data_reader.reader == nullptr);
    if (_current_task == nullptr) {
        *eos = true;
        return Status::OK();
    }

    RETURN_IF_ERROR(create_file_reader(&_data_reader.reader));
    DORIS_CHECK(_data_reader.reader != nullptr);
    RETURN_IF_ERROR(_data_reader.reader->init(_runtime_state));
    RETURN_IF_ERROR(open_reader());
    if (_data_reader.reader == nullptr) {
        *eos = _current_task == nullptr;
        return Status::OK();
    }
    *eos = false;
    return Status::OK();
}

Status TableReader::create_file_reader(std::unique_ptr<FileReader>* reader) {
    DORIS_CHECK(reader != nullptr);
    if (_format == FileFormat::PARQUET) {
        const bool enable_mapping_timestamp_tz =
                _scan_params != nullptr && _scan_params->__isset.enable_mapping_timestamp_tz &&
                _scan_params->enable_mapping_timestamp_tz;
        *reader = std::make_unique<format::parquet::ParquetReader>(
                _system_properties, _current_task->data_file, _io_ctx, _scanner_profile,
                _global_rowid_context, enable_mapping_timestamp_tz);
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
    _partition_values = std::move(options.partition_values);
    _current_task = std::make_unique<ScanTask>();
    _current_task->data_file = create_file_description(options.current_range);
    _global_rowid_context = options.global_rowid_context;
    _delete_rows = nullptr;
    _aggregate_pushdown_tried = false;
    _remaining_table_level_count = -1;
    if (_push_down_agg_type == TPushAggOp::type::COUNT &&
        options.current_range.__isset.table_format_params &&
        options.current_range.table_format_params.__isset.table_level_row_count) {
        DORIS_CHECK(options.current_range.table_format_params.table_level_row_count >= -1);
        _remaining_table_level_count =
                options.current_range.table_format_params.table_level_row_count;
    }
    if (_is_table_level_count_active()) {
        return Status::OK();
    }
    return _parse_delete_predicates(options);
}

Status TableReader::_parse_delete_predicates(const SplitReadOptions& options) {
    DeleteFileDesc desc {.fs_name = options.current_range.fs_name};
    bool has_delete_file = false;
    RETURN_IF_ERROR(_parse_deletion_vector_file(options.current_range.table_format_params, &desc,
                                                &has_delete_file));
    if (has_delete_file) {
        DORIS_CHECK(options.cache != nullptr);
        Status create_status = Status::OK();

        _delete_rows = options.cache->get<DeleteRows>(desc.key, [&]() -> DeleteRows* {
            auto* delete_rows = new DeleteRows;

            DeletionVectorReader dv_reader(_runtime_state, _scanner_profile, *_scan_params, desc,
                                           _io_ctx.get());
            create_status = dv_reader.open();
            if (!create_status.ok()) [[unlikely]] {
                return nullptr;
            }

            size_t bytes_read = desc.size;
            std::vector<char> buffer(bytes_read);
            create_status = dv_reader.read_at(desc.start_offset, {buffer.data(), bytes_read});
            if (!create_status.ok()) [[unlikely]] {
                return nullptr;
            }

            const char* buf = buffer.data();
            SCOPED_TIMER(_profile.parse_delete_file_time);
            create_status = parse_deletion_vector(buf, bytes_read, desc.format, delete_rows);
            if (!create_status.ok()) [[unlikely]] {
                return nullptr;
            }
            COUNTER_UPDATE(_profile.num_delete_rows, delete_rows->size());
            return delete_rows;
        });
        RETURN_IF_ERROR(create_status);
    }

    return Status::OK();
}
} // namespace doris::format
