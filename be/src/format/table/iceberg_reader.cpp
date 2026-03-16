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

#include "format/table/iceberg_reader.h"

#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/Metrics_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/parquet_types.h>
#include <glog/logging.h>
#include <parallel_hashmap/phmap.h>
#include <rapidjson/document.h>

#include <algorithm>
#include <cstring>
#include <functional>
#include <memory>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/consts.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/define_primitive_type.h"
#include "core/data_type/primitive_type.h"
#include "core/string_ref.h"
#include "exprs/aggregate/aggregate_function.h"
#include "format/format_common.h"
#include "format/generic_reader.h"
#include "format/orc/vorc_reader.h"
#include "format/parquet/schema_desc.h"
#include "format/parquet/vparquet_column_chunk_reader.h"
#include "format/table/deletion_vector_reader.h"
#include "format/table/iceberg_delete_file_reader_helper.h"
#include "format/table/iceberg/iceberg_orc_nested_column_utils.h"
#include "format/table/iceberg/iceberg_parquet_nested_column_utils.h"
#include "format/table/nested_column_access_helper.h"
#include "format/table/table_format_reader.h"
#include "format/table/table_schema_change_helper.h"
#include "runtime/runtime_state.h"
#include "util/coding.h"

namespace cctz {
#include "common/compile_check_begin.h"
class time_zone;
} // namespace cctz
namespace doris {
class RowDescriptor;
class SlotDescriptor;
class TupleDescriptor;

namespace io {
struct IOContext;
} // namespace io
class VExprContext;
} // namespace doris

namespace doris {
namespace {

class GroupedDeleteRowsVisitor final : public IcebergPositionDeleteVisitor {
public:
    using DeleteRows = std::vector<int64_t>;
    using DeleteFile = phmap::parallel_flat_hash_map<
            std::string, std::unique_ptr<DeleteRows>, std::hash<std::string>, std::equal_to<>,
            std::allocator<std::pair<const std::string, std::unique_ptr<DeleteRows>>>, 8,
            std::mutex>;

    explicit GroupedDeleteRowsVisitor(DeleteFile* position_delete) : _position_delete(position_delete) {}

    Status visit(const std::string& file_path, int64_t pos) override {
        if (_position_delete == nullptr) {
            return Status::InvalidArgument("position delete map is null");
        }

        auto iter = _position_delete->find(file_path);
        DeleteRows* delete_rows = nullptr;
        if (iter == _position_delete->end()) {
            delete_rows = new DeleteRows;
            (*_position_delete)[file_path] = std::unique_ptr<DeleteRows>(delete_rows);
        } else {
            delete_rows = iter->second.get();
        }
        delete_rows->push_back(pos);
        return Status::OK();
    }

private:
    DeleteFile* _position_delete;
};

} // namespace

const std::string IcebergOrcReader::ICEBERG_ORC_ATTRIBUTE = "iceberg.id";

// ============================================================================
// IcebergParquetReader: on_before_init_columns (Parquet-specific schema matching)
// ============================================================================
Status IcebergParquetReader::on_before_init_columns(
        const std::vector<ColumnDescriptor>& column_descs, std::vector<std::string>& column_names,
        std::shared_ptr<TableSchemaChangeHelper::Node>& table_info_node,
        std::set<uint64_t>& column_ids, std::set<uint64_t>& filter_column_ids) {
    _file_format = Fileformat::PARQUET;

    // Early detection of $row_id column (needed before _init_row_filters).
    // Also register it as synthesized so RowGroupReader builds row positions.
    for (const auto& desc : column_descs) {
        if (desc.name == BeConsts::ICEBERG_ROWID_COL) {
            _need_row_id_column = true;
            this->_lazy_read_ctx.predicate_synthesized_col_names.push_back(desc.name);
            LOG(INFO) << "[IcebergDebug] Parquet on_before_init_columns: detected $row_id, "
                      << "added to predicate_synthesized_col_names, size="
                      << this->_lazy_read_ctx.predicate_synthesized_col_names.size();
            break;
        }
    }

    // First call parent default to extract column names from column_descs
    // (we'll override column_names with our own logic)
    column_names.clear();
    for (const auto& desc : column_descs) {
        if (desc.category == ColumnCategory::REGULAR || desc.category == ColumnCategory::INTERNAL) {
            column_names.push_back(desc.name);
        }
    }

    // Get file metadata schema (available because _open_file() already ran)
    const FieldDescriptor* field_desc = nullptr;
    RETURN_IF_ERROR(this->get_file_metadata_schema(&field_desc));
    DCHECK(field_desc != nullptr);

    // Build table_info_node by field_id or name matching
    if (!get_scan_params().__isset.history_schema_info ||
        get_scan_params().history_schema_info.empty()) [[unlikely]] {
        RETURN_IF_ERROR(BuildTableInfoUtil::by_parquet_name(get_tuple_descriptor(), *field_desc,
                                                            table_info_node));
    } else {
        bool exist_field_id = true;
        RETURN_IF_ERROR(BuildTableInfoUtil::by_parquet_field_id(
                get_scan_params().history_schema_info.front().root_field, *field_desc,
                table_info_node, exist_field_id));
        if (!exist_field_id) {
            RETURN_IF_ERROR(BuildTableInfoUtil::by_parquet_name(get_tuple_descriptor(), *field_desc,
                                                                table_info_node));
        }
    }

    _all_required_col_names = column_names;

    // Create column IDs from field descriptor
    auto column_id_result = _create_column_ids(field_desc, get_tuple_descriptor());
    column_ids = std::move(column_id_result.column_ids);
    filter_column_ids = std::move(column_id_result.filter_column_ids);

    // Process delete files (must happen before _do_init_reader so expand col IDs are included)
    RETURN_IF_ERROR(_init_row_filters());

    // Add expand column IDs for equality delete
    for (int i = 0; i < field_desc->size(); ++i) {
        auto field_schema = field_desc->get_column(i);
        std::string col_name = field_schema->name;
        if (std::find(_expand_col_names.begin(), _expand_col_names.end(), col_name) !=
            _expand_col_names.end()) {
            column_ids.insert(field_schema->get_column_id());
        }
    }

    // Add expand column names to column_names (for _do_init_reader)
    for (const auto& col_name : _expand_col_names) {
        column_names.push_back(col_name);
    }

    // Enable group filtering for Iceberg
    _filter_groups = true;

    return Status::OK();
}

// ============================================================================
// IcebergParquetReader: _create_column_ids
// ============================================================================
ColumnIdResult IcebergParquetReader::_create_column_ids(const FieldDescriptor* field_desc,
                                                        const TupleDescriptor* tuple_descriptor) {
    auto* mutable_field_desc = const_cast<FieldDescriptor*>(field_desc);
    mutable_field_desc->assign_ids();

    std::unordered_map<int, const FieldSchema*> iceberg_id_to_field_schema_map;
    for (int i = 0; i < field_desc->size(); ++i) {
        auto field_schema = field_desc->get_column(i);
        if (!field_schema) continue;
        int iceberg_id = field_schema->field_id;
        iceberg_id_to_field_schema_map[iceberg_id] = field_schema;
    }

    std::set<uint64_t> column_ids;
    std::set<uint64_t> filter_column_ids;

    auto process_access_paths = [](const FieldSchema* parquet_field,
                                   const std::vector<TColumnAccessPath>& access_paths,
                                   std::set<uint64_t>& out_ids) {
        process_nested_access_paths(
                parquet_field, access_paths, out_ids,
                [](const FieldSchema* field) { return field->get_column_id(); },
                [](const FieldSchema* field) { return field->get_max_column_id(); },
                IcebergParquetNestedColumnUtils::extract_nested_column_ids);
    };

    for (const auto* slot : tuple_descriptor->slots()) {
        auto it = iceberg_id_to_field_schema_map.find(slot->col_unique_id());
        if (it == iceberg_id_to_field_schema_map.end()) {
            continue;
        }
        auto field_schema = it->second;

        if ((slot->col_type() != TYPE_STRUCT && slot->col_type() != TYPE_ARRAY &&
             slot->col_type() != TYPE_MAP)) {
            column_ids.insert(field_schema->column_id);
            if (slot->is_predicate()) {
                filter_column_ids.insert(field_schema->column_id);
            }
            continue;
        }

        const auto& all_access_paths = slot->all_access_paths();
        process_access_paths(field_schema, all_access_paths, column_ids);

        const auto& predicate_access_paths = slot->predicate_access_paths();
        if (!predicate_access_paths.empty()) {
            process_access_paths(field_schema, predicate_access_paths, filter_column_ids);
        }
    }
    return ColumnIdResult(std::move(column_ids), std::move(filter_column_ids));
}

// ============================================================================
// IcebergParquetReader: _read_position_delete_file
// ============================================================================
Status IcebergParquetReader::_read_position_delete_file(const TFileRangeDesc* delete_range,
                                                        DeleteFile* position_delete) {
    GroupedDeleteRowsVisitor visitor(position_delete);
    IcebergDeleteFileReaderOptions options;
    options.state = get_state();
    options.profile = get_profile();
    options.scan_params = &get_scan_params();
    options.io_ctx = get_io_ctx();
    options.meta_cache = _meta_cache;
    options.fs_name = &delete_range->fs_name;
    options.batch_size = READ_DELETE_FILE_BATCH_SIZE;
    TIcebergDeleteFileDesc delete_file;
    delete_file.path = delete_range->path;
    return read_iceberg_position_delete_file(delete_file, options, &visitor);
};

// ============================================================================
// IcebergOrcReader: on_before_init_columns (ORC-specific schema matching)
// ============================================================================
Status IcebergOrcReader::on_before_init_columns(
        const std::vector<ColumnDescriptor>& column_descs, std::vector<std::string>& column_names,
        std::shared_ptr<TableSchemaChangeHelper::Node>& table_info_node,
        std::set<uint64_t>& column_ids, std::set<uint64_t>& filter_column_ids) {
    _file_format = Fileformat::ORC;

    // Early detection of $row_id column (needed before _init_row_filters).
    // Also register it as synthesized so OrcReader builds row positions.
    for (const auto& desc : column_descs) {
        if (desc.name == BeConsts::ICEBERG_ROWID_COL) {
            _need_row_id_column = true;
            this->_lazy_read_ctx.synthesized_col_names.push_back(desc.name);
            break;
        }
    }

    // Extract column names from column_descs
    column_names.clear();
    for (const auto& desc : column_descs) {
        if (desc.category == ColumnCategory::REGULAR || desc.category == ColumnCategory::INTERNAL) {
            column_names.push_back(desc.name);
        }
    }

    // Get ORC file type (available because _create_file_reader() already ran)
    const orc::Type* orc_type_ptr = nullptr;
    RETURN_IF_ERROR(this->get_file_type(&orc_type_ptr));
    _all_required_col_names = column_names;

    // Build table_info_node by field_id or name matching
    if (!get_scan_params().__isset.history_schema_info ||
        get_scan_params().history_schema_info.empty()) [[unlikely]] {
        RETURN_IF_ERROR(BuildTableInfoUtil::by_orc_name(get_tuple_descriptor(), orc_type_ptr,
                                                        table_info_node));
    } else {
        bool exist_field_id = true;
        RETURN_IF_ERROR(BuildTableInfoUtil::by_orc_field_id(
                get_scan_params().history_schema_info.front().root_field, orc_type_ptr,
                ICEBERG_ORC_ATTRIBUTE, table_info_node, exist_field_id));
        if (!exist_field_id) {
            RETURN_IF_ERROR(BuildTableInfoUtil::by_orc_name(get_tuple_descriptor(), orc_type_ptr,
                                                            table_info_node));
        }
    }

    // Create column IDs from ORC type
    auto column_id_result = _create_column_ids(orc_type_ptr, get_tuple_descriptor());
    column_ids = std::move(column_id_result.column_ids);
    filter_column_ids = std::move(column_id_result.filter_column_ids);

    // Process delete files (must happen before _do_init_reader so expand col IDs are included)
    RETURN_IF_ERROR(_init_row_filters());

    // Add expand column IDs for equality delete
    for (uint64_t i = 0; i < orc_type_ptr->getSubtypeCount(); ++i) {
        const orc::Type* sub_type = orc_type_ptr->getSubtype(i);
        std::string col_name = orc_type_ptr->getFieldName(i);
        if (std::find(_expand_col_names.begin(), _expand_col_names.end(), col_name) !=
            _expand_col_names.end()) {
            column_ids.insert(sub_type->getColumnId());
        }
    }

    // Add expand column names to column_names (for _do_init_reader)
    for (const auto& col_name : _expand_col_names) {
        column_names.push_back(col_name);
    }

    return Status::OK();
}

// ============================================================================
// IcebergOrcReader: _create_column_ids
// ============================================================================
ColumnIdResult IcebergOrcReader::_create_column_ids(const orc::Type* orc_type,
                                                    const TupleDescriptor* tuple_descriptor) {
    std::unordered_map<int, const orc::Type*> iceberg_id_to_orc_type_map;
    for (uint64_t i = 0; i < orc_type->getSubtypeCount(); ++i) {
        auto orc_sub_type = orc_type->getSubtype(i);
        if (!orc_sub_type) continue;
        if (!orc_sub_type->hasAttributeKey(ICEBERG_ORC_ATTRIBUTE)) {
            continue;
        }
        int iceberg_id = std::stoi(orc_sub_type->getAttributeValue(ICEBERG_ORC_ATTRIBUTE));
        iceberg_id_to_orc_type_map[iceberg_id] = orc_sub_type;
    }

    std::set<uint64_t> column_ids;
    std::set<uint64_t> filter_column_ids;

    auto process_access_paths = [](const orc::Type* orc_field,
                                   const std::vector<TColumnAccessPath>& access_paths,
                                   std::set<uint64_t>& out_ids) {
        process_nested_access_paths(
                orc_field, access_paths, out_ids,
                [](const orc::Type* type) { return type->getColumnId(); },
                [](const orc::Type* type) { return type->getMaximumColumnId(); },
                IcebergOrcNestedColumnUtils::extract_nested_column_ids);
    };

    for (const auto* slot : tuple_descriptor->slots()) {
        auto it = iceberg_id_to_orc_type_map.find(slot->col_unique_id());
        if (it == iceberg_id_to_orc_type_map.end()) {
            continue;
        }
        const orc::Type* orc_field = it->second;

        if ((slot->col_type() != TYPE_STRUCT && slot->col_type() != TYPE_ARRAY &&
             slot->col_type() != TYPE_MAP)) {
            column_ids.insert(orc_field->getColumnId());
            if (slot->is_predicate()) {
                filter_column_ids.insert(orc_field->getColumnId());
            }
            continue;
        }

        const auto& all_access_paths = slot->all_access_paths();
        process_access_paths(orc_field, all_access_paths, column_ids);

        const auto& predicate_access_paths = slot->predicate_access_paths();
        if (!predicate_access_paths.empty()) {
            process_access_paths(orc_field, predicate_access_paths, filter_column_ids);
        }
    }

    return ColumnIdResult(std::move(column_ids), std::move(filter_column_ids));
}

// ============================================================================
// IcebergOrcReader: _read_position_delete_file
// ============================================================================
Status IcebergOrcReader::_read_position_delete_file(const TFileRangeDesc* delete_range,
                                                    DeleteFile* position_delete) {
    GroupedDeleteRowsVisitor visitor(position_delete);
    IcebergDeleteFileReaderOptions options;
    options.state = get_state();
    options.profile = get_profile();
    options.scan_params = &get_scan_params();
    options.io_ctx = get_io_ctx();
    options.meta_cache = _meta_cache;
    options.fs_name = &delete_range->fs_name;
    options.batch_size = READ_DELETE_FILE_BATCH_SIZE;
    TIcebergDeleteFileDesc delete_file;
    delete_file.path = delete_range->path;
    return read_iceberg_position_delete_file(delete_file, options, &visitor);
}

#include "common/compile_check_end.h"
} // namespace doris
