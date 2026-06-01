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
#include "format/table/iceberg/iceberg_orc_nested_column_utils.h"
#include "format/table/iceberg/iceberg_parquet_nested_column_utils.h"
#include "format/table/nested_column_access_helper.h"
#include "format/table/table_schema_change_helper.h"
#include "runtime/runtime_state.h"
#include "util/coding.h"

namespace cctz {
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
const std::string IcebergOrcReader::ICEBERG_ORC_ATTRIBUTE = "iceberg.id";

bool IcebergTableReader::_is_fully_dictionary_encoded(
        const tparquet::ColumnMetaData& column_metadata) {
    const auto is_dictionary_encoding = [](tparquet::Encoding::type encoding) {
        return encoding == tparquet::Encoding::PLAIN_DICTIONARY ||
               encoding == tparquet::Encoding::RLE_DICTIONARY;
    };
    const auto is_data_page = [](tparquet::PageType::type page_type) {
        return page_type == tparquet::PageType::DATA_PAGE ||
               page_type == tparquet::PageType::DATA_PAGE_V2;
    };
    const auto is_level_encoding = [](tparquet::Encoding::type encoding) {
        return encoding == tparquet::Encoding::RLE || encoding == tparquet::Encoding::BIT_PACKED;
    };

    // A column chunk may have a dictionary page but still contain plain-encoded data pages.
    // Only treat it as dictionary-coded when all data pages are dictionary encoded.
    if (column_metadata.__isset.encoding_stats) {
        bool has_data_page_stats = false;
        for (const tparquet::PageEncodingStats& enc_stat : column_metadata.encoding_stats) {
            if (is_data_page(enc_stat.page_type) && enc_stat.count > 0) {
                has_data_page_stats = true;
                if (!is_dictionary_encoding(enc_stat.encoding)) {
                    return false;
                }
            }
        }
        if (has_data_page_stats) {
            return true;
        }
    }

    bool has_dict_encoding = false;
    bool has_nondict_encoding = false;
    for (const tparquet::Encoding::type& encoding : column_metadata.encodings) {
        if (is_dictionary_encoding(encoding)) {
            has_dict_encoding = true;
        }

        if (!is_dictionary_encoding(encoding) && !is_level_encoding(encoding)) {
            has_nondict_encoding = true;
            break;
        }
    }
    if (!has_dict_encoding || has_nondict_encoding) {
        return false;
    }

    return true;
}

// ============================================================================
// IcebergParquetReader: on_before_init_reader (Parquet-specific schema matching)
// ============================================================================
Status IcebergParquetReader::on_before_init_reader(ReaderInitContext* ctx) {
    _column_descs = ctx->column_descs;
    _fill_col_name_to_block_idx = ctx->col_name_to_block_idx;
    _file_format = Fileformat::PARQUET;

    // Get file metadata schema first (available because _open_file() already ran)
    const FieldDescriptor* field_desc = nullptr;
    RETURN_IF_ERROR(this->get_file_metadata_schema(&field_desc));
    DCHECK(field_desc != nullptr);

    // Build table_info_node by field_id or name matching.
    // This must happen BEFORE column classification so we can use children_column_exists
    // to check if a column exists in the file (by field ID, not name).
    if (!get_scan_params().__isset.history_schema_info ||
        get_scan_params().history_schema_info.empty()) [[unlikely]] {
        RETURN_IF_ERROR(BuildTableInfoUtil::by_parquet_name(ctx->tuple_descriptor, *field_desc,
                                                            ctx->table_info_node));
    } else {
        bool exist_field_id = true;
        RETURN_IF_ERROR(BuildTableInfoUtil::by_parquet_field_id(
                get_scan_params().history_schema_info.front().root_field, *field_desc,
                ctx->table_info_node, exist_field_id));
        if (!exist_field_id) {
            RETURN_IF_ERROR(BuildTableInfoUtil::by_parquet_name(ctx->tuple_descriptor, *field_desc,
                                                                ctx->table_info_node));
        }
    }

    std::unordered_set<std::string> partition_col_names;
    if (ctx->range->__isset.columns_from_path_keys) {
        partition_col_names.insert(ctx->range->columns_from_path_keys.begin(),
                                   ctx->range->columns_from_path_keys.end());
    }

    // Single pass: classify columns, detect $row_id, handle partition fallback.
    bool has_partition_from_path = false;
    for (auto& desc : *ctx->column_descs) {
        if (desc.category == ColumnCategory::SYNTHESIZED) {
            if (desc.name == BeConsts::ICEBERG_ROWID_COL) {
                this->register_synthesized_column_handler(
                        BeConsts::ICEBERG_ROWID_COL, [this](Block* block, size_t rows) -> Status {
                            return _fill_iceberg_row_id(block, rows);
                        });
                continue;
            } else if (desc.name.starts_with(BeConsts::GLOBAL_ROWID_COL)) {
                auto topn_row_id_column_iter = _create_topn_row_id_column_iterator();
                this->register_synthesized_column_handler(
                        desc.name,
                        [iter = std::move(topn_row_id_column_iter), this, &desc](
                                Block* block, size_t rows) -> Status {
                            return fill_topn_row_id(iter, desc.name, block, rows);
                        });
                continue;
            }
        } else if (desc.category == ColumnCategory::REGULAR) {
            // Partition fallback: if column is a partition key and NOT in the file
            // (checked via field ID matching in table_info_node), read from path instead.
            if (partition_col_names.contains(desc.name) &&
                !ctx->table_info_node->children_column_exists(desc.name)) {
                if (config::enable_iceberg_partition_column_fallback) {
                    desc.category = ColumnCategory::PARTITION_KEY;
                    has_partition_from_path = true;
                    continue;
                }
            }
            ctx->column_names.push_back(desc.name);
        } else if (desc.category == ColumnCategory::GENERATED) {
            _init_row_lineage_columns();
            if (desc.name == ROW_LINEAGE_ROW_ID) {
                ctx->column_names.push_back(desc.name);
                this->register_generated_column_handler(
                        ROW_LINEAGE_ROW_ID, [this](Block* block, size_t rows) -> Status {
                            return _fill_row_lineage_row_id(block, rows);
                        });
                continue;
            } else if (desc.name == ROW_LINEAGE_LAST_UPDATED_SEQ_NUMBER) {
                ctx->column_names.push_back(desc.name);
                this->register_generated_column_handler(
                        ROW_LINEAGE_LAST_UPDATED_SEQ_NUMBER,
                        [this](Block* block, size_t rows) -> Status {
                            return _fill_row_lineage_last_updated_sequence_number(block, rows);
                        });
                continue;
            }
        }
    }

    // Set up partition value extraction if any partition columns need filling from path
    if (has_partition_from_path) {
        RETURN_IF_ERROR(_extract_partition_values(*ctx->range, ctx->tuple_descriptor,
                                                  _fill_partition_values));
    }

    _all_required_col_names = ctx->column_names;

    // Create column IDs from field descriptor
    auto column_id_result = _create_column_ids(field_desc, ctx->tuple_descriptor);
    ctx->column_ids = std::move(column_id_result.column_ids);
    ctx->filter_column_ids = std::move(column_id_result.filter_column_ids);

    // Build field_id -> block_column_name mapping for equality delete filtering.
    // This was previously done in init_reader() column matching (pre-CRTP refactoring).
    for (const auto* slot : ctx->tuple_descriptor->slots()) {
        _id_to_block_column_name.emplace(slot->col_unique_id(), slot->col_name());
    }

    // Process delete files (must happen before _do_init_reader so expand col IDs are included)
    RETURN_IF_ERROR(_init_row_filters());

    // Add expand column IDs for equality delete and remap expand column names
    // to match master's behavior:
    // - Use field_id to find the actual file column name in Parquet schema
    // - Prefix with __equality_delete_column__ to avoid name conflicts
    // - Correctly map table_col_name → file_col_name in table_info_node
    const static std::string EQ_DELETE_PRE = "__equality_delete_column__";
    std::unordered_map<int, std::string> field_id_to_file_col_name;
    for (int i = 0; i < field_desc->size(); ++i) {
        auto field_schema = field_desc->get_column(i);
        if (field_schema) {
            field_id_to_file_col_name[field_schema->field_id] = field_schema->name;
        }
    }

    // Rebuild _expand_col_names with proper file-column-based names
    std::vector<std::string> new_expand_col_names;
    for (size_t i = 0; i < _expand_col_names.size(); ++i) {
        const auto& old_name = _expand_col_names[i];
        // Find the field_id for this expand column
        int field_id = -1;
        for (auto& [fid, name] : _id_to_block_column_name) {
            if (name == old_name) {
                field_id = fid;
                break;
            }
        }

        std::string file_col_name = old_name;
        auto it = field_id_to_file_col_name.find(field_id);
        if (it != field_id_to_file_col_name.end()) {
            file_col_name = it->second;
        }

        std::string table_col_name = EQ_DELETE_PRE + file_col_name;

        // Update _id_to_block_column_name
        if (field_id >= 0) {
            _id_to_block_column_name[field_id] = table_col_name;
        }

        // Update _expand_columns name
        if (i < _expand_columns.size()) {
            _expand_columns[i].name = table_col_name;
        }

        new_expand_col_names.push_back(table_col_name);

        // Add column IDs
        if (it != field_id_to_file_col_name.end()) {
            for (int j = 0; j < field_desc->size(); ++j) {
                auto field_schema = field_desc->get_column(j);
                if (field_schema && field_schema->field_id == field_id) {
                    ctx->column_ids.insert(field_schema->get_column_id());
                    break;
                }
            }
        }

        // Register in table_info_node: table_col_name → file_col_name
        ctx->column_names.push_back(table_col_name);
        ctx->table_info_node->add_children(table_col_name, file_col_name,
                                           TableSchemaChangeHelper::ConstNode::get_instance());
    }
    _expand_col_names = std::move(new_expand_col_names);

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
    ParquetReader parquet_delete_reader(get_profile(), get_scan_params(), *delete_range,
                                        READ_DELETE_FILE_BATCH_SIZE, &get_state()->timezone_obj(),
                                        get_io_ctx(), get_state(), _meta_cache);
    // The delete file range has size=-1 (read whole file). We must disable
    // row group filtering before init; otherwise _do_init_reader returns EndOfFile
    // when _filter_groups && _range_size < 0.
    ParquetInitContext delete_ctx;
    delete_ctx.filter_groups = false;
    delete_ctx.column_names = delete_file_col_names;
    delete_ctx.col_name_to_block_idx =
            const_cast<std::unordered_map<std::string, uint32_t>*>(&DELETE_COL_NAME_TO_BLOCK_IDX);
    RETURN_IF_ERROR(parquet_delete_reader.init_reader(&delete_ctx));

    const tparquet::FileMetaData* meta_data = parquet_delete_reader.get_meta_data();
    bool dictionary_coded = true;
    for (const auto& row_group : meta_data->row_groups) {
        const auto& column_chunk = row_group.columns[ICEBERG_FILE_PATH_INDEX];
        if (!(column_chunk.__isset.meta_data && has_dict_page(column_chunk.meta_data))) {
            dictionary_coded = false;
            break;
        }
    }
    DataTypePtr data_type_file_path {new DataTypeString};
    DataTypePtr data_type_pos {new DataTypeInt64};
    bool eof = false;
    while (!eof) {
        Block block = {dictionary_coded
                               ? ColumnWithTypeAndName {ColumnDictI32::create(
                                                                FieldType::OLAP_FIELD_TYPE_VARCHAR),
                                                        data_type_file_path, ICEBERG_FILE_PATH}
                               : ColumnWithTypeAndName {data_type_file_path, ICEBERG_FILE_PATH},

                       {data_type_pos, ICEBERG_ROW_POS}};
        size_t read_rows = 0;
        RETURN_IF_ERROR(parquet_delete_reader.get_next_block(&block, &read_rows, &eof));

        if (read_rows <= 0) {
            break;
        }
        _gen_position_delete_file_range(block, position_delete, read_rows, dictionary_coded);
    }
    return Status::OK();
};

// ============================================================================
// IcebergOrcReader: on_before_init_reader (ORC-specific schema matching)
// ============================================================================
Status IcebergOrcReader::on_before_init_reader(ReaderInitContext* ctx) {
    _column_descs = ctx->column_descs;
    _fill_col_name_to_block_idx = ctx->col_name_to_block_idx;
    _file_format = Fileformat::ORC;

    // Get ORC file type first (available because _create_file_reader() already ran)
    const orc::Type* orc_type_ptr = nullptr;
    RETURN_IF_ERROR(this->get_file_type(&orc_type_ptr));

    // Build table_info_node by field_id or name matching.
    // This must happen BEFORE column classification so we can use children_column_exists
    // to check if a column exists in the file (by field ID, not name).
    if (!get_scan_params().__isset.history_schema_info ||
        get_scan_params().history_schema_info.empty()) [[unlikely]] {
        RETURN_IF_ERROR(BuildTableInfoUtil::by_orc_name(ctx->tuple_descriptor, orc_type_ptr,
                                                        ctx->table_info_node));
    } else {
        bool exist_field_id = true;
        RETURN_IF_ERROR(BuildTableInfoUtil::by_orc_field_id(
                get_scan_params().history_schema_info.front().root_field, orc_type_ptr,
                ICEBERG_ORC_ATTRIBUTE, ctx->table_info_node, exist_field_id));
        if (!exist_field_id) {
            RETURN_IF_ERROR(BuildTableInfoUtil::by_orc_name(ctx->tuple_descriptor, orc_type_ptr,
                                                            ctx->table_info_node));
        }
    }

    std::unordered_set<std::string> partition_col_names;
    if (ctx->range->__isset.columns_from_path_keys) {
        partition_col_names.insert(ctx->range->columns_from_path_keys.begin(),
                                   ctx->range->columns_from_path_keys.end());
    }

    // Single pass: classify columns, detect $row_id, handle partition fallback.
    bool has_partition_from_path = false;
    for (auto& desc : *ctx->column_descs) {
        if (desc.category == ColumnCategory::SYNTHESIZED) {
            if (desc.name == BeConsts::ICEBERG_ROWID_COL) {
                this->register_synthesized_column_handler(
                        BeConsts::ICEBERG_ROWID_COL, [this](Block* block, size_t rows) -> Status {
                            return _fill_iceberg_row_id(block, rows);
                        });
                continue;
            } else if (desc.name.starts_with(BeConsts::GLOBAL_ROWID_COL)) {
                auto topn_row_id_column_iter = _create_topn_row_id_column_iterator();
                this->register_synthesized_column_handler(
                        desc.name,
                        [iter = std::move(topn_row_id_column_iter), this, &desc](
                                Block* block, size_t rows) -> Status {
                            return fill_topn_row_id(iter, desc.name, block, rows);
                        });
                continue;
            }
        } else if (desc.category == ColumnCategory::REGULAR) {
            // Partition fallback: if column is a partition key and NOT in the file
            // (checked via field ID matching in table_info_node), read from path instead.
            if (partition_col_names.contains(desc.name) &&
                !ctx->table_info_node->children_column_exists(desc.name)) {
                if (config::enable_iceberg_partition_column_fallback) {
                    desc.category = ColumnCategory::PARTITION_KEY;
                    has_partition_from_path = true;
                    continue;
                }
            }
            ctx->column_names.push_back(desc.name);
        } else if (desc.category == ColumnCategory::GENERATED) {
            _init_row_lineage_columns();
            if (desc.name == ROW_LINEAGE_ROW_ID) {
                ctx->column_names.push_back(desc.name);
                this->register_generated_column_handler(
                        ROW_LINEAGE_ROW_ID, [this](Block* block, size_t rows) -> Status {
                            return _fill_row_lineage_row_id(block, rows);
                        });
                continue;
            } else if (desc.name == ROW_LINEAGE_LAST_UPDATED_SEQ_NUMBER) {
                ctx->column_names.push_back(desc.name);
                this->register_generated_column_handler(
                        ROW_LINEAGE_LAST_UPDATED_SEQ_NUMBER,
                        [this](Block* block, size_t rows) -> Status {
                            return _fill_row_lineage_last_updated_sequence_number(block, rows);
                        });
                continue;
            }
        }
    }

    if (has_partition_from_path) {
        RETURN_IF_ERROR(_extract_partition_values(*ctx->range, ctx->tuple_descriptor,
                                                  _fill_partition_values));
    }

    _all_required_col_names = ctx->column_names;

    // Create column IDs from ORC type
    auto column_id_result = _create_column_ids(orc_type_ptr, ctx->tuple_descriptor);
    ctx->column_ids = std::move(column_id_result.column_ids);
    ctx->filter_column_ids = std::move(column_id_result.filter_column_ids);

    // Build field_id -> block_column_name mapping for equality delete filtering.
    for (const auto* slot : ctx->tuple_descriptor->slots()) {
        _id_to_block_column_name.emplace(slot->col_unique_id(), slot->col_name());
    }

    // Process delete files (must happen before _do_init_reader so expand col IDs are included)
    RETURN_IF_ERROR(_init_row_filters());

    // Add expand column IDs for equality delete and remap expand column names
    // (matching master's behavior with __equality_delete_column__ prefix)
    const static std::string EQ_DELETE_PRE = "__equality_delete_column__";
    std::unordered_map<int, std::string> field_id_to_file_col_name;
    for (uint64_t i = 0; i < orc_type_ptr->getSubtypeCount(); ++i) {
        std::string col_name = orc_type_ptr->getFieldName(i);
        const orc::Type* sub_type = orc_type_ptr->getSubtype(i);
        if (sub_type->hasAttributeKey(ICEBERG_ORC_ATTRIBUTE)) {
            int fid = std::stoi(sub_type->getAttributeValue(ICEBERG_ORC_ATTRIBUTE));
            field_id_to_file_col_name[fid] = col_name;
        }
    }

    std::vector<std::string> new_expand_col_names;
    for (size_t i = 0; i < _expand_col_names.size(); ++i) {
        const auto& old_name = _expand_col_names[i];
        int field_id = -1;
        for (auto& [fid, name] : _id_to_block_column_name) {
            if (name == old_name) {
                field_id = fid;
                break;
            }
        }

        std::string file_col_name = old_name;
        auto it = field_id_to_file_col_name.find(field_id);
        if (it != field_id_to_file_col_name.end()) {
            file_col_name = it->second;
        }

        std::string table_col_name = EQ_DELETE_PRE + file_col_name;

        if (field_id >= 0) {
            _id_to_block_column_name[field_id] = table_col_name;
        }
        if (i < _expand_columns.size()) {
            _expand_columns[i].name = table_col_name;
        }
        new_expand_col_names.push_back(table_col_name);

        // Add column IDs
        if (it != field_id_to_file_col_name.end()) {
            for (uint64_t j = 0; j < orc_type_ptr->getSubtypeCount(); ++j) {
                const orc::Type* sub_type = orc_type_ptr->getSubtype(j);
                if (orc_type_ptr->getFieldName(j) == file_col_name) {
                    ctx->column_ids.insert(sub_type->getColumnId());
                    break;
                }
            }
        }

        ctx->column_names.push_back(table_col_name);
        ctx->table_info_node->add_children(table_col_name, file_col_name,
                                           TableSchemaChangeHelper::ConstNode::get_instance());
    }
    _expand_col_names = std::move(new_expand_col_names);

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
    OrcReader orc_delete_reader(get_profile(), get_state(), get_scan_params(), *delete_range,
                                READ_DELETE_FILE_BATCH_SIZE, get_state()->timezone(), get_io_ctx(),
                                _meta_cache);
    OrcInitContext delete_ctx;
    delete_ctx.column_names = delete_file_col_names;
    delete_ctx.col_name_to_block_idx =
            const_cast<std::unordered_map<std::string, uint32_t>*>(&DELETE_COL_NAME_TO_BLOCK_IDX);
    RETURN_IF_ERROR(orc_delete_reader.init_reader(&delete_ctx));

    bool eof = false;
    DataTypePtr data_type_file_path {new DataTypeString};
    DataTypePtr data_type_pos {new DataTypeInt64};
    while (!eof) {
        Block block = {{data_type_file_path, ICEBERG_FILE_PATH}, {data_type_pos, ICEBERG_ROW_POS}};

        size_t read_rows = 0;
        RETURN_IF_ERROR(orc_delete_reader.get_next_block(&block, &read_rows, &eof));

        _gen_position_delete_file_range(block, position_delete, read_rows, false);
    }
    return Status::OK();
}

} // namespace doris
