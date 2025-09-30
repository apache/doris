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

#include "hive_reader.h"

#include <vector>

#include "common/status.h"
#include "runtime/runtime_state.h"
#include "vec/exec/format/table/hive/hive_orc_nested_column_utils.h"
#include "vec/exec/format/table/hive/hive_parquet_nested_column_utils.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

Status HiveReader::get_next_block_inner(Block* block, size_t* read_rows, bool* eof) {
    RETURN_IF_ERROR(_file_format_reader->get_next_block(block, read_rows, eof));
    return Status::OK();
};

Status HiveOrcReader::init_reader(
        const std::vector<std::string>& read_table_col_names,
        const std::unordered_map<std::string, ColumnValueRangeType>* table_col_name_to_value_range,
        const VExprContextSPtrs& conjuncts, const TupleDescriptor* tuple_descriptor,
        const RowDescriptor* row_descriptor,
        const VExprContextSPtrs* not_single_slot_filter_conjuncts,
        const std::unordered_map<int, VExprContextSPtrs>* slot_id_to_filter_conjuncts) {
    auto* orc_reader = static_cast<OrcReader*>(_file_format_reader.get());

    const orc::Type* orc_type_ptr = nullptr;
    RETURN_IF_ERROR(orc_reader->get_file_type(&orc_type_ptr));
    bool is_hive_col_name = OrcReader::is_hive1_col_name(orc_type_ptr);

    if (_state->query_options().hive_orc_use_column_names && !is_hive_col_name) {
        // Directly use the table column name to match the file column name, but pay attention to the case issue.
        RETURN_IF_ERROR(BuildTableInfoUtil::by_orc_name(tuple_descriptor, orc_type_ptr,
                                                        table_info_node_ptr, _is_file_slot));
    } else {
        // hive1 / use index
        std::map<std::string, const SlotDescriptor*> slot_map; // table_name to slot
        for (const auto& slot : tuple_descriptor->slots()) {
            slot_map.emplace(slot->col_name(), slot);
        }

        // For top-level columns, use indexes to match, and for sub-columns, still use name to match columns.
        for (size_t idx = 0; idx < _params.column_idxs.size(); idx++) {
            auto table_column_name = read_table_col_names[idx];
            auto file_index = _params.column_idxs[idx];

            if (file_index >= orc_type_ptr->getSubtypeCount()) {
                table_info_node_ptr->add_not_exist_children(table_column_name);
            } else {
                auto field_node = std::make_shared<Node>();
                // For sub-columns, still use name to match columns.
                RETURN_IF_ERROR(BuildTableInfoUtil::by_orc_name(
                        slot_map[table_column_name]->type(), orc_type_ptr->getSubtype(file_index),
                        field_node));
                table_info_node_ptr->add_children(
                        table_column_name, orc_type_ptr->getFieldName(file_index), field_node);
            }
            slot_map.erase(table_column_name);
        }
        for (const auto& [partition_col_name, _] : slot_map) {
            table_info_node_ptr->add_not_exist_children(partition_col_name);
        }
    }

    auto column_id_result = ColumnIdResult();
    if (_state->query_options().hive_orc_use_column_names && !is_hive_col_name) {
        column_id_result = _create_column_ids_and_names(orc_type_ptr, tuple_descriptor);
    } else {
        column_id_result =
                _create_column_ids_and_names_by_top_level_col_index(orc_type_ptr, tuple_descriptor);
    }

    // const auto& file_col_names = column_id_result.column_names;
    const auto& column_ids = column_id_result.column_ids;

    return orc_reader->init_reader(&read_table_col_names, table_col_name_to_value_range, conjuncts,
                                   false, tuple_descriptor, row_descriptor,
                                   not_single_slot_filter_conjuncts, slot_id_to_filter_conjuncts,
                                   table_info_node_ptr, column_ids);
}

ColumnIdResult HiveOrcReader::_create_column_ids_and_names(
        const orc::Type* orc_type, const TupleDescriptor* tuple_descriptor) {
    std::set<uint64_t> column_ids;
    std::shared_ptr<TableSchemaChangeHelper::Node> schema_node = nullptr;

    if (!orc_type) {
        return ColumnIdResult(std::move(column_ids), schema_node);
    }

    // First, assign column IDs to the field descriptor
    // auto* mutable_field_desc = const_cast<FieldDescriptor*>(field_desc);
    // mutable_field_desc->assign_ids();

    // Create a mapping from iceberg field_id to orc type for quick lookup - similar to create_iceberg_projected_layout
    // std::map<int, const orc::Type*> iceberg_id_to_orc_type;
    // OrcNestedColumnUtils::_build_iceberg_id_mapping(orc_type, iceberg_id_to_orc_type);

    // Group column paths by field ID - inspired by create_iceberg_projected_layout's sequence processing
    std::unordered_map<std::string, std::vector<std::vector<std::string>>> paths_by_table_name;

    for (const auto* slot : tuple_descriptor->slots()) {
        // 假设 slot->column_access_paths() 返回 vector<vector<int>>，每个 path 代表一个 iceberg id 路径
        const auto& column_access_paths = slot->column_access_paths();

        // If column_paths is empty or has empty paths, we need the entire column
        if ((!column_access_paths.__isset.name_access_paths) ||
            column_access_paths.name_access_paths.empty() ||
            std::any_of(column_access_paths.name_access_paths.begin(),
                        column_access_paths.name_access_paths.end(),
                        [](const TColumnNameAccessPath& access_path) {
                            return access_path.path.empty();
                        })) {
            paths_by_table_name[slot->col_name()].push_back(std::vector<std::string>());
        } else {
            // Add all paths for this field ID
            for (const auto& name_access_path : column_access_paths.name_access_paths) {
                // // Convert string path to int path
                // std::vector<std::string> string_path;
                // // Simple conversion: convert string to int directly
                // for (const auto& path_element : name_access_path.path) {
                //     string_path.push_back(path_element);
                // }
                paths_by_table_name[slot->col_name()].push_back(name_access_path.path);
            }
        }
    }

    // Use the new merged efficient method
    auto result = HiveOrcNestedColumnUtils::_extract_schema_and_columns_efficiently(
            orc_type, paths_by_table_name);

    return ColumnIdResult(std::move(result.column_ids), result.schema_node);
}

ColumnIdResult HiveOrcReader::_create_column_ids_and_names_by_top_level_col_index(
        const orc::Type* orc_type, const TupleDescriptor* tuple_descriptor) {
    std::set<uint64_t> column_ids;
    std::shared_ptr<TableSchemaChangeHelper::Node> schema_node = nullptr;

    if (!orc_type) {
        return ColumnIdResult(std::move(column_ids), schema_node);
    }

    // First, assign column IDs to the field descriptor
    // auto* mutable_field_desc = const_cast<FieldDescriptor*>(field_desc);
    // mutable_field_desc->assign_ids();

    // Create a mapping from iceberg field_id to orc type for quick lookup - similar to create_iceberg_projected_layout
    // std::map<int, const orc::Type*> iceberg_id_to_orc_type;
    // OrcNestedColumnUtils::_build_iceberg_id_mapping(orc_type, iceberg_id_to_orc_type);

    // Group column paths by field ID - inspired by create_iceberg_projected_layout's sequence processing
    std::unordered_map<int, std::vector<std::vector<std::string>>> paths_by_table_index;

    for (const auto* slot : tuple_descriptor->slots()) {
        // 假设 slot->column_access_paths() 返回 vector<vector<int>>，每个 path 代表一个 iceberg id 路径
        const auto& column_access_paths = slot->column_access_paths();

        // If column_paths is empty or has empty paths, we need the entire column
        if ((!column_access_paths.__isset.name_access_paths) ||
            column_access_paths.name_access_paths.empty() ||
            std::any_of(column_access_paths.name_access_paths.begin(),
                        column_access_paths.name_access_paths.end(),
                        [](const TColumnNameAccessPath& access_path) {
                            return access_path.path.empty();
                        })) {
            paths_by_table_index[slot->col_pos()].push_back(std::vector<std::string>());
        } else {
            // Add all paths for this field ID
            for (const auto& name_access_path : column_access_paths.name_access_paths) {
                // // Convert string path to int path
                // std::vector<std::string> string_path;
                // // Simple conversion: convert string to int directly
                // for (const auto& path_element : name_access_path.path) {
                //     string_path.push_back(path_element);
                // }
                paths_by_table_index[slot->col_pos()].push_back(name_access_path.path);
            }
        }
    }

    // Use the new merged efficient method
    auto result = HiveOrcNestedColumnUtils::
            _extract_schema_and_columns_efficiently_by_top_level_col_index(orc_type,
                                                                           paths_by_table_index);

    return ColumnIdResult(std::move(result.column_ids), result.schema_node);
}

ColumnIdResult HiveOrcReader::_create_column_ids_and_names_by_index(
        const orc::Type* orc_type, const TupleDescriptor* tuple_descriptor) {
    std::set<uint64_t> column_ids;
    std::shared_ptr<TableSchemaChangeHelper::Node> schema_node = nullptr;

    if (!orc_type) {
        return ColumnIdResult(std::move(column_ids), schema_node);
    }

    // First, assign column IDs to the field descriptor
    // auto* mutable_field_desc = const_cast<FieldDescriptor*>(field_desc);
    // mutable_field_desc->assign_ids();

    // Create a mapping from iceberg field_id to orc type for quick lookup - similar to create_iceberg_projected_layout
    // std::map<int, const orc::Type*> iceberg_id_to_orc_type;
    // OrcNestedColumnUtils::_build_iceberg_id_mapping(orc_type, iceberg_id_to_orc_type);

    // Group column paths by field ID - inspired by create_iceberg_projected_layout's sequence processing
    std::unordered_map<int, std::vector<std::vector<int>>> paths_by_table_index;

    for (const auto* slot : tuple_descriptor->slots()) {
        // 假设 slot->column_access_paths() 返回 vector<vector<int>>，每个 path 代表一个 iceberg id 路径
        const auto& column_access_paths = slot->column_access_paths();

        // If column_paths is empty or has empty paths, we need the entire column
        if ((!column_access_paths.__isset.name_access_paths) ||
            column_access_paths.name_access_paths.empty() ||
            std::any_of(column_access_paths.name_access_paths.begin(),
                        column_access_paths.name_access_paths.end(),
                        [](const TColumnNameAccessPath& access_path) {
                            return access_path.path.empty();
                        })) {
            paths_by_table_index[slot->col_pos()].push_back(std::vector<int>());
        } else {
            // Add all paths for this field ID
            for (const auto& name_access_path : column_access_paths.name_access_paths) {
                // Convert string path to int path
                std::vector<int> int_path;
                // Simple conversion: convert string to int directly
                for (const auto& path_element : name_access_path.path) {
                    try {
                        // Convert string to int directly
                        int_path.push_back(std::stoi(path_element));
                    } catch (const std::exception& /* e */) {
                        // If conversion fails, use 0 as default
                        int_path.push_back(0);
                    }
                }
                paths_by_table_index[slot->col_pos()].push_back(int_path);
            }
        }
    }

    // Use the new merged efficient method
    auto result = HiveOrcNestedColumnUtils::_extract_schema_and_columns_efficiently_by_index(
            orc_type, paths_by_table_index);

    return ColumnIdResult(std::move(result.column_ids), result.schema_node);
}

Status HiveParquetReader::init_reader(
        const std::vector<std::string>& read_table_col_names,
        const std::unordered_map<std::string, ColumnValueRangeType>* table_col_name_to_value_range,
        const VExprContextSPtrs& conjuncts, const TupleDescriptor* tuple_descriptor,
        const RowDescriptor* row_descriptor,
        const std::unordered_map<std::string, int>* colname_to_slot_id,
        const VExprContextSPtrs* not_single_slot_filter_conjuncts,
        const std::unordered_map<int, VExprContextSPtrs>* slot_id_to_filter_conjuncts) {
    auto* parquet_reader = static_cast<ParquetReader*>(_file_format_reader.get());
    const FieldDescriptor* field_desc = nullptr;
    RETURN_IF_ERROR(parquet_reader->get_file_metadata_schema(&field_desc));
    DCHECK(field_desc != nullptr);

    if (_state->query_options().hive_parquet_use_column_names) {
        // Directly use the table column name to match the file column name, but pay attention to the case issue.
        RETURN_IF_ERROR(BuildTableInfoUtil::by_parquet_name(tuple_descriptor, *field_desc,
                                                            table_info_node_ptr, _is_file_slot));
    } else {                                                   // use idx
        std::map<std::string, const SlotDescriptor*> slot_map; //table_name to slot
        for (const auto& slot : tuple_descriptor->slots()) {
            slot_map.emplace(slot->col_name(), slot);
        }

        // For top-level columns, use indexes to match, and for sub-columns, still use name to match columns.
        auto parquet_fields_schema = field_desc->get_fields_schema();
        for (size_t idx = 0; idx < _params.column_idxs.size(); idx++) {
            auto table_column_name = read_table_col_names[idx];
            auto file_index = _params.column_idxs[idx];

            if (file_index >= parquet_fields_schema.size()) {
                // Non-partitioning columns, which may be columns added later.
                table_info_node_ptr->add_not_exist_children(table_column_name);
            } else {
                // Non-partitioning columns, columns that exist in both the table and the file.
                auto field_node = std::make_shared<Node>();
                // for sub-columns, still use name to match columns.
                RETURN_IF_ERROR(BuildTableInfoUtil::by_parquet_name(
                        slot_map[table_column_name]->type(), parquet_fields_schema[file_index],
                        field_node));
                table_info_node_ptr->add_children(
                        table_column_name, parquet_fields_schema[file_index].name, field_node);
            }

            slot_map.erase(table_column_name);
        }
        /*
         * `_params.column_idxs` only have `isIsFileSlot()`, so we need add `partition slot`.
         * eg:
         * Table : A, B, C, D     (D: partition column)
         * Parquet file : A, B
         * Column C is obtained by add column.
         *
         * sql : select * from table;
         * slot : A, B, C, D
         * _params.column_idxs: 0, 1, 2 (There is no 3, because column D is the partition column)
         *
         */
        for (const auto& [partition_col_name, _] : slot_map) {
            table_info_node_ptr->add_not_exist_children(partition_col_name);
        }
    }

    auto column_id_result = ColumnIdResult();
    if (_state->query_options().hive_parquet_use_column_names) {
        column_id_result = _create_column_ids_and_names(field_desc, tuple_descriptor);
    } else {
        column_id_result =
                _create_column_ids_and_names_by_top_level_col_index(field_desc, tuple_descriptor);
    }

    const auto& column_ids = column_id_result.column_ids;

    RETURN_IF_ERROR(init_row_filters());
    // std::vector<uint64_t> column_ids_vector(column_ids.begin(), column_ids.end());

    return parquet_reader->init_reader(
            read_table_col_names, table_col_name_to_value_range, conjuncts, tuple_descriptor,
            row_descriptor, colname_to_slot_id, not_single_slot_filter_conjuncts,
            slot_id_to_filter_conjuncts, table_info_node_ptr, true, column_ids);
}

ColumnIdResult HiveParquetReader::_create_column_ids_and_names(
        const FieldDescriptor* field_desc, const TupleDescriptor* tuple_descriptor) {
    std::set<uint64_t> column_ids;
    std::shared_ptr<TableSchemaChangeHelper::Node> schema_node = nullptr;

    if (!field_desc) {
        return ColumnIdResult(std::move(column_ids), schema_node);
    }

    // First, assign column IDs to the field descriptor
    auto* mutable_field_desc = const_cast<FieldDescriptor*>(field_desc);
    mutable_field_desc->assign_ids();

    // Create a mapping from iceberg field_id to FieldSchema for quick lookup - similar to create_iceberg_projected_layout

    // Group column paths by field ID - inspired by create_iceberg_projected_layout's sequence processing
    std::unordered_map<std::string, std::vector<std::vector<std::string>>> paths_by_table_name;
    std::unordered_map<int, std::vector<std::vector<std::string>>> paths_by_table_index;
    // std::unordered_map<int, std::string> field_id_to_table_name;

    for (const auto* slot : tuple_descriptor->slots()) {
        // 假设 slot->column_access_paths() 返回 vector<vector<int>>，每个 path 代表一个 iceberg id 路径
        const auto& column_access_paths = slot->column_access_paths();

        // field_id_to_table_name[slot->col_unique_id()] = slot->col_name();

        // If column_paths is empty or has empty paths, we need the entire column
        if ((!column_access_paths.__isset.name_access_paths) ||
            column_access_paths.name_access_paths.empty() ||
            std::any_of(column_access_paths.name_access_paths.begin(),
                        column_access_paths.name_access_paths.end(),
                        [](const TColumnNameAccessPath& access_path) {
                            return access_path.path.empty();
                        })) {
            paths_by_table_name[slot->col_name()].push_back(std::vector<std::string>());
            paths_by_table_index[slot->col_pos()].push_back(std::vector<std::string>());
        } else {
            // Add all paths for this field ID
            for (const auto& name_access_path : column_access_paths.name_access_paths) {
                // // Convert string path to int path
                // std::vector<int> int_path;
                // // Simple conversion: convert string to int directly
                // for (const auto& path_element : name_access_path.path) {
                //     try {
                //         // Convert string to int directly
                //         int_path.push_back(std::stoi(path_element));
                //     } catch (const std::exception& /* e */) {
                //         // If conversion fails, use 0 as default
                //         int_path.push_back(0);
                //     }
                // }
                paths_by_table_name[slot->col_name()].push_back(name_access_path.path);
                paths_by_table_index[slot->col_pos()].push_back(name_access_path.path);
            }
        }
    }

    // Use the new merged efficient method
    auto result = HiveParquetNestedColumnUtils::_extract_schema_and_columns_efficiently(
            field_desc, paths_by_table_name);

    return ColumnIdResult(std::move(result.column_ids), result.schema_node);
}

ColumnIdResult HiveParquetReader::_create_column_ids_and_names_by_top_level_col_index(
        const FieldDescriptor* field_desc, const TupleDescriptor* tuple_descriptor) {
    std::set<uint64_t> column_ids;
    std::shared_ptr<TableSchemaChangeHelper::Node> schema_node = nullptr;

    if (!field_desc) {
        return ColumnIdResult(std::move(column_ids), schema_node);
    }

    // First, assign column IDs to the field descriptor
    auto* mutable_field_desc = const_cast<FieldDescriptor*>(field_desc);
    mutable_field_desc->assign_ids();

    // Create a mapping from iceberg field_id to FieldSchema for quick lookup - similar to create_iceberg_projected_layout

    // Group column paths by field ID - inspired by create_iceberg_projected_layout's sequence processing
    std::unordered_map<std::string, std::vector<std::vector<std::string>>> paths_by_table_name;
    std::unordered_map<int, std::vector<std::vector<std::string>>> paths_by_table_index;
    // std::unordered_map<int, std::string> field_id_to_table_name;

    for (const auto* slot : tuple_descriptor->slots()) {
        // 假设 slot->column_access_paths() 返回 vector<vector<int>>，每个 path 代表一个 iceberg id 路径
        const auto& column_access_paths = slot->column_access_paths();

        // field_id_to_table_name[slot->col_unique_id()] = slot->col_name();

        // If column_paths is empty or has empty paths, we need the entire column
        if ((!column_access_paths.__isset.name_access_paths) ||
            column_access_paths.name_access_paths.empty() ||
            std::any_of(column_access_paths.name_access_paths.begin(),
                        column_access_paths.name_access_paths.end(),
                        [](const TColumnNameAccessPath& access_path) {
                            return access_path.path.empty();
                        })) {
            paths_by_table_index[slot->col_pos()].push_back(std::vector<std::string>());
        } else {
            // Add all paths for this field ID
            for (const auto& name_access_path : column_access_paths.name_access_paths) {
                // // Convert string path to int path
                // std::vector<int> int_path;
                // // Simple conversion: convert string to int directly
                // for (const auto& path_element : name_access_path.path) {
                //     try {
                //         // Convert string to int directly
                //         int_path.push_back(std::stoi(path_element));
                //     } catch (const std::exception& /* e */) {
                //         // If conversion fails, use 0 as default
                //         int_path.push_back(0);
                //     }
                // }
                paths_by_table_index[slot->col_pos()].push_back(name_access_path.path);
            }
        }
    }

    // Use the new merged efficient method
    auto result = HiveParquetNestedColumnUtils::
            _extract_schema_and_columns_efficiently_by_top_level_col_index(field_desc,
                                                                           paths_by_table_index);

    return ColumnIdResult(std::move(result.column_ids), result.schema_node);
}

ColumnIdResult HiveParquetReader::_create_column_ids_and_names_by_index(
        const FieldDescriptor* field_desc, const TupleDescriptor* tuple_descriptor) {
    std::set<uint64_t> column_ids;
    std::shared_ptr<TableSchemaChangeHelper::Node> schema_node = nullptr;

    if (!field_desc) {
        return ColumnIdResult(std::move(column_ids), schema_node);
    }

    // 为 FieldDescriptor 分配 column_id，确保后续可取到叶子列的 column_id
    auto* mutable_field_desc = const_cast<FieldDescriptor*>(field_desc);
    mutable_field_desc->assign_ids();

    // Create a mapping from iceberg field_id to FieldSchema for quick lookup - similar to create_iceberg_projected_layout

    // Group column paths by field ID - inspired by create_iceberg_projected_layout's sequence processing
    std::unordered_map<int, std::vector<std::vector<int>>> paths_by_table_index; // index to paths

    for (int i = 0; i < tuple_descriptor->slots().size(); ++i) {
        const auto* slot = tuple_descriptor->slots()[i];
        // 假设 slot->column_access_paths() 返回 vector<vector<int>>，每个 path 代表一个 iceberg id 路径
        const auto& column_access_paths = slot->column_access_paths();

        // field_id_to_table_name[slot->col_unique_id()] = slot->col_name();

        // If column_paths is empty or has empty paths, we need the entire column
        if ((!column_access_paths.__isset.name_access_paths) ||
            column_access_paths.name_access_paths.empty() ||
            std::any_of(column_access_paths.name_access_paths.begin(),
                        column_access_paths.name_access_paths.end(),
                        [](const TColumnNameAccessPath& access_path) {
                            return access_path.path.empty();
                        })) {
            paths_by_table_index[slot->col_pos()].push_back(std::vector<int>());
        } else {
            // Add all paths for this field ID
            for (const auto& name_access_path : column_access_paths.name_access_paths) {
                // Convert string path to int path
                std::vector<int> int_path;
                // Simple conversion: convert string to int directly
                for (const auto& path_element : name_access_path.path) {
                    try {
                        // Convert string to int directly
                        int_path.push_back(std::stoi(path_element));
                    } catch (const std::exception& /* e */) {
                        // If conversion fails, use 0 as default
                        int_path.push_back(0);
                    }
                }
                paths_by_table_index[slot->col_pos()].push_back(int_path);
            }
        }
    }

    // Use the new merged efficient method
    auto result = HiveParquetNestedColumnUtils::_extract_schema_and_columns_efficiently_by_index(
            field_desc, paths_by_table_index);

    // 这里不构建或返回 schema_node，因为 init_reader 中已基于索引构建了 table_info_node_ptr
    return ColumnIdResult(std::move(result.column_ids), result.schema_node);
}

#include "common/compile_check_end.h"
} // namespace doris::vectorized
