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
        column_id_result = _create_column_ids2(orc_type_ptr, tuple_descriptor);
    } else {
        column_id_result =
                _create_column_ids2_by_top_level_col_index(orc_type_ptr, tuple_descriptor);
    }

    // const auto& file_col_names = column_id_result.column_names;
    const auto& column_ids = column_id_result.column_ids;
    const auto& filter_column_ids = column_id_result.filter_column_ids;

    return orc_reader->init_reader(&read_table_col_names, table_col_name_to_value_range, conjuncts,
                                   false, tuple_descriptor, row_descriptor,
                                   not_single_slot_filter_conjuncts, slot_id_to_filter_conjuncts,
                                   table_info_node_ptr, column_ids, filter_column_ids);
}

ColumnIdResult HiveOrcReader::_create_column_ids(const orc::Type* orc_type,
                                                 const TupleDescriptor* tuple_descriptor) {
    std::shared_ptr<TableSchemaChangeHelper::Node> schema_node = nullptr;

    if (!orc_type) {
        return ColumnIdResult();
    }

    // Group column paths by field ID - inspired by create_iceberg_projected_layout's sequence processing
    std::unordered_map<std::string, std::vector<TColumnNameAccessPath>> paths_by_table_col_name;
    std::unordered_map<std::string, std::vector<TColumnNameAccessPath>>
            predicate_paths_by_table_col_name;

    for (const auto* slot : tuple_descriptor->slots()) {
        const auto& all_column_access_paths = slot->all_column_access_paths();
        // 普通字段应该没有 all_column_access_paths
        if ((slot->col_type() != TYPE_STRUCT && slot->col_type() != TYPE_ARRAY &&
             slot->col_type() != TYPE_MAP)) {
            DCHECK(!all_column_access_paths.__isset.name_access_paths ||
                   all_column_access_paths.name_access_paths.empty());
            TColumnNameAccessPath empty_path;
            empty_path.path = std::vector<std::string>();
            paths_by_table_col_name[slot->col_name()].push_back(empty_path);
            continue;
        }

        // 嵌套字段里面如果有 all_column_access_path，说明里面没有 struct。
        if (all_column_access_paths.__isset.name_access_paths &&
            !all_column_access_paths.name_access_paths.empty()) {
            // 处理 all_column_access_paths，用于生成 column_ids
            for (const auto& name_access_path : all_column_access_paths.name_access_paths) {
                DCHECK(name_access_path.path.size() >= 1 &&
                       name_access_path.path[0] == slot->col_name());
                if (!name_access_path.path.empty()) {
                    TColumnNameAccessPath remaining;
                    if (name_access_path.path.size() > 1) {
                        remaining.path.assign(name_access_path.path.begin() + 1,
                                              name_access_path.path.end());
                    }
                    paths_by_table_col_name[slot->col_name()].push_back(std::move(remaining));
                }
            }
        } else { // 嵌套字段里面没有 all_column_access_path，说明里面没有 struct，直接读取整个字段
            // TColumnNameAccessPath empty_path;
            // empty_path.path = std::vector<std::string>();
            // paths_by_table_col_name[slot->col_name()].push_back(empty_path);
        }

        if ((slot->col_type() != TYPE_STRUCT && slot->col_type() != TYPE_ARRAY &&
             slot->col_type() != TYPE_MAP)) {
            if (slot->is_predicate()) {
                TColumnNameAccessPath empty_path;
                empty_path.path = std::vector<std::string>();
                predicate_paths_by_table_col_name[slot->col_name()].push_back(empty_path);
                continue;
            } else {
                // Skip non-predicate primitive type slots
                continue;
            }
        }

        const auto& predicate_column_access_paths = slot->predicate_column_access_paths();

        if ((!predicate_column_access_paths.__isset.name_access_paths) ||
            predicate_column_access_paths.name_access_paths.empty()) {
            continue;
        }

        for (const auto& name_access_path : predicate_column_access_paths.name_access_paths) {
            DCHECK(name_access_path.path.size() >= 1 &&
                   name_access_path.path[0] == slot->col_name());
            if (!name_access_path.path.empty()) {
                // std::string first_table_col_name = name_access_path.path[0];
                TColumnNameAccessPath remaining;
                if (name_access_path.path.size() > 1) {
                    remaining.path.assign(name_access_path.path.begin() + 1,
                                          name_access_path.path.end());
                }
                predicate_paths_by_table_col_name[slot->col_name()].push_back(std::move(remaining));
            }
        }
    }

    auto column_ids = HiveOrcNestedColumnUtils::extract_schema_and_columns_efficiently(
            orc_type, paths_by_table_col_name);

    // Use the new merged efficient method
    auto filter_column_ids = HiveOrcNestedColumnUtils::extract_schema_and_columns_efficiently(
            orc_type, predicate_paths_by_table_col_name);

    return ColumnIdResult(std::move(column_ids), std::move(filter_column_ids));
}

ColumnIdResult HiveOrcReader::_create_column_ids_by_top_level_col_index(
        const orc::Type* orc_type, const TupleDescriptor* tuple_descriptor) {
    std::shared_ptr<TableSchemaChangeHelper::Node> schema_node = nullptr;

    if (!orc_type) {
        return ColumnIdResult();
    }

    // Group column paths by field ID - inspired by create_iceberg_projected_layout's sequence processing
    std::unordered_map<uint64_t, std::vector<TColumnNameAccessPath>> paths_by_table_col_index;
    std::unordered_map<uint64_t, std::vector<TColumnNameAccessPath>> predicate_paths_by_table_col_index;

    for (const auto* slot : tuple_descriptor->slots()) {
        const auto& all_column_access_paths = slot->all_column_access_paths();
        if ((slot->col_type() != TYPE_STRUCT && slot->col_type() != TYPE_ARRAY &&
             slot->col_type() != TYPE_MAP)) {
            TColumnNameAccessPath empty_path;
            empty_path.path = std::vector<std::string>();
            paths_by_table_col_index[slot->col_pos()].push_back(empty_path);
            continue;
        }

        // 处理 all_column_access_paths，用于生成 column_ids
        // 新规则：all_column_access_paths 包含顶层字段名字时表示访问顶层字段（读取所有子字段）
        if ((!all_column_access_paths.__isset.name_access_paths) ||
            all_column_access_paths.name_access_paths.empty()) {
            // 如果路径为空，创建一个空路径表示读取整个字段
            // TColumnNameAccessPath empty_path;
            // empty_path.path = std::vector<std::string>();
            // paths_by_table_col_index[slot->col_pos()].push_back(empty_path);
        } else {
            // 检查是否只包含顶层字段名（表示读取所有子字段）
            // bool is_top_level_access = false;
            // for (const auto& name_access_path : all_column_access_paths.name_access_paths) {
            //     if (name_access_path.path.size() == 1 &&
            //         name_access_path.path[0] == slot->col_name()) {
            //         // 顶层字段访问：创建空路径表示读取所有子字段
            //         TColumnNameAccessPath empty_path;
            //         empty_path.path = std::vector<std::string>();
            //         paths_by_table_col_index[slot->col_pos()].push_back(empty_path);
            //         is_top_level_access = true;
            //         break;
            //     }
            // }

            // if (!is_top_level_access) {
            //     // 非顶层访问，添加所有指定的路径
            //     for (const auto& name_access_path : all_column_access_paths.name_access_paths) {
            //         if (!name_access_path.path.empty()) {
            //             // std::string first_table_col_name = name_access_path.path[0];
            //             TColumnNameAccessPath remaining;
            //             if (name_access_path.path.size() > 1) {
            //                 remaining.path.assign(name_access_path.path.begin() + 1, name_access_path.path.end());
            //             }
            //             paths_by_table_col_index[slot->col_pos()].push_back(std::move(remaining));
            //         }
            //     }
            // }

            for (const auto& name_access_path : all_column_access_paths.name_access_paths) {
                DCHECK(name_access_path.path.size() >= 1 &&
                       name_access_path.path[0] == slot->col_name());
                if (!name_access_path.path.empty()) {
                    // std::string first_table_col_name = name_access_path.path[0];
                    TColumnNameAccessPath remaining;
                    if (name_access_path.path.size() > 1) {
                        remaining.path.assign(name_access_path.path.begin() + 1,
                                              name_access_path.path.end());
                    }
                    paths_by_table_col_index[slot->col_pos()].push_back(std::move(remaining));
                }
            }
        }

        if ((slot->col_type() != TYPE_STRUCT && slot->col_type() != TYPE_ARRAY &&
             slot->col_type() != TYPE_MAP)) {
            if (slot->is_predicate()) {
                TColumnNameAccessPath empty_path;
                empty_path.path = std::vector<std::string>();
                predicate_paths_by_table_col_index[slot->col_pos()].push_back(empty_path);
                continue;
            } else {
                // Skip non-predicate primitive type slots
                continue;
            }
        }

        const auto& predicate_column_access_paths = slot->predicate_column_access_paths();
        // 处理 all_column_access_paths，用于生成 column_ids
        // 新规则：all_column_access_paths 包含顶层字段名字时表示访问顶层字段（读取所有子字段）
        if ((!predicate_column_access_paths.__isset.name_access_paths) ||
            predicate_column_access_paths.name_access_paths.empty()) {
            // 如果路径为空，创建一个空路径表示读取整个字段
            // TColumnNameAccessPath empty_path;
            // empty_path.path = std::vector<std::string>();
            // predicate_paths_by_table_col_index[slot->col_name()].push_back(empty_path);
        } else {
            // 检查是否只包含顶层字段名（表示读取所有子字段）
            // bool is_top_level_access = false;
            // for (const auto& name_access_path : predicate_column_access_paths.name_access_paths) {
            //     if (name_access_path.path.size() == 1 &&
            //         name_access_path.path[0] == slot->col_name()) {
            //         // 顶层字段访问：创建空路径表示读取所有子字段
            //         TColumnNameAccessPath empty_path;
            //         empty_path.path = std::vector<std::string>();
            //         predicate_paths_by_table_col_index[slot->col_pos()].push_back(empty_path);
            //         is_top_level_access = true;
            //         break;
            //     }
            // }

            // if (!is_top_level_access) {
            //     // 非顶层访问，添加所有指定的路径
            //     for (const auto& name_access_path : predicate_column_access_paths.name_access_paths) {
            //         if (!name_access_path.path.empty()) {
            //             // std::string first_table_col_name = name_access_path.path[0];
            //             TColumnNameAccessPath remaining;
            //             if (name_access_path.path.size() > 1) {
            //                 remaining.path.assign(name_access_path.path.begin() + 1, name_access_path.path.end());
            //             }
            //             predicate_paths_by_table_col_index[slot->col_pos()].push_back(std::move(remaining));
            //         }
            //     }
            // }

            for (const auto& name_access_path : predicate_column_access_paths.name_access_paths) {
                DCHECK(name_access_path.path.size() >= 1 &&
                       name_access_path.path[0] == slot->col_name());
                if (!name_access_path.path.empty()) {
                    // std::string first_table_col_name = name_access_path.path[0];
                    TColumnNameAccessPath remaining;
                    if (name_access_path.path.size() > 1) {
                        remaining.path.assign(name_access_path.path.begin() + 1,
                                              name_access_path.path.end());
                    }
                    predicate_paths_by_table_col_index[slot->col_pos()].push_back(
                            std::move(remaining));
                }
            }
        }
    }

    auto column_ids =
            HiveOrcNestedColumnUtils::extract_schema_and_columns_efficiently_by_top_level_col_index(
                    orc_type, paths_by_table_col_index);

    // Use the new merged efficient method
    auto filter_column_ids =
            HiveOrcNestedColumnUtils::extract_schema_and_columns_efficiently_by_top_level_col_index(
                    orc_type, predicate_paths_by_table_col_index);

    return ColumnIdResult {std::move(column_ids), std::move(filter_column_ids)};
}

ColumnIdResult HiveOrcReader::_create_column_ids2(const orc::Type* orc_type,
                                                  const TupleDescriptor* tuple_descriptor) {
    std::shared_ptr<TableSchemaChangeHelper::Node> schema_node = nullptr;

    if (!orc_type) {
        return ColumnIdResult();
    }

    // map top-level table column name (lower-cased) -> orc::Type*
    std::unordered_map<std::string, const orc::Type*> table_col_name_to_orc_type_map;
    for (uint64_t i = 0; i < orc_type->getSubtypeCount(); ++i) {
        auto orc_sub_type = orc_type->getSubtype(i);
        if (!orc_sub_type) continue;

        std::string table_col_name = to_lower(orc_type->getFieldName(i));
        table_col_name_to_orc_type_map[table_col_name] = orc_sub_type;
    }

    std::set<uint64_t> column_ids;
    std::set<uint64_t> filter_column_ids;

    // helper to process name access paths for a given top-level orc field
    auto process_access_paths =
            [](const orc::Type* orc_field,
               const std::vector<TColumnNameAccessPath>& name_access_paths,
               std::set<uint64_t>& out_ids) {
                if (!orc_field) return;
                if (name_access_paths.empty()) return;

                std::vector<TColumnNameAccessPath> paths;
                bool has_top_level_only = false;
                for (const auto& name_access_path : name_access_paths) {
                    DCHECK(name_access_path.path.size() >= 1);
                    TColumnNameAccessPath remaining_path;
                    if (name_access_path.path.size() > 1) {
                        remaining_path.path.assign(name_access_path.path.begin() + 1,
                                                   name_access_path.path.end());
                    } else {
                        // only top-level column name => means whole field
                        remaining_path.path = std::vector<std::string>();
                    }
                    if (remaining_path.path.empty()) {
                        has_top_level_only = true;
                    }
                    paths.push_back(std::move(remaining_path));
                }

                if (has_top_level_only) {
                    uint64_t start_id = orc_field->getColumnId();
                    uint64_t max_column_id = orc_field->getMaximumColumnId();
                    for (uint64_t id = start_id; id <= max_column_id; ++id) {
                        out_ids.insert(id);
                    }
                } else if (!paths.empty()) {
                    HiveOrcNestedColumnUtils::extract_nested_column_ids_efficiently(*orc_field,
                                                                                    paths, out_ids);
                }
            };

    for (const auto* slot : tuple_descriptor->slots()) {
        const auto& all_column_access_paths = slot->all_column_access_paths();

        // primitive (non-nested) types: direct mapping by name
        if ((slot->col_type() != TYPE_STRUCT && slot->col_type() != TYPE_ARRAY &&
             slot->col_type() != TYPE_MAP)) {
            auto it = table_col_name_to_orc_type_map.find(slot->col_name());
            if (it != table_col_name_to_orc_type_map.end()) {
                column_ids.insert(it->second->getColumnId());
                if (slot->is_predicate()) {
                    filter_column_ids.insert(it->second->getColumnId());
                }
            }
            continue;
        }

        // complex types: find top-level ORC type first
        auto it = table_col_name_to_orc_type_map.find(slot->col_name());
        if (it == table_col_name_to_orc_type_map.end()) {
            continue;
        }
        const orc::Type* top_orc_field = it->second;

        // collect and process all_column_access_paths -> column_ids
        if (all_column_access_paths.__isset.name_access_paths &&
            !all_column_access_paths.name_access_paths.empty()) {
            process_access_paths(top_orc_field, all_column_access_paths.name_access_paths,
                                 column_ids);
        }

        // collect and process predicate_column_access_paths -> filter_column_ids
        const auto& predicate_column_access_paths = slot->predicate_column_access_paths();
        if (predicate_column_access_paths.__isset.name_access_paths &&
            !predicate_column_access_paths.name_access_paths.empty()) {
            process_access_paths(top_orc_field, predicate_column_access_paths.name_access_paths,
                                 filter_column_ids);
        }
    }

    return ColumnIdResult(std::move(column_ids), std::move(filter_column_ids));
}

ColumnIdResult HiveOrcReader::_create_column_ids2_by_top_level_col_index(const orc::Type* orc_type,
                                                  const TupleDescriptor* tuple_descriptor) {
    std::shared_ptr<TableSchemaChangeHelper::Node> schema_node = nullptr;

    if (!orc_type) {
        return ColumnIdResult();
    }

    // map top-level table column index -> orc::Type*
    std::unordered_map<uint64_t, const orc::Type*> table_col_pos_to_orc_type_map;
    for (uint64_t i = 0; i < orc_type->getSubtypeCount(); ++i) {
        auto orc_sub_type = orc_type->getSubtype(i);
        if (!orc_sub_type) continue;

        table_col_pos_to_orc_type_map[i] = orc_sub_type;
    }

    std::set<uint64_t> column_ids;
    std::set<uint64_t> filter_column_ids;

    // helper to process name access paths for a given top-level orc field
    auto process_access_paths =
            [](const orc::Type* orc_field,
               const std::vector<TColumnNameAccessPath>& name_access_paths,
               std::set<uint64_t>& out_ids) {
                if (!orc_field) return;
                if (name_access_paths.empty()) return;

                std::vector<TColumnNameAccessPath> paths;
                bool has_top_level_only = false;
                for (const auto& name_access_path : name_access_paths) {
                    DCHECK(name_access_path.path.size() >= 1);
                    TColumnNameAccessPath remaining_path;
                    if (name_access_path.path.size() > 1) {
                        remaining_path.path.assign(name_access_path.path.begin() + 1,
                                                   name_access_path.path.end());
                    } else {
                        // only top-level column name => means whole field
                        remaining_path.path = std::vector<std::string>();
                    }
                    if (remaining_path.path.empty()) {
                        has_top_level_only = true;
                    }
                    paths.push_back(std::move(remaining_path));
                }

                if (has_top_level_only) {
                    uint64_t start_id = orc_field->getColumnId();
                    uint64_t max_column_id = orc_field->getMaximumColumnId();
                    for (uint64_t id = start_id; id <= max_column_id; ++id) {
                        out_ids.insert(id);
                    }
                } else if (!paths.empty()) {
                    HiveOrcNestedColumnUtils::extract_nested_column_ids_efficiently(*orc_field,
                                                                                    paths, out_ids);
                }
            };

    for (const auto* slot : tuple_descriptor->slots()) {
        const auto& all_column_access_paths = slot->all_column_access_paths();

        // primitive (non-nested) types: direct mapping by pos
        if ((slot->col_type() != TYPE_STRUCT && slot->col_type() != TYPE_ARRAY &&
             slot->col_type() != TYPE_MAP)) {
            auto it = table_col_pos_to_orc_type_map.find(slot->col_pos());
            if (it != table_col_pos_to_orc_type_map.end()) {
                column_ids.insert(it->second->getColumnId());
                if (slot->is_predicate()) {
                    filter_column_ids.insert(it->second->getColumnId());
                }
            }
            continue;
        }

        // complex types: find top-level ORC type first
        auto it = table_col_pos_to_orc_type_map.find(slot->col_pos());
        if (it == table_col_pos_to_orc_type_map.end()) {
            continue;
        }
        const orc::Type* top_orc_field = it->second;

        // collect and process all_column_access_paths -> column_ids
        if (all_column_access_paths.__isset.name_access_paths &&
            !all_column_access_paths.name_access_paths.empty()) {
            process_access_paths(top_orc_field, all_column_access_paths.name_access_paths,
                                 column_ids);
        }

        // collect and process predicate_column_access_paths -> filter_column_ids
        const auto& predicate_column_access_paths = slot->predicate_column_access_paths();
        if (predicate_column_access_paths.__isset.name_access_paths &&
            !predicate_column_access_paths.name_access_paths.empty()) {
            process_access_paths(top_orc_field, predicate_column_access_paths.name_access_paths,
                                 filter_column_ids);
        }
    }

    return ColumnIdResult(std::move(column_ids), std::move(filter_column_ids));
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
        column_id_result = _create_column_ids2(field_desc, tuple_descriptor);
    } else {
        column_id_result = _create_column_ids2_by_top_level_col_index(field_desc, tuple_descriptor);
    }

    const auto& column_ids = column_id_result.column_ids;
    const auto& filter_column_ids = column_id_result.filter_column_ids;

    RETURN_IF_ERROR(init_row_filters());
    // std::vector<uint64_t> column_ids_vector(column_ids.begin(), column_ids.end());

    return parquet_reader->init_reader(
            read_table_col_names, table_col_name_to_value_range, conjuncts, tuple_descriptor,
            row_descriptor, colname_to_slot_id, not_single_slot_filter_conjuncts,
            slot_id_to_filter_conjuncts, table_info_node_ptr, true, column_ids, filter_column_ids);
}

ColumnIdResult HiveParquetReader::_create_column_ids(const FieldDescriptor* field_desc,
                                                     const TupleDescriptor* tuple_descriptor) {
    std::shared_ptr<TableSchemaChangeHelper::Node> schema_node = nullptr;

    if (!field_desc) {
        return ColumnIdResult();
    }

    // First, assign column IDs to the field descriptor
    auto* mutable_field_desc = const_cast<FieldDescriptor*>(field_desc);
    mutable_field_desc->assign_ids();

    // Group column paths by field ID - inspired by create_iceberg_projected_layout's sequence processing
    std::unordered_map<std::string, std::vector<TColumnNameAccessPath>> paths_by_table_col_name;
    std::unordered_map<std::string, std::vector<TColumnNameAccessPath>>
            predicate_paths_by_table_col_name;

    for (const auto* slot : tuple_descriptor->slots()) {
        const auto& all_column_access_paths = slot->all_column_access_paths();
        if ((slot->col_type() != TYPE_STRUCT && slot->col_type() != TYPE_ARRAY &&
             slot->col_type() != TYPE_MAP)) {
            TColumnNameAccessPath empty_path;
            empty_path.path = std::vector<std::string>();
            paths_by_table_col_name[slot->col_name()].push_back(empty_path);
            continue;
        }

        // 处理 all_column_access_paths，用于生成 column_ids
        // 新规则：all_column_access_paths 包含顶层字段名字时表示访问顶层字段（读取所有子字段）
        if ((!all_column_access_paths.__isset.name_access_paths) ||
            all_column_access_paths.name_access_paths.empty()) {
            // 如果路径为空，创建一个空路径表示读取整个字段
            // TColumnNameAccessPath empty_path;
            // empty_path.path = std::vector<std::string>();
            // paths_by_table_col_name[slot->col_name()].push_back(empty_path);
        } else {
            // 检查是否只包含顶层字段名（表示读取所有子字段）
            // bool is_top_level_access = false;
            // for (const auto& name_access_path : all_column_access_paths.name_access_paths) {
            //     if (name_access_path.path.size() == 1 &&
            //         name_access_path.path[0] == slot->col_name()) {
            //         // 顶层字段访问：创建空路径表示读取所有子字段
            //         TColumnNameAccessPath empty_path;
            //         empty_path.path = std::vector<std::string>();
            //         paths_by_table_col_name[slot->col_name()].push_back(empty_path);
            //         is_top_level_access = true;
            //         break;
            //     }
            // }

            // if (!is_top_level_access) {
            //     // 非顶层访问，添加所有指定的路径
            //     for (const auto& name_access_path : all_column_access_paths.name_access_paths) {
            //         if (!name_access_path.path.empty()) {
            //             // std::string first_table_col_name = name_access_path.path[0];
            //             TColumnNameAccessPath remaining;
            //             if (name_access_path.path.size() > 1) {
            //                 remaining.path.assign(name_access_path.path.begin() + 1, name_access_path.path.end());
            //             }
            //             paths_by_table_col_name[slot->col_name()].push_back(std::move(remaining));
            //         }
            //     }
            // }

            for (const auto& name_access_path : all_column_access_paths.name_access_paths) {
                DCHECK(name_access_path.path[0] == slot->col_name());
                if (!name_access_path.path.empty()) {
                    // std::string first_table_col_name = name_access_path.path[0];
                    TColumnNameAccessPath remaining;
                    if (name_access_path.path.size() > 1) {
                        remaining.path.assign(name_access_path.path.begin() + 1,
                                              name_access_path.path.end());
                    }
                    paths_by_table_col_name[slot->col_name()].push_back(std::move(remaining));
                }
            }
        }

        if ((slot->col_type() != TYPE_STRUCT && slot->col_type() != TYPE_ARRAY &&
             slot->col_type() != TYPE_MAP)) {
            if (slot->is_predicate()) {
                TColumnNameAccessPath empty_path;
                empty_path.path = std::vector<std::string>();
                predicate_paths_by_table_col_name[slot->col_name()].push_back(empty_path);
                continue;
            } else {
                continue;
            }
        }

        const auto& predicate_column_access_paths = slot->predicate_column_access_paths();
        // 处理 all_column_access_paths，用于生成 column_ids
        // 新规则：all_column_access_paths 包含顶层字段名字时表示访问顶层字段（读取所有子字段）
        if ((!predicate_column_access_paths.__isset.name_access_paths) ||
            predicate_column_access_paths.name_access_paths.empty()) {
            // 如果路径为空，创建一个空路径表示读取整个字段
            // TColumnNameAccessPath empty_path;
            // empty_path.path = std::vector<std::string>();
            // predicate_paths_by_table_col_name[slot->col_name()].push_back(empty_path);
        } else {
            // // 检查是否只包含顶层字段名（表示读取所有子字段）
            // bool is_top_level_access = false;
            // for (const auto& name_access_path : predicate_column_access_paths.name_access_paths) {
            //     if (name_access_path.path.size() == 1 &&
            //         name_access_path.path[0] == slot->col_name()) {
            //         // 顶层字段访问：创建空路径表示读取所有子字段
            //         TColumnNameAccessPath empty_path;
            //         empty_path.path = std::vector<std::string>();
            //         predicate_paths_by_table_col_name[slot->col_name()].push_back(empty_path);
            //         is_top_level_access = true;
            //         break;
            //     }
            // }

            // if (!is_top_level_access) {
            //     // 非顶层访问，添加所有指定的路径
            //     for (const auto& name_access_path : predicate_column_access_paths.name_access_paths) {
            //         if (!name_access_path.path.empty()) {
            //             // std::string first_table_col_name = name_access_path.path[0];
            //             TColumnNameAccessPath remaining;
            //             if (name_access_path.path.size() > 1) {
            //                 remaining.path.assign(name_access_path.path.begin() + 1, name_access_path.path.end());
            //             }
            //             predicate_paths_by_table_col_name[slot->col_name()].push_back(std::move(remaining));
            //         }
            //     }
            // }

            for (const auto& name_access_path : predicate_column_access_paths.name_access_paths) {
                DCHECK(name_access_path.path.size() >= 1 &&
                       name_access_path.path[0] == slot->col_name());
                if (!name_access_path.path.empty()) {
                    // std::string first_table_col_name = name_access_path.path[0];
                    TColumnNameAccessPath remaining;
                    if (name_access_path.path.size() > 1) {
                        remaining.path.assign(name_access_path.path.begin() + 1,
                                              name_access_path.path.end());
                    }
                    predicate_paths_by_table_col_name[slot->col_name()].push_back(
                            std::move(remaining));
                }
            }
        }
    }

    auto column_ids = HiveParquetNestedColumnUtils::extract_schema_and_columns_efficiently(
            field_desc, paths_by_table_col_name);

    // Use the new merged efficient method
    auto filter_column_ids = HiveParquetNestedColumnUtils::extract_schema_and_columns_efficiently(
            field_desc, predicate_paths_by_table_col_name);

    return ColumnIdResult(std::move(column_ids), std::move(filter_column_ids));
}

ColumnIdResult HiveParquetReader::_create_column_ids2(const FieldDescriptor* field_desc,
                                                      const TupleDescriptor* tuple_descriptor) {
    std::shared_ptr<TableSchemaChangeHelper::Node> schema_node = nullptr;

    if (!field_desc) {
        return ColumnIdResult();
    }

    // First, assign column IDs to the field descriptor
    auto* mutable_field_desc = const_cast<FieldDescriptor*>(field_desc);
    mutable_field_desc->assign_ids();

    std::unordered_map<std::string, const FieldSchema*> table_col_name_to_field_schema_map;
    for (int i = 0; i < field_desc->size(); ++i) {
        auto field_schema = field_desc->get_column(i);
        if (!field_schema) continue;

        table_col_name_to_field_schema_map[field_schema->lower_case_name] = field_schema;
    }

    std::set<uint64_t> column_ids;
    std::set<uint64_t> filter_column_ids;

    // helper to process name access paths for a given top-level parquet field
    auto process_access_paths =
            [](const FieldSchema* parquet_field,
               const std::vector<TColumnNameAccessPath>& name_access_paths,
               std::set<uint64_t>& out_ids) {
                if (!parquet_field) return;
                if (name_access_paths.empty()) return;

                std::vector<TColumnNameAccessPath> paths;
                bool has_top_level_only = false;
                for (const auto& name_access_path : name_access_paths) {
                    DCHECK(name_access_path.path.size() >= 1);
                    TColumnNameAccessPath remaining_path;
                    if (name_access_path.path.size() > 1) {
                        remaining_path.path.assign(name_access_path.path.begin() + 1,
                                                   name_access_path.path.end());
                    } else {
                        // only top-level column name => means whole field
                        remaining_path.path = std::vector<std::string>();
                    }
                    if (remaining_path.path.empty()) {
                        has_top_level_only = true;
                    }
                    paths.push_back(std::move(remaining_path));
                }

                if (has_top_level_only) {
                    uint64_t start_id = parquet_field->get_column_id();
                    uint64_t max_column_id = parquet_field->get_max_column_id();
                    for (uint64_t id = start_id; id <= max_column_id; ++id) {
                        out_ids.insert(id);
                    }
                } else if (!paths.empty()) {
                    HiveParquetNestedColumnUtils::extract_nested_column_ids_efficiently(
                            *parquet_field, paths, out_ids);
                }
            };

    for (const auto* slot : tuple_descriptor->slots()) {
        const auto& all_column_access_paths = slot->all_column_access_paths();

        // primitive (non-nested) types: direct mapping by name
        if ((slot->col_type() != TYPE_STRUCT && slot->col_type() != TYPE_ARRAY &&
             slot->col_type() != TYPE_MAP)) {
            column_ids.insert(table_col_name_to_field_schema_map[slot->col_name()]->column_id);

            // 检查是否是谓词列
            if (slot->is_predicate()) {
                filter_column_ids.insert(
                        table_col_name_to_field_schema_map[slot->col_name()]->column_id);
            }
            continue;
        }

        // complex types: find top-level parquet field first
        auto field_schema = table_col_name_to_field_schema_map[slot->col_name()];
        if (!field_schema) {
            continue;
        }

        // collect and process all_column_access_paths -> column_ids
        if (all_column_access_paths.__isset.name_access_paths &&
            !all_column_access_paths.name_access_paths.empty()) {
            process_access_paths(field_schema, all_column_access_paths.name_access_paths,
                                 column_ids);
        }

        // collect and process predicate_column_access_paths -> filter_column_ids
        const auto& predicate_column_access_paths = slot->predicate_column_access_paths();
        if (predicate_column_access_paths.__isset.name_access_paths &&
            !predicate_column_access_paths.name_access_paths.empty()) {
            process_access_paths(field_schema, predicate_column_access_paths.name_access_paths,
                                 filter_column_ids);
        }
    }

    return ColumnIdResult(std::move(column_ids), std::move(filter_column_ids));
}

ColumnIdResult HiveParquetReader::_create_column_ids2_by_top_level_col_index(const FieldDescriptor* field_desc,
                                                      const TupleDescriptor* tuple_descriptor) {
    std::shared_ptr<TableSchemaChangeHelper::Node> schema_node = nullptr;

    if (!field_desc) {
        return ColumnIdResult();
    }

    // First, assign column IDs to the field descriptor
    auto* mutable_field_desc = const_cast<FieldDescriptor*>(field_desc);
    mutable_field_desc->assign_ids();

    std::unordered_map<uint64_t, const FieldSchema*> table_col_pos_to_field_schema_map;
    for (int i = 0; i < field_desc->size(); ++i) {
        auto field_schema = field_desc->get_column(i);
        if (!field_schema) continue;

        table_col_pos_to_field_schema_map[i] = field_schema;
    }

    std::set<uint64_t> column_ids;
    std::set<uint64_t> filter_column_ids;

    // helper to process name access paths for a given top-level parquet field
    auto process_access_paths =
            [](const FieldSchema* parquet_field,
               const std::vector<TColumnNameAccessPath>& name_access_paths,
               std::set<uint64_t>& out_ids) {
                if (!parquet_field) return;
                if (name_access_paths.empty()) return;

                std::vector<TColumnNameAccessPath> paths;
                bool has_top_level_only = false;
                for (const auto& name_access_path : name_access_paths) {
                    DCHECK(name_access_path.path.size() >= 1);
                    TColumnNameAccessPath remaining_path;
                    if (name_access_path.path.size() > 1) {
                        remaining_path.path.assign(name_access_path.path.begin() + 1,
                                                   name_access_path.path.end());
                    } else {
                        // only top-level column name => means whole field
                        remaining_path.path = std::vector<std::string>();
                    }
                    if (remaining_path.path.empty()) {
                        has_top_level_only = true;
                    }
                    paths.push_back(std::move(remaining_path));
                }

                if (has_top_level_only) {
                    uint64_t start_id = parquet_field->get_column_id();
                    uint64_t max_column_id = parquet_field->get_max_column_id();
                    for (uint64_t id = start_id; id <= max_column_id; ++id) {
                        out_ids.insert(id);
                    }
                } else if (!paths.empty()) {
                    HiveParquetNestedColumnUtils::extract_nested_column_ids_efficiently(
                            *parquet_field, paths, out_ids);
                }
            };

    for (const auto* slot : tuple_descriptor->slots()) {
        const auto& all_column_access_paths = slot->all_column_access_paths();

        // primitive (non-nested) types: direct mapping by name
        if ((slot->col_type() != TYPE_STRUCT && slot->col_type() != TYPE_ARRAY &&
             slot->col_type() != TYPE_MAP)) {
            column_ids.insert(table_col_pos_to_field_schema_map[slot->col_pos()]->column_id);

            // 检查是否是谓词列
            if (slot->is_predicate()) {
                filter_column_ids.insert(
                        table_col_pos_to_field_schema_map[slot->col_pos()]->column_id);
            }
            continue;
        }

        // complex types: find top-level parquet field first
        auto field_schema = table_col_pos_to_field_schema_map[slot->col_pos()];
        if (!field_schema) {
            continue;
        }

        // collect and process all_column_access_paths -> column_ids
        if (all_column_access_paths.__isset.name_access_paths &&
            !all_column_access_paths.name_access_paths.empty()) {
            process_access_paths(field_schema, all_column_access_paths.name_access_paths,
                                 column_ids);
        }

        // collect and process predicate_column_access_paths -> filter_column_ids
        const auto& predicate_column_access_paths = slot->predicate_column_access_paths();
        if (predicate_column_access_paths.__isset.name_access_paths &&
            !predicate_column_access_paths.name_access_paths.empty()) {
            process_access_paths(field_schema, predicate_column_access_paths.name_access_paths,
                                 filter_column_ids);
        }
    }

    return ColumnIdResult(std::move(column_ids), std::move(filter_column_ids));
}



ColumnIdResult HiveParquetReader::_create_column_ids_by_top_level_col_index(
        const FieldDescriptor* field_desc, const TupleDescriptor* tuple_descriptor) {
    std::shared_ptr<TableSchemaChangeHelper::Node> schema_node = nullptr;

    if (!field_desc) {
        return ColumnIdResult();
    }

    // First, assign column IDs to the field descriptor
    auto* mutable_field_desc = const_cast<FieldDescriptor*>(field_desc);
    mutable_field_desc->assign_ids();

    // Group column paths by field ID - inspired by create_iceberg_projected_layout's sequence processing
    std::unordered_map<uint64_t, std::vector<TColumnNameAccessPath>> paths_by_table_col_index;
    std::unordered_map<uint64_t, std::vector<TColumnNameAccessPath>> predicate_paths_by_table_col_index;

    for (const auto* slot : tuple_descriptor->slots()) {
        const auto& all_column_access_paths = slot->all_column_access_paths();
        if ((slot->col_type() != TYPE_STRUCT && slot->col_type() != TYPE_ARRAY &&
             slot->col_type() != TYPE_MAP)) {
            TColumnNameAccessPath empty_path;
            empty_path.path = std::vector<std::string>();
            paths_by_table_col_index[slot->col_pos()].push_back(empty_path);
            continue;
        }

        // 处理 all_column_access_paths，用于生成 column_ids
        // 新规则：all_column_access_paths 包含顶层字段名字时表示访问顶层字段（读取所有子字段）
        if ((!all_column_access_paths.__isset.name_access_paths) ||
            all_column_access_paths.name_access_paths.empty()) {
            // 如果路径为空，创建一个空路径表示读取整个字段
            // TColumnNameAccessPath empty_path;
            // empty_path.path = std::vector<std::string>();
            // paths_by_table_col_index[slot->col_pos()].push_back(empty_path);
        } else {
            // 检查是否只包含顶层字段名（表示读取所有子字段）
            // bool is_top_level_access = false;
            // for (const auto& name_access_path : all_column_access_paths.name_access_paths) {
            //     if (name_access_path.path.size() == 1 &&
            //         name_access_path.path[0] == slot->col_name()) {
            //         // 顶层字段访问：创建空路径表示读取所有子字段
            //         TColumnNameAccessPath empty_path;
            //         empty_path.path = std::vector<std::string>();
            //         paths_by_table_col_index[slot->col_pos()].push_back(empty_path);
            //         is_top_level_access = true;
            //         break;
            //     }
            // }

            // if (!is_top_level_access) {
            //     // 非顶层访问，添加所有指定的路径
            //     for (const auto& name_access_path : all_column_access_paths.name_access_paths) {
            //         if (!name_access_path.path.empty()) {
            //             // std::string first_table_col_name = name_access_path.path[0];
            //             TColumnNameAccessPath remaining;
            //             if (name_access_path.path.size() > 1) {
            //                 remaining.path.assign(name_access_path.path.begin() + 1, name_access_path.path.end());
            //             }
            //             paths_by_table_col_index[slot->col_pos()].push_back(std::move(remaining));
            //         }
            //     }
            // }

            for (const auto& name_access_path : all_column_access_paths.name_access_paths) {
                DCHECK(name_access_path.path.size() >= 1 &&
                       name_access_path.path[0] == slot->col_name());
                if (!name_access_path.path.empty()) {
                    // std::string first_table_col_name = name_access_path.path[0];
                    TColumnNameAccessPath remaining;
                    if (name_access_path.path.size() > 1) {
                        remaining.path.assign(name_access_path.path.begin() + 1,
                                              name_access_path.path.end());
                    }
                    paths_by_table_col_index[slot->col_pos()].push_back(std::move(remaining));
                }
            }
        }

        if ((slot->col_type() != TYPE_STRUCT && slot->col_type() != TYPE_ARRAY &&
             slot->col_type() != TYPE_MAP)) {
            if (slot->is_predicate()) {
                TColumnNameAccessPath empty_path;
                empty_path.path = std::vector<std::string>();
                predicate_paths_by_table_col_index[slot->col_pos()].push_back(empty_path);
                continue;
            } else {
                // Skip non-predicate primitive type slots
                continue;
            }
        }

        const auto& predicate_column_access_paths = slot->predicate_column_access_paths();
        // 处理 all_column_access_paths，用于生成 column_ids
        // 新规则：all_column_access_paths 包含顶层字段名字时表示访问顶层字段（读取所有子字段）
        if ((!predicate_column_access_paths.__isset.name_access_paths) ||
            predicate_column_access_paths.name_access_paths.empty()) {
            // 如果路径为空，创建一个空路径表示读取整个字段
            // TColumnNameAccessPath empty_path;
            // empty_path.path = std::vector<std::string>();
            // predicate_paths_by_table_col_index[slot->col_name()].push_back(empty_path);
        } else {
            // 检查是否只包含顶层字段名（表示读取所有子字段）
            // bool is_top_level_access = false;
            // for (const auto& name_access_path : predicate_column_access_paths.name_access_paths) {
            //     if (name_access_path.path.size() == 1 &&
            //         name_access_path.path[0] == slot->col_name()) {
            //         // 顶层字段访问：创建空路径表示读取所有子字段
            //         TColumnNameAccessPath empty_path;
            //         empty_path.path = std::vector<std::string>();
            //         predicate_paths_by_table_col_index[slot->col_pos()].push_back(empty_path);
            //         is_top_level_access = true;
            //         break;
            //     }
            // }

            // if (!is_top_level_access) {
            //     // 非顶层访问，添加所有指定的路径
            //     for (const auto& name_access_path : predicate_column_access_paths.name_access_paths) {
            //         if (!name_access_path.path.empty()) {
            //             // std::string first_table_col_name = name_access_path.path[0];
            //             TColumnNameAccessPath remaining;
            //             if (name_access_path.path.size() > 1) {
            //                 remaining.path.assign(name_access_path.path.begin() + 1, name_access_path.path.end());
            //             }
            //             predicate_paths_by_table_col_index[slot->col_pos()].push_back(std::move(remaining));
            //         }
            //     }
            // }

            for (const auto& name_access_path : predicate_column_access_paths.name_access_paths) {
                DCHECK(name_access_path.path.size() >= 1 &&
                       name_access_path.path[0] == slot->col_name());
                if (!name_access_path.path.empty()) {
                    // std::string first_table_col_name = name_access_path.path[0];
                    TColumnNameAccessPath remaining;
                    if (name_access_path.path.size() > 1) {
                        remaining.path.assign(name_access_path.path.begin() + 1,
                                              name_access_path.path.end());
                    }
                    predicate_paths_by_table_col_index[slot->col_pos()].push_back(
                            std::move(remaining));
                }
            }
        }
    }

    auto column_ids = HiveParquetNestedColumnUtils::
            extract_schema_and_columns_efficiently_by_top_level_col_index(field_desc,
                                                                          paths_by_table_col_index);

    // Use the new merged efficient method
    auto filter_column_ids = HiveParquetNestedColumnUtils::
            extract_schema_and_columns_efficiently_by_top_level_col_index(
                    field_desc, predicate_paths_by_table_col_index);

    return ColumnIdResult(std::move(column_ids), std::move(filter_column_ids));
}

#include "common/compile_check_end.h"
} // namespace doris::vectorized
