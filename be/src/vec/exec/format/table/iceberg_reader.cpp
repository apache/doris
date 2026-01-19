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

#include "iceberg_reader.h"

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
#include <set>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/status.h"
#include "runtime/define_primitive_type.h"
#include "runtime/primitive_type.h"
#include "runtime/runtime_state.h"
#include "util/coding.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/exec/format/format_common.h"
#include "vec/exec/format/generic_reader.h"
#include "vec/exec/format/orc/vorc_reader.h"
#include "vec/exec/format/parquet/schema_desc.h"
#include "vec/exec/format/parquet/vparquet_column_chunk_reader.h"
#include "vec/exec/format/table/deletion_vector_reader.h"
#include "vec/exec/format/table/iceberg/iceberg_orc_nested_column_utils.h"
#include "vec/exec/format/table/iceberg/iceberg_parquet_nested_column_utils.h"
#include "vec/exec/format/table/nested_column_access_helper.h"
#include "vec/exec/format/table/table_format_reader.h"

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
namespace vectorized {
class VExprContext;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {
const std::string IcebergOrcReader::ICEBERG_ORC_ATTRIBUTE = "iceberg.id";

IcebergTableReader::IcebergTableReader(std::unique_ptr<GenericReader> file_format_reader,
                                       RuntimeProfile* profile, RuntimeState* state,
                                       const TFileScanRangeParams& params,
                                       const TFileRangeDesc& range, ShardedKVCache* kv_cache,
                                       io::IOContext* io_ctx, FileMetaCache* meta_cache)
        : TableFormatReader(std::move(file_format_reader), state, profile, params, range, io_ctx,
                            meta_cache),
          _kv_cache(kv_cache) {
    static const char* iceberg_profile = "IcebergProfile";
    ADD_TIMER(_profile, iceberg_profile);
    _iceberg_profile.num_delete_files =
            ADD_CHILD_COUNTER(_profile, "NumDeleteFiles", TUnit::UNIT, iceberg_profile);
    _iceberg_profile.num_delete_rows =
            ADD_CHILD_COUNTER(_profile, "NumDeleteRows", TUnit::UNIT, iceberg_profile);
    _iceberg_profile.delete_files_read_time =
            ADD_CHILD_TIMER(_profile, "DeleteFileReadTime", iceberg_profile);
    _iceberg_profile.delete_rows_sort_time =
            ADD_CHILD_TIMER(_profile, "DeleteRowsSortTime", iceberg_profile);
    _iceberg_profile.parse_delete_file_time =
            ADD_CHILD_TIMER(_profile, "ParseDeleteFileTime", iceberg_profile);
}

Status IcebergTableReader::get_next_block_inner(Block* block, size_t* read_rows, bool* eof) {
    RETURN_IF_ERROR(_expand_block_if_need(block));
    RETURN_IF_ERROR(_file_format_reader->get_next_block(block, read_rows, eof));

    if (_equality_delete_impls.size() > 0) {
        std::unique_ptr<IColumn::Filter> filter =
                std::make_unique<IColumn::Filter>(block->rows(), 1);
        for (auto& equality_delete_impl : _equality_delete_impls) {
            RETURN_IF_ERROR(equality_delete_impl->filter_data_block(
                    block, _col_name_to_block_idx, _id_to_block_column_name, *filter));
        }
        Block::filter_block_internal(block, *filter, block->columns());
    }

    *read_rows = block->rows();
    return _shrink_block_if_need(block);
}

Status IcebergTableReader::init_row_filters() {
    // We get the count value by doris's be, so we don't need to read the delete file
    if (_push_down_agg_type == TPushAggOp::type::COUNT && _table_level_row_count > 0) {
        return Status::OK();
    }

    const auto& table_desc = _range.table_format_params.iceberg_params;
    const auto& version = table_desc.format_version;
    if (version < MIN_SUPPORT_DELETE_FILES_VERSION) {
        return Status::OK();
    }

    std::vector<TIcebergDeleteFileDesc> position_delete_files;
    std::vector<TIcebergDeleteFileDesc> equality_delete_files;
    std::vector<TIcebergDeleteFileDesc> deletion_vector_files;
    for (const TIcebergDeleteFileDesc& desc : table_desc.delete_files) {
        if (desc.content == POSITION_DELETE) {
            position_delete_files.emplace_back(desc);
        } else if (desc.content == EQUALITY_DELETE) {
            equality_delete_files.emplace_back(desc);
        } else if (desc.content == DELETION_VECTOR) {
            deletion_vector_files.emplace_back(desc);
        }
    }

    if (!equality_delete_files.empty()) {
        RETURN_IF_ERROR(_process_equality_delete(equality_delete_files));
        _file_format_reader->set_push_down_agg_type(TPushAggOp::NONE);
    }

    if (!deletion_vector_files.empty()) {
        if (deletion_vector_files.size() != 1) [[unlikely]] {
            /*
             * Deletion vectors are a binary representation of deletes for a single data file that is more efficient
             * at execution time than position delete files. Unlike equality or position delete files, there can be
             * at most one deletion vector for a given data file in a snapshot.
             */
            return Status::DataQualityError("This iceberg data file has multiple DVs.");
        }
        RETURN_IF_ERROR(
                read_deletion_vector(table_desc.original_file_path, deletion_vector_files[0]));

        _file_format_reader->set_push_down_agg_type(TPushAggOp::NONE);
        // Readers can safely ignore position delete files if there is a DV for a data file.
    } else if (!position_delete_files.empty()) {
        RETURN_IF_ERROR(
                _position_delete_base(table_desc.original_file_path, position_delete_files));
        _file_format_reader->set_push_down_agg_type(TPushAggOp::NONE);
    }

    COUNTER_UPDATE(_iceberg_profile.num_delete_files, table_desc.delete_files.size());
    return Status::OK();
}

void IcebergTableReader::_generate_equality_delete_block(
        Block* block, const std::vector<std::string>& equality_delete_col_names,
        const std::vector<DataTypePtr>& equality_delete_col_types) {
    for (int i = 0; i < equality_delete_col_names.size(); ++i) {
        DataTypePtr data_type = make_nullable(equality_delete_col_types[i]);
        MutableColumnPtr data_column = data_type->create_column();
        block->insert(ColumnWithTypeAndName(std::move(data_column), data_type,
                                            equality_delete_col_names[i]));
    }
}

Status IcebergTableReader::_expand_block_if_need(Block* block) {
    std::set<std::string> names;
    auto block_names = block->get_names();
    names.insert(block_names.begin(), block_names.end());
    for (auto& col : _expand_columns) {
        col.column->assume_mutable()->clear();
        if (names.contains(col.name)) {
            return Status::InternalError("Wrong expand column '{}'", col.name);
        }
        names.insert(col.name);
        (*_col_name_to_block_idx)[col.name] = static_cast<uint32_t>(block->columns());
        block->insert(col);
    }
    return Status::OK();
}

Status IcebergTableReader::_shrink_block_if_need(Block* block) {
    std::set<size_t> positions_to_erase;
    for (const std::string& expand_col : _expand_col_names) {
        if (!_col_name_to_block_idx->contains(expand_col)) {
            return Status::InternalError("Wrong erase column '{}', block: {}", expand_col,
                                         block->dump_names());
        }
        positions_to_erase.emplace((*_col_name_to_block_idx)[expand_col]);
    }
    block->erase(positions_to_erase);
    for (const std::string& expand_col : _expand_col_names) {
        _col_name_to_block_idx->erase(expand_col);
    }
    return Status::OK();
}

Status IcebergTableReader::_position_delete_base(
        const std::string data_file_path, const std::vector<TIcebergDeleteFileDesc>& delete_files) {
    std::vector<DeleteRows*> delete_rows_array;
    int64_t num_delete_rows = 0;
    for (const auto& delete_file : delete_files) {
        SCOPED_TIMER(_iceberg_profile.delete_files_read_time);
        Status create_status = Status::OK();
        auto* delete_file_cache = _kv_cache->get<DeleteFile>(
                _delet_file_cache_key(delete_file.path), [&]() -> DeleteFile* {
                    auto* position_delete = new DeleteFile;
                    TFileRangeDesc delete_file_range;
                    // must use __set() method to make sure __isset is true
                    delete_file_range.__set_fs_name(_range.fs_name);
                    delete_file_range.path = delete_file.path;
                    delete_file_range.start_offset = 0;
                    delete_file_range.size = -1;
                    delete_file_range.file_size = -1;
                    //read position delete file base on delete_file_range , generate DeleteFile , add DeleteFile to kv_cache
                    create_status = _read_position_delete_file(&delete_file_range, position_delete);

                    if (!create_status) {
                        return nullptr;
                    }

                    return position_delete;
                });
        if (create_status.is<ErrorCode::END_OF_FILE>()) {
            continue;
        } else if (!create_status.ok()) {
            return create_status;
        }

        DeleteFile& delete_file_map = *((DeleteFile*)delete_file_cache);
        auto get_value = [&](const auto& v) {
            DeleteRows* row_ids = v.second.get();
            if (!row_ids->empty()) {
                delete_rows_array.emplace_back(row_ids);
                num_delete_rows += row_ids->size();
            }
        };
        delete_file_map.if_contains(data_file_path, get_value);
    }
    // Use a KV cache to store the delete rows corresponding to a data file path.
    // The Parquet/ORC reader holds a reference (pointer) to this cached entry.
    // This allows delete rows to be reused when a single data file is split into
    // multiple splits, avoiding excessive memory usage when delete rows are large.
    if (num_delete_rows > 0) {
        SCOPED_TIMER(_iceberg_profile.delete_rows_sort_time);
        _iceberg_delete_rows =
                _kv_cache->get<DeleteRows>(data_file_path,
                                           [&]() -> DeleteRows* {
                                               auto* data_file_position_delete = new DeleteRows;
                                               _sort_delete_rows(delete_rows_array, num_delete_rows,
                                                                 *data_file_position_delete);

                                               return data_file_position_delete;
                                           }

                );
        set_delete_rows();
        COUNTER_UPDATE(_iceberg_profile.num_delete_rows, num_delete_rows);
    }
    return Status::OK();
}

IcebergTableReader::PositionDeleteRange IcebergTableReader::_get_range(
        const ColumnDictI32& file_path_column) {
    IcebergTableReader::PositionDeleteRange range;
    size_t read_rows = file_path_column.get_data().size();
    const int* code_path = file_path_column.get_data().data();
    const int* code_path_start = code_path;
    const int* code_path_end = code_path + read_rows;
    while (code_path < code_path_end) {
        int code = code_path[0];
        const int* code_end = std::upper_bound(code_path, code_path_end, code);
        range.data_file_path.emplace_back(file_path_column.get_value(code).to_string());
        range.range.emplace_back(code_path - code_path_start, code_end - code_path_start);
        code_path = code_end;
    }
    return range;
}

IcebergTableReader::PositionDeleteRange IcebergTableReader::_get_range(
        const ColumnString& file_path_column) {
    IcebergTableReader::PositionDeleteRange range;
    size_t read_rows = file_path_column.size();
    size_t index = 0;
    while (index < read_rows) {
        StringRef data_path = file_path_column.get_data_at(index);
        size_t left = index - 1;
        size_t right = read_rows;
        while (left + 1 != right) {
            size_t mid = left + (right - left) / 2;
            if (file_path_column.get_data_at(mid) > data_path) {
                right = mid;
            } else {
                left = mid;
            }
        }
        range.data_file_path.emplace_back(data_path.to_string());
        range.range.emplace_back(index, left + 1);
        index = left + 1;
    }
    return range;
}

/**
 * https://iceberg.apache.org/spec/#position-delete-files
 * The rows in the delete file must be sorted by file_path then position to optimize filtering rows while scanning.
 * Sorting by file_path allows filter pushdown by file in columnar storage formats.
 * Sorting by position allows filtering rows while scanning, to avoid keeping deletes in memory.
 */
void IcebergTableReader::_sort_delete_rows(
        const std::vector<std::vector<int64_t>*>& delete_rows_array, int64_t num_delete_rows,
        std::vector<int64_t>& result) {
    if (delete_rows_array.empty()) {
        return;
    }
    if (delete_rows_array.size() == 1) {
        result.resize(num_delete_rows);
        memcpy(result.data(), delete_rows_array.front()->data(), sizeof(int64_t) * num_delete_rows);
        return;
    }
    if (delete_rows_array.size() == 2) {
        result.resize(num_delete_rows);
        std::merge(delete_rows_array.front()->begin(), delete_rows_array.front()->end(),
                   delete_rows_array.back()->begin(), delete_rows_array.back()->end(),
                   result.begin());
        return;
    }

    using vec_pair = std::pair<std::vector<int64_t>::iterator, std::vector<int64_t>::iterator>;
    result.resize(num_delete_rows);
    auto row_id_iter = result.begin();
    auto iter_end = result.end();
    std::vector<vec_pair> rows_array;
    for (auto* rows : delete_rows_array) {
        if (!rows->empty()) {
            rows_array.emplace_back(rows->begin(), rows->end());
        }
    }
    size_t array_size = rows_array.size();
    while (row_id_iter != iter_end) {
        int64_t min_index = 0;
        int64_t min = *rows_array[0].first;
        for (size_t i = 0; i < array_size; ++i) {
            if (*rows_array[i].first < min) {
                min_index = i;
                min = *rows_array[i].first;
            }
        }
        *row_id_iter++ = min;
        rows_array[min_index].first++;
        if (UNLIKELY(rows_array[min_index].first == rows_array[min_index].second)) {
            rows_array.erase(rows_array.begin() + min_index);
            array_size--;
        }
    }
}

void IcebergTableReader::_gen_position_delete_file_range(Block& block, DeleteFile* position_delete,
                                                         size_t read_rows,
                                                         bool file_path_column_dictionary_coded) {
    SCOPED_TIMER(_iceberg_profile.parse_delete_file_time);
    // todo: maybe do not need to build name to index map every time
    auto name_to_pos_map = block.get_name_to_pos_map();
    ColumnPtr path_column = block.get_by_position(name_to_pos_map[ICEBERG_FILE_PATH]).column;
    DCHECK_EQ(path_column->size(), read_rows);
    ColumnPtr pos_column = block.get_by_position(name_to_pos_map[ICEBERG_ROW_POS]).column;
    using ColumnType = typename PrimitiveTypeTraits<TYPE_BIGINT>::ColumnType;
    const int64_t* src_data = assert_cast<const ColumnType&>(*pos_column).get_data().data();
    IcebergTableReader::PositionDeleteRange range;
    if (file_path_column_dictionary_coded) {
        range = _get_range(assert_cast<const ColumnDictI32&>(*path_column));
    } else {
        range = _get_range(assert_cast<const ColumnString&>(*path_column));
    }
    for (int i = 0; i < range.range.size(); ++i) {
        std::string key = range.data_file_path[i];
        auto iter = position_delete->find(key);
        DeleteRows* delete_rows;
        if (iter == position_delete->end()) {
            delete_rows = new DeleteRows;
            std::unique_ptr<DeleteRows> delete_rows_ptr(delete_rows);
            (*position_delete)[key] = std::move(delete_rows_ptr);
        } else {
            delete_rows = iter->second.get();
        }
        const int64_t* cpy_start = src_data + range.range[i].first;
        const int64_t cpy_count = range.range[i].second - range.range[i].first;
        int64_t origin_size = delete_rows->size();
        delete_rows->resize(origin_size + cpy_count);
        int64_t* dest_position = &(*delete_rows)[origin_size];
        memcpy(dest_position, cpy_start, cpy_count * sizeof(int64_t));
    }
}

Status IcebergParquetReader::init_reader(
        const std::vector<std::string>& file_col_names,
        std::unordered_map<std::string, uint32_t>* col_name_to_block_idx,
        const VExprContextSPtrs& conjuncts,
        phmap::flat_hash_map<int, std::vector<std::shared_ptr<ColumnPredicate>>>&
                slot_id_to_predicates,
        const TupleDescriptor* tuple_descriptor, const RowDescriptor* row_descriptor,
        const std::unordered_map<std::string, int>* colname_to_slot_id,
        const VExprContextSPtrs* not_single_slot_filter_conjuncts,
        const std::unordered_map<int, VExprContextSPtrs>* slot_id_to_filter_conjuncts) {
    _file_format = Fileformat::PARQUET;
    _col_name_to_block_idx = col_name_to_block_idx;
    auto* parquet_reader = static_cast<ParquetReader*>(_file_format_reader.get());
    RETURN_IF_ERROR(parquet_reader->get_file_metadata_schema(&_data_file_field_desc));
    DCHECK(_data_file_field_desc != nullptr);

    auto column_id_result = _create_column_ids(_data_file_field_desc, tuple_descriptor);
    auto& column_ids = column_id_result.column_ids;
    const auto& filter_column_ids = column_id_result.filter_column_ids;

    RETURN_IF_ERROR(init_row_filters());

    if (!_params.__isset.history_schema_info || _params.history_schema_info.empty()) [[unlikely]] {
        RETURN_IF_ERROR(BuildTableInfoUtil::by_parquet_name(
                tuple_descriptor, *_data_file_field_desc, table_info_node_ptr));
    } else {
        std::set<std::string> read_col_name_set(file_col_names.begin(), file_col_names.end());

        bool exist_field_id = true;
        for (int idx = 0; idx < _data_file_field_desc->size(); idx++) {
            if (_data_file_field_desc->get_column(idx)->field_id == -1) {
                // the data file may be from hive table migrated to iceberg, field id is missing
                exist_field_id = false;
                break;
            }
        }
        const auto& table_schema = _params.history_schema_info.front().root_field;

        table_info_node_ptr = std::make_shared<TableSchemaChangeHelper::StructNode>();
        if (exist_field_id) {
            _all_required_col_names = file_col_names;

            // id -> table column name. columns that need read data file.
            std::unordered_map<int, std::shared_ptr<schema::external::TField>> id_to_table_field;
            for (const auto& table_field : table_schema.fields) {
                auto field = table_field.field_ptr;
                DCHECK(field->__isset.name);
                if (!read_col_name_set.contains(field->name)) {
                    continue;
                }
                id_to_table_field.emplace(field->id, field);
            }

            for (int idx = 0; idx < _data_file_field_desc->size(); idx++) {
                const auto& data_file_field = _data_file_field_desc->get_column(idx);
                auto data_file_column_id = _data_file_field_desc->get_column(idx)->field_id;

                if (id_to_table_field.contains(data_file_column_id)) {
                    const auto& table_field = id_to_table_field[data_file_column_id];

                    std::shared_ptr<TableSchemaChangeHelper::Node> field_node = nullptr;
                    RETURN_IF_ERROR(BuildTableInfoUtil::by_parquet_field_id(
                            *table_field, *data_file_field, exist_field_id, field_node));
                    table_info_node_ptr->add_children(table_field->name, data_file_field->name,
                                                      field_node);

                    _id_to_block_column_name.emplace(data_file_column_id, table_field->name);
                    id_to_table_field.erase(data_file_column_id);
                } else if (_equality_delete_col_ids.contains(data_file_column_id)) {
                    // Columns that need to be read for equality delete.
                    const static std::string EQ_DELETE_PRE = "__equality_delete_column__";

                    // Construct table column names that avoid duplication with current table schema.
                    // As the columns currently being read may have been deleted in the latest
                    // table structure or have undergone a series of schema changes...
                    std::string table_column_name = EQ_DELETE_PRE + data_file_field->name;
                    table_info_node_ptr->add_children(
                            table_column_name, data_file_field->name,
                            std::make_shared<TableSchemaChangeHelper::ConstNode>());

                    _id_to_block_column_name.emplace(data_file_column_id, table_column_name);
                    _expand_col_names.emplace_back(table_column_name);
                    auto expand_data_type = make_nullable(data_file_field->data_type);
                    _expand_columns.emplace_back(
                            ColumnWithTypeAndName {expand_data_type->create_column(),
                                                   expand_data_type, table_column_name});

                    _all_required_col_names.emplace_back(table_column_name);
                    column_ids.insert(data_file_field->get_column_id());
                }
            }
            for (const auto& [id, table_field] : id_to_table_field) {
                table_info_node_ptr->add_not_exist_children(table_field->name);
            }
        } else {
            if (!_equality_delete_col_ids.empty()) [[unlikely]] {
                return Status::InternalError(
                        "Can not read missing field id data file when have equality delete");
            }
            _all_required_col_names = file_col_names;
            std::map<std::string, size_t> file_column_idx_map;
            for (size_t idx = 0; idx < _data_file_field_desc->size(); idx++) {
                file_column_idx_map.emplace(_data_file_field_desc->get_column(idx)->name, idx);
            }

            for (const auto& table_field : table_schema.fields) {
                DCHECK(table_field.__isset.field_ptr);
                DCHECK(table_field.field_ptr->__isset.name);
                const auto& table_column_name = table_field.field_ptr->name;
                if (!read_col_name_set.contains(table_column_name)) {
                    continue;
                }
                if (!table_field.field_ptr->__isset.name_mapping ||
                    table_field.field_ptr->name_mapping.size() == 0) {
                    return Status::DataQualityError(
                            "name_mapping must be set when read missing field id data file.");
                }
                bool have_mapping = false;
                for (const auto& mapped_name : table_field.field_ptr->name_mapping) {
                    if (file_column_idx_map.contains(mapped_name)) {
                        std::shared_ptr<TableSchemaChangeHelper::Node> field_node = nullptr;
                        const auto& file_field = _data_file_field_desc->get_column(
                                file_column_idx_map.at(mapped_name));
                        RETURN_IF_ERROR(BuildTableInfoUtil::by_parquet_field_id(
                                *table_field.field_ptr, *file_field, exist_field_id, field_node));
                        table_info_node_ptr->add_children(table_column_name, file_field->name,
                                                          field_node);
                        have_mapping = true;
                        break;
                    }
                }
                if (!have_mapping) {
                    table_info_node_ptr->add_not_exist_children(table_column_name);
                }
            }
        }
    }

    return parquet_reader->init_reader(
            _all_required_col_names, _col_name_to_block_idx, conjuncts, slot_id_to_predicates,
            tuple_descriptor, row_descriptor, colname_to_slot_id, not_single_slot_filter_conjuncts,
            slot_id_to_filter_conjuncts, table_info_node_ptr, true, column_ids, filter_column_ids);
}

ColumnIdResult IcebergParquetReader::_create_column_ids(const FieldDescriptor* field_desc,
                                                        const TupleDescriptor* tuple_descriptor) {
    // First, assign column IDs to the field descriptor
    auto* mutable_field_desc = const_cast<FieldDescriptor*>(field_desc);
    mutable_field_desc->assign_ids();

    // map top-level table column iceberg_id -> FieldSchema*
    std::unordered_map<int, const FieldSchema*> iceberg_id_to_field_schema_map;

    for (int i = 0; i < field_desc->size(); ++i) {
        auto field_schema = field_desc->get_column(i);
        if (!field_schema) continue;

        int iceberg_id = field_schema->field_id;
        iceberg_id_to_field_schema_map[iceberg_id] = field_schema;
    }

    std::set<uint64_t> column_ids;
    std::set<uint64_t> filter_column_ids;

    // helper to process access paths for a given top-level parquet field
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
            // Column not found in file (e.g., partition column, added column)
            continue;
        }
        auto field_schema = it->second;

        // primitive (non-nested) types: direct mapping by name
        if ((slot->col_type() != TYPE_STRUCT && slot->col_type() != TYPE_ARRAY &&
             slot->col_type() != TYPE_MAP)) {
            column_ids.insert(field_schema->column_id);

            if (slot->is_predicate()) {
                filter_column_ids.insert(field_schema->column_id);
            }
            continue;
        }

        // complex types:
        const auto& all_access_paths = slot->all_access_paths();
        process_access_paths(field_schema, all_access_paths, column_ids);

        const auto& predicate_access_paths = slot->predicate_access_paths();
        if (!predicate_access_paths.empty()) {
            process_access_paths(field_schema, predicate_access_paths, filter_column_ids);
        }
    }
    return ColumnIdResult(std::move(column_ids), std::move(filter_column_ids));
}

Status IcebergParquetReader ::_read_position_delete_file(const TFileRangeDesc* delete_range,
                                                         DeleteFile* position_delete) {
    ParquetReader parquet_delete_reader(_profile, _params, *delete_range,
                                        READ_DELETE_FILE_BATCH_SIZE, &_state->timezone_obj(),
                                        _io_ctx, _state, _meta_cache);
    phmap::flat_hash_map<int, std::vector<std::shared_ptr<ColumnPredicate>>> tmp;
    RETURN_IF_ERROR(parquet_delete_reader.init_reader(
            delete_file_col_names,
            const_cast<std::unordered_map<std::string, uint32_t>*>(&DELETE_COL_NAME_TO_BLOCK_IDX),
            {}, tmp, nullptr, nullptr, nullptr, nullptr, nullptr,
            TableSchemaChangeHelper::ConstNode::get_instance(), false));

    std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>
            partition_columns;
    std::unordered_map<std::string, VExprContextSPtr> missing_columns;
    RETURN_IF_ERROR(parquet_delete_reader.set_fill_columns(partition_columns, missing_columns));

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

Status IcebergOrcReader::init_reader(
        const std::vector<std::string>& file_col_names,
        std::unordered_map<std::string, uint32_t>* col_name_to_block_idx,
        const VExprContextSPtrs& conjuncts, const TupleDescriptor* tuple_descriptor,
        const RowDescriptor* row_descriptor,
        const std::unordered_map<std::string, int>* colname_to_slot_id,
        const VExprContextSPtrs* not_single_slot_filter_conjuncts,
        const std::unordered_map<int, VExprContextSPtrs>* slot_id_to_filter_conjuncts) {
    _file_format = Fileformat::ORC;
    _col_name_to_block_idx = col_name_to_block_idx;
    auto* orc_reader = static_cast<OrcReader*>(_file_format_reader.get());
    RETURN_IF_ERROR(orc_reader->get_file_type(&_data_file_type_desc));
    std::vector<std::string> data_file_col_names;
    std::vector<DataTypePtr> data_file_col_types;
    RETURN_IF_ERROR(orc_reader->get_parsed_schema(&data_file_col_names, &data_file_col_types));

    auto column_id_result = _create_column_ids(_data_file_type_desc, tuple_descriptor);
    auto& column_ids = column_id_result.column_ids;
    const auto& filter_column_ids = column_id_result.filter_column_ids;

    RETURN_IF_ERROR(init_row_filters());

    if (!_params.__isset.history_schema_info || _params.history_schema_info.empty()) [[unlikely]] {
        RETURN_IF_ERROR(BuildTableInfoUtil::by_orc_name(tuple_descriptor, _data_file_type_desc,
                                                        table_info_node_ptr));
    } else {
        std::set<std::string> read_col_name_set(file_col_names.begin(), file_col_names.end());

        bool exist_field_id = true;
        for (size_t idx = 0; idx < _data_file_type_desc->getSubtypeCount(); idx++) {
            if (!_data_file_type_desc->getSubtype(idx)->hasAttributeKey(ICEBERG_ORC_ATTRIBUTE)) {
                exist_field_id = false;
                break;
            }
        }

        const auto& table_schema = _params.history_schema_info.front().root_field;
        table_info_node_ptr = std::make_shared<TableSchemaChangeHelper::StructNode>();
        if (exist_field_id) {
            _all_required_col_names = file_col_names;

            // id -> table column name. columns that need read data file.
            std::unordered_map<int, std::shared_ptr<schema::external::TField>> id_to_table_field;
            for (const auto& table_field : table_schema.fields) {
                auto field = table_field.field_ptr;
                DCHECK(field->__isset.name);
                if (!read_col_name_set.contains(field->name)) {
                    continue;
                }

                id_to_table_field.emplace(field->id, field);
            }

            for (int idx = 0; idx < _data_file_type_desc->getSubtypeCount(); idx++) {
                const auto& data_file_field = _data_file_type_desc->getSubtype(idx);
                auto data_file_column_id =
                        std::stoi(data_file_field->getAttributeValue(ICEBERG_ORC_ATTRIBUTE));
                auto const& file_column_name = _data_file_type_desc->getFieldName(idx);

                if (id_to_table_field.contains(data_file_column_id)) {
                    const auto& table_field = id_to_table_field[data_file_column_id];

                    std::shared_ptr<TableSchemaChangeHelper::Node> field_node = nullptr;
                    RETURN_IF_ERROR(BuildTableInfoUtil::by_orc_field_id(
                            *table_field, data_file_field, ICEBERG_ORC_ATTRIBUTE, exist_field_id,
                            field_node));
                    table_info_node_ptr->add_children(table_field->name, file_column_name,
                                                      field_node);

                    _id_to_block_column_name.emplace(data_file_column_id, table_field->name);
                    id_to_table_field.erase(data_file_column_id);
                } else if (_equality_delete_col_ids.contains(data_file_column_id)) {
                    // Columns that need to be read for equality delete.
                    const static std::string EQ_DELETE_PRE = "__equality_delete_column__";

                    // Construct table column names that avoid duplication with current table schema.
                    // As the columns currently being read may have been deleted in the latest
                    // table structure or have undergone a series of schema changes...
                    std::string table_column_name = EQ_DELETE_PRE + file_column_name;
                    table_info_node_ptr->add_children(
                            table_column_name, file_column_name,
                            std::make_shared<TableSchemaChangeHelper::ConstNode>());

                    _id_to_block_column_name.emplace(data_file_column_id, table_column_name);
                    _expand_col_names.emplace_back(table_column_name);

                    auto expand_data_type = make_nullable(data_file_col_types[idx]);
                    _expand_columns.emplace_back(
                            ColumnWithTypeAndName {expand_data_type->create_column(),
                                                   expand_data_type, table_column_name});

                    _all_required_col_names.emplace_back(table_column_name);
                    column_ids.insert(data_file_field->getColumnId());
                }
            }
            for (const auto& [id, table_field] : id_to_table_field) {
                table_info_node_ptr->add_not_exist_children(table_field->name);
            }
        } else {
            if (!_equality_delete_col_ids.empty()) [[unlikely]] {
                return Status::InternalError(
                        "Can not read missing field id data file when have equality delete");
            }
            _all_required_col_names = file_col_names;
            std::map<std::string, size_t> file_column_idx_map;
            for (int idx = 0; idx < _data_file_type_desc->getSubtypeCount(); idx++) {
                auto const& file_column_name = _data_file_type_desc->getFieldName(idx);
                file_column_idx_map.emplace(file_column_name, idx);
            }

            for (const auto& table_field : table_schema.fields) {
                DCHECK(table_field.__isset.field_ptr);
                DCHECK(table_field.field_ptr->__isset.name);
                const auto& table_column_name = table_field.field_ptr->name;
                if (!read_col_name_set.contains(table_column_name)) {
                    continue;
                }
                if (!table_field.field_ptr->__isset.name_mapping ||
                    table_field.field_ptr->name_mapping.size() == 0) {
                    return Status::DataQualityError(
                            "name_mapping must be set when read missing field id data file.");
                }
                auto have_mapping = false;
                for (const auto& mapped_name : table_field.field_ptr->name_mapping) {
                    if (file_column_idx_map.contains(mapped_name)) {
                        auto file_column_idx = file_column_idx_map.at(mapped_name);
                        std::shared_ptr<TableSchemaChangeHelper::Node> field_node = nullptr;
                        const auto& file_field = _data_file_type_desc->getSubtype(file_column_idx);
                        RETURN_IF_ERROR(BuildTableInfoUtil::by_orc_field_id(
                                *table_field.field_ptr, file_field, ICEBERG_ORC_ATTRIBUTE,
                                exist_field_id, field_node));
                        table_info_node_ptr->add_children(
                                table_column_name,
                                _data_file_type_desc->getFieldName(file_column_idx), field_node);
                        have_mapping = true;
                        break;
                    }
                }
                if (!have_mapping) {
                    table_info_node_ptr->add_not_exist_children(table_column_name);
                }
            }
        }
    }

    return orc_reader->init_reader(&_all_required_col_names, _col_name_to_block_idx, conjuncts,
                                   false, tuple_descriptor, row_descriptor,
                                   not_single_slot_filter_conjuncts, slot_id_to_filter_conjuncts,
                                   table_info_node_ptr, column_ids, filter_column_ids);
}

ColumnIdResult IcebergOrcReader::_create_column_ids(const orc::Type* orc_type,
                                                    const TupleDescriptor* tuple_descriptor) {
    // map top-level table column iceberg_id -> orc::Type*
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

    // helper to process access paths for a given top-level orc field
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
            // Column not found in file
            continue;
        }
        const orc::Type* orc_field = it->second;

        // primitive (non-nested) types
        if ((slot->col_type() != TYPE_STRUCT && slot->col_type() != TYPE_ARRAY &&
             slot->col_type() != TYPE_MAP)) {
            column_ids.insert(orc_field->getColumnId());
            if (slot->is_predicate()) {
                filter_column_ids.insert(orc_field->getColumnId());
            }
            continue;
        }

        // complex types
        const auto& all_access_paths = slot->all_access_paths();
        process_access_paths(orc_field, all_access_paths, column_ids);

        const auto& predicate_access_paths = slot->predicate_access_paths();
        if (!predicate_access_paths.empty()) {
            process_access_paths(orc_field, predicate_access_paths, filter_column_ids);
        }
    }

    return ColumnIdResult(std::move(column_ids), std::move(filter_column_ids));
}

Status IcebergOrcReader::_read_position_delete_file(const TFileRangeDesc* delete_range,
                                                    DeleteFile* position_delete) {
    OrcReader orc_delete_reader(_profile, _state, _params, *delete_range,
                                READ_DELETE_FILE_BATCH_SIZE, _state->timezone(), _io_ctx,
                                _meta_cache);
    RETURN_IF_ERROR(orc_delete_reader.init_reader(
            &delete_file_col_names,
            const_cast<std::unordered_map<std::string, uint32_t>*>(&DELETE_COL_NAME_TO_BLOCK_IDX),
            {}, false, {}, {}, nullptr, nullptr));

    std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>
            partition_columns;
    std::unordered_map<std::string, VExprContextSPtr> missing_columns;
    RETURN_IF_ERROR(orc_delete_reader.set_fill_columns(partition_columns, missing_columns));

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

// Directly read the deletion vector using the `content_offset` and
// `content_size_in_bytes` provided by FE in `delete_file_desc`.
// These two fields indicate the location of a blob in storage.
// Since the current format is `deletion-vector-v1`, which does not
// compress any blobs, we can temporarily skip parsing the Puffin footer.
Status IcebergTableReader::read_deletion_vector(const std::string& data_file_path,
                                                const TIcebergDeleteFileDesc& delete_file_desc) {
    Status create_status = Status::OK();
    SCOPED_TIMER(_iceberg_profile.delete_files_read_time);
    _iceberg_delete_rows = _kv_cache->get<DeleteRows>(data_file_path, [&]() -> DeleteRows* {
        auto* delete_rows = new DeleteRows;

        TFileRangeDesc delete_range;
        // must use __set() method to make sure __isset is true
        delete_range.__set_fs_name(_range.fs_name);
        delete_range.path = delete_file_desc.path;
        delete_range.start_offset = delete_file_desc.content_offset;
        delete_range.size = delete_file_desc.content_size_in_bytes;
        delete_range.file_size = -1;

        // We may consider caching the DeletionVectorReader when reading Puffin files,
        // where the underlying reader is an `InMemoryFileReader` and a single data file is
        // split into multiple splits. However, we need to ensure that the underlying
        // reader supports multi-threaded access.
        DeletionVectorReader dv_reader(_state, _profile, _params, delete_range, _io_ctx);
        create_status = dv_reader.open();
        if (!create_status.ok()) [[unlikely]] {
            return nullptr;
        }

        size_t buffer_size = delete_range.size;
        std::vector<char> buf(buffer_size);
        if (buffer_size < 12) [[unlikely]] {
            // Minimum size: 4 bytes length + 4 bytes magic + 4 bytes CRC32
            create_status = Status::DataQualityError("Deletion vector file size too small: {}",
                                                     buffer_size);
            return nullptr;
        }

        create_status = dv_reader.read_at(delete_range.start_offset, {buf.data(), buffer_size});
        if (!create_status) [[unlikely]] {
            return nullptr;
        }
        // The serialized blob contains:
        //
        // Combined length of the vector and magic bytes stored as 4 bytes, big-endian
        // A 4-byte magic sequence, D1 D3 39 64
        // The vector, serialized as described below
        // A CRC-32 checksum of the magic bytes and serialized vector as 4 bytes, big-endian

        auto total_length = BigEndian::Load32(buf.data());
        if (total_length + 8 != buffer_size) [[unlikely]] {
            create_status = Status::DataQualityError(
                    "Deletion vector length mismatch, expected: {}, actual: {}", total_length + 8,
                    buffer_size);
            return nullptr;
        }

        constexpr static char MAGIC_NUMBER[] = {'\xD1', '\xD3', '\x39', '\x64'};
        if (memcmp(buf.data() + sizeof(total_length), MAGIC_NUMBER, 4)) [[unlikely]] {
            create_status = Status::DataQualityError("Deletion vector magic number mismatch");
            return nullptr;
        }

        roaring::Roaring64Map bitmap;
        SCOPED_TIMER(_iceberg_profile.parse_delete_file_time);
        try {
            bitmap = roaring::Roaring64Map::readSafe(buf.data() + 8, buffer_size - 12);
        } catch (const std::runtime_error& e) {
            create_status = Status::DataQualityError("Decode roaring bitmap failed, {}", e.what());
            return nullptr;
        }
        // skip CRC-32 checksum

        delete_rows->reserve(bitmap.cardinality());
        for (auto it = bitmap.begin(); it != bitmap.end(); it++) {
            delete_rows->push_back(*it);
        }
        COUNTER_UPDATE(_iceberg_profile.num_delete_rows, delete_rows->size());
        return delete_rows;
    });

    RETURN_IF_ERROR(create_status);
    if (!_iceberg_delete_rows->empty()) [[likely]] {
        set_delete_rows();
    }
    return Status::OK();
}

Status IcebergParquetReader::_process_equality_delete(
        const std::vector<TIcebergDeleteFileDesc>& delete_files) {
    std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>
            partition_columns;
    std::unordered_map<std::string, VExprContextSPtr> missing_columns;

    std::map<int, const FieldSchema*> data_file_id_to_field_schema;
    for (int idx = 0; idx < _data_file_field_desc->size(); ++idx) {
        auto field_schema = _data_file_field_desc->get_column(idx);
        if (_data_file_field_desc->get_column(idx)->field_id == -1) {
            return Status::DataQualityError("Iceberg equality delete data file missing field id.");
        }
        data_file_id_to_field_schema[_data_file_field_desc->get_column(idx)->field_id] =
                field_schema;
    }

    for (const auto& delete_file : delete_files) {
        TFileRangeDesc delete_desc;
        // must use __set() method to make sure __isset is true
        delete_desc.__set_fs_name(_range.fs_name);
        delete_desc.path = delete_file.path;
        delete_desc.start_offset = 0;
        delete_desc.size = -1;
        delete_desc.file_size = -1;

        if (!delete_file.__isset.field_ids) [[unlikely]] {
            return Status::InternalError(
                    "missing delete field ids when reading equality delete file");
        }
        auto& read_column_field_ids = delete_file.field_ids;
        std::set<int> read_column_field_ids_set;
        for (const auto& field_id : read_column_field_ids) {
            read_column_field_ids_set.insert(field_id);
            _equality_delete_col_ids.insert(field_id);
        }

        auto delete_reader = ParquetReader::create_unique(
                _profile, _params, delete_desc, READ_DELETE_FILE_BATCH_SIZE,
                &_state->timezone_obj(), _io_ctx, _state, _meta_cache);
        RETURN_IF_ERROR(delete_reader->init_schema_reader());

        // the column that to read equality delete file.
        // (delete file may be have extra columns that don't need to read)
        std::vector<std::string> delete_col_names;
        std::vector<DataTypePtr> delete_col_types;
        std::vector<int> delete_col_ids;
        std::unordered_map<std::string, uint32_t> delete_col_name_to_block_idx;

        const FieldDescriptor* delete_field_desc = nullptr;
        RETURN_IF_ERROR(delete_reader->get_file_metadata_schema(&delete_field_desc));
        DCHECK(delete_field_desc != nullptr);

        auto eq_file_node = std::make_shared<TableSchemaChangeHelper::StructNode>();
        for (const auto& delete_file_field : delete_field_desc->get_fields_schema()) {
            if (delete_file_field.field_id == -1) [[unlikely]] { // missing delete_file_field id
                // equality delete file must have delete_file_field id to match column.
                return Status::DataQualityError(
                        "missing delete_file_field id when reading equality delete file");
            } else if (read_column_field_ids_set.contains(delete_file_field.field_id)) {
                // the column that need to read.
                if (delete_file_field.children.size() > 0) [[unlikely]] { // complex column
                    return Status::InternalError(
                            "can not support read complex column in equality delete file");
                } else if (!data_file_id_to_field_schema.contains(delete_file_field.field_id))
                        [[unlikely]] {
                    return Status::DataQualityError(
                            "can not find delete field id in data file schema when reading "
                            "equality delete file");
                }
                auto data_file_field = data_file_id_to_field_schema[delete_file_field.field_id];
                if (data_file_field->data_type->get_primitive_type() !=
                    delete_file_field.data_type->get_primitive_type()) [[unlikely]] {
                    return Status::NotSupported(
                            "Not Support type change in equality delete, field: {}, delete "
                            "file type: {}, data file type: {}",
                            delete_file_field.field_id, delete_file_field.data_type->get_name(),
                            data_file_field->data_type->get_name());
                }

                std::string filed_lower_name = to_lower(delete_file_field.name);
                eq_file_node->add_children(filed_lower_name, delete_file_field.name,
                                           std::make_shared<TableSchemaChangeHelper::ScalarNode>());

                delete_col_ids.emplace_back(delete_file_field.field_id);
                delete_col_names.emplace_back(filed_lower_name);
                delete_col_types.emplace_back(make_nullable(delete_file_field.data_type));

                read_column_field_ids_set.erase(delete_file_field.field_id);
            } else {
                // delete file may be have extra columns that don't need to read
            }
        }
        if (!read_column_field_ids_set.empty()) [[unlikely]] {
            return Status::DataQualityError("some field ids not found in equality delete file.");
        }

        for (uint32_t idx = 0; idx < delete_col_names.size(); ++idx) {
            delete_col_name_to_block_idx[delete_col_names[idx]] = idx;
        }
        phmap::flat_hash_map<int, std::vector<std::shared_ptr<ColumnPredicate>>> tmp;
        RETURN_IF_ERROR(delete_reader->init_reader(delete_col_names, &delete_col_name_to_block_idx,
                                                   {}, tmp, nullptr, nullptr, nullptr, nullptr,
                                                   nullptr, eq_file_node, false));
        RETURN_IF_ERROR(delete_reader->set_fill_columns(partition_columns, missing_columns));

        if (!_equality_delete_block_map.contains(delete_col_ids)) {
            _equality_delete_block_map.emplace(delete_col_ids, _equality_delete_blocks.size());
            Block block;
            _generate_equality_delete_block(&block, delete_col_names, delete_col_types);
            _equality_delete_blocks.emplace_back(block);
        }
        Block& eq_file_block = _equality_delete_blocks[_equality_delete_block_map[delete_col_ids]];
        bool eof = false;
        while (!eof) {
            Block tmp_block;
            _generate_equality_delete_block(&tmp_block, delete_col_names, delete_col_types);
            size_t read_rows = 0;
            RETURN_IF_ERROR(delete_reader->get_next_block(&tmp_block, &read_rows, &eof));
            if (read_rows > 0) {
                MutableBlock mutable_block(&eq_file_block);
                RETURN_IF_ERROR(mutable_block.merge(tmp_block));
            }
        }
    }

    for (const auto& [delete_col_ids, block_idx] : _equality_delete_block_map) {
        auto& eq_file_block = _equality_delete_blocks[block_idx];
        auto equality_delete_impl =
                EqualityDeleteBase::get_delete_impl(&eq_file_block, delete_col_ids);
        RETURN_IF_ERROR(equality_delete_impl->init(_profile));
        _equality_delete_impls.emplace_back(std::move(equality_delete_impl));
    }
    return Status::OK();
}

Status IcebergOrcReader::_process_equality_delete(
        const std::vector<TIcebergDeleteFileDesc>& delete_files) {
    std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>
            partition_columns;
    std::unordered_map<std::string, VExprContextSPtr> missing_columns;

    std::map<int, int> data_file_id_to_field_idx;
    for (int idx = 0; idx < _data_file_type_desc->getSubtypeCount(); ++idx) {
        if (!_data_file_type_desc->getSubtype(idx)->hasAttributeKey(ICEBERG_ORC_ATTRIBUTE)) {
            return Status::DataQualityError("Iceberg equality delete data file missing field id.");
        }
        auto field_id = std::stoi(
                _data_file_type_desc->getSubtype(idx)->getAttributeValue(ICEBERG_ORC_ATTRIBUTE));
        data_file_id_to_field_idx[field_id] = idx;
    }

    for (const auto& delete_file : delete_files) {
        TFileRangeDesc delete_desc;
        // must use __set() method to make sure __isset is true
        delete_desc.__set_fs_name(_range.fs_name);
        delete_desc.path = delete_file.path;
        delete_desc.start_offset = 0;
        delete_desc.size = -1;
        delete_desc.file_size = -1;

        if (!delete_file.__isset.field_ids) [[unlikely]] {
            return Status::InternalError(
                    "missing delete field ids when reading equality delete file");
        }
        auto& read_column_field_ids = delete_file.field_ids;
        std::set<int> read_column_field_ids_set;
        for (const auto& field_id : read_column_field_ids) {
            read_column_field_ids_set.insert(field_id);
            _equality_delete_col_ids.insert(field_id);
        }

        auto delete_reader = OrcReader::create_unique(_profile, _state, _params, delete_desc,
                                                      READ_DELETE_FILE_BATCH_SIZE,
                                                      _state->timezone(), _io_ctx, _meta_cache);
        RETURN_IF_ERROR(delete_reader->init_schema_reader());
        // delete file schema
        std::vector<std::string> delete_file_col_names;
        std::vector<DataTypePtr> delete_file_col_types;
        RETURN_IF_ERROR(
                delete_reader->get_parsed_schema(&delete_file_col_names, &delete_file_col_types));

        // the column that to read equality delete file.
        // (delete file maybe have extra columns that don't need to read)
        std::vector<std::string> delete_col_names;
        std::vector<DataTypePtr> delete_col_types;
        std::vector<int> delete_col_ids;
        std::unordered_map<std::string, uint32_t> delete_col_name_to_block_idx;

        const orc::Type* delete_field_desc = nullptr;
        RETURN_IF_ERROR(delete_reader->get_file_type(&delete_field_desc));
        DCHECK(delete_field_desc != nullptr);

        auto eq_file_node = std::make_shared<TableSchemaChangeHelper::StructNode>();

        for (size_t idx = 0; idx < delete_field_desc->getSubtypeCount(); idx++) {
            auto delete_file_field = delete_field_desc->getSubtype(idx);

            if (!delete_file_field->hasAttributeKey(ICEBERG_ORC_ATTRIBUTE))
                    [[unlikely]] { // missing delete_file_field id
                // equality delete file must have delete_file_field id to match column.
                return Status::DataQualityError(
                        "missing delete_file_field id when reading equality delete file");
            } else {
                auto delete_field_id =
                        std::stoi(delete_file_field->getAttributeValue(ICEBERG_ORC_ATTRIBUTE));
                if (read_column_field_ids_set.contains(delete_field_id)) {
                    // the column that need to read.
                    if (is_complex_type(delete_file_col_types[idx]->get_primitive_type()))
                            [[unlikely]] {
                        return Status::InternalError(
                                "can not support read complex column in equality delete file.");
                    } else if (!data_file_id_to_field_idx.contains(delete_field_id)) [[unlikely]] {
                        return Status::DataQualityError(
                                "can not find delete field id in data file schema when reading "
                                "equality delete file");
                    }

                    auto data_file_field = _data_file_type_desc->getSubtype(
                            data_file_id_to_field_idx[delete_field_id]);

                    if (delete_file_field->getKind() != data_file_field->getKind()) [[unlikely]] {
                        return Status::NotSupported(
                                "Not Support type change in equality delete, field: {}, delete "
                                "file type: {}, data file type: {}",
                                delete_field_id, delete_file_field->getKind(),
                                data_file_field->getKind());
                    }
                    std::string filed_lower_name = to_lower(delete_field_desc->getFieldName(idx));
                    eq_file_node->add_children(
                            filed_lower_name, delete_field_desc->getFieldName(idx),
                            std::make_shared<TableSchemaChangeHelper::ScalarNode>());

                    delete_col_ids.emplace_back(delete_field_id);
                    delete_col_names.emplace_back(filed_lower_name);
                    delete_col_types.emplace_back(make_nullable(delete_file_col_types[idx]));
                    read_column_field_ids_set.erase(delete_field_id);
                }
            }
        }
        if (!read_column_field_ids_set.empty()) [[unlikely]] {
            return Status::DataQualityError("some field ids not found in equality delete file.");
        }

        for (uint32_t idx = 0; idx < delete_col_names.size(); ++idx) {
            delete_col_name_to_block_idx[delete_col_names[idx]] = idx;
        }

        RETURN_IF_ERROR(delete_reader->init_reader(&delete_col_names, &delete_col_name_to_block_idx,
                                                   {}, false, nullptr, nullptr, nullptr, nullptr,
                                                   eq_file_node));
        RETURN_IF_ERROR(delete_reader->set_fill_columns(partition_columns, missing_columns));

        if (!_equality_delete_block_map.contains(delete_col_ids)) {
            _equality_delete_block_map.emplace(delete_col_ids, _equality_delete_blocks.size());
            Block block;
            _generate_equality_delete_block(&block, delete_col_names, delete_col_types);
            _equality_delete_blocks.emplace_back(block);
        }
        Block& eq_file_block = _equality_delete_blocks[_equality_delete_block_map[delete_col_ids]];
        bool eof = false;
        while (!eof) {
            Block tmp_block;
            _generate_equality_delete_block(&tmp_block, delete_col_names, delete_col_types);
            size_t read_rows = 0;
            RETURN_IF_ERROR(delete_reader->get_next_block(&tmp_block, &read_rows, &eof));
            if (read_rows > 0) {
                MutableBlock mutable_block(&eq_file_block);
                RETURN_IF_ERROR(mutable_block.merge(tmp_block));
            }
        }
    }

    for (const auto& [delete_col_ids, block_idx] : _equality_delete_block_map) {
        auto& eq_file_block = _equality_delete_blocks[block_idx];
        auto equality_delete_impl =
                EqualityDeleteBase::get_delete_impl(&eq_file_block, delete_col_ids);
        RETURN_IF_ERROR(equality_delete_impl->init(_profile));
        _equality_delete_impls.emplace_back(std::move(equality_delete_impl));
    }
    return Status::OK();
}
#include "common/compile_check_end.h"
} // namespace doris::vectorized
