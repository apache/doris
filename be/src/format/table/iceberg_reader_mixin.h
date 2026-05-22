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

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/consts.h"
#include "common/status.h"
#include "core/block/block.h"
#include "core/column/column_dictionary.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_struct.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "format/generic_reader.h"
#include "format/table/deletion_vector_reader.h"
#include "format/table/equality_delete.h"
#include "format/table/table_schema_change_helper.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"
#include "storage/olap_common.h"

namespace doris {
class TIcebergDeleteFileDesc;
} // namespace doris

namespace doris {

class ShardedKVCache;

// CRTP mixin for Iceberg reader functionality.
// BaseReader should be ParquetReader or OrcReader.
// Inherits BaseReader + TableSchemaChangeHelper, providing shared Iceberg logic
// (delete files, deletion vectors, equality delete, $row_id synthesis).
//
// Inheritance chain:
//   IcebergParquetReader -> IcebergReaderMixin<ParquetReader> -> ParquetReader -> GenericReader
//   IcebergOrcReader     -> IcebergReaderMixin<OrcReader>     -> OrcReader     -> GenericReader
template <typename BaseReader>
class IcebergReaderMixin : public BaseReader, public TableSchemaChangeHelper {
public:
    struct PositionDeleteRange {
        std::vector<std::string> data_file_path;
        std::vector<std::pair<int, int>> range;
    };

    // Forward BaseReader constructor arguments + Iceberg-specific kv_cache
    template <typename... Args>
    IcebergReaderMixin(ShardedKVCache* kv_cache, Args&&... args)
            : BaseReader(std::forward<Args>(args)...), _kv_cache(kv_cache) {
        static const char* iceberg_profile = "IcebergProfile";
        ADD_TIMER(this->get_profile(), iceberg_profile);
        _iceberg_profile.num_delete_files = ADD_CHILD_COUNTER(this->get_profile(), "NumDeleteFiles",
                                                              TUnit::UNIT, iceberg_profile);
        _iceberg_profile.num_delete_rows = ADD_CHILD_COUNTER(this->get_profile(), "NumDeleteRows",
                                                             TUnit::UNIT, iceberg_profile);
        _iceberg_profile.delete_files_read_time =
                ADD_CHILD_TIMER(this->get_profile(), "DeleteFileReadTime", iceberg_profile);
        _iceberg_profile.delete_rows_sort_time =
                ADD_CHILD_TIMER(this->get_profile(), "DeleteRowsSortTime", iceberg_profile);
        _iceberg_profile.parse_delete_file_time =
                ADD_CHILD_TIMER(this->get_profile(), "ParseDeleteFileTime", iceberg_profile);
    }

    ~IcebergReaderMixin() override = default;

    void set_current_file_info(const std::string& file_path, int32_t partition_spec_id,
                               const std::string& partition_data_json) {
        _current_file_path = file_path;
        _partition_spec_id = partition_spec_id;
        _partition_data_json = partition_data_json;
    }

    enum { DATA, POSITION_DELETE, EQUALITY_DELETE, DELETION_VECTOR };
    enum Fileformat { NONE, PARQUET, ORC, AVRO };

    virtual void set_delete_rows() = 0;

    // Table-level COUNT(*) is handled by CountReader (created by FileScanner after
    // init_reader). If _do_get_next_block is called, COUNT must have been resolved.
    Status _do_get_next_block(Block* block, size_t* read_rows, bool* eof) override {
        DCHECK(this->_push_down_agg_type != TPushAggOp::type::COUNT);
        return BaseReader::_do_get_next_block(block, read_rows, eof);
    }

    void set_create_row_id_column_iterator_func(
            std::function<std::shared_ptr<segment_v2::RowIdColumnIteratorV2>()> create_func) {
        _create_topn_row_id_column_iterator = create_func;
    }

protected:
    // ---- Hook implementations ----

    // Called before reading a block: expand block for equality delete columns + detect row_id
    Status on_before_read_block(Block* block) override {
        RETURN_IF_ERROR(_expand_block_if_need(block));
        return Status::OK();
    }

    /// Fill Iceberg $row_id synthesized column. Registered as handler during init.
    Status _fill_iceberg_row_id(Block* block, size_t rows) {
        int row_id_pos = block->get_position_by_name(BeConsts::ICEBERG_ROWID_COL);
        DORIS_CHECK(row_id_pos >= 0);

        // Lazy-init file info: only set when $row_id is actually needed.
        const auto& table_desc = this->get_scan_range().table_format_params.iceberg_params;
        std::string file_path = table_desc.original_file_path;
        int32_t partition_spec_id =
                table_desc.__isset.partition_spec_id ? table_desc.partition_spec_id : 0;
        std::string partition_data_json;
        if (table_desc.__isset.partition_data_json) {
            partition_data_json = table_desc.partition_data_json;
        }
        set_current_file_info(file_path, partition_spec_id, partition_data_json);

        const auto& row_ids = this->current_batch_row_positions();
        auto& col_with_type = block->get_by_position(static_cast<size_t>(row_id_pos));
        MutableColumnPtr row_id_column;
        RETURN_IF_ERROR(_build_iceberg_rowid_column(col_with_type.type, _current_file_path, row_ids,
                                                    _partition_spec_id, _partition_data_json,
                                                    &row_id_column));
        col_with_type.column = std::move(row_id_column);
        return Status::OK();
    }

    void _init_row_lineage_columns() {
        const auto& table_desc = this->get_scan_range().table_format_params.iceberg_params;
        if (table_desc.__isset.first_row_id) {
            _row_lineage_columns.first_row_id = table_desc.first_row_id;
        }
        if (table_desc.__isset.last_updated_sequence_number) {
            _row_lineage_columns.last_updated_sequence_number =
                    table_desc.last_updated_sequence_number;
        }
    }

    Status _fill_row_lineage_row_id(Block* block, size_t rows) {
        int col_pos = block->get_position_by_name(ROW_LINEAGE_ROW_ID);
        DORIS_CHECK(col_pos >= 0);

        if (_row_lineage_columns.first_row_id >= 0) {
            auto col = block->get_by_position(col_pos).column->assume_mutable();
            auto* nullable_column = assert_cast<ColumnNullable*>(col.get());
            auto& null_map = nullable_column->get_null_map_data();
            auto& data =
                    assert_cast<ColumnInt64&>(*nullable_column->get_nested_column_ptr()).get_data();
            const auto& row_ids = this->current_batch_row_positions();
            for (size_t i = 0; i < rows; ++i) {
                if (null_map[i] != 0) {
                    null_map[i] = 0;
                    data[i] = _row_lineage_columns.first_row_id + static_cast<int64_t>(row_ids[i]);
                }
            }
        }
        return Status::OK();
    }

    Status _fill_row_lineage_last_updated_sequence_number(Block* block, size_t rows) {
        int col_pos = block->get_position_by_name(ROW_LINEAGE_LAST_UPDATED_SEQ_NUMBER);
        DORIS_CHECK(col_pos >= 0);

        if (_row_lineage_columns.last_updated_sequence_number >= 0) {
            auto col = block->get_by_position(col_pos).column->assume_mutable();
            auto* nullable_column = assert_cast<ColumnNullable*>(col.get());
            auto& null_map = nullable_column->get_null_map_data();
            auto& data =
                    assert_cast<ColumnInt64&>(*nullable_column->get_nested_column_ptr()).get_data();
            for (size_t i = 0; i < rows; ++i) {
                if (null_map[i] != 0) {
                    null_map[i] = 0;
                    data[i] = _row_lineage_columns.last_updated_sequence_number;
                }
            }
        }
        return Status::OK();
    }

    // Called after reading a block: apply equality delete filter + shrink block
    Status on_after_read_block(Block* block, size_t* read_rows) override {
        if (!_equality_delete_impls.empty()) {
            std::unique_ptr<IColumn::Filter> filter =
                    std::make_unique<IColumn::Filter>(block->rows(), 1);
            for (auto& equality_delete_impl : _equality_delete_impls) {
                RETURN_IF_ERROR(equality_delete_impl->filter_data_block(
                        block, this->col_name_to_block_idx_ref(), _id_to_block_column_name,
                        *filter));
            }
            Block::filter_block_internal(block, *filter, block->columns());
            *read_rows = block->rows();
        }
        return _shrink_block_if_need(block);
    }

    // ---- Shared Iceberg methods ----

    Status _init_row_filters();
    Status _position_delete_base(const std::string data_file_path,
                                 const std::vector<TIcebergDeleteFileDesc>& delete_files);
    Status _equality_delete_base(const std::vector<TIcebergDeleteFileDesc>& delete_files);
    Status read_deletion_vector(const std::string& data_file_path,
                                const TIcebergDeleteFileDesc& delete_file_desc);

    Status _expand_block_if_need(Block* block);
    Status _shrink_block_if_need(Block* block);

    // Type aliases — must be defined before member function declarations that use them.
    using DeleteRows = std::vector<int64_t>;
    using DeleteFile = phmap::parallel_flat_hash_map<
            std::string, std::unique_ptr<DeleteRows>, std::hash<std::string>, std::equal_to<>,
            std::allocator<std::pair<const std::string, std::unique_ptr<DeleteRows>>>, 8,
            std::mutex>;

    PositionDeleteRange _get_range(const ColumnDictI32& file_path_column);
    PositionDeleteRange _get_range(const ColumnString& file_path_column);
    static void _sort_delete_rows(const std::vector<std::vector<int64_t>*>& delete_rows_array,
                                  int64_t num_delete_rows, std::vector<int64_t>& result);
    void _gen_position_delete_file_range(Block& block, DeleteFile* position_delete,
                                         size_t read_rows, bool file_path_column_dictionary_coded);
    void _generate_equality_delete_block(Block* block,
                                         const std::vector<std::string>& equality_delete_col_names,
                                         const std::vector<DataTypePtr>& equality_delete_col_types);

    // Pure virtual: format-specific delete file reading
    virtual Status _read_position_delete_file(const TFileRangeDesc*, DeleteFile*) = 0;
    virtual std::unique_ptr<GenericReader> _create_equality_reader(
            const TFileRangeDesc& delete_desc) = 0;

    static std::string _delet_file_cache_key(const std::string& path) { return "delete_" + path; }

    /// Build the Iceberg V2 row-id struct column.
    static Status _build_iceberg_rowid_column(const DataTypePtr& type, const std::string& file_path,
                                              const std::vector<rowid_t>& row_ids,
                                              int32_t partition_spec_id,
                                              const std::string& partition_data_json,
                                              MutableColumnPtr* column_out) {
        if (type == nullptr || column_out == nullptr) {
            return Status::InvalidArgument("Invalid iceberg rowid column type or output column");
        }
        MutableColumnPtr column = type->create_column();
        ColumnNullable* nullable_col = check_and_get_column<ColumnNullable>(column.get());
        ColumnStruct* struct_col = nullptr;
        if (nullable_col != nullptr) {
            struct_col =
                    check_and_get_column<ColumnStruct>(nullable_col->get_nested_column_ptr().get());
        } else {
            struct_col = check_and_get_column<ColumnStruct>(column.get());
        }
        if (struct_col == nullptr || struct_col->tuple_size() < 4) {
            return Status::InternalError("Invalid iceberg rowid column structure");
        }
        size_t num_rows = row_ids.size();
        auto& file_path_col = struct_col->get_column(0);
        auto& row_pos_col = struct_col->get_column(1);
        auto& spec_id_col = struct_col->get_column(2);
        auto& partition_data_col = struct_col->get_column(3);
        file_path_col.reserve(num_rows);
        row_pos_col.reserve(num_rows);
        spec_id_col.reserve(num_rows);
        partition_data_col.reserve(num_rows);
        for (size_t i = 0; i < num_rows; ++i) {
            file_path_col.insert_data(file_path.data(), file_path.size());
        }
        for (size_t i = 0; i < num_rows; ++i) {
            int64_t row_pos = static_cast<int64_t>(row_ids[i]);
            row_pos_col.insert_data(reinterpret_cast<const char*>(&row_pos), sizeof(row_pos));
        }
        for (size_t i = 0; i < num_rows; ++i) {
            int32_t spec_id = partition_spec_id;
            spec_id_col.insert_data(reinterpret_cast<const char*>(&spec_id), sizeof(spec_id));
        }
        for (size_t i = 0; i < num_rows; ++i) {
            partition_data_col.insert_data(partition_data_json.data(), partition_data_json.size());
        }
        if (nullable_col != nullptr) {
            nullable_col->get_null_map_data().resize_fill(num_rows, 0);
        }
        *column_out = std::move(column);
        return Status::OK();
    }

    struct IcebergProfile {
        RuntimeProfile::Counter* num_delete_files;
        RuntimeProfile::Counter* num_delete_rows;
        RuntimeProfile::Counter* delete_files_read_time;
        RuntimeProfile::Counter* delete_rows_sort_time;
        RuntimeProfile::Counter* parse_delete_file_time;
    };

    bool _need_row_id_column = false;
    std::string _current_file_path;
    int32_t _partition_spec_id = 0;
    std::string _partition_data_json;

    ShardedKVCache* _kv_cache;
    IcebergProfile _iceberg_profile;
    const std::vector<int64_t>* _iceberg_delete_rows = nullptr;
    std::vector<std::string> _expand_col_names;
    std::vector<ColumnWithTypeAndName> _expand_columns;
    std::vector<std::string> _all_required_col_names;
    Fileformat _file_format = Fileformat::NONE;

    const int64_t MIN_SUPPORT_DELETE_FILES_VERSION = 2;
    const std::string ICEBERG_FILE_PATH = "file_path";
    const std::string ICEBERG_ROW_POS = "pos";
    const std::vector<std::string> delete_file_col_names {ICEBERG_FILE_PATH, ICEBERG_ROW_POS};
    const std::unordered_map<std::string, uint32_t> DELETE_COL_NAME_TO_BLOCK_IDX = {
            {ICEBERG_FILE_PATH, 0}, {ICEBERG_ROW_POS, 1}};
    const int ICEBERG_FILE_PATH_INDEX = 0;
    const int ICEBERG_FILE_POS_INDEX = 1;
    const int READ_DELETE_FILE_BATCH_SIZE = 102400;

    // all ids that need read for eq delete (from all eq delete files)
    std::set<int> _equality_delete_col_ids;
    // eq delete column ids -> location of _equality_delete_blocks / _equality_delete_impls
    std::map<std::vector<int>, int> _equality_delete_block_map;
    // EqualityDeleteBase stores raw pointers to these blocks, so do not modify this vector after
    // creating entries in _equality_delete_impls.
    std::vector<Block> _equality_delete_blocks;
    std::vector<std::unique_ptr<EqualityDeleteBase>> _equality_delete_impls;

    // id -> block column name
    std::unordered_map<int, std::string> _id_to_block_column_name;

    // File column names used during init
    std::vector<std::string> _file_col_names;

    std::function<std::shared_ptr<segment_v2::RowIdColumnIteratorV2>()>
            _create_topn_row_id_column_iterator;

    static constexpr const char* ROW_LINEAGE_ROW_ID = "_row_id";
    static constexpr const char* ROW_LINEAGE_LAST_UPDATED_SEQ_NUMBER =
            "_last_updated_sequence_number";
    struct RowLineageColumns {
        int64_t first_row_id = -1;
        int64_t last_updated_sequence_number = -1;
    };
    RowLineageColumns _row_lineage_columns;
};

// ============================================================================
// Template method implementations (must be in header for templates)
// ============================================================================

template <typename BaseReader>
Status IcebergReaderMixin<BaseReader>::_init_row_filters() {
    // COUNT(*) short-circuit
    if (this->_push_down_agg_type == TPushAggOp::type::COUNT &&
        this->get_scan_range().table_format_params.__isset.table_level_row_count &&
        this->get_scan_range().table_format_params.table_level_row_count > 0) {
        return Status::OK();
    }

    const auto& table_desc = this->get_scan_range().table_format_params.iceberg_params;
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
        RETURN_IF_ERROR(_equality_delete_base(equality_delete_files));
        this->set_push_down_agg_type(TPushAggOp::NONE);
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
        this->set_push_down_agg_type(TPushAggOp::NONE);
    } else if (!position_delete_files.empty()) {
        RETURN_IF_ERROR(
                _position_delete_base(table_desc.original_file_path, position_delete_files));
        this->set_push_down_agg_type(TPushAggOp::NONE);
    }

    COUNTER_UPDATE(_iceberg_profile.num_delete_files, table_desc.delete_files.size());
    return Status::OK();
}

template <typename BaseReader>
Status IcebergReaderMixin<BaseReader>::_equality_delete_base(
        const std::vector<TIcebergDeleteFileDesc>& delete_files) {
    std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>
            partition_columns;
    std::unordered_map<std::string, VExprContextSPtr> missing_columns;

    for (const auto& delete_file : delete_files) {
        TFileRangeDesc delete_desc;
        delete_desc.__set_fs_name(this->get_scan_range().fs_name);
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

        std::unique_ptr<GenericReader> delete_reader = _create_equality_reader(delete_desc);
        RETURN_IF_ERROR(delete_reader->init_schema_reader());

        std::vector<std::string> equality_delete_col_names;
        std::vector<DataTypePtr> equality_delete_col_types;

        // Build delete col names/types/ids by matching field_ids from delete file schema.
        // Master iterates delete file's FieldDescriptor and uses field_id to match,
        // NOT idx-based pairing (get_parsed_schema order != field_ids order).
        std::vector<std::string> delete_col_names;
        std::vector<DataTypePtr> delete_col_types;
        std::vector<int> delete_col_ids;
        std::unordered_map<std::string, uint32_t> delete_col_name_to_block_idx;

        if (auto* parquet_reader = typeid_cast<ParquetReader*>(delete_reader.get())) {
            const FieldDescriptor* delete_field_desc = nullptr;
            RETURN_IF_ERROR(parquet_reader->get_file_metadata_schema(&delete_field_desc));
            DCHECK(delete_field_desc != nullptr);

            for (const auto& delete_file_field : delete_field_desc->get_fields_schema()) {
                if (delete_file_field.field_id == -1) [[unlikely]] {
                    return Status::DataQualityError(
                            "missing field id when reading equality delete file");
                }
                if (!read_column_field_ids_set.contains(delete_file_field.field_id)) {
                    continue;
                }
                if (delete_file_field.children.size() > 0) [[unlikely]] {
                    return Status::InternalError(
                            "can not support read complex column in equality delete file");
                }

                delete_col_ids.emplace_back(delete_file_field.field_id);
                delete_col_names.emplace_back(delete_file_field.name);
                delete_col_types.emplace_back(make_nullable(delete_file_field.data_type));

                int field_id = delete_file_field.field_id;
                if (!_id_to_block_column_name.contains(field_id)) {
                    _id_to_block_column_name.emplace(field_id, delete_file_field.name);
                    _expand_col_names.emplace_back(delete_file_field.name);
                    _expand_columns.emplace_back(
                            make_nullable(delete_file_field.data_type)->create_column(),
                            make_nullable(delete_file_field.data_type), delete_file_field.name);
                }
            }
            for (uint32_t idx = 0; idx < delete_col_names.size(); ++idx) {
                delete_col_name_to_block_idx[delete_col_names[idx]] = idx;
            }
            // Delete files have TFileRangeDesc.size=-1, which would cause
            // set_fill_columns to return EndOfFile("No row group to read")
            // when _filter_groups is true. Master passes filter_groups=false.
            ParquetInitContext eq_delete_ctx;
            eq_delete_ctx.filter_groups = false;
            eq_delete_ctx.column_names = delete_col_names;
            eq_delete_ctx.col_name_to_block_idx = &delete_col_name_to_block_idx;
            auto st2 = parquet_reader->init_reader(&eq_delete_ctx);
            if (!st2.ok()) {
                return st2;
            }
        } else if (auto* orc_reader = typeid_cast<OrcReader*>(delete_reader.get())) {
            // For ORC: use get_parsed_schema with field_ids from delete_file
            // ORC field_ids come from the Thrift descriptor, not from ORC metadata
            RETURN_IF_ERROR(delete_reader->get_parsed_schema(&equality_delete_col_names,
                                                             &equality_delete_col_types));
            for (uint32_t idx = 0; idx < equality_delete_col_names.size(); ++idx) {
                if (idx < read_column_field_ids.size()) {
                    int field_id = read_column_field_ids[idx];
                    if (!read_column_field_ids_set.contains(field_id)) continue;
                    delete_col_ids.emplace_back(field_id);
                    delete_col_names.emplace_back(equality_delete_col_names[idx]);
                    delete_col_types.emplace_back(make_nullable(equality_delete_col_types[idx]));
                    if (!_id_to_block_column_name.contains(field_id)) {
                        _id_to_block_column_name.emplace(field_id, equality_delete_col_names[idx]);
                        _expand_col_names.emplace_back(equality_delete_col_names[idx]);
                        _expand_columns.emplace_back(
                                make_nullable(equality_delete_col_types[idx])->create_column(),
                                make_nullable(equality_delete_col_types[idx]),
                                equality_delete_col_names[idx]);
                    }
                }
            }
            for (uint32_t idx = 0; idx < delete_col_names.size(); ++idx) {
                delete_col_name_to_block_idx[delete_col_names[idx]] = idx;
            }
            OrcInitContext eq_delete_ctx;
            eq_delete_ctx.column_names = delete_col_names;
            eq_delete_ctx.col_name_to_block_idx = &delete_col_name_to_block_idx;
            RETURN_IF_ERROR(orc_reader->init_reader(&eq_delete_ctx));
        } else {
            return Status::InternalError("Unsupported format of delete file");
        }

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
            auto st = delete_reader->get_next_block(&tmp_block, &read_rows, &eof);
            if (!st.ok()) {
                return st;
            }
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
        RETURN_IF_ERROR(equality_delete_impl->init(this->get_profile()));
        _equality_delete_impls.emplace_back(std::move(equality_delete_impl));
    }
    return Status::OK();
}

template <typename BaseReader>
void IcebergReaderMixin<BaseReader>::_generate_equality_delete_block(
        Block* block, const std::vector<std::string>& equality_delete_col_names,
        const std::vector<DataTypePtr>& equality_delete_col_types) {
    for (int i = 0; i < equality_delete_col_names.size(); ++i) {
        DataTypePtr data_type = make_nullable(equality_delete_col_types[i]);
        MutableColumnPtr data_column = data_type->create_column();
        block->insert(ColumnWithTypeAndName(std::move(data_column), data_type,
                                            equality_delete_col_names[i]));
    }
}

template <typename BaseReader>
Status IcebergReaderMixin<BaseReader>::_expand_block_if_need(Block* block) {
    std::set<std::string> names;
    auto block_names = block->get_names();
    names.insert(block_names.begin(), block_names.end());
    for (auto& col : _expand_columns) {
        col.column->assume_mutable()->clear();
        if (names.contains(col.name)) {
            return Status::InternalError("Wrong expand column '{}'", col.name);
        }
        names.insert(col.name);
        (*this->col_name_to_block_idx_ref())[col.name] = static_cast<uint32_t>(block->columns());
        block->insert(col);
    }
    return Status::OK();
}

template <typename BaseReader>
Status IcebergReaderMixin<BaseReader>::_shrink_block_if_need(Block* block) {
    std::set<size_t> positions_to_erase;
    for (const std::string& expand_col : _expand_col_names) {
        if (!this->col_name_to_block_idx_ref()->contains(expand_col)) {
            return Status::InternalError("Wrong erase column '{}', block: {}", expand_col,
                                         block->dump_names());
        }
        positions_to_erase.emplace((*this->col_name_to_block_idx_ref())[expand_col]);
    }
    block->erase(positions_to_erase);
    for (const std::string& expand_col : _expand_col_names) {
        this->col_name_to_block_idx_ref()->erase(expand_col);
    }
    return Status::OK();
}

template <typename BaseReader>
Status IcebergReaderMixin<BaseReader>::_position_delete_base(
        const std::string data_file_path, const std::vector<TIcebergDeleteFileDesc>& delete_files) {
    std::vector<DeleteRows*> delete_rows_array;
    int64_t num_delete_rows = 0;
    for (const auto& delete_file : delete_files) {
        SCOPED_TIMER(_iceberg_profile.delete_files_read_time);
        Status create_status = Status::OK();
        auto* delete_file_cache = _kv_cache->template get<DeleteFile>(
                _delet_file_cache_key(delete_file.path), [&]() -> DeleteFile* {
                    auto* position_delete = new DeleteFile;
                    TFileRangeDesc delete_file_range;
                    delete_file_range.__set_fs_name(this->get_scan_range().fs_name);
                    delete_file_range.path = delete_file.path;
                    delete_file_range.start_offset = 0;
                    delete_file_range.size = -1;
                    delete_file_range.file_size = -1;
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
    if (num_delete_rows > 0) {
        SCOPED_TIMER(_iceberg_profile.delete_rows_sort_time);
        _iceberg_delete_rows =
                _kv_cache->template get<DeleteRows>(data_file_path, [&]() -> DeleteRows* {
                    auto* data_file_position_delete = new DeleteRows;
                    _sort_delete_rows(delete_rows_array, num_delete_rows,
                                      *data_file_position_delete);
                    return data_file_position_delete;
                });
        set_delete_rows();
        COUNTER_UPDATE(_iceberg_profile.num_delete_rows, num_delete_rows);
    }
    return Status::OK();
}

template <typename BaseReader>
typename IcebergReaderMixin<BaseReader>::PositionDeleteRange
IcebergReaderMixin<BaseReader>::_get_range(const ColumnDictI32& file_path_column) {
    PositionDeleteRange range;
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

template <typename BaseReader>
typename IcebergReaderMixin<BaseReader>::PositionDeleteRange
IcebergReaderMixin<BaseReader>::_get_range(const ColumnString& file_path_column) {
    PositionDeleteRange range;
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

template <typename BaseReader>
void IcebergReaderMixin<BaseReader>::_sort_delete_rows(
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

template <typename BaseReader>
void IcebergReaderMixin<BaseReader>::_gen_position_delete_file_range(
        Block& block, DeleteFile* position_delete, size_t read_rows,
        bool file_path_column_dictionary_coded) {
    SCOPED_TIMER(_iceberg_profile.parse_delete_file_time);
    auto name_to_pos_map = block.get_name_to_pos_map();
    ColumnPtr path_column = block.get_by_position(name_to_pos_map[ICEBERG_FILE_PATH]).column;
    DCHECK_EQ(path_column->size(), read_rows);
    ColumnPtr pos_column = block.get_by_position(name_to_pos_map[ICEBERG_ROW_POS]).column;
    using ColumnType = typename PrimitiveTypeTraits<TYPE_BIGINT>::ColumnType;
    const int64_t* src_data = assert_cast<const ColumnType&>(*pos_column).get_data().data();
    PositionDeleteRange range;
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

template <typename BaseReader>
Status IcebergReaderMixin<BaseReader>::read_deletion_vector(
        const std::string& data_file_path, const TIcebergDeleteFileDesc& delete_file_desc) {
    Status create_status = Status::OK();
    SCOPED_TIMER(_iceberg_profile.delete_files_read_time);
    _iceberg_delete_rows = _kv_cache->template get<
            DeleteRows>(data_file_path, [&]() -> DeleteRows* {
        auto* delete_rows = new DeleteRows;

        TFileRangeDesc delete_range;
        delete_range.__set_fs_name(this->get_scan_range().fs_name);
        delete_range.path = delete_file_desc.path;
        delete_range.start_offset = delete_file_desc.content_offset;
        delete_range.size = delete_file_desc.content_size_in_bytes;
        delete_range.file_size = -1;

        DeletionVectorReader dv_reader(this->get_state(), this->get_profile(),
                                       this->get_scan_params(), delete_range, this->get_io_ctx());
        create_status = dv_reader.open();
        if (!create_status.ok()) [[unlikely]] {
            return nullptr;
        }

        size_t buffer_size = delete_range.size;
        std::vector<char> buf(buffer_size);
        if (buffer_size < 12) [[unlikely]] {
            create_status = Status::DataQualityError("Deletion vector file size too small: {}",
                                                     buffer_size);
            return nullptr;
        }

        create_status = dv_reader.read_at(delete_range.start_offset, {buf.data(), buffer_size});
        if (!create_status) [[unlikely]] {
            return nullptr;
        }

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

} // namespace doris
