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

#include <gen_cpp/ExternalTableSchema_types.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
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
#include "core/data_type/data_type_struct.h"
#include "core/data_type/primitive_type.h"
#include "format/generic_reader.h"
#include "format/table/equality_delete.h"
#include "format/table/iceberg_default_value.h"
#include "format/table/iceberg_delete_file_reader_helper.h"
#include "format/table/iceberg_scan_semantics.h"
#include "format/table/table_schema_change_helper.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"
#include "storage/olap_common.h"
#include "util/string_util.h"

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
        _iceberg_profile.decoded_cache_hit_count =
                ADD_CHILD_COUNTER(this->get_profile(), "DeletionVectorDecodedCacheHitCount",
                                  TUnit::UNIT, iceberg_profile);
        _iceberg_profile.decoded_cache_miss_count =
                ADD_CHILD_COUNTER(this->get_profile(), "DeletionVectorDecodedCacheMissCount",
                                  TUnit::UNIT, iceberg_profile);
        _iceberg_profile.file_cache_hit_count =
                ADD_CHILD_COUNTER(this->get_profile(), "DeletionVectorFileCacheHitCount",
                                  TUnit::UNIT, iceberg_profile);
        _iceberg_profile.file_cache_miss_count =
                ADD_CHILD_COUNTER(this->get_profile(), "DeletionVectorFileCacheMissCount",
                                  TUnit::UNIT, iceberg_profile);
        _iceberg_profile.file_cache_peer_read_count =
                ADD_CHILD_COUNTER(this->get_profile(), "DeletionVectorFileCachePeerReadCount",
                                  TUnit::UNIT, iceberg_profile);
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
    virtual void set_deletion_vector() = 0;

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

    Status TEST_read_deletion_vector(const std::string& data_file_path,
                                     const TIcebergDeleteFileDesc& delete_file_desc) {
        return read_deletion_vector(data_file_path, delete_file_desc);
    }

    Status TEST_position_delete_base(const std::string& data_file_path,
                                     const std::vector<TIcebergDeleteFileDesc>& delete_files) {
        return _position_delete_base(data_file_path, delete_files);
    }

    void TEST_set_column_name_to_block_index(
            std::unordered_map<std::string, uint32_t>* column_name_to_block_index) {
        this->col_name_to_block_idx_ref() = column_name_to_block_index;
    }

    Status TEST_register_missing_equality_delete_column(int32_t field_id, const std::string& name,
                                                        const DataTypePtr& delete_key_type) {
        return _register_missing_equality_delete_column(field_id, name, delete_key_type);
    }

    Status TEST_materialize_missing_equality_delete_columns(Block* block, size_t rows) {
        return _materialize_missing_equality_delete_columns(block, rows);
    }

    const std::vector<int32_t>& TEST_expand_col_field_ids() const { return _expand_col_field_ids; }

protected:
    // ---- Hook implementations ----

    // Called before reading a block: expand block for equality delete columns + detect row_id
    Status on_before_read_block(Block* block) override {
        RETURN_IF_ERROR(_expand_block_if_need(block));
        return Status::OK();
    }

    // Iceberg initial defaults belong to the table schema, not to the generic FE slot default.
    // V1 keeps master's primitive-default behavior; V2 also materializes missing optional/required
    // fields and complex defaults through the recursive Iceberg schema metadata.
    Status on_fill_missing_columns(Block* block, size_t rows,
                                   const std::vector<std::string>& cols) override {
        if (!supports_iceberg_scan_semantics_v1(&this->get_scan_params())) {
            return BaseReader::on_fill_missing_columns(block, rows, cols);
        }
        const bool use_v2_semantics = supports_iceberg_scan_semantics_v2(&this->get_scan_params());
        std::vector<std::string> base_reader_columns;
        for (const auto& col_name : cols) {
            const auto* field = _find_current_schema_field(col_name);
            if (field == nullptr || (!use_v2_semantics && !field->__isset.initial_default_value)) {
                base_reader_columns.push_back(col_name);
                continue;
            }

            DORIS_CHECK(this->_fill_col_name_to_block_idx != nullptr);
            const auto position = this->_fill_col_name_to_block_idx->find(col_name);
            if (position == this->_fill_col_name_to_block_idx->end()) {
                return Status::InternalError("Missing column: {} not found in block {}", col_name,
                                             block->dump_structure());
            }
            DORIS_CHECK(position->second < block->columns());

            auto default_value = _missing_initial_default_values.find(col_name);
            if (default_value == _missing_initial_default_values.end()) {
                ColumnPtr value;
                RETURN_IF_ERROR(iceberg::create_initial_default_column(
                        *field, block->get_by_position(position->second).type, &value));
                default_value =
                        _missing_initial_default_values.emplace(col_name, std::move(value)).first;
            }
            auto column_guard = block->mutate_column_scoped(position->second);
            auto& mutable_column = column_guard.mutable_column();
            mutable_column->insert_many_from(*default_value->second, 0, rows);
        }
        return BaseReader::on_fill_missing_columns(block, rows, base_reader_columns);
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
            auto column_guard = block->mutate_column_scoped(col_pos);
            auto* nullable_column =
                    assert_cast<ColumnNullable*>(column_guard.mutable_column().get());
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
            auto column_guard = block->mutate_column_scoped(col_pos);
            auto* nullable_column =
                    assert_cast<ColumnNullable*>(column_guard.mutable_column().get());
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
        RETURN_IF_ERROR(_materialize_nested_equality_delete_columns(block));
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
    const schema::external::TStructField* _current_schema_root() const;
    const schema::external::TField* _find_current_schema_field(const std::string& name) const;
    const schema::external::TField* _find_schema_field(int32_t field_id) const;
    static bool _find_schema_field_path_in_field(
            const schema::external::TField* field, int32_t field_id,
            std::vector<const schema::external::TField*>* path);
    static bool _find_schema_field_path_in_root(const schema::external::TStructField* root,
                                                int32_t field_id,
                                                std::vector<const schema::external::TField*>* path);
    std::vector<const schema::external::TField*> _find_schema_field_path(int32_t field_id) const;
    Status _create_missing_equality_delete_value(int32_t field_id,
                                                 const DataTypePtr& delete_key_type,
                                                 size_t physical_path_size,
                                                 ColumnPtr* const value) const;
    Status _register_missing_equality_delete_column(int32_t field_id, const std::string& name,
                                                    const DataTypePtr& delete_key_type);
    Status _materialize_missing_equality_delete_column(Block* block, const std::string& name,
                                                       const ColumnPtr& value, size_t rows);
    Status _materialize_missing_equality_delete_columns(Block* block, size_t rows);

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
    Status _gen_position_delete_file_range(Block& block, DeleteFile* position_delete,
                                           size_t read_rows,
                                           bool file_path_column_dictionary_coded);
    void _generate_equality_delete_block(Block* block,
                                         const std::vector<std::string>& equality_delete_col_names,
                                         const std::vector<DataTypePtr>& equality_delete_col_types);
    struct NestedEqualityDeleteColumn {
        int32_t field_id = -1;
        std::string block_name;
        DataTypePtr leaf_type;
        std::vector<size_t> child_indexes;
        ColumnPtr missing_value;
    };
    struct EqualityDeleteReadSpec {
        NestedEqualityDeleteColumn nested_field;
        std::string leaf_name;
        std::string root_name;
        DataTypePtr root_type;
    };
    static bool _find_parquet_equality_delete_path(const FieldSchema& field, int32_t field_id,
                                                   std::vector<const FieldSchema*>* path,
                                                   std::vector<size_t>* child_indexes);
    static bool _find_orc_equality_delete_path(const orc::Type* field,
                                               const std::string& field_name, int32_t field_id,
                                               std::vector<const orc::Type*>* path,
                                               std::vector<std::string>* names,
                                               std::vector<size_t>* child_indexes);
    Status _build_parquet_equality_delete_read_specs(
            ParquetReader* reader, const TIcebergDeleteFileDesc& delete_file,
            std::vector<EqualityDeleteReadSpec>* read_specs) const;
    Status _build_orc_equality_delete_read_specs(
            OrcReader* reader, const TIcebergDeleteFileDesc& delete_file,
            std::vector<EqualityDeleteReadSpec>* read_specs) const;
    Status _build_equality_delete_read_specs(GenericReader* reader,
                                             const TIcebergDeleteFileDesc& delete_file,
                                             std::vector<EqualityDeleteReadSpec>* read_specs) const;
    void _register_equality_delete_read_specs(
            const std::vector<EqualityDeleteReadSpec>& read_specs,
            std::vector<std::string>* delete_col_names, std::vector<DataTypePtr>* delete_col_types,
            std::vector<int>* delete_col_ids, std::vector<std::string>* read_root_names,
            std::vector<DataTypePtr>* read_root_types,
            std::unordered_map<std::string, uint32_t>* read_root_positions);
    static Status _initialize_equality_delete_reader(
            GenericReader* reader, const std::vector<std::string>& read_root_names,
            std::unordered_map<std::string, uint32_t>* read_root_positions);
    Status _merge_equality_delete_rows(
            GenericReader* reader, const std::vector<EqualityDeleteReadSpec>& read_specs,
            const std::vector<std::string>& read_root_names,
            const std::vector<DataTypePtr>& read_root_types,
            const std::unordered_map<std::string, uint32_t>& read_root_positions,
            Block* eq_file_block) const;
    Status _read_equality_delete_file(const TIcebergDeleteFileDesc& delete_file);
    Status _extract_nested_equality_delete_column(const ColumnPtr& root_column,
                                                  const NestedEqualityDeleteColumn& nested_field,
                                                  ColumnPtr* leaf_column) const;
    Status _materialize_nested_equality_delete_columns(Block* block);

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
        auto* nullable_col = check_and_get_column<ColumnNullable>(column.get());
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
            auto row_pos = static_cast<int64_t>(row_ids[i]);
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
        RuntimeProfile::Counter* decoded_cache_hit_count;
        RuntimeProfile::Counter* decoded_cache_miss_count;
        RuntimeProfile::Counter* file_cache_hit_count;
        RuntimeProfile::Counter* file_cache_miss_count;
        RuntimeProfile::Counter* file_cache_peer_read_count;
    };

    bool _need_row_id_column = false;
    std::string _current_file_path;
    int32_t _partition_spec_id = 0;
    std::string _partition_data_json;

    ShardedKVCache* _kv_cache;
    IcebergProfile _iceberg_profile;
    const std::vector<int64_t>* _iceberg_delete_rows = nullptr;
    const DeletionVector* _iceberg_deletion_vector = nullptr;
    std::vector<std::string> _expand_col_names;
    std::vector<int32_t> _expand_col_field_ids;
    std::vector<ColumnWithTypeAndName> _expand_columns;
    std::unordered_map<std::string, ColumnPtr> _missing_initial_default_values;
    std::unordered_map<std::string, ColumnPtr> _missing_equality_delete_values;
    std::vector<NestedEqualityDeleteColumn> _nested_equality_delete_columns;
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
    // COUNT(*) short-circuit. A table-level row count of 0 (e.g. an all-deleted table read with
    // ignore_iceberg_dangling_delete, where total-records == total-position-deletes) is still a
    // valid pushed-down count, so accept >= 0 -- matching FileScanner and the Paimon readers. FE
    // sends -1 when there is no table-level count; using > 0 here would drop a genuine 0 into the
    // delete-applying path below and never produce the intended CountReader(0).
    if (this->_push_down_agg_type == TPushAggOp::type::COUNT &&
        this->get_scan_range().table_format_params.__isset.table_level_row_count &&
        this->get_scan_range().table_format_params.table_level_row_count >= 0) {
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
bool IcebergReaderMixin<BaseReader>::_find_parquet_equality_delete_path(
        const FieldSchema& field, int32_t field_id, std::vector<const FieldSchema*>* path,
        std::vector<size_t>* child_indexes) {
    DORIS_CHECK(path != nullptr);
    DORIS_CHECK(child_indexes != nullptr);
    path->push_back(&field);
    if (field.field_id == field_id) {
        return true;
    }
    for (size_t index = 0; index < field.children.size(); ++index) {
        child_indexes->push_back(index);
        if (_find_parquet_equality_delete_path(field.children[index], field_id, path,
                                               child_indexes)) {
            return true;
        }
        child_indexes->pop_back();
    }
    path->pop_back();
    return false;
}

template <typename BaseReader>
bool IcebergReaderMixin<BaseReader>::_find_orc_equality_delete_path(
        const orc::Type* field, const std::string& field_name, int32_t field_id,
        std::vector<const orc::Type*>* path, std::vector<std::string>* names,
        std::vector<size_t>* child_indexes) {
    DORIS_CHECK(field != nullptr);
    DORIS_CHECK(path != nullptr);
    DORIS_CHECK(names != nullptr);
    DORIS_CHECK(child_indexes != nullptr);
    path->push_back(field);
    names->push_back(field_name);
    if (field->hasAttributeKey("iceberg.id") &&
        std::stoi(field->getAttributeValue("iceberg.id")) == field_id) {
        return true;
    }
    for (size_t index = 0; index < field->getSubtypeCount(); ++index) {
        child_indexes->push_back(index);
        if (_find_orc_equality_delete_path(field->getSubtype(index), field->getFieldName(index),
                                           field_id, path, names, child_indexes)) {
            return true;
        }
        child_indexes->pop_back();
    }
    path->pop_back();
    names->pop_back();
    return false;
}

template <typename BaseReader>
Status IcebergReaderMixin<BaseReader>::_build_parquet_equality_delete_read_specs(
        ParquetReader* reader, const TIcebergDeleteFileDesc& delete_file,
        std::vector<EqualityDeleteReadSpec>* read_specs) const {
    DORIS_CHECK(reader != nullptr);
    DORIS_CHECK(read_specs != nullptr);
    const FieldDescriptor* delete_field_desc = nullptr;
    RETURN_IF_ERROR(reader->get_file_metadata_schema(&delete_field_desc));
    DORIS_CHECK(delete_field_desc != nullptr);
    for (const auto field_id : delete_file.field_ids) {
        std::vector<const FieldSchema*> path;
        std::vector<size_t> child_indexes;
        for (const auto& root : delete_field_desc->get_fields_schema()) {
            if (_find_parquet_equality_delete_path(root, field_id, &path, &child_indexes)) {
                break;
            }
        }
        if (path.empty()) {
            return Status::DataQualityError(
                    "missing field id {} when reading equality delete file {}", field_id,
                    delete_file.path);
        }
        const auto* root = path.front();
        const auto* leaf = path.back();
        if (!leaf->children.empty()) {
            return Status::NotSupported(
                    "Iceberg equality delete does not support complex column {}", leaf->name);
        }
        read_specs->push_back({
                .nested_field =
                        {
                                .field_id = field_id,
                                .block_name = leaf->name,
                                .leaf_type = make_nullable(leaf->data_type),
                                .child_indexes = std::move(child_indexes),
                                .missing_value = nullptr,
                        },
                .leaf_name = leaf->name,
                .root_name = root->name,
                .root_type = make_nullable(root->data_type),
        });
    }
    return Status::OK();
}

template <typename BaseReader>
Status IcebergReaderMixin<BaseReader>::_build_orc_equality_delete_read_specs(
        OrcReader* reader, const TIcebergDeleteFileDesc& delete_file,
        std::vector<EqualityDeleteReadSpec>* read_specs) const {
    DORIS_CHECK(reader != nullptr);
    DORIS_CHECK(read_specs != nullptr);
    const auto* delete_root = reader->get_file_root_type();
    DORIS_CHECK(delete_root != nullptr);
    for (const auto field_id : delete_file.field_ids) {
        std::vector<const orc::Type*> path;
        std::vector<std::string> names;
        std::vector<size_t> child_indexes;
        for (size_t root_index = 0; root_index < delete_root->getSubtypeCount(); ++root_index) {
            if (_find_orc_equality_delete_path(delete_root->getSubtype(root_index),
                                               delete_root->getFieldName(root_index), field_id,
                                               &path, &names, &child_indexes)) {
                break;
            }
        }
        if (path.empty()) {
            return Status::DataQualityError(
                    "missing field id {} when reading equality delete file {}", field_id,
                    delete_file.path);
        }
        const auto* root = path.front();
        const auto* leaf = path.back();
        if (leaf->getSubtypeCount() > 0) {
            return Status::NotSupported(
                    "Iceberg equality delete does not support complex column {}", names.back());
        }
        read_specs->push_back({
                .nested_field =
                        {
                                .field_id = field_id,
                                .block_name = names.back(),
                                .leaf_type = make_nullable(reader->convert_to_doris_type(leaf)),
                                .child_indexes = std::move(child_indexes),
                                .missing_value = nullptr,
                        },
                .leaf_name = names.back(),
                .root_name = names.front(),
                .root_type = make_nullable(reader->convert_to_doris_type(root)),
        });
    }
    return Status::OK();
}

template <typename BaseReader>
Status IcebergReaderMixin<BaseReader>::_build_equality_delete_read_specs(
        GenericReader* reader, const TIcebergDeleteFileDesc& delete_file,
        std::vector<EqualityDeleteReadSpec>* read_specs) const {
    DORIS_CHECK(reader != nullptr);
    DORIS_CHECK(read_specs != nullptr);
    if (auto* parquet_reader = typeid_cast<ParquetReader*>(reader)) {
        return _build_parquet_equality_delete_read_specs(parquet_reader, delete_file, read_specs);
    }
    if (auto* orc_reader = typeid_cast<OrcReader*>(reader)) {
        return _build_orc_equality_delete_read_specs(orc_reader, delete_file, read_specs);
    }
    return Status::InternalError("Unsupported format of delete file");
}

template <typename BaseReader>
void IcebergReaderMixin<BaseReader>::_register_equality_delete_read_specs(
        const std::vector<EqualityDeleteReadSpec>& read_specs,
        std::vector<std::string>* delete_col_names, std::vector<DataTypePtr>* delete_col_types,
        std::vector<int>* delete_col_ids, std::vector<std::string>* read_root_names,
        std::vector<DataTypePtr>* read_root_types,
        std::unordered_map<std::string, uint32_t>* read_root_positions) {
    DORIS_CHECK(delete_col_names != nullptr);
    DORIS_CHECK(delete_col_types != nullptr);
    DORIS_CHECK(delete_col_ids != nullptr);
    DORIS_CHECK(read_root_names != nullptr);
    DORIS_CHECK(read_root_types != nullptr);
    DORIS_CHECK(read_root_positions != nullptr);
    for (const auto& spec : read_specs) {
        delete_col_ids->push_back(spec.nested_field.field_id);
        delete_col_names->push_back(spec.leaf_name);
        delete_col_types->push_back(spec.nested_field.leaf_type);
        if (!_id_to_block_column_name.contains(spec.nested_field.field_id)) {
            _id_to_block_column_name.emplace(spec.nested_field.field_id, spec.leaf_name);
            _expand_col_names.push_back(spec.leaf_name);
            _expand_col_field_ids.push_back(spec.nested_field.field_id);
            _expand_columns.emplace_back(spec.nested_field.leaf_type->create_column(),
                                         spec.nested_field.leaf_type, spec.leaf_name);
        }
        if (!read_root_positions->contains(spec.root_name)) {
            read_root_positions->emplace(spec.root_name, read_root_names->size());
            read_root_names->push_back(spec.root_name);
            read_root_types->push_back(spec.root_type);
        }
    }
}

template <typename BaseReader>
Status IcebergReaderMixin<BaseReader>::_initialize_equality_delete_reader(
        GenericReader* reader, const std::vector<std::string>& read_root_names,
        std::unordered_map<std::string, uint32_t>* read_root_positions) {
    DORIS_CHECK(reader != nullptr);
    DORIS_CHECK(read_root_positions != nullptr);
    if (auto* parquet_reader = typeid_cast<ParquetReader*>(reader)) {
        // Delete files have TFileRangeDesc.size=-1, which would cause
        // set_fill_columns to return EndOfFile("No row group to read") when filtering is enabled.
        ParquetInitContext context;
        context.filter_groups = false;
        context.column_names = read_root_names;
        context.col_name_to_block_idx = read_root_positions;
        return parquet_reader->init_reader(&context);
    }
    auto* orc_reader = typeid_cast<OrcReader*>(reader);
    DORIS_CHECK(orc_reader != nullptr);
    OrcInitContext context;
    context.column_names = read_root_names;
    context.col_name_to_block_idx = read_root_positions;
    return orc_reader->init_reader(&context);
}

template <typename BaseReader>
Status IcebergReaderMixin<BaseReader>::_merge_equality_delete_rows(
        GenericReader* reader, const std::vector<EqualityDeleteReadSpec>& read_specs,
        const std::vector<std::string>& read_root_names,
        const std::vector<DataTypePtr>& read_root_types,
        const std::unordered_map<std::string, uint32_t>& read_root_positions,
        Block* eq_file_block) const {
    DORIS_CHECK(reader != nullptr);
    DORIS_CHECK(eq_file_block != nullptr);
    bool eof = false;
    while (!eof) {
        Block raw_block;
        for (size_t index = 0; index < read_root_names.size(); ++index) {
            raw_block.insert({read_root_types[index]->create_column(), read_root_types[index],
                              read_root_names[index]});
        }
        size_t read_rows = 0;
        RETURN_IF_ERROR(reader->get_next_block(&raw_block, &read_rows, &eof));
        if (read_rows == 0) {
            continue;
        }
        Block key_block;
        for (const auto& spec : read_specs) {
            ColumnPtr key_column;
            RETURN_IF_ERROR(_extract_nested_equality_delete_column(
                    raw_block.get_by_position(read_root_positions.at(spec.root_name)).column,
                    spec.nested_field, &key_column));
            key_block.insert({std::move(key_column), spec.nested_field.leaf_type, spec.leaf_name});
        }
        ScopedMutableBlock scoped_mutable_block(eq_file_block);
        RETURN_IF_ERROR(scoped_mutable_block.mutable_block().merge(key_block));
    }
    return Status::OK();
}

template <typename BaseReader>
Status IcebergReaderMixin<BaseReader>::_read_equality_delete_file(
        const TIcebergDeleteFileDesc& delete_file) {
    if (!delete_file.__isset.field_ids) [[unlikely]] {
        return Status::InternalError("missing delete field ids when reading equality delete file");
    }
    TFileRangeDesc delete_desc;
    delete_desc.__set_fs_name(this->get_scan_range().fs_name);
    delete_desc.path = delete_file.path;
    delete_desc.start_offset = 0;
    delete_desc.size = -1;
    delete_desc.file_size = -1;

    std::unique_ptr<GenericReader> reader = _create_equality_reader(delete_desc);
    RETURN_IF_ERROR(reader->init_schema_reader());
    std::vector<EqualityDeleteReadSpec> read_specs;
    RETURN_IF_ERROR(_build_equality_delete_read_specs(reader.get(), delete_file, &read_specs));

    std::vector<std::string> delete_col_names;
    std::vector<DataTypePtr> delete_col_types;
    std::vector<int> delete_col_ids;
    std::vector<std::string> read_root_names;
    std::vector<DataTypePtr> read_root_types;
    std::unordered_map<std::string, uint32_t> read_root_positions;
    _register_equality_delete_read_specs(read_specs, &delete_col_names, &delete_col_types,
                                         &delete_col_ids, &read_root_names, &read_root_types,
                                         &read_root_positions);
    RETURN_IF_ERROR(_initialize_equality_delete_reader(reader.get(), read_root_names,
                                                       &read_root_positions));

    if (!_equality_delete_block_map.contains(delete_col_ids)) {
        _equality_delete_block_map.emplace(delete_col_ids, _equality_delete_blocks.size());
        Block block;
        _generate_equality_delete_block(&block, delete_col_names, delete_col_types);
        _equality_delete_blocks.emplace_back(std::move(block));
    }
    Block& eq_file_block = _equality_delete_blocks[_equality_delete_block_map[delete_col_ids]];
    return _merge_equality_delete_rows(reader.get(), read_specs, read_root_names, read_root_types,
                                       read_root_positions, &eq_file_block);
}

template <typename BaseReader>
Status IcebergReaderMixin<BaseReader>::_equality_delete_base(
        const std::vector<TIcebergDeleteFileDesc>& delete_files) {
    for (const auto& delete_file : delete_files) {
        RETURN_IF_ERROR(_read_equality_delete_file(delete_file));
        for (const auto field_id : delete_file.field_ids) {
            _equality_delete_col_ids.insert(field_id);
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
        if (_missing_equality_delete_values.contains(col.name)) {
            // Missing equality keys are logical columns, not physical file columns. Add them only
            // after the base reader has established the batch row count.
            continue;
        }
        if (names.contains(col.name)) {
            return Status::InternalError("Wrong expand column '{}'", col.name);
        }
        names.insert(col.name);
        (*this->col_name_to_block_idx_ref())[col.name] = block->columns();
        block->insert({col.type->create_column(), col.type, col.name});
    }
    return Status::OK();
}

template <typename BaseReader>
const schema::external::TStructField* IcebergReaderMixin<BaseReader>::_current_schema_root() const {
    const auto& scan_params = this->get_scan_params();
    if (!scan_params.__isset.history_schema_info || scan_params.history_schema_info.empty()) {
        return nullptr;
    }
    const schema::external::TSchema* current_schema = &scan_params.history_schema_info.front();
    if (scan_params.__isset.current_schema_id) {
        const auto schema_it = std::ranges::find_if(
                scan_params.history_schema_info, [&](const schema::external::TSchema& schema) {
                    return schema.__isset.schema_id &&
                           schema.schema_id == scan_params.current_schema_id;
                });
        if (schema_it == scan_params.history_schema_info.end()) {
            return nullptr;
        }
        current_schema = &*schema_it;
    }
    return current_schema->__isset.root_field ? &current_schema->root_field : nullptr;
}

template <typename BaseReader>
const schema::external::TField* IcebergReaderMixin<BaseReader>::_find_current_schema_field(
        const std::string& name) const {
    const auto* root = _current_schema_root();
    if (root == nullptr) {
        return nullptr;
    }
    const auto field = std::ranges::find_if(root->fields, [&](const auto& field_ptr) {
        return field_ptr.__isset.field_ptr && field_ptr.field_ptr != nullptr &&
               field_ptr.field_ptr->__isset.name && iequal(field_ptr.field_ptr->name, name);
    });
    return field == root->fields.end() ? nullptr : field->field_ptr.get();
}

template <typename BaseReader>
const schema::external::TField* IcebergReaderMixin<BaseReader>::_find_schema_field(
        int32_t field_id) const {
    auto path = _find_schema_field_path(field_id);
    return path.empty() ? nullptr : path.back();
}

template <typename BaseReader>
bool IcebergReaderMixin<BaseReader>::_find_schema_field_path_in_field(
        const schema::external::TField* field, int32_t field_id,
        std::vector<const schema::external::TField*>* path) {
    DORIS_CHECK(path != nullptr);
    if (field == nullptr) {
        return false;
    }
    path->push_back(field);
    if (field->__isset.id && field->id == field_id) {
        return true;
    }
    if (field->__isset.nestedField) {
        if (field->nestedField.__isset.struct_field &&
            field->nestedField.struct_field.__isset.fields) {
            for (const auto& child_ptr : field->nestedField.struct_field.fields) {
                if (child_ptr.__isset.field_ptr && child_ptr.field_ptr != nullptr &&
                    _find_schema_field_path_in_field(child_ptr.field_ptr.get(), field_id, path)) {
                    return true;
                }
            }
        } else if (field->nestedField.__isset.array_field &&
                   field->nestedField.array_field.__isset.item_field) {
            const auto& child_ptr = field->nestedField.array_field.item_field;
            if (child_ptr.__isset.field_ptr && child_ptr.field_ptr != nullptr &&
                _find_schema_field_path_in_field(child_ptr.field_ptr.get(), field_id, path)) {
                return true;
            }
        } else if (field->nestedField.__isset.map_field) {
            const auto& map = field->nestedField.map_field;
            if (map.__isset.key_field && map.key_field.__isset.field_ptr &&
                map.key_field.field_ptr != nullptr &&
                _find_schema_field_path_in_field(map.key_field.field_ptr.get(), field_id, path)) {
                return true;
            }
            if (map.__isset.value_field && map.value_field.__isset.field_ptr &&
                map.value_field.field_ptr != nullptr &&
                _find_schema_field_path_in_field(map.value_field.field_ptr.get(), field_id, path)) {
                return true;
            }
        }
    }
    path->pop_back();
    return false;
}

template <typename BaseReader>
bool IcebergReaderMixin<BaseReader>::_find_schema_field_path_in_root(
        const schema::external::TStructField* root, int32_t field_id,
        std::vector<const schema::external::TField*>* path) {
    DORIS_CHECK(path != nullptr);
    if (root == nullptr || !root->__isset.fields) {
        return false;
    }
    return std::ranges::any_of(root->fields, [&](const auto& field_ptr) {
        return field_ptr.__isset.field_ptr && field_ptr.field_ptr != nullptr &&
               _find_schema_field_path_in_field(field_ptr.field_ptr.get(), field_id, path);
    });
}

template <typename BaseReader>
std::vector<const schema::external::TField*>
IcebergReaderMixin<BaseReader>::_find_schema_field_path(int32_t field_id) const {
    std::vector<const schema::external::TField*> path;
    if (_find_schema_field_path_in_root(_current_schema_root(), field_id, &path)) {
        return path;
    }

    // Equality deletes remain applicable after their key is dropped from the current schema.
    // FE can retain that field's metadata in history_schema_info; field IDs are stable, so recover
    // the original initial-default/required semantics from any historical schema that contains it.
    const auto& scan_params = this->get_scan_params();
    if (!scan_params.__isset.history_schema_info) {
        return {};
    }
    for (const auto& schema : scan_params.history_schema_info) {
        if (!schema.__isset.root_field) {
            continue;
        }
        path.clear();
        if (_find_schema_field_path_in_root(&schema.root_field, field_id, &path)) {
            return path;
        }
    }
    return {};
}

template <typename BaseReader>
Status IcebergReaderMixin<BaseReader>::_extract_nested_equality_delete_column(
        const ColumnPtr& root_column, const NestedEqualityDeleteColumn& nested_field,
        ColumnPtr* leaf_column) const {
    DORIS_CHECK(static_cast<bool>(root_column));
    DORIS_CHECK(nested_field.leaf_type != nullptr);
    DORIS_CHECK(leaf_column != nullptr);
    const IColumn* current = root_column.get();
    std::vector<const NullMap*> ancestor_null_maps;
    for (size_t child_index : nested_field.child_indexes) {
        if (const auto* nullable = check_and_get_column<ColumnNullable>(*current);
            nullable != nullptr) {
            ancestor_null_maps.push_back(&nullable->get_null_map_data());
            current = &nullable->get_nested_column();
        }
        const auto* struct_column = check_and_get_column<ColumnStruct>(*current);
        if (struct_column == nullptr || child_index >= struct_column->tuple_size()) {
            return Status::InternalError(
                    "Iceberg equality delete path for field id {} is absent from column {}",
                    nested_field.field_id, root_column->get_name());
        }
        current = &struct_column->get_column(child_index);
    }
    if (const auto* nullable = check_and_get_column<ColumnNullable>(*current);
        nullable != nullptr) {
        ancestor_null_maps.push_back(&nullable->get_null_map_data());
        current = &nullable->get_nested_column();
    }
    ColumnPtr repeated_missing_value;
    if (static_cast<bool>(nested_field.missing_value)) {
        repeated_missing_value = iceberg::repeat_initial_default_column(nested_field.missing_value,
                                                                        root_column->size());
        current = repeated_missing_value.get();
        if (const auto* nullable = check_and_get_column<ColumnNullable>(*current);
            nullable != nullptr) {
            ancestor_null_maps.push_back(&nullable->get_null_map_data());
            current = &nullable->get_nested_column();
        }
    }

    auto result = ColumnNullable::create(remove_nullable(nested_field.leaf_type)->create_column(),
                                         ColumnUInt8::create());
    auto& result_data = result->get_nested_column();
    auto& result_null_map = result->get_null_map_data();
    result_data.reserve(root_column->size());
    result_null_map.reserve(root_column->size());
    for (size_t row = 0; row < root_column->size(); ++row) {
        bool is_null = false;
        for (const auto* null_map : ancestor_null_maps) {
            if ((*null_map)[row] != 0) {
                is_null = true;
                break;
            }
        }
        if (is_null) {
            result_data.insert_default();
            result_null_map.push_back(1);
        } else {
            result_data.insert_from(*current, row);
            result_null_map.push_back(0);
        }
    }
    *leaf_column = std::move(result);
    return Status::OK();
}

template <typename BaseReader>
Status IcebergReaderMixin<BaseReader>::_materialize_nested_equality_delete_columns(Block* block) {
    DORIS_CHECK(block != nullptr);
    for (const auto& nested_field : _nested_equality_delete_columns) {
        const auto position = this->col_name_to_block_idx_ref()->find(nested_field.block_name);
        DORIS_CHECK(position != this->col_name_to_block_idx_ref()->end());
        DORIS_CHECK(position->second < block->columns());
        auto& column = block->get_by_position(position->second);
        ColumnPtr leaf;
        RETURN_IF_ERROR(_extract_nested_equality_delete_column(column.column, nested_field, &leaf));
        column.column = std::move(leaf);
        column.type = make_nullable(nested_field.leaf_type);
    }
    return Status::OK();
}

template <typename BaseReader>
Status IcebergReaderMixin<BaseReader>::_create_missing_equality_delete_value(
        int32_t field_id, const DataTypePtr& delete_key_type, size_t physical_path_size,
        ColumnPtr* const value) const {
    DORIS_CHECK(delete_key_type != nullptr);
    DORIS_CHECK(value != nullptr);
    const auto table_path = _find_schema_field_path(field_id);
    if (table_path.empty()) {
        // Without field-id-bound current or historical metadata BE cannot distinguish a true NULL
        // initial default from a non-NULL default, so continuing would risk silently keeping or
        // deleting wrong rows.
        return Status::InternalError(
                "Missing Iceberg schema metadata for equality-delete field id {}", field_id);
    }
    const size_t missing_index =
            physical_path_size < table_path.size() ? physical_path_size : table_path.size() - 1;
    const auto* missing_field = table_path[missing_index];
    DORIS_CHECK(missing_field != nullptr);

    if (!supports_iceberg_scan_semantics_v2(&this->get_scan_params()) &&
        !missing_field->__isset.initial_default_value) {
        *value = delete_key_type->create_column_const(1, Field());
        return Status::OK();
    }

    DataTypePtr missing_type = delete_key_type;
    for (size_t index = table_path.size(); index > missing_index + 1; --index) {
        const auto* parent = table_path[index - 2];
        const auto* child = table_path[index - 1];
        DORIS_CHECK(parent != nullptr);
        DORIS_CHECK(child != nullptr);
        DORIS_CHECK(child->__isset.name);
        if (!parent->__isset.nestedField || !parent->nestedField.__isset.struct_field) {
            return Status::NotSupported(
                    "Iceberg equality delete field id {} has a non-struct missing ancestor",
                    field_id);
        }
        missing_type = std::make_shared<DataTypeStruct>(DataTypes {std::move(missing_type)},
                                                        Strings {child->name});
        if (parent->__isset.is_optional && parent->is_optional) {
            missing_type = make_nullable(missing_type);
        }
    }

    ColumnPtr missing_root_value;
    RETURN_IF_ERROR(iceberg::create_initial_default_column(*missing_field, missing_type,
                                                           &missing_root_value));
    if (missing_index + 1 == table_path.size()) {
        *value = std::move(missing_root_value);
        return Status::OK();
    }

    NestedEqualityDeleteColumn missing_path {
            .field_id = field_id,
            .block_name = "",
            .leaf_type = delete_key_type,
            .child_indexes = std::vector<size_t>(table_path.size() - missing_index - 1, 0),
            .missing_value = nullptr,
    };
    return _extract_nested_equality_delete_column(missing_root_value, missing_path, value);
}

template <typename BaseReader>
Status IcebergReaderMixin<BaseReader>::_register_missing_equality_delete_column(
        int32_t field_id, const std::string& name, const DataTypePtr& delete_key_type) {
    DORIS_CHECK(delete_key_type != nullptr);
    ColumnPtr default_column;
    RETURN_IF_ERROR(
            _create_missing_equality_delete_value(field_id, delete_key_type, 0, &default_column));
    const bool inserted =
            _missing_equality_delete_values.emplace(name, std::move(default_column)).second;
    DORIS_CHECK(inserted);
    this->register_synthesized_column_handler(
            name, [this, name](Block* block, size_t rows) -> Status {
                DORIS_CHECK(_missing_equality_delete_values.contains(name));
                return _materialize_missing_equality_delete_column(
                        block, name, _missing_equality_delete_values.at(name), rows);
            });
    return Status::OK();
}

template <typename BaseReader>
Status IcebergReaderMixin<BaseReader>::_materialize_missing_equality_delete_column(
        Block* block, const std::string& name, const ColumnPtr& value, size_t rows) {
    if (!this->col_name_to_block_idx_ref()->contains(name)) {
        // ORC must not register a key that is absent from the file as a physical child. In that
        // case the reader block has no slot for the synthesized key, so append one here before
        // equality-delete filtering. MultiEqualityDelete requires a full, batch-sized column.
        const auto expand_col = std::ranges::find_if(
                _expand_columns,
                [&](const ColumnWithTypeAndName& col) { return col.name == name; });
        DORIS_CHECK(expand_col != _expand_columns.end());
        (*this->col_name_to_block_idx_ref())[name] = block->columns();
        block->insert(
                {iceberg::repeat_initial_default_column(value, rows), expand_col->type, name});
        return Status::OK();
    }
    const auto position = this->col_name_to_block_idx_ref()->at(name);
    DORIS_CHECK(position < block->columns());
    DORIS_CHECK(block->get_by_position(position).column->empty());
    // MultiEqualityDelete hashes each key column directly. Materialize the repeated default so
    // every key has the batch row count; a ColumnConst keeps only one nested value and therefore
    // cannot participate in the row-wise multi-column hash contract.
    block->get_by_position(position).column = iceberg::repeat_initial_default_column(value, rows);
    return Status::OK();
}

template <typename BaseReader>
Status IcebergReaderMixin<BaseReader>::_materialize_missing_equality_delete_columns(Block* block,
                                                                                    size_t rows) {
    for (const auto& [name, value] : _missing_equality_delete_values) {
        RETURN_IF_ERROR(_materialize_missing_equality_delete_column(block, name, value, rows));
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
                    auto position_delete = std::make_unique<DeleteFile>();
                    TFileRangeDesc delete_file_range;
                    delete_file_range.__set_fs_name(this->get_scan_range().fs_name);
                    delete_file_range.path = delete_file.path;
                    delete_file_range.start_offset = 0;
                    delete_file_range.size = -1;
                    delete_file_range.file_size = -1;
                    create_status =
                            _read_position_delete_file(&delete_file_range, position_delete.get());
                    if (!create_status) {
                        return nullptr;
                    }
                    return position_delete.release();
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
                    auto data_file_position_delete = std::make_unique<DeleteRows>();
                    _sort_delete_rows(delete_rows_array, num_delete_rows,
                                      *data_file_position_delete);
                    return data_file_position_delete.release();
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
Status IcebergReaderMixin<BaseReader>::_gen_position_delete_file_range(
        Block& block, DeleteFile* position_delete, size_t read_rows,
        bool file_path_column_dictionary_coded) {
    SCOPED_TIMER(_iceberg_profile.parse_delete_file_time);
    auto name_to_pos_map = block.get_name_to_pos_map();
    ColumnPtr path_column = block.get_by_position(name_to_pos_map[ICEBERG_FILE_PATH]).column;
    DCHECK_EQ(path_column->size(), read_rows);
    ColumnPtr pos_column = block.get_by_position(name_to_pos_map[ICEBERG_ROW_POS]).column;
    if (const auto* nullable_col = check_and_get_column<ColumnNullable>(*path_column);
        nullable_col != nullptr) {
        if (nullable_col->has_null(0, read_rows)) {
            return Status::Corruption(
                    "Iceberg position delete column file_path contains null values");
        }
        path_column = remove_nullable(path_column);
    }
    if (const auto* nullable_col = check_and_get_column<ColumnNullable>(*pos_column);
        nullable_col != nullptr) {
        if (nullable_col->has_null(0, read_rows)) {
            return Status::Corruption("Iceberg position delete column pos contains null values");
        }
        pos_column = remove_nullable(pos_column);
    }
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
    return Status::OK();
}

template <typename BaseReader>
Status IcebergReaderMixin<BaseReader>::read_deletion_vector(
        const std::string& data_file_path, const TIcebergDeleteFileDesc& delete_file_desc) {
    size_t bytes_read = 0;
    RETURN_IF_ERROR(validate_iceberg_deletion_vector_descriptor(delete_file_desc, bytes_read));

    Status create_status = Status::OK();
    SCOPED_TIMER(_iceberg_profile.delete_files_read_time);
    bool decoded_cache_hit = false;
    _iceberg_deletion_vector = _kv_cache->template get<DeletionVector>(
            build_iceberg_deletion_vector_cache_key(data_file_path, delete_file_desc),
            [&]() -> DeletionVector* {
                auto deletion_vector = std::make_unique<DeletionVector>();

                io::FileCacheStatistics file_cache_stats;
                IcebergDeleteFileReaderOptions options;
                options.state = this->get_state();
                options.profile = this->get_profile();
                options.scan_params = &this->get_scan_params();
                options.io_ctx = this->get_io_ctx();
                options.fs_name = &this->get_scan_range().fs_name;
                options.deletion_vector_file_cache_stats = &file_cache_stats;
                create_status = read_iceberg_deletion_vector(delete_file_desc, options,
                                                             deletion_vector.get());
                COUNTER_UPDATE(_iceberg_profile.file_cache_hit_count,
                               file_cache_stats.num_local_io_total);
                COUNTER_UPDATE(_iceberg_profile.file_cache_miss_count,
                               file_cache_stats.num_remote_io_total);
                COUNTER_UPDATE(_iceberg_profile.file_cache_peer_read_count,
                               file_cache_stats.num_peer_io_total);
                if (!create_status.ok()) [[unlikely]] {
                    return nullptr;
                }

                SCOPED_TIMER(_iceberg_profile.parse_delete_file_time);
                COUNTER_UPDATE(_iceberg_profile.num_delete_rows, deletion_vector->cardinality());
                return deletion_vector.release();
            },
            &decoded_cache_hit);

    RETURN_IF_ERROR(create_status);
    COUNTER_UPDATE(decoded_cache_hit ? _iceberg_profile.decoded_cache_hit_count
                                     : _iceberg_profile.decoded_cache_miss_count,
                   1);
    if (!_iceberg_deletion_vector->isEmpty()) [[likely]] {
        set_deletion_vector();
    }
    return Status::OK();
}

} // namespace doris
