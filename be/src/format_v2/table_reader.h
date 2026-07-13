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

#include <bvar/status.h>

#include <algorithm>
#include <exception>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/cast_set.h"
#include "common/exception.h"
#include "common/logging.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_array.h"
#include "core/column/column_const.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "core/field.h"
#include "exec/common/stringop_substring.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "exprs/vexpr_fwd.h"
#include "exprs/vslot_ref.h"
#include "format/table/deletion_vector.h"
#include "format_v2/column_data.h"
#include "format_v2/column_mapper.h"
#include "format_v2/expr/cast.h"
#include "format_v2/expr/delete_predicate.h"
#include "format_v2/file_reader.h"
#include "format_v2/parquet/reader/column_reader.h"
#include "format_v2/schema_projection.h"
#include "gen_cpp/PlanNodes_types.h"
#include "io/io_common.h"
#include "runtime/descriptors.h"
#include "storage/segment/condition_cache.h"

namespace doris {
class Block;
struct DeleteFileDesc;
class RuntimeState;
} // namespace doris

namespace doris::format {

using DeleteRows = std::vector<int64_t>;

// Row-level predicates on table/global schema. They are rewritten to file-local expressions when
// possible, and remain the source of row-level filtering after localization.
struct TableFilter {
    VExprContextSPtr conjunct;
    std::vector<GlobalIndex> global_indices;
};

struct ScanTask {
    virtual ~ScanTask() = default;

    std::unique_ptr<io::FileDescription> data_file;
};

struct ProjectedColumnBuildContext {
    const TFileScanRangeParams* scan_params = nullptr;
    const TFileRangeDesc* range = nullptr;
    RuntimeState* runtime_state = nullptr;
    std::optional<ColumnDefinition> schema_column = std::nullopt;
    size_t next_file_column_idx = 0;
};

struct ReadProfile {
    RuntimeProfile::Counter* num_delete_files = nullptr;
    RuntimeProfile::Counter* num_delete_rows = nullptr;
    RuntimeProfile::Counter* parse_delete_file_time = nullptr;
    RuntimeProfile::Counter* decoded_dv_cache_hit_count = nullptr;
    RuntimeProfile::Counter* decoded_dv_cache_miss_count = nullptr;
    RuntimeProfile::Counter* dv_file_cache_hit_count = nullptr;
    RuntimeProfile::Counter* dv_file_cache_miss_count = nullptr;
    RuntimeProfile::Counter* dv_file_cache_peer_read_count = nullptr;
    RuntimeProfile::Counter* exec_timer = nullptr;
    RuntimeProfile::Counter* prepare_split_timer = nullptr;
    RuntimeProfile::Counter* finalize_timer = nullptr;
    RuntimeProfile::Counter* create_reader_timer = nullptr;
    RuntimeProfile::Counter* pushdown_agg_timer = nullptr;
    RuntimeProfile::Counter* open_reader_timer = nullptr;
    RuntimeProfile::Counter* runtime_filter_partition_prune_timer = nullptr;
    RuntimeProfile::Counter* runtime_filter_partition_pruned_range_counter = nullptr;
};

struct TableReadOptions {
    // Columns need to be read from file and output by table reader. They are all in table/global
    // schema semantics.
    const std::vector<ColumnDefinition> projected_columns;
    // All complex conjuncts from scan operator
    const VExprContextSPtrs conjuncts;
    // File format of the underlying data files, needed for reader initialization and reader-level
    // filter pushdown.
    const FileFormat format;
    TFileScanRangeParams* scan_params;
    std::shared_ptr<io::IOContext> io_ctx;
    RuntimeState* runtime_state;
    RuntimeProfile* scanner_profile;
    // File formats without complete self-describing metadata, such as CSV, Text, and JSON, need
    // the FE-planned physical file slots to build their file-local schema and deserialize values.
    const std::vector<SlotDescriptor*>* file_slot_descs = nullptr;
    // Push-down aggregate type.
    const TPushAggOp::type push_down_agg_type = TPushAggOp::type::NONE;
    // Digest of stable pushed-down predicates. A zero digest disables condition cache.
    uint64_t condition_cache_digest = 0;
};

struct SplitReadOptions {
    // Split-level information for reader initialization, which may include file path, partition values, delete file info, etc. The content is table format specific and opaque to table reader base class; it's the responsibility of the concrete table reader implementation to parse necessary information for reader initialization and filter pushdown.
    std::map<std::string, Field> partition_values;
    // Latest scanner conjuncts rewritten to table/global column indices. Runtime filters can
    // arrive after TableReader::init(), so split preparation must receive a fresh snapshot.
    VExprContextSPtrs partition_prune_conjuncts;
    ShardedKVCache* cache;
    TFileRangeDesc current_range;
    FileFormat current_split_format = FileFormat::PARQUET;
    std::optional<GlobalRowIdContext> global_rowid_context;
};

// Base class for table-level readers.
// This layer owns common table-level orchestration, such as split iteration, dynamic partition
// pruning, delete handling and conversion from file-local blocks to table-schema blocks. Concrete
// table-format readers only need to provide format-specific hooks for opening readers and parsing
// split metadata.
class TableReader {
public:
    virtual ~TableReader() = default;

    // Initialize common runtime options for the table reader. Subclasses may call this from their
    // own init(options); table-format schema and split metadata are provided later per split.
    virtual Status init(TableReadOptions&& options);

    // FileScannerV2 adjusts this before each get_block() using an adaptive bytes-per-row estimate.
    // Store it here as well as forwarding to the current reader so newly opened split readers start
    // with the latest predicted batch size.
    void set_batch_size(size_t batch_size) {
        _batch_size = std::max<size_t>(1, batch_size);
        if (_data_reader.reader != nullptr) {
            _data_reader.reader->set_batch_size(_batch_size);
        }
    }

    // Prepare for reading a new split/task.
    // 1. Pass a new split/task to reader, which will be used in subsequent open_reader() to initialize the underlying file reader.
    // 2. Parse delete predicates from split/task information, which will be used for later dynamic filtering and delete handling.
    virtual Status prepare_split(const SplitReadOptions& options);

    virtual bool current_split_pruned() const { return _current_split_pruned; }

    // Discard the active split after the caller decides an error is ignorable, for example a
    // stale external-table file listing that returns NOT_FOUND. The next prepare_split() must start
    // with no concrete reader or split-local state left from the failed split.
    virtual Status abort_split() {
        if (_data_reader.reader != nullptr) {
            RETURN_IF_ERROR(close_current_reader());
        } else {
            _current_task.reset();
            _current_file_description.reset();
        }
        _delete_rows = nullptr;
        _remaining_table_level_count = -1;
        _current_split_pruned = false;
        return Status::OK();
    }

    // Public entry point for reading a table-schema block. The base class opens the current reader,
    // advances across EOF, and closes exhausted readers. Subclasses provide protected hooks for
    // table-format-specific behavior.
    virtual Status get_block(Block* block, bool* eos) {
        SCOPED_TIMER(_profile.exec_timer);
        DORIS_CHECK(block->columns() == _projected_columns.size());
        block->clear_column_data(_projected_columns.size());

        while (true) {
            if (*eos) {
                return Status::OK();
            }
            if (_io_ctx != nullptr && _io_ctx->should_stop) {
                *eos = true;
                return Status::OK();
            }
            if (!_data_reader.reader) {
                if (_is_table_level_count_active()) {
                    RETURN_IF_ERROR(_read_table_level_count(block, eos));
                    return Status::OK();
                }
                RETURN_IF_ERROR(create_next_reader(eos));
                if (!_data_reader.reader) {
                    DCHECK(*eos);
                    return Status::OK();
                }
            }

            // Materialize a reduced row set for upper aggregate operators when aggregate
            // pushdown can be applied. This is not the final aggregate result: COUNT emits
            // `count` default rows for the upper COUNT(*), and MIN/MAX emits two rows containing
            // file-level min/max values for the upper MIN/MAX.
            if (!_aggregate_pushdown_tried) {
                SCOPED_TIMER(_profile.pushdown_agg_timer);
                bool pushed_down = false;
                const auto status = _try_materialize_aggregate_pushdown_rows(block, &pushed_down);
                if (!status.ok()) {
                    if (_io_ctx != nullptr && _io_ctx->should_stop &&
                        status.is<ErrorCode::END_OF_FILE>()) {
                        *eos = true;
                        return Status::OK();
                    }
                    return status;
                }
                if (pushed_down) {
                    return Status::OK();
                }
            }

            bool current_eof = false;
            _data_reader.block_template.clear_column_data(
                    cast_set<int64_t>(_data_reader.file_block_layout.size()));
            size_t current_rows = 0;
            RETURN_IF_ERROR(_data_reader.reader->get_block(&_data_reader.block_template,
                                                           &current_rows, &current_eof));
            const bool stopped_during_read = _io_ctx != nullptr && _io_ctx->should_stop;
            if (current_rows == 0) {
                if (current_eof) {
                    _current_reader_reached_eof = !stopped_during_read;
                    RETURN_IF_ERROR(close_current_reader());
                }
                continue;
            }
            DCHECK_EQ(_data_reader.block_template.columns(), _data_reader.file_block_layout.size())
                    << _data_reader.block_template.dump_structure();
#ifndef NDEBUG
            RETURN_IF_ERROR(_check_file_block_columns("after file reader get_block", current_rows));
#endif
            DORIS_CHECK(block->columns() == _data_reader.column_mapper->mappings().size());
            RETURN_IF_ERROR(finalize_chunk(block, current_rows));
#ifndef NDEBUG
            RETURN_IF_ERROR(
                    _check_table_block_columns("after finalize_chunk", block, current_rows));
#endif
            if (current_eof) {
                _current_reader_reached_eof = !stopped_during_read;
                RETURN_IF_ERROR(close_current_reader());
            }
            return Status::OK();
        }
    }

    // Close the table reader and the currently active file reader. Subclasses that hold additional
    // table-format resources should override this and call TableReader::close() first.
    virtual Status close() {
        if (_data_reader.reader) {
            RETURN_IF_ERROR(close_current_reader());
        }
        _current_task.reset();
        _current_file_description.reset();
        _remaining_table_level_count = -1;
        return Status::OK();
    }

    int64_t condition_cache_hit_count() const { return _condition_cache_hit_count; }

    virtual std::string debug_string() const;

    virtual Status annotate_projected_column(const TFileScanSlotInfo& slot_info,
                                             ProjectedColumnBuildContext* context,
                                             ColumnDefinition* column) const;

    virtual Status validate_projected_columns(const ProjectedColumnBuildContext& context) const {
        (void)context;
        return Status::OK();
    }

protected:
    // Parse deletion vector information from table format specific file description.
    virtual Status _parse_deletion_vector_file(const TTableFormatFileDesc& t_desc,
                                               DeleteFileDesc* desc, bool* has_delete_file) {
        *has_delete_file = false;
        return Status::OK();
    }

    // Advance to the next reader. This closes the current reader first and then opens the next
    // concrete reader. Subclasses should not duplicate this loop.
    Status create_next_reader(bool* eos);
    virtual Status create_file_reader(std::unique_ptr<FileReader>* reader);
    virtual TableColumnMappingMode mapping_mode() const { return TableColumnMappingMode::BY_NAME; }
    virtual Status annotate_file_schema(std::vector<ColumnDefinition>* file_schema) {
        DORIS_CHECK(file_schema != nullptr);
        return Status::OK();
    }

    // Open the concrete reader for the current split/task and build the file-local scan request.
    virtual Status open_reader() {
        SCOPED_TIMER(_profile.open_reader_timer);
        // 1. Get file schema and create column mapping.
        std::vector<ColumnDefinition> file_schema;
        RETURN_IF_ERROR(_data_reader.reader->get_schema(&file_schema));
        // For Paimon/Hudi, FE can provide field ids through `history_schema_info`. Annotate the
        // file schema before column mapping when the table format maps columns by field id.
        RETURN_IF_ERROR(annotate_file_schema(&file_schema));
        _data_reader.file_schema = file_schema;
        _mapper_options.mode = mapping_mode();

        _data_reader.column_mapper = _data_reader.reader->create_column_mapper(_mapper_options);
        DORIS_CHECK(_data_reader.column_mapper != nullptr);
        RETURN_IF_ERROR(_data_reader.column_mapper->create_mapping(_projected_columns,
                                                                   _partition_values, file_schema));
        DORIS_CHECK(_data_reader.column_mapper->mappings().size() == _projected_columns.size());

        // 2. Build table filters based on conjuncts and column predicates.
        RETURN_IF_ERROR(_build_table_filters_from_conjuncts());

        // 3. Create file scan request based on column mapping and table filters, then open file
        // reader with the request. File scan request carries row-level expression filters and
        // file-level pruning hints. Only expression filters decide returned rows.
        auto file_request = std::make_shared<FileScanRequest>();
        RETURN_IF_ERROR(_data_reader.column_mapper->create_scan_request(
                _table_filters, _projected_columns, file_request.get(), _runtime_state));
        bool constant_filter_pruned_split = false;
        RETURN_IF_ERROR(_evaluate_constant_filters(&constant_filter_pruned_split));
        if (constant_filter_pruned_split) {
            RETURN_IF_ERROR(close_current_reader());
            return Status::OK();
        }
        RETURN_IF_ERROR(customize_file_scan_request(file_request.get()));
        RETURN_IF_ERROR(_open_local_filter_exprs(*file_request));
        _data_reader.file_block_layout.clear();
        _data_reader.block_template.clear();
        _data_reader.file_block_layout.resize(file_request->local_positions.size());

        // 4. Build file block layout from file schema and column mapping. The layout describes
        // the block returned by file reader before table-column materialization.
        for (const auto& [file_column_id, block_position] : file_request->local_positions) {
            DORIS_CHECK(block_position.value() < _data_reader.file_block_layout.size());
            const auto* field = _find_column_definition(_data_reader.file_schema, file_column_id);
            DORIS_CHECK(field != nullptr);

            ColumnDefinition projected_field;
            {
                auto it = std::find_if(
                        file_request->non_predicate_columns.begin(),
                        file_request->non_predicate_columns.end(),
                        [&](const LocalColumnIndex& p) { return p.column_id() == file_column_id; });
                if (it != file_request->non_predicate_columns.end()) {
                    RETURN_IF_ERROR(project_column_definition(*field, *it, &projected_field));
                }
            }
            {
                auto it = std::find_if(
                        file_request->predicate_columns.begin(),
                        file_request->predicate_columns.end(),
                        [&](const LocalColumnIndex& p) { return p.column_id() == file_column_id; });
                if (it != file_request->predicate_columns.end()) {
                    RETURN_IF_ERROR(project_column_definition(*field, *it, &projected_field));
                }
            }
            _data_reader.file_block_layout[block_position.value()] = {
                    .file_column_id = file_column_id,
                    .name = projected_field.name,
                    .type = projected_field.type,
            };
            DORIS_CHECK(_data_reader.file_block_layout[block_position.value()].type != nullptr);
        }

        // 5. Prepare block template from file block layout. The block template stores the block
        // returned by file reader before table-column materialization.
        _data_reader.block_template.reserve(_data_reader.file_block_layout.size());
        for (const auto& column : _data_reader.file_block_layout) {
            _data_reader.block_template.insert(
                    {column.type->create_column(), column.type, column.name});
        }
        if (VLOG_DEBUG_IS_ON) {
            VLOG_DEBUG << "TableReader debug: " << debug_string();
        }
        RETURN_IF_ERROR(_open_mapping_exprs());
        RETURN_IF_ERROR(_data_reader.reader->open(file_request));
        RETURN_IF_ERROR(_init_reader_condition_cache(*file_request));
        return Status::OK();
    }

    Status _build_table_filters_from_conjuncts();
    Status _evaluate_partition_prune_conjuncts(const VExprContextSPtrs& conjuncts,
                                               bool* can_filter_all);
    Status _build_partition_prune_block(Block* block) const;
    Status _open_local_filter_exprs(const FileScanRequest& file_request);
    Status _init_reader_condition_cache(const FileScanRequest& file_request);
    void _finalize_reader_condition_cache();
    bool _should_enable_condition_cache(const FileScanRequest& file_request) const;

    Status _evaluate_constant_filters(bool* can_filter_all) {
        DORIS_CHECK(can_filter_all != nullptr);
        *can_filter_all = false;
        for (const auto& table_filter : _table_filters) {
            if (table_filter.conjunct == nullptr ||
                // RuntimeFilterExpr does not implement execute_column_impl(); it is evaluated by
                // the row-level filter path through execute_filter(). Constant split pruning uses
                // VExprContext::execute() on a one-row synthetic block, so runtime filters must not
                // be pre-executed here even when their referenced slot maps to a constant value.
                table_filter.conjunct->root()->is_rf_wrapper() ||
                !_table_filter_has_only_constant_entries(table_filter)) {
                continue;
            }
            Block eval_block;
            RETURN_IF_ERROR(_build_constant_filter_block(table_filter, &eval_block));
            RowDescriptor row_desc;
            RETURN_IF_ERROR(table_filter.conjunct->prepare(_runtime_state, row_desc));
            RETURN_IF_ERROR(table_filter.conjunct->open(_runtime_state));
            int result_column_id = -1;
            RETURN_IF_ERROR(table_filter.conjunct->execute(&eval_block, &result_column_id));
            DORIS_CHECK(result_column_id >= 0);
            if (_filter_result_filters_all(eval_block.get_by_position(result_column_id).column)) {
                *can_filter_all = true;
                return Status::OK();
            }
        }
        return Status::OK();
    }

    bool _table_filter_has_only_constant_entries(const TableFilter& table_filter) const {
        const auto& filter_entries = _data_reader.column_mapper->filter_entries();
        for (const auto global_index : table_filter.global_indices) {
            const auto entry_it = filter_entries.find(global_index);
            if (entry_it == filter_entries.end() || !entry_it->second.is_constant()) {
                return false;
            }
        }
        return !table_filter.global_indices.empty();
    }

    Status _build_constant_filter_block(const TableFilter& table_filter, Block* eval_block) {
        DORIS_CHECK(eval_block != nullptr);
        eval_block->clear();
        const auto& mappings = _data_reader.column_mapper->mappings();
        const auto& filter_entries = _data_reader.column_mapper->filter_entries();
        DORIS_CHECK(mappings.size() == _projected_columns.size());
        for (size_t column_idx = 0; column_idx < mappings.size(); ++column_idx) {
            const auto global_index = GlobalIndex(column_idx);
            const auto& mapping = mappings[column_idx];
            const auto entry_it = filter_entries.find(global_index);
            const bool referenced_by_filter =
                    std::find(table_filter.global_indices.begin(),
                              table_filter.global_indices.end(),
                              global_index) != table_filter.global_indices.end();
            if (referenced_by_filter && entry_it != filter_entries.end() &&
                entry_it->second.is_constant()) {
                ColumnPtr constant_column;
                RETURN_IF_ERROR(_materialize_constant_filter_column(
                        entry_it->second.constant_index(), &constant_column));
                eval_block->insert({std::move(constant_column), mapping.table_type,
                                    mapping.table_column_name});
            } else {
                eval_block->insert({mapping.table_type->create_column_const_with_default_value(1),
                                    mapping.table_type, mapping.table_column_name});
            }
        }
        return Status::OK();
    }

    Status _materialize_constant_filter_column(ConstantIndex constant_index, ColumnPtr* column) {
        DORIS_CHECK(column != nullptr);
        const auto& constant_entry = _data_reader.column_mapper->constant_map().get(constant_index);
        DORIS_CHECK(constant_entry.expr != nullptr);
        DORIS_CHECK(constant_entry.type != nullptr);
        RowDescriptor row_desc;
        RETURN_IF_ERROR(constant_entry.expr->prepare(_runtime_state, row_desc));
        RETURN_IF_ERROR(constant_entry.expr->open(_runtime_state));
        Block eval_block;
        eval_block.insert({constant_entry.type->create_column_const_with_default_value(1),
                           constant_entry.type, "__table_reader_constant_filter"});
        int result_column_id = -1;
        RETURN_IF_ERROR(constant_entry.expr->execute(&eval_block, &result_column_id));
        DORIS_CHECK(result_column_id >= 0);
        *column = eval_block.get_by_position(result_column_id).column;
        DORIS_CHECK((*column)->size() == 1);
        return Status::OK();
    }

    static bool _filter_result_filters_all(const ColumnPtr& filter_column) {
        DORIS_CHECK(filter_column.get() != nullptr);
        DORIS_CHECK(filter_column->size() == 1);
        return !filter_column->get_bool(0);
    }

    virtual Status customize_file_scan_request(FileScanRequest* file_request) {
        return _append_delete_predicate(file_request);
    }

    bool _is_table_level_count_active() const { return _remaining_table_level_count >= 0; }

    Status _materialize_count_rows(size_t rows, Block* block) const {
        DORIS_CHECK(block != nullptr);
        DORIS_CHECK(block->columns() > 0 || rows == 0);
        for (size_t column_idx = 0; column_idx < block->columns(); ++column_idx) {
            auto column = block->get_by_position(column_idx).type->create_column();
            column->resize(rows);
            block->replace_by_position(column_idx, std::move(column));
        }
        return Status::OK();
    }

    Status _read_table_level_count(Block* block, bool* eos) {
        DORIS_CHECK(block != nullptr);
        DORIS_CHECK(eos != nullptr);
        DORIS_CHECK(_push_down_agg_type == TPushAggOp::type::COUNT);
        DORIS_CHECK(_remaining_table_level_count >= 0);
        if (_remaining_table_level_count == 0) {
            _remaining_table_level_count = -1;
            _current_task.reset();
            *eos = true;
            return Status::OK();
        }

        const int64_t batch_size = _runtime_state == nullptr
                                           ? _remaining_table_level_count
                                           : static_cast<int64_t>(_runtime_state->batch_size());
        const auto rows = std::min(_remaining_table_level_count, batch_size);
        RETURN_IF_ERROR(_materialize_count_rows(cast_set<size_t>(rows), block));
        _remaining_table_level_count -= rows;
        *eos = false;
        return Status::OK();
    }

    void _append_file_scan_column(FileScanRequest* request, LocalColumnId column_id,
                                  std::vector<LocalColumnIndex>* scan_columns) {
        DORIS_CHECK(request != nullptr);
        DORIS_CHECK(scan_columns != nullptr);
        FileScanRequestBuilder builder(request);
        Status status;
        if (scan_columns == &request->predicate_columns) {
            status = builder.add_predicate_column(column_id);
        } else {
            DORIS_CHECK(scan_columns == &request->non_predicate_columns);
            status = builder.add_non_predicate_column(column_id);
        }
        DORIS_CHECK(status.ok()) << status.to_string();
        if (column_id == LocalColumnId(ROW_POSITION_COLUMN_ID) &&
            _find_column_definition(_data_reader.file_schema, column_id) == nullptr) {
            _data_reader.file_schema.push_back(row_position_column_definition());
        }
    }

    // Append DeletePredicate to file scan request if there are deletes. The predicate will be evaluated in file reader level and filter out deleted rows before returning data to table reader.
    Status _append_delete_predicate(FileScanRequest* request) {
        DORIS_CHECK(request != nullptr);
        if ((_delete_rows == nullptr || _delete_rows->empty()) &&
            (_deletion_vector == nullptr || _deletion_vector->isEmpty())) {
            return Status::OK();
        }
        const auto row_position_column_id = LocalColumnId(ROW_POSITION_COLUMN_ID);
        _append_file_scan_column(request, row_position_column_id, &request->predicate_columns);

        const auto block_position = request->local_positions.at(row_position_column_id);
        auto append_predicate = [&](auto& deleted_rows) {
            auto delete_predicate = std::make_shared<DeletePredicate>(deleted_rows);
            delete_predicate->add_child(VSlotRef::create_shared(
                    cast_set<int>(block_position.value()), cast_set<int>(block_position.value()),
                    -1, std::make_shared<DataTypeInt64>(), ROW_POSITION_COLUMN_NAME));
            request->delete_conjuncts.push_back(
                    VExprContext::create_shared(std::move(delete_predicate)));
        };
        if (_delete_rows != nullptr && !_delete_rows->empty()) {
            append_predicate(*_delete_rows);
        }
        if (_deletion_vector != nullptr && !_deletion_vector->isEmpty()) {
            append_predicate(*_deletion_vector);
        }
        return Status::OK();
    }

    // Close the current concrete reader. This hook is called by both create_next_reader() and
    // close(), so it should remain idempotent.
    virtual Status close_current_reader() {
        _finalize_reader_condition_cache();
        RETURN_IF_ERROR(_data_reader.reader->close());
        _data_reader.reader.reset();
        if (_data_reader.column_mapper != nullptr) {
            _data_reader.column_mapper->clear();
            _data_reader.column_mapper.reset();
        }
        _table_filters.clear();
        _data_reader.file_schema.clear();
        _data_reader.file_block_layout.clear();
        _data_reader.block_template.clear();
        _current_task.reset();
        _current_file_description.reset();
        _current_reader_reached_eof = false;
        return Status::OK();
    }

    void _record_scan_rows(size_t rows) {
        if (_io_ctx != nullptr && _io_ctx->file_reader_stats != nullptr) {
            _io_ctx->file_reader_stats->read_rows += rows;
        }
    }

    // Finalize file-local block to table/global schema block.
    Status finalize_chunk(Block* block, const size_t rows) {
        SCOPED_TIMER(_profile.finalize_timer);
        size_t idx = 0;
        for (const auto& mapping : _data_reader.column_mapper->mappings()) {
            ColumnPtr column;
            RETURN_IF_ERROR(_materialize_mapping_column(mapping, &_data_reader.block_template, rows,
                                                        &column));
            block->replace_by_position(idx, IColumn::mutate(std::move(column)));
            idx++;
        }
        RETURN_IF_ERROR(materialize_virtual_columns(block));
        // Enforce CHAR/VARCHAR length declared by the table schema after all file-to-table
        // materialization has finished.
        RETURN_IF_ERROR(_truncate_char_or_varchar_columns(block));
        return Status::OK();
    }

    // Materialize virtual columns in the table block, such as Iceberg _row_id and
    // _last_updated_sequence_number. This runs after normal column materialization so finalize
    // expressions can reference those virtual columns.
    virtual Status materialize_virtual_columns(Block* table_block) { return Status::OK(); }

#ifndef NDEBUG
    Status _check_file_block_columns(std::string_view stage, size_t rows) {
        DORIS_CHECK(_data_reader.block_template.columns() == _data_reader.file_block_layout.size());
        for (size_t idx = 0; idx < _data_reader.block_template.columns(); ++idx) {
            const auto& file_block_column = _data_reader.file_block_layout[idx];
            const auto& column_with_type = _data_reader.block_template.get_by_position(idx);
            const auto* column = column_with_type.column.get();
            try {
                if (column == nullptr) {
                    auto st = Status::InternalError(
                            "Invalid file block column {} at {}: file_column_id={}, name='{}', "
                            "type={}, column=null, expected_rows={}, reader={}",
                            idx, stage, file_block_column.file_column_id.value(),
                            file_block_column.name,
                            file_block_column.type == nullptr ? "null"
                                                              : file_block_column.type->get_name(),
                            rows, debug_string());
                    LOG(WARNING) << st;
                    return st;
                }
                column->sanity_check();
                auto st = column_with_type.check_type_and_column_match();
                if (!st.ok()) {
                    auto contextual_status = Status::InternalError(
                            "Invalid file block column {} at {}: file_column_id={}, name='{}', "
                            "type={}, column={}, column_size={}, expected_rows={}, error={}, "
                            "reader={}",
                            idx, stage, file_block_column.file_column_id.value(),
                            file_block_column.name,
                            file_block_column.type == nullptr ? "null"
                                                              : file_block_column.type->get_name(),
                            column->get_name(), column->size(), rows, st.to_string(),
                            debug_string());
                    LOG(WARNING) << contextual_status;
                    return contextual_status;
                }
            } catch (const Exception& e) {
                auto st = Status::InternalError(
                        "Invalid file block column {} at {}: file_column_id={}, name='{}', "
                        "type={}, column={}, column_size={}, expected_rows={}, error={}, "
                        "reader={}",
                        idx, stage, file_block_column.file_column_id.value(),
                        file_block_column.name,
                        file_block_column.type == nullptr ? "null"
                                                          : file_block_column.type->get_name(),
                        column == nullptr ? "null" : column->get_name(),
                        column == nullptr ? 0 : column->size(), rows, e.to_string(),
                        debug_string());
                LOG(WARNING) << st;
                return st;
            } catch (const std::exception& e) {
                auto st = Status::InternalError(
                        "Invalid file block column {} at {}: file_column_id={}, name='{}', "
                        "type={}, column={}, column_size={}, expected_rows={}, error={}, "
                        "reader={}",
                        idx, stage, file_block_column.file_column_id.value(),
                        file_block_column.name,
                        file_block_column.type == nullptr ? "null"
                                                          : file_block_column.type->get_name(),
                        column == nullptr ? "null" : column->get_name(),
                        column == nullptr ? 0 : column->size(), rows, e.what(), debug_string());
                LOG(WARNING) << st;
                return st;
            }
        }
        return Status::OK();
    }

    Status _check_table_block_columns(std::string_view stage, const Block* block, size_t rows) {
        DORIS_CHECK(block != nullptr);
        DORIS_CHECK(block->columns() == _data_reader.column_mapper->mappings().size());
        for (size_t idx = 0; idx < block->columns(); ++idx) {
            const auto& mapping = _data_reader.column_mapper->mappings()[idx];
            const auto& column_with_type = block->get_by_position(idx);
            const auto* column = column_with_type.column.get();
            try {
                if (column == nullptr) {
                    auto st = Status::InternalError(
                            "Invalid table block column {} at {}: table_column='{}', "
                            "global_index={}, type={}, column=null, expected_rows={}, mapping={}",
                            idx, stage, mapping.table_column_name, mapping.global_index.value(),
                            mapping.table_type == nullptr ? "null" : mapping.table_type->get_name(),
                            rows, mapping.debug_string());
                    LOG(WARNING) << st;
                    return st;
                }
                column->sanity_check();
                auto st = column_with_type.check_type_and_column_match();
                if (!st.ok()) {
                    auto contextual_status = Status::InternalError(
                            "Invalid table block column {} at {}: table_column='{}', "
                            "global_index={}, type={}, column={}, column_size={}, "
                            "expected_rows={}, error={}, mapping={}",
                            idx, stage, mapping.table_column_name, mapping.global_index.value(),
                            mapping.table_type == nullptr ? "null" : mapping.table_type->get_name(),
                            column->get_name(), column->size(), rows, st.to_string(),
                            mapping.debug_string());
                    LOG(WARNING) << contextual_status;
                    return contextual_status;
                }
            } catch (const Exception& e) {
                auto st = Status::InternalError(
                        "Invalid table block column {} at {}: table_column='{}', global_index={}, "
                        "type={}, column={}, column_size={}, expected_rows={}, error={}, "
                        "mapping={}",
                        idx, stage, mapping.table_column_name, mapping.global_index.value(),
                        mapping.table_type == nullptr ? "null" : mapping.table_type->get_name(),
                        column == nullptr ? "null" : column->get_name(),
                        column == nullptr ? 0 : column->size(), rows, e.to_string(),
                        mapping.debug_string());
                LOG(WARNING) << st;
                return st;
            } catch (const std::exception& e) {
                auto st = Status::InternalError(
                        "Invalid table block column {} at {}: table_column='{}', global_index={}, "
                        "type={}, column={}, column_size={}, expected_rows={}, error={}, "
                        "mapping={}",
                        idx, stage, mapping.table_column_name, mapping.global_index.value(),
                        mapping.table_type == nullptr ? "null" : mapping.table_type->get_name(),
                        column == nullptr ? "null" : column->get_name(),
                        column == nullptr ? 0 : column->size(), rows, e.what(),
                        mapping.debug_string());
                LOG(WARNING) << st;
                return st;
            }
        }
        return Status::OK();
    }
#endif

    Status _truncate_char_or_varchar_columns(Block* block) {
        DORIS_CHECK(block != nullptr);
        if (_runtime_state == nullptr ||
            !_runtime_state->query_options().truncate_char_or_varchar_columns) {
            return Status::OK();
        }
        DORIS_CHECK(block->columns() == _data_reader.column_mapper->mappings().size());
        for (size_t idx = 0; idx < _data_reader.column_mapper->mappings().size(); ++idx) {
            const auto& mapping = _data_reader.column_mapper->mappings()[idx];
            if (!_should_truncate_char_or_varchar_column(mapping)) {
                continue;
            }
            const auto target_len =
                    assert_cast<const DataTypeString*>(remove_nullable(mapping.table_type).get())
                            ->len();
            _truncate_char_or_varchar_column(block, idx, target_len);
        }
        return Status::OK();
    }

    // Return true when the table schema has a bounded CHAR/VARCHAR length that is stricter than
    // the file-side type. Examples:
    // - table VARCHAR(10), file VARCHAR(20): truncate to 10;
    // - table VARCHAR(10), file STRING: truncate to 10 because STRING has no declared bound;
    // - table STRING, any file type: no truncation because the target has no bound.
    static bool _should_truncate_char_or_varchar_column(const ColumnMapping& mapping) {
        if (mapping.table_type == nullptr) {
            return false;
        }
        const auto table_type = remove_nullable(mapping.table_type);
        const auto primitive_type = table_type->get_primitive_type();
        if (primitive_type != TYPE_VARCHAR && primitive_type != TYPE_CHAR) {
            return false;
        }
        const auto target_len = assert_cast<const DataTypeString*>(table_type.get())->len();
        if (target_len <= 0) {
            return false;
        }
        if (mapping.file_type == nullptr) {
            return true;
        }
        const auto file_type = remove_nullable(mapping.file_type);
        DORIS_CHECK(file_type != nullptr);
        int file_len = -1;
        if (file_type->get_primitive_type() == TYPE_VARCHAR ||
            file_type->get_primitive_type() == TYPE_CHAR ||
            file_type->get_primitive_type() == TYPE_STRING) {
            file_len = assert_cast<const DataTypeString*>(file_type.get())->len();
        }

        return file_len < 0 || target_len < file_len;
    }

    // Truncate a materialized CHAR/VARCHAR column in place by reusing the vectorized substring
    // implementation: substring(column, 1, len). Nullable columns are unwrapped before substring
    // execution and wrapped back with the original null map afterward, because substring operates
    // on the nested string payload only.
    static void _truncate_char_or_varchar_column(Block* block, size_t idx, int len) {
        DORIS_CHECK(block != nullptr);
        auto int_type = std::make_shared<DataTypeInt32>();
        const auto num_columns_without_result = cast_set<uint32_t>(block->columns());
        auto& target = block->get_by_position(idx);
        const bool is_nullable = target.type->is_nullable();
        ColumnPtr input_column = target.column;
        ColumnPtr null_map_column;
        if (is_nullable) {
            const auto* nullable_column = assert_cast<const ColumnNullable*>(target.column.get());
            input_column = nullable_column->get_nested_column_ptr();
            null_map_column = nullable_column->get_null_map_column_ptr();
        }
        block->replace_by_position(idx, std::move(input_column));
        block->insert({int_type->create_column_const(block->rows(), to_field<TYPE_INT>(1)),
                       int_type, "const 1"});
        block->insert({int_type->create_column_const(block->rows(), to_field<TYPE_INT>(len)),
                       int_type, "const len"});
        block->insert({nullptr, std::make_shared<DataTypeString>(), "result"});

        ColumnNumbers temp_arguments(3);
        temp_arguments[0] = cast_set<uint32_t>(idx);
        temp_arguments[1] = num_columns_without_result;
        temp_arguments[2] = num_columns_without_result + 1;
        const uint32_t result_column_id = num_columns_without_result + 2;
        SubstringUtil::substring_execute(*block, temp_arguments, result_column_id, block->rows());

        ColumnPtr result_column = block->get_by_position(result_column_id).column;
        if (is_nullable) {
            result_column = ColumnNullable::create(std::move(result_column), null_map_column);
        }
        block->replace_by_position(idx, std::move(result_column));
        block->erase_tail(num_columns_without_result);
    }

    Status _try_materialize_aggregate_pushdown_rows(Block* block, bool* pushed_down) {
        DORIS_CHECK(block != nullptr);
        DORIS_CHECK(pushed_down != nullptr);
        *pushed_down = false;
        block->clear_column_data(_projected_columns.size());
        _aggregate_pushdown_tried = true;
        if (!_supports_aggregate_pushdown(_push_down_agg_type)) {
            return Status::OK();
        }

        FileAggregateRequest file_request;
        RETURN_IF_ERROR(_build_file_aggregate_request(_push_down_agg_type, &file_request));
        FileAggregateResult file_result;
        const auto status = _data_reader.reader->get_aggregate_result(file_request, &file_result);
        if (status.is<ErrorCode::NOT_IMPLEMENTED_ERROR>()) {
            return Status::OK();
        }
        RETURN_IF_ERROR(status);
        RETURN_IF_ERROR(
                _materialize_aggregate_pushdown_rows(_push_down_agg_type, file_result, block));
        *pushed_down = true;
        RETURN_IF_ERROR(close_current_reader());
        return Status::OK();
    }

    virtual bool _supports_aggregate_pushdown(TPushAggOp::type agg_type) const {
        // Only COUNT and MIN/MAX can be push down.
        if (agg_type != TPushAggOp::type::COUNT && agg_type != TPushAggOp::type::MINMAX) {
            return false;
        }
        // Only support aggregate pushdown when there is no delete or filter, so
        // the reduced rows consumed by the upper aggregate remain semantically equivalent to a
        // normal scan.
        if ((_delete_rows != nullptr && !_delete_rows->empty()) ||
            (_deletion_vector != nullptr && !_deletion_vector->isEmpty())) {
            return false;
        }
        if (!_table_filters.empty()) {
            return false;
        }
        if (agg_type == TPushAggOp::type::COUNT) {
            return true;
        }
        // For MIN/MAX, only support direct file-to-table column mappings. The two emitted rows
        // must be enough for the upper MIN/MAX aggregate without evaluating default expressions or
        // virtual columns.
        for (const auto& mapping : _data_reader.column_mapper->mappings()) {
            if (!mapping.file_local_id.has_value() ||
                mapping.virtual_column_type != TableVirtualColumnType::INVALID ||
                mapping.default_expr != nullptr || mapping.file_type == nullptr ||
                mapping.table_type == nullptr) {
                return false;
            }
            if (!_can_push_down_minmax_for_mapping(mapping)) {
                return false;
            }
        }
        return true;
    }

    static ColumnPtr _detach_column(ColumnPtr column) {
        DORIS_CHECK(column.get() != nullptr);
        return IColumn::mutate(std::move(column));
    }

    static Status _align_column_nullability(ColumnPtr* column, const DataTypePtr& table_type) {
        DORIS_CHECK(column != nullptr);
        DORIS_CHECK(column->get() != nullptr);
        DORIS_CHECK(table_type != nullptr);
        // Must return non-const column
        *column = (*column)->convert_to_full_column_if_const();
        if (table_type->is_nullable()) {
            const auto& nested_type =
                    assert_cast<const DataTypeNullable&>(*table_type).get_nested_type();
            if (!(*column)->is_nullable()) {
                RETURN_IF_ERROR(_align_column_nullability(column, nested_type));
                *column = make_nullable(*column);
                return Status::OK();
            }
            const auto& nullable_column = assert_cast<const ColumnNullable&>(**column);
            ColumnPtr nested_column = nullable_column.get_nested_column_ptr();
            RETURN_IF_ERROR(_align_column_nullability(&nested_column, nested_type));
            *column = ColumnNullable::create(nested_column,
                                             nullable_column.get_null_map_column_ptr());
            return Status::OK();
        }
        if ((*column)->is_nullable()) {
            const auto& nullable_column = assert_cast<const ColumnNullable&>(**column);
            if (nullable_column.has_null()) {
                return Status::InternalError(
                        "Default expression produced NULL for non-nullable table column");
            }
            ColumnPtr nested_column = nullable_column.get_nested_column_ptr();
            RETURN_IF_ERROR(_align_column_nullability(&nested_column, table_type));
            *column = nested_column;
            return Status::OK();
        }
        if (const auto* array_type = typeid_cast<const DataTypeArray*>(table_type.get())) {
            const auto& array_column = assert_cast<const ColumnArray&>(**column);
            ColumnPtr nested_column = array_column.get_data_ptr();
            RETURN_IF_ERROR(
                    _align_column_nullability(&nested_column, array_type->get_nested_type()));
            *column = ColumnArray::create(nested_column, array_column.get_offsets_ptr());
            return Status::OK();
        }
        if (const auto* map_type = typeid_cast<const DataTypeMap*>(table_type.get())) {
            const auto& map_column = assert_cast<const ColumnMap&>(**column);
            ColumnPtr key_column = map_column.get_keys_ptr();
            ColumnPtr value_column = map_column.get_values_ptr();
            RETURN_IF_ERROR(_align_column_nullability(&key_column, map_type->get_key_type()));
            RETURN_IF_ERROR(_align_column_nullability(&value_column, map_type->get_value_type()));
            *column = ColumnMap::create(key_column, value_column, map_column.get_offsets_ptr());
            return Status::OK();
        }
        if (const auto* struct_type = typeid_cast<const DataTypeStruct*>(table_type.get())) {
            const auto& struct_column = assert_cast<const ColumnStruct&>(**column);
            Columns columns = struct_column.get_columns_copy();
            DORIS_CHECK(columns.size() == struct_type->get_elements().size());
            for (size_t i = 0; i < columns.size(); ++i) {
                RETURN_IF_ERROR(
                        _align_column_nullability(&columns[i], struct_type->get_element(i)));
            }
            *column = ColumnStruct::create(columns);
            return Status::OK();
        }
        return Status::OK();
    }

    static Status _execute_default_expr_without_root_type_check(
            const VExprContextSPtr& default_expr, const Block* block,
            ColumnWithTypeAndName* result_data) {
        DORIS_CHECK(default_expr != nullptr);
        DORIS_CHECK(block != nullptr);
        DORIS_CHECK(result_data != nullptr);
        ColumnPtr result_column;
        Status st;
        RETURN_IF_CATCH_EXCEPTION({
            st = default_expr->root()->execute_column_impl(default_expr.get(), block, nullptr,
                                                           block->rows(), result_column);
        });
        RETURN_IF_ERROR(st);
        DORIS_CHECK(result_column.get() != nullptr);
        if (result_column->size() != block->rows()) {
            return Status::InternalError(
                    "Default expr {} return column size {} not equal to expected size {}",
                    default_expr->expr_name(), result_column->size(), block->rows());
        }
        result_data->column = result_column;
        result_data->type = default_expr->execute_type(block);
        result_data->name = default_expr->expr_name();
        return Status::OK();
    }

    Status _cast_column_to_type(ColumnPtr* column, const DataTypePtr& file_type,
                                const DataTypePtr& table_type,
                                const std::string& column_name) const {
        DORIS_CHECK(column != nullptr);
        DORIS_CHECK(column->get() != nullptr);
        DORIS_CHECK(file_type != nullptr);
        DORIS_CHECK(table_type != nullptr);
        if (file_type->equals(*table_type)) {
            return Status::OK();
        }

        DataTypePtr input_type = file_type;
        // Cast wrappers unwrap nullable inputs according to the declared input type, so keep the
        // root nullability of the declared type aligned with the actual column shape.
        if ((*column)->is_nullable() && !input_type->is_nullable()) {
            input_type = make_nullable(input_type);
        } else if (!(*column)->is_nullable() && input_type->is_nullable()) {
            input_type = remove_nullable(input_type);
        }
        Block cast_block;
        cast_block.insert({*column, input_type, column_name});
        auto slot_ref = VSlotRef::create_shared(0, 0, -1, input_type, column_name);
        auto cast_expr = Cast::create_shared(table_type);
        cast_expr->add_child(std::move(slot_ref));
        auto cast_ctx = VExprContext::create_shared(std::move(cast_expr));
        RowDescriptor row_desc;
        RETURN_IF_ERROR(cast_ctx->prepare(_runtime_state, row_desc));
        RETURN_IF_ERROR(cast_ctx->open(_runtime_state));
        ColumnPtr cast_column;
        RETURN_IF_ERROR(cast_ctx->execute(&cast_block, cast_column));
        *column = std::move(cast_column);
        return Status::OK();
    }

    Status _materialize_present_child_mapping_column(const ColumnMapping& mapping,
                                                     const ColumnPtr& file_column,
                                                     const size_t rows, ColumnPtr* column) {
        DORIS_CHECK(column != nullptr);
        DORIS_CHECK(mapping.file_type != nullptr);
        DORIS_CHECK(mapping.table_type != nullptr);
        *column = file_column;
        if (!mapping.is_trivial) {
            if (!mapping.child_mappings.empty()) {
                RETURN_IF_ERROR(
                        _materialize_complex_mapping_column(mapping, *column, rows, column));
            } else {
                RETURN_IF_ERROR(_cast_column_to_type(column, mapping.file_type, mapping.table_type,
                                                     mapping.file_column_name));
            }
        }
        RETURN_IF_ERROR(_align_column_nullability(column, mapping.table_type));
        return Status::OK();
    }

    Status _materialize_mapping_column(const ColumnMapping& mapping, Block* current_block,
                                       const size_t rows, ColumnPtr* column) {
        if (!mapping.is_trivial && mapping.file_local_id.has_value() &&
            !mapping.child_mappings.empty()) {
            DCHECK(mapping.projection != nullptr);
            int res_id;
            auto st = mapping.projection->execute(current_block, &res_id);
            if (!st.ok()) {
                return Status::InternalError(
                        "Failed to execute complex mapping projection for table column '{}' "
                        "(global_index={}, file_local_id={}, rows={}): {}, mapping={}",
                        mapping.table_column_name, mapping.global_index.value(),
                        *mapping.file_local_id, rows, st.to_string(), mapping.debug_string());
            }
            ColumnPtr result_column = current_block->get_by_position(res_id).column;
            RETURN_IF_ERROR(
                    _materialize_complex_mapping_column(mapping, result_column, rows, column));
            return Status::OK();
        }
        if (mapping.projection != nullptr) {
            int res_id;
            auto st = mapping.projection->execute(current_block, &res_id);
            if (!st.ok()) {
                std::string file_local_id = "null";
                if (mapping.file_local_id.has_value()) {
                    file_local_id = std::to_string(*mapping.file_local_id);
                }
                return Status::InternalError(
                        "Failed to execute mapping projection for table column '{}' "
                        "(global_index={}, file_local_id={}, rows={}): {}, mapping={}",
                        mapping.table_column_name, mapping.global_index.value(), file_local_id,
                        rows, st.to_string(), mapping.debug_string());
            }
            ColumnPtr result_column = current_block->get_by_position(res_id).column;
            *column = _detach_column(std::move(result_column));
            return Status::OK();
        }
        if (mapping.default_expr != nullptr) {
            if (current_block->rows() == rows) {
                ColumnWithTypeAndName result;
                RETURN_IF_ERROR(_execute_default_expr_without_root_type_check(
                        mapping.default_expr, current_block, &result));
                ColumnPtr result_column = result.column;
                RETURN_IF_ERROR(_align_column_nullability(&result_column, mapping.table_type));
                *column = _detach_column(std::move(result_column));
            } else {
                DORIS_CHECK(mapping.constant_index.has_value());
                Block eval_block;
                eval_block.insert({mapping.table_type->create_column_const_with_default_value(rows),
                                   mapping.table_type, "__table_reader_const_rows"});
                ColumnWithTypeAndName result;
                RETURN_IF_ERROR(_execute_default_expr_without_root_type_check(
                        mapping.default_expr, &eval_block, &result));
                ColumnPtr result_column = result.column;
                RETURN_IF_ERROR(_align_column_nullability(&result_column, mapping.table_type));
                *column = _detach_column(std::move(result_column));
            }
            return Status::OK();
        }
        ColumnPtr result_column = mapping.table_type->create_column_const_with_default_value(rows);
        *column = _detach_column(std::move(result_column));
        return Status::OK();
    }

    Status _materialize_complex_mapping_column(const ColumnMapping& mapping,
                                               const ColumnPtr& file_column, const size_t rows,
                                               ColumnPtr* column) {
        DORIS_CHECK(mapping.table_type != nullptr);
        DORIS_CHECK(file_column.get() != nullptr);
        const auto table_type = remove_nullable(mapping.table_type);
        switch (table_type->get_primitive_type()) {
        case TYPE_STRUCT:
            RETURN_IF_ERROR(_materialize_struct_mapping_column(mapping, file_column, rows, column));
            break;
        case TYPE_ARRAY:
            RETURN_IF_ERROR(_materialize_array_mapping_column(mapping, file_column, rows, column));
            break;
        case TYPE_MAP:
            RETURN_IF_ERROR(_materialize_map_mapping_column(mapping, file_column, rows, column));
            break;
        default:
            *column = _detach_column(file_column);
            break;
        }
        return Status::OK();
    }

    static std::vector<const ColumnMapping*> _present_child_mappings_in_file_order(
            const std::vector<ColumnMapping>& child_mappings) {
        std::vector<const ColumnMapping*> result;
        result.reserve(child_mappings.size());
        for (const auto& child_mapping : child_mappings) {
            if (child_mapping.file_local_id.has_value()) {
                result.push_back(&child_mapping);
            }
        }
        std::ranges::sort(result, [](const ColumnMapping* lhs, const ColumnMapping* rhs) {
            DORIS_CHECK(lhs->file_local_id.has_value());
            DORIS_CHECK(rhs->file_local_id.has_value());
            return *lhs->file_local_id < *rhs->file_local_id;
        });
        return result;
    }

    static size_t _file_child_ordinal_for_mapping(
            const ColumnMapping& mapping, const ColumnMapping& child_mapping,
            const std::vector<const ColumnMapping*>& file_ordered_children) {
        DORIS_CHECK(child_mapping.file_local_id.has_value());
        if (!mapping.projected_file_children.empty()) {
            const auto child_it = std::ranges::find_if(
                    mapping.projected_file_children, [&](const ColumnDefinition& file_child) {
                        return file_child.file_local_id() == *child_mapping.file_local_id;
                    });
            DORIS_CHECK(child_it != mapping.projected_file_children.end());
            return static_cast<size_t>(
                    std::distance(mapping.projected_file_children.begin(), child_it));
        }
        const auto child_it = std::ranges::find(file_ordered_children, &child_mapping);
        DORIS_CHECK(child_it != file_ordered_children.end());
        return static_cast<size_t>(std::distance(file_ordered_children.begin(), child_it));
    }

    static std::vector<const ColumnMapping*> _child_mappings_in_table_type_order(
            const ColumnMapping& mapping, const DataTypeStruct& table_type) {
        std::vector<const ColumnMapping*> result;
        result.reserve(mapping.child_mappings.size());
        for (size_t child_idx = 0; child_idx < table_type.get_elements().size(); ++child_idx) {
            const auto& child_name = table_type.get_element_name(child_idx);
            const auto child_it = std::ranges::find_if(
                    mapping.child_mappings, [&](const ColumnMapping& child_mapping) {
                        return child_mapping.table_column_name == child_name;
                    });
            DORIS_CHECK(child_it != mapping.child_mappings.end())
                    << mapping.debug_string() << ", table_child_name=" << child_name;
            result.push_back(&*child_it);
        }
        return result;
    }

    static const IColumn* _nested_column_if_nullable(const ColumnPtr& column,
                                                     const NullMap** null_map) {
        DORIS_CHECK(column.get() != nullptr);
        if (const auto* nullable_column = check_and_get_column<ColumnNullable>(*column)) {
            if (null_map != nullptr) {
                *null_map = &nullable_column->get_null_map_data();
            }
            return &nullable_column->get_nested_column();
        }
        return column.get();
    }

    Status _materialize_struct_mapping_column(const ColumnMapping& mapping,
                                              const ColumnPtr& file_column, const size_t rows,
                                              ColumnPtr* column) {
        DORIS_CHECK(mapping.table_type != nullptr);
        const auto* table_type =
                assert_cast<const DataTypeStruct*>(remove_nullable(mapping.table_type).get());
        const auto full_file_column = file_column->convert_to_full_column_if_const();
        const NullMap* parent_null_map = nullptr;
        const auto* nested_file_column =
                _nested_column_if_nullable(full_file_column, &parent_null_map);
        const auto* file_struct = assert_cast<const ColumnStruct*>(nested_file_column);
        DORIS_CHECK(table_type->get_elements().size() == mapping.child_mappings.size());

        Columns child_columns;
        child_columns.reserve(mapping.child_mappings.size());
        const auto file_ordered_children =
                _present_child_mappings_in_file_order(mapping.child_mappings);
        const auto table_ordered_children =
                _child_mappings_in_table_type_order(mapping, *table_type);
        for (const auto* child_mapping : table_ordered_children) {
            DORIS_CHECK(child_mapping != nullptr);
            if (!child_mapping->file_local_id.has_value()) {
                child_columns.push_back(
                        child_mapping->table_type->create_column_const_with_default_value(rows)
                                ->convert_to_full_column_if_const());
                continue;
            }
            const auto file_child_idx =
                    _file_child_ordinal_for_mapping(mapping, *child_mapping, file_ordered_children);
            DORIS_CHECK(file_child_idx < file_struct->get_columns().size());
            ColumnPtr child_column = file_struct->get_column_ptr(file_child_idx);
            RETURN_IF_ERROR(_materialize_present_child_mapping_column(*child_mapping, child_column,
                                                                      rows, &child_column));
            child_columns.push_back(std::move(child_column));
        }
        MutableColumns mutable_child_columns;
        mutable_child_columns.reserve(child_columns.size());
        for (auto& child_column : child_columns) {
            mutable_child_columns.push_back(IColumn::mutate(std::move(child_column)));
        }
        auto result = ColumnStruct::create(std::move(mutable_child_columns));
        if (mapping.table_type->is_nullable()) {
            auto null_map = ColumnUInt8::create();
            auto& null_map_data = null_map->get_data();
            null_map_data.resize(rows);
            if (parent_null_map != nullptr) {
                DORIS_CHECK(parent_null_map->size() == rows);
                null_map_data.assign(parent_null_map->begin(), parent_null_map->end());
            } else {
                std::fill(null_map_data.begin(), null_map_data.end(), 0);
            }
            *column = ColumnNullable::create(std::move(result), std::move(null_map));
        } else {
            *column = std::move(result);
        }
        return Status::OK();
    }

    Status _materialize_array_mapping_column(const ColumnMapping& mapping,
                                             const ColumnPtr& file_column, const size_t rows,
                                             ColumnPtr* column) {
        DORIS_CHECK(mapping.child_mappings.size() == 1);
        const auto full_file_column = file_column->convert_to_full_column_if_const();
        const NullMap* parent_null_map = nullptr;
        const auto* nested_file_column =
                _nested_column_if_nullable(full_file_column, &parent_null_map);
        const auto* file_array = assert_cast<const ColumnArray*>(nested_file_column);
        ColumnPtr nested_column = file_array->get_data_ptr();
        const auto& element_mapping = mapping.child_mappings[0];
        RETURN_IF_ERROR(_materialize_present_child_mapping_column(
                element_mapping, nested_column, nested_column->size(), &nested_column));
        auto offsets_column = file_array->get_offsets_ptr()->convert_to_full_column_if_const();
        auto result = ColumnArray::create(IColumn::mutate(std::move(nested_column)),
                                          IColumn::mutate(std::move(offsets_column)));
        if (mapping.table_type->is_nullable()) {
            auto null_map = ColumnUInt8::create();
            auto& null_map_data = null_map->get_data();
            null_map_data.resize(rows);
            if (parent_null_map != nullptr) {
                DORIS_CHECK(parent_null_map->size() == rows);
                null_map_data.assign(parent_null_map->begin(), parent_null_map->end());
            } else {
                std::fill(null_map_data.begin(), null_map_data.end(), 0);
            }
            *column = ColumnNullable::create(std::move(result), std::move(null_map));
        } else {
            *column = std::move(result);
        }
        return Status::OK();
    }

    Status _materialize_map_mapping_column(const ColumnMapping& mapping,
                                           const ColumnPtr& file_column, const size_t rows,
                                           ColumnPtr* column) {
        const auto full_file_column = file_column->convert_to_full_column_if_const();
        const NullMap* parent_null_map = nullptr;
        const auto* nested_file_column =
                _nested_column_if_nullable(full_file_column, &parent_null_map);
        const auto* file_map = assert_cast<const ColumnMap*>(nested_file_column);
        ColumnPtr key_column = file_map->get_keys_ptr();
        ColumnPtr value_column = file_map->get_values_ptr();

        const ColumnMapping* key_mapping = nullptr;
        const ColumnMapping* value_mapping = nullptr;
        for (const auto& child_mapping : mapping.child_mappings) {
            if (!child_mapping.file_local_id.has_value()) {
                continue;
            }
            if (*child_mapping.file_local_id == 0) {
                key_mapping = &child_mapping;
            } else if (*child_mapping.file_local_id == 1) {
                value_mapping = &child_mapping;
            }
        }

        if (key_mapping != nullptr) {
            RETURN_IF_ERROR(_materialize_present_child_mapping_column(
                    *key_mapping, key_column, key_column->size(), &key_column));
        }
        if (value_mapping != nullptr) {
            RETURN_IF_ERROR(_materialize_present_child_mapping_column(
                    *value_mapping, value_column, value_column->size(), &value_column));
        }
        auto offsets_column = file_map->get_offsets_ptr()->convert_to_full_column_if_const();
        auto result = ColumnMap::create(IColumn::mutate(std::move(key_column)),
                                        IColumn::mutate(std::move(value_column)),
                                        IColumn::mutate(std::move(offsets_column)));
        if (mapping.table_type->is_nullable()) {
            auto null_map = ColumnUInt8::create();
            auto& null_map_data = null_map->get_data();
            null_map_data.resize(rows);
            if (parent_null_map != nullptr) {
                DORIS_CHECK(parent_null_map->size() == rows);
                null_map_data.assign(parent_null_map->begin(), parent_null_map->end());
            } else {
                std::fill(null_map_data.begin(), null_map_data.end(), 0);
            }
            *column = ColumnNullable::create(std::move(result), std::move(null_map));
        } else {
            *column = std::move(result);
        }
        return Status::OK();
    }

    Status _open_mapping_exprs() {
        RowDescriptor row_desc;
        for (const auto& mapping : _data_reader.column_mapper->mappings()) {
            if (mapping.projection != nullptr) {
                RETURN_IF_ERROR(mapping.projection->prepare(_runtime_state, row_desc));
                RETURN_IF_ERROR(mapping.projection->open(_runtime_state));
            }
            if (mapping.default_expr != nullptr) {
                RETURN_IF_ERROR(mapping.default_expr->prepare(_runtime_state, row_desc));
                RETURN_IF_ERROR(mapping.default_expr->open(_runtime_state));
            }
        }
        return Status::OK();
    }

    Status _build_file_aggregate_request(TPushAggOp::type agg_type,
                                         FileAggregateRequest* request) const {
        DORIS_CHECK(request != nullptr);
        DORIS_CHECK(_supports_aggregate_pushdown(agg_type));
        request->agg_type = agg_type;
        request->columns.clear();
        if (agg_type == TPushAggOp::type::COUNT) {
            // COUNT pushdown historically meant COUNT(*) and therefore carried no columns. For
            // complex COUNT(col), materializing the full MAP/LIST/STRUCT value only to test the
            // top-level NULL bit can be extremely expensive. When the scan projects exactly one
            // directly-mapped complex column, pass that file column to the reader so formats such
            // as Parquet can count the column shape from metadata/levels without decoding payload
            // values like MAP value strings. Other COUNT cases stay on the existing row-count path
            // to avoid changing count(*) semantics.
            if (_data_reader.column_mapper->mappings().size() == 1) {
                const auto& mapping = _data_reader.column_mapper->mappings()[0];
                if (mapping.file_local_id.has_value() && mapping.file_type != nullptr &&
                    is_complex_type(remove_nullable(mapping.file_type)->get_primitive_type()) &&
                    mapping.virtual_column_type == TableVirtualColumnType::INVALID &&
                    mapping.default_expr == nullptr) {
                    FileAggregateRequest::Column column;
                    column.projection =
                            LocalColumnIndex::top_level(LocalColumnId(*mapping.file_local_id));
                    request->columns.push_back(std::move(column));
                }
            }
            return Status::OK();
        }
        request->columns.reserve(_data_reader.column_mapper->mappings().size());
        for (const auto& mapping : _data_reader.column_mapper->mappings()) {
            DORIS_CHECK(mapping.file_local_id.has_value());
            FileAggregateRequest::Column column;
            column.projection = LocalColumnIndex::top_level(LocalColumnId(*mapping.file_local_id));
            if (!mapping.child_mappings.empty()) {
                RETURN_IF_ERROR(build_aggregate_projection(mapping, &column.projection));
            }
            request->columns.push_back(std::move(column));
        }
        return Status::OK();
    }

    Status _materialize_aggregate_pushdown_rows(TPushAggOp::type agg_type,
                                                const FileAggregateResult& file_result,
                                                Block* block) {
        if (agg_type == TPushAggOp::type::COUNT) {
            // COUNT pushdown is not a final count value. It emits `count` default rows so the
            // upper COUNT(*) aggregate can count them and produce the final result, including
            // zero rows when count is 0.
            DORIS_CHECK(file_result.count >= 0);
            return _materialize_count_rows(cast_set<size_t>(file_result.count), block);
        }
        // MIN/MAX pushdown emits two rows, min first and max second, for each projected column.
        // The upper MIN/MAX aggregate consumes those two rows to produce the final aggregate value.
        DORIS_CHECK(file_result.columns.size() == _data_reader.column_mapper->mappings().size());
        DORIS_CHECK(block->columns() == _data_reader.column_mapper->mappings().size());
        Block file_block;
        file_block.reserve(_data_reader.file_block_layout.size());
        for (const auto& column : _data_reader.file_block_layout) {
            file_block.insert({column.type->create_column(), column.type, column.name});
        }
        for (size_t column_idx = 0; column_idx < file_result.columns.size(); ++column_idx) {
            const auto& result_column = file_result.columns[column_idx];
            if (!result_column.has_min || !result_column.has_max) {
                return Status::NotSupported("Missing min/max aggregate result for column {}",
                                            _projected_columns[column_idx].name);
            }
            bool found_file_column = false;
            for (size_t block_position = 0; block_position < _data_reader.file_block_layout.size();
                 ++block_position) {
                if (_data_reader.file_block_layout[block_position].file_column_id ==
                    file_result.columns[column_idx].projection.column_id()) {
                    found_file_column = true;
                    auto column = file_block.get_by_position(block_position)
                                          .type->create_column()
                                          ->assert_mutable();
                    RETURN_IF_ERROR(_insert_aggregate_projection_value(
                            file_result.columns[column_idx].projection, result_column.min_value,
                            column.get()));
                    RETURN_IF_ERROR(_insert_aggregate_projection_value(
                            file_result.columns[column_idx].projection, result_column.max_value,
                            column.get()));
                    file_block.replace_by_position(block_position, std::move(column));
                    break;
                }
            }
            DORIS_CHECK(found_file_column);
        }
        for (size_t column_idx = 0; column_idx < _data_reader.column_mapper->mappings().size();
             ++column_idx) {
            ColumnPtr table_column;
            RETURN_IF_ERROR(
                    _materialize_mapping_column(_data_reader.column_mapper->mappings()[column_idx],
                                                &file_block, 2, &table_column));
            block->replace_by_position(column_idx, std::move(table_column));
        }
        return Status::OK();
    }

    struct FileBlockColumn {
        LocalColumnId file_column_id = LocalColumnId::invalid();
        std::string name;
        DataTypePtr type;
    };

    struct DataReader {
        std::unique_ptr<FileReader> reader;
        std::unique_ptr<TableColumnMapper> column_mapper;
        // Schema of the data file, also including virtual column (row position).
        std::vector<ColumnDefinition> file_schema;
        // Layout of the block returned by file reader, determined by column mapping and file
        // schema. It is used for file reader to materialize columns into correct type and position.
        std::vector<FileBlockColumn> file_block_layout;
        Block block_template;
    };
    DataReader _data_reader;
    std::vector<ColumnDefinition> _projected_columns;
    std::unique_ptr<ScanTask> _current_task;
    std::optional<io::FileDescription> _current_file_description;
    // Range-level compression has higher priority than scan-param compression. TVF/load can keep
    // the logical format as CSV/TEXT while carrying the concrete compression such as GZ or LZO on
    // each TFileRangeDesc, matching the old FileScanner reader contract.
    TFileCompressType::type _current_range_compress_type = TFileCompressType::UNKNOWN;
    std::optional<TUniqueId> _current_range_load_id;
    TFileRangeDesc _current_file_range_desc;
    std::shared_ptr<io::FileSystemProperties> _system_properties;
    // partition key -> value
    std::map<std::string, Field> _partition_values;
    // Predicates built from scan conjuncts before file-level localization.
    std::vector<TableFilter> _table_filters;
    VExprContextSPtrs _conjuncts;
    ReadProfile _profile;
    // Parsed from row-position based delete files, including position delete and deletion vector.
    DeleteRows* _delete_rows = nullptr;
    DeletionVector* _deletion_vector = nullptr;
    TFileScanRangeParams* _scan_params;
    std::shared_ptr<io::IOContext> _io_ctx;
    RuntimeState* _runtime_state;
    RuntimeProfile* _scanner_profile;
    const std::vector<SlotDescriptor*>* _file_slot_descs = nullptr;
    FileFormat _format;
    TPushAggOp::type _push_down_agg_type = TPushAggOp::type::NONE;
    size_t _batch_size = 0;
    uint64_t _condition_cache_digest = 0;
    segment_v2::ConditionCache::ExternalCacheKey _condition_cache_key;
    std::shared_ptr<std::vector<bool>> _condition_cache;
    std::shared_ptr<ConditionCacheContext> _condition_cache_ctx;
    int64_t _condition_cache_hit_count = 0;
    bool _current_reader_reached_eof = false;
    int64_t _remaining_table_level_count = -1;
    std::optional<GlobalRowIdContext> _global_rowid_context;
    bool _aggregate_pushdown_tried = false;
    bool _current_split_pruned = false;
    TableColumnMapperOptions _mapper_options;

private:
    static const ColumnDefinition* _find_column_definition(
            const std::vector<ColumnDefinition>& schema, LocalColumnId column_id) {
        for (const auto& field : schema) {
            if (field.file_local_id() == column_id.value()) {
                return &field;
            }
        }
        return nullptr;
    }

    static bool _can_push_down_minmax_for_mapping(const ColumnMapping& mapping) {
        if (mapping.child_mappings.empty()) {
            return true;
        }
        const auto primitive_type = remove_nullable(mapping.file_type)->get_primitive_type();
        if (primitive_type != TYPE_STRUCT) {
            return false;
        }
        size_t mapped_children = 0;
        const ColumnMapping* mapped_child = nullptr;
        for (const auto& child_mapping : mapping.child_mappings) {
            if (!child_mapping.file_local_id.has_value()) {
                continue;
            }
            ++mapped_children;
            mapped_child = &child_mapping;
        }
        return mapped_children == 1 && mapped_child != nullptr &&
               _can_push_down_minmax_for_mapping(*mapped_child);
    }

    static Status build_aggregate_projection(const ColumnMapping& mapping,
                                             LocalColumnIndex* projection) {
        DORIS_CHECK(projection != nullptr);
        DORIS_CHECK(mapping.file_local_id.has_value());
        *projection = LocalColumnIndex::local(*mapping.file_local_id);
        projection->children.clear();
        projection->project_all_children = true;
        if (mapping.child_mappings.empty()) {
            return Status::OK();
        }
        projection->project_all_children = false;
        for (const auto& child_mapping : mapping.child_mappings) {
            if (!child_mapping.file_local_id.has_value()) {
                continue;
            }
            LocalColumnIndex child_projection;
            RETURN_IF_ERROR(build_aggregate_projection(child_mapping, &child_projection));
            projection->children.push_back(std::move(child_projection));
        }
        DORIS_CHECK(projection->children.size() == 1);
        return Status::OK();
    }

    static Status _insert_aggregate_projection_value(const LocalColumnIndex& projection,
                                                     const Field& value, IColumn* column) {
        DORIS_CHECK(column != nullptr);
        if (auto* nullable_column = check_and_get_column<ColumnNullable>(*column)) {
            RETURN_IF_ERROR(_insert_aggregate_projection_value(
                    projection, value, &nullable_column->get_nested_column()));
            nullable_column->get_null_map_data().push_back(0);
            return Status::OK();
        }
        if (projection.project_all_children || projection.children.empty()) {
            column->insert(value);
            return Status::OK();
        }
        auto* struct_column = assert_cast<ColumnStruct*>(column);
        DORIS_CHECK(projection.children.size() == 1);
        const auto& child_projection = projection.children[0];
        DORIS_CHECK(struct_column->get_columns().size() == 1);
        RETURN_IF_ERROR(_insert_aggregate_projection_value(child_projection, value,
                                                           &struct_column->get_column(0)));
        return Status::OK();
    }

    // Parse a DV into its compressed bitmap. Position delete files continue to use _delete_rows.
    Status _parse_delete_predicates(const SplitReadOptions& options);
};

} // namespace doris::format
