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

#include <gen_cpp/PlanNodes_types.h>

#include <functional>
#include <memory>
#include <set>
#include <string>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "core/column/column.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "exprs/vexpr_fwd.h"
#include "format/column_descriptor.h"
#include "format/table/table_schema_change_helper.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "storage/predicate/block_column_predicate.h"
#include "storage/segment/common.h"
#include "util/profile_collector.h"

namespace doris {
class ColumnPredicate;
} // namespace doris

namespace doris {
#include "common/compile_check_begin.h"

class Block;
class VSlotRef;

// Context passed from FileScanner to readers for condition cache integration.
// On MISS: readers populate filter_result per-granule during predicate evaluation.
// On HIT: readers skip granules where filter_result[granule] == false.
struct ConditionCacheContext {
    bool is_hit = false;
    std::shared_ptr<std::vector<bool>> filter_result; // per-granule: true = has surviving rows
    int64_t base_granule = 0; // global granule index of the first granule in filter_result
    static constexpr int GRANULE_SIZE = 2048;
};

/// Base reader interface for all file readers.
/// A GenericReader is responsible for reading a file and returning
/// a set of blocks with specified schema.
///
/// Also provides hook virtual methods that table-format subclasses
/// (Iceberg, Hive ACID, Paimon, Hudi) override via CRTP or direct inheritance.
///
/// These hooks implement the Template Method pattern:
///   init_reader:      on_before_init_reader  → _do_init_reader → on_after_init_reader
///   get_next_block:   on_before_read_block    → _do_get_next_block → on_after_read_block
///
/// Also provides shared default implementations for filling partition/missing
/// columns.
class GenericReader : public ProfileCollector {
public:
    GenericReader() : _push_down_agg_type(TPushAggOp::type::NONE) {}
    void set_push_down_agg_type(TPushAggOp::type push_down_agg_type) {
        if (!_push_down_agg_type_locked) {
            _push_down_agg_type = push_down_agg_type;
        }
    }
    // Lock the current push_down_agg_type so FileScanner cannot override it.
    // Used by readers that must disable COUNT pushdown (e.g., ACID deletes, Paimon DV).
    void lock_push_down_agg_type() { _push_down_agg_type_locked = true; }
    TPushAggOp::type get_push_down_agg_type() const { return _push_down_agg_type; }

    virtual Status get_next_block(Block* block, size_t* read_rows, bool* eof) = 0;

    // Type is always nullable to process illegal values.
    // Results are cached after the first successful call.
    Status get_columns(std::unordered_map<std::string, DataTypePtr>* name_to_type) {
        if (_get_columns_cached) {
            *name_to_type = _cached_name_to_type;
            return Status::OK();
        }
        RETURN_IF_ERROR(_get_columns_impl(name_to_type));
        _cached_name_to_type = *name_to_type;
        _get_columns_cached = true;

        return Status::OK();
    }

    virtual Status _get_columns_impl(std::unordered_map<std::string, DataTypePtr>* name_to_type) {
        return Status::NotSupported("get_columns is not implemented");
    }

    // This method is responsible for initializing the resource for parsing schema.
    // It will be called before `get_parsed_schema`.
    virtual Status init_schema_reader() {
        return Status::NotSupported("init_schema_reader is not implemented for this reader.");
    }
    // `col_types` is always nullable to process illegal values.
    virtual Status get_parsed_schema(std::vector<std::string>* col_names,
                                     std::vector<DataTypePtr>* col_types) {
        return Status::NotSupported("get_parsed_schema is not implemented for this reader.");
    }
    ~GenericReader() override = default;

    virtual Status close() { return Status::OK(); }

    Status read_by_rows(const std::list<int64_t>& row_ids) {
        _read_by_rows = true;
        _row_ids = row_ids;
        return _set_read_one_line_impl();
    }

    /// The reader is responsible for counting the number of rows read,
    /// because some readers, such as parquet/orc,
    /// can skip some pages/rowgroups through indexes.
    virtual bool count_read_rows() { return false; }

    /// Returns true if on_before_init_reader has already set _column_descs.
    bool has_column_descs() const { return _column_descs != nullptr; }

    /// Hook called before core init. Sets up _column_descs, _fill_col_name_to_block_idx,
    /// partition values, and builds column_names for file reading.
    /// Subclasses override to customize schema mapping.
    /// Also called by FileScanner for simple readers that don't have their own init_reader.
    virtual Status on_before_init_reader(
            std::vector<ColumnDescriptor>& column_descs, std::vector<std::string>& column_names,
            std::shared_ptr<TableSchemaChangeHelper::Node>& table_info_node,
            std::set<uint64_t>& column_ids, std::set<uint64_t>& filter_column_ids,
            const TFileScanRangeParams& params, const TFileRangeDesc& range,
            const TupleDescriptor* tuple_descriptor, const RowDescriptor* row_descriptor,
            RuntimeState* state, std::unordered_map<std::string, uint32_t>* col_name_to_block_idx);

    /// Get missing columns computed by get_columns().
    const std::unordered_set<std::string>& missing_cols() const { return _fill_missing_cols; }

    void set_fill_column_data(
            const std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>&
                    partition_values,
            const std::unordered_map<std::string, VExprContextSPtr>& missing_defaults,
            std::unordered_map<std::string, uint32_t>* col_name_to_block_idx) {
        _fill_partition_values = partition_values;
        _fill_missing_defaults = missing_defaults;
        _fill_col_name_to_block_idx = col_name_to_block_idx;
    }

    // ---- Fill-column hooks (called by RowGroupReader and ORC get_next_block) ----

    /// Fill partition columns from metadata values.
    /// Default implementation: deserializes partition value for each named column.
    virtual Status on_fill_partition_columns(Block* block, size_t rows,
                                             const std::vector<std::string>& cols) {
        DataTypeSerDe::FormatOptions text_format_options;
        for (const auto& col_name : cols) {
            auto it = _fill_partition_values.find(col_name);
            if (it == _fill_partition_values.end()) {
                continue;
            }
            auto col_ptr = block->get_by_position((*_fill_col_name_to_block_idx)[col_name])
                                   .column->assume_mutable();
            const auto& [value, slot_desc] = it->second;
            auto text_serde = slot_desc->get_data_type_ptr()->get_serde();
            Slice slice(value.data(), value.size());
            uint64_t num_deserialized = 0;
            if (text_serde->deserialize_column_from_fixed_json(
                        *col_ptr, slice, rows, &num_deserialized, text_format_options) !=
                Status::OK()) {
                return Status::InternalError("Failed to fill partition column: {}={}",
                                             slot_desc->col_name(), value);
            }
            if (num_deserialized != rows) {
                return Status::InternalError(
                        "Failed to fill partition column: {}={}. "
                        "Expected rows: {}, actual: {}",
                        slot_desc->col_name(), value, num_deserialized, rows);
            }
        }
        return Status::OK();
    }

    /// Fill missing columns with default values or null.
    /// Default implementation: fills null or evaluates default expression.
    virtual Status on_fill_missing_columns(Block* block, size_t rows,
                                           const std::vector<std::string>& cols) {
        for (const auto& col_name : cols) {
            if (!_fill_col_name_to_block_idx->contains(col_name)) {
                return Status::InternalError("Missing column: {} not found in block {}", col_name,
                                             block->dump_structure());
            }
            auto it = _fill_missing_defaults.find(col_name);
            VExprContextSPtr ctx = (it != _fill_missing_defaults.end()) ? it->second : nullptr;

            if (ctx == nullptr) {
                auto mutable_column =
                        block->get_by_position((*_fill_col_name_to_block_idx)[col_name])
                                .column->assume_mutable();
                auto* nullable_column = static_cast<ColumnNullable*>(mutable_column.get());
                nullable_column->insert_many_defaults(rows);
            } else {
                ColumnPtr result_column_ptr;
                RETURN_IF_ERROR(ctx->execute(block, result_column_ptr));
                if (result_column_ptr->use_count() == 1) {
                    auto mutable_column = result_column_ptr->assume_mutable();
                    mutable_column->resize(rows);
                    result_column_ptr = result_column_ptr->convert_to_full_column_if_const();
                    auto origin_column_type =
                            block->get_by_position((*_fill_col_name_to_block_idx)[col_name]).type;
                    bool is_nullable = origin_column_type->is_nullable();
                    block->replace_by_position(
                            (*_fill_col_name_to_block_idx)[col_name],
                            is_nullable ? make_nullable(result_column_ptr) : result_column_ptr);
                }
            }
        }
        return Status::OK();
    }

    // ---- Synthesized column handler registry ----

    /// Handler type: fills a single synthesized column into the block.
    using SynthesizedColumnHandler = std::function<Status(Block* block, size_t rows)>;

    /// Register a handler for a synthesized column. Called during init
    /// (e.g. on_after_init_reader for Iceberg $row_id, _do_init_reader for TopN row_id).
    void register_synthesized_column_handler(const std::string& col_name,
                                             SynthesizedColumnHandler handler) {
        _synthesized_col_handlers.emplace_back(col_name, std::move(handler));
    }

    /// Dispatch all registered synthesized column handlers.
    /// Replaces the previous virtual on_fill_synthesized_columns hook.
    Status fill_synthesized_columns(Block* block, size_t rows) {
        for (auto& [name, handler] : _synthesized_col_handlers) {
            RETURN_IF_ERROR(handler(block, rows));
        }
        return Status::OK();
    }

    /// Unified fill for partition + missing + synthesized columns.
    /// Called by simple readers (CSV, JSON, Text, etc.) at end of get_next_block.
    /// Parquet/ORC fill internally via RowGroupReader / get_next_block hooks.
    Status fill_remaining_columns(Block* block, size_t rows) {
        std::vector<std::string> part_col_names;
        for (auto& kv : _fill_partition_values) {
            part_col_names.push_back(kv.first);
        }
        RETURN_IF_ERROR(on_fill_partition_columns(block, rows, part_col_names));
        std::vector<std::string> miss_col_names;
        for (auto& kv : _fill_missing_defaults) {
            miss_col_names.push_back(kv.first);
        }
        RETURN_IF_ERROR(on_fill_missing_columns(block, rows, miss_col_names));
        RETURN_IF_ERROR(fill_synthesized_columns(block, rows));
        return Status::OK();
    }

    /// Check if any synthesized column handlers are registered.
    bool has_synthesized_column_handlers() const { return !_synthesized_col_handlers.empty(); }

    /// Fill generated columns (may have file values, null rows are backfilled).
    /// Default is no-op.
    virtual Status on_fill_generated_columns(Block* block, size_t rows,
                                             const std::vector<std::string>& cols) {
        return Status::OK();
    }

protected:
    // ---- Init-time hooks ----

    /// Called after core init completes. Subclasses override to process
    /// delete files, deletion vectors, etc.
    virtual Status on_after_init_reader(const TFileScanRangeParams& params,
                                        const TFileRangeDesc& range,
                                        const TupleDescriptor* tuple_descriptor,
                                        const RowDescriptor* row_descriptor, RuntimeState* state) {
        return Status::OK();
    }

    // ---- Read-time hooks ----

    /// Called before reading a block. Subclasses override to modify block
    /// structure (e.g. add ACID columns, expand for equality delete).
    virtual Status on_before_read_block(Block* block) { return Status::OK(); }

    /// Called after reading a block. Subclasses override to post-process
    /// (e.g. remove ACID columns, apply equality delete filter).
    virtual Status on_after_read_block(Block* block, size_t* read_rows) { return Status::OK(); }

    virtual Status _set_read_one_line_impl() {
        return Status::NotSupported("read_by_rows is not implemented for this reader.");
    }

    /// Phase 1: Prepare partition/missing column data from range/params.
    /// Stores results in _prepared_partition_col_descs and _prepared_missing_col_descs.
    /// Call early (before on_before_init_reader) so hooks can access this data.
    /// Prerequisites: file is opened (get_columns() callable).
    Status _init_common_reader_states(
            std::vector<ColumnDescriptor>& column_descs, std::vector<std::string>& column_names,
            const TFileScanRangeParams& params, const TFileRangeDesc& range,
            const TupleDescriptor* tuple_descriptor, const RowDescriptor* row_descriptor,
            RuntimeState* state, std::unordered_map<std::string, uint32_t>* col_name_to_block_idx);

    Status _prepare_fill_columns(const std::vector<ColumnDescriptor>& column_descs,
                                 const TFileScanRangeParams& params, const TFileRangeDesc& range,
                                 const TupleDescriptor* tuple_descriptor,
                                 const RowDescriptor* row_descriptor, RuntimeState* state);

    const size_t _MIN_BATCH_SIZE = 4064; // 4094 - 32(padding)

    TPushAggOp::type _push_down_agg_type {};
    bool _push_down_agg_type_locked = false;

public:
    // Pass condition cache context to the reader for HIT/MISS tracking.
    virtual void set_condition_cache_context(std::shared_ptr<ConditionCacheContext> ctx) {}

    // Returns the total number of rows the reader will produce.
    // Used to pre-allocate condition cache with the correct number of granules.
    virtual int64_t get_total_rows() const { return 0; }

    // Returns true if this reader has delete operations (e.g. Iceberg position/equality deletes,
    // Hive ACID deletes). Used to disable condition cache when deletes are present, since cached
    // granule results may become stale if delete files change between queries.
    virtual bool has_delete_operations() const { return false; }

protected:
    bool _read_by_rows = false;
    std::list<int64_t> _row_ids;

    // Cache to save some common part such as file footer.
    // Maybe null if not used
    FileMetaCache* _meta_cache = nullptr;

    // ---- Column descriptors (set by init_reader, owned by FileScanner) ----
    const std::vector<ColumnDescriptor>* _column_descs = nullptr;

    // ---- Fill column data (set by _prepare_fill_columns / set_fill_column_data) ----
    std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>
            _fill_partition_values;
    std::unordered_map<std::string, VExprContextSPtr> _fill_missing_defaults;

    std::unordered_map<std::string, uint32_t>* _fill_col_name_to_block_idx = nullptr;
    std::unordered_set<std::string> _fill_missing_cols;

    // ---- Synthesized column handlers ----
    std::vector<std::pair<std::string, SynthesizedColumnHandler>> _synthesized_col_handlers;

    // ---- get_columns cache ----
    bool _get_columns_cached = false;
    std::unordered_map<std::string, DataTypePtr> _cached_name_to_type;
};

/// Provides an accessor for the current batch's row positions within the file.
/// Implemented by RowGroupReader (Parquet) and OrcReader.
class RowPositionProvider {
public:
    virtual ~RowPositionProvider() = default;
    virtual const std::vector<segment_v2::rowid_t>& current_batch_row_positions() const = 0;
};

#include "common/compile_check_end.h"
} // namespace doris
