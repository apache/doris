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

#include <functional>
#include <string>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/status.h"
#include "core/column/column.h"
#include "core/column/column_nullable.h"
#include "exprs/vexpr_fwd.h"
#include "format/generic_reader.h"

namespace doris {
class TFileRangeDesc;
class TupleDescriptor;
class SlotDescriptor;
} // namespace doris

namespace doris {

/// Intermediate base class for "table readers" used by FileScanner.
///
/// Owns all column-filling state and logic:
///   - partition column values (from path metadata)
///   - missing column defaults (columns not in file)
///   - synthesized column handlers (e.g. Iceberg $row_id)
///
/// Provides default on_after_read_block that auto-fills these columns.
/// Parquet/ORC override to no-op (they fill per-batch internally).
///
/// Also provides the default on_before_init_reader for simple readers
/// (CSV, JSON, etc.) that auto-computes partition/missing columns.
/// ORC/Parquet override on_before_init_reader with format-specific schema matching.
class TableFormatReader : public GenericReader {
public:
    /// Get missing columns computed by on_before_init_reader / get_columns().
    const std::unordered_set<std::string>& missing_cols() const { return _fill_missing_cols; }

    // ---- Fill-column hooks (called by RowGroupReader and ORC per-batch reading) ----

    /// Fill partition columns from metadata values.
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

    using SynthesizedColumnHandler = std::function<Status(Block* block, size_t rows)>;

    void register_synthesized_column_handler(const std::string& col_name,
                                             SynthesizedColumnHandler handler) {
        _synthesized_col_handlers.emplace_back(col_name, std::move(handler));
    }

    Status fill_synthesized_columns(Block* block, size_t rows) {
        for (auto& [name, handler] : _synthesized_col_handlers) {
            RETURN_IF_ERROR(handler(block, rows));
        }
        return Status::OK();
    }

    Status clear_synthesized_columns(Block* block) {
        for (auto& [name, handler] : _synthesized_col_handlers) {
            int col_pos = block->get_position_by_name(name);
            if (col_pos < 0) {
                continue;
            }
            block->get_by_position(static_cast<size_t>(col_pos)).column->assume_mutable()->clear();
        }
        return Status::OK();
    }

    using GeneratedColumnHandler = std::function<Status(Block* block, size_t rows)>;

    struct ColumnOptimizationTypes {
        using Type = int;

        static constexpr Type NONE = 0x00;
        static constexpr Type LAZY_READ = 0x01;
        static constexpr Type DICT_FILTER = 0x02;
        static constexpr Type MIN_MAX = 0x04;
        static constexpr Type DEFAULT = LAZY_READ | DICT_FILTER | MIN_MAX;
    };

    void set_column_optimizations(const std::string& col_name,
                                  ColumnOptimizationTypes::Type optimizations) {
        if (optimizations == ColumnOptimizationTypes::DEFAULT) {
            _column_optimizations.erase(col_name);
            return;
        }
        _column_optimizations[col_name] = optimizations;
    }

    ColumnOptimizationTypes::Type get_column_optimizations(const std::string& col_name) const {
        auto it = _column_optimizations.find(col_name);
        if (it != _column_optimizations.end()) {
            return it->second;
        }
        return ColumnOptimizationTypes::DEFAULT;
    }

    bool has_column_optimization(const std::string& col_name,
                                 ColumnOptimizationTypes::Type optimization) const {
        return (get_column_optimizations(col_name) & optimization) == optimization;
    }

    // Transitional helper kept for compatibility while readers migrate to bitmask checks.
    bool disable_column_opt(const std::string& col_name) const {
        return get_column_optimizations(col_name) == ColumnOptimizationTypes::NONE;
    }

    void register_generated_column_handler(const std::string& col_name,
                                           GeneratedColumnHandler handler) {
        _generated_col_handlers.emplace_back(col_name, std::move(handler));
        // Generated columns may depend on computed values that are not compatible with
        // lazy read, dictionary filtering, or min-max filtering.
        set_column_optimizations(col_name, ColumnOptimizationTypes::NONE);
    }

    Status fill_generated_columns(Block* block, size_t rows) {
        for (auto& [name, handler] : _generated_col_handlers) {
            RETURN_IF_ERROR(handler(block, rows));
        }
        return Status::OK();
    }

    Status clear_generated_columns(Block* block) {
        for (auto& [name, handler] : _generated_col_handlers) {
            int col_pos = block->get_position_by_name(name);
            if (col_pos < 0) {
                continue;
            }
            block->get_by_position(static_cast<size_t>(col_pos)).column->assume_mutable()->clear();
        }
        return Status::OK();
    }

    /// Unified fill for partition + missing + synthesized columns.
    /// Called automatically by on_after_read_block for simple readers.
    /// Parquet/ORC call individual on_fill_* methods per-batch internally.
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
        RETURN_IF_ERROR(fill_generated_columns(block, rows));
        return Status::OK();
    }

    bool has_synthesized_column_handlers() const { return !_synthesized_col_handlers.empty(); }

    bool has_generated_column_handlers() const { return !_generated_col_handlers.empty(); }

    /// Fill generated columns. Default is no-op.
    virtual Status on_fill_generated_columns(Block* block, size_t rows,
                                             const std::vector<std::string>& cols) {
        return Status::OK();
    }

    /// Default on_before_init_reader for simple readers (CSV, JSON, etc.).
    /// Auto-computes partition values, missing columns, and table_info_node.
    /// ORC/Parquet/Hive/Iceberg override with format-specific schema matching.
    Status on_before_init_reader(ReaderInitContext* ctx) override;

protected:
    /// Default on_after_read_block: auto-fill partition/missing/synthesized columns.
    /// Parquet/ORC override to no-op (they fill per-batch internally).
    Status on_after_read_block(Block* block, size_t* read_rows) override {
        if (*read_rows > 0 && _push_down_agg_type != TPushAggOp::type::COUNT) {
            RETURN_IF_ERROR(fill_remaining_columns(block, *read_rows));
        }
        return Status::OK();
    }

    /// Extracts partition key→value pairs from the file range.
    /// Static utility called by on_before_init_reader implementations.
    static Status _extract_partition_values(
            const TFileRangeDesc& range, const TupleDescriptor* tuple_descriptor,
            std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>&
                    partition_values);

    // ---- Fill column data (set by on_before_init_reader / _do_init_reader) ----
    std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>
            _fill_partition_values;
    std::unordered_map<std::string, VExprContextSPtr> _fill_missing_defaults;
    std::unordered_map<std::string, uint32_t>* _fill_col_name_to_block_idx = nullptr;
    std::unordered_set<std::string> _fill_missing_cols;

    // ---- Synthesized column handlers ----
    std::vector<std::pair<std::string, SynthesizedColumnHandler>> _synthesized_col_handlers;

    // ---- Generated column handlers ----
    std::vector<std::pair<std::string, GeneratedColumnHandler>> _generated_col_handlers;

    // ---- Column optimization flags ----
    std::unordered_map<std::string, ColumnOptimizationTypes::Type> _column_optimizations;
};

} // namespace doris
