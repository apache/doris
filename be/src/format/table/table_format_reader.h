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
#include "exprs/vexpr_context.h"
#include "format/column_processor.h"
#include "format/generic_reader.h"
#include "format/table/table_schema_change_helper.h"
#include "runtime/descriptors.h"
#include "storage/segment/common.h"

namespace doris {

using segment_v2::rowid_t;

class Block;

/// Provides an accessor for the current batch's row positions within the file.
/// Implemented by RowGroupReader (Parquet) and OrcReader.
class RowPositionProvider {
public:
    virtual ~RowPositionProvider() = default;
    virtual const std::vector<rowid_t>& current_batch_row_positions() const = 0;
};

/// Intermediate base class between GenericReader and file-format readers
/// (ParquetReader, OrcReader). Provides the hook virtual methods that
/// table-format subclasses (Iceberg, Hive ACID, Paimon, Hudi) override
/// via CRTP or direct inheritance.
///
/// These hooks implement the Template Method pattern:
///   init_reader:      on_before_init_columns  → _do_init_reader → on_after_init
///   get_next_block:   on_before_read_block    → _do_get_next_block → on_after_read_block
///
/// Also provides shared default implementations for filling partition/missing
/// columns, eliminating the separate ColumnProcessor hierarchy.
class TableFormatReader : public GenericReader {
public:
    /// Store partition/missing column data for use by on_fill_xxx_columns hooks.
    /// Called by OrcReader::set_fill_columns and ParquetReader::set_fill_columns.
    void set_fill_column_data(
            const std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>&
                    partition_values,
            const std::unordered_map<std::string, VExprContextSPtr>& missing_defaults,
            std::unordered_map<std::string, uint32_t>* col_name_to_block_idx) {
        _fill_partition_values = partition_values;
        _fill_missing_defaults = missing_defaults;
        _fill_col_name_to_block_idx = col_name_to_block_idx;
    }

protected:
    // ---- Init-time hooks ----

    /// Called before core init. Subclasses override to customize column selection,
    /// schema mapping (table_info_node), and column/filter IDs.
    /// Default: extracts REGULAR + INTERNAL column names from column_descs.
    virtual Status on_before_init_columns(
            const std::vector<ColumnDescriptor>& column_descs,
            std::vector<std::string>& column_names,
            std::shared_ptr<TableSchemaChangeHelper::Node>& table_info_node,
            std::set<uint64_t>& column_ids, std::set<uint64_t>& filter_column_ids) {
        for (const auto& desc : column_descs) {
            if (desc.category == ColumnCategory::REGULAR ||
                desc.category == ColumnCategory::INTERNAL) {
                column_names.push_back(desc.name);
            }
        }
        return Status::OK();
    }

    /// Called after core init completes. Subclasses override to process
    /// delete files, deletion vectors, etc.
    virtual Status on_after_init() { return Status::OK(); }

    // ---- Read-time hooks ----

    /// Called before reading a block. Subclasses override to modify block
    /// structure (e.g. add ACID columns, expand for equality delete).
    virtual Status on_before_read_block(Block* block) { return Status::OK(); }

    /// Called after reading a block. Subclasses override to post-process
    /// (e.g. remove ACID columns, apply equality delete filter).
    virtual Status on_after_read_block(Block* block, size_t* read_rows) { return Status::OK(); }

    // ---- Fill-column hooks (public: called by RowGroupReader and ORC get_next_block) ----
public:
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

    /// Fill synthesized columns (values fully computed at runtime,
    /// e.g. Iceberg $row_id). Default is no-op.
    virtual Status on_fill_synthesized_columns(Block* block, size_t rows,
                                               const std::vector<std::string>& cols) {
        return Status::OK();
    }

    /// Fill generated columns (may have file values, null rows are backfilled).
    /// Default is no-op.
    virtual Status on_fill_generated_columns(Block* block, size_t rows,
                                             const std::vector<std::string>& cols) {
        return Status::OK();
    }

    // ---- Fill column data (set by set_fill_columns) ----
    std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>
            _fill_partition_values;
    std::unordered_map<std::string, VExprContextSPtr> _fill_missing_defaults;
    std::unordered_map<std::string, uint32_t>* _fill_col_name_to_block_idx = nullptr;
};

} // namespace doris
