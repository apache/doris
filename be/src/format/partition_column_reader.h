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
#include <tuple>
#include <unordered_map>
#include <vector>

#include "core/block/block.h"
#include "format/column_descriptor.h"
#include "format/generic_reader.h"
#include "format/table/partition_column_filler.h"

namespace doris {
#include "common/compile_check_begin.h"

// PartitionColumnReader is used for the "partition_column_value_only" optimization.
//
// When an aggregation (min/max) only depends on the partition columns of an external
// (Hive/Hudi) table, the value can be derived purely from partition metadata. Each scan range
// corresponds to one data file living under a partition directory, and its partition column
// values are already carried by `columns_from_path` in the TFileRangeDesc -- FileScanner turns
// them into `_partition_col_descs` in `_generate_partition_columns()`.
//
// This reader therefore does NOT open or read any data file. It emits exactly ONE row per scan
// range, filling every requested column with its own partition value. This preserves the exact
// "a partition without any data file produces no row" semantics, because a partition with no
// file never generates a scan range in the first place, while a partition that owns an (even
// empty) file still gets one scan range and thus contributes its partition value.
//
// NOTE: unlike the legacy FileScanner::_fill_columns_from_path(), partition/missing/synthesized
// columns are filled by the READER in the current architecture (see
// TableFormatReader::on_after_read_block). So this reader must fill the partition values itself;
// it cannot just report a row count.
class PartitionColumnReader : public GenericReader {
public:
    // All four arguments are owned by FileScanner and outlive this reader (it is created per scan
    // range inside FileScanner::_get_next_reader()). `_partition_col_descs` and
    // `_partition_value_is_null` are re-filled per range by _generate_partition_columns(), so they
    // are held by pointer and read lazily in _do_get_next_block().
    PartitionColumnReader(
            const std::vector<ColumnDescriptor>* column_descs,
            const std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>*
                    partition_col_descs,
            const std::unordered_map<std::string, bool>* partition_value_is_null,
            const std::unordered_map<std::string, uint32_t>* col_name_to_block_idx)
            : _column_descs(column_descs),
              _partition_col_descs(partition_col_descs),
              _partition_value_is_null(partition_value_is_null),
              _col_name_to_block_idx(col_name_to_block_idx) {}

    ~PartitionColumnReader() override = default;

protected:
    // No file is opened: everything this reader needs is already in the scan range.
    Status _open_file_reader(ReaderInitContext* /*ctx*/) override { return Status::OK(); }

    Status _do_init_reader(ReaderInitContext* /*ctx*/) override {
        _emitted = false;
        return Status::OK();
    }

    // Emit exactly one row (per scan range / data file) whose every column carries that range's
    // partition column value. FileScanner only installs this reader when ALL requested columns are
    // partition columns, so filling them is all that is needed to produce a complete row.
    Status _do_get_next_block(Block* block, size_t* read_rows, bool* eof) override {
        if (_emitted) {
            *read_rows = 0;
            *eof = true;
            return Status::OK();
        }
        _emitted = true;

        for (const ColumnDescriptor& col_desc : *_column_descs) {
            auto value_it = _partition_col_descs->find(col_desc.name);
            // FileScanner guards against this before installing the reader; stay defensive so an
            // unexpected shape surfaces as a clear error instead of a silently short column.
            if (value_it == _partition_col_descs->end()) {
                return Status::InternalError("Partition column {} has no value from path",
                                             col_desc.name);
            }
            auto idx_it = _col_name_to_block_idx->find(col_desc.name);
            if (idx_it == _col_name_to_block_idx->end()) {
                return Status::InternalError("Partition column {} not found in block",
                                             col_desc.name);
            }
            bool explicit_null_marker = false;
            auto null_it = _partition_value_is_null->find(col_desc.name);
            if (null_it != _partition_value_is_null->end()) {
                explicit_null_marker = null_it->second;
            }
            const auto& [value, slot_desc] = value_it->second;
            auto column_guard = block->mutate_column_scoped(idx_it->second);
            auto& col_ptr = column_guard.mutable_column();
            RETURN_IF_ERROR(fill_partition_column_from_path_value(*col_ptr, *slot_desc, value,
                                                                 1, explicit_null_marker));
        }

        *read_rows = 1;
        *eof = true;
        return Status::OK();
    }

    Status close() override { return Status::OK(); }

private:
    const std::vector<ColumnDescriptor>* _column_descs = nullptr;
    const std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>*
            _partition_col_descs = nullptr;
    const std::unordered_map<std::string, bool>* _partition_value_is_null = nullptr;
    const std::unordered_map<std::string, uint32_t>* _col_name_to_block_idx = nullptr;

    bool _emitted = false;
};

#include "common/compile_check_end.h"
} // namespace doris
