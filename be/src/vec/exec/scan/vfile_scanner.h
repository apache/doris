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

#include "exec/olap_common.h"
#include "exec/text_converter.h"
#include "exprs/bloomfilter_predicate.h"
#include "exprs/function_filter.h"
#include "io/file_factory.h"
#include "runtime/tuple.h"
#include "vec/exec/format/generic_reader.h"
#include "vec/exec/scan/vscanner.h"

namespace doris::vectorized {

class NewFileScanNode;

class VFileScanner : public VScanner {
public:
    VFileScanner(RuntimeState* state, NewFileScanNode* parent, int64_t limit,
                 const TFileScanRange& scan_range, RuntimeProfile* profile);

    Status open(RuntimeState* state) override;

    Status close(RuntimeState* state) override;

public:
    Status prepare(VExprContext** vconjunct_ctx_ptr,
                   std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range);

protected:
    Status _get_block_impl(RuntimeState* state, Block* block, bool* eof) override;

    Status _get_next_reader();

    // TODO: cast input block columns type to string.
    Status _cast_src_block(Block* block) { return Status::OK(); };

protected:
    std::unique_ptr<TextConverter> _text_converter;
    const TFileScanRangeParams& _params;
    const std::vector<TFileRangeDesc>& _ranges;
    int _next_range;

    std::unique_ptr<GenericReader> _cur_reader;
    bool _cur_reader_eof;
    std::unordered_map<std::string, ColumnValueRangeType>* _colname_to_value_range;
    // File source slot descriptors
    std::vector<SlotDescriptor*> _file_slot_descs;
    // File slot id to index in _file_slot_descs
    std::unordered_map<SlotId, int> _file_slot_index_map;
    // file col name to index in _file_slot_descs
    std::map<std::string, int> _file_slot_name_map;
    // col names from _file_slot_descs
    std::vector<std::string> _file_col_names;
    // Partition source slot descriptors
    std::vector<SlotDescriptor*> _partition_slot_descs;
    // Partition slot id to index in _partition_slot_descs
    std::unordered_map<SlotId, int> _partition_slot_index_map;
    // created from param.expr_of_dest_slot
    // For query, it saves default value expr of all dest columns, or nullptr for NULL.
    // For load, it saves convertion expr/default value of all dest columns.
    std::vector<vectorized::VExprContext*> _dest_vexpr_ctx;
    // dest slot name to index in _dest_vexpr_ctx;
    std::unordered_map<std::string, int> _dest_slot_name_to_idx;
    // col name to default value expr
    std::unordered_map<std::string, vectorized::VExprContext*> _col_default_value_ctx;
    // the map values of dest slot id to src slot desc
    // if there is not key of dest slot id in dest_sid_to_src_sid_without_trans, it will be set to nullptr
    std::vector<SlotDescriptor*> _src_slot_descs_order_by_dest;
    // dest slot desc index to src slot desc index
    std::unordered_map<int, int> _dest_slot_to_src_slot_index;

    std::unordered_map<std::string, size_t> _src_block_name_to_idx;

    // Get from GenericReader, save the existing columns in file to their type.
    std::unordered_map<std::string, TypeDescriptor> _name_to_col_type;
    // Get from GenericReader, save columns that requried by scan but not exist in file.
    // These columns will be filled by default value or null.
    std::unordered_set<std::string> _missing_cols;

    // For load task
    std::unique_ptr<doris::vectorized::VExprContext*> _pre_conjunct_ctx_ptr;
    std::unique_ptr<RowDescriptor> _src_row_desc;
    // row desc for default exprs
    std::unique_ptr<RowDescriptor> _default_val_row_desc;

    // Mem pool used to allocate _src_tuple and _src_tuple_row
    std::unique_ptr<MemPool> _mem_pool;

    // Profile
    RuntimeProfile* _profile;

    bool _scanner_eof = false;
    int _rows = 0;
    int _num_of_columns_from_file;

    bool _src_block_mem_reuse = false;
    bool _strict_mode;

    bool _src_block_init = false;
    Block* _src_block_ptr;
    Block _src_block;

private:
    RuntimeProfile::Counter* _get_block_timer = nullptr;
    RuntimeProfile::Counter* _cast_to_input_block_timer = nullptr;
    RuntimeProfile::Counter* _fill_path_columns_timer = nullptr;
    RuntimeProfile::Counter* _fill_missing_columns_timer = nullptr;
    RuntimeProfile::Counter* _pre_filter_timer = nullptr;
    RuntimeProfile::Counter* _convert_to_output_block_timer = nullptr;

private:
    Status _init_expr_ctxes();
    Status _init_src_block(Block* block);
    Status _cast_to_input_block(Block* block);
    Status _fill_columns_from_path(size_t rows);
    Status _fill_missing_columns(size_t rows);
    Status _pre_filter_src_block();
    Status _convert_to_output_block(Block* block);

    void _reset_counter() {
        _counter.num_rows_unselected = 0;
        _counter.num_rows_filtered = 0;
    }
};
} // namespace doris::vectorized
