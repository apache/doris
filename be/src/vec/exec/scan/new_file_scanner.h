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

#include "exec/text_converter.h"
#include "exprs/bloomfilter_predicate.h"
#include "exprs/function_filter.h"
#include "runtime/tuple.h"
#include "vec/exec/scan/vscanner.h"

namespace doris::vectorized {

class NewFileScanNode;

class NewFileScanner : public VScanner {
public:
    NewFileScanner(RuntimeState* state, NewFileScanNode* parent, int64_t limit,
                   const TFileScanRange& scan_range, MemTracker* tracker, RuntimeProfile* profile,
                   const std::vector<TExpr>& pre_filter_texprs);

    Status open(RuntimeState* state) override;

    Status prepare(VExprContext** vconjunct_ctx_ptr);

protected:
    // Use prefilters to filter input block
    Status _filter_input_block(Block* block);
    Status _materialize_dest_block(vectorized::Block* output_block);

protected:
    virtual void _init_profiles(RuntimeProfile* profile) = 0;

    Status _fill_columns_from_path(vectorized::Block* output_block, size_t rows);
    Status init_block(vectorized::Block* block);

    std::unique_ptr<TextConverter> _text_converter;

    const TFileScanRangeParams& _params;

    const std::vector<TFileRangeDesc>& _ranges;
    int _next_range;

    // Used for constructing tuple
    std::vector<SlotDescriptor*> _required_slot_descs;
    // File source slot descriptors
    std::vector<SlotDescriptor*> _file_slot_descs;
    // File slot id to index map.
    std::map<SlotId, int> _file_slot_index_map;
    // Partition source slot descriptors
    std::vector<SlotDescriptor*> _partition_slot_descs;
    // Partition slot id to index map
    std::map<SlotId, int> _partition_slot_index_map;
    std::unique_ptr<RowDescriptor> _row_desc;
    doris::Tuple* _src_tuple;
    TupleRow* _src_tuple_row;

    // Mem pool used to allocate _src_tuple and _src_tuple_row
    std::unique_ptr<MemPool> _mem_pool;

    // Dest tuple descriptor and dest expr context
    const TupleDescriptor* _dest_tuple_desc;

    // Profile
    RuntimeProfile* _profile;
    RuntimeProfile::Counter* _rows_read_counter;
    RuntimeProfile::Counter* _read_timer;

    bool _scanner_eof = false;
    int _rows = 0;
    int _num_of_columns_from_file;

    const std::vector<TExpr> _pre_filter_texprs;

    std::vector<vectorized::VExprContext*> _dest_vexpr_ctx;
    // to filter src tuple directly.
    std::unique_ptr<vectorized::VExprContext*> _vpre_filter_ctx_ptr;

    // the map values of dest slot id to src slot desc
    // if there is not key of dest slot id in dest_sid_to_src_sid_without_trans, it will be set to nullptr
    std::vector<SlotDescriptor*> _src_slot_descs_order_by_dest;

    bool _src_block_mem_reuse = false;
    bool _strict_mode;

private:
    Status _init_expr_ctxes();
};
} // namespace doris::vectorized
