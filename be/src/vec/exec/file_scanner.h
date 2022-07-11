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

#include "common/status.h"
#include "exec/base_scanner.h"
#include "exec/text_converter.h"
#include "exprs/expr.h"
#include "util/runtime_profile.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"

namespace doris::vectorized {
class FileScanner {
public:
    FileScanner(RuntimeState* state, RuntimeProfile* profile, const TFileScanRangeParams& params,
                const std::vector<TFileRangeDesc>& ranges,
                const std::vector<TExpr>& pre_filter_texprs, ScannerCounter* counter);

    virtual ~FileScanner() = default;

    virtual void reg_conjunct_ctxs(const TupleId& tupleId,
                                   const std::vector<ExprContext*>& conjunct_ctxs);

    // Open this scanner, will initialize information need to
    virtual Status open();

    // Get next block
    virtual Status get_next(vectorized::Block* block, bool* eof) {
        return Status::NotSupported("Not Implemented get block");
    }

    // Close this scanner
    virtual void close() = 0;

protected:
    virtual void _init_profiles(RuntimeProfile* profile) = 0;

    Status finalize_block(vectorized::Block* dest_block, bool* eof);
    Status init_block(vectorized::Block* block);

    std::unique_ptr<TextConverter> _text_converter;

    RuntimeState* _state;
    const TFileScanRangeParams& _params;

    const std::vector<TFileRangeDesc>& _ranges;
    int _next_range;
    // used for process stat
    ScannerCounter* _counter;

    // Used for constructing tuple
    std::vector<SlotDescriptor*> _required_slot_descs;
    std::vector<SlotDescriptor*> _file_slot_descs;
    std::map<SlotId, int> _file_slot_index_map;
    std::vector<SlotDescriptor*> _partition_slot_descs;
    std::map<SlotId, int> _partition_slot_index_map;

    std::unique_ptr<RowDescriptor> _row_desc;

    std::shared_ptr<MemTracker> _mem_tracker;
    // Mem pool used to allocate _src_tuple and _src_tuple_row
    std::unique_ptr<MemPool> _mem_pool;

    const std::vector<TExpr> _pre_filter_texprs;

    // Profile
    RuntimeProfile* _profile;
    RuntimeProfile::Counter* _rows_read_counter;
    RuntimeProfile::Counter* _read_timer;

    bool _scanner_eof = false;
    int _rows = 0;
    long _read_row_counter = 0;

    std::unique_ptr<vectorized::VExprContext*> _vpre_filter_ctx_ptr;
    int _num_of_columns_from_file;

    // File formats based push down predicate
    std::vector<ExprContext*> _conjunct_ctxs;
    // slot_ids for parquet predicate push down are in tuple desc
    TupleId _tupleId;

private:
    Status _init_expr_ctxes();
    Status _filter_block(vectorized::Block* output_block);
    Status _fill_columns_from_path(vectorized::Block* output_block);
};

} // namespace doris::vectorized
