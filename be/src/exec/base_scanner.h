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
#include "exprs/expr.h"
#include "runtime/tuple.h"
#include "util/runtime_profile.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"

namespace doris {

class Tuple;
class TupleDescriptor;
class TupleRow;
class RowDescriptor;
class RuntimeState;
class ExprContext;

namespace vectorized {
class VExprContext;
class IColumn;
using MutableColumnPtr = IColumn::MutablePtr;
} // namespace vectorized

// The counter will be passed to each scanner.
// Note that this struct is not thread safe.
// So if we support concurrent scan in the future, we need to modify this struct.
struct ScannerCounter {
    ScannerCounter() : num_rows_filtered(0), num_rows_unselected(0) {}

    int64_t num_rows_filtered;   // unqualified rows (unmatched the dest schema, or no partition)
    int64_t num_rows_unselected; // rows filtered by predicates
};

class BaseScanner {
public:
    BaseScanner(RuntimeState* state, RuntimeProfile* profile, const TBrokerScanRangeParams& params,
                const std::vector<TBrokerRangeDesc>& ranges,
                const std::vector<TNetworkAddress>& broker_addresses,
                const std::vector<TExpr>& pre_filter_texprs, ScannerCounter* counter);

    virtual ~BaseScanner() {
        Expr::close(_dest_expr_ctx, _state);
        if (_state->enable_vectorized_exec()) {
            vectorized::VExpr::close(_dest_vexpr_ctx, _state);
        }
    }

    // Register conjuncts for push down
    virtual void reg_conjunct_ctxs(const TupleId& tupleId,
                                   const std::vector<ExprContext*>& conjunct_ctxs);

    virtual Status init_expr_ctxes();
    // Open this scanner, will initialize information need to
    virtual Status open();

    // Get next tuple
    virtual Status get_next(Tuple* tuple, MemPool* tuple_pool, bool* eof, bool* fill_tuple) = 0;

    // Get next block
    virtual Status get_next(vectorized::Block* block, bool* eof) {
        return Status::NotSupported("Not Implemented get block");
    }

    // Close this scanner
    virtual void close() = 0;
    Status fill_dest_tuple(Tuple* dest_tuple, MemPool* mem_pool, bool* fill_tuple);

    void fill_slots_of_columns_from_path(int start,
                                         const std::vector<std::string>& columns_from_path);

    void free_expr_local_allocations();

protected:
    Status _fill_dest_block(vectorized::Block* dest_block, bool* eof);
    virtual Status _init_src_block();

    bool is_null(const Slice& slice);
    bool is_array(const Slice& slice);
    bool check_array_format(std::vector<Slice>& split_values);

    RuntimeState* _state;
    const TBrokerScanRangeParams& _params;

    //const TBrokerScanRangeParams& _params;
    const std::vector<TBrokerRangeDesc>& _ranges;
    const std::vector<TNetworkAddress>& _broker_addresses;
    int _next_range;
    // used for process stat
    ScannerCounter* _counter;

    // Used for constructing tuple
    // slots for value read from broker file
    std::vector<SlotDescriptor*> _src_slot_descs;
    std::unique_ptr<RowDescriptor> _row_desc;
    Tuple* _src_tuple;
    TupleRow* _src_tuple_row;

    // Mem pool used to allocate _src_tuple and _src_tuple_row
    std::unique_ptr<MemPool> _mem_pool;

    // Dest tuple descriptor and dest expr context
    const TupleDescriptor* _dest_tuple_desc;
    std::vector<ExprContext*> _dest_expr_ctx;
    // the map values of dest slot id to src slot desc
    // if there is not key of dest slot id in dest_sid_to_src_sid_without_trans, it will be set to nullptr
    std::vector<SlotDescriptor*> _src_slot_descs_order_by_dest;

    // dest slot desc index to src slot desc index
    std::unordered_map<int, int> _dest_slot_to_src_slot_index;

    // to filter src tuple directly
    // the `_pre_filter_texprs` is the origin thrift exprs passed from scan node,
    // and will be converted to `_pre_filter_ctxs` when scanner is open.
    const std::vector<TExpr> _pre_filter_texprs;
    std::vector<ExprContext*> _pre_filter_ctxs;

    bool _strict_mode;

    int32_t _line_counter;
    // Profile
    RuntimeProfile* _profile;
    RuntimeProfile::Counter* _rows_read_counter;
    RuntimeProfile::Counter* _read_timer;
    RuntimeProfile::Counter* _materialize_timer;

    // Used to record whether a row of data is successfully read.
    bool _success = false;
    bool _scanner_eof = false;

    // for vectorized load
    std::vector<vectorized::VExprContext*> _dest_vexpr_ctx;
    std::unique_ptr<vectorized::VExprContext*> _vpre_filter_ctx_ptr;
    vectorized::Block _src_block;
    bool _src_block_mem_reuse = false;
    int _num_of_columns_from_file;

    // slot_ids for parquet predicate push down are in tuple desc
    TupleId _tupleId = -1;
    std::vector<ExprContext*> _conjunct_ctxs;

private:
    Status _filter_src_block();
    void _fill_columns_from_path();
    Status _materialize_dest_block(vectorized::Block* output_block);
    Status _fill_dest_tuple(Tuple* dest_tuple, MemPool* mem_pool);
};

} /* namespace doris */
