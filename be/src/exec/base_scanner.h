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

#ifndef BE_SRC_EXEC_BASE_SCANNER_H_
#define BE_SRC_EXEC_BASE_SCANNER_H_

#include "common/status.h"
#include "exprs/expr.h"
#include "runtime/tuple.h"
#include "util/runtime_profile.h"

namespace doris {

class Tuple;
class TupleDescriptor;
class TupleRow;
class RowDescriptor;
class MemTracker;
class RuntimeState;
class ExprContext;

struct ScannerCounter {
    ScannerCounter() : num_rows_filtered(0), num_rows_unselected(0) {}

    int64_t num_rows_filtered;   // unqualified rows (unmatched the dest schema, or no partition)
    int64_t num_rows_unselected; // rows filtered by predicates
};

class BaseScanner {
public:
    BaseScanner(RuntimeState* state, RuntimeProfile* profile, const TBrokerScanRangeParams& params,
                ScannerCounter* counter);
    virtual ~BaseScanner() { Expr::close(_dest_expr_ctx, _state); };

    virtual Status init_expr_ctxes();
    // Open this scanner, will initialize information need to
    virtual Status open();

    // Get next tuple
    virtual Status get_next(Tuple* tuple, MemPool* tuple_pool, bool* eof) = 0;

    // Close this scanner
    virtual void close() = 0;
    bool fill_dest_tuple(Tuple* dest_tuple, MemPool* mem_pool);

    void fill_slots_of_columns_from_path(int start,
                                         const std::vector<std::string>& columns_from_path);

protected:
    RuntimeState* _state;
    const TBrokerScanRangeParams& _params;
    // used for process stat
    ScannerCounter* _counter;

    // Used for constructing tuple
    // slots for value read from broker file
    std::vector<SlotDescriptor*> _src_slot_descs;
    std::unique_ptr<RowDescriptor> _row_desc;
    Tuple* _src_tuple;
    TupleRow* _src_tuple_row;

    std::shared_ptr<MemTracker> _mem_tracker;
    // Mem pool used to allocate _src_tuple and _src_tuple_row
    MemPool _mem_pool;

    // Dest tuple descriptor and dest expr context
    const TupleDescriptor* _dest_tuple_desc;
    std::vector<ExprContext*> _dest_expr_ctx;
    // the map values of dest slot id to src slot desc
    // if there is not key of dest slot id in dest_sid_to_src_sid_without_trans, it will be set to nullptr
    std::vector<SlotDescriptor*> _src_slot_descs_order_by_dest;

    bool _strict_mode;
    // Profile
    RuntimeProfile* _profile;
    RuntimeProfile::Counter* _rows_read_counter;
    RuntimeProfile::Counter* _read_timer;
    RuntimeProfile::Counter* _materialize_timer;
};

} /* namespace doris */

#endif /* BE_SRC_EXEC_BASE_SCANNER_H_ */
