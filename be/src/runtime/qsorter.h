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

#ifndef DORIS_BE_RUNTIME_QSORTER_H
#define DORIS_BE_RUNTIME_QSORTER_H

#include <vector>

#include "common/status.h"
#include "runtime/sorter.h"

namespace doris {

class ExprContext;
class RowBatch;
class RowDescriptor;
class RuntimeState;
class TupleRow;
class MemPool;

// This sorter use memory heap to sort data added.
// So when data is too large, 'add_batch' will return failure
class QSorter : public Sorter {
public:
    QSorter(const RowDescriptor& row_desc, const std::vector<ExprContext*>& order_expr_ctxs,
            RuntimeState* state);

    virtual ~QSorter() {}

    virtual Status prepare(RuntimeState* state);

    // Add data to be sorted.
    virtual Status add_batch(RowBatch* batch);

    // call when all data be added
    virtual Status input_done();

    // fetch data already sorted,
    // client must insure that call this function AFTER call input_done
    virtual Status get_next(RowBatch* batch, bool* eos);

    virtual Status close(RuntimeState* state);
    // hll merge will create
    MemPool* get_mem_pool() { return _tuple_pool.get(); };

private:
    Status insert_tuple_row(TupleRow* input_row);

    const RowDescriptor& _row_desc;
    const std::vector<ExprContext*>& _order_expr_ctxs;
    std::vector<ExprContext*> _lhs_expr_ctxs;
    std::vector<ExprContext*> _rhs_expr_ctxs;

    // After computing the TopN in the priority_queue, pop them and put them in this vector
    std::vector<TupleRow*> _sorted_rows;
    std::vector<TupleRow*>::iterator _next_iter;

    // Stores everything referenced in _priority_queue
    std::unique_ptr<MemPool> _tuple_pool;
};

} // namespace doris

#endif
