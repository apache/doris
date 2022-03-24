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

#ifndef DORIS_BE_SRC_QUERY_EXEC_TOPN_NODE_H
#define DORIS_BE_SRC_QUERY_EXEC_TOPN_NODE_H

#include <queue>

#include "exec/exec_node.h"
#include "runtime/descriptors.h"
#include "util/sort_heap.h"
#include "util/tuple_row_compare.h"

namespace doris {

class MemPool;
class RuntimeState;
class Tuple;

// Node for in-memory TopN (ORDER BY ... LIMIT)
// This handles the case where the result fits in memory.  This node will do a deep
// copy of the tuples that are necessary for the output.
// This is implemented by storing rows in a priority queue.
class TopNNode : public ExecNode {
public:
    TopNNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    virtual ~TopNNode();

    virtual Status init(const TPlanNode& tnode, RuntimeState* state = nullptr);

    virtual Status prepare(RuntimeState* state);
    virtual Status open(RuntimeState* state);
    virtual Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos);
    virtual Status close(RuntimeState* state);
    virtual void push_down_predicate(RuntimeState* state, std::list<ExprContext*>* expr_ctxs);

protected:
    virtual void debug_string(int indentation_level, std::stringstream* out) const;

private:
    friend class TupleLessThan;

    // Inserts a tuple row into the priority queue if it's in the TopN.  Creates a deep
    // copy of tuple_row, which it stores in _tuple_pool.
    void insert_tuple_row(TupleRow* tuple_row);

    // Flatten and reverse the priority queue.
    void prepare_for_output();

    // number rows to skipped
    int64_t _offset;

    // _sort_exec_exprs contains the ordering expressions used for tuple comparison and
    // the materialization exprs for the output tuple.
    SortExecExprs _sort_exec_exprs;
    std::vector<bool> _is_asc_order;
    std::vector<bool> _nulls_first;

    // Cached descriptor for the materialized tuple. Assigned in Prepare().
    TupleDescriptor* _materialized_tuple_desc;

    // Comparator for _priority_queue.
    std::unique_ptr<TupleRowComparator> _tuple_row_less_than;

    // After computing the TopN in the priority_queue, pop them and put them in this vector
    std::vector<Tuple*> _sorted_top_n;

    // Tuple allocated once from _tuple_pool and reused in InsertTupleRow to
    // materialize input tuples if necessary. After materialization, _tmp_tuple may be
    // copied into the tuple pool and inserted into the priority queue.
    Tuple* _tmp_tuple;

    // Stores everything referenced in _priority_queue
    std::unique_ptr<MemPool> _tuple_pool;

    // Iterator over elements in _sorted_top_n.
    std::vector<Tuple*>::iterator _get_next_iter;
    // std::vector<TupleRow*>::iterator _get_next_iter;

    // True if the _limit comes from DEFAULT_ORDER_BY_LIMIT and the query option
    // ABORT_ON_DEFAULT_LIMIT_EXCEEDED is set.
    bool _abort_on_default_limit_exceeded;

    /////////////////////////////////////////
    // BEGIN: Members that must be Reset()

    // Number of rows skipped. Used for adhering to _offset.
    int64_t _num_rows_skipped;

    // The priority queue will never have more elements in it than the LIMIT.
    std::unique_ptr<SortingHeap<Tuple*, std::vector<Tuple*>, TupleRowComparator>> _priority_queue;

    // END: Members that must be Reset()
    /////////////////////////////////////////
};

}; // namespace doris

#endif
