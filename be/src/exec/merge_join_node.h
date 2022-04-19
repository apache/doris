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

#ifndef DORIS_BE_SRC_QUERY_EXEC_MERGE_JOIN_NODE_H
#define DORIS_BE_SRC_QUERY_EXEC_MERGE_JOIN_NODE_H

#include <string>
#include <thread>
#include <unordered_set>

#include "exec/exec_node.h"
#include "gen_cpp/PlanNodes_types.h" // for TJoinOp
#include "runtime/row_batch.h"

namespace doris {

class MemPool;
class TupleRow;

// Node for in-memory merge joins:
// find the minimal tuple and output
class MergeJoinNode : public ExecNode {
public:
    MergeJoinNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

    ~MergeJoinNode();

    virtual Status init(const TPlanNode& tnode, RuntimeState* state = nullptr);
    virtual Status prepare(RuntimeState* state);
    virtual Status open(RuntimeState* state);
    virtual Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos);
    virtual Status close(RuntimeState* state);

protected:
    void debug_string(int indentation_level, std::stringstream* out) const;

private:
    // our equi-join predicates "<lhs> = <rhs>" are separated into
    // _left_exprs (over child(0)) and _right_exprs (over child(1))
    // check which expr is min
    std::vector<ExprContext*> _left_expr_ctxs;
    std::vector<ExprContext*> _right_expr_ctxs;

    // non-equi-join conjuncts from the JOIN clause
    std::vector<ExprContext*> _other_join_conjunct_ctxs;

    bool _eos; // if true, nothing left to return in get_next()

    struct ChildReaderContext {
        RowBatch batch;
        int row_idx;
        bool is_eos;
        TupleRow* current_row;
        ChildReaderContext(const RowDescriptor& desc, int batch_size)
                : batch(desc, batch_size),
                  row_idx(0),
                  is_eos(false),
                  current_row(nullptr) {}
    };
    // _left_batch must be cleared before calling get_next().  used cache child(0)'s data
    // _right_batch must be cleared before calling get_next().  used cache child(1)'s data
    // does not initialize all tuple ptrs in the row, only the ones that it
    // is responsible for.
    std::unique_ptr<ChildReaderContext> _left_child_ctx;
    std::unique_ptr<ChildReaderContext> _right_child_ctx;
    // _build_tuple_idx[i] is the tuple index of child(1)'s tuple[i] in the output row
    std::vector<int> _right_tuple_idx;
    int _right_tuple_size;
    int _left_tuple_size;
    RowBatch* _out_batch;

    typedef int (*CompareFn)(const void*, const void*);
    std::vector<CompareFn> _cmp_func;

    // byte size of result tuple row (sum of the tuple ptrs, not the tuple data).
    // This should be the same size as the probe tuple row.
    int _result_tuple_row_size;

    void create_output_row(TupleRow* out, TupleRow* left, TupleRow* right);
    Status compare_row(TupleRow* left_row, TupleRow* right_row, bool* is_lt);
    Status get_next_row(RuntimeState* state, TupleRow* out_row, bool* eos);
    Status get_input_row(RuntimeState* state, int child_idx);
};

} // namespace doris

#endif
