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

#ifndef DORIS_BE_SRC_QUERY_EXEC_INTERSECT_NODE_H
#define DORIS_BE_SRC_QUERY_EXEC_INTERSECT_NODE_H

#include "exec/exec_node.h"
#include "exec/hash_table.h"

namespace doris {

class MemPool;
class RowBatch;
class TupleRow;

// Node that calulate the intersect results of its children by either materializing their
// evaluated expressions into row batches or passing through (forwarding) the
// batches if the input tuple layout is identical to the output tuple layout
// and expressions don't need to be evaluated. The children should be ordered
// such that all passthrough children come before the children that need
// materialization. The interscet node pulls from its children sequentially, i.e.
// it exhausts one child completely before moving on to the next one.
class IntersectNode : public ExecNode {
public:
    IntersectNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

    virtual Status init(const TPlanNode& tnode, RuntimeState* state = nullptr);
    virtual Status prepare(RuntimeState* state);
    virtual Status open(RuntimeState* state);
    virtual Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos);
    virtual Status close(RuntimeState* state);

private:
    // Exprs materialized by this node. The i-th result expr list refers to the i-th child.
    std::vector<std::vector<ExprContext*>> _child_expr_lists;

    std::unique_ptr<HashTable> _hash_tbl;
    HashTable::Iterator _hash_tbl_iterator;
    std::unique_ptr<RowBatch> _probe_batch;
    // holds everything referenced in _hash_tbl
    std::unique_ptr<MemPool> _build_pool;

    std::vector<int> _build_tuple_idx;
    int _build_tuple_size;
    int _build_tuple_row_size;
    std::vector<bool> _find_nulls;

    RuntimeProfile::Counter* _build_timer;        // time to build hash table
    RuntimeProfile::Counter* _probe_timer;        // time to probe
};

}; // namespace doris

#endif // DORIS_BE_SRC_QUERY_EXEC_INTERSECT_NODE_H