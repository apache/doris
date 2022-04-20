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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/exec/select-node.h
// and modified by Doris

#ifndef DORIS_BE_SRC_QUERY_EXEC_SELECT_NODE_H
#define DORIS_BE_SRC_QUERY_EXEC_SELECT_NODE_H

#include "exec/exec_node.h"
#include "runtime/mem_pool.h"

namespace doris {

class Tuple;
class TupleRow;

// Node that evaluates conjuncts and enforces a limit but otherwise passes along
// the rows pulled from its child unchanged.
class SelectNode : public ExecNode {
public:
    SelectNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

    virtual Status prepare(RuntimeState* state);
    virtual Status open(RuntimeState* state);
    virtual Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos);
    virtual Status close(RuntimeState* state);

private:
    // current row batch of child
    std::unique_ptr<RowBatch> _child_row_batch;

    // index of current row in _child_row_batch
    int _child_row_idx;

    // true if last get_next() call on child signalled eos
    bool _child_eos;

    // Copy rows from _child_row_batch for which _conjuncts evaluate to true to
    // output_batch, up to _limit.
    // Return true if limit was hit or output_batch should be returned, otherwise false.
    bool copy_rows(RowBatch* output_batch);
};

} // namespace doris

#endif
