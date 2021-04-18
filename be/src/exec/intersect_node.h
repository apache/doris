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

#include "exec/set_operation_node.h"

namespace doris {

class MemPool;
class RowBatch;
class TupleRow;

// Node that calculate the intersect results of its children by either materializing their
// evaluated expressions into row batches or passing through (forwarding) the
// batches if the input tuple layout is identical to the output tuple layout
// and expressions don't need to be evaluated. The children should be ordered
// such that all passthrough children come before the children that need
// materialization. The intersect node pulls from its children sequentially, i.e.
// it exhausts one child completely before moving on to the next one.
class IntersectNode : public SetOperationNode {
public:
    IntersectNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

    virtual Status init(const TPlanNode& tnode, RuntimeState* state = nullptr);
    virtual Status open(RuntimeState* state);
    virtual Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos);
};

}; // namespace doris
