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

#ifndef DORIS_BE_SRC_QUERY_EXEC_OLAP_REWRITE_NODE_H
#define DORIS_BE_SRC_QUERY_EXEC_OLAP_REWRITE_NODE_H

#include <memory>

#include "exec/exec_node.h"
#include "runtime/mem_pool.h"

namespace doris {

class Tuple;
class TupleRow;

// OlapRewriteNode used to filter row that not suite for Olap Table
class OlapRewriteNode : public ExecNode {
public:
    OlapRewriteNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

    virtual Status init(const TPlanNode& tnode, RuntimeState* state = nullptr);
    virtual ~OlapRewriteNode() {}
    virtual Status prepare(RuntimeState* state);
    virtual Status open(RuntimeState* state);
    virtual Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos);
    virtual Status close(RuntimeState* state);

private:
    // Copy rows from _child_row_batch for which _conjuncts evaluate to true to
    // output_batch, up to _limit.
    // Return true if limit was hit or output_batch should be returned, otherwise false.
    bool copy_rows(RuntimeState* state, RowBatch* output_batch);
    bool copy_one_row(TupleRow* src_row, Tuple* tuple, MemPool* pool, std::stringstream* ss);

    // current row batch of child
    std::unique_ptr<RowBatch> _child_row_batch;

    // index of current row in _child_row_batch
    int _child_row_idx;

    // true if last get_next() call on child signalled eos
    bool _child_eos;

    std::vector<ExprContext*> _columns;
    std::vector<TColumnType> _column_types;

    TupleId _output_tuple_id;
    TupleDescriptor* _output_tuple_desc;

    std::vector<DecimalValue> _max_decimal_val;
    std::vector<DecimalV2Value> _max_decimalv2_val;
};

} // namespace doris

#endif
