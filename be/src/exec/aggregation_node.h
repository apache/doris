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

#ifndef DORIS_BE_SRC_QUERY_EXEC_AGGREGATION_NODE_H
#define DORIS_BE_SRC_QUERY_EXEC_AGGREGATION_NODE_H

#include <boost/scoped_ptr.hpp>
#include <functional>

#include "exec/exec_node.h"
#include "exec/hash_table.h"
#include "runtime/descriptors.h"
#include "runtime/free_list.hpp"
#include "runtime/mem_pool.h"
#include "runtime/string_value.h"

namespace doris {

class AggFnEvaluator;
class RowBatch;
class RuntimeState;
struct StringValue;
class Tuple;
class TupleDescriptor;
class SlotDescriptor;

// Node for in-memory hash aggregation.
// The node creates a hash set of aggregation output tuples, which
// contain slots for all grouping and aggregation exprs (the grouping
// slots precede the aggregation expr slots in the output tuple descriptor).
//
// For string aggregation, we need to append additional data to the tuple object
// to reduce the number of string allocations (since we cannot know the length of
// the output string beforehand).  For each string slot in the output tuple, a int32
// will be appended to the end of the normal tuple data that stores the size of buffer
// for that string slot.  This also results in the correct alignment because StringValue
// slots are 8-byte aligned and form the tail end of the tuple.
class AggregationNode : public ExecNode {
public:
    AggregationNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    virtual ~AggregationNode();

    virtual Status init(const TPlanNode& tnode, RuntimeState* state = nullptr);
    virtual Status prepare(RuntimeState* state);
    virtual Status open(RuntimeState* state);
    virtual Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos);
    virtual Status close(RuntimeState* state);

    virtual void debug_string(int indentation_level, std::stringstream* out) const;
    virtual void push_down_predicate(RuntimeState* state, std::list<ExprContext*>* expr_ctxs);

private:
    boost::scoped_ptr<HashTable> _hash_tbl;
    HashTable::Iterator _output_iterator;

    std::vector<AggFnEvaluator*> _aggregate_evaluators;

    /// FunctionContext for each agg fn and backing pool.
    std::vector<doris_udf::FunctionContext*> _agg_fn_ctxs;
    boost::scoped_ptr<MemPool> _agg_fn_pool;

    // Exprs used to evaluate input rows
    std::vector<ExprContext*> _probe_expr_ctxs;
    // Exprs used to insert constructed aggregation tuple into the hash table.
    // All the exprs are simply SlotRefs for the agg tuple.
    std::vector<ExprContext*> _build_expr_ctxs;

    /// Tuple into which Update()/Merge()/Serialize() results are stored.
    TupleId _intermediate_tuple_id;
    TupleDescriptor* _intermediate_tuple_desc;

    /// Tuple into which Finalize() results are stored. Possibly the same as
    /// the intermediate tuple.
    TupleId _output_tuple_id;
    TupleDescriptor* _output_tuple_desc;

    Tuple* _singleton_output_tuple; // result of aggregation w/o GROUP BY
    boost::scoped_ptr<MemPool> _tuple_pool;

    typedef void (*ProcessRowBatchFn)(AggregationNode*, RowBatch*);
    // Jitted ProcessRowBatch function pointer.  Null if codegen is disabled.
    ProcessRowBatchFn _process_row_batch_fn;

    // Certain aggregates require a finalize step, which is the final step of the
    // aggregate after consuming all input rows. The finalize step converts the aggregate
    // value into its final form. This is true if this node contains aggregate that requires
    // a finalize step.
    bool _needs_finalize;

    // Time spent processing the child rows
    RuntimeProfile::Counter* _build_timer;
    // Time spent returning the aggregated rows
    RuntimeProfile::Counter* _get_results_timer;
    // Num buckets in hash table
    RuntimeProfile::Counter* _hash_table_buckets_counter;
    // Load factor in hash table
    RuntimeProfile::Counter* _hash_table_load_factor_counter;

    // Constructs a new aggregation output tuple (allocated from _tuple_pool),
    // initialized to grouping values computed over '_current_row'.
    // Aggregation expr slots are set to their initial values.
    Tuple* construct_intermediate_tuple();

    // Updates the aggregation output tuple 'tuple' with aggregation values
    // computed over 'row'.
    void update_tuple(Tuple* tuple, TupleRow* row);

    // Called when all rows have been aggregated for the aggregation tuple to compute final
    // aggregate values
    Tuple* finalize_tuple(Tuple* tuple, MemPool* pool);

    // Do the aggregation for all tuple rows in the batch
    void process_row_batch_no_grouping(RowBatch* batch, MemPool* pool);
    void process_row_batch_with_grouping(RowBatch* batch, MemPool* pool);
};

} // namespace doris

#endif
