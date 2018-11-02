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

#ifndef DORIS_BE_SRC_QUERY_EXEC_PRE_AGGREGATION_NODE_H
#define DORIS_BE_SRC_QUERY_EXEC_PRE_AGGREGATION_NODE_H

#include <boost/scoped_ptr.hpp>
#include <functional>

#include "exec/exec_node.h"
#include "exec/hash_table.h"
#include "runtime/descriptors.h"
#include "runtime/free_list.hpp"
#include "runtime/mem_pool.h"

namespace doris {

class AggregateExpr;
class AggregationTuple;
class RowBatch;
class RuntimeState;
struct StringValue;
class Tuple;
class TupleDescriptor;

// this class do aggregate before hash join execute
class PreAggregationNode : public ExecNode {
public:
    PreAggregationNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    virtual ~PreAggregationNode();

    virtual Status prepare(RuntimeState* state);
    virtual Status open(RuntimeState* state);
    virtual Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos);
    virtual Status close(RuntimeState* state);

protected:
    virtual void debug_string(int indentation_level, std::stringstream* out) const;

private:
    // Constructs a new aggregation output tuple (allocated from _tuple_pool),
    // initialized to grouping values computed over '_current_row'.
    // Aggregation expr slots are set to their initial values.
    TupleRow* construct_row(TupleRow* in_row);
    Status construct_single_row();

    // Updates the aggregation output tuple 'tuple' with aggregation values
    // computed over 'row'.
    Status update_agg_row(TupleRow* agg_row, TupleRow* row);

    // Do the aggregation for all tuple rows in the batch
    Status process_row_batch_no_grouping(RowBatch* batch);
    Status process_row_batch_with_grouping(RowBatch* batch);

    bool _construct_fail;
    bool _is_init;
    boost::scoped_ptr<HashTable> _hash_tbl;
    HashTable::Iterator _output_iterator;
    std::vector<int> _children_tuple;

    int _tuple_row_size;
    bool _use_aggregate;
    bool _child_eos;

    int _input_record_num;
    int _input_record_num_sum;
    int _agg_record_num;
    int _agg_record_num_sum;
    int _bad_agg_num;

    int _bad_agg_latch;
    int _agg_record_latch;
    int _agg_rate_latch;

    int _build_tuple_size;
    std::vector<Expr*> _aggregate_exprs;
    // Exprs used to evaluate input rows
    std::vector<Expr*> _probe_exprs;
    // Exprs used to insert constructed aggregation tuple into the hash table.
    // All the exprs are simply SlotRefs for the agg tuple.
    std::vector<Expr*> _build_exprs;
    // result of aggregation without GROUP BY
    // because there is only one result row of this query witch has no group by
    TupleRow* _singleton_agg_row;

    boost::scoped_ptr<MemPool> _tuple_pool;

    // Time spent processing the child rows
    RuntimeProfile::Counter* _build_timer;
    // Time spent returning the aggregated rows
    RuntimeProfile::Counter* _get_results_timer;
    // Num buckets in hash table
    RuntimeProfile::Counter* _hash_table_buckets_counter;
    // Load factor in hash table
    RuntimeProfile::Counter* _hash_table_load_factor_counter;
};
}
#endif
