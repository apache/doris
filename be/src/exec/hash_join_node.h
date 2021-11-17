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

#ifndef DORIS_BE_SRC_QUERY_EXEC_HASH_JOIN_NODE_H
#define DORIS_BE_SRC_QUERY_EXEC_HASH_JOIN_NODE_H

#include <future>
#include <string>
#include <thread>
#include <unordered_set>

#include "exec/exec_node.h"
#include "exec/hash_table.h"
#include "exprs/runtime_filter_slots.h"
#include "gen_cpp/PlanNodes_types.h"

namespace doris {

class MemPool;
class RowBatch;
class TupleRow;

// Node for in-memory hash joins:
// - builds up a hash table with the rows produced by our right input
//   (child(1)); build exprs are the rhs exprs of our equi-join predicates
// - for each row from our left input, probes the hash table to retrieve
//   matching entries; the probe exprs are the lhs exprs of our equi-join predicates
//
// Row batches:
// - In general, we are not able to pass our output row batch on to our left child (when
//   we're fetching the probe rows): if we have a 1xn join, our output will contain
//   multiple rows per left input row
// - TODO: fix this, so in the case of 1x1/nx1 joins (for instance, fact to dimension tbl)
//   we don't do these extra copies
class HashJoinNode : public ExecNode {
public:
    HashJoinNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

    ~HashJoinNode();

    // set up _build- and _probe_exprs
    virtual Status init(const TPlanNode& tnode, RuntimeState* state = nullptr);
    virtual Status prepare(RuntimeState* state);
    virtual Status open(RuntimeState* state);
    virtual Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos);
    virtual Status close(RuntimeState* state);

protected:
    void debug_string(int indentation_level, std::stringstream* out) const;

private:
    friend class IRuntimeFilter;

    std::unique_ptr<HashTable> _hash_tbl;
    HashTable::Iterator _hash_tbl_iterator;

    // for right outer joins, keep track of what's been joined
    typedef std::unordered_set<TupleRow*> BuildTupleRowSet;
    BuildTupleRowSet _joined_build_rows;

    TJoinOp::type _join_op;

    // our equi-join predicates "<lhs> = <rhs>" are separated into
    // _build_exprs (over child(1)) and _probe_exprs (over child(0))
    std::vector<ExprContext*> _probe_expr_ctxs;
    std::vector<ExprContext*> _build_expr_ctxs;
    // true: the operator of eq join predicate is null safe equal => '<=>'
    // false: the operator of eq join predicate is equal => '='
    std::vector<bool> _is_null_safe_eq_join;
    std::list<ExprContext*> _push_down_expr_ctxs;

    // non-equi-join conjuncts from the JOIN clause
    std::vector<ExprContext*> _other_join_conjunct_ctxs;

    // derived from _join_op
    bool _match_all_probe; // output all rows coming from the probe input
    bool _match_one_build; // match at most one build row to each probe row
    bool _match_all_build; // output all rows coming from the build input
    bool _build_unique;    // build a hash table without duplicated rows

    bool _matched_probe;                  // if true, we have matched the current probe row
    bool _eos;                            // if true, nothing left to return in get_next()
    std::unique_ptr<MemPool> _build_pool; // holds everything referenced in _hash_tbl

    // Size of the TupleRow (just the Tuple ptrs) from the build (right) and probe (left)
    // sides. Set to zero if the build/probe tuples are not returned, e.g., for semi joins.
    // Cached because it is used in the hot path.
    int _probe_tuple_row_size;
    int _build_tuple_row_size;

    // _probe_batch must be cleared before calling get_next().  The child node
    // does not initialize all tuple ptrs in the row, only the ones that it
    // is responsible for.
    std::unique_ptr<RowBatch> _probe_batch;
    int _probe_batch_pos; // current scan pos in _probe_batch
    int _probe_counter;
    bool _probe_eos; // if true, probe child has no more rows to process
    TupleRow* _current_probe_row;

    // _build_tuple_idx[i] is the tuple index of child(1)'s tuple[i] in the output row
    std::vector<int> _build_tuple_idx;
    int _build_tuple_size;

    // byte size of result tuple row (sum of the tuple ptrs, not the tuple data).
    // This should be the same size as the probe tuple row.
    int _result_tuple_row_size;

    // HashJoinNode::process_probe_batch() exactly
    typedef int (*ProcessProbeBatchFn)(HashJoinNode*, RowBatch*, RowBatch*, int);
    // Jitted ProcessProbeBatch function pointer.  Null if codegen is disabled.
    ProcessProbeBatchFn _process_probe_batch_fn;

    // record anti join pos in get_next()
    HashTable::Iterator* _anti_join_last_pos;

    RuntimeProfile::Counter* _build_timer;     // time to build hash table
    RuntimeProfile::Counter* _push_down_timer; // time to build hash table
    RuntimeProfile::Counter* _push_compute_timer;
    RuntimeProfile::Counter* _probe_timer;           // time to probe
    RuntimeProfile::Counter* _build_rows_counter;    // num build rows
    RuntimeProfile::Counter* _probe_rows_counter;    // num probe rows
    RuntimeProfile::Counter* _build_buckets_counter; // num buckets in hash table
    RuntimeProfile::Counter* _hash_tbl_load_factor_counter;
    RuntimeProfile::Counter* _hash_table_list_min_size;
    RuntimeProfile::Counter* _hash_table_list_max_size;

    // Supervises ConstructHashTable in a separate thread, and
    // returns its status in the promise parameter.
    void build_side_thread(RuntimeState* state, std::promise<Status>* status);

    // We parallelise building the build-side with Open'ing the
    // probe-side. If, for example, the probe-side child is another
    // hash-join node, it can start to build its own build-side at the
    // same time.
    Status construct_hash_table(RuntimeState* state);

    // GetNext helper function for the common join cases: Inner join, left semi and left
    // outer
    Status left_join_get_next(RuntimeState* state, RowBatch* row_batch, bool* eos);

    // Processes a probe batch for the common (non right-outer join) cases.
    //  out_batch: the batch for resulting tuple rows
    //  probe_batch: the probe batch to process.  This function can be called to
    //    continue processing a batch in the middle
    //  max_added_rows: maximum rows that can be added to out_batch
    // return the number of rows added to out_batch
    int process_probe_batch(RowBatch* out_batch, RowBatch* probe_batch, int max_added_rows);

    // Construct the build hash table, adding all the rows in 'build_batch'
    Status process_build_batch(RuntimeState* state, RowBatch* build_batch);

    // Write combined row, consisting of probe_row and build_row, to out_row.
    // This is replaced by codegen.
    void create_output_row(TupleRow* out_row, TupleRow* probe_row, TupleRow* build_row);

    // Returns a debug string for probe_rows.  Probe rows have tuple ptrs that are
    // uninitialized; the left hand child only populates the tuple ptrs it is responsible
    // for.  This function outputs just the probe row values and leaves the build
    // side values as nullptr.
    // This is only used for debugging and outputting the left child rows before
    // doing the join.
    std::string get_probe_row_output_string(TupleRow* probe_row);

    std::vector<TRuntimeFilterDesc> _runtime_filter_descs;
};

} // namespace doris

#endif
