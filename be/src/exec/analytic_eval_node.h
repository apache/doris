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

#ifndef INF_DORIS_BE_SRC_EXEC_ANALYTIC_EVAL_NODE_H
#define INF_DORIS_BE_SRC_EXEC_ANALYTIC_EVAL_NODE_H

#include "exec/exec_node.h"
#include "exprs/expr.h"
//#include "exprs/expr_context.h"
#include "runtime/buffered_block_mgr2.h"
#include "runtime/buffered_tuple_stream2.h"
#include "runtime/buffered_tuple_stream2.inline.h"
#include "runtime/tuple.h"
#include "thrift/protocol/TDebugProtocol.h"

namespace doris {

class AggFnEvaluator;

// Evaluates analytic functions with a single pass over input rows. It is assumed
// that the input has already been sorted on all of the partition exprs and then the
// order by exprs. If there is no order by clause or partition clause, the input is
// unsorted. Uses a BufferedTupleStream to buffer input rows which are returned in a
// streaming fashion as entire row batches of output are ready to be returned, though in
// some cases the entire input must actually be consumed to produce any output rows.
//
// The output row is composed of the tuples from the child node followed by a single
// result tuple that holds the values of the evaluated analytic functions (one slot per
// analytic function).
//
// When enough input rows have been consumed to produce the results of all analytic
// functions for one or more rows (e.g. because the order by values are different for a
// RANGE window), the results of all the analytic functions for those rows are produced
// in a result tuple by calling GetValue()/Finalize() on the evaluators and storing the
// tuple in result_tuples_. Input row batches are fetched from the BufferedTupleStream,
// copied into output row batches, and the associated result tuple is set in each
// corresponding row. Result tuples may apply to many rows (e.g. an arbitrary number or
// an entire partition) so result_tuples_ stores a pair of the stream index (the last
// row in the stream it applies to) and the tuple.
//
// Input rows are consumed in a streaming fashion until enough input has been consumed
// in order to produce enough output rows. In some cases, this may mean that only a
// single input batch is needed to produce the results for an output batch, e.g.
// "SELECT RANK OVER (ORDER BY unique_col) ... ", but in other cases, an arbitrary
// number of rows may need to be buffered before result rows can be produced, e.g. if
// multiple rows have the same values for the order by exprs. The number of buffered
// rows may be an entire partition or even the entire input. Therefore, the output
// rows are buffered and may spill to disk via the BufferedTupleStream.
class AnalyticEvalNode : public ExecNode {
public:
    ~AnalyticEvalNode() {}
    AnalyticEvalNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

    virtual Status init(const TPlanNode& tnode, RuntimeState* state = nullptr);
    virtual Status prepare(RuntimeState* state);
    virtual Status open(RuntimeState* state);
    virtual Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos);
    virtual Status close(RuntimeState* state);

protected:
    // Frees local allocations from _evaluators
    // virtual Status QueryMaintenance(RuntimeState* state);

    virtual void debug_string(int indentation_level, std::stringstream* out) const;

private:
    // The scope over which analytic functions are evaluated. Functions are either
    // evaluated over a window (specified by a TAnalyticWindow) or an entire partition.
    // This is used to avoid more complex logic where we often branch based on these
    // cases, e.g. whether or not there is a window (i.e. no window = PARTITION) is stored
    // separately from the window type (assuming there is a window).
    enum AnalyticFnScope {
        // Analytic functions are evaluated over an entire partition (or the entire data set
        // if no partition clause was specified). Every row within a partition is added to
        // _curr_tuple and buffered in the _input_stream. Once all rows in a partition have
        // been consumed, a single result tuple is added to _result_tuples for all rows in
        // that partition.
        PARTITION,

        // Functions are evaluated over windows specified with range boundaries. Currently
        // only supports the 'default window', i.e. UNBOUNDED PRECEDING to CURRENT ROW. In
        // this case, when the values of the order by expressions change between rows a
        // result tuple is added to _result_tuples for the previous rows with the same values
        // for the order by expressions. This happens in try_add_result_tuple_for_prev_row()
        // because we determine if the order by expression values changed between the
        // previous and current row.
        RANGE,

        // Functions are evaluated over windows specified with rows boundaries. A result
        // tuple is added for every input row (except for some cases where the window extends
        // before or after the partition). When the end boundary is offset from the current
        // row, input rows are consumed and result tuples are produced for the associated
        // preceding or following row. When the start boundary is offset from the current
        // row, the first tuple (i.e. the input to the analytic functions) from the input
        // rows are buffered in _window_tuples because they must later be removed from the
        // window (by calling AggFnEvaluator::Remove() with the expired tuple to remove it
        // from the current row). When either the start or end boundaries are offset from the
        // current row, there is special casing around partition boundaries.
        ROWS
    };

    // Evaluates analytic functions over _curr_child_batch. Each input row is passed
    // to the evaluators and added to _input_stream where they are stored until a tuple
    // containing the results of the analytic functions for that row is ready to be
    // returned. When enough rows have been processed so that results can be produced for
    // one or more rows, a tuple containing those results are stored in _result_tuples.
    // That tuple gets set in the associated output row(s) later in get_next_output_batch().
    Status process_child_batch(RuntimeState* state);

    // Processes child batches (calling process_child_batch()) until enough output rows
    // are ready to return an output batch.
    Status process_child_batches(RuntimeState* state);

    // Returns a batch of output rows from _input_stream with the analytic function
    // results (from _result_tuples) set as the last tuple.
    Status get_next_output_batch(RuntimeState* state, RowBatch* row_batch, bool* eos);

    // Determines if there is a window ending at the previous row, and if so, calls
    // add_result_tuple() with the index of the previous row in _input_stream. next_partition
    // indicates if the current row is the start of a new partition. stream_idx is the
    // index of the current input row from _input_stream.
    void try_add_result_tuple_for_prev_row(bool next_partition, int64_t stream_idx, TupleRow* row);

    // Determines if there is a window ending at the current row, and if so, calls
    // add_result_tuple() with the index of the current row in _input_stream. stream_idx is
    // the index of the current input row from _input_stream.
    void try_add_result_tuple_for_curr_row(int64_t stream_idx, TupleRow* row);

    // Adds additional result tuples at the end of a partition, e.g. if the end bound is
    // FOLLOWING. partition_idx is the index into _input_stream of the new partition,
    // prev_partition_idx is the index of the previous partition.
    void try_add_remaining_results(int64_t partition_idx, int64_t prev_partition_idx);

    // Removes rows from _curr_tuple (by calling AggFnEvaluator::Remove()) that are no
    // longer in the window (i.e. they are before the window start boundary). stream_idx
    // is the index of the row in _input_stream that is currently being processed in
    // process_child_batch().
    void try_remove_rows_before_window(int64_t stream_idx);

    // Initializes state at the start of a new partition. stream_idx is the index of the
    // current input row from _input_stream.
    void init_next_partition(int64_t stream_idx);

    // Produces a result tuple with analytic function results by calling GetValue() or
    // Finalize() for _curr_tuple on the _evaluators. The result tuple is stored in
    // _result_tuples with the index into _input_stream specified by stream_idx.
    void add_result_tuple(int64_t stream_idx);

    // Gets the number of rows that are ready to be returned by subsequent calls to
    // get_next_output_batch().
    int64_t num_output_rows_ready() const;

    // Resets the slots in current_tuple_ that store the intermediate results for lead().
    // This is necessary to produce the default value (set by Init()).
    void reset_lead_fn_slots();

    // Evaluates the predicate pred_ctx over _child_tuple_cmp_row, which is a TupleRow*
    // containing the previous row and the current row set during process_child_batch().
    bool prev_row_compare(ExprContext* pred_ctx);

    // Debug string containing current state. If 'detailed', per-row state is included.
    std::string debug_state_string(bool detailed) const;

    std::string debug_evaluated_rows_string() const;

    // Debug string containing the window definition.
    std::string debug_window_string() const;

    // Window over which the analytic functions are evaluated. Only used if _fn_scope
    // is ROWS or RANGE.
    // TODO: _fn_scope and _window are candidates to be removed during codegen
    const TAnalyticWindow _window;

    // Tuple descriptor for storing intermediate values of analytic fn evaluation.
    const TupleDescriptor* _intermediate_tuple_desc;

    // Tuple descriptor for storing results of analytic fn evaluation.
    const TupleDescriptor* _result_tuple_desc;

    // Tuple descriptor of the buffered tuple (identical to the input child tuple, which is
    // assumed to come from a single SortNode). nullptr if both partition_exprs and
    // order_by_exprs are empty.
    TupleDescriptor* _buffered_tuple_desc;

    // TupleRow* composed of the first child tuple and the buffered tuple, used by
    // _partition_by_eq_expr_ctx and _order_by_eq_expr_ctx. Set in prepare() if
    // _buffered_tuple_desc is not nullptr, allocated from _mem_pool.
    TupleRow* _child_tuple_cmp_row;

    // Expr context for a predicate that checks if child tuple '<' buffered tuple for
    // partitioning exprs.
    ExprContext* _partition_by_eq_expr_ctx;

    // Expr context for a predicate that checks if child tuple '<' buffered tuple for
    // order by exprs.
    ExprContext* _order_by_eq_expr_ctx;

    // The scope over which analytic functions are evaluated.
    // TODO: Consider adding additional state to capture whether different kinds of window
    // bounds need to be maintained, e.g. (_fn_scope == ROWS && _window.__isset.end_bound).
    AnalyticFnScope _fn_scope;

    // Offset from the current row for ROWS windows with start or end bounds specified
    // with offsets. Is positive if the offset is FOLLOWING, negative if PRECEDING, and 0
    // if type is CURRENT ROW or UNBOUNDED PRECEDING/FOLLOWING.
    int64_t _rows_start_offset;
    int64_t _rows_end_offset;

    // Analytic function evaluators.
    std::vector<AggFnEvaluator*> _evaluators;

    // Indicates if each evaluator is the lead() fn. Used by reset_lead_fn_slots() to
    // determine which slots need to be reset.
    std::vector<bool> _is_lead_fn;

    // If true, evaluating FIRST_VALUE requires special null handling when initializing new
    // partitions determined by the offset. Set in Open() by inspecting the agg fns.
    bool _has_first_val_null_offset;
    long _first_val_null_offset;

    // FunctionContext for each analytic function. String data returned by the analytic
    // functions is allocated via these contexts.
    std::vector<doris_udf::FunctionContext*> _fn_ctxs;

    // Queue of tuples which are ready to be set in output rows, with the index into
    // the _input_stream stream of the last TupleRow that gets the Tuple. Pairs are
    // pushed onto the queue in process_child_batch() and dequeued in order in
    // get_next_output_batch(). The size of _result_tuples is limited by 2 times the
    // row batch size because we only process input batches if there are not enough
    // result tuples to produce a single batch of output rows. In the worst case there
    // may be a single result tuple per output row and _result_tuples.size() may be one
    // less than the row batch size, in which case we will process another input row batch
    // (inserting one result tuple per input row) before returning a row batch.
    std::list<std::pair<int64_t, Tuple*>> _result_tuples;

    // Index in _input_stream of the most recently added result tuple.
    int64_t _last_result_idx;

    // Child tuples (described by _child_tuple_desc) that are currently within the window
    // and the index into _input_stream of the row they're associated with. Only used when
    // window start bound is PRECEDING or FOLLOWING. Tuples in this list are deep copied
    // and owned by curr_window_tuple_pool_.
    // TODO: Remove and use BufferedTupleStream (needs support for multiple readers).
    std::list<std::pair<int64_t, Tuple*>> _window_tuples;
    TupleDescriptor* _child_tuple_desc;

    // Pools used to allocate result tuples (added to _result_tuples and later returned)
    // and window tuples (added to _window_tuples to buffer the current window). Resources
    // are transferred from _curr_tuple_pool to _prev_tuple_pool once it is at least
    // MAX_TUPLE_POOL_SIZE bytes. Resources from _prev_tuple_pool are transferred to an
    // output row batch when all result tuples it contains have been returned and all
    // window tuples it contains are no longer needed.
    std::unique_ptr<MemPool> _curr_tuple_pool;
    std::unique_ptr<MemPool> _prev_tuple_pool;

    // The index of the last row from _input_stream associated with output row containing
    // resources in _prev_tuple_pool. -1 when the pool is empty. Resources from
    // _prev_tuple_pool can only be transferred to an output batch once all rows containing
    // these tuples have been returned.
    int64_t _prev_pool_last_result_idx;

    // The index of the last row from _input_stream associated with window tuples
    // containing resources in _prev_tuple_pool. -1 when the pool is empty. Resources from
    // _prev_tuple_pool can only be transferred to an output batch once all rows containing
    // these tuples are no longer needed (removed from the _window_tuples).
    int64_t _prev_pool_last_window_idx;

    // The tuple described by _intermediate_tuple_desc storing intermediate state for the
    // _evaluators. When enough input rows have been consumed to produce the analytic
    // function results, a result tuple (described by _result_tuple_desc) is created and
    // the agg fn results are written to that tuple by calling Finalize()/GetValue()
    // on the evaluators with _curr_tuple as the source tuple.
    Tuple* _curr_tuple;

    // A tuple described by _result_tuple_desc used when calling Finalize() on the
    // _evaluators to release resources between partitions; the value is never used.
    // TODO: Remove when agg fns implement a separate Close() method to release resources.
    Tuple* _dummy_result_tuple;

    // Index of the row in _input_stream at which the current partition started.
    int64_t _curr_partition_idx;

    // Previous input row used to compare partition boundaries and to determine when the
    // order-by expressions change.
    TupleRow* _prev_input_row;

    // Current and previous input row batches from the child. RowBatches are allocated
    // once and reused. Previous input row batch owns _prev_input_row between calls to
    // process_child_batch(). The prev batch is Reset() after calling process_child_batch()
    // and then swapped with the curr batch so the RowBatch owning _prev_input_row is
    // stored in _prev_child_batch for the next call to process_child_batch().
    std::unique_ptr<RowBatch> _prev_child_batch;
    std::unique_ptr<RowBatch> _curr_child_batch;

    // Block manager client used by _input_stream. Not owned.
    BufferedBlockMgr2::Client* _block_mgr_client;

    // Buffers input rows added in process_child_batch() until enough rows are able to
    // be returned by get_next_output_batch(), in which case row batches are returned from
    // the front of the stream and the underlying buffered blocks are deleted once read.
    // The number of rows that must be buffered may vary from an entire partition (e.g.
    // no order by clause) to a single row (e.g. ROWS windows). When the amount of
    // buffered data exceeds the available memory in the underlying BufferedBlockMgr,
    // _input_stream is unpinned (i.e., possibly spilled to disk if necessary).
    // TODO: Consider re-pinning unpinned streams when possible.
    std::unique_ptr<BufferedTupleStream2> _input_stream;

    // Pool used for O(1) allocations that live until close.
    std::unique_ptr<MemPool> _mem_pool;

    // True when there are no more input rows to consume from our child.
    bool _input_eos;

    // Time spent processing the child rows.
    RuntimeProfile::Counter* _evaluation_timer;
};

} // namespace doris

#endif
