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
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/exec/exec-node.h
// and modified by Doris

#pragma once

#include <mutex>
#include <sstream>
#include <vector>

#include "common/status.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/bufferpool/buffer_pool.h"
#include "runtime/descriptors.h"
#include "runtime/query_statistics.h"
#include "service/backend_options.h"
#include "util/blocking_queue.hpp"
#include "util/runtime_profile.h"
#include "util/telemetry/telemetry.h"
#include "vec/exprs/vexpr_context.h"

namespace doris {
class Expr;
class ExprContext;
class ObjectPool;
class Counters;
class RowBatch;
class RuntimeState;
class TPlan;
class TupleRow;
class DataSink;
class MemTracker;

namespace vectorized {
class Block;
class VExpr;
} // namespace vectorized

namespace pipeline {
class PipelineFragmentContext;
class Pipeline;
class OperatorBase;
} // namespace pipeline

using std::string;
using std::stringstream;
using std::vector;
using std::map;
using std::lock_guard;
using std::mutex;

// Superclass of all executor nodes.
// All subclasses need to make sure to check RuntimeState::is_cancelled()
// periodically in order to ensure timely termination after the cancellation
// flag gets set.
class ExecNode {
public:
    // Init conjuncts.
    ExecNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

    virtual ~ExecNode();

    /// Initializes this object from the thrift tnode desc. The subclass should
    /// do any initialization that can fail in Init() rather than the ctor.
    /// If overridden in subclass, must first call superclass's Init().
    virtual Status init(const TPlanNode& tnode, RuntimeState* state);

    // Sets up internal structures, etc., without doing any actual work.
    // Must be called prior to open(). Will only be called once in this
    // node's lifetime.
    // All code generation (adding functions to the LlvmCodeGen object) must happen
    // in prepare().  Retrieving the jit compiled function pointer must happen in
    // open().
    // If overridden in subclass, must first call superclass's prepare().
    virtual Status prepare(RuntimeState* state);

    // Performs any preparatory work prior to calling get_next().
    // Can be called repeatedly (after calls to close()).
    // Caller must not be holding any io buffers. This will cause deadlock.
    virtual Status open(RuntimeState* state);

    // Alloc and open resource for the node
    // Only pipeline operator use exec node need to impl the virtual function
    // so only vectorized exec node need to impl
    virtual Status alloc_resource(RuntimeState* state);

    // Retrieves rows and returns them via row_batch. Sets eos to true
    // if subsequent calls will not retrieve any more rows.
    // Data referenced by any tuples returned in row_batch must not be overwritten
    // by the callee until close() is called. The memory holding that data
    // can be returned via row_batch's tuple_data_pool (in which case it may be deleted
    // by the caller) or held on to by the callee. The row_batch, including its
    // tuple_data_pool, will be destroyed by the caller at some point prior to the final
    // close() call.
    // In other words, if the memory holding the tuple data will be referenced
    // by the callee in subsequent get_next() calls, it must *not* be attached to the
    // row_batch's tuple_data_pool.
    // Caller must not be holding any io buffers. This will cause deadlock.
    // TODO: AggregationNode and HashJoinNode cannot be "re-opened" yet.
    virtual Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos);
    virtual Status get_next(RuntimeState* state, vectorized::Block* block, bool* eos);
    // new interface to compatible new optimizers in FE
    Status get_next_after_projects(RuntimeState* state, vectorized::Block* block, bool* eos);

    // Emit data, both need impl with method: sink
    // Eg: Aggregation, Sort, Scan
    virtual Status pull(RuntimeState* state, vectorized::Block* output_block, bool* eos) {
        return get_next(state, output_block, eos);
    }

    virtual Status push(RuntimeState* state, vectorized::Block* input_block, bool eos) {
        return Status::OK();
    }

    bool can_read() const { return _can_read; }

    // Sink Data to ExecNode to do some stock work, both need impl with method: get_result
    // `eos` means source is exhausted, exec node should do some finalize work
    // Eg: Aggregation, Sort
    virtual Status sink(RuntimeState* state, vectorized::Block* input_block, bool eos);

    // Resets the stream of row batches to be retrieved by subsequent GetNext() calls.
    // Clears all internal state, returning this node to the state it was in after calling
    // Prepare() and before calling Open(). This function must not clear memory
    // still owned by this node that is backing rows returned in GetNext().
    // Prepare() and Open() must have already been called before calling Reset().
    // GetNext() may have optionally been called (not necessarily until eos).
    // Close() must not have been called.
    // Reset() is not idempotent. Calling it multiple times in a row without a preceding
    // call to Open() is invalid.
    // If overridden in a subclass, must call superclass's Reset() at the end. The default
    // implementation calls Reset() on children.
    // Note that this function may be called many times (proportional to the input data),
    // so should be fast.
    virtual Status reset(RuntimeState* state);

    // This should be called before close() and after get_next(), it is responsible for
    // collecting statistics sent with row batch, it can't be called when prepare() returns
    // error.
    virtual Status collect_query_statistics(QueryStatistics* statistics);

    // close() will get called for every exec node, regardless of what else is called and
    // the status of these calls (i.e. prepare() may never have been called, or
    // prepare()/open()/get_next() returned with an error).
    // close() releases all resources that were allocated in open()/get_next(), even if the
    // latter ended with an error. close() can be called if the node has been prepared or
    // the node is closed.
    // After calling close(), the caller calls open() again prior to subsequent calls to
    // get_next(). The default implementation updates runtime profile counters and calls
    // close() on the children. To ensure that close() is called on the entire plan tree,
    // each implementation should start out by calling the default implementation.
    virtual Status close(RuntimeState* state);

    void increase_ref() { ++_ref; }
    int decrease_ref() { return --_ref; }

    // Release and close resource for the node
    // Only pipeline operator use exec node need to impl the virtual function
    // so only vectorized exec node need to impl
    virtual void release_resource(RuntimeState* state);

    // Creates exec node tree from list of nodes contained in plan via depth-first
    // traversal. All nodes are placed in pool.
    // Returns error if 'plan' is corrupted, otherwise success.
    static Status create_tree(RuntimeState* state, ObjectPool* pool, const TPlan& plan,
                              const DescriptorTbl& descs, ExecNode** root);

    // Collect all nodes of given 'node_type' that are part of this subtree, and return in
    // 'nodes'.
    void collect_nodes(TPlanNodeType::type node_type, std::vector<ExecNode*>* nodes);

    // Collect all scan node types.
    void collect_scan_nodes(std::vector<ExecNode*>* nodes);

    virtual void prepare_for_next() {}

    // When the agg node is the scan node direct parent,
    // we directly return agg object from scan node to agg node,
    // and don't serialize the agg object.
    // This improve is cautious, we ensure the correctness firstly.
    void try_do_aggregate_serde_improve();

    typedef bool (*EvalConjunctsFn)(ExprContext* const* ctxs, int num_ctxs, TupleRow* row);
    // Evaluate exprs over row.  Returns true if all exprs return true.
    // TODO: This doesn't use the vector<Expr*> signature because I haven't figured
    // out how to deal with declaring a templated std:vector type in IR
    static bool eval_conjuncts(ExprContext* const* ctxs, int num_ctxs, TupleRow* row);

    // Returns a string representation in DFS order of the plan rooted at this.
    std::string debug_string() const;

    virtual void push_down_predicate(RuntimeState* state, std::list<ExprContext*>* expr_ctxs);

    // recursive helper method for generating a string for Debug_string().
    // implementations should call debug_string(int, std::stringstream) on their children.
    // Input parameters:
    //   indentation_level: Current level in plan tree.
    // Output parameters:
    //   out: Stream to accumulate debug string.
    virtual void debug_string(int indentation_level, std::stringstream* out) const;

    const std::vector<ExprContext*>& conjunct_ctxs() const { return _conjunct_ctxs; }

    int id() const { return _id; }
    TPlanNodeType::type type() const { return _type; }
    virtual const RowDescriptor& row_desc() const {
        return _output_row_descriptor ? *_output_row_descriptor : _row_descriptor;
    }
    virtual const RowDescriptor& intermediate_row_desc() const { return _row_descriptor; }
    int64_t rows_returned() const { return _num_rows_returned; }
    int64_t limit() const { return _limit; }
    bool reached_limit() const { return _limit != -1 && _num_rows_returned >= _limit; }
    const std::vector<TupleId>& get_tuple_ids() const { return _tuple_ids; }

    RuntimeProfile* runtime_profile() const { return _runtime_profile.get(); }
    RuntimeProfile::Counter* memory_used_counter() const { return _memory_used_counter; }

    MemTracker* mem_tracker_held() const { return _mem_tracker_held.get(); }
    MemTracker* mem_tracker_growh() const { return _mem_tracker_growh.get(); }
    std::shared_ptr<MemTracker> mem_tracker_growh_shared() const { return _mem_tracker_growh; }

    OpentelemetrySpan get_next_span() { return _get_next_span; }

    virtual std::string get_name();

    // Names of counters shared by all exec nodes
    static const std::string ROW_THROUGHPUT_COUNTER;

    ExecNode* child(int i) { return _children[i]; }

    size_t children_count() const { return _children.size(); }

protected:
    friend class DataSink;

    /// Initialize 'buffer_pool_client_' and claim the initial reservation for this
    /// ExecNode. Only needs to be called by ExecNodes that will use the client.
    /// The client is automatically cleaned up in Close(). Should not be called if
    /// the client is already open.
    /// The ExecNode must return the initial reservation to
    /// QueryState::initial_reservations(), which is done automatically in Close() as long
    /// as the initial reservation is not released before Close().
    Status claim_buffer_reservation(RuntimeState* state);

    /// Release all memory of block which got from child. The block
    // 1. clear mem of valid column get from child, make sure child can reuse the mem
    // 2. delete and release the column which create by function all and other reason
    void release_block_memory(vectorized::Block& block, uint16_t child_idx = 0);

    /// Only use in vectorized exec engine to check whether reach limit and cut num row for block
    // and add block rows for profile
    void reached_limit(vectorized::Block* block, bool* eos);

    /// Only use in vectorized exec engine try to do projections to trans _row_desc -> _output_row_desc
    Status do_projections(vectorized::Block* origin_block, vectorized::Block* output_block);

    /// Extends blocking queue for row batches. Row batches have a property that
    /// they must be processed in the order they were produced, even in cancellation
    /// paths. Preceding row batches can contain ptrs to memory in subsequent row batches
    /// and we need to make sure those ptrs stay valid.
    /// Row batches that are added after Shutdown() are queued in another queue, which can
    /// be cleaned up during Close().
    /// All functions are thread safe.
    class RowBatchQueue : public BlockingQueue<RowBatch*> {
    public:
        /// max_batches is the maximum number of row batches that can be queued.
        /// When the queue is full, producers will block.
        RowBatchQueue(int max_batches);
        ~RowBatchQueue();

        /// Adds a batch to the queue. This is blocking if the queue is full.
        void AddBatch(RowBatch* batch);

        /// Adds a batch to the queue. If the queue is full, this blocks until space becomes
        /// available or 'timeout_micros' has elapsed.
        /// Returns true if the element was added to the queue, false if it wasn't. If this
        /// method returns false, the queue didn't take ownership of the batch and it must be
        /// managed externally.
        bool AddBatchWithTimeout(RowBatch* batch, int64_t timeout_micros);

        /// Gets a row batch from the queue. Returns nullptr if there are no more.
        /// This function blocks.
        /// Returns nullptr after Shutdown().
        RowBatch* GetBatch();

        /// Deletes all row batches in cleanup_queue_. Not valid to call AddBatch()
        /// after this is called.
        /// Returns the number of io buffers that were released (for debug tracking)
        int Cleanup();

    private:
        /// Lock protecting cleanup_queue_
        // SpinLock lock_;
        // TODO(dhc): need to modify spinlock
        std::mutex lock_;

        /// Queue of orphaned row batches
        std::list<RowBatch*> cleanup_queue_;
    };

    int _id; // unique w/in single plan tree
    TPlanNodeType::type _type;
    ObjectPool* _pool;
    std::vector<Expr*> _conjuncts;
    std::vector<ExprContext*> _conjunct_ctxs;
    std::vector<TupleId> _tuple_ids;

    std::unique_ptr<doris::vectorized::VExprContext*> _vconjunct_ctx_ptr;

    std::vector<ExecNode*> _children;
    RowDescriptor _row_descriptor;
    vectorized::Block _origin_block;

    std::unique_ptr<RowDescriptor> _output_row_descriptor;
    std::vector<doris::vectorized::VExprContext*> _projections;

    /// Resource information sent from the frontend.
    const TBackendResourceProfile _resource_profile;

    int64_t _limit; // -1: no limit
    int64_t _num_rows_returned;

    std::unique_ptr<RuntimeProfile> _runtime_profile;

    // Record the memory size held by this node.
    std::unique_ptr<MemTracker> _mem_tracker_held;
    // Record the memory size allocated by this node.
    // Similar to tcmalloc heap profile growh, only track memory alloc, not track memory free.
    std::shared_ptr<MemTracker> _mem_tracker_growh;

    RuntimeProfile::Counter* _rows_returned_counter;
    RuntimeProfile::Counter* _rows_returned_rate;
    // Account for peak memory used by this node
    RuntimeProfile::Counter* _memory_used_counter;
    RuntimeProfile::Counter* _projection_timer;

    /// Since get_next is a frequent operation, it is not necessary to generate a span for each call
    /// to the get_next method. Therefore, the call of the get_next method in the ExecNode is
    /// merged into this _get_next_span. The _get_next_span is initialized by
    /// INIT_AND_SCOPE_GET_NEXT_SPAN when the get_next method is called for the first time
    /// (recording the start timestamp), and is ended by RETURN_IF_ERROR_AND_CHECK_SPAN after the
    /// last call to the get_next method (the record is terminated timestamp).
    OpentelemetrySpan _get_next_span;

    // Execution options that are determined at runtime.  This is added to the
    // runtime profile at close().  Examples for options logged here would be
    // "Codegen Enabled"
    std::mutex _exec_options_lock;
    std::string _runtime_exec_options;

    /// Buffer pool client for this node. Initialized with the node's minimum reservation
    /// in ClaimBufferReservation(). After initialization, the client must hold onto at
    /// least the minimum reservation so that it can be returned to the initial
    /// reservations pool in Close().
    BufferPool::ClientHandle _buffer_pool_client;

    // Set to true if this is a vectorized exec node.
    bool _is_vec = false;

    bool is_closed() const { return _is_closed; }

    // TODO(zc)
    /// Pointer to the containing SubplanNode or nullptr if not inside a subplan.
    /// Set by SubplanNode::Init(). Not owned.
    // SubplanNode* containing_subplan_;

    /// Returns true if this node is inside the right-hand side plan tree of a SubplanNode.
    /// Valid to call in or after Prepare().
    bool is_in_subplan() const { return false; }

    // Create a single exec node derived from thrift node; place exec node in 'pool'.
    static Status create_node(RuntimeState* state, ObjectPool* pool, const TPlanNode& tnode,
                              const DescriptorTbl& descs, ExecNode** node);

    static Status create_tree_helper(RuntimeState* state, ObjectPool* pool,
                                     const std::vector<TPlanNode>& tnodes,
                                     const DescriptorTbl& descs, ExecNode* parent, int* node_idx,
                                     ExecNode** root);

    virtual bool is_scan_node() const { return false; }

    void init_runtime_profile(const std::string& name);

    // Appends option to '_runtime_exec_options'
    void add_runtime_exec_option(const std::string& option);

    /// Frees any local allocations made by evals_to_free_ and returns the result of
    /// state->CheckQueryState(). Nodes should call this periodically, e.g. once per input
    /// row batch. This should not be called outside the main execution thread.
    //
    /// Nodes may override this to add extra periodic cleanup, e.g. freeing other local
    /// allocations. ExecNodes overriding this function should return
    /// ExecNode::QueryMaintenance().
    virtual Status QueryMaintenance(RuntimeState* state, const std::string& msg) WARN_UNUSED_RESULT;
    std::atomic<bool> _can_read = false;

private:
    friend class pipeline::OperatorBase;
    bool _is_closed;
    bool _is_resource_released = false;
    std::atomic_int _ref; // used by pipeline operator to release resource.
};

} // namespace doris
