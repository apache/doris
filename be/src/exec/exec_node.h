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

#ifndef DORIS_BE_SRC_QUERY_EXEC_EXEC_NODE_H
#define DORIS_BE_SRC_QUERY_EXEC_EXEC_NODE_H

#include <mutex>
#include <sstream>
#include <vector>

#include "common/status.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/bufferpool/buffer_pool.h"
#include "runtime/descriptors.h"
#include "runtime/mem_pool.h"
#include "runtime/query_statistics.h"
#include "service/backend_options.h"
#include "util/blocking_queue.hpp"
#include "util/runtime_profile.h"
#include "util/uid_util.h" // for print_id

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
}

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
    virtual Status init(const TPlanNode& tnode, RuntimeState* state = nullptr);

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
    virtual Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) = 0;
    virtual Status get_next(RuntimeState* state, vectorized::Block* block, bool* eos);

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

    // Creates exec node tree from list of nodes contained in plan via depth-first
    // traversal. All nodes are placed in pool.
    // Returns error if 'plan' is corrupted, otherwise success.
    static Status create_tree(RuntimeState* state, ObjectPool* pool, const TPlan& plan,
                              const DescriptorTbl& descs, ExecNode** root);

    // Set debug action for node with given id in 'tree'
    static void set_debug_options(int node_id, TExecNodePhase::type phase,
                                  TDebugAction::type action, ExecNode* tree);

    // Collect all nodes of given 'node_type' that are part of this subtree, and return in
    // 'nodes'.
    void collect_nodes(TPlanNodeType::type node_type, std::vector<ExecNode*>* nodes);

    // Collect all scan node types.
    void collect_scan_nodes(std::vector<ExecNode*>* nodes);

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
    const RowDescriptor& row_desc() const { return _row_descriptor; }
    int64_t rows_returned() const { return _num_rows_returned; }
    int64_t limit() const { return _limit; }
    bool reached_limit() { return _limit != -1 && _num_rows_returned >= _limit; }
    const std::vector<TupleId>& get_tuple_ids() const { return _tuple_ids; }

    RuntimeProfile* runtime_profile() { return _runtime_profile.get(); }
    RuntimeProfile::Counter* memory_used_counter() const { return _memory_used_counter; }

    std::shared_ptr<MemTracker> mem_tracker() const { return _mem_tracker; }

    std::shared_ptr<MemTracker> expr_mem_tracker() const { return _expr_mem_tracker; }

    MemPool* expr_mem_pool() { return _expr_mem_pool.get(); }

    // Extract node id from p->name().
    static int get_node_id_from_profile(RuntimeProfile* p);

    // Names of counters shared by all exec nodes
    static const std::string ROW_THROUGHPUT_COUNTER;

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

    /// Release any unused reservation in excess of the node's initial reservation. Returns
    /// an error if releasing the reservation requires flushing pages to disk, and that
    /// fails.
    Status release_unused_reservation();

    /// Enable the increase reservation denial probability on 'buffer_pool_client_' based on
    /// the 'debug_action_' set on this node. Returns an error if 'debug_action_param_' is
    /// invalid.
    //Status enable_deny_reservation_debug_action();

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

        /// Gets a row batch from the queue. Returns NULL if there are no more.
        /// This function blocks.
        /// Returns NULL after Shutdown().
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

    /// Resource information sent from the frontend.
    const TBackendResourceProfile _resource_profile;

    // debug-only: if _debug_action is not INVALID, node will perform action in
    // _debug_phase
    TExecNodePhase::type _debug_phase;
    TDebugAction::type _debug_action;

    int64_t _limit; // -1: no limit
    int64_t _num_rows_returned;

    boost::scoped_ptr<RuntimeProfile> _runtime_profile;

    /// Account for peak memory used by this node
    std::shared_ptr<MemTracker> _mem_tracker;

    /// MemTracker used by 'expr_mem_pool_'.
    std::shared_ptr<MemTracker> _expr_mem_tracker;

    /// MemPool for allocating data structures used by expression evaluators in this node.
    /// Created in Prepare().
    boost::scoped_ptr<MemPool> _expr_mem_pool;

    RuntimeProfile::Counter* _rows_returned_counter;
    RuntimeProfile::Counter* _rows_returned_rate;
    // Account for peak memory used by this node
    RuntimeProfile::Counter* _memory_used_counter;

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

    ExecNode* child(int i) { return _children[i]; }

    bool is_closed() const { return _is_closed; }

    // TODO(zc)
    /// Pointer to the containing SubplanNode or NULL if not inside a subplan.
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

    // Executes _debug_action if phase matches _debug_phase.
    // 'phase' must not be INVALID.
    Status exec_debug_action(TExecNodePhase::type phase);

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

private:
    bool _is_closed;
};

#define LIMIT_EXCEEDED(tracker, state, msg)                                                   \
    do {                                                                                      \
        stringstream str;                                                                     \
        str << "Memory exceed limit. " << msg << " ";                                         \
        str << "Backend: " << BackendOptions::get_localhost() << ", ";                        \
        str << "fragment: " << print_id(state->fragment_instance_id()) << " ";                \
        str << "Used: " << tracker->consumption() << ", Limit: " << tracker->limit() << ". "; \
        str << "You can change the limit by session variable exec_mem_limit.";                \
        return Status::MemoryLimitExceeded(str.str());                                        \
    } while (false)

#define RETURN_IF_LIMIT_EXCEEDED(state, msg)                                                \
    do {                                                                                    \
        /* if (UNLIKELY(MemTracker::limit_exceeded(*(state)->mem_trackers()))) { */         \
        MemTracker* tracker = state->instance_mem_tracker()->find_limit_exceeded_tracker(); \
        if (tracker != nullptr) {                                                           \
            LIMIT_EXCEEDED(tracker, state, msg);                                            \
        }                                                                                   \
    } while (false)
} // namespace doris

#endif
