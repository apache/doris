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

#ifndef DORIS_BE_SRC_QUERY_EXEC_BLOCKING_JOIN_NODE_H
#define DORIS_BE_SRC_QUERY_EXEC_BLOCKING_JOIN_NODE_H

#include <future>
#include <string>
#include <thread>

#include "exec/exec_node.h"
#include "gen_cpp/PlanNodes_types.h"

namespace doris {

class MemPool;
class RowBatch;
class TupleRow;

// Abstract base class for join nodes that block while consuming all rows from their
// right child in open().
class BlockingJoinNode : public ExecNode {
public:
    BlockingJoinNode(const std::string& node_name, const TJoinOp::type join_op, ObjectPool* pool,
                     const TPlanNode& tnode, const DescriptorTbl& descs);

    virtual ~BlockingJoinNode();

    // Subclasses should call BlockingJoinNode::init() and then perform any other init()
    // work, e.g. creating expr trees.
    virtual Status init(const TPlanNode& tnode, RuntimeState* state = nullptr);

    // Subclasses should call BlockingJoinNode::prepare() and then perform any other
    // prepare() work, e.g. codegen.
    virtual Status prepare(RuntimeState* state);

    // Open prepares the build side structures (subclasses should implement
    // construct_build_side()) and then prepares for GetNext with the first left child row
    // (subclasses should implement init_get_next()).
    virtual Status open(RuntimeState* state);

    // Subclasses should close any other structures and then call
    // BlockingJoinNode::close().
    virtual Status close(RuntimeState* state);

private:
    const std::string _node_name;
    TJoinOp::type _join_op;
    bool _eos;                            // if true, nothing left to return in get_next()
    std::unique_ptr<MemPool> _build_pool; // holds everything referenced from build side

    // _left_batch must be cleared before calling get_next().  The child node
    // does not initialize all tuple ptrs in the row, only the ones that it
    // is responsible for.
    std::unique_ptr<RowBatch> _left_batch;
    int _left_batch_pos; // current scan pos in _left_batch
    bool _left_side_eos; // if true, left child has no more rows to process
    TupleRow* _current_left_child_row;

    // _build_tuple_idx[i] is the tuple index of child(1)'s tuple[i] in the output row
    std::vector<int> _build_tuple_idx;
    int _build_tuple_size;

    // Size of the TupleRow (just the Tuple ptrs) from the build (right) and probe (left)
    // sides.
    int _probe_tuple_row_size;
    int _build_tuple_row_size;

    // byte size of result tuple row (sum of the tuple ptrs, not the tuple data).
    // This should be the same size as the left child tuple row.
    int _result_tuple_row_size;

    RuntimeProfile::Counter* _build_timer;            // time to prepare build side
    RuntimeProfile::Counter* _left_child_timer;       // time to process left child batch
    RuntimeProfile::Counter* _build_row_counter;      // num build rows
    RuntimeProfile::Counter* _left_child_row_counter; // num left child rows

    // Init the build-side state for a new left child row (e.g. hash table iterator or list
    // iterator) given the first row. Used in open() to prepare for get_next().
    // A nullptr ptr for first_left_child_row indicates the left child eos.
    virtual void init_get_next(TupleRow* first_left_child_row) = 0;

    // We parallelize building the build-side with Opening the
    // left child. If, for example, the left child is another
    // join node, it can start to build its own build-side at the
    // same time.
    virtual Status construct_build_side(RuntimeState* state) = 0;

    // Gives subclasses an opportunity to add debug output to the debug string printed by
    // debug_string().
    virtual void add_to_debug_string(int indentation_level, std::stringstream* out) const {}

    // Subclasses should not override, use add_to_debug_string() to add to the result.
    virtual void debug_string(int indentation_level, std::stringstream* out) const;

    // Returns a debug string for the left child's 'row'. They have tuple ptrs that are
    // uninitialized; the left child only populates the tuple ptrs it is responsible
    // for.  This function outputs just the row values and leaves the build
    // side values as nullptr.
    // This is only used for debugging and outputting the left child rows before
    // doing the join.
    std::string get_left_child_row_string(TupleRow* row);

    // Write combined row, consisting of the left child's 'left_row' and right child's
    // 'build_row' to 'out_row'.
    // This is replaced by codegen.
    void create_output_row(TupleRow* out_row, TupleRow* left_row, TupleRow* build_row);

    friend class CrossJoinNode;

private:
    // Supervises ConstructBuildSide in a separate thread, and returns its status in the
    // promise parameter.
    void build_side_thread(RuntimeState* state, std::promise<Status>* status);
};

} // namespace doris

#endif
