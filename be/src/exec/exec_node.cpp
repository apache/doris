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

#include "exec/exec_node.h"

#include <thrift/protocol/TDebugProtocol.h>
#include <unistd.h>

#include <sstream>

#include "common/object_pool.h"
#include "common/status.h"
#include "exec/analytic_eval_node.h"
#include "exec/assert_num_rows_node.h"
#include "exec/broker_scan_node.h"
#include "exec/cross_join_node.h"
#include "exec/empty_set_node.h"
#include "exec/es_http_scan_node.h"
#include "exec/es_scan_node.h"
#include "exec/except_node.h"
#include "exec/exchange_node.h"
#include "exec/hash_join_node.h"
#include "exec/intersect_node.h"
#include "exec/merge_join_node.h"
#include "exec/merge_node.h"
#include "exec/mysql_scan_node.h"
#include "exec/odbc_scan_node.h"
#include "exec/olap_scan_node.h"
#include "exec/partitioned_aggregation_node.h"
#include "exec/repeat_node.h"
#include "exec/schema_scan_node.h"
#include "exec/select_node.h"
#include "exec/spill_sort_node.h"
#include "exec/table_function_node.h"
#include "exec/topn_node.h"
#include "exec/union_node.h"
#include "exprs/expr_context.h"
#include "odbc_scan_node.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/initial_reservations.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "util/debug_util.h"
#include "util/runtime_profile.h"
#include "vec/core/block.h"
#include "vec/exec/join/vhash_join_node.h"
#include "vec/exec/vaggregation_node.h"
#include "vec/exec/vanalytic_eval_node.h"
#include "vec/exec/vassert_num_rows_node.h"
#include "vec/exec/vcross_join_node.h"
#include "vec/exec/vempty_set_node.h"
#include "vec/exec/ves_http_scan_node.h"
#include "vec/exec/vexcept_node.h"
#include "vec/exec/vexchange_node.h"
#include "vec/exec/vintersect_node.h"
#include "vec/exec/vmysql_scan_node.h"
#include "vec/exec/vodbc_scan_node.h"
#include "vec/exec/volap_scan_node.h"
#include "vec/exec/vrepeat_node.h"
#include "vec/exec/vschema_scan_node.h"
#include "vec/exec/vselect_node.h"
#include "vec/exec/vsort_node.h"
#include "vec/exec/vtable_function_node.h"
#include "vec/exec/vunion_node.h"
#include "vec/exprs/vexpr.h"

namespace doris {

const std::string ExecNode::ROW_THROUGHPUT_COUNTER = "RowsReturnedRate";

int ExecNode::get_node_id_from_profile(RuntimeProfile* p) {
    return p->metadata();
}

ExecNode::RowBatchQueue::RowBatchQueue(int max_batches) : BlockingQueue<RowBatch*>(max_batches) {}

ExecNode::RowBatchQueue::~RowBatchQueue() {
    DCHECK(cleanup_queue_.empty());
}

void ExecNode::RowBatchQueue::AddBatch(RowBatch* batch) {
    if (!blocking_put(batch)) {
        std::lock_guard<std::mutex> lock(lock_);
        cleanup_queue_.push_back(batch);
    }
}

bool ExecNode::RowBatchQueue::AddBatchWithTimeout(RowBatch* batch, int64_t timeout_micros) {
    // return blocking_put_with_timeout(batch, timeout_micros);
    return blocking_put(batch);
}

RowBatch* ExecNode::RowBatchQueue::GetBatch() {
    RowBatch* result = nullptr;
    if (blocking_get(&result)) {
        return result;
    }
    return nullptr;
}

int ExecNode::RowBatchQueue::Cleanup() {
    int num_io_buffers = 0;

    // RowBatch* batch = nullptr;
    // while ((batch = GetBatch()) != nullptr) {
    //   num_io_buffers += batch->num_io_buffers();
    //   delete batch;
    // }

    lock_guard<std::mutex> l(lock_);
    for (std::list<RowBatch*>::iterator it = cleanup_queue_.begin(); it != cleanup_queue_.end();
         ++it) {
        // num_io_buffers += (*it)->num_io_buffers();
        delete *it;
    }
    cleanup_queue_.clear();
    return num_io_buffers;
}

ExecNode::ExecNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : _id(tnode.node_id),
          _type(tnode.node_type),
          _pool(pool),
          _tuple_ids(tnode.row_tuples),
          _row_descriptor(descs, tnode.row_tuples, tnode.nullable_tuples),
          _resource_profile(tnode.resource_profile),
          _debug_phase(TExecNodePhase::INVALID),
          _debug_action(TDebugAction::WAIT),
          _limit(tnode.limit),
          _num_rows_returned(0),
          _rows_returned_counter(nullptr),
          _rows_returned_rate(nullptr),
          _memory_used_counter(nullptr),
          _is_closed(false) {}

ExecNode::~ExecNode() {}

void ExecNode::push_down_predicate(RuntimeState* state, std::list<ExprContext*>* expr_ctxs) {
    if (_type != TPlanNodeType::AGGREGATION_NODE) {
        for (int i = 0; i < _children.size(); ++i) {
            _children[i]->push_down_predicate(state, expr_ctxs);
            if (expr_ctxs->size() == 0) {
                return;
            }
        }
    }

    std::list<ExprContext*>::iterator iter = expr_ctxs->begin();
    while (iter != expr_ctxs->end()) {
        if ((*iter)->root()->is_bound(&_tuple_ids)) {
            // LOG(INFO) << "push down success expr is " << (*iter)->debug_string()
            //          << " and node is " << debug_string();
            (*iter)->prepare(state, row_desc(), _expr_mem_tracker);
            (*iter)->open(state);
            _conjunct_ctxs.push_back(*iter);
            iter = expr_ctxs->erase(iter);
        } else {
            ++iter;
        }
    }
}

Status ExecNode::init(const TPlanNode& tnode, RuntimeState* state) {
    std::string profile;
    if (state && state->enable_vectorized_exec()) {
        profile = "V" + print_plan_node_type(tnode.node_type);
    } else {
        profile = print_plan_node_type(tnode.node_type);
    }
    init_runtime_profile(profile);

    if (tnode.__isset.vconjunct) {
        _vconjunct_ctx_ptr.reset(new doris::vectorized::VExprContext*);
        RETURN_IF_ERROR(doris::vectorized::VExpr::create_expr_tree(_pool, tnode.vconjunct,
                                                                   _vconjunct_ctx_ptr.get()));
    }
    RETURN_IF_ERROR(Expr::create_expr_trees(_pool, tnode.conjuncts, &_conjunct_ctxs));

    return Status::OK();
}

Status ExecNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::PREPARE));
    DCHECK(_runtime_profile.get() != nullptr);
    _rows_returned_counter = ADD_COUNTER(_runtime_profile, "RowsReturned", TUnit::UNIT);
    _rows_returned_rate = runtime_profile()->add_derived_counter(
            ROW_THROUGHPUT_COUNTER, TUnit::UNIT_PER_SECOND,
            std::bind<int64_t>(&RuntimeProfile::units_per_second, _rows_returned_counter,
                               runtime_profile()->total_time_counter()),
            "");
    _mem_tracker = MemTracker::create_tracker(-1, "ExecNode:" + _runtime_profile->name(),
                                              state->instance_mem_tracker(),
                                              MemTrackerLevel::VERBOSE, _runtime_profile.get());
    SCOPED_SWITCH_TASK_THREAD_LOCAL_MEM_TRACKER(_mem_tracker);
    _expr_mem_tracker = MemTracker::create_tracker(-1, "ExecNode:Exprs:" + _runtime_profile->name(),
                                                   _mem_tracker);

    if (_vconjunct_ctx_ptr) {
        RETURN_IF_ERROR((*_vconjunct_ctx_ptr)->prepare(state, row_desc(), expr_mem_tracker()));
    }
    RETURN_IF_ERROR(Expr::prepare(_conjunct_ctxs, state, row_desc(), expr_mem_tracker()));

    // TODO(zc):
    // AddExprCtxsToFree(_conjunct_ctxs);
    for (int i = 0; i < _children.size(); ++i) {
        RETURN_IF_ERROR(_children[i]->prepare(state));
    }

    return Status::OK();
}

Status ExecNode::open(RuntimeState* state) {
    SCOPED_SWITCH_TASK_THREAD_LOCAL_MEM_TRACKER(_mem_tracker);
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::OPEN));
    if (_vconjunct_ctx_ptr) {
        RETURN_IF_ERROR((*_vconjunct_ctx_ptr)->open(state));
    }
    return Expr::open(_conjunct_ctxs, state);
}

Status ExecNode::reset(RuntimeState* state) {
    _num_rows_returned = 0;
    for (int i = 0; i < _children.size(); ++i) {
        RETURN_IF_ERROR(_children[i]->reset(state));
    }
    return Status::OK();
}

Status ExecNode::collect_query_statistics(QueryStatistics* statistics) {
    DCHECK(statistics != nullptr);
    for (auto child_node : _children) {
        child_node->collect_query_statistics(statistics);
    }
    return Status::OK();
}

Status ExecNode::close(RuntimeState* state) {
    if (_is_closed) {
        return Status::OK();
    }
    _is_closed = true;
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::CLOSE));

    if (_rows_returned_counter != nullptr) {
        COUNTER_SET(_rows_returned_counter, _num_rows_returned);
    }

    Status result;
    for (int i = 0; i < _children.size(); ++i) {
        auto st = _children[i]->close(state);
        if (result.ok() && !st.ok()) {
            result = st;
        }
    }

    if (_vconjunct_ctx_ptr) (*_vconjunct_ctx_ptr)->close(state);
    Expr::close(_conjunct_ctxs, state);

    if (_buffer_pool_client.is_registered()) {
        VLOG_FILE << _id << " returning reservation " << _resource_profile.min_reservation;
        state->initial_reservations()->Return(&_buffer_pool_client,
                                              _resource_profile.min_reservation);
        state->exec_env()->buffer_pool()->DeregisterClient(&_buffer_pool_client);
    }

    return result;
}

void ExecNode::add_runtime_exec_option(const std::string& str) {
    lock_guard<mutex> l(_exec_options_lock);

    if (_runtime_exec_options.empty()) {
        _runtime_exec_options = str;
    } else {
        _runtime_exec_options.append(", ");
        _runtime_exec_options.append(str);
    }

    runtime_profile()->add_info_string("ExecOption", _runtime_exec_options);
}

Status ExecNode::create_tree(RuntimeState* state, ObjectPool* pool, const TPlan& plan,
                             const DescriptorTbl& descs, ExecNode** root) {
    if (plan.nodes.size() == 0) {
        *root = nullptr;
        return Status::OK();
    }

    int node_idx = 0;
    RETURN_IF_ERROR(create_tree_helper(state, pool, plan.nodes, descs, nullptr, &node_idx, root));

    if (node_idx + 1 != plan.nodes.size()) {
        // TODO: print thrift msg for diagnostic purposes.
        return Status::InternalError(
                "Plan tree only partially reconstructed. Not all thrift nodes were used.");
    }

    return Status::OK();
}

Status ExecNode::create_tree_helper(RuntimeState* state, ObjectPool* pool,
                                    const std::vector<TPlanNode>& tnodes,
                                    const DescriptorTbl& descs, ExecNode* parent, int* node_idx,
                                    ExecNode** root) {
    // propagate error case
    if (*node_idx >= tnodes.size()) {
        // TODO: print thrift msg
        return Status::InternalError("Failed to reconstruct plan tree from thrift.");
    }
    const TPlanNode& tnode = tnodes[*node_idx];

    int num_children = tnodes[*node_idx].num_children;
    ExecNode* node = nullptr;
    RETURN_IF_ERROR(create_node(state, pool, tnodes[*node_idx], descs, &node));

    // assert(parent != nullptr || (node_idx == 0 && root_expr != nullptr));
    if (parent != nullptr) {
        parent->_children.push_back(node);
    } else {
        *root = node;
    }

    for (int i = 0; i < num_children; i++) {
        ++*node_idx;
        RETURN_IF_ERROR(create_tree_helper(state, pool, tnodes, descs, node, node_idx, nullptr));

        // we are expecting a child, but have used all nodes
        // this means we have been given a bad tree and must fail
        if (*node_idx >= tnodes.size()) {
            // TODO: print thrift msg
            return Status::InternalError("Failed to reconstruct plan tree from thrift.");
        }
    }

    RETURN_IF_ERROR(node->init(tnode, state));

    // build up tree of profiles; add children >0 first, so that when we print
    // the profile, child 0 is printed last (makes the output more readable)
    for (int i = 1; i < node->_children.size(); ++i) {
        node->runtime_profile()->add_child(node->_children[i]->runtime_profile(), true, nullptr);
    }

    if (!node->_children.empty()) {
        node->runtime_profile()->add_child(node->_children[0]->runtime_profile(), true, nullptr);
    }

    return Status::OK();
}

Status ExecNode::create_node(RuntimeState* state, ObjectPool* pool, const TPlanNode& tnode,
                             const DescriptorTbl& descs, ExecNode** node) {
    std::stringstream error_msg;

    if (state->enable_vectorized_exec()) {
        switch (tnode.node_type) {
        case TPlanNodeType::OLAP_SCAN_NODE:
        case TPlanNodeType::ASSERT_NUM_ROWS_NODE:
        case TPlanNodeType::HASH_JOIN_NODE:
        case TPlanNodeType::AGGREGATION_NODE:
        case TPlanNodeType::UNION_NODE:
        case TPlanNodeType::CROSS_JOIN_NODE:
        case TPlanNodeType::SORT_NODE:
        case TPlanNodeType::EXCHANGE_NODE:
        case TPlanNodeType::ODBC_SCAN_NODE:
        case TPlanNodeType::MYSQL_SCAN_NODE:
        case TPlanNodeType::INTERSECT_NODE:
        case TPlanNodeType::EXCEPT_NODE:
        case TPlanNodeType::ES_HTTP_SCAN_NODE:
        case TPlanNodeType::EMPTY_SET_NODE:
        case TPlanNodeType::SCHEMA_SCAN_NODE:
        case TPlanNodeType::ANALYTIC_EVAL_NODE:
        case TPlanNodeType::SELECT_NODE:
        case TPlanNodeType::REPEAT_NODE:
        case TPlanNodeType::TABLE_FUNCTION_NODE:
            break;
        default: {
            const auto& i = _TPlanNodeType_VALUES_TO_NAMES.find(tnode.node_type);
            const char* str = "unknown node type";

            if (i != _TPlanNodeType_VALUES_TO_NAMES.end()) {
                str = i->second;
            }
            error_msg << "V" << str << " not implemented";
            return Status::InternalError(error_msg.str());
        }
        }
    }

    VLOG_CRITICAL << "tnode:\n" << apache::thrift::ThriftDebugString(tnode);
    switch (tnode.node_type) {
    case TPlanNodeType::MYSQL_SCAN_NODE:
#ifdef DORIS_WITH_MYSQL
        if (state->enable_vectorized_exec()) {
            *node = pool->add(new vectorized::VMysqlScanNode(pool, tnode, descs));
        } else
            *node = pool->add(new MysqlScanNode(pool, tnode, descs));
        return Status::OK();
#else
        return Status::InternalError(
                "Don't support MySQL table, you should rebuild Doris with WITH_MYSQL option ON");
#endif
    case TPlanNodeType::ODBC_SCAN_NODE:
        if (state->enable_vectorized_exec()) {
            *node = pool->add(new vectorized::VOdbcScanNode(pool, tnode, descs));
        } else
            *node = pool->add(new OdbcScanNode(pool, tnode, descs));
        return Status::OK();

    case TPlanNodeType::ES_SCAN_NODE:
        *node = pool->add(new EsScanNode(pool, tnode, descs));
        return Status::OK();

    case TPlanNodeType::ES_HTTP_SCAN_NODE:
        if (state->enable_vectorized_exec()) {
            *node = pool->add(new vectorized::VEsHttpScanNode(pool, tnode, descs));
        } else {
            *node = pool->add(new EsHttpScanNode(pool, tnode, descs));
        }
        return Status::OK();

    case TPlanNodeType::SCHEMA_SCAN_NODE:
        if (state->enable_vectorized_exec()) {
            *node = pool->add(new vectorized::VSchemaScanNode(pool, tnode, descs));
        } else {
            *node = pool->add(new SchemaScanNode(pool, tnode, descs));
        }
        return Status::OK();

    case TPlanNodeType::OLAP_SCAN_NODE:
        if (state->enable_vectorized_exec()) {
            *node = pool->add(new vectorized::VOlapScanNode(pool, tnode, descs));
        } else {
            *node = pool->add(new OlapScanNode(pool, tnode, descs));
        }
        return Status::OK();

    case TPlanNodeType::AGGREGATION_NODE:
        if (state->enable_vectorized_exec()) {
            *node = pool->add(new vectorized::AggregationNode(pool, tnode, descs));
        } else {
            *node = pool->add(new PartitionedAggregationNode(pool, tnode, descs));
        }
        return Status::OK();

    case TPlanNodeType::HASH_JOIN_NODE:
        if (state->enable_vectorized_exec()) {
            *node = pool->add(new vectorized::HashJoinNode(pool, tnode, descs));
        } else {
            *node = pool->add(new HashJoinNode(pool, tnode, descs));
        }
        return Status::OK();

    case TPlanNodeType::CROSS_JOIN_NODE:
        if (state->enable_vectorized_exec()) {
            *node = pool->add(new vectorized::VCrossJoinNode(pool, tnode, descs));
        } else {
            *node = pool->add(new CrossJoinNode(pool, tnode, descs));
        }
        return Status::OK();

    case TPlanNodeType::MERGE_JOIN_NODE:
        *node = pool->add(new MergeJoinNode(pool, tnode, descs));
        return Status::OK();

    case TPlanNodeType::EMPTY_SET_NODE:
        if (state->enable_vectorized_exec()) {
            *node = pool->add(new vectorized::VEmptySetNode(pool, tnode, descs));
        } else {
            *node = pool->add(new EmptySetNode(pool, tnode, descs));
        }
        return Status::OK();

    case TPlanNodeType::EXCHANGE_NODE:
        if (state->enable_vectorized_exec()) {
            *node = pool->add(new doris::vectorized::VExchangeNode(pool, tnode, descs));
        } else {
            *node = pool->add(new ExchangeNode(pool, tnode, descs));
        }
        return Status::OK();

    case TPlanNodeType::SELECT_NODE:
        if (state->enable_vectorized_exec()) {
            *node = pool->add(new doris::vectorized::VSelectNode(pool, tnode, descs));
        } else {
            *node = pool->add(new SelectNode(pool, tnode, descs));
        }
        return Status::OK();

    case TPlanNodeType::SORT_NODE:
        if (state->enable_vectorized_exec()) {
            *node = pool->add(new vectorized::VSortNode(pool, tnode, descs));
        } else {
            if (tnode.sort_node.use_top_n) {
                *node = pool->add(new TopNNode(pool, tnode, descs));
            } else {
                *node = pool->add(new SpillSortNode(pool, tnode, descs));
            }
        }

        return Status::OK();
    case TPlanNodeType::ANALYTIC_EVAL_NODE:
        if (state->enable_vectorized_exec()) {
            *node = pool->add(new vectorized::VAnalyticEvalNode(pool, tnode, descs));
        } else {
            *node = pool->add(new AnalyticEvalNode(pool, tnode, descs));
        }
        return Status::OK();

    case TPlanNodeType::MERGE_NODE:
        *node = pool->add(new MergeNode(pool, tnode, descs));
        return Status::OK();

    case TPlanNodeType::UNION_NODE:
        if (state->enable_vectorized_exec()) {
            *node = pool->add(new vectorized::VUnionNode(pool, tnode, descs));
        } else {
            *node = pool->add(new UnionNode(pool, tnode, descs));
        }
        return Status::OK();

    case TPlanNodeType::INTERSECT_NODE:
        if (state->enable_vectorized_exec()) {
            *node = pool->add(new vectorized::VIntersectNode(pool, tnode, descs));
        } else {
            *node = pool->add(new IntersectNode(pool, tnode, descs));
        }
        return Status::OK();

    case TPlanNodeType::EXCEPT_NODE:
        if (state->enable_vectorized_exec()) {
            *node = pool->add(new vectorized::VExceptNode(pool, tnode, descs));
        } else {
            *node = pool->add(new ExceptNode(pool, tnode, descs));
        }
        return Status::OK();

    case TPlanNodeType::BROKER_SCAN_NODE:
        *node = pool->add(new BrokerScanNode(pool, tnode, descs));
        return Status::OK();

    case TPlanNodeType::REPEAT_NODE:
        if (state->enable_vectorized_exec()) {
            *node = pool->add(new vectorized::VRepeatNode(pool, tnode, descs));
        } else {
            *node = pool->add(new RepeatNode(pool, tnode, descs));
        }
        return Status::OK();

    case TPlanNodeType::ASSERT_NUM_ROWS_NODE:
        if (state->enable_vectorized_exec()) {
            *node = pool->add(new vectorized::VAssertNumRowsNode(pool, tnode, descs));
        } else {
            *node = pool->add(new AssertNumRowsNode(pool, tnode, descs));
        }
        return Status::OK();

    case TPlanNodeType::TABLE_FUNCTION_NODE:
        if (state->enable_vectorized_exec()) {
            *node = pool->add(new vectorized::VTableFunctionNode(pool, tnode, descs));
        } else {
            *node = pool->add(new TableFunctionNode(pool, tnode, descs));
        }
        return Status::OK();

    default:
        map<int, const char*>::const_iterator i =
                _TPlanNodeType_VALUES_TO_NAMES.find(tnode.node_type);
        const char* str = "unknown node type";

        if (i != _TPlanNodeType_VALUES_TO_NAMES.end()) {
            str = i->second;
        }

        error_msg << str << " not implemented";
        return Status::InternalError(error_msg.str());
    }

    return Status::OK();
}

void ExecNode::set_debug_options(int node_id, TExecNodePhase::type phase, TDebugAction::type action,
                                 ExecNode* root) {
    if (root->_id == node_id) {
        root->_debug_phase = phase;
        root->_debug_action = action;
        return;
    }

    for (int i = 0; i < root->_children.size(); ++i) {
        set_debug_options(node_id, phase, action, root->_children[i]);
    }
}

std::string ExecNode::debug_string() const {
    std::stringstream out;
    this->debug_string(0, &out);
    return out.str();
}

void ExecNode::debug_string(int indentation_level, std::stringstream* out) const {
    *out << " conjuncts=" << Expr::debug_string(_conjuncts);
    *out << " id=" << _id;
    *out << " type=" << print_plan_node_type(_type);
    *out << " tuple_ids=[";
    for (auto id : _tuple_ids) {
        *out << id << ", ";
    }
    *out << "]";

    for (int i = 0; i < _children.size(); ++i) {
        *out << "\n";
        _children[i]->debug_string(indentation_level + 1, out);
    }
}

bool ExecNode::eval_conjuncts(ExprContext* const* ctxs, int num_ctxs, TupleRow* row) {
    for (int i = 0; i < num_ctxs; ++i) {
        BooleanVal v = ctxs[i]->get_boolean_val(row);
        if (v.is_null || !v.val) {
            return false;
        }
    }
    return true;
}

void ExecNode::collect_nodes(TPlanNodeType::type node_type, std::vector<ExecNode*>* nodes) {
    if (_type == node_type) {
        nodes->push_back(this);
    }

    for (int i = 0; i < _children.size(); ++i) {
        _children[i]->collect_nodes(node_type, nodes);
    }
}

void ExecNode::collect_scan_nodes(vector<ExecNode*>* nodes) {
    collect_nodes(TPlanNodeType::OLAP_SCAN_NODE, nodes);
    collect_nodes(TPlanNodeType::BROKER_SCAN_NODE, nodes);
    collect_nodes(TPlanNodeType::ES_SCAN_NODE, nodes);
    collect_nodes(TPlanNodeType::ES_HTTP_SCAN_NODE, nodes);
}

void ExecNode::try_do_aggregate_serde_improve() {
    std::vector<ExecNode*> agg_node;
    collect_nodes(TPlanNodeType::AGGREGATION_NODE, &agg_node);
    if (agg_node.size() != 1) {
        return;
    }

    if (agg_node[0]->_children.size() != 1) {
        return;
    }

    if (agg_node[0]->_children[0]->type() != TPlanNodeType::OLAP_SCAN_NODE) {
        return;
    }

    OlapScanNode* scan_node = static_cast<OlapScanNode*>(agg_node[0]->_children[0]);
    scan_node->set_no_agg_finalize();
}

void ExecNode::init_runtime_profile(const std::string& name) {
    std::stringstream ss;
    ss << name << " (id=" << _id << ")";
    _runtime_profile.reset(new RuntimeProfile(ss.str()));
    _runtime_profile->set_metadata(_id);
}

Status ExecNode::exec_debug_action(TExecNodePhase::type phase) {
    DCHECK(phase != TExecNodePhase::INVALID);

    if (_debug_phase != phase) {
        return Status::OK();
    }

    if (_debug_action == TDebugAction::FAIL) {
        return Status::InternalError("Debug Action: FAIL");
    }

    if (_debug_action == TDebugAction::WAIT) {
        while (true) {
            sleep(1);
        }
    }

    return Status::OK();
}

Status ExecNode::claim_buffer_reservation(RuntimeState* state) {
    DCHECK(!_buffer_pool_client.is_registered());
    BufferPool* buffer_pool = ExecEnv::GetInstance()->buffer_pool();
    // Check the minimum buffer size in case the minimum buffer size used by the planner
    // doesn't match this backend's.
    std::stringstream ss;
    if (_resource_profile.__isset.spillable_buffer_size &&
        _resource_profile.spillable_buffer_size < buffer_pool->min_buffer_len()) {
        ss << "Spillable buffer size for node " << _id << " of "
           << _resource_profile.spillable_buffer_size
           << "bytes is less than the minimum buffer pool buffer size of "
           << buffer_pool->min_buffer_len() << "bytes";
        return Status::InternalError(ss.str());
    }

    ss << print_plan_node_type(_type) << " id=" << _id << " ptr=" << this;
    RETURN_IF_ERROR(buffer_pool->RegisterClient(ss.str(), state->instance_buffer_reservation(),
                                                mem_tracker(), buffer_pool->GetSystemBytesLimit(),
                                                runtime_profile(), &_buffer_pool_client));

    state->initial_reservations()->Claim(&_buffer_pool_client, _resource_profile.min_reservation);
    /*
    if (debug_action_ == TDebugAction::SET_DENY_RESERVATION_PROBABILITY &&
        (debug_phase_ == TExecNodePhase::PREPARE || debug_phase_ == TExecNodePhase::OPEN)) {
       // We may not have been able to enable the debug action at the start of Prepare() or
       // Open() because the client is not registered then. Do it now to be sure that it is
       // effective.
               RETURN_IF_ERROR(EnableDenyReservationDebugAction());
    } 
*/
    return Status::OK();
}

Status ExecNode::release_unused_reservation() {
    return _buffer_pool_client.DecreaseReservationTo(_resource_profile.min_reservation);
}

void ExecNode::release_block_memory(vectorized::Block& block, uint16_t child_idx) {
    DCHECK(child_idx < _children.size());
    block.clear_column_data(child(child_idx)->row_desc().num_materialized_slots());
}

void ExecNode::reached_limit(vectorized::Block* block, bool* eos) {
    if (_limit != -1 and _num_rows_returned + block->rows() >= _limit) {
        block->set_num_rows(_limit - _num_rows_returned);
        *eos = true;
    }

    _num_rows_returned += block->rows();
    COUNTER_SET(_rows_returned_counter, _num_rows_returned);
}

/*
Status ExecNode::enable_deny_reservation_debug_action() {
  DCHECK_EQ(debug_action_, TDebugAction::SET_DENY_RESERVATION_PROBABILITY);
  DCHECK(_buffer_pool_client.is_registered());
  // Parse [0.0, 1.0] probability.
  StringParser::ParseResult parse_result;
  double probability = StringParser::StringToFloat<double>(
      debug_action_param_.c_str(), debug_action_param_.size(), &parse_result);
  if (parse_result != StringParser::PARSE_SUCCESS || probability < 0.0
      || probability > 1.0) {
    return Status::InternalError(strings::Substitute(
        "Invalid SET_DENY_RESERVATION_PROBABILITY param: '$0'", debug_action_param_));
  }
  _buffer_pool_client.SetDebugDenyIncreaseReservation(probability);
  return Status::OK()();
}
*/

Status ExecNode::QueryMaintenance(RuntimeState* state, const std::string& msg) {
    // TODO chenhao , when introduce latest AnalyticEvalNode open it
    // ScalarExprEvaluator::FreeLocalAllocations(evals_to_free_);
    return state->check_query_state(msg);
}

Status ExecNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
    return Status::NotSupported("Not Implemented get batch");
}

Status ExecNode::get_next(RuntimeState* state, vectorized::Block* block, bool* eos) {
    return Status::NotSupported("Not Implemented get block");
}

} // namespace doris
