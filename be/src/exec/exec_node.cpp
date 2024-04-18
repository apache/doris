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
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/exec/exec-node.cpp
// and modified by Doris

#include "exec/exec_node.h"

#include <gen_cpp/Metrics_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <thrift/protocol/TDebugProtocol.h>

#include <map>
#include <memory>
#include <sstream>
#include <utility>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "common/logging.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "exec/scan_node.h"
#include "runtime/descriptors.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/runtime_state.h"
#include "util/debug_util.h"
#include "util/runtime_profile.h"
#include "util/uid_util.h"
#include "vec/columns/column_nullable.h"
#include "vec/core/block.h"
#include "vec/exec/distinct_vaggregation_node.h"
#include "vec/exec/join/vhash_join_node.h"
#include "vec/exec/join/vnested_loop_join_node.h"
#include "vec/exec/scan/group_commit_scan_node.h"
#include "vec/exec/scan/new_es_scan_node.h"
#include "vec/exec/scan/new_file_scan_node.h"
#include "vec/exec/scan/new_jdbc_scan_node.h"
#include "vec/exec/scan/new_odbc_scan_node.h"
#include "vec/exec/scan/new_olap_scan_node.h"
#include "vec/exec/scan/vmeta_scan_node.h"
#include "vec/exec/scan/vscan_node.h"
#include "vec/exec/vaggregation_node.h"
#include "vec/exec/vanalytic_eval_node.h"
#include "vec/exec/vassert_num_rows_node.h"
#include "vec/exec/vdata_gen_scan_node.h"
#include "vec/exec/vempty_set_node.h"
#include "vec/exec/vexchange_node.h"
#include "vec/exec/vmysql_scan_node.h" // IWYU pragma: keep
#include "vec/exec/vpartition_sort_node.h"
#include "vec/exec/vrepeat_node.h"
#include "vec/exec/vschema_scan_node.h"
#include "vec/exec/vselect_node.h"
#include "vec/exec/vset_operation_node.h"
#include "vec/exec/vsort_node.h"
#include "vec/exec/vtable_function_node.h"
#include "vec/exec/vunion_node.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/utils/util.hpp"

namespace doris {

const std::string ExecNode::ROW_THROUGHPUT_COUNTER = "RowsProducedRate";

ExecNode::ExecNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : _id(tnode.node_id),
          _type(tnode.node_type),
          _pool(pool),
          _tuple_ids(tnode.row_tuples),
          _row_descriptor(descs, tnode.row_tuples, tnode.nullable_tuples),
          _resource_profile(tnode.resource_profile),
          _limit(tnode.limit) {
    if (tnode.__isset.output_tuple_id) {
        _output_row_descriptor = std::make_unique<RowDescriptor>(
                descs, std::vector {tnode.output_tuple_id}, std::vector {true});
    }
    if (!tnode.intermediate_output_tuple_id_list.empty()) {
        // common subexpression elimination
        _intermediate_output_row_descriptor.reserve(tnode.intermediate_output_tuple_id_list.size());
        for (auto output_tuple_id : tnode.intermediate_output_tuple_id_list) {
            _intermediate_output_row_descriptor.push_back(
                    RowDescriptor(descs, std::vector {output_tuple_id}, std::vector {true}));
        }
    }

    _query_statistics = std::make_shared<QueryStatistics>();
}

ExecNode::~ExecNode() = default;

Status ExecNode::init(const TPlanNode& tnode, RuntimeState* state) {
    init_runtime_profile(get_name());
    if (!tnode.intermediate_output_tuple_id_list.empty()) {
        if (!tnode.__isset.output_tuple_id) {
            return Status::InternalError("no final output tuple id");
        }
        if (tnode.intermediate_output_tuple_id_list.size() !=
            tnode.intermediate_projections_list.size()) {
            return Status::InternalError(
                    "intermediate_output_tuple_id_list size:{} not match "
                    "intermediate_projections_list size:{}",
                    tnode.intermediate_output_tuple_id_list.size(),
                    tnode.intermediate_projections_list.size());
        }
    }

    if (tnode.__isset.vconjunct) {
        vectorized::VExprContextSPtr context;
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_tree(tnode.vconjunct, context));
        _conjuncts.emplace_back(context);
    } else if (tnode.__isset.conjuncts) {
        for (const auto& conjunct : tnode.conjuncts) {
            vectorized::VExprContextSPtr context;
            RETURN_IF_ERROR(vectorized::VExpr::create_expr_tree(conjunct, context));
            _conjuncts.emplace_back(context);
        }
    }

    // create the projections expr
    if (tnode.__isset.projections) {
        DCHECK(tnode.__isset.output_tuple_id);
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_trees(tnode.projections, _projections));
    }
    if (!tnode.intermediate_projections_list.empty()) {
        DCHECK(tnode.__isset.projections) << "no final projections";
        _intermediate_projections.reserve(tnode.intermediate_projections_list.size());
        for (const auto& tnode_projections : tnode.intermediate_projections_list) {
            vectorized::VExprContextSPtrs projections;
            RETURN_IF_ERROR(vectorized::VExpr::create_expr_trees(tnode_projections, projections));
            _intermediate_projections.push_back(projections);
        }
    }
    return Status::OK();
}

Status ExecNode::prepare(RuntimeState* state) {
    DCHECK(_runtime_profile.get() != nullptr);
    _exec_timer = ADD_TIMER_WITH_LEVEL(runtime_profile(), "ExecTime", 1);
    _rows_returned_counter =
            ADD_COUNTER_WITH_LEVEL(_runtime_profile, "RowsProduced", TUnit::UNIT, 1);
    _output_bytes_counter =
            ADD_COUNTER_WITH_LEVEL(_runtime_profile, "BytesProduced", TUnit::BYTES, 1);
    _block_count_counter =
            ADD_COUNTER_WITH_LEVEL(_runtime_profile, "BlocksProduced", TUnit::UNIT, 1);
    _projection_timer = ADD_TIMER(_runtime_profile, "ProjectionTime");
    _rows_returned_rate = runtime_profile()->add_derived_counter(
            ROW_THROUGHPUT_COUNTER, TUnit::UNIT_PER_SECOND,
            [this, capture0 = runtime_profile()->total_time_counter()] {
                return RuntimeProfile::units_per_second(_rows_returned_counter, capture0);
            },
            "");
    _memory_used_counter = ADD_LABEL_COUNTER(runtime_profile(), "MemoryUsage");
    _peak_memory_usage_counter = _runtime_profile->AddHighWaterMarkCounter(
            "PeakMemoryUsage", TUnit::BYTES, "MemoryUsage");
    _mem_tracker = std::make_unique<MemTracker>("ExecNode:" + _runtime_profile->name());

    for (auto& conjunct : _conjuncts) {
        RETURN_IF_ERROR(conjunct->prepare(state, intermediate_row_desc()));
    }

    for (int i = 0; i < _intermediate_projections.size(); i++) {
        RETURN_IF_ERROR(vectorized::VExpr::prepare(_intermediate_projections[i], state,
                                                   intermediate_row_desc(i)));
    }

    RETURN_IF_ERROR(vectorized::VExpr::prepare(_projections, state, projections_row_desc()));

    if (has_output_row_descriptor()) {
        RETURN_IF_ERROR(
                vectorized::VExpr::check_expr_output_type(_projections, *_output_row_descriptor));
    }

    for (auto& i : _children) {
        RETURN_IF_ERROR(i->prepare(state));
    }
    return Status::OK();
}

Status ExecNode::alloc_resource(RuntimeState* state) {
    for (auto& conjunct : _conjuncts) {
        RETURN_IF_ERROR(conjunct->open(state));
    }
    for (auto& projections : _intermediate_projections) {
        RETURN_IF_ERROR(vectorized::VExpr::open(projections, state));
    }
    RETURN_IF_ERROR(vectorized::VExpr::open(_projections, state));
    return Status::OK();
}

Status ExecNode::open(RuntimeState* state) {
    return alloc_resource(state);
}

Status ExecNode::reset(RuntimeState* state) {
    _num_rows_returned = 0;
    for (auto& i : _children) {
        RETURN_IF_ERROR(i->reset(state));
    }
    return Status::OK();
}

void ExecNode::release_resource(doris::RuntimeState* state) {
    if (!_is_resource_released) {
        if (_rows_returned_counter != nullptr) {
            COUNTER_SET(_rows_returned_counter, _num_rows_returned);
        }

        _is_resource_released = true;
    }
    if (_peak_memory_usage_counter) {
        _peak_memory_usage_counter->set(_mem_tracker->peak_consumption());
    }
}

Status ExecNode::close(RuntimeState* state) {
    if (_is_closed) {
        LOG(INFO) << "query= " << print_id(state->query_id())
                  << " fragment_instance_id=" << print_id(state->fragment_instance_id())
                  << " already closed";
        return Status::OK();
    }
    _is_closed = true;

    Status result;
    for (auto& i : _children) {
        auto st = i->close(state);
        if (result.ok() && !st.ok()) {
            result = st;
        }
    }
    release_resource(state);
    LOG(INFO) << "query= " << print_id(state->query_id())
              << ", fragment_instance_id=" << print_id(state->fragment_instance_id())
              << ", id=" << _id << " type=" << print_plan_node_type(_type) << " closed";
    return result;
}

void ExecNode::add_runtime_exec_option(const std::string& str) {
    std::lock_guard<std::mutex> l(_exec_options_lock);

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
    if (plan.nodes.empty()) {
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
                                    const std::vector<TPlanNode>& thrift_plan_nodes,
                                    const DescriptorTbl& descs, ExecNode* parent, int* node_idx,
                                    ExecNode** root) {
    // propagate error case
    if (*node_idx >= thrift_plan_nodes.size()) {
        // TODO: print thrift msg
        return Status::InternalError("Failed to reconstruct plan tree from thrift.");
    }

    const TPlanNode& cur_plan_node = thrift_plan_nodes[*node_idx];
    int num_children = cur_plan_node.num_children;

    // Step 1 Create current ExecNode according to current thrift plan node.
    ExecNode* cur_exec_node = nullptr;
    RETURN_IF_ERROR(create_node(state, pool, cur_plan_node, descs, &cur_exec_node));
    if (cur_exec_node != nullptr && state->get_query_ctx()) {
        state->get_query_ctx()->register_query_statistics(cur_exec_node->get_query_statistics());
    }

    // Step 1.1
    // Record current node if we have parent or record myself as root node.
    if (parent != nullptr) {
        parent->_children.push_back(cur_exec_node);
    } else {
        *root = cur_exec_node;
    }

    // Step 2
    // Create child ExecNode tree of current node in a recursive manner.
    for (int i = 0; i < num_children; i++) {
        ++*node_idx;
        RETURN_IF_ERROR(create_tree_helper(state, pool, thrift_plan_nodes, descs, cur_exec_node,
                                           node_idx, nullptr));

        // we are expecting a child, but have used all nodes
        // this means we have been given a bad tree and must fail
        if (*node_idx >= thrift_plan_nodes.size()) {
            // TODO: print thrift msg
            return Status::InternalError("Failed to reconstruct plan tree from thrift.");
        }
    }

    // Step 3 Init myself after sub ExecNode tree is created and initialized
    RETURN_IF_ERROR(cur_exec_node->init(cur_plan_node, state));

    // build up tree of profiles; add children >0 first, so that when we print
    // the profile, child 0 is printed last (makes the output more readable)
    for (int i = 1; i < cur_exec_node->_children.size(); ++i) {
        cur_exec_node->runtime_profile()->add_child(cur_exec_node->_children[i]->runtime_profile(),
                                                    true, nullptr);
    }

    if (!cur_exec_node->_children.empty()) {
        cur_exec_node->runtime_profile()->add_child(cur_exec_node->_children[0]->runtime_profile(),
                                                    true, nullptr);
    }

    return Status::OK();
}

// NOLINTBEGIN(readability-function-size)
Status ExecNode::create_node(RuntimeState* state, ObjectPool* pool, const TPlanNode& tnode,
                             const DescriptorTbl& descs, ExecNode** node) {
    VLOG_CRITICAL << "tnode:\n" << apache::thrift::ThriftDebugString(tnode);

    switch (tnode.node_type) {
    case TPlanNodeType::MYSQL_SCAN_NODE:
#ifdef DORIS_WITH_MYSQL
        *node = pool->add(new vectorized::VMysqlScanNode(pool, tnode, descs));
        return Status::OK();
#else
        return Status::InternalError(
                "Don't support MySQL table, you should rebuild Doris with WITH_MYSQL option ON");
#endif
    case TPlanNodeType::ODBC_SCAN_NODE:
        *node = pool->add(new vectorized::NewOdbcScanNode(pool, tnode, descs));
        return Status::OK();

    case TPlanNodeType::JDBC_SCAN_NODE:
        if (config::enable_java_support) {
            *node = pool->add(new vectorized::NewJdbcScanNode(pool, tnode, descs));
            return Status::OK();
        } else {
            return Status::InternalError(
                    "Jdbc scan node is disabled, you can change be config enable_java_support "
                    "to true and restart be.");
        }

    case TPlanNodeType::ES_HTTP_SCAN_NODE:
        *node = pool->add(new vectorized::NewEsScanNode(pool, tnode, descs));
        return Status::OK();

    case TPlanNodeType::SCHEMA_SCAN_NODE:
        *node = pool->add(new vectorized::VSchemaScanNode(pool, tnode, descs));
        return Status::OK();

    case TPlanNodeType::META_SCAN_NODE:
        *node = pool->add(new vectorized::VMetaScanNode(pool, tnode, descs));
        return Status::OK();

    case TPlanNodeType::OLAP_SCAN_NODE:
        *node = pool->add(new vectorized::NewOlapScanNode(pool, tnode, descs));
        return Status::OK();

    case TPlanNodeType::AGGREGATION_NODE:
        if (tnode.agg_node.aggregate_functions.empty() && state->enable_pipeline_exec()) {
            *node = pool->add(new vectorized::DistinctAggregationNode(pool, tnode, descs));
        } else {
            *node = pool->add(new vectorized::AggregationNode(pool, tnode, descs));
        }
        return Status::OK();

    case TPlanNodeType::HASH_JOIN_NODE:
        *node = pool->add(new vectorized::HashJoinNode(pool, tnode, descs));
        return Status::OK();

    case TPlanNodeType::CROSS_JOIN_NODE:
        *node = pool->add(new vectorized::VNestedLoopJoinNode(pool, tnode, descs));
        return Status::OK();

    case TPlanNodeType::EMPTY_SET_NODE:
        *node = pool->add(new vectorized::VEmptySetNode(pool, tnode, descs));
        return Status::OK();

    case TPlanNodeType::EXCHANGE_NODE:
        *node = pool->add(new doris::vectorized::VExchangeNode(pool, tnode, descs));
        return Status::OK();

    case TPlanNodeType::SELECT_NODE:
        *node = pool->add(new doris::vectorized::VSelectNode(pool, tnode, descs));
        return Status::OK();

    case TPlanNodeType::SORT_NODE:
        *node = pool->add(new vectorized::VSortNode(pool, tnode, descs));
        return Status::OK();

    case TPlanNodeType::ANALYTIC_EVAL_NODE:
        *node = pool->add(new vectorized::VAnalyticEvalNode(pool, tnode, descs));
        return Status::OK();

    case TPlanNodeType::MERGE_NODE:
        RETURN_ERROR_IF_NON_VEC;

    case TPlanNodeType::UNION_NODE:
        *node = pool->add(new vectorized::VUnionNode(pool, tnode, descs));
        return Status::OK();

    case TPlanNodeType::INTERSECT_NODE:
        *node = pool->add(new vectorized::VIntersectNode(pool, tnode, descs));
        return Status::OK();

    case TPlanNodeType::EXCEPT_NODE:
        *node = pool->add(new vectorized::VExceptNode(pool, tnode, descs));
        return Status::OK();

    case TPlanNodeType::FILE_SCAN_NODE:
        *node = pool->add(new vectorized::NewFileScanNode(pool, tnode, descs));
        return Status::OK();

    case TPlanNodeType::REPEAT_NODE:
        *node = pool->add(new vectorized::VRepeatNode(pool, tnode, descs));
        return Status::OK();

    case TPlanNodeType::ASSERT_NUM_ROWS_NODE:
        *node = pool->add(new vectorized::VAssertNumRowsNode(pool, tnode, descs));
        return Status::OK();

    case TPlanNodeType::TABLE_FUNCTION_NODE:
        *node = pool->add(new vectorized::VTableFunctionNode(pool, tnode, descs));
        return Status::OK();

    case TPlanNodeType::DATA_GEN_SCAN_NODE:
        *node = pool->add(new vectorized::VDataGenFunctionScanNode(pool, tnode, descs));
        return Status::OK();

    case TPlanNodeType::PARTITION_SORT_NODE:
        *node = pool->add(new vectorized::VPartitionSortNode(pool, tnode, descs));
        return Status::OK();

    case TPlanNodeType::GROUP_COMMIT_SCAN_NODE:
        *node = pool->add(new vectorized::GroupCommitScanNode(pool, tnode, descs));
        return Status::OK();

    default:
        auto i = _TPlanNodeType_VALUES_TO_NAMES.find(tnode.node_type);
        const char* str = "unknown node type";

        if (i != _TPlanNodeType_VALUES_TO_NAMES.end()) {
            str = i->second;
        }

        std::stringstream error_msg;
        error_msg << str << " not implemented";
        return Status::InternalError(error_msg.str());
    }

    return Status::OK();
}
// NOLINTEND(readability-function-size)

std::string ExecNode::debug_string() const {
    std::stringstream out;
    this->debug_string(0, &out);
    return out.str();
}

void ExecNode::debug_string(int indentation_level, std::stringstream* out) const {
    *out << " id=" << _id;
    *out << " type=" << print_plan_node_type(_type);
    *out << " tuple_ids=[";
    for (auto id : _tuple_ids) {
        *out << id << ", ";
    }
    *out << "]";

    for (auto* i : _children) {
        *out << "\n";
        i->debug_string(indentation_level + 1, out);
    }
}

void ExecNode::collect_nodes(TPlanNodeType::type node_type, std::vector<ExecNode*>* nodes) {
    if (_type == node_type) {
        nodes->push_back(this);
    }

    for (auto& i : _children) {
        i->collect_nodes(node_type, nodes);
    }
}

void ExecNode::collect_scan_nodes(vector<ExecNode*>* nodes) {
    collect_nodes(TPlanNodeType::OLAP_SCAN_NODE, nodes);
    collect_nodes(TPlanNodeType::ES_HTTP_SCAN_NODE, nodes);
    collect_nodes(TPlanNodeType::DATA_GEN_SCAN_NODE, nodes);
    collect_nodes(TPlanNodeType::FILE_SCAN_NODE, nodes);
    collect_nodes(TPlanNodeType::META_SCAN_NODE, nodes);
    collect_nodes(TPlanNodeType::JDBC_SCAN_NODE, nodes);
    collect_nodes(TPlanNodeType::ODBC_SCAN_NODE, nodes);
}

void ExecNode::init_runtime_profile(const std::string& name) {
    std::stringstream ss;
    ss << name << " (id=" << _id << ")";
    _runtime_profile = std::make_unique<RuntimeProfile>(ss.str());
    _runtime_profile->set_metadata(_id);
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

Status ExecNode::get_next(RuntimeState* state, vectorized::Block* block, bool* eos) {
    return Status::NotSupported("Not Implemented get block");
}

std::string ExecNode::get_name() {
    return "V" + print_plan_node_type(_type);
}

Status ExecNode::do_projections(vectorized::Block* origin_block, vectorized::Block* output_block) {
    SCOPED_TIMER(_exec_timer);
    SCOPED_TIMER(_projection_timer);
    const size_t rows = origin_block->rows();
    if (rows == 0) {
        return Status::OK();
    }
    vectorized::Block input_block = *origin_block;

    std::vector<int> result_column_ids;
    for (auto& projections : _intermediate_projections) {
        result_column_ids.resize(projections.size());
        for (int i = 0; i < projections.size(); i++) {
            RETURN_IF_ERROR(projections[i]->execute(&input_block, &result_column_ids[i]));
        }
        input_block.shuffle_columns(result_column_ids);
    }

    DCHECK_EQ(rows, input_block.rows());
    auto insert_column_datas = [&](auto& to, vectorized::ColumnPtr& from, size_t rows) {
        if (to->is_nullable() && !from->is_nullable()) {
            if (_keep_origin || !from->is_exclusive()) {
                auto& null_column = reinterpret_cast<vectorized::ColumnNullable&>(*to);
                null_column.get_nested_column().insert_range_from(*from, 0, rows);
                null_column.get_null_map_column().get_data().resize_fill(rows, 0);
            } else {
                to = make_nullable(from, false)->assume_mutable();
            }
        } else {
            if (_keep_origin || !from->is_exclusive()) {
                to->insert_range_from(*from, 0, rows);
            } else {
                to = from->assume_mutable();
            }
        }
    };

    using namespace vectorized;
    MutableBlock mutable_block =
            VectorizedUtils::build_mutable_mem_reuse_block(output_block, *_output_row_descriptor);

    auto& mutable_columns = mutable_block.mutable_columns();

    DCHECK_EQ(mutable_columns.size(), _projections.size());

    for (int i = 0; i < mutable_columns.size(); ++i) {
        auto result_column_id = -1;
        RETURN_IF_ERROR(_projections[i]->execute(&input_block, &result_column_id));
        auto column_ptr = input_block.get_by_position(result_column_id)
                                  .column->convert_to_full_column_if_const();
        //TODO: this is a quick fix, we need a new function like "change_to_nullable" to do it
        insert_column_datas(mutable_columns[i], column_ptr, rows);
    }
    DCHECK(mutable_block.rows() == rows);
    output_block->set_columns(std::move(mutable_columns));

    return Status::OK();
}

Status ExecNode::get_next_after_projects(
        RuntimeState* state, vectorized::Block* block, bool* eos,
        const std::function<Status(RuntimeState*, vectorized::Block*, bool*)>& func,
        bool clear_data) {
    if (_output_row_descriptor) {
        if (clear_data) {
            clear_origin_block();
        }
        RETURN_IF_ERROR(func(state, &_origin_block, eos));
        RETURN_IF_ERROR(do_projections(&_origin_block, block));
    } else {
        RETURN_IF_ERROR(func(state, block, eos));
    }
    _peak_memory_usage_counter->set(_mem_tracker->peak_consumption());

    if (block && !block->empty()) {
        COUNTER_UPDATE(_output_bytes_counter, block->allocated_bytes());
        COUNTER_UPDATE(_block_count_counter, 1);
    }
    return Status::OK();
}

Status ExecNode::sink(RuntimeState* state, vectorized::Block* input_block, bool eos) {
    return Status::NotSupported("{} not implements sink", get_name());
}

} // namespace doris
