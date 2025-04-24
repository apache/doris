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

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "common/status.h"
#include "pipeline/exec/exchange_sink_buffer.h"
#include "pipeline/exec/exchange_sink_operator.h"
#include "runtime/runtime_state.h"
#include "udf/udf.h"
#include "vec/sink/writer/vhive_utils.h"

namespace doris {

#define FAIL_IF_ERROR_OR_CATCH_EXCEPTION(stmt)                  \
    do {                                                        \
        try {                                                   \
            {                                                   \
                Status _status_ = (stmt);                       \
                if (UNLIKELY(!_status_.ok())) {                 \
                    EXPECT_TRUE(false) << _status_.to_string(); \
                }                                               \
            }                                                   \
        } catch (const doris::Exception& e) {                   \
            EXPECT_TRUE(false) << e.what();                     \
        }                                                       \
    } while (0)

class TQueryOptionsBuilder {
public:
    explicit TQueryOptionsBuilder() : _query_options() {}

    TQueryOptionsBuilder& set_batch_size(int batch_size) {
        _query_options.batch_size = batch_size;
        return *this;
    }
    TQueryOptionsBuilder& set_execution_timeout(int execution_timeout) {
        _query_options.execution_timeout = execution_timeout;
        return *this;
    }
    TQueryOptionsBuilder& set_mem_limit(int64_t mem_limit) {
        _query_options.mem_limit = mem_limit;
        return *this;
    }
    TQueryOptionsBuilder& set_query_type(TQueryType::type& query_type) {
        _query_options.query_type = query_type;
        return *this;
    }
    TQueryOptionsBuilder& set_be_exec_version(int be_exec_version) {
        _query_options.be_exec_version = be_exec_version;
        return *this;
    }
    TQueryOptionsBuilder& set_enable_local_exchange(bool enable_local_exchange) {
        _query_options.__set_enable_local_exchange(enable_local_exchange);
        return *this;
    }
    TQueryOptionsBuilder& set_enable_local_shuffle(bool enable_local_shuffle) {
        _query_options.__set_enable_local_shuffle(enable_local_shuffle);
        return *this;
    }
    TQueryOptionsBuilder& set_enable_reserve_memory(bool enable_reserve_memory) {
        _query_options.__set_enable_reserve_memory(enable_reserve_memory);
        return *this;
    }
    TQueryOptionsBuilder& set_runtime_filter_wait_infinitely(bool runtime_filter_wait_infinitely) {
        _query_options.__set_runtime_filter_wait_infinitely(runtime_filter_wait_infinitely);
        return *this;
    }
    TQueryOptionsBuilder& set_runtime_filter_max_in_num(int64_t runtime_filter_max_in_num) {
        _query_options.__set_runtime_filter_max_in_num(runtime_filter_max_in_num);
        return *this;
    }
    TQueryOptionsBuilder& set_runtime_bloom_filter_min_size(int64_t runtime_bloom_filter_min_size) {
        _query_options.__set_runtime_bloom_filter_min_size(runtime_bloom_filter_min_size);
        return *this;
    }
    TQueryOptionsBuilder& set_runtime_bloom_filter_max_size(int64_t runtime_bloom_filter_max_size) {
        _query_options.__set_runtime_bloom_filter_max_size(runtime_bloom_filter_max_size);
        return *this;
    }

    TQueryOptions& build() { return _query_options; }

    TQueryOptionsBuilder(const TQueryOptionsBuilder&) = delete;
    void operator=(const TQueryOptionsBuilder&) = delete;

private:
    TQueryOptions _query_options;
};

class TPlanNodeBuilder {
public:
    explicit TPlanNodeBuilder(TPlanNodeId node_id, TPlanNodeType::type node_type,
                              int num_children = 0, int64_t limit = -1, bool compact_data = true)
            : _plan_node() {
        _plan_node.node_id = node_id;
        _plan_node.node_type = node_type;
        _plan_node.num_children = num_children;
        _plan_node.limit = limit;
        _plan_node.compact_data = compact_data;
    }

    TPlanNodeBuilder& set_is_serial_operator(bool is_serial_operator) {
        _plan_node.__set_is_serial_operator(is_serial_operator);
        return *this;
    }
    TPlanNodeBuilder& set_exchange_node(TExchangeNode& node) {
        _plan_node.__set_exchange_node(node);
        return *this;
    }
    TPlanNodeBuilder& set_hash_join_node(THashJoinNode& node) {
        _plan_node.__set_hash_join_node(node);
        return *this;
    }
    TPlanNodeBuilder& append_row_tuples(TTupleId tuple_id, bool nullable) {
        _plan_node.row_tuples.emplace_back(tuple_id);
        _plan_node.nullable_tuples.emplace_back(nullable);
        return *this;
    }

    TPlanNodeBuilder& set_output_tuple_id(TTupleId output_tuple_id) {
        _plan_node.__set_output_tuple_id(output_tuple_id);
        return *this;
    }
    TPlanNodeBuilder& append_projections(TExpr& projections) {
        _plan_node.__isset.projections = true;
        _plan_node.projections.push_back(projections);
        return *this;
    }
    TPlanNodeBuilder& append_runtime_filters(TRuntimeFilterDesc& runtime_filter) {
        _plan_node.__isset.runtime_filters = true;
        _plan_node.runtime_filters.push_back(runtime_filter);
        return *this;
    }

    TPlanNode& build() { return _plan_node; }

    TPlanNodeBuilder(const TPlanNodeBuilder&) = delete;
    void operator=(const TPlanNodeBuilder&) = delete;

private:
    TPlanNode _plan_node;
};
class TExchangeNodeBuilder {
public:
    explicit TExchangeNodeBuilder() : _plan_node() {}

    TExchangeNodeBuilder& set_partition_type(TPartitionType::type partition_type) {
        _plan_node.__set_partition_type(partition_type);
        return *this;
    }
    TExchangeNodeBuilder& append_input_row_tuples(TTupleId tuple_id) {
        _plan_node.input_row_tuples.emplace_back(tuple_id);
        return *this;
    }
    TExchangeNode& build() { return _plan_node; }
    TExchangeNodeBuilder(const TExchangeNodeBuilder&) = delete;
    void operator=(const TExchangeNodeBuilder&) = delete;

private:
    TExchangeNode _plan_node;
};

class TDataSinkBuilder {
public:
    explicit TDataSinkBuilder(TDataSinkType::type type) : _sink() { _sink.type = type; }

    TDataSinkBuilder& set_stream_sink(TDataStreamSink& stream_sink) {
        _sink.stream_sink = stream_sink;
        return *this;
    }
    TDataSink& build() { return _sink; }

    TDataSinkBuilder(const TDataSinkBuilder&) = delete;
    void operator=(const TDataSinkBuilder&) = delete;

private:
    TDataSink _sink;
};

class TDataStreamSinkBuilder {
public:
    explicit TDataStreamSinkBuilder(TPlanNodeId dest_node_id, TDataPartition output_partition)
            : _sink() {
        _sink.dest_node_id = dest_node_id;
        _sink.output_partition = output_partition;
    }

    TDataStreamSink& build() { return _sink; }

    TDataStreamSinkBuilder(const TDataStreamSinkBuilder&) = delete;
    void operator=(const TDataStreamSinkBuilder&) = delete;

private:
    TDataStreamSink _sink;
};

class TPlanFragmentDestinationBuilder {
public:
    explicit TPlanFragmentDestinationBuilder(TUniqueId fragment_instance_id, TNetworkAddress server,
                                             TNetworkAddress brpc_server)
            : _dest() {
        _dest.fragment_instance_id = fragment_instance_id;
        _dest.server = server;
        _dest.brpc_server = brpc_server;
    }

    TPlanFragmentDestination& build() { return _dest; }

    TPlanFragmentDestinationBuilder(const TPlanFragmentDestinationBuilder&) = delete;
    void operator=(const TPlanFragmentDestinationBuilder&) = delete;

private:
    TPlanFragmentDestination _dest;
};

class TTypeDescBuilder {
public:
    explicit TTypeDescBuilder() : _desc() {
        _desc.__set_result_is_nullable(false);
        _desc.__set_is_nullable(false);
    }

    TTypeDescBuilder& set_types(TTypeNode type_node) {
        _desc.types.push_back(type_node);
        return *this;
    }

    TTypeDesc& build() { return _desc; }
    TTypeDescBuilder(const TTypeDescBuilder&) = delete;
    void operator=(const TTypeDescBuilder&) = delete;

private:
    TTypeDesc _desc;
};

class TTypeNodeBuilder {
public:
    explicit TTypeNodeBuilder() : _desc() {}

    TTypeNodeBuilder& set_type(TTypeNodeType::type type) {
        _desc.type = type;
        return *this;
    }

    TTypeNodeBuilder& set_scalar_type(TPrimitiveType::type type, int len = 0, int precision = 0,
                                      int scale = 0) {
        TScalarType scalar_type;
        scalar_type.type = type;
        scalar_type.__set_len(len);
        scalar_type.__set_precision(precision);
        scalar_type.__set_scale(scale);
        _desc.__set_scalar_type(scalar_type);
        return *this;
    }

    TTypeNode& build() { return _desc; }
    TTypeNodeBuilder(const TTypeNodeBuilder&) = delete;
    void operator=(const TTypeNodeBuilder&) = delete;

private:
    TTypeNode _desc;
};

class TDataPartitionBuilder {
public:
    explicit TDataPartitionBuilder(TPartitionType::type type) : _partition() {
        _partition.type = type;
    }

    TDataPartitionBuilder& append_partition_exprs(TExpr expr) {
        _partition.partition_exprs.push_back(expr);
        return *this;
    }
    TDataPartitionBuilder& append_partition_infos(TRangePartition info) {
        _partition.partition_infos.push_back(info);
        return *this;
    }

    TDataPartition& build() { return _partition; }
    TDataPartitionBuilder(const TDataPartitionBuilder&) = delete;
    void operator=(const TDataPartitionBuilder&) = delete;

private:
    TDataPartition _partition;
};

class THashJoinNodeBuilder {
public:
    explicit THashJoinNodeBuilder(TJoinOp::type join_op,
                                  std::vector<TEqJoinCondition>& eq_join_conjuncts)
            : _plan_node() {
        _plan_node.__set_join_op(join_op);
        _plan_node.__set_eq_join_conjuncts(eq_join_conjuncts);
    }
    explicit THashJoinNodeBuilder(TJoinOp::type join_op,
                                  std::vector<TEqJoinCondition>&& eq_join_conjuncts)
            : _plan_node() {
        _plan_node.__set_join_op(join_op);
        _plan_node.__set_eq_join_conjuncts(eq_join_conjuncts);
    }

    THashJoinNodeBuilder& set_is_broadcast_join(bool is_broadcast_join) {
        _plan_node.__set_is_broadcast_join(is_broadcast_join);
        return *this;
    }
    THashJoinNodeBuilder& append_vintermediate_tuple_id_list(TTupleId vintermediate_tuple_id_list) {
        _plan_node.vintermediate_tuple_id_list.push_back(vintermediate_tuple_id_list);
        return *this;
    }
    THashJoinNodeBuilder& set_dist_type(TJoinDistributionType::type dist_type) {
        _plan_node.__set_dist_type(dist_type);
        return *this;
    }
    THashJoinNode& build() { return _plan_node; }
    THashJoinNodeBuilder(const THashJoinNodeBuilder&) = delete;
    void operator=(const THashJoinNodeBuilder&) = delete;

private:
    THashJoinNode _plan_node;
};

class TSlotRefBuilder {
public:
    explicit TSlotRefBuilder(TSlotId slot_id, TTupleId tuple_id) : _slot_ref() {
        _slot_ref.__set_slot_id(slot_id);
        _slot_ref.__set_tuple_id(tuple_id);
    }

    TSlotRefBuilder& set_col_unique_id(int col_unique_id) {
        _slot_ref.__set_col_unique_id(col_unique_id);
        return *this;
    }
    TSlotRef& build() { return _slot_ref; }
    TSlotRefBuilder(const TSlotRefBuilder&) = delete;
    void operator=(const TSlotRefBuilder&) = delete;

private:
    TSlotRef _slot_ref;
};

class TExprNodeBuilder {
public:
    explicit TExprNodeBuilder(TExprNodeType::type node_type, TTypeDesc& type, int num_children,
                              TExprOpcode::type opcode = TExprOpcode::INVALID_OPCODE)
            : _expr_node() {
        _expr_node.__set_node_type(node_type);
        _expr_node.__set_type(type);
        _expr_node.__set_num_children(num_children);
        _expr_node.__set_opcode(opcode);
        _expr_node.__set_is_nullable(false);
    }
    explicit TExprNodeBuilder(TExprNodeType::type node_type, TTypeDesc&& type, int num_children,
                              TExprOpcode::type opcode = TExprOpcode::INVALID_OPCODE)
            : _expr_node() {
        _expr_node.__set_node_type(node_type);
        _expr_node.__set_type(type);
        _expr_node.__set_num_children(num_children);
        _expr_node.__set_opcode(opcode);
    }

    TExprNodeBuilder& set_slot_ref(TSlotRef& slot_ref) {
        _expr_node.__set_slot_ref(slot_ref);
        return *this;
    }
    TExprNode& build() { return _expr_node; }
    TExprNodeBuilder(const TExprNodeBuilder&) = delete;
    void operator=(const TExprNodeBuilder&) = delete;

private:
    TExprNode _expr_node;
};

class TExprBuilder {
public:
    explicit TExprBuilder() : _expr() {}
    TExprBuilder& append_nodes(TExprNode& node) {
        _expr.nodes.push_back(node);
        return *this;
    }
    TExpr& build() { return _expr; }
    TExprBuilder(const TExprBuilder&) = delete;
    void operator=(const TExprBuilder&) = delete;

private:
    TExpr _expr;
};

class TEqJoinConditionBuilder {
public:
    explicit TEqJoinConditionBuilder(TExpr& left, TExpr& right,
                                     TExprOpcode::type opcode = TExprOpcode::EQ)
            : _eq_conjuncts() {
        _eq_conjuncts.__set_left(left);
        _eq_conjuncts.__set_right(right);
        _eq_conjuncts.__set_opcode(opcode);
    }
    TEqJoinCondition& build() { return _eq_conjuncts; }
    TEqJoinConditionBuilder(const TEqJoinConditionBuilder&) = delete;
    void operator=(const TEqJoinConditionBuilder&) = delete;

private:
    TEqJoinCondition _eq_conjuncts;
};

class TRuntimeFilterParamsBuilder {
public:
    explicit TRuntimeFilterParamsBuilder(
            TNetworkAddress runtime_filter_merge_addr = TNetworkAddress(),
            std::map<int, std::vector<TRuntimeFilterTargetParams>> rid_to_target_param = {},
            std::map<int, int> runtime_filter_builder_num = {},
            std::map<int, std::vector<TRuntimeFilterTargetParamsV2>> rid_to_target_paramv2 = {})
            : _params() {
        _params.__set_runtime_filter_merge_addr(runtime_filter_merge_addr);
        _params.__set_rid_to_target_param(rid_to_target_param);
        _params.__set_runtime_filter_builder_num(runtime_filter_builder_num);
        _params.__set_rid_to_target_paramv2(rid_to_target_paramv2);
    }
    TRuntimeFilterParamsBuilder& add_rid_to_runtime_filter(
            int rid, TRuntimeFilterDesc param = TRuntimeFilterDesc()) {
        _params.__isset.rid_to_runtime_filter = true;
        _params.rid_to_runtime_filter[rid] = param;
        return *this;
    }
    TRuntimeFilterParamsBuilder& add_runtime_filter_builder_num(int rid, int builder_num) {
        _params.runtime_filter_builder_num[rid] = builder_num;
        return *this;
    }
    TRuntimeFilterParamsBuilder& add_rid_to_target_paramv2(
            int rid, std::vector<TRuntimeFilterTargetParamsV2> target_paramv2 = {}) {
        _params.rid_to_target_paramv2[rid] = target_paramv2;
        return *this;
    }
    TRuntimeFilterParams& build() { return _params; }
    TRuntimeFilterParamsBuilder(const TRuntimeFilterParamsBuilder&) = delete;
    void operator=(const TRuntimeFilterParamsBuilder&) = delete;

private:
    TRuntimeFilterParams _params;
};

class TRuntimeFilterDescBuilder {
public:
    static TExpr get_default_expr() {
        return TExprBuilder()
                .append_nodes(
                        TExprNodeBuilder(
                                TExprNodeType::SLOT_REF,
                                TTypeDescBuilder()
                                        .set_types(TTypeNodeBuilder()
                                                           .set_type(TTypeNodeType::SCALAR)
                                                           .set_scalar_type(TPrimitiveType::INT)
                                                           .build())
                                        .build(),
                                0)
                                .set_slot_ref(TSlotRefBuilder(0, 0).build())
                                .build())
                .build();
    }

    explicit TRuntimeFilterDescBuilder(
            int filter_id, TExpr& src_expr, int expr_order,
            std::map<TPlanNodeId, TExpr> planId_to_target_expr, bool is_broadcast_join = false,
            bool has_local_targets = true, bool has_remote_targets = false,
            TRuntimeFilterType::type type = TRuntimeFilterType::IN_OR_BLOOM) {
        _desc.__set_filter_id(filter_id);
        _desc.__set_src_expr(src_expr);
        _desc.__set_expr_order(expr_order);
        _desc.__set_planId_to_target_expr(planId_to_target_expr);
        _desc.__set_is_broadcast_join(is_broadcast_join);
        _desc.__set_has_local_targets(has_local_targets);
        _desc.__set_has_remote_targets(has_remote_targets);
        _desc.__set_type(type);
    }
    explicit TRuntimeFilterDescBuilder(
            int filter_id = 0, TExpr src_expr = get_default_expr(), int expr_order = 0,
            std::map<TPlanNodeId, TExpr> planId_to_target_expr = std::map<TPlanNodeId, TExpr> {}) {
        _desc.__set_filter_id(filter_id);
        _desc.__set_src_expr(src_expr);
        _desc.__set_expr_order(expr_order);
        _desc.__set_planId_to_target_expr(planId_to_target_expr);
        _desc.__set_is_broadcast_join(false);
        _desc.__set_has_local_targets(true);
        _desc.__set_has_remote_targets(false);
        _desc.__set_null_aware(false);
        _desc.__set_type(TRuntimeFilterType::IN_OR_BLOOM);
    }

    TRuntimeFilterDescBuilder& set_bloom_filter_size_bytes(int64_t bloom_filter_size_bytes) {
        _desc.__set_bloom_filter_size_bytes(bloom_filter_size_bytes);
        return *this;
    }

    TRuntimeFilterDescBuilder& set_is_broadcast_join(bool is_broadcast_join) {
        _desc.__set_is_broadcast_join(is_broadcast_join);
        return *this;
    }

    TRuntimeFilterDescBuilder& set_mode(bool local) {
        _desc.__set_has_local_targets(local);
        _desc.__set_has_remote_targets(!local);
        return *this;
    }

    TRuntimeFilterDescBuilder& add_planId_to_target_expr(int node_id,
                                                         TExpr expr = get_default_expr()) {
        _desc.planId_to_target_expr[node_id] = expr;
        return *this;
    }

    TRuntimeFilterDescBuilder& set_type(TRuntimeFilterType::type type) {
        _desc.__set_type(type);
        return *this;
    }

    TRuntimeFilterDescBuilder& set_build_bf_by_runtime_size(bool build_bf_by_runtime_size) {
        _desc.__set_build_bf_by_runtime_size(build_bf_by_runtime_size);
        return *this;
    }
    TRuntimeFilterDesc& build() { return _desc; }
    TRuntimeFilterDescBuilder(const TRuntimeFilterDescBuilder&) = delete;
    void operator=(const TRuntimeFilterDescBuilder&) = delete;

private:
    TRuntimeFilterDesc _desc;
};

} // namespace doris
