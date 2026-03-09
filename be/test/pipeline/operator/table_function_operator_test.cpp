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

#include "pipeline/exec/table_function_operator.h"

#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Types_types.h>
#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "operator_helper.h"
#include "testutil/column_helper.h"
#include "testutil/creators.h"
#include "testutil/mock/mock_descriptors.h"
#include "testutil/mock/mock_literal_expr.h"
#include "testutil/mock/mock_operators.h"
#include "testutil/mock/mock_slot_ref.h"
#include "vec/core/block.h"
namespace doris::pipeline {

using namespace vectorized;

class MockTableFunctionChildOperator : public OperatorXBase {
public:
    Status get_block_after_projects(RuntimeState* state, vectorized::Block* block,
                                    bool* eos) override {
        return Status::OK();
    }

    Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos) override {
        return Status::OK();
    }
    Status setup_local_state(RuntimeState* state, LocalStateInfo& info) override {
        return Status::OK();
    }

    const RowDescriptor& row_desc() const override { return *_mock_row_desc; }

private:
    std::unique_ptr<MockRowDescriptor> _mock_row_desc;
};

struct MockTableFunctionOperatorX : public TableFunctionOperatorX {
    MockTableFunctionOperatorX() = default;
    RowDescriptor& row_descriptor() override { return *_mock_row_descriptor; }
    std::unique_ptr<MockRowDescriptor> _mock_row_descriptor;
};

class MockTableFunction : public TableFunction {
public:
    MockTableFunction() = default;
    ~MockTableFunction() override = default;

    Status process_init(Block* block, RuntimeState* state) override {
        std::cout << "process_init" << std::endl;
        return Status::OK();
    }
    void process_row(size_t row_idx) override {
        std::cout << "process_row" << std::endl;

        _cur_size = 5;
    }
    void process_close() override {}
    void get_same_many_values(MutableColumnPtr& column, int length) override {
        std::cout << "get_same_many_values" << std::endl;
        column->insert_many_defaults(length);
    }
    int get_value(MutableColumnPtr& column, int max_step) override {
        std::cout << "get_value" << std::endl;
        max_step = std::min(max_step, (int)(_cur_size - _cur_offset));
        column->insert_many_defaults(max_step);
        forward(max_step);
        return max_step;
    }
};

struct MockTableFunctionLocalState : TableFunctionLocalState {
    MockTableFunctionLocalState(RuntimeState* state, OperatorXBase* parent)
            : TableFunctionLocalState(state, parent) {}

    Status _clone_table_function(RuntimeState* state) override {
        auto& p = _parent->cast<TableFunctionOperatorX>();
        _vfn_ctxs.resize(p._vfn_ctxs.size());
        for (size_t i = 0; i < _vfn_ctxs.size(); i++) {
            RETURN_IF_ERROR(p._vfn_ctxs[i]->clone(state, _vfn_ctxs[i]));
            vectorized::TableFunction* fn = p._fns[i];
            fn->set_expr_context(_vfn_ctxs[i]);
            _fns.push_back(fn);
        }
        return Status::OK();
    }
};

struct TableFunctionOperatorTest : public ::testing::Test {
    void SetUp() override {
        state = std::make_shared<MockRuntimeState>();
        op = std::make_shared<MockTableFunctionOperatorX>();
        child_op = std::make_shared<MockTableFunctionChildOperator>();
        op->_child = child_op;
        fns.clear();
    }

    std::shared_ptr<MockTableFunctionOperatorX> op;
    std::shared_ptr<MockTableFunctionChildOperator> child_op;

    std::shared_ptr<MockRuntimeState> state;

    RuntimeProfile profile {""};

    std::unique_ptr<MockTableFunctionLocalState> local_state_uptr;

    MockTableFunctionLocalState* local_state;

    std::vector<std::shared_ptr<TableFunction>> fns;

    ObjectPool pool;
};

TEST_F(TableFunctionOperatorTest, single_fn_test) {
    {
        op->_vfn_ctxs =
                MockSlotRef::create_mock_contexts(DataTypes {std::make_shared<DataTypeInt32>()});
        auto fn = std::make_shared<MockTableFunction>();
        fns.push_back(fn);
        op->_fns.push_back(fn.get());
        op->_output_slot_ids.push_back(true);
        child_op->_mock_row_desc.reset(new MockRowDescriptor {{}, &pool});
        op->_mock_row_descriptor.reset(
                new MockRowDescriptor {{std::make_shared<vectorized::DataTypeInt32>()}, &pool});
        op->_fn_num = 1;
        EXPECT_TRUE(op->prepare(state.get()));

        local_state_uptr = std::make_unique<MockTableFunctionLocalState>(state.get(), op.get());
        local_state = local_state_uptr.get();
        LocalStateInfo info {.parent_profile = &profile,
                             .scan_ranges = {},
                             .shared_state = nullptr,
                             .shared_state_map = {},
                             .task_idx = 0};
        EXPECT_TRUE(local_state->init(state.get(), info));
        state->resize_op_id_to_local_state(-100);
        state->emplace_local_state(op->operator_id(), std::move(local_state_uptr));
        EXPECT_TRUE(local_state->open(state.get()));
    }

    {
        *local_state->_child_block = ColumnHelper::create_block<DataTypeInt32>({1});
        auto st = op->push(state.get(), local_state->_child_block.get(), true);
        EXPECT_TRUE(st) << st.msg();
    }
    {
        Block block;
        bool eos = false;
        auto st = op->pull(state.get(), &block, &eos);
        EXPECT_TRUE(st) << st.msg();
        std::cout << block.dump_data() << std::endl;
        std::cout << eos << std::endl;
        EXPECT_TRUE(ColumnHelper::block_equal(
                block, ColumnHelper::create_block<DataTypeInt32>({0, 0, 0, 0, 0})));
    }
}

TEST_F(TableFunctionOperatorTest, single_fn_test2) {
    {
        op->_vfn_ctxs =
                MockSlotRef::create_mock_contexts(DataTypes {std::make_shared<DataTypeInt32>()});
        auto fn = std::make_shared<MockTableFunction>();
        fns.push_back(fn);
        op->_fns.push_back(fn.get());
        op->_output_slot_ids.push_back(true);
        child_op->_mock_row_desc.reset(
                new MockRowDescriptor {{std::make_shared<vectorized::DataTypeInt32>()}, &pool});
        op->_mock_row_descriptor.reset(
                new MockRowDescriptor {{std::make_shared<vectorized::DataTypeInt32>(),
                                        std::make_shared<vectorized::DataTypeInt32>()},
                                       &pool});
        op->_fn_num = 1;
        EXPECT_TRUE(op->prepare(state.get()));

        local_state_uptr = std::make_unique<MockTableFunctionLocalState>(state.get(), op.get());
        local_state = local_state_uptr.get();
        LocalStateInfo info {.parent_profile = &profile,
                             .scan_ranges = {},
                             .shared_state = nullptr,
                             .shared_state_map = {},
                             .task_idx = 0};
        EXPECT_TRUE(local_state->init(state.get(), info));
        state->resize_op_id_to_local_state(-100);
        state->emplace_local_state(op->operator_id(), std::move(local_state_uptr));
        EXPECT_TRUE(local_state->open(state.get()));
    }

    {
        Block block = ColumnHelper::create_block<DataTypeInt32>({1});
        *local_state->_child_block = ColumnHelper::create_block<DataTypeInt32>({1});
        auto st = op->push(state.get(), local_state->_child_block.get(), true);
        EXPECT_TRUE(st) << st.msg();
    }
    {
        Block block;
        bool eos = false;
        auto st = op->pull(state.get(), &block, &eos);
        EXPECT_TRUE(st) << st.msg();
        std::cout << block.dump_data() << std::endl;
        std::cout << eos << std::endl;
        EXPECT_TRUE(ColumnHelper::block_equal(block, ColumnHelper::create_block<DataTypeInt32>(
                                                             {1, 1, 1, 1, 1}, {0, 0, 0, 0, 0})));
    }
}

TEST_F(TableFunctionOperatorTest, single_two_test) {
    {
        op->_vfn_ctxs = MockSlotRef::create_mock_contexts(
                DataTypes {std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeInt32>()});
        {
            auto fn = std::make_shared<MockTableFunction>();
            fns.push_back(fn);
            op->_fns.push_back(fn.get());
            op->_output_slot_ids.push_back(true);
        }
        {
            auto fn = std::make_shared<MockTableFunction>();
            fns.push_back(fn);
            op->_fns.push_back(fn.get());
            op->_output_slot_ids.push_back(true);
        }

        child_op->_mock_row_desc.reset(new MockRowDescriptor {{}, &pool});
        op->_mock_row_descriptor.reset(
                new MockRowDescriptor {{std::make_shared<vectorized::DataTypeInt32>(),
                                        std::make_shared<vectorized::DataTypeInt32>()},
                                       &pool});
        op->_fn_num = 2;
        EXPECT_TRUE(op->prepare(state.get()));

        local_state_uptr = std::make_unique<MockTableFunctionLocalState>(state.get(), op.get());
        local_state = local_state_uptr.get();
        LocalStateInfo info {.parent_profile = &profile,
                             .scan_ranges = {},
                             .shared_state = nullptr,
                             .shared_state_map = {},
                             .task_idx = 0};
        EXPECT_TRUE(local_state->init(state.get(), info));
        state->resize_op_id_to_local_state(-100);
        state->emplace_local_state(op->operator_id(), std::move(local_state_uptr));
        EXPECT_TRUE(local_state->open(state.get()));
    }

    {
        Block block = ColumnHelper::create_block<DataTypeInt32>({1});
        *local_state->_child_block = ColumnHelper::create_block<DataTypeInt32>({1});
        auto st = op->push(state.get(), local_state->_child_block.get(), true);
        EXPECT_TRUE(st) << st.msg();
    }
    {
        Block block;
        bool eos = false;
        auto st = op->pull(state.get(), &block, &eos);
        EXPECT_TRUE(st) << st.msg();
        std::cout << block.dump_data() << std::endl;
        std::cout << eos << std::endl;
        EXPECT_TRUE(ColumnHelper::block_equal(
                block,
                ColumnHelper::create_block<DataTypeInt32>(
                        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                         0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})));
    }
}

TEST_F(TableFunctionOperatorTest, single_two_eos_test) {
    {
        op->_vfn_ctxs = MockSlotRef::create_mock_contexts(
                DataTypes {std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeInt32>()});
        {
            auto fn = std::make_shared<MockTableFunction>();
            fns.push_back(fn);
            op->_fns.push_back(fn.get());
            op->_output_slot_ids.push_back(true);
        }
        {
            auto fn = std::make_shared<MockTableFunction>();
            fns.push_back(fn);
            op->_fns.push_back(fn.get());
            op->_output_slot_ids.push_back(true);
        }

        child_op->_mock_row_desc.reset(new MockRowDescriptor {{}, &pool});
        op->_mock_row_descriptor.reset(
                new MockRowDescriptor {{std::make_shared<vectorized::DataTypeInt32>(),
                                        std::make_shared<vectorized::DataTypeInt32>()},
                                       &pool});
        op->_fn_num = 2;
        EXPECT_TRUE(op->prepare(state.get()));

        local_state_uptr = std::make_unique<MockTableFunctionLocalState>(state.get(), op.get());
        local_state = local_state_uptr.get();
        LocalStateInfo info {.parent_profile = &profile,
                             .scan_ranges = {},
                             .shared_state = nullptr,
                             .shared_state_map = {},
                             .task_idx = 0};
        EXPECT_TRUE(local_state->init(state.get(), info));
        state->resize_op_id_to_local_state(-100);
        state->emplace_local_state(op->operator_id(), std::move(local_state_uptr));
        EXPECT_TRUE(local_state->open(state.get()));
    }

    {
        Block block = ColumnHelper::create_block<DataTypeInt32>({1});
        *local_state->_child_block = ColumnHelper::create_block<DataTypeInt32>({1});
        auto st = op->push(state.get(), local_state->_child_block.get(), false);
        EXPECT_TRUE(st) << st.msg();
    }
    {
        Block block;
        bool eos = false;
        auto st = op->pull(state.get(), &block, &eos);
        EXPECT_TRUE(st) << st.msg();
        std::cout << block.dump_data() << std::endl;
        std::cout << eos << std::endl;
        EXPECT_TRUE(ColumnHelper::block_equal(
                block,
                ColumnHelper::create_block<DataTypeInt32>(
                        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                         0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})));
    }

    {
        Block block = ColumnHelper::create_block<DataTypeInt32>({1});
        *local_state->_child_block = ColumnHelper::create_block<DataTypeInt32>({1});
        local_state->_child_eos = true;
        auto st = op->push(state.get(), local_state->_child_block.get(), true);
        EXPECT_TRUE(st) << st.msg();
    }
    {
        Block block;
        bool eos = false;
        auto st = op->pull(state.get(), &block, &eos);
        EXPECT_TRUE(st) << st.msg();
        EXPECT_TRUE(eos);
    }
}

struct UnnestTest : public ::testing::Test {
    void SetUp() override {
        type_node_bool.type = TTypeNodeType::SCALAR;
        type_node_bool.scalar_type.type = TPrimitiveType::BOOLEAN;
        type_node_bool.__isset.scalar_type = true;

        type_node_int.type = TTypeNodeType::SCALAR;
        type_node_int.scalar_type.type = TPrimitiveType::INT;
        type_node_int.__isset.scalar_type = true;

        type_node_arr.type = TTypeNodeType::ARRAY;
        type_node_arr.contains_null = true;
        type_node_arr.__isset.contains_nulls = true;
        type_node_arr.contains_nulls.emplace_back(true);

        type_desc_int.types.emplace_back(type_node_int);
        type_desc_int.byte_size = -1;

        type_desc_array_int.types.emplace_back(type_node_arr);
        type_desc_array_int.types.emplace_back(type_node_int);
        type_desc_array_int.byte_size = -1;

        type_desc_boolean.types.emplace_back(type_node_bool);
        type_desc_boolean.byte_size = -1;

        // input block tuple
        tuple0.id = tuple_id++;
        tuple0.byteSize = 0;
        tuple0.numNullBytes = 0;
        tuple0.tableId = 0;

        // unnested output tuple
        tuple1.id = tuple_id++;
        tuple1.byteSize = 0;
        tuple1.numNullBytes = 0;

        // projection tuple for unnest
        tuple2.id = tuple_id++;
        tuple2.byteSize = 0;
        tuple2.numNullBytes = 0;

        // SlotRefs for input slots
        // `id` column, type int
        slot_ref_id.slot_id = slot_id++;
        slot_ref_id.tuple_id = tuple0.id;
        slot_ref_id.col_unique_id = col_unique_id++;
        slot_ref_id.is_virtual_slot = false;

        // `tags` column, type array<int>
        slot_ref_tags.slot_id = slot_id++;
        slot_ref_tags.tuple_id = tuple0.id;
        slot_ref_tags.col_unique_id = col_unique_id++;
        slot_ref_tags.is_virtual_slot = false;

        // unnested output slot, tag
        slot_ref_unnest_tag.slot_id = slot_id++;
        slot_ref_unnest_tag.tuple_id = tuple1.id;
        slot_ref_unnest_tag.col_unique_id = -1;
        slot_ref_unnest_tag.is_virtual_slot = false;

        slot_desc_id.id = slot_ref_id.slot_id;
        slot_desc_id.parent = tuple0.id;
        slot_desc_id.slotType = type_desc_int;
        slot_desc_id.columnPos = -1;
        slot_desc_id.byteOffset = 0;
        slot_desc_id.nullIndicatorByte = 0;
        slot_desc_id.nullIndicatorBit = 0;
        slot_desc_id.colName = "id";
        slot_desc_id.slotIdx = -1;
        slot_desc_id.isMaterialized = true;
        slot_desc_id.col_unique_id = slot_ref_id.col_unique_id;
        slot_desc_id.is_key = true;
        slot_desc_id.need_materialize = true;
        slot_desc_id.is_auto_increment = false;
        slot_desc_id.primitive_type = TPrimitiveType::INT;

        slot_desc_tags.id = slot_ref_tags.slot_id;
        slot_desc_tags.parent = tuple0.id;
        slot_desc_tags.slotType = type_desc_array_int;
        slot_desc_tags.columnPos = -1;
        slot_desc_tags.byteOffset = 0;
        slot_desc_tags.nullIndicatorByte = 0;
        slot_desc_tags.nullIndicatorBit = 0;
        slot_desc_tags.colName = "tags";
        slot_desc_tags.slotIdx = -1;
        slot_desc_tags.isMaterialized = true;
        slot_desc_tags.col_unique_id = slot_ref_tags.col_unique_id;
        slot_desc_tags.is_key = false;
        slot_desc_tags.need_materialize = true;
        slot_desc_tags.is_auto_increment = false;
        slot_desc_tags.primitive_type = TPrimitiveType::ARRAY;

        slot_desc_unnest_tag.id = slot_ref_unnest_tag.slot_id;
        slot_desc_unnest_tag.parent = tuple1.id;
        slot_desc_unnest_tag.slotType = type_desc_int;
        slot_desc_unnest_tag.columnPos = -1;
        slot_desc_unnest_tag.byteOffset = 0;
        slot_desc_unnest_tag.nullIndicatorByte = 0;
        slot_desc_unnest_tag.nullIndicatorBit = 0;
        slot_desc_unnest_tag.colName = "tag#3";
        slot_desc_unnest_tag.slotIdx = -1;
        slot_desc_unnest_tag.isMaterialized = true;
        slot_desc_unnest_tag.col_unique_id = -1;
        slot_desc_unnest_tag.is_key = false;
        slot_desc_unnest_tag.need_materialize = true;
        slot_desc_unnest_tag.is_auto_increment = false;
        slot_desc_unnest_tag.primitive_type = TPrimitiveType::INT;

        slot_desc_id_2 = slot_desc_id;
        slot_desc_id_2.id = slot_id++;
        slot_desc_id_2.parent = tuple2.id;

        slot_desc_unnest_tag2 = slot_desc_unnest_tag;
        slot_desc_unnest_tag2.id = slot_id++;
        slot_desc_unnest_tag2.parent = tuple2.id;
    }
    void setup_exec_env() {
        runtime_state = std::make_unique<MockRuntimeState>();
        obj_pool = std::make_unique<ObjectPool>();
        query_ctx = generate_one_query();
        runtime_profile = std::make_shared<RuntimeProfile>("test");
        runtime_state->batsh_size = 5;
        runtime_state->_query_ctx = query_ctx.get();
        runtime_state->_query_id = query_ctx->query_id();
        runtime_state->resize_op_id_to_local_state(-100);
        runtime_state->set_max_operator_id(-100);
    }

    void setup_plan_node(TPlanNode& tplan_node_table_function_node, bool outer) {
        // Set main TPlanNode properties
        tplan_node_table_function_node.node_id = 0;
        tplan_node_table_function_node.node_type = TPlanNodeType::TABLE_FUNCTION_NODE;
        tplan_node_table_function_node.num_children = 1;
        tplan_node_table_function_node.limit = -1;

        tplan_node_table_function_node.row_tuples.push_back(tuple0.id);
        tplan_node_table_function_node.row_tuples.push_back(tuple1.id);
        tplan_node_table_function_node.nullable_tuples.push_back(false);
        tplan_node_table_function_node.nullable_tuples.push_back(false);
        tplan_node_table_function_node.compact_data = false;
        tplan_node_table_function_node.is_serial_operator = false;

        // setup table function node
        tplan_node_table_function_node.__set_table_function_node(TTableFunctionNode());

        // setup fnCallExprList of table function node
        TExpr fn_expr;

        fn_expr.nodes.emplace_back();
        fn_expr.nodes[0].node_type = TExprNodeType::FUNCTION_CALL;
        fn_expr.nodes[0].type.types.emplace_back(type_node_int);
        fn_expr.nodes[0].num_children = 1;

        // setup TFunction of table function node
        fn_expr.nodes[0].__set_fn(TFunction());
        fn_expr.nodes[0].fn.__set_name(TFunctionName());
        fn_expr.nodes[0].fn.name.function_name = outer ? "explode_outer" : "explode";

        fn_expr.nodes[0].fn.arg_types.emplace_back(type_desc_array_int);
        fn_expr.nodes[0].fn.ret_type.types.emplace_back(type_node_int);

        fn_expr.nodes[0].fn.has_var_args = false;
        fn_expr.nodes[0].fn.signature = outer ? "explode_outer(array<int>)" : "explode(array<int>)";
        fn_expr.nodes[0].fn.__set_scalar_fn(TScalarFunction());
        fn_expr.nodes[0].fn.scalar_fn.symbol = "";
        fn_expr.nodes[0].fn.id = 0;
        fn_expr.nodes[0].fn.vectorized = true;
        fn_expr.nodes[0].fn.is_udtf_function = false;
        fn_expr.nodes[0].fn.is_static_load = false;
        fn_expr.nodes[0].fn.expiration_time = 360;
        fn_expr.nodes[0].is_nullable = true;

        // explode input slot ref: array<int>
        fn_expr.nodes.emplace_back();
        fn_expr.nodes[1].node_type = TExprNodeType::SLOT_REF;
        fn_expr.nodes[1].type = type_desc_array_int;
        fn_expr.nodes[1].num_children = 0;
        fn_expr.nodes[1].__set_slot_ref(TSlotRef());
        fn_expr.nodes[1].slot_ref = slot_ref_tags;
        fn_expr.nodes[1].output_scale = -1;
        fn_expr.nodes[1].is_nullable = true;
        fn_expr.nodes[1].label = "tags";

        tplan_node_table_function_node.table_function_node.fnCallExprList.push_back(fn_expr);

        // Set output slot IDs
        tplan_node_table_function_node.table_function_node.outputSlotIds.push_back(slot_desc_id.id);
        tplan_node_table_function_node.table_function_node.outputSlotIds.push_back(
                slot_desc_unnest_tag.id);

        // Set expand conjuncts: tag = id
        TExpr expand_expr;
        {
            TExprNode expr_node;
            expr_node.node_type = TExprNodeType::BINARY_PRED;
            expr_node.type = type_desc_boolean;

            expr_node.opcode = TExprOpcode::EQ;
            expr_node.num_children = 2;
            expr_node.output_scale = -1;

            expr_node.__set_fn(TFunction());
            expr_node.fn.__set_name(TFunctionName());
            expr_node.fn.name.function_name = "eq";
            expr_node.fn.binary_type = TFunctionBinaryType::BUILTIN;
            expr_node.fn.arg_types.push_back(type_desc_int);
            expr_node.fn.arg_types.push_back(type_desc_int);
            expr_node.fn.ret_type = type_desc_boolean;
            expr_node.fn.has_var_args = false;
            expr_node.fn.signature = "eq(int, int)";
            expr_node.fn.id = 0;
            expr_node.fn.is_udtf_function = false;

            expr_node.child_type = TPrimitiveType::INT;
            expr_node.is_nullable = true;

            expand_expr.nodes.emplace_back(expr_node);
        }
        {
            TExprNode expr_node;
            expr_node.node_type = TExprNodeType::SLOT_REF;
            expr_node.type = type_desc_int;
            expr_node.num_children = 0;

            expr_node.slot_ref = slot_ref_unnest_tag;
            expr_node.output_scale = -1;
            expr_node.is_nullable = true;
            expr_node.label = "tag";

            expand_expr.nodes.emplace_back(expr_node);
        }
        {
            TExprNode expr_node;
            expr_node.node_type = TExprNodeType::SLOT_REF;
            expr_node.type = type_desc_int;
            expr_node.num_children = 0;

            expr_node.slot_ref = slot_ref_id;
            expr_node.output_scale = -1;
            expr_node.is_nullable = true;
            expr_node.label = "id";

            expand_expr.nodes.emplace_back(expr_node);
        }
        tplan_node_table_function_node.table_function_node.expand_conjuncts.push_back(expand_expr);
        // end of TTableFunctionNode setup

        // Set projections
        TExpr texpr_proj0;
        texpr_proj0.nodes.emplace_back();
        texpr_proj0.nodes[0].node_type = TExprNodeType::SLOT_REF;
        texpr_proj0.nodes[0].type = type_desc_int;
        texpr_proj0.nodes[0].num_children = 0;
        texpr_proj0.nodes[0].__set_slot_ref(TSlotRef());
        texpr_proj0.nodes[0].slot_ref = slot_ref_id;
        texpr_proj0.nodes[0].output_scale = -1;
        texpr_proj0.nodes[0].is_nullable = true;
        texpr_proj0.nodes[0].label = "id";

        TExpr texpr_proj1;
        texpr_proj1.nodes.emplace_back();
        texpr_proj1.nodes[0].node_type = TExprNodeType::SLOT_REF;
        texpr_proj1.nodes[0].type = type_desc_int;
        texpr_proj1.nodes[0].num_children = 0;
        texpr_proj1.nodes[0].__set_slot_ref(TSlotRef());
        texpr_proj1.nodes[0].slot_ref = slot_ref_unnest_tag;
        texpr_proj1.nodes[0].output_scale = -1;
        texpr_proj1.nodes[0].is_nullable = true;
        texpr_proj1.nodes[0].label = "tag";

        tplan_node_table_function_node.projections.push_back(texpr_proj0);
        tplan_node_table_function_node.projections.push_back(texpr_proj1);
        tplan_node_table_function_node.output_tuple_id = tuple2.id;
        tplan_node_table_function_node.nereids_id = 144;
        // end of tplan_node_table_function_node
    }

    std::shared_ptr<TableFunctionOperatorX> create_test_operators(
            DescriptorTbl* desc_tbl, const TPlanNode& tplan_node_table_function_node) {
        runtime_state->set_desc_tbl(desc_tbl);
        RowDescriptor mock_row_desc(runtime_state->desc_tbl(), {tuple0.id});
        LocalStateInfo local_state_info {.parent_profile = runtime_profile.get(),
                                         .scan_ranges = {},
                                         .shared_state = nullptr,
                                         .shared_state_map = {},
                                         .task_idx = 0};
        PipelineXLocalStateBase* local_state = nullptr;
        std::shared_ptr<TableFunctionOperatorX> table_func_op;
        std::shared_ptr<MockSourceOperator> mock_source_operator;

        table_func_op = std::make_shared<TableFunctionOperatorX>(
                obj_pool.get(), tplan_node_table_function_node, 0, *desc_tbl);

        mock_source_operator = std::make_shared<MockSourceOperator>();
        mock_source_operator->_row_descriptor = mock_row_desc;
        EXPECT_TRUE(table_func_op->set_child(mock_source_operator));

        auto st = table_func_op->init(tplan_node_table_function_node, runtime_state.get());
        EXPECT_TRUE(st.ok());
        st = table_func_op->prepare(runtime_state.get());
        EXPECT_TRUE(st.ok());

        st = table_func_op->setup_local_state(runtime_state.get(), local_state_info);
        EXPECT_TRUE(st.ok()) << "setup_local_state failed: " << st.to_string();

        local_state = runtime_state->get_local_state(table_func_op->operator_id());
        EXPECT_TRUE(local_state);

        st = local_state->open(runtime_state.get());
        EXPECT_TRUE(st.ok()) << "open failed: " << st.to_string();

        return table_func_op;
    }

    TTypeNode type_node_bool;
    TTypeNode type_node_int;
    TTypeNode type_node_arr;
    TTypeDesc type_desc_int;
    TTypeDesc type_desc_array_int;
    TTypeDesc type_desc_boolean;

    ::doris::TSlotId slot_id = 0;
    ::doris::TTupleId tuple_id = 0;
    int32_t col_unique_id = 0;

    TTupleDescriptor tuple0;
    TTupleDescriptor tuple1;
    TTupleDescriptor tuple2;

    TSlotRef slot_ref_id;
    TSlotRef slot_ref_tags;
    TSlotRef slot_ref_unnest_tag;

    TSlotDescriptor slot_desc_id;
    TSlotDescriptor slot_desc_tags;
    TSlotDescriptor slot_desc_unnest_tag;
    TSlotDescriptor slot_desc_id_2;
    TSlotDescriptor slot_desc_unnest_tag2;

    std::unique_ptr<MockRuntimeState> runtime_state;
    std::unique_ptr<ObjectPool> obj_pool;
    std::shared_ptr<QueryContext> query_ctx;
    std::shared_ptr<RuntimeProfile> runtime_profile;
};

TEST_F(UnnestTest, inner) {
    TDescriptorTable desc_table;
    desc_table.tupleDescriptors.push_back(tuple0);
    desc_table.tupleDescriptors.push_back(tuple1);
    desc_table.tupleDescriptors.push_back(tuple2);

    desc_table.slotDescriptors.push_back(slot_desc_id);
    desc_table.slotDescriptors.push_back(slot_desc_tags);
    desc_table.slotDescriptors.push_back(slot_desc_unnest_tag);
    desc_table.slotDescriptors.push_back(slot_desc_id_2);
    desc_table.slotDescriptors.push_back(slot_desc_unnest_tag2);

    setup_exec_env();

    // Set main TPlanNode properties
    TPlanNode tplan_node_table_function_node;
    setup_plan_node(tplan_node_table_function_node, false);

    DescriptorTbl* desc_tbl;
    auto st = DescriptorTbl::create(obj_pool.get(), desc_table, &desc_tbl);
    EXPECT_TRUE(st.ok());

    vectorized::DataTypePtr data_type_int(std::make_shared<vectorized::DataTypeInt32>());
    auto data_type_int_nullable = make_nullable(data_type_int);
    vectorized::DataTypePtr data_type_array_type(
            std::make_shared<vectorized::DataTypeArray>(data_type_int_nullable));
    auto data_type_array_type_nullable = make_nullable(data_type_array_type);

    auto build_input_block = [&](const std::vector<int32_t>& ids,
                                 const std::vector<std::vector<int32_t>>& array_rows) {
        auto result_block = std::make_unique<Block>();
        auto id_column = ColumnInt32::create();
        for (const auto& id : ids) {
            id_column->insert_data((const char*)(&id), 0);
        }
        result_block->insert(ColumnWithTypeAndName(make_nullable(std::move(id_column)),
                                                   data_type_int_nullable, "id"));

        auto arr_data_column = ColumnInt32::create();
        auto arr_offsets_column = ColumnOffset64::create();
        std::vector<ColumnArray::Offset64> offsets;
        offsets.push_back(0);

        for (const auto& array_row : array_rows) {
            for (const auto& v : array_row) {
                arr_data_column->insert_data((const char*)(&v), 0);
            }
            offsets.push_back(arr_data_column->size());
        }
        for (size_t i = 1; i < offsets.size(); i++) {
            arr_offsets_column->insert_data((const char*)(&offsets[i]), 0);
        }
        auto array_column = ColumnArray::create(make_nullable(std::move(arr_data_column)),
                                                std::move(arr_offsets_column));
        result_block->insert(ColumnWithTypeAndName(make_nullable(std::move(array_column)),
                                                   data_type_array_type_nullable, "tags"));
        return result_block;
    };

    {
        std::shared_ptr<TableFunctionOperatorX> table_func_op =
                create_test_operators(desc_tbl, tplan_node_table_function_node);
        auto* local_state = runtime_state->get_local_state(table_func_op->operator_id());
        auto* table_func_local_state = dynamic_cast<TableFunctionLocalState*>(local_state);
        std::vector<int32_t> ids = {1, 2};
        auto input_block = build_input_block(ids, {{1, 2, 1, 4, 5}, {1, 3, 3, 4, 5}});

        table_func_local_state->_child_block = std::move(input_block);
        table_func_local_state->_child_eos = true;

        auto expected_output_block = table_func_local_state->_child_block->clone_empty();
        auto unnested_tag_column = ColumnInt32::create();
        unnested_tag_column->insert_data((const char*)(ids.data()), 0);
        unnested_tag_column->insert_data((const char*)(ids.data()), 0);
        expected_output_block.insert(ColumnWithTypeAndName(
                make_nullable(std::move(unnested_tag_column)), data_type_int_nullable, "tag"));
        auto mutable_columns = expected_output_block.mutate_columns();
        mutable_columns[0]->insert_from(
                *table_func_local_state->_child_block->get_by_position(0).column, 0);
        mutable_columns[0]->insert_from(
                *table_func_local_state->_child_block->get_by_position(0).column, 0);
        mutable_columns[1]->insert_default();
        mutable_columns[1]->insert_default();
        expected_output_block.set_columns(std::move(mutable_columns));

        // std::cout << "input block: \n"
        //           << table_func_local_state->_child_block->dump_data() << std::endl;
        st = table_func_op->push(runtime_state.get(), table_func_local_state->_child_block.get(),
                                 table_func_local_state->_child_eos);
        ASSERT_TRUE(st.ok()) << "push failed: " << st.to_string();

        bool eos = false;
        Block output_block;
        st = table_func_op->pull(runtime_state.get(), &output_block, &eos);
        ASSERT_TRUE(st.ok()) << "pull failed: " << st.to_string();
        // std::cout << "output block: \n" << output_block.dump_data() << std::endl;
        // std::cout << "expected output block: \n" << expected_output_block.dump_data() << std::endl;
        EXPECT_FALSE(eos);
        EXPECT_TRUE(ColumnHelper::block_equal(output_block, expected_output_block));

        output_block.clear();
        st = table_func_op->pull(runtime_state.get(), &output_block, &eos);
        ASSERT_TRUE(st.ok()) << "pull failed: " << st.to_string();
        // std::cout << "output block: \n" << output_block.dump_data() << std::endl;
        EXPECT_TRUE(output_block.rows() == 0);
        // EXPECT_TRUE(eos);
    }
}

TEST_F(UnnestTest, outer) {
    TDescriptorTable desc_table;
    desc_table.tupleDescriptors.push_back(tuple0);
    desc_table.tupleDescriptors.push_back(tuple1);
    desc_table.tupleDescriptors.push_back(tuple2);

    desc_table.slotDescriptors.push_back(slot_desc_id);
    desc_table.slotDescriptors.push_back(slot_desc_tags);
    desc_table.slotDescriptors.push_back(slot_desc_unnest_tag);
    desc_table.slotDescriptors.push_back(slot_desc_id_2);
    desc_table.slotDescriptors.push_back(slot_desc_unnest_tag2);

    setup_exec_env();

    // Set main TPlanNode properties
    TPlanNode tplan_node_table_function_node;
    setup_plan_node(tplan_node_table_function_node, true);

    DescriptorTbl* desc_tbl;
    auto st = DescriptorTbl::create(obj_pool.get(), desc_table, &desc_tbl);
    EXPECT_TRUE(st.ok());

    vectorized::DataTypePtr data_type_int(std::make_shared<vectorized::DataTypeInt32>());
    auto data_type_int_nullable = make_nullable(data_type_int);
    vectorized::DataTypePtr data_type_array_type(
            std::make_shared<vectorized::DataTypeArray>(data_type_int_nullable));
    auto data_type_array_type_nullable = make_nullable(data_type_array_type);

    auto build_input_block = [&](const std::vector<int32_t>& ids,
                                 const std::vector<std::vector<int32_t>>& array_rows) {
        auto result_block = std::make_unique<Block>();
        auto id_column = ColumnInt32::create();
        for (const auto& id : ids) {
            id_column->insert_data((const char*)(&id), 0);
        }
        result_block->insert(ColumnWithTypeAndName(make_nullable(std::move(id_column)),
                                                   data_type_int_nullable, "id"));

        auto arr_data_column = ColumnInt32::create();
        auto arr_offsets_column = ColumnOffset64::create();
        std::vector<ColumnArray::Offset64> offsets;
        offsets.push_back(0);

        for (const auto& array_row : array_rows) {
            for (const auto& v : array_row) {
                arr_data_column->insert_data((const char*)(&v), 0);
            }
            offsets.push_back(arr_data_column->size());
        }
        for (size_t i = 1; i < offsets.size(); i++) {
            arr_offsets_column->insert_data((const char*)(&offsets[i]), 0);
        }
        auto array_column = ColumnArray::create(make_nullable(std::move(arr_data_column)),
                                                std::move(arr_offsets_column));
        result_block->insert(ColumnWithTypeAndName(make_nullable(std::move(array_column)),
                                                   data_type_array_type_nullable, "tags"));
        return result_block;
    };

    {
        std::shared_ptr<TableFunctionOperatorX> table_func_op =
                create_test_operators(desc_tbl, tplan_node_table_function_node);
        auto* local_state = runtime_state->get_local_state(table_func_op->operator_id());
        auto* table_func_local_state = dynamic_cast<TableFunctionLocalState*>(local_state);

        std::vector<int32_t> ids = {1, 2};
        auto input_block = build_input_block(ids, {{1, 2, 1, 4, 5}, {1, 3, 3, 4, 5}});

        table_func_local_state->_child_block = std::move(input_block);
        table_func_local_state->_child_eos = true;

        auto expected_output_block = table_func_local_state->_child_block->clone_empty();
        auto unnested_tag_column = ColumnInt32::create();
        unnested_tag_column->insert_data((const char*)(ids.data()), 0);
        unnested_tag_column->insert_data((const char*)(ids.data()), 0);
        expected_output_block.insert(ColumnWithTypeAndName(
                make_nullable(std::move(unnested_tag_column)), data_type_int_nullable, "tag"));
        auto mutable_columns = expected_output_block.mutate_columns();
        mutable_columns[0]->insert_from(
                *table_func_local_state->_child_block->get_by_position(0).column, 0);
        mutable_columns[0]->insert_from(
                *table_func_local_state->_child_block->get_by_position(0).column, 0);
        mutable_columns[1]->insert_default();
        mutable_columns[1]->insert_default();
        expected_output_block.set_columns(std::move(mutable_columns));

        // std::cout << "input block: \n"
        //           << table_func_local_state->_child_block->dump_data() << std::endl;
        st = table_func_op->push(runtime_state.get(), table_func_local_state->_child_block.get(),
                                 table_func_local_state->_child_eos);
        ASSERT_TRUE(st.ok()) << "push failed: " << st.to_string();

        bool eos = false;
        Block output_block;
        st = table_func_op->pull(runtime_state.get(), &output_block, &eos);
        ASSERT_TRUE(st.ok()) << "pull failed: " << st.to_string();
        // std::cout << "output block: \n" << output_block.dump_data() << std::endl;
        // std::cout << "expected output block: \n" << expected_output_block.dump_data() << std::endl;
        EXPECT_FALSE(eos);
        EXPECT_TRUE(ColumnHelper::block_equal(output_block, expected_output_block));

        output_block.clear();
        expected_output_block.clear_column_data();
        mutable_columns = expected_output_block.mutate_columns();
        mutable_columns[0]->insert_from(
                *table_func_local_state->_child_block->get_by_position(0).column, 1);
        mutable_columns[1]->insert_default();
        mutable_columns[2]->insert_default();
        expected_output_block.set_columns(std::move(mutable_columns));
        st = table_func_op->pull(runtime_state.get(), &output_block, &eos);
        ASSERT_TRUE(st.ok()) << "pull failed: " << st.to_string();
        // std::cout << "output block: \n" << output_block.dump_data() << std::endl;
        // std::cout << "expected output block: \n" << expected_output_block.dump_data() << std::endl;
        EXPECT_TRUE(eos);
        EXPECT_TRUE(ColumnHelper::block_equal(output_block, expected_output_block));
    }
}
} // namespace doris::pipeline