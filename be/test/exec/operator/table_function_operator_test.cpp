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

#include "exec/operator/table_function_operator.h"

#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Types_types.h>
#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "core/block/block.h"
#include "core/column/column_array.h"
#include "core/column/column_nullable.h"
#include "core/column/column_struct.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "exec/operator/operator_helper.h"
#include "exprs/function/array/function_array_utils.h"
#include "testutil/column_helper.h"
#include "testutil/creators.h"
#include "testutil/mock/mock_descriptors.h"
#include "testutil/mock/mock_literal_expr.h"
#include "testutil/mock/mock_operators.h"
#include "testutil/mock/mock_slot_ref.h"

namespace doris {

class MockTableFunctionChildOperator : public OperatorXBase {
public:
    Status get_block_after_projects(RuntimeState* state, Block* block, bool* eos) override {
        return Status::OK();
    }

    Status get_block(RuntimeState* state, Block* block, bool* eos) override { return Status::OK(); }
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

class MockFastExplodeTableFunction : public TableFunction {
public:
    explicit MockFastExplodeTableFunction(bool* get_value_called)
            : _get_value_called(get_value_called) {}

    Status process_init(Block* block, RuntimeState* /*state*/) override {
        // Expose array offsets / nested column for TableFunctionLocalState block fast path.
        ColumnArrayExecutionData data;
        auto array_col = block->get_by_position(1).column->convert_to_full_column_if_const();
        if (!extract_column_array_info(*array_col, data)) {
            return Status::InternalError("invalid array column");
        }
        _detail = data;
        return Status::OK();
    }

    void process_row(size_t row_idx) override {
        TableFunction::process_row(row_idx);
        if (!_detail.array_nullmap_data || !_detail.array_nullmap_data[row_idx]) {
            _array_offset = row_idx == 0 ? 0 : (*_detail.offsets_ptr)[row_idx - 1];
            _cur_size = (*_detail.offsets_ptr)[row_idx] - _array_offset;
        }
    }

    void process_close() override {
        _detail.reset();
        _array_offset = 0;
    }

    void get_same_many_values(MutableColumnPtr& column, int length) override {
        column->insert_many_defaults(length);
    }

    int get_value(MutableColumnPtr& column, int max_step) override {
        if (_get_value_called) {
            // Slow path indicator: fast path tests expect this to remain false.
            *_get_value_called = true;
        }
        max_step = std::min(max_step, cast_set<int>(_cur_size - _cur_offset));
        const size_t pos = _array_offset + _cur_offset;
        if (current_empty()) {
            column->insert_default();
            max_step = 1;
        } else if (column->is_nullable()) {
            auto* nullable_column = assert_cast<ColumnNullable*>(column.get());
            nullable_column->get_nested_column_ptr()->insert_range_from(*_detail.nested_col, pos,
                                                                        max_step);
            auto* nullmap_column =
                    assert_cast<ColumnUInt8*>(nullable_column->get_null_map_column_ptr().get());
            const size_t old_size = nullmap_column->size();
            nullmap_column->resize(old_size + max_step);
            if (_detail.nested_nullmap_data != nullptr) {
                memcpy(nullmap_column->get_data().data() + old_size,
                       _detail.nested_nullmap_data + pos, max_step * sizeof(UInt8));
            } else {
                memset(nullmap_column->get_data().data() + old_size, 0, max_step * sizeof(UInt8));
            }
        } else {
            column->insert_range_from(*_detail.nested_col, pos, max_step);
        }
        forward(max_step);
        return max_step;
    }

    bool support_block_fast_path() const override { return true; }

    Status prepare_block_fast_path(Block* /*block*/, RuntimeState* /*state*/,
                                   BlockFastPathContext* ctx) override {
        // NOTE: process_init() must be called before this to fill `_detail`.
        ctx->array_nullmap_data = _detail.array_nullmap_data;
        ctx->offsets_ptr = _detail.offsets_ptr;
        ctx->nested_col = _detail.nested_col;
        ctx->nested_nullmap_data = _detail.nested_nullmap_data;
        return Status::OK();
    }

private:
    bool* _get_value_called = nullptr;
    ColumnArrayExecutionData _detail;
    size_t _array_offset = 0;
};

struct MockTableFunctionLocalState : TableFunctionLocalState {
    MockTableFunctionLocalState(RuntimeState* state, OperatorXBase* parent)
            : TableFunctionLocalState(state, parent) {}

    Status _clone_table_function(RuntimeState* state) override {
        auto& p = _parent->cast<TableFunctionOperatorX>();
        _vfn_ctxs.resize(p._vfn_ctxs.size());
        for (size_t i = 0; i < _vfn_ctxs.size(); i++) {
            RETURN_IF_ERROR(p._vfn_ctxs[i]->clone(state, _vfn_ctxs[i]));
            TableFunction* fn = p._fns[i];
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

    void prepare_fast_explode_operator(const DataTypePtr& child_array_type,
                                       const DataTypePtr& output_type, bool* get_value_called,
                                       std::vector<bool> output_slot_ids = {true, true, true}) {
        auto int_type = std::make_shared<DataTypeInt32>();
        op->_vfn_ctxs = MockSlotRef::create_mock_contexts(DataTypes {int_type});
        auto fn = std::make_shared<MockFastExplodeTableFunction>(get_value_called);
        fns.push_back(fn);
        op->_fns.push_back(fn.get());
        op->_output_slot_ids = std::move(output_slot_ids);

        child_op->_mock_row_desc.reset(new MockRowDescriptor {{int_type, child_array_type}, &pool});
        op->_mock_row_descriptor.reset(
                new MockRowDescriptor {{int_type, child_array_type, output_type}, &pool});

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

    void push_child_block(Block block) {
        *local_state->_child_block = std::move(block);
        auto st = op->push(state.get(), local_state->_child_block.get(), true);
        EXPECT_TRUE(st) << st.msg();
    }

    Block pull_child_block(bool* eos = nullptr) {
        Block block;
        bool local_eos = false;
        auto st = op->pull(state.get(), &block, &local_eos);
        EXPECT_TRUE(st) << st.msg();
        if (eos != nullptr) {
            *eos = local_eos;
        }
        return block;
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
                new MockRowDescriptor {{std::make_shared<DataTypeInt32>()}, &pool});
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
                new MockRowDescriptor {{std::make_shared<DataTypeInt32>()}, &pool});
        op->_mock_row_descriptor.reset(new MockRowDescriptor {
                {std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeInt32>()}, &pool});
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

TEST_F(TableFunctionOperatorTest, block_fast_path_explode) {
    bool get_value_called = false;
    auto int_type = std::make_shared<DataTypeInt32>();
    auto arr_type = std::make_shared<DataTypeArray>(int_type);
    prepare_fast_explode_operator(arr_type, int_type, &get_value_called, {true, false, true});

    {
        auto id_col = ColumnHelper::create_column<DataTypeInt32>({10, 20, 30, 40});
        auto nested = ColumnInt32::create();
        nested->insert_value(1);
        nested->insert_value(2);
        nested->insert_value(3);
        nested->insert_value(4);
        auto offsets = ColumnArray::ColumnOffsets::create();
        offsets->insert_value(1);
        offsets->insert_value(3);
        offsets->insert_value(3);
        offsets->insert_value(4);
        auto arr_col = ColumnArray::create(std::move(nested), std::move(offsets));

        push_child_block(
                Block({ColumnWithTypeAndName(id_col, int_type, "id"),
                       ColumnWithTypeAndName(ColumnPtr(std::move(arr_col)), arr_type, "arr")}));
    }

    {
        bool eos = false;
        auto block = pull_child_block(&eos);
        EXPECT_FALSE(get_value_called);

        auto expected_id = ColumnHelper::create_column<DataTypeInt32>({10, 20, 20, 40});

        auto expected_nested = ColumnInt32::create();
        expected_nested->insert_value(1);
        expected_nested->insert_value(2);
        expected_nested->insert_value(3);
        expected_nested->insert_value(2);
        expected_nested->insert_value(3);
        expected_nested->insert_value(4);
        auto expected_offsets = ColumnArray::ColumnOffsets::create();
        expected_offsets->insert_value(1);
        expected_offsets->insert_value(3);
        expected_offsets->insert_value(5);
        expected_offsets->insert_value(6);
        auto expected_arr =
                ColumnArray::create(std::move(expected_nested), std::move(expected_offsets));

        auto expected_out = ColumnHelper::create_column<DataTypeInt32>({1, 2, 3, 4});

        auto int_type = std::make_shared<DataTypeInt32>();
        auto arr_type = std::make_shared<DataTypeArray>(int_type);
        Block expected({ColumnWithTypeAndName(expected_id, int_type, "id"),
                        ColumnWithTypeAndName(ColumnPtr(std::move(expected_arr)), arr_type, "arr"),
                        ColumnWithTypeAndName(expected_out, int_type, "x")});
        EXPECT_TRUE(ColumnHelper::block_equal(block, expected));
    }
}

TEST_F(TableFunctionOperatorTest, block_fast_path_explode_batch_truncate) {
    state->_batch_size = 2;
    bool get_value_called = false;
    auto int_type = std::make_shared<DataTypeInt32>();
    auto arr_type = std::make_shared<DataTypeArray>(int_type);
    prepare_fast_explode_operator(arr_type, int_type, &get_value_called);

    {
        auto id_col = ColumnHelper::create_column<DataTypeInt32>({10, 20, 30, 40});
        auto nested = ColumnInt32::create();
        nested->insert_value(1);
        nested->insert_value(2);
        nested->insert_value(3);
        nested->insert_value(4);
        auto offsets = ColumnArray::ColumnOffsets::create();
        offsets->insert_value(1);
        offsets->insert_value(3);
        offsets->insert_value(3);
        offsets->insert_value(4);
        auto arr_col = ColumnArray::create(std::move(nested), std::move(offsets));

        push_child_block(
                Block({ColumnWithTypeAndName(id_col, int_type, "id"),
                       ColumnWithTypeAndName(ColumnPtr(std::move(arr_col)), arr_type, "arr")}));
    }

    {
        bool eos = false;
        auto block = pull_child_block(&eos);
        EXPECT_FALSE(get_value_called);

        auto expected_id = ColumnHelper::create_column<DataTypeInt32>({10, 20});
        auto expected_nested = ColumnInt32::create();
        expected_nested->insert_value(1);
        expected_nested->insert_value(2);
        expected_nested->insert_value(3);
        auto expected_offsets = ColumnArray::ColumnOffsets::create();
        expected_offsets->insert_value(1);
        expected_offsets->insert_value(3);
        auto expected_arr =
                ColumnArray::create(std::move(expected_nested), std::move(expected_offsets));
        auto expected_out = ColumnHelper::create_column<DataTypeInt32>({1, 2});

        auto int_type = std::make_shared<DataTypeInt32>();
        auto arr_type = std::make_shared<DataTypeArray>(int_type);
        Block expected({ColumnWithTypeAndName(expected_id, int_type, "id"),
                        ColumnWithTypeAndName(ColumnPtr(std::move(expected_arr)), arr_type, "arr"),
                        ColumnWithTypeAndName(expected_out, int_type, "x")});
        EXPECT_TRUE(ColumnHelper::block_equal(block, expected));
    }

    {
        bool eos = false;
        auto block = pull_child_block(&eos);

        auto expected_id = ColumnHelper::create_column<DataTypeInt32>({20, 40});
        auto expected_nested = ColumnInt32::create();
        expected_nested->insert_value(2);
        expected_nested->insert_value(3);
        expected_nested->insert_value(4);
        auto expected_offsets = ColumnArray::ColumnOffsets::create();
        expected_offsets->insert_value(2);
        expected_offsets->insert_value(3);
        auto expected_arr =
                ColumnArray::create(std::move(expected_nested), std::move(expected_offsets));
        auto expected_out = ColumnHelper::create_column<DataTypeInt32>({3, 4});

        auto int_type = std::make_shared<DataTypeInt32>();
        auto arr_type = std::make_shared<DataTypeArray>(int_type);
        Block expected({ColumnWithTypeAndName(expected_id, int_type, "id"),
                        ColumnWithTypeAndName(ColumnPtr(std::move(expected_arr)), arr_type, "arr"),
                        ColumnWithTypeAndName(expected_out, int_type, "x")});
        EXPECT_TRUE(ColumnHelper::block_equal(block, expected));
    }
}

TEST_F(TableFunctionOperatorTest, block_fast_path_explode_nullable_array_skip) {
    bool get_value_called = false;
    auto int_type = std::make_shared<DataTypeInt32>();
    auto arr_type = std::make_shared<DataTypeArray>(int_type);
    prepare_fast_explode_operator(arr_type, int_type, &get_value_called);

    {
        auto id_col = ColumnHelper::create_column<DataTypeInt32>({10, 20, 30});
        auto nested = ColumnInt32::create();
        nested->insert_value(1);
        nested->insert_value(2);
        auto offsets = ColumnArray::ColumnOffsets::create();
        offsets->insert_value(1);
        offsets->insert_value(1);
        offsets->insert_value(2);
        auto arr_col = ColumnArray::create(std::move(nested), std::move(offsets));

        push_child_block(
                Block({ColumnWithTypeAndName(id_col, int_type, "id"),
                       ColumnWithTypeAndName(ColumnPtr(std::move(arr_col)), arr_type, "arr")}));
    }

    {
        bool eos = false;
        auto block = pull_child_block(&eos);
        EXPECT_FALSE(get_value_called);

        auto expected_id = ColumnHelper::create_column<DataTypeInt32>({10, 30});

        auto expected_nested = ColumnInt32::create();
        expected_nested->insert_value(1);
        expected_nested->insert_value(2);
        auto expected_offsets = ColumnArray::ColumnOffsets::create();
        expected_offsets->insert_value(1);
        expected_offsets->insert_value(2);
        auto expected_arr =
                ColumnArray::create(std::move(expected_nested), std::move(expected_offsets));

        auto expected_out = ColumnHelper::create_column<DataTypeInt32>({1, 2});

        auto int_type = std::make_shared<DataTypeInt32>();
        auto arr_type = std::make_shared<DataTypeArray>(int_type);
        Block expected({ColumnWithTypeAndName(expected_id, int_type, "id"),
                        ColumnWithTypeAndName(ColumnPtr(std::move(expected_arr)), arr_type, "arr"),
                        ColumnWithTypeAndName(expected_out, int_type, "x")});
        EXPECT_TRUE(ColumnHelper::block_equal(block, expected));
    }
}

TEST_F(TableFunctionOperatorTest, block_fast_path_explode_nullable_array_null_row) {
    bool get_value_called = false;
    auto int_type = std::make_shared<DataTypeInt32>();
    auto arr_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeArray>(int_type));
    prepare_fast_explode_operator(arr_type, int_type, &get_value_called);

    {
        auto id_col = ColumnHelper::create_column<DataTypeInt32>({10, 20, 30});
        auto nested = ColumnInt32::create();
        nested->insert_value(1);
        nested->insert_value(2);
        auto offsets = ColumnArray::ColumnOffsets::create();
        offsets->insert_value(1);
        offsets->insert_value(1);
        offsets->insert_value(2);
        auto arr_data = ColumnArray::create(std::move(nested), std::move(offsets));
        auto null_map = ColumnUInt8::create();
        null_map->insert_value(0);
        null_map->insert_value(1);
        null_map->insert_value(0);
        auto arr_col = ColumnNullable::create(std::move(arr_data), std::move(null_map));

        push_child_block(
                Block({ColumnWithTypeAndName(id_col, int_type, "id"),
                       ColumnWithTypeAndName(ColumnPtr(std::move(arr_col)), arr_type, "arr")}));
    }

    {
        bool eos = false;
        auto block = pull_child_block(&eos);
        EXPECT_FALSE(get_value_called);

        auto expected_id = ColumnHelper::create_column<DataTypeInt32>({10, 30});
        auto expected_nested = ColumnInt32::create();
        expected_nested->insert_value(1);
        expected_nested->insert_value(2);
        auto expected_offsets = ColumnArray::ColumnOffsets::create();
        expected_offsets->insert_value(1);
        expected_offsets->insert_value(2);
        auto expected_arr_data =
                ColumnArray::create(std::move(expected_nested), std::move(expected_offsets));
        auto expected_arr_null = ColumnUInt8::create();
        expected_arr_null->insert_value(0);
        expected_arr_null->insert_value(0);
        auto expected_arr =
                ColumnNullable::create(std::move(expected_arr_data), std::move(expected_arr_null));
        auto expected_out = ColumnHelper::create_column<DataTypeInt32>({1, 2});

        auto int_type = std::make_shared<DataTypeInt32>();
        auto arr_type =
                std::make_shared<DataTypeNullable>(std::make_shared<DataTypeArray>(int_type));
        Block expected({ColumnWithTypeAndName(expected_id, int_type, "id"),
                        ColumnWithTypeAndName(ColumnPtr(std::move(expected_arr)), arr_type, "arr"),
                        ColumnWithTypeAndName(expected_out, int_type, "x")});
        EXPECT_TRUE(ColumnHelper::block_equal(block, expected));
    }
}

TEST_F(TableFunctionOperatorTest, block_fast_path_explode_nullable_array_misaligned_fallback) {
    bool get_value_called = false;
    auto int_type = std::make_shared<DataTypeInt32>();
    auto arr_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeArray>(int_type));
    prepare_fast_explode_operator(arr_type, int_type, &get_value_called);

    {
        auto id_col = ColumnHelper::create_column<DataTypeInt32>({10, 20, 30});
        auto nested = ColumnInt32::create();
        nested->insert_value(1);
        nested->insert_value(9);
        nested->insert_value(2);
        auto offsets = ColumnArray::ColumnOffsets::create();
        offsets->insert_value(1);
        offsets->insert_value(2);
        offsets->insert_value(3);
        auto arr_data = ColumnArray::create(std::move(nested), std::move(offsets));
        auto null_map = ColumnUInt8::create();
        null_map->insert_value(0);
        null_map->insert_value(1);
        null_map->insert_value(0);
        auto arr_col = ColumnNullable::create(std::move(arr_data), std::move(null_map));

        push_child_block(
                Block({ColumnWithTypeAndName(id_col, int_type, "id"),
                       ColumnWithTypeAndName(ColumnPtr(std::move(arr_col)), arr_type, "arr")}));
    }

    {
        bool eos = false;
        auto block = pull_child_block(&eos);
        EXPECT_TRUE(get_value_called);

        auto expected_id = ColumnHelper::create_column<DataTypeInt32>({10, 30});
        auto expected_nested = ColumnInt32::create();
        expected_nested->insert_value(1);
        expected_nested->insert_value(2);
        auto expected_offsets = ColumnArray::ColumnOffsets::create();
        expected_offsets->insert_value(1);
        expected_offsets->insert_value(2);
        auto expected_arr_data =
                ColumnArray::create(std::move(expected_nested), std::move(expected_offsets));
        auto expected_arr_null = ColumnUInt8::create();
        expected_arr_null->insert_value(0);
        expected_arr_null->insert_value(0);
        auto expected_arr =
                ColumnNullable::create(std::move(expected_arr_data), std::move(expected_arr_null));
        auto expected_out = ColumnHelper::create_column<DataTypeInt32>({1, 2});

        auto int_type = std::make_shared<DataTypeInt32>();
        auto arr_type =
                std::make_shared<DataTypeNullable>(std::make_shared<DataTypeArray>(int_type));
        Block expected({ColumnWithTypeAndName(expected_id, int_type, "id"),
                        ColumnWithTypeAndName(ColumnPtr(std::move(expected_arr)), arr_type, "arr"),
                        ColumnWithTypeAndName(expected_out, int_type, "x")});
        EXPECT_TRUE(ColumnHelper::block_equal(block, expected));
    }
}

TEST_F(TableFunctionOperatorTest,
       block_fast_path_explode_nullable_array_partial_gap_uses_slow_path) {
    state->_batch_size = 2;
    bool get_value_called = false;
    auto int_type = std::make_shared<DataTypeInt32>();
    auto arr_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeArray>(int_type));
    prepare_fast_explode_operator(arr_type, int_type, &get_value_called);

    {
        auto id_col = ColumnHelper::create_column<DataTypeInt32>({10, 20, 30});
        auto nested = ColumnInt32::create();
        nested->insert_value(1);
        nested->insert_value(2);
        nested->insert_value(3);
        nested->insert_value(9);
        nested->insert_value(4);
        auto offsets = ColumnArray::ColumnOffsets::create();
        offsets->insert_value(3);
        offsets->insert_value(4);
        offsets->insert_value(5);
        auto arr_data = ColumnArray::create(std::move(nested), std::move(offsets));
        auto null_map = ColumnUInt8::create();
        null_map->insert_value(0);
        null_map->insert_value(1);
        null_map->insert_value(0);
        auto arr_col = ColumnNullable::create(std::move(arr_data), std::move(null_map));

        push_child_block(
                Block({ColumnWithTypeAndName(id_col, int_type, "id"),
                       ColumnWithTypeAndName(ColumnPtr(std::move(arr_col)), arr_type, "arr")}));
    }

    {
        bool eos = false;
        auto block = pull_child_block(&eos);
        EXPECT_TRUE(get_value_called);

        auto expected_id = ColumnHelper::create_column<DataTypeInt32>({10, 10});
        auto expected_nested = ColumnInt32::create();
        expected_nested->insert_value(1);
        expected_nested->insert_value(2);
        expected_nested->insert_value(3);
        expected_nested->insert_value(1);
        expected_nested->insert_value(2);
        expected_nested->insert_value(3);
        auto expected_offsets = ColumnArray::ColumnOffsets::create();
        expected_offsets->insert_value(3);
        expected_offsets->insert_value(6);
        auto expected_arr_data =
                ColumnArray::create(std::move(expected_nested), std::move(expected_offsets));
        auto expected_arr_null = ColumnUInt8::create();
        expected_arr_null->insert_value(0);
        expected_arr_null->insert_value(0);
        auto expected_arr =
                ColumnNullable::create(std::move(expected_arr_data), std::move(expected_arr_null));
        auto expected_out = ColumnHelper::create_column<DataTypeInt32>({1, 2});

        Block expected({ColumnWithTypeAndName(expected_id, int_type, "id"),
                        ColumnWithTypeAndName(ColumnPtr(std::move(expected_arr)), arr_type, "arr"),
                        ColumnWithTypeAndName(expected_out, int_type, "x")});
        EXPECT_TRUE(ColumnHelper::block_equal(block, expected));
    }

    {
        bool eos = false;
        auto block = pull_child_block(&eos);

        auto expected_id = ColumnHelper::create_column<DataTypeInt32>({10, 30});
        auto expected_nested = ColumnInt32::create();
        expected_nested->insert_value(1);
        expected_nested->insert_value(2);
        expected_nested->insert_value(3);
        expected_nested->insert_value(4);
        auto expected_offsets = ColumnArray::ColumnOffsets::create();
        expected_offsets->insert_value(3);
        expected_offsets->insert_value(4);
        auto expected_arr_data =
                ColumnArray::create(std::move(expected_nested), std::move(expected_offsets));
        auto expected_arr_null = ColumnUInt8::create();
        expected_arr_null->insert_value(0);
        expected_arr_null->insert_value(0);
        auto expected_arr =
                ColumnNullable::create(std::move(expected_arr_data), std::move(expected_arr_null));
        auto expected_out = ColumnHelper::create_column<DataTypeInt32>({3, 4});

        Block expected({ColumnWithTypeAndName(expected_id, int_type, "id"),
                        ColumnWithTypeAndName(ColumnPtr(std::move(expected_arr)), arr_type, "arr"),
                        ColumnWithTypeAndName(expected_out, int_type, "x")});
        EXPECT_TRUE(ColumnHelper::block_equal(block, expected));
    }
}

TEST_F(TableFunctionOperatorTest, block_fast_path_explode_nullable_elements) {
    bool get_value_called = false;
    auto int_type = std::make_shared<DataTypeInt32>();
    auto nested_type = std::make_shared<DataTypeNullable>(int_type);
    auto arr_type = std::make_shared<DataTypeArray>(nested_type);
    prepare_fast_explode_operator(arr_type, nested_type, &get_value_called);

    {
        auto id_col = ColumnHelper::create_column<DataTypeInt32>({10});

        auto nested_data = ColumnInt32::create();
        nested_data->insert_value(1);
        nested_data->insert_value(0);
        nested_data->insert_value(2);
        auto nested_null = ColumnUInt8::create();
        nested_null->insert_value(0);
        nested_null->insert_value(1);
        nested_null->insert_value(0);
        auto nested = ColumnNullable::create(std::move(nested_data), std::move(nested_null));

        auto offsets = ColumnArray::ColumnOffsets::create();
        offsets->insert_value(3);
        auto arr_col = ColumnArray::create(std::move(nested), std::move(offsets));

        push_child_block(
                Block({ColumnWithTypeAndName(id_col, int_type, "id"),
                       ColumnWithTypeAndName(ColumnPtr(std::move(arr_col)), arr_type, "arr")}));
    }

    {
        bool eos = false;
        auto block = pull_child_block(&eos);
        EXPECT_FALSE(get_value_called);

        auto expected_id = ColumnHelper::create_column<DataTypeInt32>({10, 10, 10});

        auto expected_nested_data = ColumnInt32::create();
        expected_nested_data->insert_value(1);
        expected_nested_data->insert_value(0);
        expected_nested_data->insert_value(2);
        expected_nested_data->insert_value(1);
        expected_nested_data->insert_value(0);
        expected_nested_data->insert_value(2);
        expected_nested_data->insert_value(1);
        expected_nested_data->insert_value(0);
        expected_nested_data->insert_value(2);
        auto expected_nested_null = ColumnUInt8::create();
        expected_nested_null->insert_value(0);
        expected_nested_null->insert_value(1);
        expected_nested_null->insert_value(0);
        expected_nested_null->insert_value(0);
        expected_nested_null->insert_value(1);
        expected_nested_null->insert_value(0);
        expected_nested_null->insert_value(0);
        expected_nested_null->insert_value(1);
        expected_nested_null->insert_value(0);
        auto expected_nested = ColumnNullable::create(std::move(expected_nested_data),
                                                      std::move(expected_nested_null));
        auto expected_offsets = ColumnArray::ColumnOffsets::create();
        expected_offsets->insert_value(3);
        expected_offsets->insert_value(6);
        expected_offsets->insert_value(9);
        auto expected_arr =
                ColumnArray::create(std::move(expected_nested), std::move(expected_offsets));

        auto expected_out_data = ColumnInt32::create();
        expected_out_data->insert_value(1);
        expected_out_data->insert_value(0);
        expected_out_data->insert_value(2);
        auto expected_out_null = ColumnUInt8::create();
        expected_out_null->insert_value(0);
        expected_out_null->insert_value(1);
        expected_out_null->insert_value(0);
        auto expected_out =
                ColumnNullable::create(std::move(expected_out_data), std::move(expected_out_null));

        Block expected(
                {ColumnWithTypeAndName(expected_id, int_type, "id"),
                 ColumnWithTypeAndName(ColumnPtr(std::move(expected_arr)), arr_type, "arr"),
                 ColumnWithTypeAndName(ColumnPtr(std::move(expected_out)), nested_type, "x")});
        EXPECT_TRUE(ColumnHelper::block_equal(block, expected));
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
        op->_mock_row_descriptor.reset(new MockRowDescriptor {
                {std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeInt32>()}, &pool});
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
        op->_mock_row_descriptor.reset(new MockRowDescriptor {
                {std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeInt32>()}, &pool});
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
        runtime_state->_batch_size = 5;
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

    void setup_explode_plan_node(TPlanNode& tplan_node_table_function_node, bool outer) {
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

    DataTypePtr data_type_int(std::make_shared<DataTypeInt32>());
    auto data_type_int_nullable = make_nullable(data_type_int);
    DataTypePtr data_type_array_type(std::make_shared<DataTypeArray>(data_type_int_nullable));
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

    DataTypePtr data_type_int(std::make_shared<DataTypeInt32>());
    auto data_type_int_nullable = make_nullable(data_type_int);
    DataTypePtr data_type_array_type(std::make_shared<DataTypeArray>(data_type_int_nullable));
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

// Test inner mode fast path with NULL and empty arrays (they should be skipped)
TEST_F(UnnestTest, inner_with_nulls_fast_path) {
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
    runtime_state->_batch_size = 4096;

    TPlanNode tplan_node;
    setup_explode_plan_node(tplan_node, false); // inner, no conjuncts → fast path

    DescriptorTbl* desc_tbl;
    auto st = DescriptorTbl::create(obj_pool.get(), desc_table, &desc_tbl);
    EXPECT_TRUE(st.ok());

    DataTypePtr data_type_int(std::make_shared<DataTypeInt32>());
    auto data_type_int_nullable = make_nullable(data_type_int);
    DataTypePtr data_type_array_type(std::make_shared<DataTypeArray>(data_type_int_nullable));
    auto data_type_array_type_nullable = make_nullable(data_type_array_type);

    // Build input: ids=[1,2,3,4,5], arrays=[[10,20,30], NULL, [40,50], [], [60]]
    auto build_input = [&]() {
        auto result_block = std::make_unique<Block>();

        auto id_column = ColumnInt32::create();
        for (int32_t id : {1, 2, 3, 4, 5}) {
            id_column->insert_data((const char*)(&id), 0);
        }
        result_block->insert(ColumnWithTypeAndName(make_nullable(std::move(id_column)),
                                                   data_type_int_nullable, "id"));

        auto arr_data = ColumnInt32::create();
        auto arr_offsets = ColumnOffset64::create();
        auto arr_nullmap = ColumnUInt8::create();

        // Row 0: [10, 20, 30]
        for (int32_t v : {10, 20, 30}) {
            arr_data->insert_data((const char*)(&v), 0);
        }
        ColumnArray::Offset64 off = 3;
        arr_offsets->insert_data((const char*)(&off), 0);
        arr_nullmap->get_data().push_back(0);

        // Row 1: NULL
        arr_offsets->insert_data((const char*)(&off), 0); // same offset
        arr_nullmap->get_data().push_back(1);

        // Row 2: [40, 50]
        for (int32_t v : {40, 50}) {
            arr_data->insert_data((const char*)(&v), 0);
        }
        off = 5;
        arr_offsets->insert_data((const char*)(&off), 0);
        arr_nullmap->get_data().push_back(0);

        // Row 3: []
        arr_offsets->insert_data((const char*)(&off), 0); // same offset
        arr_nullmap->get_data().push_back(0);

        // Row 4: [60]
        int32_t v60 = 60;
        arr_data->insert_data((const char*)(&v60), 0);
        off = 6;
        arr_offsets->insert_data((const char*)(&off), 0);
        arr_nullmap->get_data().push_back(0);

        auto array_column =
                ColumnArray::create(make_nullable(std::move(arr_data)), std::move(arr_offsets));
        auto nullable_array =
                ColumnNullable::create(std::move(array_column), std::move(arr_nullmap));
        result_block->insert(ColumnWithTypeAndName(std::move(nullable_array),
                                                   data_type_array_type_nullable, "tags"));
        return result_block;
    };

    {
        auto table_func_op = create_test_operators(desc_tbl, tplan_node);
        auto* local_state = runtime_state->get_local_state(table_func_op->operator_id());
        auto* tfl = dynamic_cast<TableFunctionLocalState*>(local_state);

        tfl->_child_block = build_input();
        tfl->_child_eos = true;

        st = table_func_op->push(runtime_state.get(), tfl->_child_block.get(), tfl->_child_eos);
        ASSERT_TRUE(st.ok()) << "push failed: " << st.to_string();

        bool eos = false;
        Block output_block;
        st = table_func_op->pull(runtime_state.get(), &output_block, &eos);
        ASSERT_TRUE(st.ok()) << "pull failed: " << st.to_string();

        // Expected: 6 rows — NULL row (id=2) and empty row (id=4) are skipped
        EXPECT_EQ(output_block.rows(), 6);

        // Verify ids: [1,1,1,3,3,5]
        auto id_col = output_block.get_by_position(0).column;
        auto check_id = [&](size_t row, int32_t expected) {
            auto field = (*id_col)[row];
            EXPECT_EQ(field.get<TYPE_INT>(), expected) << "row " << row;
        };
        check_id(0, 1);
        check_id(1, 1);
        check_id(2, 1);
        check_id(3, 3);
        check_id(4, 3);
        check_id(5, 5);

        // Verify tags: [10,20,30,40,50,60]
        auto tag_col = output_block.get_by_position(2).column;
        auto check_tag = [&](size_t row, int32_t expected) {
            auto field = (*tag_col)[row];
            EXPECT_EQ(field.get<TYPE_INT>(), expected) << "row " << row;
        };
        check_tag(0, 10);
        check_tag(1, 20);
        check_tag(2, 30);
        check_tag(3, 40);
        check_tag(4, 50);
        check_tag(5, 60);
    }
}

// Test outer mode fast path with NULL and empty arrays (should emit NULL rows)
TEST_F(UnnestTest, outer_with_nulls_fast_path) {
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
    runtime_state->_batch_size = 4096;

    TPlanNode tplan_node;
    setup_explode_plan_node(tplan_node, true); // outer, no conjuncts → fast path

    DescriptorTbl* desc_tbl;
    auto st = DescriptorTbl::create(obj_pool.get(), desc_table, &desc_tbl);
    EXPECT_TRUE(st.ok());

    DataTypePtr data_type_int(std::make_shared<DataTypeInt32>());
    auto data_type_int_nullable = make_nullable(data_type_int);
    DataTypePtr data_type_array_type(std::make_shared<DataTypeArray>(data_type_int_nullable));
    auto data_type_array_type_nullable = make_nullable(data_type_array_type);

    // Build input: ids=[1,2,3,4,5], arrays=[[10,20], NULL, [30], [], [40,50]]
    auto build_input = [&]() {
        auto result_block = std::make_unique<Block>();

        auto id_column = ColumnInt32::create();
        for (int32_t id : {1, 2, 3, 4, 5}) {
            id_column->insert_data((const char*)(&id), 0);
        }
        result_block->insert(ColumnWithTypeAndName(make_nullable(std::move(id_column)),
                                                   data_type_int_nullable, "id"));

        auto arr_data = ColumnInt32::create();
        auto arr_offsets = ColumnOffset64::create();
        auto arr_nullmap = ColumnUInt8::create();

        // Row 0: [10, 20]
        for (int32_t v : {10, 20}) {
            arr_data->insert_data((const char*)(&v), 0);
        }
        ColumnArray::Offset64 off = 2;
        arr_offsets->insert_data((const char*)(&off), 0);
        arr_nullmap->get_data().push_back(0);

        // Row 1: NULL
        arr_offsets->insert_data((const char*)(&off), 0);
        arr_nullmap->get_data().push_back(1);

        // Row 2: [30]
        int32_t v30 = 30;
        arr_data->insert_data((const char*)(&v30), 0);
        off = 3;
        arr_offsets->insert_data((const char*)(&off), 0);
        arr_nullmap->get_data().push_back(0);

        // Row 3: []
        arr_offsets->insert_data((const char*)(&off), 0);
        arr_nullmap->get_data().push_back(0);

        // Row 4: [40, 50]
        for (int32_t v : {40, 50}) {
            arr_data->insert_data((const char*)(&v), 0);
        }
        off = 5;
        arr_offsets->insert_data((const char*)(&off), 0);
        arr_nullmap->get_data().push_back(0);

        auto array_column =
                ColumnArray::create(make_nullable(std::move(arr_data)), std::move(arr_offsets));
        auto nullable_array =
                ColumnNullable::create(std::move(array_column), std::move(arr_nullmap));
        result_block->insert(ColumnWithTypeAndName(std::move(nullable_array),
                                                   data_type_array_type_nullable, "tags"));
        return result_block;
    };

    {
        auto table_func_op = create_test_operators(desc_tbl, tplan_node);
        auto* local_state = runtime_state->get_local_state(table_func_op->operator_id());
        auto* tfl = dynamic_cast<TableFunctionLocalState*>(local_state);

        tfl->_child_block = build_input();
        tfl->_child_eos = true;

        st = table_func_op->push(runtime_state.get(), tfl->_child_block.get(), tfl->_child_eos);
        ASSERT_TRUE(st.ok()) << "push failed: " << st.to_string();

        bool eos = false;
        Block output_block;
        st = table_func_op->pull(runtime_state.get(), &output_block, &eos);
        ASSERT_TRUE(st.ok()) << "pull failed: " << st.to_string();

        // Expected: 7 rows
        // (1,10), (1,20), (2,NULL), (3,30), (4,NULL), (5,40), (5,50)
        EXPECT_EQ(output_block.rows(), 7);

        // Verify ids: [1,1,2,3,4,5,5]
        auto id_col = output_block.get_by_position(0).column;
        auto check_id = [&](size_t row, int32_t expected) {
            auto field = (*id_col)[row];
            EXPECT_EQ(field.get<TYPE_INT>(), expected) << "row " << row;
        };
        check_id(0, 1);
        check_id(1, 1);
        check_id(2, 2);
        check_id(3, 3);
        check_id(4, 4);
        check_id(5, 5);
        check_id(6, 5);

        // Verify tags: [10, 20, NULL, 30, NULL, 40, 50]
        auto tag_col = output_block.get_by_position(2).column;
        auto check_tag = [&](size_t row, int32_t expected) {
            auto field = (*tag_col)[row];
            EXPECT_EQ(field.get<TYPE_INT>(), expected) << "row " << row;
        };
        auto check_null = [&](size_t row) {
            EXPECT_TRUE(tag_col->is_null_at(row)) << "row " << row << " should be null";
        };
        check_tag(0, 10);
        check_tag(1, 20);
        check_null(2);
        check_tag(3, 30);
        check_null(4);
        check_tag(5, 40);
        check_tag(6, 50);
    }
}
// Test posexplode inner mode fast path with NULL and empty arrays
TEST_F(UnnestTest, posexplode_with_nulls_fast_path) {
    // Build struct type descriptor for posexplode output:
    // STRUCT(pos: INT NOT NULL, val: NULLABLE<INT>)
    TTypeNode type_node_struct;
    type_node_struct.type = TTypeNodeType::STRUCT;
    {
        TStructField sf_pos;
        sf_pos.__set_name("pos");
        sf_pos.__set_contains_null(false);
        TStructField sf_val;
        sf_val.__set_name("val");
        sf_val.__set_contains_null(true);
        type_node_struct.__set_struct_fields({sf_pos, sf_val});
    }

    TTypeDesc type_desc_struct;
    type_desc_struct.types.emplace_back(type_node_struct);
    type_desc_struct.types.emplace_back(type_node_int); // pos sub-type
    type_desc_struct.types.emplace_back(type_node_int); // val sub-type
    type_desc_struct.byte_size = -1;

    // Create new tuple/slot descriptors for posexplode using continuing IDs
    // After constructor: slot_id=5, tuple_id=3, col_unique_id=3
    TTupleDescriptor pe_tuple_tf;
    pe_tuple_tf.id = tuple_id++;
    pe_tuple_tf.byteSize = 0;
    pe_tuple_tf.numNullBytes = 0;

    TTupleDescriptor pe_tuple_proj;
    pe_tuple_proj.id = tuple_id++;
    pe_tuple_proj.byteSize = 0;
    pe_tuple_proj.numNullBytes = 0;

    // TF output slot: struct type
    TSlotRef pe_slot_ref_out;
    pe_slot_ref_out.slot_id = slot_id++;
    pe_slot_ref_out.tuple_id = pe_tuple_tf.id;
    pe_slot_ref_out.col_unique_id = -1;
    pe_slot_ref_out.is_virtual_slot = false;

    TSlotDescriptor pe_slot_desc_out;
    pe_slot_desc_out.id = pe_slot_ref_out.slot_id;
    pe_slot_desc_out.parent = pe_tuple_tf.id;
    pe_slot_desc_out.slotType = type_desc_struct;
    pe_slot_desc_out.columnPos = -1;
    pe_slot_desc_out.byteOffset = 0;
    pe_slot_desc_out.nullIndicatorByte = 0;
    pe_slot_desc_out.nullIndicatorBit = 0;
    pe_slot_desc_out.colName = "posexplode#3";
    pe_slot_desc_out.slotIdx = -1;
    pe_slot_desc_out.isMaterialized = true;
    pe_slot_desc_out.col_unique_id = -1;
    pe_slot_desc_out.is_key = false;
    pe_slot_desc_out.need_materialize = true;
    pe_slot_desc_out.is_auto_increment = false;
    pe_slot_desc_out.primitive_type = TPrimitiveType::STRUCT;

    // Projection slots
    TSlotDescriptor pe_slot_desc_id_proj = slot_desc_id;
    pe_slot_desc_id_proj.id = slot_id++;
    pe_slot_desc_id_proj.parent = pe_tuple_proj.id;

    TSlotDescriptor pe_slot_desc_out_proj = pe_slot_desc_out;
    pe_slot_desc_out_proj.id = slot_id++;
    pe_slot_desc_out_proj.parent = pe_tuple_proj.id;

    TDescriptorTable desc_table;
    desc_table.tupleDescriptors.push_back(tuple0);
    desc_table.tupleDescriptors.push_back(pe_tuple_tf);
    desc_table.tupleDescriptors.push_back(pe_tuple_proj);
    desc_table.slotDescriptors.push_back(slot_desc_id);
    desc_table.slotDescriptors.push_back(slot_desc_tags);
    desc_table.slotDescriptors.push_back(pe_slot_desc_out);
    desc_table.slotDescriptors.push_back(pe_slot_desc_id_proj);
    desc_table.slotDescriptors.push_back(pe_slot_desc_out_proj);

    setup_exec_env();
    runtime_state->_batch_size = 4096;

    // Build plan node for posexplode (inner mode, no conjuncts → fast path)
    TPlanNode tplan_node;
    {
        tplan_node.node_id = 0;
        tplan_node.node_type = TPlanNodeType::TABLE_FUNCTION_NODE;
        tplan_node.num_children = 1;
        tplan_node.limit = -1;

        tplan_node.row_tuples.push_back(tuple0.id);
        tplan_node.row_tuples.push_back(pe_tuple_tf.id);
        tplan_node.nullable_tuples.push_back(false);
        tplan_node.nullable_tuples.push_back(false);
        tplan_node.compact_data = false;
        tplan_node.is_serial_operator = false;

        tplan_node.__set_table_function_node(TTableFunctionNode());

        TExpr fn_expr;
        fn_expr.nodes.emplace_back();
        fn_expr.nodes[0].node_type = TExprNodeType::FUNCTION_CALL;
        fn_expr.nodes[0].type.types.emplace_back(type_node_struct);
        fn_expr.nodes[0].type.types.emplace_back(type_node_int);
        fn_expr.nodes[0].type.types.emplace_back(type_node_int);
        fn_expr.nodes[0].num_children = 1;

        fn_expr.nodes[0].__set_fn(TFunction());
        fn_expr.nodes[0].fn.__set_name(TFunctionName());
        fn_expr.nodes[0].fn.name.function_name = "posexplode";
        fn_expr.nodes[0].fn.arg_types.emplace_back(type_desc_array_int);
        fn_expr.nodes[0].fn.ret_type = type_desc_struct;
        fn_expr.nodes[0].fn.has_var_args = false;
        fn_expr.nodes[0].fn.signature = "posexplode(array<int>)";
        fn_expr.nodes[0].fn.__set_scalar_fn(TScalarFunction());
        fn_expr.nodes[0].fn.scalar_fn.symbol = "";
        fn_expr.nodes[0].fn.id = 0;
        fn_expr.nodes[0].fn.vectorized = true;
        fn_expr.nodes[0].fn.is_udtf_function = false;
        fn_expr.nodes[0].fn.is_static_load = false;
        fn_expr.nodes[0].fn.expiration_time = 360;
        fn_expr.nodes[0].is_nullable = true;

        fn_expr.nodes.emplace_back();
        fn_expr.nodes[1].node_type = TExprNodeType::SLOT_REF;
        fn_expr.nodes[1].type = type_desc_array_int;
        fn_expr.nodes[1].num_children = 0;
        fn_expr.nodes[1].__set_slot_ref(TSlotRef());
        fn_expr.nodes[1].slot_ref = slot_ref_tags;
        fn_expr.nodes[1].output_scale = -1;
        fn_expr.nodes[1].is_nullable = true;
        fn_expr.nodes[1].label = "tags";

        tplan_node.table_function_node.fnCallExprList.push_back(fn_expr);

        tplan_node.table_function_node.outputSlotIds.push_back(slot_desc_id.id);
        tplan_node.table_function_node.outputSlotIds.push_back(pe_slot_desc_out.id);

        // Projections
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
        texpr_proj1.nodes[0].type = type_desc_struct;
        texpr_proj1.nodes[0].num_children = 0;
        texpr_proj1.nodes[0].__set_slot_ref(TSlotRef());
        texpr_proj1.nodes[0].slot_ref = pe_slot_ref_out;
        texpr_proj1.nodes[0].output_scale = -1;
        texpr_proj1.nodes[0].is_nullable = true;
        texpr_proj1.nodes[0].label = "posexplode";

        tplan_node.projections.push_back(texpr_proj0);
        tplan_node.projections.push_back(texpr_proj1);
        tplan_node.output_tuple_id = pe_tuple_proj.id;
        tplan_node.nereids_id = 144;
    }

    DescriptorTbl* desc_tbl;
    auto st = DescriptorTbl::create(obj_pool.get(), desc_table, &desc_tbl);
    EXPECT_TRUE(st.ok());

    DataTypePtr data_type_int(std::make_shared<DataTypeInt32>());
    auto data_type_int_nullable = make_nullable(data_type_int);
    DataTypePtr data_type_array_type(std::make_shared<DataTypeArray>(data_type_int_nullable));
    auto data_type_array_type_nullable = make_nullable(data_type_array_type);

    // Build input: ids=[1,2,3,4], arrays=[[10,20], NULL, [30], []]
    auto build_input = [&]() {
        auto result_block = std::make_unique<Block>();

        auto id_column = ColumnInt32::create();
        for (int32_t id : {1, 2, 3, 4}) {
            id_column->insert_data((const char*)(&id), 0);
        }
        result_block->insert(ColumnWithTypeAndName(make_nullable(std::move(id_column)),
                                                   data_type_int_nullable, "id"));

        auto arr_data = ColumnInt32::create();
        auto arr_offsets = ColumnOffset64::create();
        auto arr_nullmap = ColumnUInt8::create();

        // Row 0: [10, 20]
        for (int32_t v : {10, 20}) {
            arr_data->insert_data((const char*)(&v), 0);
        }
        ColumnArray::Offset64 off = 2;
        arr_offsets->insert_data((const char*)(&off), 0);
        arr_nullmap->get_data().push_back(0);

        // Row 1: NULL
        arr_offsets->insert_data((const char*)(&off), 0);
        arr_nullmap->get_data().push_back(1);

        // Row 2: [30]
        int32_t v30 = 30;
        arr_data->insert_data((const char*)(&v30), 0);
        off = 3;
        arr_offsets->insert_data((const char*)(&off), 0);
        arr_nullmap->get_data().push_back(0);

        // Row 3: []
        arr_offsets->insert_data((const char*)(&off), 0);
        arr_nullmap->get_data().push_back(0);

        auto array_column =
                ColumnArray::create(make_nullable(std::move(arr_data)), std::move(arr_offsets));
        auto nullable_array =
                ColumnNullable::create(std::move(array_column), std::move(arr_nullmap));
        result_block->insert(ColumnWithTypeAndName(std::move(nullable_array),
                                                   data_type_array_type_nullable, "tags"));
        return result_block;
    };

    {
        auto table_func_op = create_test_operators(desc_tbl, tplan_node);
        auto* local_state = runtime_state->get_local_state(table_func_op->operator_id());
        auto* tfl = dynamic_cast<TableFunctionLocalState*>(local_state);

        tfl->_child_block = build_input();
        tfl->_child_eos = true;

        st = table_func_op->push(runtime_state.get(), tfl->_child_block.get(), tfl->_child_eos);
        ASSERT_TRUE(st.ok()) << "push failed: " << st.to_string();

        bool eos = false;
        Block output_block;
        st = table_func_op->pull(runtime_state.get(), &output_block, &eos);
        ASSERT_TRUE(st.ok()) << "pull failed: " << st.to_string();

        // Expected: 3 rows (NULL and empty arrays are skipped in inner mode)
        // Row 0: id=1, struct=(pos=0, val=10)
        // Row 1: id=1, struct=(pos=1, val=20)
        // Row 2: id=3, struct=(pos=0, val=30)
        EXPECT_EQ(output_block.rows(), 3);

        // Verify ids: [1, 1, 3]
        auto id_col = output_block.get_by_position(0).column;
        auto check_id = [&](size_t row, int32_t expected) {
            auto field = (*id_col)[row];
            EXPECT_EQ(field.get<TYPE_INT>(), expected) << "row " << row;
        };
        check_id(0, 1);
        check_id(1, 1);
        check_id(2, 3);

        // Verify struct output column at position 2
        auto out_col = output_block.get_by_position(2).column;
        auto* nullable_out = assert_cast<const ColumnNullable*>(out_col.get());
        for (size_t i = 0; i < 3; ++i) {
            EXPECT_FALSE(nullable_out->is_null_at(i))
                    << "struct row " << i << " should not be null";
        }
        auto* struct_col = assert_cast<const ColumnStruct*>(&nullable_out->get_nested_column());
        auto* pos_col = assert_cast<const ColumnInt32*>(&struct_col->get_column(0));
        auto* val_nullable = assert_cast<const ColumnNullable*>(&struct_col->get_column(1));
        auto* val_col = assert_cast<const ColumnInt32*>(&val_nullable->get_nested_column());

        // Verify positions: [0, 1, 0]
        EXPECT_EQ(pos_col->get_element(0), 0);
        EXPECT_EQ(pos_col->get_element(1), 1);
        EXPECT_EQ(pos_col->get_element(2), 0);

        // Verify values: [10, 20, 30]
        EXPECT_EQ(val_col->get_element(0), 10);
        EXPECT_EQ(val_col->get_element(1), 20);
        EXPECT_EQ(val_col->get_element(2), 30);
    }
}
} // namespace doris
