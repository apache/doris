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

#include "runtime_filter/runtime_filter_producer_helper.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "common/object_pool.h"
#include "pipeline/exec/hashjoin_build_sink.h"
#include "pipeline/exec/mock_operator.h"
#include "pipeline/exec/operator.h"
#include "pipeline/pipeline_task.h"
#include "runtime_filter/runtime_filter_test_utils.h"
#include "vec/columns/columns_number.h"
#include "vec/data_types/data_type_number.h"

namespace doris {

class RuntimeFilterProducerHelperTest : public RuntimeFilterTest {
    void SetUp() override {
        RuntimeFilterTest::SetUp();
        _pipeline = std::make_shared<pipeline::Pipeline>(0, INSTANCE_NUM, INSTANCE_NUM);
        _op.reset(new pipeline::MockOperatorX());
        FAIL_IF_ERROR_OR_CATCH_EXCEPTION(_pipeline->add_operator(_op, 2));

        _sink.reset(new pipeline::HashJoinBuildSinkOperatorX(
                &_pool, 0, _op->operator_id(),
                TPlanNodeBuilder(0, TPlanNodeType::HASH_JOIN_NODE).build(), _tbl));
        FAIL_IF_ERROR_OR_CATCH_EXCEPTION(_pipeline->set_sink(_sink));

        for (int i = 0; i < INSTANCE_NUM; i++) {
            _tasks.emplace_back(new pipeline::PipelineTask(_pipeline, 0, _runtime_states[i].get(),
                                                           nullptr, &_profile, {}, 0));
            _runtime_states[i]->set_task(_tasks.back().get());
        }
    }

    pipeline::OperatorPtr _op;
    pipeline::DataSinkOperatorPtr _sink;
    pipeline::PipelinePtr _pipeline;
    std::vector<std::shared_ptr<pipeline::PipelineTask>> _tasks;
    ObjectPool _pool;
};

TEST_F(RuntimeFilterProducerHelperTest, basic) {
    auto helper = RuntimeFilterProducerHelper(&_profile, true, false);

    vectorized::VExprContextSPtr ctx;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(vectorized::VExpr::create_expr_tree(
            TRuntimeFilterDescBuilder::get_default_expr(), ctx));
    ctx->_last_result_column_id = 0;

    vectorized::VExprContextSPtrs build_expr_ctxs = {ctx};
    std::vector<TRuntimeFilterDesc> runtime_filter_descs = {TRuntimeFilterDescBuilder().build()};
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            helper.init(_runtime_states[0].get(), build_expr_ctxs, runtime_filter_descs));

    vectorized::Block block;
    auto column = vectorized::ColumnInt32::create();
    column->insert(1);
    column->insert(2);
    block.insert({std::move(column), std::make_shared<vectorized::DataTypeInt32>(), "col1"});

    vectorized::SharedHashTableContextPtr shared_hash_table_ctx;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            helper.process(_runtime_states[0].get(), &block, shared_hash_table_ctx));
}

TEST_F(RuntimeFilterProducerHelperTest, wake_up_eraly) {
    auto helper = RuntimeFilterProducerHelper(&_profile, true, false);

    vectorized::VExprContextSPtr ctx;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(vectorized::VExpr::create_expr_tree(
            TRuntimeFilterDescBuilder::get_default_expr(), ctx));
    ctx->_last_result_column_id = 0;

    vectorized::VExprContextSPtrs build_expr_ctxs = {ctx};
    std::vector<TRuntimeFilterDesc> runtime_filter_descs = {
            TRuntimeFilterDescBuilder().set_build_bf_by_runtime_size(true).build()};
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            helper.init(_runtime_states[0].get(), build_expr_ctxs, runtime_filter_descs));

    vectorized::Block block;
    auto column = vectorized::ColumnInt32::create();
    column->insert(1);
    column->insert(2);
    block.insert({std::move(column), std::make_shared<vectorized::DataTypeInt32>(), "col1"});

    vectorized::SharedHashTableContextPtr shared_hash_table_ctx;
    _tasks[0]->set_wake_up_early();
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            helper.process(_runtime_states[0].get(), &block, shared_hash_table_ctx));
}

TEST_F(RuntimeFilterProducerHelperTest, skip_process) {
    auto helper = RuntimeFilterProducerHelper(&_profile, true, false);

    vectorized::VExprContextSPtr ctx;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(vectorized::VExpr::create_expr_tree(
            TRuntimeFilterDescBuilder::get_default_expr(), ctx));
    ctx->_last_result_column_id = 0;

    vectorized::VExprContextSPtrs build_expr_ctxs = {ctx};
    std::vector<TRuntimeFilterDesc> runtime_filter_descs = {
            TRuntimeFilterDescBuilder().set_build_bf_by_runtime_size(true).build()};
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            helper.init(_runtime_states[0].get(), build_expr_ctxs, runtime_filter_descs));

    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(helper.skip_process(_runtime_states[0].get()));

    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            helper.send_filter_size(_runtime_states[0].get(), 123, nullptr));

    vectorized::Block block;
    auto column = vectorized::ColumnInt32::create();
    column->insert(1);
    column->insert(2);
    block.insert({std::move(column), std::make_shared<vectorized::DataTypeInt32>(), "col1"});

    vectorized::SharedHashTableContextPtr shared_hash_table_ctx;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            helper.process(_runtime_states[0].get(), &block, shared_hash_table_ctx));
}

TEST_F(RuntimeFilterProducerHelperTest, broadcast) {
    auto helper = RuntimeFilterProducerHelper(&_profile, true, true);

    vectorized::VExprContextSPtr ctx;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(vectorized::VExpr::create_expr_tree(
            TRuntimeFilterDescBuilder::get_default_expr(), ctx));
    ctx->_last_result_column_id = 0;

    vectorized::VExprContextSPtrs build_expr_ctxs = {ctx};
    std::vector<TRuntimeFilterDesc> runtime_filter_descs = {TRuntimeFilterDescBuilder().build()};
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            helper.init(_runtime_states[0].get(), build_expr_ctxs, runtime_filter_descs));

    vectorized::Block block;
    auto column = vectorized::ColumnInt32::create();
    column->insert(1);
    column->insert(2);
    block.insert({std::move(column), std::make_shared<vectorized::DataTypeInt32>(), "col1"});

    vectorized::SharedHashTableContextPtr shared_hash_table_ctx =
            std::make_shared<vectorized::SharedHashTableContext>();
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            helper.process(_runtime_states[0].get(), &block, shared_hash_table_ctx));

    auto helper2 = RuntimeFilterProducerHelper(&_profile, false, true);
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            helper2.init(_runtime_states[1].get(), build_expr_ctxs, runtime_filter_descs));
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            helper2.process(_runtime_states[1].get(), &block, shared_hash_table_ctx));
}

} // namespace doris
