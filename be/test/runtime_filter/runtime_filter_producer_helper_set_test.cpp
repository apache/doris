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

#include "runtime_filter/runtime_filter_producer_helper_set.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "common/object_pool.h"
#include "pipeline/exec/hashjoin_build_sink.h"
#include "pipeline/exec/mock_operator.h"
#include "pipeline/exec/operator.h"
#include "pipeline/pipeline_task.h"
#include "runtime_filter/runtime_filter_test_utils.h"
#include "vec/data_types/data_type_number.h"
#include "vec/exprs/vslot_ref.h"

namespace doris {

class RuntimeFilterProducerHelperSetTest : public RuntimeFilterTest {
    void SetUp() override {
        RuntimeFilterTest::SetUp();
        _pipeline = std::make_shared<pipeline::Pipeline>(0, INSTANCE_NUM, INSTANCE_NUM);
        _op.reset(new pipeline::MockOperatorX());
        FAIL_IF_ERROR_OR_CATCH_EXCEPTION(_pipeline->add_operator(_op, 2));

        _sink.reset(new pipeline::HashJoinBuildSinkOperatorX(
                &_pool, 0, _op->operator_id(),
                TPlanNodeBuilder(0, TPlanNodeType::HASH_JOIN_NODE).build(), _tbl));
        FAIL_IF_ERROR_OR_CATCH_EXCEPTION(_pipeline->set_sink(_sink));

        _task.reset(new pipeline::PipelineTask(_pipeline, 0, _runtime_states[0].get(), nullptr,
                                               &_profile, {}, 0));
    }

    pipeline::OperatorPtr _op;
    pipeline::DataSinkOperatorPtr _sink;
    pipeline::PipelinePtr _pipeline;
    std::shared_ptr<pipeline::PipelineTask> _task;
    ObjectPool _pool;
};

TEST_F(RuntimeFilterProducerHelperSetTest, basic) {
    auto helper = RuntimeFilterProducerHelperSet();

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
    column->insert(vectorized::Field::create_field<TYPE_INT>(1));
    column->insert(vectorized::Field::create_field<TYPE_INT>(2));
    block.insert({std::move(column), std::make_shared<vectorized::DataTypeInt32>(), "col1"});

    std::map<int, std::shared_ptr<RuntimeFilterWrapper>> runtime_filters;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(helper.process(_runtime_states[0].get(), &block, 2));
}

} // namespace doris
