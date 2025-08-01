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

#include "runtime_filter/runtime_filter_consumer_helper.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "common/object_pool.h"
#include "pipeline/exec/hashjoin_build_sink.h"
#include "pipeline/exec/mock_operator.h"
#include "pipeline/exec/operator.h"
#include "pipeline/pipeline_task.h"
#include "runtime/descriptors.h"
#include "runtime_filter/runtime_filter_consumer.h"
#include "runtime_filter/runtime_filter_test_utils.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_number.h"

namespace doris {

class RuntimeFilterConsumerHelperTest : public RuntimeFilterTest {
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

        ExecEnv::GetInstance()->_init_runtime_filter_timer_queue();
    }

    pipeline::OperatorPtr _op;
    pipeline::DataSinkOperatorPtr _sink;
    pipeline::PipelinePtr _pipeline;
    std::shared_ptr<pipeline::PipelineTask> _task;
    ObjectPool _pool;
};

TEST_F(RuntimeFilterConsumerHelperTest, basic) {
    vectorized::VExprContextSPtr ctx;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(vectorized::VExpr::create_expr_tree(
            TRuntimeFilterDescBuilder::get_default_expr(), ctx));
    ctx->_last_result_column_id = 0;

    vectorized::VExprContextSPtrs build_expr_ctxs = {ctx};
    std::vector<TRuntimeFilterDesc> runtime_filter_descs = {
            TRuntimeFilterDescBuilder().add_planId_to_target_expr(0).build(),
            TRuntimeFilterDescBuilder().add_planId_to_target_expr(0).build()};

    std::vector<std::shared_ptr<pipeline::Dependency>> runtime_filter_dependencies;
    SlotDescriptor slot_desc;
    slot_desc._type = vectorized::DataTypeFactory::instance().create_data_type(
            PrimitiveType::TYPE_INT, false);
    TupleDescriptor tuple_desc;
    tuple_desc.add_slot(&slot_desc);
    RowDescriptor row_desc;
    _tbl._slot_desc_map[0] = &slot_desc;
    const_cast<std::vector<TupleDescriptor*>&>(row_desc._tuple_desc_map).push_back(&tuple_desc);
    auto helper = RuntimeFilterConsumerHelper(runtime_filter_descs);

    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            helper.init(_runtime_states[0].get(), true, 0, 0, runtime_filter_dependencies, ""));

    vectorized::VExprContextSPtrs conjuncts;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            helper.acquire_runtime_filter(_runtime_states[0].get(), conjuncts, row_desc));
    ASSERT_EQ(conjuncts.size(), 0);

    std::shared_ptr<RuntimeFilterProducer> producer;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(RuntimeFilterProducer::create(
            _query_ctx.get(), runtime_filter_descs.data(), &producer));
    producer->set_wrapper_state_and_ready_to_publish(RuntimeFilterWrapper::State::READY);
    helper._consumers[0]->signal(producer.get());

    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            helper.acquire_runtime_filter(_runtime_states[0].get(), conjuncts, row_desc));
    ASSERT_EQ(conjuncts.size(), 1);

    conjuncts.clear();
    int arrived_rf_num = -1;
    helper._consumers[1]->signal(producer.get());
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(helper.try_append_late_arrival_runtime_filter(
            _runtime_states[0].get(), &arrived_rf_num, conjuncts, row_desc));
    ASSERT_EQ(conjuncts.size(), 1);
    ASSERT_EQ(arrived_rf_num, 2);
}

} // namespace doris
