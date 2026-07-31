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

#include "exec/runtime_filter/runtime_filter_consumer_helper.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <atomic>
#include <memory>
#include <vector>

#include "common/object_pool.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_number.h"
#include "exec/operator/hashjoin_build_sink.h"
#include "exec/operator/mock_operator.h"
#include "exec/operator/operator.h"
#include "exec/pipeline/pipeline_task.h"
#include "exec/runtime_filter/runtime_filter_consumer.h"
#include "exec/runtime_filter/runtime_filter_producer.h"
#include "exec/runtime_filter/runtime_filter_test_utils.h"
#include "runtime/descriptors.h"

namespace doris {

class RuntimeFilterConsumerHelperTest : public RuntimeFilterTest {
    void SetUp() override {
        RuntimeFilterTest::SetUp();
        _pipeline = std::make_shared<Pipeline>(0, INSTANCE_NUM, INSTANCE_NUM);
        _op.reset(new MockOperatorX());
        FAIL_IF_ERROR_OR_CATCH_EXCEPTION(_pipeline->add_operator(_op, 2));

        _sink.reset(new HashJoinBuildSinkOperatorX(
                &_pool, 0, _op->operator_id(),
                TPlanNodeBuilder(0, TPlanNodeType::HASH_JOIN_NODE).build(), _tbl));
        FAIL_IF_ERROR_OR_CATCH_EXCEPTION(_pipeline->set_sink(_sink));

        _task.reset(new PipelineTask(_pipeline, 0, _runtime_states[0].get(), nullptr, &_profile, {},
                                     0));

        ExecEnv::GetInstance()->_init_runtime_filter_timer_queue();
    }

    OperatorPtr _op;
    DataSinkOperatorPtr _sink;
    PipelinePtr _pipeline;
    std::shared_ptr<PipelineTask> _task;
    ObjectPool _pool;
};

TEST_F(RuntimeFilterConsumerHelperTest, late_runtime_filter_container) {
    std::vector<TRuntimeFilterDesc> runtime_filter_descs = {
            TRuntimeFilterDescBuilder(101).add_planId_to_target_expr(0).build(),
            TRuntimeFilterDescBuilder(202)
                    .set_type(TRuntimeFilterType::MIN_MAX)
                    .add_planId_to_target_expr(0)
                    .build(),
            TRuntimeFilterDescBuilder(303).add_planId_to_target_expr(0).build()};

    std::vector<std::shared_ptr<Dependency>> runtime_filter_dependencies;
    SlotDescriptor slot_desc;
    slot_desc._type = DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_INT, false);
    TupleDescriptor tuple_desc;
    tuple_desc.add_slot(&slot_desc);
    RowDescriptor row_desc;
    _tbl._slot_desc_map[0] = &slot_desc;
    const_cast<std::vector<TupleDescriptor*>&>(row_desc._tuple_desc_map).push_back(&tuple_desc);
    auto helper = RuntimeFilterConsumerHelper(runtime_filter_descs);

    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            helper.init(_runtime_states[0].get(), true, 0, 0, runtime_filter_dependencies, ""));

    std::shared_ptr<RuntimeFilterProducer> ready_producer;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(RuntimeFilterProducer::create(
            _query_ctx.get(), &runtime_filter_descs[0], &ready_producer));
    ready_producer->set_wrapper_state_and_ready_to_publish(RuntimeFilterWrapper::State::READY);
    helper._consumers[0]->signal(ready_producer.get());

    VExprContextSPtrs conjuncts;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            helper.acquire_runtime_filter(_runtime_states[0].get(), conjuncts, row_desc));
    ASSERT_EQ(conjuncts.size(), 1);

    auto container = helper.late_runtime_filter_container();
    ASSERT_NE(container, nullptr);
    ASSERT_EQ(container->filters.size(), 2);
    EXPECT_EQ(container->filters[0].filter_id, 202);
    EXPECT_EQ(container->filters[1].filter_id, 303);
    EXPECT_FALSE(container->filters[0].valid.load(std::memory_order_acquire));
    EXPECT_FALSE(container->filters[1].valid.load(std::memory_order_acquire));
    EXPECT_EQ(container->filters[0].expr, nullptr);
    EXPECT_EQ(container->filters[1].expr, nullptr);
    EXPECT_EQ(container->arrived_cnt.load(std::memory_order_acquire), 0);
    const auto* fixed_entries = container->filters.data();

    std::shared_ptr<RuntimeFilterProducer> accepted_producer;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(RuntimeFilterProducer::create(
            _query_ctx.get(), &runtime_filter_descs[1], &accepted_producer));
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(accepted_producer->init(123));
    accepted_producer->set_wrapper_state_and_ready_to_publish(RuntimeFilterWrapper::State::READY);
    helper._consumers[1]->signal(accepted_producer.get());

    VExprContextSPtrs late_conjuncts;
    int arrived_rf_num = -1;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(helper.try_append_late_arrival_runtime_filter(
            _runtime_states[0].get(), row_desc, arrived_rf_num, late_conjuncts,
            [](const VExprSPtr&) { return true; }));
    ASSERT_EQ(late_conjuncts.size(), 2);
    EXPECT_EQ(arrived_rf_num, 2);
    EXPECT_EQ(container->filters.data(), fixed_entries);
    ASSERT_TRUE(container->filters[0].valid.load(std::memory_order_acquire));
    ASSERT_NE(container->filters[0].expr, nullptr);
    EXPECT_EQ(container->filters[0].expr->size(), 2);
    EXPECT_NE((*container->filters[0].expr)[0].get(), late_conjuncts[0].get());
    EXPECT_NE((*container->filters[0].expr)[1].get(), late_conjuncts[1].get());
    EXPECT_FALSE(container->filters[1].valid.load(std::memory_order_acquire));
    EXPECT_EQ(container->filters[1].expr, nullptr);
    EXPECT_EQ(container->arrived_cnt.load(std::memory_order_acquire), 1);

    std::shared_ptr<RuntimeFilterProducer> rejected_producer;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(RuntimeFilterProducer::create(
            _query_ctx.get(), &runtime_filter_descs[2], &rejected_producer));
    rejected_producer->set_wrapper_state_and_ready_to_publish(RuntimeFilterWrapper::State::READY);
    helper._consumers[2]->signal(rejected_producer.get());

    late_conjuncts.clear();
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(helper.try_append_late_arrival_runtime_filter(
            _runtime_states[0].get(), row_desc, arrived_rf_num, late_conjuncts,
            [](const VExprSPtr&) { return false; }));
    ASSERT_EQ(late_conjuncts.size(), 1);
    EXPECT_EQ(arrived_rf_num, 3);
    EXPECT_EQ(container->filters.data(), fixed_entries);
    EXPECT_FALSE(container->filters[1].valid.load(std::memory_order_acquire));
    EXPECT_EQ(container->filters[1].expr, nullptr);
    EXPECT_EQ(container->arrived_cnt.load(std::memory_order_acquire), 1);
}

} // namespace doris
