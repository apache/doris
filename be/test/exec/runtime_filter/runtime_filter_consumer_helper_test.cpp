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

#include "common/object_pool.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_number.h"
#include "exec/operator/hashjoin_build_sink.h"
#include "exec/operator/mock_operator.h"
#include "exec/operator/operator.h"
#include "exec/pipeline/pipeline_task.h"
#include "exec/runtime_filter/runtime_filter_consumer.h"
#include "exec/runtime_filter/runtime_filter_consumer_helper.h"
#include "exec/runtime_filter/runtime_filter_test_utils.h"
#include "runtime/descriptors.h"

namespace doris {

class RuntimeFilterConsumerHelperTest : public RuntimeFilterTest {
    void SetUp() override {
        auto* exec_env = ExecEnv::GetInstance();
        _previous_timer_queue = exec_env->runtime_filter_timer_queue();
        exec_env->_init_runtime_filter_timer_queue();

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
    }

    void TearDown() override {
        // Each test creates a timer queue. Stop it and restore the previous queue so running
        // multiple helper tests does not leak detached timer threads or affect other suites.
        auto* exec_env = ExecEnv::GetInstance();
        exec_env->runtime_filter_timer_queue()->stop();
        delete exec_env->runtime_filter_timer_queue();
        exec_env->_runtime_filter_timer_queue = _previous_timer_queue;
        RuntimeFilterTest::TearDown();
    }

    OperatorPtr _op;
    DataSinkOperatorPtr _sink;
    PipelinePtr _pipeline;
    std::shared_ptr<PipelineTask> _task;
    ObjectPool _pool;
    RuntimeFilterTimerQueue* _previous_timer_queue = nullptr;
};

TEST_F(RuntimeFilterConsumerHelperTest, basic) {
    VExprContextSPtr ctx;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            VExpr::create_expr_tree(TRuntimeFilterDescBuilder::get_default_expr(), ctx));
    ctx->_last_result_column_id = 0;

    VExprContextSPtrs build_expr_ctxs = {ctx};
    std::vector<TRuntimeFilterDesc> runtime_filter_descs = {
            TRuntimeFilterDescBuilder().add_planId_to_target_expr(0).build(),
            TRuntimeFilterDescBuilder().add_planId_to_target_expr(0).build()};

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

    VExprContextSPtrs conjuncts;
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
            _runtime_states[0].get(), row_desc, arrived_rf_num, conjuncts));
    ASSERT_EQ(conjuncts.size(), 1);
    ASSERT_EQ(arrived_rf_num, 2);
}

TEST_F(RuntimeFilterConsumerHelperTest, register_consumers_without_wait) {
    auto target_expr = TRuntimeFilterDescBuilder::get_default_expr();
    target_expr.nodes[0].slot_ref.__set_slot_id(7);
    for (bool need_local_merge : {false, true}) {
        for (bool has_remote_targets : {false, true}) {
            auto desc = TRuntimeFilterDescBuilder(2 * need_local_merge + has_remote_targets)
                                .set_mode(!has_remote_targets)
                                .add_planId_to_target_expr(0)
                                .add_planId_to_target_expr(1, target_expr)
                                .build();
            RuntimeFilterConsumerHelper helper({desc});
            FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
                    helper.register_consumers(_runtime_states[0].get(), need_local_merge, 1));

            auto* mgr = has_remote_targets || need_local_merge
                                ? _runtime_states[0]->global_runtime_filter_mgr()
                                : _runtime_states[0]->local_runtime_filter_mgr();
            auto consumers = mgr->get_consume_filters(desc.filter_id);
            ASSERT_EQ(consumers.size(), 1);
            ASSERT_EQ(helper._consumers.size(), 1);
            EXPECT_EQ(consumers[0], helper._consumers[0]);
            EXPECT_EQ(consumers[0]->_probe_expr.nodes[0].slot_ref.slot_id, 7);
            EXPECT_TRUE(consumers[0]->_filter_timer.empty());
            EXPECT_FALSE(consumers[0]->is_applied());
        }
    }
}

TEST_F(RuntimeFilterConsumerHelperTest, late_arrival_without_wait) {
    auto desc = TRuntimeFilterDescBuilder()
                        .add_planId_to_target_expr(0)
                        .add_planId_to_target_expr(1)
                        .build();
    RuntimeFilterConsumerHelper waiting_helper({desc});
    RuntimeFilterConsumerHelper non_waiting_helper({desc});
    auto* state = _runtime_states[0].get();
    std::vector<std::shared_ptr<Dependency>> dependencies;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            waiting_helper.init(state, true, 0, 0, dependencies, "RF_DEPENDENCY"));
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(non_waiting_helper.register_consumers(state, true, 1));

    ASSERT_EQ(dependencies.size(), 1);
    ASSERT_EQ(waiting_helper._consumers[0]->_filter_timer.size(), 1);
    EXPECT_FALSE(dependencies[0]->ready());
    EXPECT_TRUE(non_waiting_helper._consumers[0]->_filter_timer.empty());
    ASSERT_EQ(state->global_runtime_filter_mgr()->get_consume_filters(desc.filter_id).size(), 2);

    SlotDescriptor slot_desc;
    slot_desc._type = DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_INT, false);
    TupleDescriptor tuple_desc;
    tuple_desc.add_slot(&slot_desc);
    RowDescriptor row_desc;
    _tbl._slot_desc_map[0] = &slot_desc;
    const_cast<std::vector<TupleDescriptor*>&>(row_desc._tuple_desc_map).push_back(&tuple_desc);

    VExprContextSPtrs non_waiting_conjuncts;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            non_waiting_helper.acquire_runtime_filter(state, non_waiting_conjuncts, row_desc));
    EXPECT_TRUE(non_waiting_conjuncts.empty());
    EXPECT_FALSE(non_waiting_helper._is_all_rf_applied);

    std::shared_ptr<RuntimeFilterProducer> producer;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            RuntimeFilterProducer::create(_query_ctx.get(), &desc, &producer));
    producer->set_wrapper_state_and_ready_to_publish(RuntimeFilterWrapper::State::READY);
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(producer->_send_to_local_targets(state, producer.get(), true));
    EXPECT_TRUE(dependencies[0]->ready());

    int arrived_rf_num = 0;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(non_waiting_helper.try_append_late_arrival_runtime_filter(
            state, row_desc, arrived_rf_num, non_waiting_conjuncts));
    ASSERT_EQ(non_waiting_conjuncts.size(), 1);
    EXPECT_EQ(arrived_rf_num, 1);
    EXPECT_TRUE(non_waiting_helper._is_all_rf_applied);

    // Each target consumes the same published filter independently. Acquiring it at the
    // non-waiting target must not prevent the waiting target from acquiring its expression.
    VExprContextSPtrs waiting_conjuncts;
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(
            waiting_helper.acquire_runtime_filter(state, waiting_conjuncts, row_desc));
    ASSERT_EQ(waiting_conjuncts.size(), 1);
    FAIL_IF_ERROR_OR_CATCH_EXCEPTION(non_waiting_helper.try_append_late_arrival_runtime_filter(
            state, row_desc, arrived_rf_num, non_waiting_conjuncts));
    EXPECT_EQ(non_waiting_conjuncts.size(), 1);
}

} // namespace doris
