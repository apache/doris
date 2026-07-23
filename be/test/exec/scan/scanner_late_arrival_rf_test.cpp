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

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <list>

#include "common/object_pool.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_number.h"
#include "exec/operator/mock_scan_operator.h"
#include "exec/runtime_filter/runtime_filter_consumer.h"
#include "exec/runtime_filter/runtime_filter_consumer_helper.h"
#include "exec/runtime_filter/runtime_filter_producer.h"
#include "exec/runtime_filter/runtime_filter_test_utils.h"
#include "exec/scan/scanner.h"
// branch-4.1 creates VRuntimeFilterWrapper before the RuntimeFilterExpr rename, so assert the
// branch-native wrapper type while preserving the late-arrival batching invariant.
#include "exprs/vruntimefilter_wrapper.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_descriptors.h"
#include "testutil/mock/mock_runtime_state.h"
#include "testutil/mock/mock_slot_ref.h"

namespace doris {

// Minimal concrete Scanner so the abstract base can be instantiated. The
// behavior under test (`Scanner::try_append_late_arrival_runtime_filter`) lives
// entirely in the base class and never calls `_get_block_impl`.
class TestScanner final : public Scanner {
public:
    TestScanner(RuntimeState* state, ScanLocalStateBase* local_state, int64_t limit,
                RuntimeProfile* profile)
            : Scanner(state, local_state, limit, profile) {}

    void add_block(Block block) { _blocks.push_back(std::move(block)); }

protected:
    Status _get_block_impl(RuntimeState* /*state*/, Block* block, bool* eof) override {
        if (_blocks.empty()) {
            *eof = true;
            return Status::OK();
        }
        *eof = false;
        block->swap(_blocks.front());
        _blocks.pop_front();
        return Status::OK();
    }

private:
    std::list<Block> _blocks;
};

class HighCostPredicate final : public VExpr {
public:
    HighCostPredicate() : VExpr(std::make_shared<DataTypeUInt8>(), false) {}

    Status execute_column_impl(VExprContext*, const Block*, const Selector*, size_t count,
                               ColumnPtr& result_column) const override {
        result_column = ColumnUInt8::create(count, 1);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }
    double execute_cost() const override { return 100.0; }

private:
    const std::string _expr_name = "high_cost_stateful_predicate";
};

class ScannerLateArrivalRfTest : public RuntimeFilterTest {
public:
    void SetUp() override {
        RuntimeFilterTest::SetUp();
        // The runtime filter helper schedules a timer on the global queue when
        // it initialises, so the queue must exist before init() is called.
        ExecEnv::GetInstance()->_init_runtime_filter_timer_queue();
    }
};

// Regression: PR #59786 accidentally removed `_applied_rf_num = arrived_rf_num`
// from `Scanner::try_append_late_arrival_runtime_filter`, leaving the counter
// at 0 forever. Subsequent calls then re-clone conjunct ctxs every time, the
// `ApplyAllRuntimeFilters=True` info-string is never emitted, and the
// `_applied_rf_num <= _total_rf_num` DCHECK becomes dead. This test asserts
// the counter advances after RFs arrive and that the second call short-circuits
// via the fast path at the top of the function.
TEST_F(ScannerLateArrivalRfTest, applied_rf_num_advances_after_late_arrival) {
    _runtime_states[0]->_query_options.__set_enable_adjust_conjunct_order_by_cost(true);
    std::vector<TRuntimeFilterDesc> rf_descs = {
            TRuntimeFilterDescBuilder().add_planId_to_target_expr(0).build(),
            TRuntimeFilterDescBuilder().add_planId_to_target_expr(0).build()};

    SlotDescriptor slot_desc;
    slot_desc._type = DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_INT, false);
    TupleDescriptor tuple_desc;
    tuple_desc.add_slot(&slot_desc);
    RowDescriptor row_desc;
    _tbl._slot_desc_map[0] = &slot_desc;
    const_cast<std::vector<TupleDescriptor*>&>(row_desc._tuple_desc_map).push_back(&tuple_desc);

    // MockScanOperatorX feeds `_runtime_filter_descs` and `_row_descriptor`
    // into the local state's helper. Access to the protected members is
    // permitted by the build-wide `-fno-access-control` flag.
    auto op = std::make_shared<MockScanOperatorX>();
    op->_runtime_filter_descs = rf_descs;
    op->_row_descriptor = row_desc;

    auto local_state = std::make_shared<MockScanLocalState>(_runtime_states[0].get(), op.get());

    auto initial_conjunct = VExprContext::create_shared(std::make_shared<HighCostPredicate>());
    ASSERT_TRUE(initial_conjunct->prepare(_runtime_states[0].get(), row_desc).ok());
    ASSERT_TRUE(initial_conjunct->open(_runtime_states[0].get()).ok());
    local_state->_conjuncts.push_back(initial_conjunct);

    std::vector<std::shared_ptr<Dependency>> rf_dependencies;
    ASSERT_TRUE(local_state->_helper.init(_runtime_states[0].get(), true, 0, 0, rf_dependencies, "")
                        .ok());

    std::shared_ptr<RuntimeFilterProducer> producer;
    ASSERT_TRUE(RuntimeFilterProducer::create(_query_ctx.get(), rf_descs.data(), &producer).ok());
    producer->set_wrapper_state_and_ready_to_publish(RuntimeFilterWrapper::State::READY);
    local_state->_helper._consumers[0]->signal(producer.get());
    ASSERT_TRUE(local_state->_helper
                        .acquire_runtime_filter(_runtime_states[0].get(), local_state->_conjuncts,
                                                row_desc)
                        .ok());
    ASSERT_EQ(local_state->_conjuncts.size(), 2);

    auto scanner = std::make_unique<TestScanner>(_runtime_states[0].get(), local_state.get(),
                                                 -1 /*limit*/, &_profile);
    ASSERT_TRUE(scanner->init(_runtime_states[0].get(), local_state->_conjuncts).ok());
    auto second_scanner = std::make_unique<TestScanner>(_runtime_states[0].get(), local_state.get(),
                                                        -1 /*limit*/, &_profile);
    ASSERT_TRUE(second_scanner->init(_runtime_states[0].get(), local_state->_conjuncts).ok());
    ASSERT_EQ(scanner->_total_rf_num, 2);
    ASSERT_EQ(scanner->_applied_rf_num, 0);

    local_state->_helper._consumers[1]->signal(producer.get());

    // First call after both RFs arrived: counter must advance to total. Before
    // the fix this stayed at 0 because the assignment was missing.
    ASSERT_TRUE(scanner->try_append_late_arrival_runtime_filter().ok());
    ASSERT_EQ(scanner->_applied_rf_num, 2);
    ASSERT_EQ(scanner->_late_arrival_rf_conjuncts.size(), 1);
    for (const auto& conjunct : scanner->_late_arrival_rf_conjuncts) {
        EXPECT_NE(dynamic_cast<const VRuntimeFilterWrapper*>(conjunct->root().get()), nullptr);
    }
    ASSERT_EQ(scanner->_conjuncts.size(), 3);
    EXPECT_EQ(scanner->_conjuncts.back()->expr_name(), "high_cost_stateful_predicate");

    // The first scanner consumes the shared helper's expression, so another scanner can only get
    // the exact delta from the local state's append-only RF batch history.
    ASSERT_TRUE(second_scanner->try_append_late_arrival_runtime_filter().ok());
    ASSERT_EQ(second_scanner->_applied_rf_num, 2);
    ASSERT_EQ(second_scanner->_late_arrival_rf_conjuncts.size(), 1);
    EXPECT_NE(dynamic_cast<const VRuntimeFilterWrapper*>(
                      second_scanner->_late_arrival_rf_conjuncts[0]->root().get()),
              nullptr);

    // Second call: must hit the fast-path early return without re-cloning.
    // We clear `_conjuncts` and verify the function does NOT repopulate them;
    // if `_applied_rf_num` were still 0 the function would call
    // `clone_conjunct_ctxs` and overwrite the cleared vector.
    scanner->_conjuncts.clear();
    ASSERT_TRUE(scanner->try_append_late_arrival_runtime_filter().ok());
    ASSERT_EQ(scanner->_applied_rf_num, 2);
    ASSERT_TRUE(scanner->_conjuncts.empty());
}

TEST(ScannerProjectionTest, merges_padding_block_when_limit_eos_without_extra_flag) {
    ObjectPool pool;
    auto data_type = std::make_shared<DataTypeInt32>();
    auto row_descriptor = MockRowDescriptor({data_type}, &pool);

    MockRuntimeState state;
    state._batch_size = 6;

    auto op = std::make_shared<MockScanOperatorX>();
    op->_row_descriptor = row_descriptor;
    op->_output_row_descriptor =
            std::make_unique<MockRowDescriptor>(std::vector<DataTypePtr> {data_type}, &pool);
    op->_output_tuple_desc = op->_output_row_descriptor->tuple_descriptors()[0];

    auto local_state = std::make_shared<MockScanLocalState>(&state, op.get());
    local_state->_projections = MockSlotRef::create_mock_contexts(0, data_type);

    RuntimeProfile profile("scanner");
    TestScanner scanner(&state, local_state.get(), 7, &profile);
    ASSERT_TRUE(scanner.init(&state, {}).ok());
    scanner.add_block(ColumnHelper::create_block<DataTypeInt32>({0, 1}));
    scanner.add_block(ColumnHelper::create_block<DataTypeInt32>({2, 3, 4, 5, 6}));

    Block first_output;
    bool eos = false;
    ASSERT_TRUE(scanner.get_block_after_projects(&state, &first_output, &eos).ok());
    EXPECT_TRUE(eos);
    EXPECT_EQ(first_output.rows(), 7);
}

} // namespace doris
