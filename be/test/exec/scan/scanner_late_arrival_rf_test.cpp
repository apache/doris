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

#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_number.h"
#include "exec/operator/mock_scan_operator.h"
#include "exec/runtime_filter/runtime_filter_consumer.h"
#include "exec/runtime_filter/runtime_filter_consumer_helper.h"
#include "exec/runtime_filter/runtime_filter_producer.h"
#include "exec/runtime_filter/runtime_filter_test_utils.h"
#include "exec/scan/scanner.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"

namespace doris {

// Minimal concrete Scanner so the abstract base can be instantiated. The
// behavior under test (`Scanner::try_append_late_arrival_runtime_filter`) lives
// entirely in the base class and never calls `_get_block_impl`.
class TestScanner final : public Scanner {
public:
    TestScanner(RuntimeState* state, ScanLocalStateBase* local_state, int64_t limit,
                RuntimeProfile* profile)
            : Scanner(state, local_state, limit, profile) {}

protected:
    Status _get_block_impl(RuntimeState* /*state*/, Block* /*block*/, bool* eof) override {
        *eof = true;
        return Status::OK();
    }
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

    std::vector<std::shared_ptr<Dependency>> rf_dependencies;
    ASSERT_TRUE(local_state->_helper.init(_runtime_states[0].get(), true, 0, 0, rf_dependencies, "")
                        .ok());

    auto scanner = std::make_unique<TestScanner>(_runtime_states[0].get(), local_state.get(),
                                                 -1 /*limit*/, &_profile);
    ASSERT_TRUE(scanner->init(_runtime_states[0].get(), {}).ok());
    ASSERT_EQ(scanner->_total_rf_num, 2);
    ASSERT_EQ(scanner->_applied_rf_num, 0);

    std::shared_ptr<RuntimeFilterProducer> producer;
    ASSERT_TRUE(RuntimeFilterProducer::create(_query_ctx.get(), rf_descs.data(), &producer).ok());
    producer->set_wrapper_state_and_ready_to_publish(RuntimeFilterWrapper::State::READY);
    local_state->_helper._consumers[0]->signal(producer.get());
    local_state->_helper._consumers[1]->signal(producer.get());

    // First call after both RFs arrived: counter must advance to total. Before
    // the fix this stayed at 0 because the assignment was missing.
    ASSERT_TRUE(scanner->try_append_late_arrival_runtime_filter().ok());
    ASSERT_EQ(scanner->_applied_rf_num, 2);

    // Second call: must hit the fast-path early return without re-cloning.
    // We clear `_conjuncts` and verify the function does NOT repopulate them;
    // if `_applied_rf_num` were still 0 the function would call
    // `clone_conjunct_ctxs` and overwrite the cleared vector.
    scanner->_conjuncts.clear();
    ASSERT_TRUE(scanner->try_append_late_arrival_runtime_filter().ok());
    ASSERT_EQ(scanner->_applied_rf_num, 2);
    ASSERT_TRUE(scanner->_conjuncts.empty());
}

} // namespace doris
