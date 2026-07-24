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

#include "exec/operator/olap_scan_operator.h"

#include <gtest/gtest.h>

#include <memory>

#include "exec/operator/mock_scan_operator.h"
#include "gen_cpp/PlanNodes_types.h"
#include "testutil/mock/mock_runtime_state.h"

namespace doris {

// Verifies that when a scan is a binlog merge scan (MIN_DELTA / DETAIL), all predicate
// push-down decisions return UNACCEPTABLE / false. MIN_DELTA / DETAIL scans read through
// BlockReader's merge (op synthesis + BEFORE/AFTER split), so predicates must be kept above
// the storage reader; pushing them down would filter rows before the merge and produce wrong
// results.
class OlapScanOperatorBinlogPushDownTest : public testing::Test {
protected:
    void SetUp() override {
        _state = std::make_shared<MockRuntimeState>();
        // The push-down methods only read _scan_ranges in the binlog branch and never cast the
        // parent, so a default-constructible MockScanOperatorX is a sufficient parent here.
        _parent = std::make_shared<MockScanOperatorX>();
        _local_state = OlapScanLocalState::create_shared(_state.get(), _parent.get());

        auto range = std::make_unique<TPaloScanRange>();
        range->__set_binlog_scan_type(TBinlogScanType::MIN_DELTA);
        _local_state->_scan_ranges.push_back(std::move(range));
        ASSERT_TRUE(_local_state->_is_binlog_merge_scan());
    }

    std::shared_ptr<MockRuntimeState> _state;
    std::shared_ptr<MockScanOperatorX> _parent;
    std::shared_ptr<OlapScanLocalState> _local_state;
};

TEST_F(OlapScanOperatorBinlogPushDownTest, DisableBloomFilterPushDown) {
    EXPECT_EQ(_local_state->_should_push_down_bloom_filter(), PushDownType::UNACCEPTABLE);
}

TEST_F(OlapScanOperatorBinlogPushDownTest, DisableTopnFilterPushDown) {
    EXPECT_EQ(_local_state->_should_push_down_topn_filter(), PushDownType::UNACCEPTABLE);
}

TEST_F(OlapScanOperatorBinlogPushDownTest, DisableInPredicatePushDown) {
    EXPECT_EQ(_local_state->_should_push_down_in_predicate(), PushDownType::UNACCEPTABLE);
}

TEST_F(OlapScanOperatorBinlogPushDownTest, DisableIsNullPredicatePushDown) {
    // The binlog branch returns before dereferencing fn_call, so nullptr is safe here.
    EXPECT_EQ(_local_state->_should_push_down_is_null_predicate(nullptr),
              PushDownType::UNACCEPTABLE);
}

TEST_F(OlapScanOperatorBinlogPushDownTest, DisableBinaryPredicatePushDown) {
    Field constant_val;
    EXPECT_EQ(_local_state->_should_push_down_binary_predicate(nullptr, nullptr, constant_val, {}),
              PushDownType::UNACCEPTABLE);
}

TEST_F(OlapScanOperatorBinlogPushDownTest, DisableFunctionFilterPushDown) {
    PushDownType pdt = PushDownType::ACCEPTABLE;
    EXPECT_TRUE(
            _local_state->_should_push_down_function_filter(nullptr, nullptr, nullptr, nullptr, pdt)
                    .ok());
    EXPECT_EQ(pdt, PushDownType::UNACCEPTABLE);
}

TEST_F(OlapScanOperatorBinlogPushDownTest, DisableCommonExprPushDown) {
    EXPECT_FALSE(_local_state->_should_push_down_common_expr(VExprSPtr {}));
}

} // namespace doris
