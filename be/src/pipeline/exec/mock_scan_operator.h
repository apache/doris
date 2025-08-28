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

#include "pipeline/exec/scan_operator.h"

#ifdef BE_TEST
namespace doris::pipeline {

class MockScanOperatorX;
class MockScanLocalState final : public ScanLocalState<MockScanLocalState> {
public:
    using Parent = MockScanOperatorX;
    friend class MockScanOperatorX;
    ENABLE_FACTORY_CREATOR(MockScanLocalState);
    MockScanLocalState(RuntimeState* state, OperatorXBase* parent)
            : ScanLocalState(state, parent) {}

private:
    PushDownType _should_push_down_bloom_filter() override { return PushDownType::ACCEPTABLE; }

    PushDownType _should_push_down_bitmap_filter() override { return PushDownType::ACCEPTABLE; }

    PushDownType _should_push_down_is_null_predicate() override { return PushDownType::ACCEPTABLE; }

    bool _should_push_down_common_expr() override { return true; }
};

class MockScanOperatorX final : public ScanOperatorX<MockScanLocalState> {
public:
    friend class OlapScanLocalState;
    MockScanOperatorX() = default;
};
} // namespace doris::pipeline
#endif