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

#pragma once

#include <stdint.h>

#include <string>

#include "common/status.h"
#include "operator.h"
#include "pipeline/exec/scan_operator.h"

namespace doris {

namespace vectorized {
class NewOlapScanner;
}
} // namespace doris

namespace doris::pipeline {

class MetaScanOperatorX;
class MetaScanLocalState final : public ScanLocalState<MetaScanLocalState> {
public:
    using Parent = MetaScanOperatorX;
    using Base = ScanLocalState<MetaScanLocalState>;
    ENABLE_FACTORY_CREATOR(MetaScanLocalState);
    MetaScanLocalState(RuntimeState* state, OperatorXBase* parent) : Base(state, parent) {}

private:
    friend class vectorized::NewOlapScanner;

    void set_scan_ranges(RuntimeState* state,
                         const std::vector<TScanRangeParams>& scan_ranges) override;
    Status _init_scanners(std::list<vectorized::VScannerSPtr>* scanners) override;
    Status _process_conjuncts(RuntimeState* state) override;

    std::vector<TScanRangeParams> _scan_ranges;
};

class MetaScanOperatorX final : public ScanOperatorX<MetaScanLocalState> {
public:
    MetaScanOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                      const DescriptorTbl& descs);

private:
    friend class MetaScanLocalState;

    TupleId _tuple_id;
    TUserIdentity _user_identity;
};

} // namespace doris::pipeline
