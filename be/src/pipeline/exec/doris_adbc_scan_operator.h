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
#include "common/compile_check_begin.h"

namespace vectorized {
class NewDorisAdbcScanner;
}
} // namespace doris

namespace doris::pipeline {

class DorisAdbcScanOperatorX;
class DorisAdbcScanLocalState final : public ScanLocalState<DorisAdbcScanLocalState> {
public:
    using Parent = DorisAdbcScanOperatorX;
    using Base = ScanLocalState<DorisAdbcScanLocalState>;
    ENABLE_FACTORY_CREATOR(DorisAdbcScanLocalState);
    DorisAdbcScanLocalState(RuntimeState* state, OperatorXBase* parent) : Base(state, parent) {}
    Status _init_scanners(std::list<vectorized::ScannerSPtr>* scanners) override;

    std::string name_suffix() const override;

private:
    friend class vectorized::NewDorisAdbcScanner;
    void set_scan_ranges(RuntimeState* state,
                         const std::vector<TScanRangeParams>& scan_ranges) override;
    std::vector<std::unique_ptr<TDorisArrowScanRange>> _scan_ranges;
};

class DorisAdbcScanOperatorX final : public ScanOperatorX<DorisAdbcScanLocalState> {
public:
    DorisAdbcScanOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                    const DescriptorTbl& descs, int parallel_tasks);

private:
    friend class DorisAdbcScanLocalState;
    std::string _table_name;
    TupleId _tuple_id;
};

#include "common/compile_check_end.h"
} // namespace doris::pipeline
