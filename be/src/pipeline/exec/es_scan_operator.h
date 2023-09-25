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
#include "pipeline/pipeline_x/operator.h"
#include "vec/exec/scan/vscan_node.h"

namespace doris {
class ExecNode;

namespace vectorized {
class NewOlapScanner;
}
} // namespace doris

namespace doris::pipeline {

class EsScanOperatorX;
class EsScanLocalState final : public ScanLocalState<EsScanLocalState> {
public:
    using Parent = EsScanOperatorX;
    using Base = ScanLocalState<EsScanLocalState>;
    ENABLE_FACTORY_CREATOR(EsScanLocalState);
    EsScanLocalState(RuntimeState* state, OperatorXBase* parent) : Base(state, parent) {}

private:
    friend class vectorized::NewOlapScanner;

    void set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) override;
    Status _init_profile() override;
    Status _process_conjuncts() override;
    Status _init_scanners(std::list<vectorized::VScannerSPtr>* scanners) override;

    std::vector<std::unique_ptr<TEsScanRange>> _scan_ranges;
    std::unique_ptr<RuntimeProfile> _es_profile;
    RuntimeProfile::Counter* _rows_read_counter;
    RuntimeProfile::Counter* _read_timer;
    RuntimeProfile::Counter* _materialize_timer;
};

class EsScanOperatorX final : public ScanOperatorX<EsScanLocalState> {
public:
    EsScanOperatorX(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    Status prepare(RuntimeState* state) override;

private:
    friend class EsScanLocalState;

    TupleId _tuple_id;
    TupleDescriptor* _tuple_desc;

    std::map<std::string, std::string> _properties;
    std::map<std::string, std::string> _fields_context;
    std::map<std::string, std::string> _docvalue_context;

    std::vector<std::string> _column_names;
};

} // namespace doris::pipeline
