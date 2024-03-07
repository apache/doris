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

namespace vectorized {
class NewJdbcScanner;
}
} // namespace doris

namespace doris::pipeline {

class JDBCScanOperatorX;
class JDBCScanLocalState final : public ScanLocalState<JDBCScanLocalState> {
public:
    using Parent = JDBCScanOperatorX;
    ENABLE_FACTORY_CREATOR(JDBCScanLocalState);
    JDBCScanLocalState(RuntimeState* state, OperatorXBase* parent)
            : ScanLocalState<JDBCScanLocalState>(state, parent) {}
    Status _init_scanners(std::list<vectorized::VScannerSPtr>* scanners) override;

    std::string name_suffix() const override;

private:
    friend class vectorized::NewJdbcScanner;
};

class JDBCScanOperatorX final : public ScanOperatorX<JDBCScanLocalState> {
public:
    JDBCScanOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                      const DescriptorTbl& descs, int parallel_tasks);

private:
    friend class JDBCScanLocalState;
    std::string _table_name;
    TupleId _tuple_id;
    std::string _query_string;
    TOdbcTableType::type _table_type;
};

} // namespace doris::pipeline
