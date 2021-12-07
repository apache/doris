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

#include "exec/exec_node.h"
#include "gen_cpp/PlanNodes_types.h"

namespace doris {

// Node for assert row count
class AssertNumRowsNode : public ExecNode {
public:
    AssertNumRowsNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    virtual ~AssertNumRowsNode() {};

    virtual Status init(const TPlanNode& tnode, RuntimeState* state = nullptr);
    virtual Status prepare(RuntimeState* state);
    virtual Status open(RuntimeState* state);
    virtual Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos);
    virtual Status close(RuntimeState* state);

private:
    int64_t _desired_num_rows;
    const std::string _subquery_string;
    TAssertion::type _assertion;
};

} // namespace doris
