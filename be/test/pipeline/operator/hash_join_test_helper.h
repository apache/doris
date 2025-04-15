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

#include <gen_cpp/DataSinks_types.h>
#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gmock/gmock-actions.h>
#include <gmock/gmock-function-mocker.h>
#include <gmock/gmock-spec-builders.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "join_test_helper.h"
#include "pipeline/exec/hashjoin_build_sink.h"
#include "pipeline/exec/hashjoin_probe_operator.h"

namespace doris::pipeline {
class HashJoinTestHelper : public JoinTestHelper {
public:
    TPlanNode create_test_plan_node(const TJoinOp::type& join_op_type,
                                    const std::vector<TPrimitiveType::type>& key_types,
                                    const std::vector<bool>& left_keys_nullable,
                                    const std::vector<bool>& right_keys_nullable,
                                    const bool is_mark_join = false,
                                    const size_t mark_join_conjuncts_size = 0,
                                    const bool null_safe_equal = false,
                                    const bool has_other_join_conjuncts = false);

    TDescriptorTable create_test_table_descriptor(TPlanNode& tnode);

    void add_mark_join_conjuncts(TPlanNode& join_node, std::vector<TExpr>& conjuncts);
    void add_other_join_conjuncts(TPlanNode& join_node, std::vector<TExpr>& conjuncts);

    std::pair<std::shared_ptr<HashJoinProbeOperatorX>, std::shared_ptr<HashJoinBuildSinkOperatorX>>
    create_operators(const TPlanNode& tnode);

protected:
    std::vector<TExprNode*> _left_slots, _right_slots;
};

} // namespace doris::pipeline