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

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include "vec/exprs/vexpr.h"

namespace doris {
namespace vectorized {

class MockVExpr : public VExpr {
public:
    MOCK_CONST_METHOD1(clone, VExpr*(ObjectPool* pool));
    MOCK_CONST_METHOD0(expr_name, const std::string&());
    MOCK_METHOD3(execute,
                 Status(VExprContext* context, vectorized::Block* block, int* result_column_id));
}; // class MockVExpr

} // namespace vectorized
} // namespace doris
