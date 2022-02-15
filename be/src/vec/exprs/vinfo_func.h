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

#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/exprs/vexpr.h"

#include <iostream>
#include <string>

namespace doris {
class TExprNode;

namespace vectorized {
class VInfoFunc : public VExpr {
public:
    VInfoFunc(const TExprNode& node);
    virtual ~VInfoFunc() {}

    virtual VExpr* clone(doris::ObjectPool* pool) const override { return pool->add(new VInfoFunc(*this)); }
    virtual const std::string& expr_name() const override { return _expr_name; }
    virtual Status execute(VExprContext* context, vectorized::Block* block, int* result_column_id) override;

private:
    const std::string _expr_name = "vinfofunc expr";
    ColumnPtr _column_ptr;
};
} // namespace vectorized

} // namespace doris

