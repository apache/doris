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

#include <memory>
#include <string>

#include "common/object_pool.h"
#include "common/status.h"
#include "vec/exprs/function_context.h"
#include "vec/exprs/vexpr.h"

namespace doris {
class BloomFilterFuncBase;
class RowDescriptor;
class RuntimeState;
class TExprNode;
namespace vectorized {
class Block;
class VExprContext;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {
class VBloomPredicate final : public VExpr {
    ENABLE_FACTORY_CREATOR(VBloomPredicate);

public:
    VBloomPredicate(const TExprNode& node);
    ~VBloomPredicate() override = default;

    Status execute_column(VExprContext* context, const Block* block, Selector* selector,
                          size_t count, ColumnPtr& result_column) const override;

    Status execute_runtime_filter(VExprContext* context, const Block* block,
                                  const uint8_t* __restrict filter, size_t count,
                                  ColumnPtr& result_column, ColumnPtr* arg_column) const override;

    Status prepare(RuntimeState* state, const RowDescriptor& desc, VExprContext* context) override;
    Status open(RuntimeState* state, VExprContext* context,
                FunctionContext::FunctionStateScope scope) override;
    void close(VExprContext* context, FunctionContext::FunctionStateScope scope) override;
    const std::string& expr_name() const override;
    void set_filter(std::shared_ptr<BloomFilterFuncBase> filter);

    std::shared_ptr<BloomFilterFuncBase> get_bloom_filter_func() const override { return _filter; }

    uint64_t get_digest(uint64_t seed) const override;

private:
    Status _do_execute(VExprContext* context, const Block* block, const uint8_t* __restrict filter,
                       Selector* selector, size_t count, ColumnPtr& result_column) const;

    std::shared_ptr<BloomFilterFuncBase> _filter;
    inline static const std::string EXPR_NAME = "bloom_predicate";
};
} // namespace doris::vectorized
