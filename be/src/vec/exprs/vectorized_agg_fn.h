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
#include "runtime/types.h"
#include "util/runtime_profile.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type.h"
#include "vec/exprs/vexpr_context.h"

namespace doris {
class RuntimeState;
class SlotDescriptor;
namespace vectorized {
class AggFnEvaluator {
public:
    static Status create(ObjectPool* pool, const TExpr& desc, AggFnEvaluator** result);

    Status prepare(RuntimeState* state, const RowDescriptor& desc, MemPool* pool,
                   const SlotDescriptor* intermediate_slot_desc,
                   const SlotDescriptor* output_slot_desc,
                   const std::shared_ptr<MemTracker>& mem_tracker);

    void set_timer(RuntimeProfile::Counter* exec_timer, RuntimeProfile::Counter* merge_timer,
                   RuntimeProfile::Counter* expr_timer) {
        _exec_timer = exec_timer;
        _merge_timer = merge_timer;
        _expr_timer = expr_timer;
    }

    Status open(RuntimeState* state);

    void close(RuntimeState* state);

    // create/destroy AGG Data
    void create(AggregateDataPtr place);
    void destroy(AggregateDataPtr place);

    // agg_function
    void execute_single_add(Block* block, AggregateDataPtr place, Arena* arena = nullptr);

    void execute_batch_add(Block* block, size_t offset, AggregateDataPtr* places,
                           Arena* arena = nullptr);

    void insert_result_info(AggregateDataPtr place, IColumn* column);

    void reset(AggregateDataPtr place);

    DataTypePtr& data_type() { return _data_type; }

    const AggregateFunctionPtr& function() { return _function; }
    static std::string debug_string(const std::vector<AggFnEvaluator*>& exprs);
    std::string debug_string() const;
    bool is_merge() const { return _is_merge; }
    const std::vector<VExprContext*>& input_exprs_ctxs() const { return _input_exprs_ctxs; }

private:
    const TFunction _fn;

    const bool _is_merge;

    AggFnEvaluator(const TExprNode& desc);

    void _calc_argment_columns(Block* block);

    const TypeDescriptor _return_type;
    const TypeDescriptor _intermediate_type;

    const SlotDescriptor* _intermediate_slot_desc;
    const SlotDescriptor* _output_slot_desc;

    RuntimeProfile::Counter* _exec_timer;
    RuntimeProfile::Counter* _merge_timer;
    RuntimeProfile::Counter* _expr_timer;

    // input context
    std::vector<VExprContext*> _input_exprs_ctxs;

    DataTypePtr _data_type;

    AggregateFunctionPtr _function;

    std::string _expr_name;

    std::vector<const IColumn*> _agg_columns;
};
} // namespace vectorized

} // namespace doris
