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
#include <gen_cpp/Types_types.h>
#include <stddef.h>

#include <string>
#include <vector>

#include "common/status.h"
#include "runtime/types.h"
#include "util/runtime_profile.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/core/sort_description.h"
#include "vec/data_types/data_type.h"
#include "vec/exprs/vexpr_fwd.h"

namespace doris {
class RuntimeState;
class SlotDescriptor;
class ObjectPool;
class RowDescriptor;
class TExpr;
class TExprNode;
class TSortInfo;

namespace vectorized {
class Arena;
class Block;
class BufferWritable;
class IColumn;

class AggFnEvaluator {
    ENABLE_FACTORY_CREATOR(AggFnEvaluator);

public:
    static Status create(ObjectPool* pool, const TExpr& desc, const TSortInfo& sort_info,
                         AggFnEvaluator** result);

    Status prepare(RuntimeState* state, const RowDescriptor& desc,
                   const SlotDescriptor* intermediate_slot_desc,
                   const SlotDescriptor* output_slot_desc);

    void set_timer(RuntimeProfile::Counter* merge_timer, RuntimeProfile::Counter* expr_timer) {
        _merge_timer = merge_timer;
        _expr_timer = expr_timer;
    }

    Status open(RuntimeState* state);

    // create/destroy AGG Data
    void create(AggregateDataPtr place);
    void destroy(AggregateDataPtr place);

    // agg_function
    Status execute_single_add(Block* block, AggregateDataPtr place, Arena* arena = nullptr);

    Status execute_batch_add(Block* block, size_t offset, AggregateDataPtr* places,
                             Arena* arena = nullptr, bool agg_many = false);

    Status execute_batch_add_selected(Block* block, size_t offset, AggregateDataPtr* places,
                                      Arena* arena = nullptr);

    Status streaming_agg_serialize(Block* block, BufferWritable& buf, const size_t num_rows,
                                   Arena* arena);

    Status streaming_agg_serialize_to_column(Block* block, MutableColumnPtr& dst,
                                             const size_t num_rows, Arena* arena);

    void insert_result_info(AggregateDataPtr place, IColumn* column);

    void insert_result_info_vec(const std::vector<AggregateDataPtr>& place, size_t offset,
                                IColumn* column, const size_t num_rows);

    void reset(AggregateDataPtr place);

    DataTypePtr& data_type() { return _data_type; }

    const AggregateFunctionPtr& function() { return _function; }
    static std::string debug_string(const std::vector<AggFnEvaluator*>& exprs);
    std::string debug_string() const;
    bool is_merge() const { return _is_merge; }
    const VExprContextSPtrs& input_exprs_ctxs() const { return _input_exprs_ctxs; }

    static Status check_agg_fn_output(int key_size,
                                      const std::vector<vectorized::AggFnEvaluator*>& agg_fn,
                                      const RowDescriptor& output_row_desc);

    void set_version(const int version) { _function->set_version(version); }

    AggFnEvaluator* clone(RuntimeState* state, ObjectPool* pool);

private:
    const TFunction _fn;

    const bool _is_merge;

    AggFnEvaluator(const TExprNode& desc);
    AggFnEvaluator(AggFnEvaluator& evaluator, RuntimeState* state);

    Status _calc_argument_columns(Block* block);

    DataTypes _argument_types_with_sort;
    DataTypes _real_argument_types;

    const TypeDescriptor _return_type;

    const SlotDescriptor* _intermediate_slot_desc = nullptr;
    const SlotDescriptor* _output_slot_desc = nullptr;

    RuntimeProfile::Counter* _merge_timer = nullptr;
    RuntimeProfile::Counter* _expr_timer = nullptr;

    // input context
    VExprContextSPtrs _input_exprs_ctxs;

    SortDescription _sort_description;

    DataTypePtr _data_type;

    AggregateFunctionPtr _function;

    std::string _expr_name;

    std::vector<const IColumn*> _agg_columns;
};
} // namespace vectorized

} // namespace doris
