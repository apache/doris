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

#include <cstddef>
#include <string>
#include <vector>

#include "common/be_mock_util.h"
#include "common/status.h"
#include "core/data_type/data_type.h"
#include "exec/sort/sort_description.h"
#include "exprs/aggregate/aggregate_function.h"
#include "exprs/vexpr_fwd.h"
#include "runtime/runtime_profile.h"

namespace doris {

class RuntimeState;
class SlotDescriptor;
class ObjectPool;
class RowDescriptor;
class TExpr;
class TExprNode;
class TSortInfo;

class Arena;
class Block;
class BufferWritable;
class IColumn;
class QueryContext;

class AggFnEvaluator {
public:
    ENABLE_FACTORY_CREATOR(AggFnEvaluator);
    MOCK_DEFINE(virtual) ~AggFnEvaluator() = default;

public:
    static Status create(ObjectPool* pool, const TExpr& desc, const TSortInfo& sort_info,
                         const bool without_key, const bool is_window_function,
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
    Status execute_single_add(Block* block, AggregateDataPtr place, Arena& arena);

    Status execute_batch_add(Block* block, size_t offset, AggregateDataPtr* places, Arena& arena,
                             bool agg_many = false);

    Status execute_batch_add_selected(Block* block, size_t offset, AggregateDataPtr* places,
                                      Arena& arena);

    Status streaming_agg_serialize_to_column(Block* block, MutableColumnPtr& dst,
                                             const size_t num_rows, Arena& arena);

    void add_range_single_place(int64_t partition_start, int64_t partition_end, int64_t frame_start,
                                int64_t frame_end, AggregateDataPtr place, const IColumn** columns,
                                Arena& arena, UInt8* use_null_result,
                                UInt8* could_use_previous_result);

    void execute_function_with_incremental(int64_t partition_start, int64_t partition_end,
                                           int64_t frame_start, int64_t frame_end,
                                           AggregateDataPtr place, const IColumn** columns,
                                           Arena& arena, bool previous_is_nul, bool end_is_nul,
                                           bool has_null, UInt8* use_null_result,
                                           UInt8* could_use_previous_result);

    void insert_result_info(AggregateDataPtr place, IColumn* column);

    void insert_result_info_vec(const std::vector<AggregateDataPtr>& place, size_t offset,
                                IColumn* column, const size_t num_rows);

    void insert_result_info_range(ConstAggregateDataPtr place, IColumn* column, size_t start,
                                  size_t end);

    void reset(AggregateDataPtr place);

    DataTypePtr& data_type() { return _data_type; }

    String get_name() const { return _function->get_name(); }
    DataTypePtr get_return_type() const { return _function->get_return_type(); }
    size_t size_of_data() const { return _function->size_of_data(); }
    size_t align_of_data() const { return _function->align_of_data(); }
    bool result_column_could_resize() const { return _function->result_column_could_resize(); }
    bool supported_incremental_mode() const { return _function->supported_incremental_mode(); }
    bool is_simple_count() const { return _function->is_simple_count(); }
    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena& arena) const {
        _function->merge(place, rhs, arena);
    }
    void serialize_to_column(const std::vector<AggregateDataPtr>& places, size_t offset,
                             MutableColumnPtr& dst, const size_t num_rows) const {
        _function->serialize_to_column(places, offset, dst, num_rows);
    }
    void serialize_without_key_to_column(ConstAggregateDataPtr place, IColumn& to) const {
        _function->serialize_without_key_to_column(place, to);
    }
    void deserialize_and_merge_from_column(AggregateDataPtr place, const IColumn& column,
                                           Arena& arena) const {
        _function->deserialize_and_merge_from_column(place, column, arena);
    }
    void deserialize_and_merge_from_column_range(AggregateDataPtr place, const IColumn& column,
                                                 size_t begin, size_t end, Arena& arena) const {
        _function->deserialize_and_merge_from_column_range(place, column, begin, end, arena);
    }
    void deserialize_and_merge_vec(const AggregateDataPtr* places, size_t offset,
                                   AggregateDataPtr rhs, const IColumn* column, Arena& arena,
                                   const size_t num_rows) const {
        _function->deserialize_and_merge_vec(places, offset, rhs, column, arena, num_rows);
    }
    void deserialize_and_merge_vec_selected(const AggregateDataPtr* places, size_t offset,
                                            AggregateDataPtr rhs, const IColumn* column,
                                            Arena& arena, const size_t num_rows) const {
        _function->deserialize_and_merge_vec_selected(places, offset, rhs, column, arena, num_rows);
    }
    MutableColumnPtr create_serialize_column() const {
        return _function->create_serialize_column();
    }
    DataTypePtr get_serialized_type() const { return _function->get_serialized_type(); }
    void set_query_context(QueryContext* context) { _function->set_query_context(context); }

    static std::string debug_string(const std::vector<AggFnEvaluator*>& exprs);
    std::string debug_string() const;
    bool is_merge() const { return _is_merge; }
    const VExprContextSPtrs& input_exprs_ctxs() const { return _input_exprs_ctxs; }

    static Status check_agg_fn_output(uint32_t key_size, const std::vector<AggFnEvaluator*>& agg_fn,
                                      const RowDescriptor& output_row_desc);

    void set_version(const int version) { _function->set_version(version); }

    AggFnEvaluator* clone(RuntimeState* state, ObjectPool* pool);

    bool is_blockable() const;

private:
    const TFunction _fn;

    const bool _is_merge;
    // We need this flag to distinguish between the two types of aggregation functions:
    // 1. executed without group by key (agg function used with window function is also regarded as this type)
    // 2. executed with group by key
    const bool _without_key;

    const bool _is_window_function;

    AggFnEvaluator(const TExprNode& desc, const bool without_key, const bool is_window_function);
    AggFnEvaluator(AggFnEvaluator& evaluator, RuntimeState* state);

#ifdef BE_TEST
    AggFnEvaluator(bool is_merge, bool without_key, const bool is_window_function)
            : _is_merge(is_merge),
              _without_key(without_key),
              _is_window_function(is_window_function) {};
#endif
    Status _calc_argument_columns(Block* block);

    DataTypes _argument_types_with_sort;
    DataTypes _real_argument_types;

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

} // namespace doris
