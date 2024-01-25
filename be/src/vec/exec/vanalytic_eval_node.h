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

#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Types_types.h>
#include <stddef.h>
#include <stdint.h>

#include <atomic>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "common/global_types.h"
#include "common/status.h"
#include "exec/exec_node.h"
#include "util/runtime_profile.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/common/arena.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type.h"
#include "vec/exprs/vexpr_fwd.h"

namespace doris {
class DescriptorTbl;
class ObjectPool;
class RuntimeState;
class TupleDescriptor;

} // namespace doris

namespace doris::vectorized {

struct BlockRowPos {
    BlockRowPos() : block_num(0), row_num(0), pos(0) {}
    int64_t block_num; //the pos at which block
    int64_t row_num;   //the pos at which row
    int64_t pos;       //pos = all blocks size + row_num
    std::string debug_string() {
        std::string res = "\t block_num: ";
        res += std::to_string(block_num);
        res += "\t row_num: ";
        res += std::to_string(row_num);
        res += "\t pos: ";
        res += std::to_string(pos);
        return res;
    }
};

class AggFnEvaluator;

enum AnalyticFnScope { PARTITION, RANGE, ROWS };

class VAnalyticEvalNode : public ExecNode {
public:
    ~VAnalyticEvalNode() override = default;
    VAnalyticEvalNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;
    Status get_next(RuntimeState* state, vectorized::Block* block, bool* eos) override;
    Status close(RuntimeState* state) override;
    Status alloc_resource(RuntimeState* state) override;
    void release_resource(RuntimeState* state) override;
    Status sink(doris::RuntimeState* state, vectorized::Block* input_block, bool eos) override;
    Status pull(doris::RuntimeState* state, vectorized::Block* output_block, bool* eos) override;
    bool can_read();
    bool can_write();

protected:
    using ExecNode::debug_string;
    virtual std::string debug_string();

private:
    Status _get_next_for_rows(size_t rows);
    Status _get_next_for_range(size_t rows);
    Status _get_next_for_partition(size_t rows);

    void _execute_for_win_func(int64_t partition_start, int64_t partition_end, int64_t frame_start,
                               int64_t frame_end);

    Status _reset_agg_status();
    Status _init_result_columns();
    Status _create_agg_status();
    Status _destroy_agg_status();
    Status _insert_range_column(vectorized::Block* block, const VExprContextSPtr& expr,
                                IColumn* dst_column, size_t length);

    void _update_order_by_range();
    bool _init_next_partition(BlockRowPos found_partition_end);
    void _insert_result_info(int64_t current_block_rows);
    Status _output_current_block(Block* block);
    BlockRowPos _get_partition_by_end();
    BlockRowPos _compare_row_to_find_end(int idx, BlockRowPos start, BlockRowPos end,
                                         bool need_check_first = false);

    Status _fetch_next_block_data(RuntimeState* state);
    Status _consumed_block_and_init_partition(RuntimeState* state, bool* next_partition, bool* eos);
    bool whether_need_next_partition(BlockRowPos found_partition_end);

    std::string debug_window_bound_string(TAnalyticWindowBoundary b);
    using vectorized_execute = std::function<void(int64_t peer_group_start, int64_t peer_group_end,
                                                  int64_t frame_start, int64_t frame_end)>;
    using vectorized_get_next = std::function<Status(size_t rows)>;
    using vectorized_get_result = std::function<void(int64_t current_block_rows)>;
    using vectorized_closer = std::function<void()>;

    struct executor {
        vectorized_execute execute;
        vectorized_get_next get_next;
        vectorized_get_result insert_result;
        vectorized_closer close;
    };

    executor _executor;

    void _release_mem();

private:
    std::vector<Block> _input_blocks;
    std::vector<int64_t> input_block_first_row_positions;
    std::vector<AggFnEvaluator*> _agg_functions;
    std::vector<VExprContextSPtrs> _agg_expr_ctxs;
    VExprContextSPtrs _partition_by_eq_expr_ctxs;
    VExprContextSPtrs _order_by_eq_expr_ctxs;
    std::vector<std::vector<MutableColumnPtr>> _agg_intput_columns;
    std::vector<MutableColumnPtr> _result_window_columns;

    BlockRowPos _order_by_start;
    BlockRowPos _order_by_end;
    BlockRowPos _partition_by_start;
    BlockRowPos _partition_by_end;
    BlockRowPos _all_block_end;
    std::vector<int64_t> _ordey_by_column_idxs;
    std::vector<int64_t> _partition_by_column_idxs;

    bool _input_eos = false;
    bool _next_partition = false;
    std::atomic_bool _need_more_input = true;
    BlockRowPos _found_partition_end;
    int64_t _input_total_rows = 0;
    int64_t _output_block_index = 0;
    int64_t _window_end_position = 0;
    int64_t _current_row_position = 0;
    int64_t _rows_start_offset = 0;
    int64_t _rows_end_offset = 0;
    size_t _agg_functions_size = 0;
    bool _agg_functions_created = false;
    bool _current_window_empty = false;

    /// The offset of the n-th functions.
    std::vector<size_t> _offsets_of_aggregate_states;
    /// The total size of the row from the functions.
    size_t _total_size_of_aggregate_states = 0;
    /// The max align size for functions
    size_t _align_aggregate_states = 1;
    std::unique_ptr<Arena> _agg_arena_pool;
    AggregateDataPtr _fn_place_ptr;

    TTupleId _buffered_tuple_id = 0;
    TupleId _intermediate_tuple_id;
    TupleId _output_tuple_id;
    TAnalyticWindow _window;
    AnalyticFnScope _fn_scope;
    TupleDescriptor* _intermediate_tuple_desc;
    TupleDescriptor* _output_tuple_desc;
    std::vector<int64_t> _origin_cols;

    RuntimeProfile::Counter* _evaluation_timer;
    RuntimeProfile::Counter* _memory_usage_counter;
    RuntimeProfile::HighWaterMarkCounter* _blocks_memory_usage;

    std::vector<bool> _change_to_nullable_flags;
};
} // namespace doris::vectorized
