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

#include "dependency.h"

#include <memory>
#include <mutex>

#include "common/logging.h"
#include "pipeline/pipeline_task.h"
#include "pipeline/pipeline_x/pipeline_x_task.h"
#include "runtime/memory/mem_tracker.h"

namespace doris::pipeline {

void Dependency::add_block_task(PipelineXTask* task) {
    std::unique_lock<std::mutex> lc(_task_lock);
    DCHECK(task->get_state() != PipelineTaskState::RUNNABLE);
    DCHECK(avoid_using_blocked_queue(task->get_state()));
    _block_task.push_back(task);
}

void Dependency::try_to_wake_up_task() {
    std::unique_lock<std::mutex> lc(_task_lock);
    if (_block_task.empty()) {
        return;
    }
    for (auto* task : _block_task) {
        DCHECK(task->get_state() != PipelineTaskState::RUNNABLE);
        if (task->try_wake_up(this)) {
            _wake_up_task_counter++;
        }
    }
    _block_task.clear();
}

template Status HashJoinDependency::extract_join_column<true>(
        vectorized::Block&,
        COW<vectorized::IColumn>::mutable_ptr<vectorized::ColumnVector<unsigned char>>&,
        std::vector<vectorized::IColumn const*, std::allocator<vectorized::IColumn const*>>&,
        std::vector<int, std::allocator<int>> const&);

template Status HashJoinDependency::extract_join_column<false>(
        vectorized::Block&,
        COW<vectorized::IColumn>::mutable_ptr<vectorized::ColumnVector<unsigned char>>&,
        std::vector<vectorized::IColumn const*, std::allocator<vectorized::IColumn const*>>&,
        std::vector<int, std::allocator<int>> const&);

std::string Dependency::debug_string(int indentation_level) {
    fmt::memory_buffer debug_string_buffer;
    fmt::format_to(debug_string_buffer, "{}{}: id={}, done={}",
                   std::string(indentation_level * 2, ' '), _name, _id,
                   read_blocked_by() == nullptr);
    return fmt::to_string(debug_string_buffer);
}

std::string AndDependency::debug_string(int indentation_level) {
    fmt::memory_buffer debug_string_buffer;
    fmt::format_to(debug_string_buffer, "{}{}: id={}, done={}, children=[",
                   std::string(indentation_level * 2, ' '), _name, _id,
                   read_blocked_by() == nullptr);
    for (auto& child : _children) {
        fmt::format_to(debug_string_buffer, "{}, \n", child->debug_string(indentation_level = 1));
    }
    fmt::format_to(debug_string_buffer, "{}]", std::string(indentation_level * 2, ' '));
    return fmt::to_string(debug_string_buffer);
}

std::string OrDependency::debug_string(int indentation_level) {
    fmt::memory_buffer debug_string_buffer;
    fmt::format_to(debug_string_buffer, "{}{}: id={}, done={}, children=[",
                   std::string(indentation_level * 2, ' '), _name, _id,
                   read_blocked_by() == nullptr);
    for (auto& child : _children) {
        fmt::format_to(debug_string_buffer, "{}, \n", child->debug_string(indentation_level = 1));
    }
    fmt::format_to(debug_string_buffer, "{}]", std::string(indentation_level * 2, ' '));
    return fmt::to_string(debug_string_buffer);
}

Status AggDependency::reset_hash_table() {
    return std::visit(
            [&](auto&& agg_method) {
                auto& hash_table = *agg_method.hash_table;
                using HashTableType = std::decay_t<decltype(hash_table)>;

                agg_method.reset();

                hash_table.for_each_mapped([&](auto& mapped) {
                    if (mapped) {
                        static_cast<void>(destroy_agg_status(mapped));
                        mapped = nullptr;
                    }
                });

                _agg_state.aggregate_data_container.reset(new vectorized::AggregateDataContainer(
                        sizeof(typename HashTableType::key_type),
                        ((_total_size_of_aggregate_states + _align_aggregate_states - 1) /
                         _align_aggregate_states) *
                                _align_aggregate_states));
                hash_table = HashTableType();
                _agg_state.agg_arena_pool.reset(new vectorized::Arena);
                return Status::OK();
            },
            _agg_state.agg_data->method_variant);
}

Status AggDependency::destroy_agg_status(vectorized::AggregateDataPtr data) {
    for (int i = 0; i < _agg_state.aggregate_evaluators.size(); ++i) {
        _agg_state.aggregate_evaluators[i]->function()->destroy(data +
                                                                _offsets_of_aggregate_states[i]);
    }
    return Status::OK();
}

Status AggDependency::create_agg_status(vectorized::AggregateDataPtr data) {
    for (int i = 0; i < _agg_state.aggregate_evaluators.size(); ++i) {
        try {
            _agg_state.aggregate_evaluators[i]->create(data + _offsets_of_aggregate_states[i]);
        } catch (...) {
            for (int j = 0; j < i; ++j) {
                _agg_state.aggregate_evaluators[j]->destroy(data + _offsets_of_aggregate_states[j]);
            }
            throw;
        }
    }
    return Status::OK();
}

Status AggDependency::merge_spilt_data() {
    CHECK(!_agg_state.spill_context.stream_ids.empty());

    for (auto& reader : _agg_state.spill_context.readers) {
        CHECK_LT(_agg_state.spill_context.read_cursor, reader->block_count());
        reader->seek(_agg_state.spill_context.read_cursor);
        vectorized::Block block;
        bool eos = false;
        RETURN_IF_ERROR(reader->read(&block, &eos));

        // TODO
        //        if (!block.empty()) {
        //            auto st = _merge_with_serialized_key_helper<false /* limit */, true /* for_spill */>(
        //                    &block);
        //            RETURN_IF_ERROR(st);
        //        }
    }
    _agg_state.spill_context.read_cursor++;
    return Status::OK();
}

void AggDependency::release_tracker() {
    mem_tracker()->release(_mem_usage_record.used_in_state + _mem_usage_record.used_in_arena);
}

vectorized::BlockRowPos AnalyticDependency::get_partition_by_end() {
    if (_analytic_state.current_row_position <
        _analytic_state.partition_by_end.pos) { //still have data, return partition_by_end directly
        return _analytic_state.partition_by_end;
    }

    if (_analytic_state.partition_by_eq_expr_ctxs.empty() ||
        (_analytic_state.input_total_rows == 0)) { //no partition_by, the all block is end
        return _analytic_state.all_block_end;
    }

    vectorized::BlockRowPos cal_end = _analytic_state.all_block_end;
    for (size_t i = 0; i < _analytic_state.partition_by_eq_expr_ctxs.size();
         ++i) { //have partition_by, binary search the partiton end
        cal_end = compare_row_to_find_end(_analytic_state.partition_by_column_idxs[i],
                                          _analytic_state.partition_by_end, cal_end);
    }
    cal_end.pos =
            _analytic_state.input_block_first_row_positions[cal_end.block_num] + cal_end.row_num;
    return cal_end;
}

//_partition_by_columns,_order_by_columns save in blocks, so if need to calculate the boundary, may find in which blocks firstly
vectorized::BlockRowPos AnalyticDependency::compare_row_to_find_end(int idx,
                                                                    vectorized::BlockRowPos start,
                                                                    vectorized::BlockRowPos end,
                                                                    bool need_check_first) {
    int64_t start_init_row_num = start.row_num;
    vectorized::ColumnPtr start_column =
            _analytic_state.input_blocks[start.block_num].get_by_position(idx).column;
    vectorized::ColumnPtr start_next_block_column = start_column;

    DCHECK_LE(start.block_num, end.block_num);
    DCHECK_LE(start.block_num, _analytic_state.input_blocks.size() - 1);
    int64_t start_block_num = start.block_num;
    int64_t end_block_num = end.block_num;
    int64_t mid_blcok_num = end.block_num;
    // To fix this problem: https://github.com/apache/doris/issues/15951
    // in this case, the partition by column is last row of block, so it's pointed to a new block at row = 0, range is: [left, right)
    // From the perspective of order by column, the two values are exactly equal.
    // so the range will be get wrong because it's compare_at == 0 with next block at row = 0
    if (need_check_first && end.block_num > 0 && end.row_num == 0) {
        end.block_num--;
        end_block_num--;
        end.row_num = _analytic_state.input_blocks[end_block_num].rows();
    }
    //binary search find in which block
    while (start_block_num < end_block_num) {
        mid_blcok_num = (start_block_num + end_block_num + 1) >> 1;
        start_next_block_column =
                _analytic_state.input_blocks[mid_blcok_num].get_by_position(idx).column;
        //Compares (*this)[n] and rhs[m], this: start[init_row]  rhs: mid[0]
        if (start_column->compare_at(start_init_row_num, 0, *start_next_block_column, 1) == 0) {
            start_block_num = mid_blcok_num;
        } else {
            end_block_num = mid_blcok_num - 1;
        }
    }

    // have check the start.block_num:  start_column[start_init_row_num] with mid_blcok_num start_next_block_column[0]
    // now next block must not be result, so need check with end_block_num: start_next_block_column[last_row]
    if (end_block_num == mid_blcok_num - 1) {
        start_next_block_column =
                _analytic_state.input_blocks[end_block_num].get_by_position(idx).column;
        int64_t block_size = _analytic_state.input_blocks[end_block_num].rows();
        if ((start_column->compare_at(start_init_row_num, block_size - 1, *start_next_block_column,
                                      1) == 0)) {
            start.block_num = end_block_num + 1;
            start.row_num = 0;
            return start;
        }
    }

    //check whether need get column again, maybe same as first init
    // if the start_block_num have move to forword, so need update start block num and compare it from row_num=0
    if (start_block_num != start.block_num) {
        start_init_row_num = 0;
        start.block_num = start_block_num;
        start_column = _analytic_state.input_blocks[start.block_num].get_by_position(idx).column;
    }
    //binary search, set start and end pos
    int64_t start_pos = start_init_row_num;
    int64_t end_pos = _analytic_state.input_blocks[start.block_num].rows();
    //if end_block_num haven't moved, only start_block_num go to the end block
    //so could use the end.row_num for binary search
    if (start.block_num == end.block_num) {
        end_pos = end.row_num;
    }
    while (start_pos < end_pos) {
        int64_t mid_pos = (start_pos + end_pos) >> 1;
        if (start_column->compare_at(start_init_row_num, mid_pos, *start_column, 1)) {
            end_pos = mid_pos;
        } else {
            start_pos = mid_pos + 1;
        }
    }
    start.row_num = start_pos; //update row num, return the find end
    return start;
}

bool AnalyticDependency::whether_need_next_partition(vectorized::BlockRowPos& found_partition_end) {
    if (_analytic_state.input_eos ||
        (_analytic_state.current_row_position <
         _analytic_state.partition_by_end.pos)) { //now still have partition data
        return false;
    }
    if ((_analytic_state.partition_by_eq_expr_ctxs.empty() && !_analytic_state.input_eos) ||
        (found_partition_end.pos == 0)) { //no partition, get until fetch to EOS
        return true;
    }
    if (!_analytic_state.partition_by_eq_expr_ctxs.empty() &&
        found_partition_end.pos == _analytic_state.all_block_end.pos &&
        !_analytic_state.input_eos) { //current partition data calculate done
        return true;
    }
    return false;
}

Status HashJoinDependency::do_evaluate(vectorized::Block& block,
                                       vectorized::VExprContextSPtrs& exprs,
                                       RuntimeProfile::Counter& expr_call_timer,
                                       std::vector<int>& res_col_ids) {
    for (size_t i = 0; i < exprs.size(); ++i) {
        int result_col_id = -1;
        // execute build column
        {
            SCOPED_TIMER(&expr_call_timer);
            RETURN_IF_ERROR(exprs[i]->execute(&block, &result_col_id));
        }

        // TODO: opt the column is const
        block.get_by_position(result_col_id).column =
                block.get_by_position(result_col_id).column->convert_to_full_column_if_const();
        res_col_ids[i] = result_col_id;
    }
    return Status::OK();
}

std::vector<uint16_t> HashJoinDependency::convert_block_to_null(vectorized::Block& block) {
    std::vector<uint16_t> results;
    for (int i = 0; i < block.columns(); ++i) {
        if (auto& column_type = block.safe_get_by_position(i); !column_type.type->is_nullable()) {
            DCHECK(!column_type.column->is_nullable());
            column_type.column = make_nullable(column_type.column);
            column_type.type = make_nullable(column_type.type);
            results.emplace_back(i);
        }
    }
    return results;
}

void SetSharedState::set_probe_finished_children(int child_id) {
    {
        std::unique_lock<std::mutex> lc(child_lock);
        probe_finished_children_index[child_id] = true;
    }
    for (SetDependency* dep : probe_finished_children_dependency) {
        if (dep->write_blocked_by() == nullptr) {
            dep->set_ready_for_write();
        }
    }
}

template <bool BuildSide>
Status HashJoinDependency::extract_join_column(vectorized::Block& block,
                                               vectorized::ColumnUInt8::MutablePtr& null_map,
                                               vectorized::ColumnRawPtrs& raw_ptrs,
                                               const std::vector<int>& res_col_ids) {
    for (size_t i = 0; i < _join_state.build_exprs_size; ++i) {
        if (_join_state.is_null_safe_eq_join[i]) {
            raw_ptrs[i] = block.get_by_position(res_col_ids[i]).column.get();
        } else {
            auto column = block.get_by_position(res_col_ids[i]).column.get();
            if (auto* nullable = check_and_get_column<vectorized::ColumnNullable>(*column)) {
                auto& col_nested = nullable->get_nested_column();
                auto& col_nullmap = nullable->get_null_map_data();

                if constexpr (!BuildSide) {
                    DCHECK(null_map != nullptr);
                    vectorized::VectorizedUtils::update_null_map(null_map->get_data(), col_nullmap);
                }
                if (_join_state.store_null_in_hash_table[i]) {
                    raw_ptrs[i] = nullable;
                } else {
                    if constexpr (BuildSide) {
                        DCHECK(null_map != nullptr);
                        vectorized::VectorizedUtils::update_null_map(null_map->get_data(),
                                                                     col_nullmap);
                    }
                    raw_ptrs[i] = &col_nested;
                }
            } else {
                raw_ptrs[i] = column;
            }
        }
    }
    return Status::OK();
}

bool RuntimeFilterTimer::has_ready() {
    std::unique_lock<std::mutex> lc(_lock);
    return _runtime_filter->is_ready();
}

void RuntimeFilterTimer::call_timeout() {
    std::unique_lock<std::mutex> lc(_lock);
    if (_call_ready) {
        return;
    }
    _call_timeout = true;
    if (_parent) {
        _parent->sub_filters();
    }
}

void RuntimeFilterTimer::call_ready() {
    std::unique_lock<std::mutex> lc(_lock);
    if (_call_timeout) {
        return;
    }
    _call_ready = true;
    if (_parent) {
        _parent->sub_filters();
    }
}

void RuntimeFilterTimer::call_has_ready() {
    std::unique_lock<std::mutex> lc(_lock);
    DCHECK(!_call_timeout);
    if (!_call_ready) {
        _parent->sub_filters();
    }
}

void RuntimeFilterTimer::call_has_release() {
    // When the use count is equal to 1, only the timer queue still holds ownership,
    // so there is no need to take any action.
}

struct RuntimeFilterTimerQueue {
    constexpr static int64_t interval = 50;
    void start() {
        while (true) {
            std::unique_lock<std::mutex> lk(cv_m);

            cv.wait(lk, [this] { return !_que.empty(); });
            {
                std::unique_lock<std::mutex> lc(_que_lock);
                std::list<std::shared_ptr<RuntimeFilterTimer>> new_que;
                for (auto& it : _que) {
                    if (it.use_count() == 1) {
                        it->call_has_release();
                    } else if (it->has_ready()) {
                        it->call_has_ready();
                    } else {
                        int64_t ms_since_registration = MonotonicMillis() - it->registration_time();
                        if (ms_since_registration > it->wait_time_ms()) {
                            it->call_timeout();
                        } else {
                            new_que.push_back(std::move(it));
                        }
                    }
                }
                new_que.swap(_que);
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(interval));
        }
    }
    ~RuntimeFilterTimerQueue() { _thread.detach(); }
    RuntimeFilterTimerQueue() { _thread = std::thread(&RuntimeFilterTimerQueue::start, this); }
    static void push_filter_timer(std::shared_ptr<RuntimeFilterTimer> filter) {
        static RuntimeFilterTimerQueue timer_que;

        timer_que.push(filter);
    }

    void push(std::shared_ptr<RuntimeFilterTimer> filter) {
        std::unique_lock<std::mutex> lc(_que_lock);
        _que.push_back(filter);
        cv.notify_all();
    }

    std::thread _thread;
    std::condition_variable cv;
    std::mutex cv_m;
    std::mutex _que_lock;

    std::list<std::shared_ptr<RuntimeFilterTimer>> _que;
};

void RuntimeFilterDependency::add_filters(IRuntimeFilter* runtime_filter) {
    _filters++;
    int64_t registration_time = runtime_filter->registration_time();
    int32 wait_time_ms = runtime_filter->wait_time_ms();
    auto filter_timer = std::make_shared<RuntimeFilterTimer>(
            registration_time, wait_time_ms,
            std::dynamic_pointer_cast<RuntimeFilterDependency>(shared_from_this()), runtime_filter);
    runtime_filter->set_filter_timer(filter_timer);
    RuntimeFilterTimerQueue::push_filter_timer(filter_timer);
}

void RuntimeFilterDependency::sub_filters() {
    _filters--;
    if (_filters == 0) {
        *_blocked_by_rf = false;
    }
}

} // namespace doris::pipeline
