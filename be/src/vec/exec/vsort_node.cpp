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

#include "vec/exec/vsort_node.h"

#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/Metrics_types.h>
#include <gen_cpp/PlanNodes_types.h>

#include <atomic>
#include <functional>
#include <ostream>
#include <string>
#include <utility>

#include "common/status.h"
#include "runtime/descriptors.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/query_context.h"
#include "runtime/runtime_predicate.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"
#include "vec/common/sort/heap_sorter.h"
#include "vec/common/sort/topn_sorter.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/sort_description.h"

namespace doris {
class ObjectPool;
} // namespace doris

namespace doris::vectorized {

VSortNode::VSortNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs),
          _offset(tnode.sort_node.__isset.offset ? tnode.sort_node.offset : 0),
          _reuse_mem(true) {}

Status VSortNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    RETURN_IF_ERROR(_vsort_exec_exprs.init(tnode.sort_node.sort_info, _pool));
    _is_asc_order = tnode.sort_node.sort_info.is_asc_order;
    _nulls_first = tnode.sort_node.sort_info.nulls_first;
    const auto& row_desc = child(0)->row_desc();

    // If `limit` is smaller than HEAP_SORT_THRESHOLD, we consider using heap sort in priority.
    // To do heap sorting, each income block will be filtered by heap-top row. There will be some
    // `memcpy` operations. To ensure heap sort will not incur performance fallback, we should
    // exclude cases which incoming blocks has string column which is sensitive to operations like
    // `filter` and `memcpy`
    if (_limit > 0 && _limit + _offset < HeapSorter::HEAP_SORT_THRESHOLD &&
        (tnode.sort_node.sort_info.use_two_phase_read || !row_desc.has_varlen_slots())) {
        _sorter = HeapSorter::create_unique(_vsort_exec_exprs, _limit, _offset, _pool,
                                            _is_asc_order, _nulls_first, row_desc);
        _reuse_mem = false;
    } else if (_limit > 0 && row_desc.has_varlen_slots() &&
               _limit + _offset < TopNSorter::TOPN_SORT_THRESHOLD) {
        _sorter =
                TopNSorter::create_unique(_vsort_exec_exprs, _limit, _offset, _pool, _is_asc_order,
                                          _nulls_first, row_desc, state, _runtime_profile.get());
    } else {
        _sorter =
                FullSorter::create_unique(_vsort_exec_exprs, _limit, _offset, _pool, _is_asc_order,
                                          _nulls_first, row_desc, state, _runtime_profile.get());
    }

    _sorter->init_profile(_runtime_profile.get());
    return Status::OK();
}

Status VSortNode::prepare(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::prepare(state));
    SCOPED_TIMER(_exec_timer);
    _runtime_profile->add_info_string("TOP-N", _limit == -1 ? "false" : "true");

    _memory_usage_counter = ADD_LABEL_COUNTER(runtime_profile(), "MemoryUsage");
    _sort_blocks_memory_usage =
            ADD_CHILD_COUNTER(runtime_profile(), "SortBlocks", TUnit::BYTES, "MemoryUsage");

    RETURN_IF_ERROR(_vsort_exec_exprs.prepare(state, child(0)->row_desc(), _row_descriptor));
    _child_get_next_timer = ADD_TIMER(runtime_profile(), "ChildGetResultTime");
    _get_next_timer = ADD_TIMER(runtime_profile(), "GetResultTime");
    _sink_timer = ADD_TIMER(runtime_profile(), "PartialSortTotalTime");
    return Status::OK();
}

Status VSortNode::alloc_resource(doris::RuntimeState* state) {
    SCOPED_TIMER(_exec_timer);
    RETURN_IF_ERROR(ExecNode::alloc_resource(state));
    RETURN_IF_ERROR(_vsort_exec_exprs.open(state));
    RETURN_IF_CANCELLED(state);

    return Status::OK();
}

Status VSortNode::sink(RuntimeState* state, vectorized::Block* input_block, bool eos) {
    SCOPED_TIMER(_exec_timer);
    if (input_block->rows() > 0) {
        RETURN_IF_ERROR(_sorter->append_block(input_block));
        RETURN_IF_CANCELLED(state);
        if (!_reuse_mem) {
            input_block->clear();
        }
    }

    if (eos) {
        RETURN_IF_ERROR(_sorter->prepare_for_read());
        _can_read = true;
    }
    return Status::OK();
}

Status VSortNode::open(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));
    RETURN_IF_ERROR(child(0)->open(state));

    // The child has been opened and the sorter created. Sort the input.
    // The final merge is done on-demand as rows are requested in get_next().
    bool eos = false;
    std::unique_ptr<Block> upstream_block = Block::create_unique();
    do {
        {
            SCOPED_TIMER(_child_get_next_timer);
            RETURN_IF_ERROR(child(0)->get_next_after_projects(
                    state, upstream_block.get(), &eos,
                    std::bind((Status(ExecNode::*)(RuntimeState*, vectorized::Block*, bool*)) &
                                      ExecNode::get_next,
                              _children[0], std::placeholders::_1, std::placeholders::_2,
                              std::placeholders::_3)));
        }
        {
            SCOPED_TIMER(_sink_timer);
            RETURN_IF_ERROR_OR_CATCH_EXCEPTION(sink(state, upstream_block.get(), eos));
        }

    } while (!eos);

    RETURN_IF_ERROR(child(0)->close(state));

    mem_tracker()->consume(_sorter->data_size());
    COUNTER_UPDATE(_sort_blocks_memory_usage, _sorter->data_size());

    return Status::OK();
}

Status VSortNode::pull(doris::RuntimeState* state, vectorized::Block* output_block, bool* eos) {
    SCOPED_TIMER(_exec_timer);
    SCOPED_TIMER(_get_next_timer);
    RETURN_IF_ERROR_OR_CATCH_EXCEPTION(_sorter->get_next(state, output_block, eos));
    reached_limit(output_block, eos);
    return Status::OK();
}

Status VSortNode::get_next(RuntimeState* state, Block* block, bool* eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());

    return pull(state, block, eos);
}

Status VSortNode::reset(RuntimeState* state) {
    return Status::OK();
}

void VSortNode::release_resource(doris::RuntimeState* state) {
    _vsort_exec_exprs.close(state);
    _sorter = nullptr;
    ExecNode::release_resource(state);
}

Status VSortNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    return ExecNode::close(state);
}

void VSortNode::debug_string(int indentation_level, stringstream* out) const {
    *out << string(indentation_level * 2, ' ');
    *out << "VSortNode(";
    for (int i = 0; i < _is_asc_order.size(); ++i) {
        *out << (i > 0 ? " " : "") << (_is_asc_order[i] ? "asc" : "desc") << " nulls "
             << (_nulls_first[i] ? "first" : "last");
    }
    ExecNode::debug_string(indentation_level, out);
    *out << ")";
}

} // namespace doris::vectorized
