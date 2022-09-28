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

#include "common/config.h"
#include "exec/sort_exec_exprs.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "util/debug_util.h"
#include "vec/common/sort/heap_sorter.h"
#include "vec/common/sort/topn_sorter.h"
#include "vec/core/sort_block.h"
#include "vec/utils/util.hpp"

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
        !row_desc.has_varlen_slots()) {
        _sorter.reset(new HeapSorter(_vsort_exec_exprs, _limit, _offset, _pool, _is_asc_order,
                                     _nulls_first, row_desc));
        _reuse_mem = false;
    } else if (_limit > 0 && row_desc.has_varlen_slots() && _limit > 0 &&
               _limit + _offset < TopNSorter::TOPN_SORT_THRESHOLD) {
        _sorter.reset(new TopNSorter(_vsort_exec_exprs, _limit, _offset, _pool, _is_asc_order,
                                     _nulls_first, row_desc));
    } else {
        _sorter.reset(new FullSorter(_vsort_exec_exprs, _limit, _offset, _pool, _is_asc_order,
                                     _nulls_first, row_desc));
    }

    _sorter->init_profile(_runtime_profile.get());

    return Status::OK();
}

Status VSortNode::prepare(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    _runtime_profile->add_info_string("TOP-N", _limit == -1 ? "false" : "true");
    RETURN_IF_ERROR(ExecNode::prepare(state));
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());
    RETURN_IF_ERROR(_vsort_exec_exprs.prepare(state, child(0)->row_desc(), _row_descriptor));
    return Status::OK();
}

Status VSortNode::open(RuntimeState* state) {
    START_AND_SCOPE_SPAN(state->get_tracer(), span, "VSortNode::open");
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());
    RETURN_IF_ERROR(_vsort_exec_exprs.open(state));
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(state->check_query_state("vsort, while open."));
    RETURN_IF_ERROR(child(0)->open(state));

    // The child has been opened and the sorter created. Sort the input.
    // The final merge is done on-demand as rows are requested in get_next().
    bool eos = false;
    std::unique_ptr<Block> upstream_block(new Block());
    do {
        RETURN_IF_ERROR_AND_CHECK_SPAN(
                child(0)->get_next_after_projects(state, upstream_block.get(), &eos),
                child(0)->get_next_span(), eos);
        if (upstream_block->rows() != 0) {
            RETURN_IF_ERROR(_sorter->append_block(upstream_block.get()));
            RETURN_IF_CANCELLED(state);
            RETURN_IF_ERROR(state->check_query_state("vsort, while sorting input."));
            if (!_reuse_mem) {
                upstream_block.reset(new Block());
            }
        }
    } while (!eos);

    RETURN_IF_ERROR(_sorter->prepare_for_read());
    return Status::OK();
}

Status VSortNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
    *eos = true;
    return Status::NotSupported("Not Implemented VSortNode::get_next scalar");
}

Status VSortNode::get_next(RuntimeState* state, Block* block, bool* eos) {
    INIT_AND_SCOPE_GET_NEXT_SPAN(state->get_tracer(), _get_next_span, "VSortNode::get_next");
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());

    RETURN_IF_ERROR(_sorter->get_next(state, block, eos));
    reached_limit(block, eos);
    return Status::OK();
}

Status VSortNode::reset(RuntimeState* state) {
    return Status::OK();
}

Status VSortNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    START_AND_SCOPE_SPAN(state->get_tracer(), span, "VSortNode::close");
    _vsort_exec_exprs.close(state);
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
