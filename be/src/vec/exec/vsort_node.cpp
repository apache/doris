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
#include "pipeline/pipeline.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "util/debug_util.h"
#include "vec/common/sort/heap_sorter.h"
#include "vec/common/sort/topn_sorter.h"
#include "vec/core/sort_block.h"

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
        (tnode.sort_node.use_topn_opt || !row_desc.has_varlen_slots())) {
        _sorter.reset(new HeapSorter(_vsort_exec_exprs, _limit, _offset, _pool, _is_asc_order,
                                     _nulls_first, row_desc));
        _reuse_mem = false;
        _use_topn_opt = tnode.sort_node.use_topn_opt;
        // init runtime predicate
        if (_use_topn_opt) {
            auto query_ctx = state->get_query_fragments_ctx();
            auto first_sort_expr_node = tnode.sort_node.sort_info.sort_tuple_slot_exprs[0].nodes[0];
            if (first_sort_expr_node.node_type == TExprNodeType::SLOT_REF) {
                auto first_sort_slot = first_sort_expr_node.slot_ref;
                for (auto tuple_desc : row_desc.tuple_descriptors()) {
                    if (tuple_desc->id() != first_sort_slot.tuple_id) {
                        continue;
                    }
                    for (auto slot : tuple_desc->slots()) {
                        if (slot->id() == first_sort_slot.slot_id) {
                            RETURN_IF_ERROR(
                                    query_ctx->get_runtime_predicate().init(slot->type().type));
                            break;
                        }
                    }
                }
            }
            if (!query_ctx->get_runtime_predicate().inited()) {
                return Status::InternalError("runtime predicate is not properly initialized");
            }
        }
    } else if (_limit > 0 && row_desc.has_varlen_slots() &&
               _limit + _offset < TopNSorter::TOPN_SORT_THRESHOLD) {
        _sorter.reset(new TopNSorter(_vsort_exec_exprs, _limit, _offset, _pool, _is_asc_order,
                                     _nulls_first, row_desc, state, _runtime_profile.get()));
    } else {
        _sorter.reset(new FullSorter(_vsort_exec_exprs, _limit, _offset, _pool, _is_asc_order,
                                     _nulls_first, row_desc, state, _runtime_profile.get()));
    }

    _sorter->init_profile(_runtime_profile.get());

    return Status::OK();
}

Status VSortNode::prepare(RuntimeState* state) {
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker_growh());
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::prepare(state));
    _runtime_profile->add_info_string("TOP-N", _limit == -1 ? "false" : "true");

    auto* memory_usage = _runtime_profile->create_child("MemoryUsage", true, true);
    _runtime_profile->add_child(memory_usage, false, nullptr);
    _sort_blocks_memory_usage = ADD_COUNTER(memory_usage, "SortBlocks", TUnit::BYTES);

    RETURN_IF_ERROR(_vsort_exec_exprs.prepare(state, child(0)->row_desc(), _row_descriptor));
    return Status::OK();
}

Status VSortNode::alloc_resource(doris::RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::alloc_resource(state));
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker_growh());
    RETURN_IF_ERROR(_vsort_exec_exprs.open(state));
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(state->check_query_state("vsort, while open."));

    return Status::OK();
}

Status VSortNode::sink(RuntimeState* state, vectorized::Block* input_block, bool eos) {
    if (input_block->rows() > 0) {
        RETURN_IF_ERROR(_sorter->append_block(input_block));
        RETURN_IF_CANCELLED(state);
        RETURN_IF_ERROR(state->check_query_state("vsort, while sorting input."));

        // update runtime predicate
        if (_use_topn_opt) {
            Field new_top = _sorter->get_top_value();
            if (!new_top.is_null() && new_top != old_top) {
                auto& sort_description = _sorter->get_sort_description();
                auto col = input_block->get_by_position(sort_description[0].column_number);
                bool is_reverse = sort_description[0].direction < 0;
                auto query_ctx = state->get_query_fragments_ctx();
                RETURN_IF_ERROR(
                        query_ctx->get_runtime_predicate().update(new_top, col.name, is_reverse));
                old_top = std::move(new_top);
            }
        }

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
    START_AND_SCOPE_SPAN(state->get_tracer(), span, "VSortNode::open");
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));
    RETURN_IF_ERROR(child(0)->open(state));

    // The child has been opened and the sorter created. Sort the input.
    // The final merge is done on-demand as rows are requested in get_next().
    bool eos = false;
    std::unique_ptr<Block> upstream_block(new Block());
    do {
        RETURN_IF_ERROR_AND_CHECK_SPAN(
                child(0)->get_next_after_projects(
                        state, upstream_block.get(), &eos,
                        std::bind((Status(ExecNode::*)(RuntimeState*, vectorized::Block*, bool*)) &
                                          ExecNode::get_next,
                                  _children[0], std::placeholders::_1, std::placeholders::_2,
                                  std::placeholders::_3)),
                child(0)->get_next_span(), eos);
        SCOPED_CONSUME_MEM_TRACKER(mem_tracker_growh());
        RETURN_IF_ERROR(sink(state, upstream_block.get(), eos));
    } while (!eos);

    child(0)->close(state);

    mem_tracker_held()->consume(_sorter->data_size());
    COUNTER_UPDATE(_sort_blocks_memory_usage, _sorter->data_size());

    return Status::OK();
}

Status VSortNode::pull(doris::RuntimeState* state, vectorized::Block* output_block, bool* eos) {
    RETURN_IF_ERROR(_sorter->get_next(state, output_block, eos));
    reached_limit(output_block, eos);
    if (*eos) {
        _runtime_profile->add_info_string("Spilled", _sorter->is_spilled() ? "true" : "false");
    }
    return Status::OK();
}

Status VSortNode::get_next(RuntimeState* state, Block* block, bool* eos) {
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker_growh());
    INIT_AND_SCOPE_GET_NEXT_SPAN(state->get_tracer(), _get_next_span, "VSortNode::get_next");
    SCOPED_TIMER(_runtime_profile->total_time_counter());

    return pull(state, block, eos);
}

Status VSortNode::reset(RuntimeState* state) {
    return Status::OK();
}

void VSortNode::release_resource(doris::RuntimeState* state) {
    START_AND_SCOPE_SPAN(state->get_tracer(), span, "VSortNode::close");
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
