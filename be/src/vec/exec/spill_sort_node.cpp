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

#include "vec/exec/spill_sort_node.h"

#include <glog/logging.h>

#include <memory>

#include "common/status.h"
#include "vec/spill/spill_stream_manager.h"
namespace doris {
namespace vectorized {
SpillSortNode::SpillSortNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs),
          _offset(tnode.sort_node.__isset.offset ? tnode.sort_node.offset : 0),
          t_plan_node_(tnode),
          desc_tbl_(descs) {}

Status SpillSortNode::init(const TPlanNode& tnode, RuntimeState* state) {
    state_ = state;
    return ExecNode::init(tnode, state);
}
Status SpillSortNode::prepare(RuntimeState* state) {
    return ExecNode::prepare(state);
}

Status SpillSortNode::alloc_resource(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::alloc_resource(state));

    return _prepare_inmemory_sort_node(state);
}

Status SpillSortNode::_prepare_inmemory_sort_node(RuntimeState* state) {
    in_memory_sort_node_ = std::make_unique<VSortNode>(_pool, t_plan_node_, desc_tbl_);
    in_memory_sort_node_->set_children(get_children());
    in_memory_sort_node_->set_prepare_children(false);
    RETURN_IF_ERROR(in_memory_sort_node_->init(t_plan_node_));
    RETURN_IF_ERROR(in_memory_sort_node_->prepare(state));
    RETURN_IF_ERROR(in_memory_sort_node_->alloc_resource(state));
    return Status::OK();
}

void SpillSortNode::release_resource(doris::RuntimeState* state) {
    in_memory_sort_node_->release_resource(state);
    in_memory_sort_node_.reset();

    ExecNode::release_resource(state);
}

Status SpillSortNode::open(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::open(state));
    return child(0)->open(state);
}

Status SpillSortNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    return ExecNode::close(state);
}

void SpillSortNode::update_spill_block_batch_size(const Block* block) {
    auto rows = block->rows();
    if (rows > 0 && 0 == avg_row_bytes_) {
        avg_row_bytes_ = std::max((std::size_t)1, block->bytes() / rows);
        spill_block_batch_size_ =
                (SORT_BLOCK_SPILL_BATCH_BYTES + avg_row_bytes_ - 1) / avg_row_bytes_;
    }
}
Status SpillSortNode::sink(RuntimeState* state, Block* input_block, bool eos) {
    Status st;
    if (input_block->rows() > 0) {
        update_spill_block_batch_size(input_block);
        st = in_memory_sort_node_->sink(state, input_block, false);
        if (!st.is<ErrorCode::MEM_LIMIT_EXCEEDED>()) {
            RETURN_IF_ERROR(st);
        }
    }
    if (eos || st.is<ErrorCode::MEM_LIMIT_EXCEEDED>()) {
        SpillStreamSPtr stream;
        RETURN_IF_ERROR(ExecEnv::GetInstance()->spill_stream_mgr()->register_spill_stream(
                stream, print_id(state->query_id()), "sort", id(), spill_block_batch_size_,
                SORT_BLOCK_SPILL_BATCH_BYTES, runtime_profile()));
        Blocks blocks;
        st = in_memory_sort_node_->release_sorted_blocks(state, blocks, spill_block_batch_size_);

        if (in_memory_sort_node_->is_append_block_oom()) {
            Block sorted_block;
            SortDescription sort_description;
            RETURN_IF_ERROR(
                    in_memory_sort_node_->sort_block(*input_block, sorted_block, sort_description));
            blocks.emplace_back(std::move(sorted_block));
        }

        RETURN_IF_ERROR(stream->add_blocks(std::move(blocks), false));
        sorted_streams_.emplace_back(stream);
    }
    if (eos) {
        RETURN_IF_ERROR(_prepare_for_pull(state));
    }
    return Status::OK();
}

Status SpillSortNode::_prepare_for_pull(RuntimeState* state) {
    if (sorted_streams_.size() < 2) {
        ready_for_pull_ = true;
        return Status::OK();
    }
    auto sort_description = in_memory_sort_node_->get_sort_description();
    while (true) {
        int max_stream_count = (sorted_streams_.size() + 1) / 2;
        max_stream_count = std::max(2, max_stream_count);
        RETURN_IF_ERROR(_create_intermediate_merger(max_stream_count, sort_description));
        // all the remaining streams can be merged in a run
        if (sorted_streams_.empty()) {
            break;
        }

        SpillStreamSPtr stream;
        RETURN_IF_ERROR(ExecEnv::GetInstance()->spill_stream_mgr()->register_spill_stream(
                stream, print_id(state->query_id()), "sort", id(), spill_block_batch_size_,
                SORT_BLOCK_SPILL_BATCH_BYTES, runtime_profile()));

        bool eos = false;
        while (!eos) {
            merge_sorted_block_.clear_column_data();
            RETURN_IF_ERROR(merger_->get_next(&merge_sorted_block_, &eos));
            RETURN_IF_ERROR(stream->add_blocks({merge_sorted_block_}, false));
        }
        sorted_streams_.emplace_back(stream);
    }
    return Status::OK();
}

Status SpillSortNode::_create_intermediate_merger(int num_blocks,
                                                  const SortDescription& sort_description) {
    std::vector<BlockSupplier> child_block_suppliers;
    merger_.reset(new VSortedRunMerger(sort_description, spill_block_batch_size_, _limit, _offset,
                                       runtime_profile()));

    current_merging_streams_.clear();
    for (int i = 0; i < num_blocks && !sorted_streams_.empty(); ++i) {
        auto stream = sorted_streams_.front();
        current_merging_streams_.emplace_back(stream);
        child_block_suppliers.emplace_back(std::bind(std::mem_fn(&SpillStream::get_next),
                                                     stream.get(), std::placeholders::_1,
                                                     std::placeholders::_2));

        sorted_streams_.pop_front();
    }
    RETURN_IF_ERROR(merger_->prepare(child_block_suppliers));
    return Status::OK();
}

bool SpillSortNode::io_task_finished() {
    return true;
}
Status SpillSortNode::pull(doris::RuntimeState* state, vectorized::Block* output_block, bool* eos) {
    if (!ready_for_pull_) {
        return Status::OK();
    }
    if (sorted_streams_.size() < 2) {
        return sorted_streams_[0]->get_next(output_block, eos);
    }
    return merger_->get_next(output_block, eos);
}
} // namespace vectorized
} // namespace doris