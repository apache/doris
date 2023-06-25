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
#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>

#include "common/status.h"
#include "exec/exec_node.h"
#include "vec/exec/vsort_node.h"
#include "vec/spill/spill_stream.h"

namespace doris {
namespace vectorized {

using InMemorySortNodeUPtr = std::unique_ptr<VSortNode>;

class SpillSortNode : public ExecNode {
public:
    SpillSortNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

    Status init(const TPlanNode& tnode, RuntimeState* state = nullptr) override;

    Status prepare(RuntimeState* state) override;

    Status alloc_resource(RuntimeState* state) override;

    void release_resource(RuntimeState* state) override;

    Status open(RuntimeState* state) override;

    Status close(RuntimeState* state) override;

    Status pull(RuntimeState* state, vectorized::Block* output_block, bool* eos) override;

    Status sink(RuntimeState* state, vectorized::Block* input_block, bool eos) override;

    size_t revokable_mem_size() const override;

    Status revoke_memory() override;

    bool io_task_finished();

private:
    static constexpr size_t SPILL_BUFFERED_BLOCK_SIZE = 4 * 1024 * 1024;
    static constexpr size_t SPILL_BUFFERED_BLOCK_BYTES = 256 << 20;

    Status _prepare_inmemory_sort_node(RuntimeState* state);
    void update_spill_block_batch_size(const Block* block);
    Status _prepare_for_pull(RuntimeState* state);
    Status _create_intermediate_merger(int num_blocks, const SortDescription& sort_description);
    Status _release_in_mem_sorted_blocks();

    RuntimeState* state_;
    ThreadPool* io_thread_pool_;
    Status status_;
    std::unique_ptr<std::promise<Status>> spill_merge_promise_;
    bool sink_eos_ = false;
    int64_t _offset;
    TPlanNode t_plan_node_;
    DescriptorTbl desc_tbl_;
    InMemorySortNodeUPtr in_memory_sort_node_;
    SpillStreamSPtr spilling_stream_;
    std::deque<SpillStreamSPtr> sorted_streams_;
    std::vector<SpillStreamSPtr> current_merging_streams_;
    size_t avg_row_bytes_ = 0;
    int spill_block_batch_size_;
    Block merge_sorted_block_;
    std::unique_ptr<VSortedRunMerger> merger_;
};
} // namespace vectorized
} // namespace doris
