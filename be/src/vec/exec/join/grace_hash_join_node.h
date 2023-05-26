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

#include "common/status.h"
#include "util/countdown_latch.h"
#include "vec/exec/join/vhash_join_node.h"
#include "vec/spill/spill_stream.h"

namespace doris {
namespace vectorized {

using InMemoryHashJoinNodeUPtr = std::unique_ptr<HashJoinNode>;

class JoinPartition {
public:
    Status prepare(RuntimeState* state, RuntimeProfile* profile, const std::string& operator_name,
                   int node_id);

    Status add_build_rows(Block* block, std::vector<int>& rows);

    Status add_probe_block(Block& block) {
        probe_stream_->add_block(block);
        return Status::OK();
    }
    Status push_probe_block(Block& block) {
        // in_mem_hash_join_node_->push(block);
        return Status::OK();
    }

    Status build_hash_table(RuntimeState* state, ObjectPool* pool, const TPlanNode& t_plan_node,
                            const DescriptorTbl& desc_tbl);

    // force spill build blocks
    Status flush_build_stream() { return build_stream_->flush(); }

    Status flush_probe_stream() { return probe_stream_->flush(); }

    bool build_stream_can_write() const { return !is_flushing_build_stream(); }

    bool is_flushing_build_stream() const { return build_stream_->is_flushing(); }

    bool is_flushing_probe_stream() const { return probe_stream_->is_flushing(); }

    bool is_spilled() const { return build_stream_->is_spilled() || probe_stream_->is_spilled(); }

    bool has_hash_table() const { return in_mem_hash_join_node_ != nullptr; }

    bool is_building_hash_table() const { return is_building_hash_table_; }

    bool is_ready_for_probe() const { return is_ready_for_probe_; }

    size_t build_data_bytes() const { return build_data_bytes_; }

    Status restore_build_data();

private:
    Status _reserve(int row_count);

    bool _reach_limit() const {
        return _mutable_block->rows() > BLOCK_ROWS || _mutable_block->bytes() > BLOCK_BYTES;
    }

    Status _add_rows_skip_mem_check(Block* block, std::vector<int>& rows);

    static constexpr size_t BLOCK_ROWS = 1024 * 1024;
    static constexpr size_t BLOCK_BYTES = 64 << 20;

    InMemoryHashJoinNodeUPtr in_mem_hash_join_node_;
    SpillStreamSPtr build_stream_;
    SpillStreamSPtr probe_stream_;
    int64_t build_row_count_ = 0;
    size_t build_data_bytes_ = 0;
    int64_t probe_row_count_ = 0;
    size_t probe_data_bytes_ = 0;

    std::unique_ptr<MutableBlock> _mutable_block;

    bool is_building_hash_table_ = false;

    std::atomic_bool is_ready_for_probe_ = false;
};
using JoinPartitionSPtr = std::shared_ptr<JoinPartition>;

struct NoBlockCountDownLatch {
    void reset(int32_t total) { _count_down = total; }

    void count_down() {
        _count_down--;
        DCHECK_GE(_count_down, 0);
    }

    bool ready() const { return _count_down == 0; }

private:
    std::atomic_int32_t _count_down {};
};
class GraceHashJoinNode : public ExecNode {
public:
    GraceHashJoinNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

    Status init(const TPlanNode& tnode, RuntimeState* state = nullptr) override;

    Status prepare(RuntimeState* state) override;

    // 对Build数据进行分区，必要时落盘。
    Status sink(doris::RuntimeState* state, vectorized::Block* input_block, bool eos) override;

    // 对Probe数据进行分区和probe。
    Status push(RuntimeState* state, vectorized::Block* input_block, bool eos) override;

    Status pull(RuntimeState* state, vectorized::Block* output_block, bool* eos) override;

    bool need_more_input_data() const;

    bool should_build_hash_table() const { return should_build_hash_table_; }

    bool can_sink_write() const {
        for (int i = 0; i < PARTITION_COUNT; ++i) {
            if (!current_partitions_[i]->build_stream_can_write()) {
                return false;
            }
        }
        return true;
    }

private:
    GraceHashJoinNode* _as_mutable() const { return const_cast<GraceHashJoinNode*>(this); }

    int _ready_build_partitions_count() const {
        int n = 0;
        for (int i = 0; i < PARTITION_COUNT; ++i) {
            if (current_partitions_[i]->is_ready_for_probe()) {
                ++n;
            }
        }
        return n;
    }

    Status _trigger_build_hash_tables(RuntimeState* state);
    Status _build_partition_hash_table(RuntimeState* state, int i);

    int _smallest_join_partition() const {
        int index = 0;
        auto smallest_bytes_ = current_partitions_[0]->build_data_bytes();
        for (int i = 1; i < PARTITION_COUNT; ++i) {
            if (current_partitions_[i]->build_data_bytes() < smallest_bytes_) {
                smallest_bytes_ = current_partitions_[i]->build_data_bytes();
                index = i;
            }
        }
        return index;
    }

    static constexpr int PARTITION_COUNT = 16;

    RuntimeState* state_;

    TPlanNode t_plan_node_;
    DescriptorTbl desc_tbl_;

    using VExprContexts = std::vector<VExprContext*>;
    // probe expr
    VExprContexts probe_expr_ctxs_;
    // build expr
    VExprContexts build_expr_ctxs_;

    ThreadPool* io_thread_pool_;
    std::vector<JoinPartitionSPtr> current_partitions_;

    // current processing partitions: the inital partitons or splitted sub partitions
    // of a large partition
    std::vector<JoinPartitionSPtr> processing_partitions_;

    // all the partitions that are spilled and waiting for further processing,
    // will be processed one by one
    std::vector<JoinPartitionSPtr> spilled_partitions_;

    // the current spilled partition that are processing, may need split if it does
    // not fit in the memory.
    JoinPartitionSPtr current_spilled_partition_;

    RuntimeProfile::Counter* _build_expr_call_timer;
    RuntimeProfile::Counter* _probe_expr_call_timer;

    SharedHashTableContextPtr _shared_hash_table_context = nullptr;
    bool should_build_hash_table_ = true;
    Block build_block_;
    Block probe_block_;
    NoBlockCountDownLatch build_hash_table_latch_;
};
} // namespace vectorized
} // namespace doris
