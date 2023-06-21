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
#include "util/countdown_latch.h"
#include "vec/exec/join/vhash_join_node.h"
#include "vec/spill/spill_stream.h"

namespace doris {
namespace vectorized {

using InMemoryHashJoinNodeUPtr = std::unique_ptr<HashJoinNode>;

class GraceHashJoinNode;

class JoinPartition {
public:
    JoinPartition(GraceHashJoinNode* parent, int level) : parent_(parent), level_(level) {}
    Status prepare(RuntimeState* state, RuntimeProfile* profile, const std::string& operator_name,
                   int node_id);

    Status add_build_rows(Block* block, const std::vector<int>& rows, bool eos);

    Status add_probe_rows(RuntimeState* state, Block* block, std::vector<int>& rows, bool eos);

    Status push_probe_block(RuntimeState* state, Block* input_block, bool eos);

    Status probe(RuntimeState* state, vectorized::Block* output_block, bool* eos) {
        return in_mem_hash_join_node_->pull(state, output_block, eos);
    }

    Status build_eos() { return build_stream_->done_write(); }

    Status probe_eos() { return probe_stream_->done_write(); }

    Status spilled_probe_not_repartitioned(RuntimeState* state, Block* output_block, bool* eos) {
        bool partition_eos = false;
        if (need_more_probe_data()) {
            Block block;
            RETURN_IF_ERROR(probe_stream_->get_next(&block, &partition_eos));
            RETURN_IF_ERROR(in_mem_hash_join_node_->push(state, &block, partition_eos));
        }
        return probe(state, output_block, eos);
    }

    Status get_next_probe_block(RuntimeState* state, Block* output_block, bool* eos) {
        return probe_stream_->get_next(output_block, eos);
    }
    bool has_nex_probe_block() { return probe_stream_->has_next(); }

    Status unpin_build_stream();

    Status unpin_probe_stream();

    bool is_build_partition_spilled() const { return build_stream_->is_spilled(); }

    bool is_probe_partition_spilled() const { return probe_stream_->is_spilled(); }

    bool is_ready_for_probe() const { return is_ready_for_probe_; }

    bool need_more_probe_data() const { return in_mem_hash_join_node_->need_more_input_data(); }

    bool current_probe_finished() const { return in_mem_hash_join_node_->current_probe_finished(); }

    bool is_processed() const { return is_processed_; }
    void set_is_processed() { is_processed_ = true; }

    void close(RuntimeState* state) {
        close_build(state);
        close_probe();
    }
    void close_build(RuntimeState* state);
    void close_probe();

    size_t build_data_bytes() const { return build_data_bytes_; }
    size_t probe_data_bytes() const { return probe_data_bytes_; }

private:
    friend class GraceHashJoinNode;

    Status _build_hash_table(RuntimeState* state, ObjectPool* pool, const TPlanNode& t_plan_node,
                             const DescriptorTbl& desc_tbl);

    bool _block_reach_limit(MutableBlock* mutable_block) const {
        return mutable_block->rows() > BLOCK_ROWS || mutable_block->bytes() > BLOCK_BYTES;
    }

    static constexpr size_t BLOCK_ROWS = 1024 * 1024;
    static constexpr size_t BLOCK_BYTES = 64 << 20;

    GraceHashJoinNode* parent_;
    int level_;
    InMemoryHashJoinNodeUPtr in_mem_hash_join_node_;
    SpillStreamSPtr build_stream_;
    SpillStreamSPtr probe_stream_;
    int64_t build_row_count_ = 0;
    size_t build_data_bytes_ = 0;
    int64_t probe_row_count_ = 0;
    size_t probe_data_bytes_ = 0;
    bool is_processed_ = false;

    std::unique_ptr<MutableBlock> mutable_probe_block_;

    std::atomic_bool is_ready_for_probe_ = false;
};
using JoinPartitionSPtr = std::shared_ptr<JoinPartition>;

class GraceHashJoinNode : public ExecNode {
public:
    GraceHashJoinNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

    Status init(const TPlanNode& tnode, RuntimeState* state = nullptr) override;

    Status prepare(RuntimeState* state) override;

    Status alloc_resource(RuntimeState* state) override;

    void release_resource(RuntimeState* state) override;

    // 对Build数据进行分区，必要时落盘。
    Status sink(doris::RuntimeState* state, vectorized::Block* input_block, bool eos) override;

    // 对Probe数据进行分区和probe。
    Status push(RuntimeState* state, vectorized::Block* input_block, bool eos) override;

    Status pull(RuntimeState* state, vectorized::Block* output_block, bool* eos) override;

    const RowDescriptor& row_desc() const override { return *_output_row_desc; }
    const RowDescriptor& intermediate_row_desc() const override { return *_intermediate_row_desc; }

    bool need_more_input_data() const;

    bool should_build_hash_table() const { return should_build_hash_table_; }

    bool can_sink_write() const { return true; }

    bool io_task_finished() { return !is_building_hash_table_ && !is_io_task_running_; }

private:
    friend class JoinPartition;

    static constexpr int PARTITION_COUNT = 16;

    static constexpr int MAX_PARTITION_DEPTH = 16;

    Status _close_current_partitions(RuntimeState* state) {
        for (auto& partition : current_partitions_) {
            partition->close(state);
        }
        current_partitions_.clear();
        return Status::OK();
    }

    Status _prepare_current_partitions(RuntimeState* state, int level);

    GraceHashJoinNode* _as_mutable() const { return const_cast<GraceHashJoinNode*>(this); }

    int _ready_build_partitions_count() const {
        int n = 0;
        if (!current_partitions_.empty()) {
            for (auto& partition : current_partitions_) {
                if (partition->is_ready_for_probe()) {
                    ++n;
                }
            }
        } else {
            DCHECK(current_spilled_partition_);
            if (current_spilled_partition_->is_ready_for_probe()) {
                ++n;
            }
        }
        return n;
    }

    Status _build_hash_tables_async(RuntimeState* state);

    int _min_build_partition() const {
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

    int _in_memory_build_partition_count() const {
        int n = 0;
        for (auto& partition : current_partitions_) {
            if (!partition->is_build_partition_spilled()) {
                ++n;
            }
        }
        return n;
    }

    int _max_in_memory_build_partition() const {
        int index = -1;
        size_t max_bytes = 0;
        for (int i = 1; i < PARTITION_COUNT; ++i) {
            if (!current_partitions_[i]->is_build_partition_spilled() &&
                current_partitions_[i]->build_data_bytes() > max_bytes) {
                max_bytes = current_partitions_[i]->build_data_bytes();
                index = i;
            }
        }
        return index;
    }

    int _max_probe_partition_to_spill() const {
        int index = -1;
        size_t max_bytes = 0;
        for (int i = 1; i < PARTITION_COUNT; ++i) {
            if (!current_partitions_[i]->is_ready_for_probe() &&
                !current_partitions_[i]->is_probe_partition_spilled() &&
                current_partitions_[i]->probe_data_bytes() > max_bytes) {
                max_bytes = current_partitions_[i]->probe_data_bytes();
                index = i;
            }
        }
        return index;
    }

    void _update_status(Status status);

    void _calc_columns_hash(vectorized::Block* input_block, const std::vector<int>& column_ids,
                            std::vector<int> (&partition2rows)[PARTITION_COUNT]);

    Status _reserve_memory_for_build_partitions(const Block* input_block);
    Status _reserve_memory_for_probe_partitions(vectorized::Block* input_block);

    void _get_partitions_to_process();

    Status _build_in_memory_hash_tables_sync(int& success_count);

    // process spilled partitions
    Status _prepare_spilled_probe(RuntimeState* state, bool* eos);

    Status _repartition_build_data(RuntimeState* state, JoinPartitionSPtr partition);
    Status _repartition_probe_data(RuntimeState* state, JoinPartitionSPtr partition);

    Status _repartitioned_probe(RuntimeState* state, Block* output_block, bool* eos);

    Status _probe_spilled_partition(RuntimeState* state, Block* output_block, bool* eos);

    Status _partition_probe_block(RuntimeState* state, Block* block, bool eos, bool is_repartition);
    Status _partition_build_block(RuntimeState* state, Block* block, bool eos, bool is_repartition);

    bool need_more_probe_data() const;

    Status _status() {
        std::lock_guard guard(status_lock_);
        return status_;
    }

    std::unique_ptr<RowDescriptor> _output_row_desc;
    std::unique_ptr<RowDescriptor> _intermediate_row_desc;

    std::mutex status_lock_;
    Status status_;

    RuntimeState* state_;

    ThreadPool* io_thread_pool_;

    TPlanNode t_plan_node_;
    DescriptorTbl desc_tbl_;

    // probe expr
    VExprContextSPtrs probe_expr_ctxs_;
    // build expr
    VExprContextSPtrs build_expr_ctxs_;

    RuntimeProfile::Counter* _build_expr_call_timer;
    RuntimeProfile::Counter* _probe_expr_call_timer;
    std::vector<int> build_column_ids_;
    std::vector<int> probe_column_ids_;

    // current processing partitions: the inital partitons or splitted sub partitions
    // of a large partition
    std::vector<JoinPartitionSPtr> current_partitions_;

    // all the partitions that are spilled and waiting for further processing,
    // will be processed one by one
    std::vector<JoinPartitionSPtr> spilled_partitions_;

    // the current spilled partition that are processing, may need split if it does
    // not fit in the memory.
    JoinPartitionSPtr current_spilled_partition_;

    SharedHashTableContextPtr _shared_hash_table_context = nullptr;
    bool should_build_hash_table_ = true;

    bool sink_eos_ = false;
    bool probe_push_eos_ = false;

    bool first_push_ = true;
    bool is_spilled_probe_ = false;
    std::atomic_bool is_building_hash_table_ = false;
    std::atomic_bool is_io_task_running_ = false;
};
} // namespace vectorized
} // namespace doris
