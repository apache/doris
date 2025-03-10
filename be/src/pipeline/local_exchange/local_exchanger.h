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

#include "pipeline/dependency.h"
#include "pipeline/exec/operator.h"

namespace doris {
#include "common/compile_check_begin.h"
namespace vectorized {
template <typename T>
void clear_blocks(moodycamel::ConcurrentQueue<T>& blocks,
                  RuntimeProfile::Counter* memory_used_counter = nullptr);

class PartitionerBase;
} // namespace vectorized
namespace pipeline {
class LocalExchangeSourceLocalState;
class LocalExchangeSinkLocalState;

struct Profile {
    RuntimeProfile::Counter* compute_hash_value_timer = nullptr;
    RuntimeProfile::Counter* distribute_timer = nullptr;
    RuntimeProfile::Counter* copy_data_timer = nullptr;
};

struct SinkInfo {
    int* channel_id;
    vectorized::PartitionerBase* partitioner;
    LocalExchangeSinkLocalState* local_state;
    std::map<int, int>* shuffle_idx_to_instance_idx;
};

struct SourceInfo {
    int channel_id;
    LocalExchangeSourceLocalState* local_state;
};
/**
 * One exchanger is hold by one `LocalExchangeSharedState`. And one `LocalExchangeSharedState` is
 * shared by all local exchange sink operators and source operators with the same id.
 *
 * In exchanger, two block queues is maintained, one is data block queue and another is free block queue.
 *
 * In details, data block queue has queues as many as source operators. Each source operator will get
 * data block from the corresponding queue. Data blocks is push into the queue by sink operators. One
 * sink operator will push blocks into one or more queues.
 *
 * Free block is used to reuse the allocated memory. To reduce the memory limit, we also use a conf
 * to limit the size of free block queue.
 */
class ExchangerBase {
public:
    /**
     * `BlockWrapper` is used to wrap a data block with a reference count.
     *
     * In function `unref()`, if `ref_count` decremented to 0, which means this block is not needed by
     * operators, so we put it into `_free_blocks` to reuse its memory if needed and refresh memory usage
     * in current queue.
     *
     * Note: `ref_count` will be larger than 1 only if this block is shared between multiple queues in
     * shuffle exchanger.
     */
    class BlockWrapper {
    public:
        ENABLE_FACTORY_CREATOR(BlockWrapper);
        BlockWrapper(vectorized::Block&& data_block, LocalExchangeSharedState* shared_state,
                     int channel_id)
                : _data_block(std::move(data_block)),
                  _shared_state(shared_state),
                  _allocated_bytes(_data_block.allocated_bytes()) {
            if (_shared_state) {
                _shared_state->add_total_mem_usage(_allocated_bytes);
            }
        }
        ~BlockWrapper() {
            if (_shared_state != nullptr) {
                DCHECK_GT(_allocated_bytes, 0);
                // `_channel_ids` may be empty if exchanger is shuffled exchanger and channel id is
                // not used by `sub_total_mem_usage`. So we just pass -1 here.
                _shared_state->sub_total_mem_usage(_allocated_bytes);
                if (_shared_state->exchanger->_free_block_limit == 0 ||
                    _shared_state->exchanger->_free_blocks.size_approx() <
                            _shared_state->exchanger->_free_block_limit *
                                    _shared_state->exchanger->_num_sources) {
                    _data_block.clear_column_data();
                    // Free blocks is used to improve memory efficiency. Failure during pushing back
                    // free block will not incur any bad result so just ignore the return value.
                    _shared_state->exchanger->_free_blocks.enqueue(std::move(_data_block));
                }
            };
        }
        void record_channel_id(int channel_id) {
            _channel_ids.push_back(channel_id);
            if (_shared_state) {
                _shared_state->add_mem_usage(channel_id, _allocated_bytes);
            }
        }

    private:
        friend class ShuffleExchanger;
        friend class BucketShuffleExchanger;
        friend class PassthroughExchanger;
        friend class BroadcastExchanger;
        friend class PassToOneExchanger;
        friend class AdaptivePassthroughExchanger;
        template <typename BlockType>
        friend class Exchanger;

        vectorized::Block _data_block;
        LocalExchangeSharedState* _shared_state;
        std::vector<int> _channel_ids;
        const size_t _allocated_bytes;
    };
    ExchangerBase(int running_sink_operators, int num_partitions, int free_block_limit)
            : _running_sink_operators(running_sink_operators),
              _running_source_operators(num_partitions),
              _num_partitions(num_partitions),
              _num_senders(running_sink_operators),
              _num_sources(num_partitions),
              _free_block_limit(free_block_limit) {}
    ExchangerBase(int running_sink_operators, int num_sources, int num_partitions,
                  int free_block_limit)
            : _running_sink_operators(running_sink_operators),
              _running_source_operators(num_sources),
              _num_partitions(num_partitions),
              _num_senders(running_sink_operators),
              _num_sources(num_sources),
              _free_block_limit(free_block_limit) {}
    virtual ~ExchangerBase() = default;
    virtual Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos,
                             Profile&& profile, SourceInfo&& source_info) = 0;
    virtual Status sink(RuntimeState* state, vectorized::Block* in_block, bool eos,
                        Profile&& profile, SinkInfo&& sink_info) = 0;
    virtual ExchangeType get_type() const = 0;
    // Called if a local exchanger source operator are closed. Free the unused data block in data_queue.
    virtual void close(SourceInfo&& source_info) = 0;
    // Called if all local exchanger source operators are closed. We free the memory in
    // `_free_blocks` here.
    virtual void finalize();

    virtual std::string data_queue_debug_string(int i) = 0;

    void set_low_memory_mode() {
        _free_block_limit = 0;
        clear_blocks(_free_blocks);
    }

protected:
    friend struct LocalExchangeSharedState;
    friend class LocalExchangeSourceLocalState;
    friend class LocalExchangeSinkOperatorX;
    friend class LocalExchangeSinkLocalState;
    std::atomic<int> _running_sink_operators = 0;
    std::atomic<int> _running_source_operators = 0;
    const int _num_partitions;
    const int _num_senders;
    const int _num_sources;
    std::atomic_int _free_block_limit = 0;
    moodycamel::ConcurrentQueue<vectorized::Block> _free_blocks;
};

struct PartitionedRowIdxs {
    std::shared_ptr<vectorized::PODArray<uint32_t>> row_idxs;
    uint32_t offset_start;
    uint32_t length;
};

using PartitionedBlock =
        std::pair<std::shared_ptr<ExchangerBase::BlockWrapper>, PartitionedRowIdxs>;

struct RowRange {
    uint32_t offset_start;
    size_t length;
};
using BroadcastBlock = std::pair<std::shared_ptr<ExchangerBase::BlockWrapper>, RowRange>;

template <typename BlockType>
struct BlockQueue {
    std::atomic<bool> eos = false;
    moodycamel::ConcurrentQueue<BlockType> data_queue;
    moodycamel::ProducerToken ptok {data_queue};
    BlockQueue() : eos(false), data_queue(moodycamel::ConcurrentQueue<BlockType>()) {}
    BlockQueue(BlockQueue<BlockType>&& other)
            : eos(other.eos.load()), data_queue(std::move(other.data_queue)) {}
    inline bool enqueue(BlockType const& item) {
        if (!eos) {
            if (!data_queue.enqueue(ptok, item)) [[unlikely]] {
                throw Exception(ErrorCode::INTERNAL_ERROR,
                                "Exception occurs in data queue [size = {}] of local exchange.",
                                data_queue.size_approx());
            }
            return true;
        }
        return false;
    }

    inline bool enqueue(BlockType&& item) {
        if (!eos) {
            if (!data_queue.enqueue(ptok, std::move(item))) [[unlikely]] {
                throw Exception(ErrorCode::INTERNAL_ERROR,
                                "Exception occurs in data queue [size = {}] of local exchange.",
                                data_queue.size_approx());
            }
            return true;
        }
        return false;
    }

    bool try_dequeue(BlockType& item) { return data_queue.try_dequeue(item); }

    void set_eos() { eos = true; }
};

using BlockWrapperSPtr = std::shared_ptr<ExchangerBase::BlockWrapper>;

template <typename BlockType>
class Exchanger : public ExchangerBase {
public:
    Exchanger(int running_sink_operators, int num_partitions, int free_block_limit)
            : ExchangerBase(running_sink_operators, num_partitions, free_block_limit) {
        _data_queue.resize(num_partitions);
        _m.resize(num_partitions);
        for (size_t i = 0; i < num_partitions; i++) {
            _m[i] = std::make_unique<std::mutex>();
        }
    }
    Exchanger(int running_sink_operators, int num_sources, int num_partitions, int free_block_limit)
            : ExchangerBase(running_sink_operators, num_sources, num_partitions, free_block_limit) {
        _data_queue.resize(num_sources);
        _m.resize(num_sources);
        for (size_t i = 0; i < num_sources; i++) {
            _m[i] = std::make_unique<std::mutex>();
        }
    }
    ~Exchanger() override = default;
    std::string data_queue_debug_string(int i) override {
        return fmt::format("Data Queue {}: [size approx = {}, eos = {}]", i,
                           _data_queue[i].data_queue.size_approx(), _data_queue[i].eos);
    }

protected:
    // Enqueue data block and set downstream source operator to read.
    void _enqueue_data_and_set_ready(int channel_id, LocalExchangeSinkLocalState* local_state,
                                     BlockType&& block);
    bool _dequeue_data(LocalExchangeSourceLocalState* local_state, BlockType& block, bool* eos,
                       vectorized::Block* data_block, int channel_id);

    void _enqueue_data_and_set_ready(int channel_id, BlockType&& block);
    bool _dequeue_data(BlockType& block, bool* eos, vectorized::Block* data_block, int channel_id);
    std::vector<BlockQueue<BlockType>> _data_queue;
    std::vector<std::unique_ptr<std::mutex>> _m;
};

class LocalExchangeSourceLocalState;
class LocalExchangeSinkLocalState;

class ShuffleExchanger : public Exchanger<PartitionedBlock> {
public:
    ENABLE_FACTORY_CREATOR(ShuffleExchanger);
    ShuffleExchanger(int running_sink_operators, int num_sources, int num_partitions,
                     int free_block_limit)
            : Exchanger<PartitionedBlock>(running_sink_operators, num_sources, num_partitions,
                                          free_block_limit) {
        DCHECK_GT(num_partitions, 0);
        DCHECK_GT(num_sources, 0);
        _partition_rows_histogram.resize(running_sink_operators);
    }
    ~ShuffleExchanger() override = default;
    Status sink(RuntimeState* state, vectorized::Block* in_block, bool eos, Profile&& profile,
                SinkInfo&& sink_info) override;

    Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos, Profile&& profile,
                     SourceInfo&& source_info) override;
    void close(SourceInfo&& source_info) override;
    ExchangeType get_type() const override { return ExchangeType::HASH_SHUFFLE; }

protected:
    Status _split_rows(RuntimeState* state, const uint32_t* __restrict channel_ids,
                       vectorized::Block* block, int channel_id,
                       LocalExchangeSinkLocalState* local_state,
                       std::map<int, int>* shuffle_idx_to_instance_idx);
    Status _split_rows(RuntimeState* state, const uint32_t* __restrict channel_ids,
                       vectorized::Block* block, int channel_id);
    std::vector<std::vector<uint32_t>> _partition_rows_histogram;
};

class BucketShuffleExchanger final : public ShuffleExchanger {
    ENABLE_FACTORY_CREATOR(BucketShuffleExchanger);
    BucketShuffleExchanger(int running_sink_operators, int num_sources, int num_partitions,
                           int free_block_limit)
            : ShuffleExchanger(running_sink_operators, num_sources, num_partitions,
                               free_block_limit) {}
    ~BucketShuffleExchanger() override = default;
    ExchangeType get_type() const override { return ExchangeType::BUCKET_HASH_SHUFFLE; }
};

class PassthroughExchanger final : public Exchanger<BlockWrapperSPtr> {
public:
    ENABLE_FACTORY_CREATOR(PassthroughExchanger);
    PassthroughExchanger(int running_sink_operators, int num_partitions, int free_block_limit)
            : Exchanger<BlockWrapperSPtr>(running_sink_operators, num_partitions,
                                          free_block_limit) {}
    ~PassthroughExchanger() override = default;
    Status sink(RuntimeState* state, vectorized::Block* in_block, bool eos, Profile&& profile,
                SinkInfo&& sink_info) override;

    Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos, Profile&& profile,
                     SourceInfo&& source_info) override;
    ExchangeType get_type() const override { return ExchangeType::PASSTHROUGH; }
    void close(SourceInfo&& source_info) override;
};

class PassToOneExchanger final : public Exchanger<BlockWrapperSPtr> {
public:
    ENABLE_FACTORY_CREATOR(PassToOneExchanger);
    PassToOneExchanger(int running_sink_operators, int num_partitions, int free_block_limit)
            : Exchanger<BlockWrapperSPtr>(running_sink_operators, num_partitions,
                                          free_block_limit) {}
    ~PassToOneExchanger() override = default;
    Status sink(RuntimeState* state, vectorized::Block* in_block, bool eos, Profile&& profile,
                SinkInfo&& sink_info) override;

    Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos, Profile&& profile,
                     SourceInfo&& source_info) override;
    ExchangeType get_type() const override { return ExchangeType::PASS_TO_ONE; }
    void close(SourceInfo&& source_info) override;
};
class BroadcastExchanger final : public Exchanger<BroadcastBlock> {
public:
    ENABLE_FACTORY_CREATOR(BroadcastExchanger);
    BroadcastExchanger(int running_sink_operators, int num_partitions, int free_block_limit)
            : Exchanger<BroadcastBlock>(running_sink_operators, num_partitions, free_block_limit) {}
    ~BroadcastExchanger() override = default;
    Status sink(RuntimeState* state, vectorized::Block* in_block, bool eos, Profile&& profile,
                SinkInfo&& sink_info) override;

    Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos, Profile&& profile,
                     SourceInfo&& source_info) override;
    ExchangeType get_type() const override { return ExchangeType::BROADCAST; }
    void close(SourceInfo&& source_info) override;
};

//The code in AdaptivePassthroughExchanger is essentially
// a copy of ShuffleExchanger and PassthroughExchanger.
class AdaptivePassthroughExchanger : public Exchanger<BlockWrapperSPtr> {
public:
    ENABLE_FACTORY_CREATOR(AdaptivePassthroughExchanger);
    AdaptivePassthroughExchanger(int running_sink_operators, int num_partitions,
                                 int free_block_limit)
            : Exchanger<BlockWrapperSPtr>(running_sink_operators, num_partitions,
                                          free_block_limit) {
        _partition_rows_histogram.resize(running_sink_operators);
    }
    Status sink(RuntimeState* state, vectorized::Block* in_block, bool eos, Profile&& profile,
                SinkInfo&& sink_info) override;

    Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos, Profile&& profile,
                     SourceInfo&& source_info) override;
    ExchangeType get_type() const override { return ExchangeType::ADAPTIVE_PASSTHROUGH; }

    void close(SourceInfo&& source_info) override;

private:
    Status _passthrough_sink(RuntimeState* state, vectorized::Block* in_block,
                             SinkInfo&& sink_info);
    Status _shuffle_sink(RuntimeState* state, vectorized::Block* in_block, SinkInfo&& sink_info);
    Status _split_rows(RuntimeState* state, const uint32_t* __restrict channel_ids,
                       vectorized::Block* block, SinkInfo&& sink_info);

    std::atomic_bool _is_pass_through = false;
    std::atomic_int32_t _total_block = 0;
    std::vector<std::vector<uint32_t>> _partition_rows_histogram;
};
#include "common/compile_check_end.h"
} // namespace pipeline
} // namespace doris