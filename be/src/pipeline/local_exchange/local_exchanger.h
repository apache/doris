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
class PartitionerBase;
}
namespace pipeline {
class LocalExchangeSourceLocalState;
class LocalExchangeSinkLocalState;
struct BlockWrapper;
class SortSourceOperatorX;
class ChannelSelector;

struct Profile {
    RuntimeProfile::Counter* compute_hash_value_timer = nullptr;
    RuntimeProfile::Counter* distribute_timer = nullptr;
    RuntimeProfile::Counter* copy_data_timer = nullptr;
    RuntimeProfile::Counter* enqueue_rows_counter = nullptr;
    RuntimeProfile::Counter* dequeue_rows_counter = nullptr;
    RuntimeProfile::Counter* get_block_failed_counter = nullptr;
};

struct SinkInfo {
    int channel_id;
    vectorized::PartitionerBase* partitioner;
    LocalExchangeSinkLocalState* local_state;
    ChannelSelector* channel_selector;
};

struct SourceInfo {
    int channel_id;
    LocalExchangeSourceLocalState* local_state;
    vectorized::MutableBlock* accumulated_block = nullptr;
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
    ExchangerBase(int running_sink_operators, int num_partitions, int free_block_limit)
            : _running_sink_operators(running_sink_operators),
              _running_source_operators(num_partitions),
              _num_partitions(num_partitions),
              _num_senders(running_sink_operators),
              _num_sources(num_partitions),
              _free_block_limit(free_block_limit) {
        _mutable_blocks.resize(_num_partitions);
    }
    ExchangerBase(int running_sink_operators, int num_sources, int num_partitions,
                  int free_block_limit)
            : _running_sink_operators(running_sink_operators),
              _running_source_operators(num_sources),
              _num_partitions(num_partitions),
              _num_senders(running_sink_operators),
              _num_sources(num_sources),
              _free_block_limit(free_block_limit) {
        _mutable_blocks.resize(num_sources);
    }
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

    int num_sources() const { return _num_sources; }

    virtual std::string data_queue_debug_string(int i) = 0;
    std::string debug_string() const {
        return fmt::format("{}({}, {} -> {})", get_exchange_type_name(get_type()), _num_partitions,
                           _num_senders, _num_sources);
    }

protected:
    friend struct LocalExchangeSharedState;
    friend struct BlockWrapper;
    friend class LocalExchangeSourceLocalState;
    friend class LocalExchangeSinkOperatorX;
    friend class LocalExchangeSinkLocalState;
    std::atomic<int> _running_sink_operators = 0;
    std::atomic<int> _running_source_operators = 0;
    const int _num_partitions;
    const int _num_senders;
    const int _num_sources;
    const int _free_block_limit = 0;
    moodycamel::ConcurrentQueue<vectorized::Block> _free_blocks;
    std::vector<std::unique_ptr<vectorized::MutableBlock>> _mutable_blocks;
};

struct PartitionedRowIdxs {
    std::shared_ptr<vectorized::PODArray<uint32_t>> row_idxs;
    uint32_t offset_start;
    uint32_t length;
};

using PartitionedBlock = std::pair<std::shared_ptr<BlockWrapper>, PartitionedRowIdxs>;

struct RowRange {
    uint32_t offset_start;
    size_t length;
};
using BroadcastBlock = std::pair<std::shared_ptr<BlockWrapper>, RowRange>;

template <typename BlockItem>
class BlockQueueBase {
public:
    using BlockType = BlockItem;
    BlockQueueBase() : _eos(false) {}
    virtual ~BlockQueueBase() = default;
    virtual bool enqueue(BlockItem&& item) = 0;
    virtual bool try_dequeue(BlockItem& item) = 0;

    void set_eos() { _eos = true; }
    bool eos() const { return _eos; }

    virtual size_t size_approx() const = 0;

protected:
    std::atomic<bool> _eos = false;
};

template <typename BlockItem>
class ConcurrentBlockQueue final : public BlockQueueBase<BlockItem> {
public:
    static constexpr bool IsSerialized = false;
    ConcurrentBlockQueue()
            : BlockQueueBase<BlockItem>(), _data_queue(moodycamel::ConcurrentQueue<BlockItem>()) {}
    size_t size_approx() const override { return _data_queue.size_approx(); }

    bool enqueue(BlockItem&& item) override {
        if (!BlockQueueBase<BlockItem>::_eos) {
            if (!_data_queue.enqueue(_ptok, std::move(item))) [[unlikely]] {
                throw Exception(ErrorCode::INTERNAL_ERROR,
                                "Exception occurs in data queue [size = {}] of local exchange.",
                                _data_queue.size_approx());
            }
            return true;
        }
        return false;
    }

    bool try_dequeue(BlockItem& item) override { return _data_queue.try_dequeue(item); }

private:
    moodycamel::ConcurrentQueue<BlockItem> _data_queue;
    moodycamel::ProducerToken _ptok {_data_queue};
};

template <typename BlockItem>
class SerializedBlockQueue final : public BlockQueueBase<BlockItem> {
public:
    static constexpr bool IsSerialized = true;
    SerializedBlockQueue() : BlockQueueBase<BlockItem>() {}
    size_t size_approx() const override { return _data_queue ? 1 : 0; }
    bool enqueue(BlockItem&& item) override {
        if (!BlockQueueBase<BlockItem>::_eos) {
            CHECK(_data_queue == nullptr);
            _data_queue = std::make_unique<BlockItem>(std::move(item));
            return true;
        }
        return false;
    }

    bool try_dequeue(BlockItem& item) override {
        if (!_data_queue) {
            return false;
        }
        item = std::move(*_data_queue.release());
        return true;
    }

private:
    std::unique_ptr<BlockItem> _data_queue = nullptr;
};

using BlockWrapperSPtr = std::shared_ptr<BlockWrapper>;

template <typename QueueType>
class Exchanger : public ExchangerBase {
public:
    Exchanger(int running_sink_operators, int num_partitions, int free_block_limit)
            : ExchangerBase(running_sink_operators, num_partitions, free_block_limit) {
        _data_queue.resize(num_partitions);
        _m.resize(num_partitions);
        for (size_t i = 0; i < num_partitions; i++) {
            _m[i] = std::make_unique<std::mutex>();
            _data_queue[i] = std::make_unique<QueueType>();
        }
    }
    Exchanger(int running_sink_operators, int num_sources, int num_partitions, int free_block_limit)
            : ExchangerBase(running_sink_operators, num_sources, num_partitions, free_block_limit) {
        _data_queue.resize(num_sources);
        _m.resize(num_sources);
        for (size_t i = 0; i < num_sources; i++) {
            _m[i] = std::make_unique<std::mutex>();
            _data_queue[i] = std::make_unique<QueueType>();
        }
    }
    ~Exchanger() override = default;
    std::string data_queue_debug_string(int i) override {
        return fmt::format("Data Queue {}: [size approx = {}, eos = {}]", i,
                           _data_queue[i]->size_approx(), _data_queue[i]->eos());
    }

protected:
    // Enqueue data block and set downstream source operator to read.
    void _enqueue_data_and_set_ready(RuntimeProfile::Counter* enqueue_rows_counter, int channel_id,
                                     LocalExchangeSinkLocalState* local_state,
                                     typename QueueType::BlockType&& block);
    bool _dequeue_data(RuntimeProfile::Counter* dequeue_rows_counter,
                       RuntimeProfile::Counter* get_block_failed_counter,
                       LocalExchangeSourceLocalState* local_state,
                       typename QueueType::BlockType& block, bool* eos,
                       vectorized::Block* data_block, int channel_id);

    void _enqueue_data_and_set_ready(RuntimeProfile::Counter* enqueue_rows_counter, int channel_id,
                                     typename QueueType::BlockType&& block);
    bool _dequeue_data(RuntimeProfile::Counter* dequeue_rows_counter,
                       RuntimeProfile::Counter* get_block_failed_counter,
                       typename QueueType::BlockType& block, bool* eos,
                       vectorized::Block* data_block, int channel_id);
    std::vector<std::unique_ptr<QueueType>> _data_queue;
    std::vector<std::unique_ptr<std::mutex>> _m;
};

class LocalExchangeSourceLocalState;
class LocalExchangeSinkLocalState;

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
struct BlockWrapper {
    ENABLE_FACTORY_CREATOR(BlockWrapper);
    BlockWrapper(vectorized::Block&& data_block_) : data_block(std::move(data_block_)) {}
    BlockWrapper(vectorized::Block& data_block_) : data_block(data_block_) {}
    ~BlockWrapper() { DCHECK_EQ(ref_count.load(), 0); }
    void ref(int delta) { ref_count += delta; }
    void unref(ExchangerBase* exchanger, LocalExchangeSharedState* shared_state,
               size_t allocated_bytes, int channel_id) {
        if (ref_count.fetch_sub(1) == 1) {
            DCHECK(allocated_bytes > 0 || shared_state == nullptr);
            if (shared_state) {
                shared_state->sub_total_mem_usage(allocated_bytes, channel_id);
            }
            if (data_block.mem_reuse() &&
                exchanger->_free_blocks.size_approx() <
                        exchanger->_free_block_limit * exchanger->_num_senders) {
                data_block.clear_column_data();
                // Free blocks is used to improve memory efficiency. Failure during pushing back
                // free block will not incur any bad result so just ignore the return value.
                exchanger->_free_blocks.enqueue(std::move(data_block));
            } else if (exchanger->_free_block_limit == 0) {
                // Columns in this block must be removed.
                data_block.clear();
            }
        }
    }

    void unref(ExchangerBase* exchanger, LocalExchangeSharedState* shared_state = nullptr,
               int channel_id = 0) {
        unref(exchanger, shared_state, data_block.allocated_bytes(), channel_id);
    }
    int ref_value() const { return ref_count.load(); }
    std::atomic<int> ref_count = 0;
    vectorized::Block data_block;
};

template <typename QueueType>
class ShuffleExchanger : public Exchanger<QueueType> {
public:
    using Parent = Exchanger<QueueType>;
    ENABLE_FACTORY_CREATOR(ShuffleExchanger);
    ShuffleExchanger(int running_sink_operators, int num_sources, int num_partitions,
                     int free_block_limit)
            : Exchanger<QueueType>(running_sink_operators, num_sources, num_partitions,
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
    template <typename ChannelIdType>
    Status _split_rows(Profile&& profile, RuntimeState* state,
                       const ChannelIdType* __restrict channel_ids, vectorized::Block* block,
                       int channel_id, ChannelSelector* channel_selector,
                       LocalExchangeSinkLocalState* local_state);
    template <typename ChannelIdType>
    Status _split_rows(Profile&& profile, RuntimeState* state,
                       const ChannelIdType* __restrict channel_ids, vectorized::Block* block,
                       int channel_id, ChannelSelector* channel_selector);
    std::vector<std::vector<uint32_t>> _partition_rows_histogram;
};

class BucketShuffleExchanger final
        : public ShuffleExchanger<ConcurrentBlockQueue<PartitionedBlock>> {
    ENABLE_FACTORY_CREATOR(BucketShuffleExchanger);
    BucketShuffleExchanger(int running_sink_operators, int num_sources, int num_partitions,
                           int free_block_limit)
            : ShuffleExchanger<ConcurrentBlockQueue<PartitionedBlock>>(
                      running_sink_operators, num_sources, num_partitions, free_block_limit) {}
    ~BucketShuffleExchanger() override = default;
    ExchangeType get_type() const override { return ExchangeType::BUCKET_HASH_SHUFFLE; }
};

template <typename QueueType>
class PassthroughExchanger final : public Exchanger<QueueType> {
public:
    using Parent = Exchanger<QueueType>;
    ENABLE_FACTORY_CREATOR(PassthroughExchanger);
    PassthroughExchanger(int running_sink_operators, int num_partitions, int free_block_limit)
            : Exchanger<QueueType>(running_sink_operators, num_partitions, free_block_limit) {}
    ~PassthroughExchanger() override = default;
    Status sink(RuntimeState* state, vectorized::Block* in_block, bool eos, Profile&& profile,
                SinkInfo&& sink_info) override;

    Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos, Profile&& profile,
                     SourceInfo&& source_info) override;
    ExchangeType get_type() const override { return ExchangeType::PASSTHROUGH; }
    void close(SourceInfo&& source_info) override;
};

class PassToOneExchanger final : public Exchanger<ConcurrentBlockQueue<BlockWrapperSPtr>> {
public:
    ENABLE_FACTORY_CREATOR(PassToOneExchanger);
    PassToOneExchanger(int running_sink_operators, int num_partitions, int free_block_limit)
            : Exchanger<ConcurrentBlockQueue<BlockWrapperSPtr>>(running_sink_operators,
                                                                num_partitions, free_block_limit) {}
    ~PassToOneExchanger() override = default;
    Status sink(RuntimeState* state, vectorized::Block* in_block, bool eos, Profile&& profile,
                SinkInfo&& sink_info) override;

    Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos, Profile&& profile,
                     SourceInfo&& source_info) override;
    ExchangeType get_type() const override { return ExchangeType::PASS_TO_ONE; }
    void close(SourceInfo&& source_info) override;
};

class LocalMergeSortExchanger final : public Exchanger<ConcurrentBlockQueue<BlockWrapperSPtr>> {
public:
    ENABLE_FACTORY_CREATOR(LocalMergeSortExchanger);
    LocalMergeSortExchanger(std::shared_ptr<SortSourceOperatorX> sort_source,
                            int running_sink_operators, int num_partitions, int free_block_limit)
            : Exchanger<ConcurrentBlockQueue<BlockWrapperSPtr>>(running_sink_operators,
                                                                num_partitions, free_block_limit),
              _sort_source(std::move(sort_source)) {}
    ~LocalMergeSortExchanger() override = default;
    Status sink(RuntimeState* state, vectorized::Block* in_block, bool eos, Profile&& profile,
                SinkInfo&& sink_info) override;

    Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos, Profile&& profile,
                     SourceInfo&& source_info) override;
    ExchangeType get_type() const override { return ExchangeType::LOCAL_MERGE_SORT; }

    Status build_merger(Profile&& profile, RuntimeState* statem,
                        LocalExchangeSourceLocalState* local_state);

    void close(SourceInfo&& source_info) override {}
    void finalize() override;

private:
    std::unique_ptr<vectorized::VSortedRunMerger> _merger;
    std::shared_ptr<SortSourceOperatorX> _sort_source;
    std::vector<std::atomic_int64_t> _queues_mem_usege;
};

template <typename QueueType>
class BroadcastExchanger final : public Exchanger<QueueType> {
public:
    using Parent = Exchanger<QueueType>;
    ENABLE_FACTORY_CREATOR(BroadcastExchanger);
    BroadcastExchanger(int running_sink_operators, int num_partitions, int free_block_limit)
            : Exchanger<QueueType>(running_sink_operators, num_partitions, free_block_limit) {}
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
class AdaptivePassthroughExchanger : public Exchanger<ConcurrentBlockQueue<BlockWrapperSPtr>> {
public:
    ENABLE_FACTORY_CREATOR(AdaptivePassthroughExchanger);
    AdaptivePassthroughExchanger(int running_sink_operators, int num_partitions,
                                 int free_block_limit)
            : Exchanger<ConcurrentBlockQueue<BlockWrapperSPtr>>(running_sink_operators,
                                                                num_partitions, free_block_limit) {
        _partition_rows_histogram.resize(running_sink_operators);
    }
    Status sink(RuntimeState* state, vectorized::Block* in_block, bool eos, Profile&& profile,
                SinkInfo&& sink_info) override;

    Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos, Profile&& profile,
                     SourceInfo&& source_info) override;
    ExchangeType get_type() const override { return ExchangeType::ADAPTIVE_PASSTHROUGH; }

    void close(SourceInfo&& source_info) override;

private:
    Status _passthrough_sink(Profile&& profile, RuntimeState* state, vectorized::Block* in_block,
                             SinkInfo&& sink_info);
    Status _shuffle_sink(Profile&& profile, RuntimeState* state, vectorized::Block* in_block,
                         SinkInfo&& sink_info);
    Status _split_rows(Profile&& profile, RuntimeState* state,
                       const uint32_t* __restrict channel_ids, vectorized::Block* block,
                       SinkInfo&& sink_info);

    std::atomic_bool _is_pass_through = false;
    std::atomic_int32_t _total_block = 0;
    std::vector<std::vector<uint32_t>> _partition_rows_histogram;
};
#include "common/compile_check_end.h"
} // namespace pipeline
} // namespace doris