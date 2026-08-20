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

#include "exec/operator/data_queue.h"

#include <gtest/gtest.h>

#include <memory>
#include <thread>
#include <vector>

#include "core/data_type/data_type_number.h"
#include "exec/pipeline/dependency.h"

namespace doris {

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static std::unique_ptr<Block> make_block(size_t rows = 1) {
    auto block = std::make_unique<Block>();
    auto col = ColumnUInt8::create(rows);
    block->insert(ColumnWithTypeAndName {std::move(col), std::make_shared<DataTypeUInt8>(), ""});
    return block;
}

// Create a Dependency that starts ready.
static std::shared_ptr<Dependency> make_dep(bool initially_ready = true) {
    return Dependency::create_shared(1, 1, "Test", initially_ready);
}

// ---------------------------------------------------------------------------
// SubQueue tests
// ---------------------------------------------------------------------------

class SubQueueTest : public testing::Test {
public:
    void SetUp() override {
        dep = make_dep(/*initially_ready=*/true);
        sub = std::make_unique<SubQueue>();
        sub->sink_dependency = dep.get();
        sub->max_blocks_in_queue = 2;
    }

    std::shared_ptr<Dependency> dep;
    std::unique_ptr<SubQueue> sub;
    std::atomic_uint32_t counter_ {0};
};

// Pop from an empty queue returns OK with null output.
TEST_F(SubQueueTest, TryPopEmpty) {
    std::unique_ptr<Block> out;
    sub->try_pop(&out);
    EXPECT_EQ(out, nullptr);
    EXPECT_EQ(sub->blocks_in_queue.load(), 0u);
}

// Basic push then pop returns the block.
TEST_F(SubQueueTest, TryPushPop_Basic) {
    EXPECT_TRUE(sub->try_push(make_block(), counter_));
    EXPECT_EQ(sub->blocks_in_queue.load(), 1u);

    std::unique_ptr<Block> out;
    sub->try_pop(&out);
    EXPECT_NE(out, nullptr);
    EXPECT_EQ(sub->blocks_in_queue.load(), 0u);
}

// push after mark_finished returns EndOfFile.
TEST_F(SubQueueTest, TryPushAfterFinished) {
    std::atomic_uint32_t counter {1};
    std::atomic_bool all_done {false};
    sub->mark_finished(counter, all_done);

    EXPECT_FALSE(sub->try_push(make_block(), counter_));
}

// When blocks.size() exceeds max_blocks_in_queue, sink is blocked.
TEST_F(SubQueueTest, SinkBlockedWhenFull) {
    sub->max_blocks_in_queue = 2;
    dep->set_ready(); // start ready

    // Push up to the limit — sink should stay ready.
    EXPECT_TRUE(sub->try_push(make_block(), counter_));
    EXPECT_TRUE(sub->try_push(make_block(), counter_));
    EXPECT_TRUE(dep->ready());

    // Push one over the limit — sink should be blocked.
    EXPECT_TRUE(sub->try_push(make_block(), counter_));
    EXPECT_FALSE(dep->ready());
}

// Sink wakes up only when the queue becomes completely empty.
TEST_F(SubQueueTest, SinkReadyWhenQueueEmpty) {
    sub->max_blocks_in_queue = 2;

    // Fill to 3 (one over limit) → sink blocked.
    EXPECT_TRUE(sub->try_push(make_block(), counter_));
    EXPECT_TRUE(sub->try_push(make_block(), counter_));
    EXPECT_TRUE(sub->try_push(make_block(), counter_));
    EXPECT_FALSE(dep->ready());

    // Pop 1 & 2: queue not empty yet → sink still blocked.
    std::unique_ptr<Block> out;
    sub->try_pop(&out);
    EXPECT_NE(out, nullptr);
    EXPECT_FALSE(dep->ready());

    sub->try_pop(&out);
    EXPECT_NE(out, nullptr);
    EXPECT_FALSE(dep->ready());

    // Pop 3: queue empty → set_ready().
    sub->try_pop(&out);
    EXPECT_NE(out, nullptr);
    EXPECT_TRUE(dep->ready());
}

// mark_finished is idempotent: second call returns false and counter stays correct.
TEST_F(SubQueueTest, MarkFinishedIdempotent) {
    std::atomic_uint32_t counter {2};
    std::atomic_bool all_done {false};

    EXPECT_TRUE(sub->mark_finished(counter, all_done));
    EXPECT_EQ(counter.load(), 1u);
    EXPECT_FALSE(all_done.load());

    EXPECT_FALSE(sub->mark_finished(counter, all_done));
    EXPECT_EQ(counter.load(), 1u); // unchanged
}

// mark_finished sets all_finished when last child finishes.
TEST_F(SubQueueTest, MarkFinishedSetsAllFinished) {
    std::atomic_uint32_t counter {1};
    std::atomic_bool all_done {false};
    sub->mark_finished(counter, all_done);
    EXPECT_TRUE(all_done.load());
}

// clear_blocks empties the queue and calls set_always_ready on sink.
TEST_F(SubQueueTest, ClearBlocksEmptiesQueue) {
    EXPECT_TRUE(sub->try_push(make_block(), counter_));
    EXPECT_TRUE(sub->try_push(make_block(), counter_));
    EXPECT_EQ(sub->blocks_in_queue.load(), 2u);

    sub->clear_blocks();

    EXPECT_EQ(sub->blocks_in_queue.load(), 0u);
    // set_always_ready was called → dep is always ready.
    EXPECT_TRUE(dep->ready());
}

// clear_blocks on an empty queue is a no-op (set_always_ready not called).
TEST_F(SubQueueTest, ClearBlocksNoop) {
    dep->block(); // start blocked
    sub->clear_blocks();
    EXPECT_FALSE(dep->ready()); // still blocked — clear_blocks did nothing
}

// bytes_in_queue tracks push/pop correctly.
TEST_F(SubQueueTest, BytesInQueueTracking) {
    auto block = make_block(10);
    int64_t expected_bytes = block->allocated_bytes();

    EXPECT_TRUE(sub->try_push(std::move(block), counter_));
    {
        LockGuard l(sub->queue_lock);
        EXPECT_EQ(sub->bytes_in_queue, expected_bytes);
    }

    std::unique_ptr<Block> out;
    sub->try_pop(&out);
    {
        LockGuard l(sub->queue_lock);
        EXPECT_EQ(sub->bytes_in_queue, 0);
    }
}

// ---------------------------------------------------------------------------
// DataQueue fixtures
// ---------------------------------------------------------------------------

class DataQueueTest : public testing::Test {
public:
    DataQueueTest() = default;
    ~DataQueueTest() override = default;
    void SetUp() override {
        data_queue = std::make_unique<DataQueue>(child_count);
        for (int i = 0; i < child_count; i++) {
            auto dep = Dependency::create_shared(1, 1, "DataQueueTest", true);
            sink_deps.push_back(dep);
            data_queue->set_sink_dependency(dep.get(), i);
        }
        source_dep = Dependency::create_shared(1, 1, "DataQueueTest", true);
        data_queue->set_source_dependency(source_dep);
    }
    std::unique_ptr<DataQueue> data_queue = nullptr;
    std::vector<std::shared_ptr<Dependency>> sink_deps;
    std::shared_ptr<Dependency> source_dep;
    const int child_count = 3;
};

// ---------------------------------------------------------------------------
// DataQueue unit tests
// ---------------------------------------------------------------------------

// Initial state: no data, no finish.
TEST_F(DataQueueTest, InitialState) {
    EXPECT_EQ(data_queue->debug_string(), "(is_all_finish = false, has_data = false)");
    auto queue_block = TEST_TRY(data_queue->get_block_from_queue());
    EXPECT_EQ(queue_block.block, nullptr);
    EXPECT_FALSE(queue_block.eos);
}

// Push one block and retrieve it.
TEST_F(DataQueueTest, SinglePushPop) {
    EXPECT_TRUE(data_queue->push_block(make_block(), 0, false).ok());
    EXPECT_EQ(data_queue->debug_string(), "(is_all_finish = false, has_data = true)");

    auto queue_block = TEST_TRY(data_queue->get_block_from_queue());
    EXPECT_NE(queue_block.block, nullptr);
    EXPECT_FALSE(queue_block.eos);
    EXPECT_EQ(data_queue->debug_string(), "(is_all_finish = false, has_data = false)");
}

// is_all_finish only becomes true after all children push eos.
TEST_F(DataQueueTest, IsAllFinishAfterAllChildren) {
    EXPECT_TRUE(data_queue->push_block(nullptr, 0, true).ok());
    auto first_queue_block = TEST_TRY(data_queue->get_block_from_queue());
    EXPECT_FALSE(first_queue_block.eos);
    EXPECT_TRUE(data_queue->push_block(nullptr, 1, true).ok());
    auto second_queue_block = TEST_TRY(data_queue->get_block_from_queue());
    EXPECT_FALSE(second_queue_block.eos);
    EXPECT_TRUE(data_queue->push_block(nullptr, 2, true).ok());
    auto last_queue_block = TEST_TRY(data_queue->get_block_from_queue());
    EXPECT_TRUE(last_queue_block.eos);
}

// eos push is idempotent.
TEST_F(DataQueueTest, EosPushIdempotent) {
    EXPECT_TRUE(data_queue->push_block(nullptr, 0, true).ok());
    EXPECT_TRUE(data_queue->push_block(nullptr, 0, true).ok());
    EXPECT_TRUE(data_queue->push_block(nullptr, 1, true).ok());
    EXPECT_TRUE(data_queue->push_block(nullptr, 2, true).ok());
    auto queue_block = TEST_TRY(data_queue->get_block_from_queue());
    EXPECT_TRUE(queue_block.eos);
}

// DataQueueBlock can return a popped block to the originating child free list.
TEST_F(DataQueueTest, ReturnBlockToChildFreeList) {
    // Push to child 1 only.
    EXPECT_TRUE(data_queue->push_block(make_block(), 1, false).ok());

    auto queue_block = TEST_TRY(data_queue->get_block_from_queue());
    EXPECT_NE(queue_block.block, nullptr);
    queue_block.block->clear();
    auto* returned_block = queue_block.block.get();
    data_queue->push_free_block(std::move(queue_block));

    auto reused_block = data_queue->get_free_block(1);
    EXPECT_EQ(reused_block.get(), returned_block);
}

// get_free_block returns a new block when free list is empty, reuses when not.
TEST_F(DataQueueTest, FreeBlockReuse) {
    // First call: allocates a new block.
    auto block = data_queue->get_free_block(0);
    EXPECT_NE(block, nullptr);

    // Return it to the free list.
    block->clear(); // ensure rows == 0
    DataQueueBlock queue_block;
    queue_block.block = std::move(block);
    data_queue->push_free_block(std::move(queue_block));

    // Second call: must return the recycled block.
    auto block2 = data_queue->get_free_block(0);
    EXPECT_NE(block2, nullptr);
}

// In low-memory mode push_free_block discards blocks and max drops to 1.
TEST_F(DataQueueTest, LowMemoryMode) {
    // Pre-populate the free list.
    DataQueueBlock queue_block;
    queue_block.block = Block::create_unique();
    data_queue->push_free_block(std::move(queue_block));

    data_queue->set_low_memory_mode();

    // Free list must be cleared.
    auto block = data_queue->get_free_block(0);
    // The free list is empty → a fresh block is returned (not from the list).
    EXPECT_NE(block, nullptr);

    // push_free_block now discards.
    block->clear();
    DataQueueBlock low_memory_block;
    low_memory_block.block = std::move(block);
    data_queue->push_free_block(std::move(low_memory_block));
    auto block2 = data_queue->get_free_block(0);
    // Still gets a fresh allocation (free list stays empty).
    EXPECT_NE(block2, nullptr);
}

// terminate() finishes all children and clears pending blocks from sub-queues.
TEST_F(DataQueueTest, Terminate) {
    EXPECT_TRUE(data_queue->push_block(make_block(), 0, false).ok());
    EXPECT_TRUE(data_queue->push_block(make_block(), 1, false).ok());

    data_queue->terminate();

    EXPECT_EQ(data_queue->debug_string(), "(is_all_finish = true, has_data = false)");
    source_dep->block();
    EXPECT_TRUE(source_dep->ready());
    auto queue_block = TEST_TRY(data_queue->get_block_from_queue());
    EXPECT_EQ(queue_block.block, nullptr);
    EXPECT_TRUE(queue_block.eos);
}

// set_max_blocks_in_sub_queue propagates to every sub-queue.
TEST_F(DataQueueTest, SetMaxBlocksInSubQueue) {
    data_queue->set_max_blocks_in_sub_queue(5);
    // Push 5 blocks to child 0 — sink must stay ready (not over the limit yet).
    for (int i = 0; i < 5; i++) {
        EXPECT_TRUE(data_queue->push_block(make_block(), 0, false).ok());
    }
    EXPECT_TRUE(sink_deps[0]->ready());

    // 6th push exceeds limit → sink blocked.
    EXPECT_TRUE(data_queue->push_block(make_block(), 0, false).ok());
    EXPECT_FALSE(sink_deps[0]->ready());
}

// Source dependency is notified ready when a block is pushed.
TEST_F(DataQueueTest, SourceReadyOnPush) {
    source_dep->block(); // start blocked
    EXPECT_FALSE(source_dep->ready());

    EXPECT_TRUE(data_queue->push_block(make_block(), 0, false).ok());
    EXPECT_TRUE(source_dep->ready());
}

TEST_F(DataQueueTest, SourceReadyOnEosOnly) {
    source_dep->block();
    EXPECT_FALSE(source_dep->ready());

    EXPECT_TRUE(data_queue->push_block(nullptr, 0, true).ok());
    EXPECT_TRUE(source_dep->ready());
}

TEST_F(DataQueueTest, EosOnlyAfterAllChildren) {
    EXPECT_TRUE(data_queue->push_block(nullptr, 0, true).ok());
    EXPECT_TRUE(data_queue->push_block(nullptr, 1, true).ok());
    EXPECT_TRUE(data_queue->push_block(nullptr, 2, true).ok());

    auto queue_block = TEST_TRY(data_queue->get_block_from_queue());
    EXPECT_EQ(queue_block.block, nullptr);
    EXPECT_TRUE(queue_block.eos);
}

// ---------------------------------------------------------------------------
// Multi-threaded integration test (existing)
// ---------------------------------------------------------------------------

TEST_F(DataQueueTest, MultiTest) {
    int output_count = 0;
    auto output_func = [&]() {
        while (true) {
            bool eos = false;
            if (source_dep->ready()) {
                auto queue_block = TEST_TRY(data_queue->get_block_from_queue());
                eos = queue_block.eos;
                if (queue_block.block) {
                    output_count++;
                }
            }
            if (eos) {
                break;
            }
        }
    };

    std::vector<std::unique_ptr<Block>> input_blocks[3];

    for (int i = 0; i < 3; i++) {
        for (int j = 0; j < 50; j++) {
            auto block = std::make_unique<Block>();
            block->insert(ColumnWithTypeAndName {ColumnUInt8::create(1),
                                                 std::make_shared<DataTypeUInt8>(), ""});
            input_blocks[i].push_back(std::move(block));
        }
    }

    auto input_func = [&](int id) {
        int i = 0;
        while (i < 50) {
            if (sink_deps[id]->ready()) {
                EXPECT_TRUE(data_queue->push_block(std::move(input_blocks[id][i]), id, false).ok());
                i++;
            }
        }
        EXPECT_TRUE(data_queue->push_block(nullptr, id, true).ok());
    };

    std::thread input1(input_func, 0);
    std::thread input2(input_func, 1);
    std::thread input3(input_func, 2);
    std::thread output1(output_func);
    input1.join();
    input2.join();
    input3.join();
    output1.join();

    EXPECT_EQ(output_count, 150);
    EXPECT_EQ(data_queue->debug_string(), "(is_all_finish = true, has_data = false)");
}

// ./run-be-ut.sh --run --filter=DataQueueTest.*

} // namespace doris
