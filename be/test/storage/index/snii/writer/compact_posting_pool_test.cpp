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

#include "snii/writer/compact_posting_pool.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <random>
#include <utility>
#include <vector>

#include "common/status.h"

using snii::writer::CompactPostingPool;

namespace {

// Test helper bundling a chain's append handle, its level, and its head -- the
// same per-term state the real accumulator keeps -- so tests can append by value.
struct Chain {
    CompactPostingPool::SliceWriter w;
    uint8_t level = 0;
    uint32_t head = 0;
    void start(CompactPostingPool* pool) { head = pool->start_chain(&w, &level); }
    void put(CompactPostingPool* pool, uint8_t b) { pool->append_byte(&w, &level, b); }
};

// Reads back the whole chain into a vector for comparison.
std::vector<uint8_t> ReadChain(const CompactPostingPool& pool, uint32_t head, uint64_t len) {
    std::vector<uint8_t> out;
    out.reserve(len);
    CompactPostingPool::Cursor c = pool.cursor(head, len);
    while (c.has_next()) {
        out.push_back(c.next());
    }
    return out;
}

} // namespace

// A single chain shorter than one slice round-trips exactly.
TEST(SniiCompactPostingPool, TinyChainRoundTrips) {
    CompactPostingPool pool;
    Chain ch;
    ch.start(&pool);
    const std::vector<uint8_t> data = {7, 0, 255, 42};
    for (uint8_t b : data) {
        ch.put(&pool, b);
    }
    EXPECT_EQ(ReadChain(pool, ch.head, data.size()), data);
    EXPECT_EQ(pool.payload_bytes(), data.size());
}

// A chain that spans many slice levels round-trips exactly (exercises forward
// pointers across several geometric slice sizes).
TEST(SniiCompactPostingPool, MultiSliceChainRoundTrips) {
    CompactPostingPool pool;
    Chain ch;
    ch.start(&pool);
    std::vector<uint8_t> data;
    for (uint32_t i = 0; i < 5000; ++i) {
        data.push_back(static_cast<uint8_t>(i * 31 + 7));
    }
    for (uint8_t b : data) {
        ch.put(&pool, b);
    }
    EXPECT_EQ(ReadChain(pool, ch.head, data.size()), data);
    EXPECT_EQ(pool.payload_bytes(), data.size());
}

// Many INTERLEAVED chains (the real SPIMI access pattern) stay independent: a byte
// written to chain A never appears in chain B's read-back.
TEST(SniiCompactPostingPool, InterleavedChainsIndependent) {
    CompactPostingPool pool;
    constexpr int kChains = 64;
    std::vector<Chain> chains(kChains);
    std::vector<std::vector<uint8_t>> expect(kChains);
    for (auto& ch : chains) {
        ch.start(&pool);
    }

    std::mt19937 rng(12345);
    // Append bytes to chains in a randomized interleaving so slices for different
    // chains land in the same blocks intermixed.
    for (int round = 0; round < 20000; ++round) {
        const int c = static_cast<int>(rng() % kChains);
        const auto b = static_cast<uint8_t>(rng());
        chains[c].put(&pool, b);
        expect[c].push_back(b);
    }
    for (int i = 0; i < kChains; ++i) {
        EXPECT_EQ(ReadChain(pool, chains[i].head, expect[i].size()), expect[i]) << "chain " << i;
    }
}

// MANY chains + MANY bytes force the arena across several 32 KiB block
// boundaries. This is the regression for a block-boundary bump bug: a run that
// exactly fills a block must allocate the next block before handing out the
// boundary offset, never returning an offset into a not-yet-allocated block.
TEST(SniiCompactPostingPool, ManyChainsAcrossBlockBoundaries) {
    CompactPostingPool pool;
    constexpr int kChains = 2000;
    std::vector<Chain> chains(kChains);
    std::vector<std::vector<uint8_t>> expect(kChains);
    for (auto& ch : chains) {
        ch.start(&pool);
    }

    std::mt19937 rng(98765);
    for (int round = 0; round < 1'000'000; ++round) {
        const int c = static_cast<int>(rng() % kChains);
        const auto b = static_cast<uint8_t>(rng());
        chains[c].put(&pool, b);
        expect[c].push_back(b);
    }
    for (int i = 0; i < kChains; ++i) {
        EXPECT_EQ(ReadChain(pool, chains[i].head, expect[i].size()), expect[i]) << "chain " << i;
    }
}

// An empty chain (started but never written) reads back as zero bytes.
TEST(SniiCompactPostingPool, EmptyChain) {
    CompactPostingPool pool;
    Chain ch;
    ch.start(&pool);
    EXPECT_TRUE(ReadChain(pool, ch.head, 0).empty());
}

// A chain that exactly fills a slice boundary (no extra byte) reads back exactly,
// without dereferencing the (still-zero) forward pointer.
TEST(SniiCompactPostingPool, ExactSliceBoundary) {
    CompactPostingPool pool;
    Chain ch;
    ch.start(&pool);
    // Exactly fill the level-0 slice (kSliceSizes[0] payload bytes) and stop.
    std::vector<uint8_t> data;
    for (uint32_t i = 0; i < CompactPostingPool::kSliceSizes_level0(); ++i) {
        data.push_back(static_cast<uint8_t>(i + 1));
    }
    for (uint8_t b : data) {
        ch.put(&pool, b);
    }
    EXPECT_EQ(ReadChain(pool, ch.head, data.size()), data);
    // Now extend by one byte (forces the forward link) and re-read fully.
    ch.put(&pool, 99);
    data.push_back(99);
    EXPECT_EQ(ReadChain(pool, ch.head, data.size()), data);
}

// Cursor CONTRACT: `budget` is an UPPER BOUND on bytes yielded, NOT a required exact
// length. The cursor is SELF-TERMINATING -- it stops at the chain tail (a zero forward
// pointer) no matter how large the budget. This pins both halves of the single contract:
//   (1) an exact-length budget round-trips the written bytes, and
//   (2) an OVER-LARGE budget (as the production caller passes -- the write-head offset)
//       stays MEMORY-SAFE: next() never follows the tail's zero forward pointer off the
//       chain into block 0 (UB); it yields at most the tail-slice's payload region (the
//       written bytes plus the slice's zero-initialized unwritten tail) and then stops.
//
// WITHOUT the tail check (next_head == 0 -> stop), looping on has_next() with an
// over-large budget would hit the slice boundary, read the still-zero tail forward
// pointer, jump cur_ to offset 0, and re-read block 0's live bytes (an ALIAS) -- exactly
// the misuse the old "remaining" contract left latent. This test fails without the fix.
TEST(SniiCompactPostingPool, CursorOverLargeBudgetSelfTerminates) {
    CompactPostingPool pool;
    // Lay down a DISTINCT first chain so block 0 offset 0 holds recognizable bytes; if the
    // cursor ever aliased offset 0 it would yield these, which we assert it never does.
    Chain decoy;
    decoy.start(&pool);
    const std::vector<uint8_t> decoy_bytes = {0xDE, 0xAD, 0xBE, 0xEF};
    for (uint8_t b : decoy_bytes) {
        decoy.put(&pool, b);
    }
    ASSERT_EQ(decoy.head, 0U) << "first chain must own pool offset 0 (the alias target)";

    // A second short chain whose tail forward pointer is still zero (single level-0 slice).
    Chain ch;
    ch.start(&pool);
    const std::vector<uint8_t> data = {11, 22, 33};
    for (uint8_t b : data) {
        ch.put(&pool, b);
    }

    // (1) exact-length budget round-trips the written bytes.
    EXPECT_EQ(ReadChain(pool, ch.head, data.size()), data);

    // (2) MISUSE: a budget far larger than the payload. The cursor must self-terminate at
    // the chain tail, yielding at most the level-0 slice's payload region and stopping.
    const uint32_t slice0 = CompactPostingPool::kSliceSizes_level0();
    const uint32_t over_budget = 100U * CompactPostingPool::kBlockSize; // absurdly large
    CompactPostingPool::Cursor c = pool.cursor(ch.head, over_budget);
    std::vector<uint8_t> pulled;
    uint32_t pulls = 0;
    while (c.has_next()) {
        pulled.push_back(c.next());
        ASSERT_LT(++pulls, over_budget) << "cursor failed to self-terminate at the chain tail";
    }
    // It stops at the tail: exactly the level-0 slice's payload region (written bytes plus
    // the slice's zero-initialized unwritten tail), and NOTHING beyond.
    EXPECT_EQ(pulled.size(), slice0)
            << "over-large budget must stop at the tail slice's end, not run on";
    ASSERT_GE(pulled.size(), data.size());
    EXPECT_EQ(std::vector<uint8_t>(pulled.begin(), pulled.begin() + data.size()), data);
    for (size_t i = data.size(); i < pulled.size(); ++i) {
        EXPECT_EQ(pulled[i], 0U) << "unwritten tail byte " << i << " must read as zero";
    }
    // CRITICAL: the cursor must NEVER have aliased the decoy at offset 0.
    for (uint8_t b : pulled) {
        EXPECT_NE(b, 0xDEU) << "cursor aliased block 0 -- tail check failed";
    }
}

// Computes an EXACT one-block fill layout analytically from the public slice schedule:
// `pad` level-0 chains (each consuming one level-0 slice allocation) followed by ONE
// chain grown until it has just allocated the slice at `grow_to_level`, such that the
// total arena bytes consumed equal exactly kBlockSize. Returns false if no such layout
// exists for the current schedule. No magic numbers -- everything derives from the
// kSliceSize_at / kNextLevel_at accessors, so a schedule change is handled automatically.
static bool ExactBlockFillLayout(uint32_t* pad, int* grow_to_level) {
    const uint32_t block = CompactPostingPool::kBlockSize;
    const uint32_t l0_alloc =
            CompactPostingPool::kSliceSizes_level0() + CompactPostingPool::kPtrBytes;
    // cum = bytes a single growing chain consumes after allocating slices for levels
    // 0..L inclusive (each slice allocation is payload-cap + kPtrBytes).
    uint32_t cum = 0;
    for (int level = 0; level < CompactPostingPool::kLevelCount; ++level) {
        cum += CompactPostingPool::kSliceSize_at(level) + CompactPostingPool::kPtrBytes;
        if (cum > block) {
            break;
        }
        const uint32_t rem = block - cum;
        if (rem % l0_alloc == 0) {
            *pad = rem / l0_alloc;
            *grow_to_level = level;
            return true;
        }
    }
    return false;
}

// DETERMINISTIC coverage of alloc_run case (c): a previous allocation EXACTLY fills a
// 32 KiB block, leaving next_offset_ on the block boundary (in_block == 0) over a block
// that is already fully consumed. The next allocation MUST detect this (via the
// tail_exists guard) and start a FRESH block -- it must NOT mistake in_block == 0 for an
// empty fresh block and hand back offset 0, which would alias block 0's live bytes.
//
// Today this branch is only hit probabilistically by the RNG-seeded interleave tests.
// Here we drive it EXACTLY: compute a layout (analytically, from the public schedule)
// whose allocations fill block 0 to its LAST byte (next_offset_ == kBlockSize), realize
// it on a real pool, then start one more chain. Its head MUST be exactly kBlockSize
// (block 1, byte 0) -- never 0 -- and every chain in block 0 MUST still round-trip,
// proving the case-(c) allocation did not return offset 0 and clobber block 0.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST(SniiCompactPostingPool, AllocRunExactBlockFillStartsFreshBlock) {
    const uint32_t block = CompactPostingPool::kBlockSize;

    uint32_t pad = 0;
    int grow_to_level = 0;
    ASSERT_TRUE(ExactBlockFillLayout(&pad, &grow_to_level))
            << "no exact one-block fill exists for the current slice schedule";

    CompactPostingPool pool;
    std::vector<Chain> chains;
    std::vector<std::vector<uint8_t>> data;

    // Lay down `pad` level-0 chains, each with one identifiable payload byte. None may
    // spill out of block 0 (the layout was computed so they all fit).
    for (uint32_t i = 0; i < pad; ++i) {
        Chain c;
        c.start(&pool);
        const auto b = static_cast<uint8_t>(0x40 + (i & 0x3F));
        c.put(&pool, b);
        chains.push_back(c);
        data.push_back({b});
        ASSERT_EQ(pool.arena_bytes(), block) << "padding chain " << i << " spilled early";
    }

    // Grow ONE chain until it has just allocated the slice at `grow_to_level`, writing no
    // byte into that final slice so its tail ends exactly on the block boundary.
    Chain g;
    g.start(&pool);
    std::vector<uint8_t> gbytes;
    uint32_t guard = 0;
    while (std::cmp_less(g.level, grow_to_level)) {
        const auto b = static_cast<uint8_t>(0x80 + (gbytes.size() & 0x3F));
        g.put(&pool, b);
        gbytes.push_back(b);
        ASSERT_EQ(pool.arena_bytes(), block) << "grown chain spilled before the boundary";
        ASSERT_LT(++guard, block) << "grow loop did not converge";
    }
    chains.push_back(g);
    data.push_back(gbytes);

    // Block 0 is now full to its LAST byte: next_offset_ == kBlockSize, still one block.
    ASSERT_EQ(pool.arena_bytes(), block) << "block 0 should be exactly full, one block";

    // The case-(c) allocation: starting a new chain must open a FRESH block at offset
    // kBlockSize, NOT alias offset 0.
    Chain probe;
    probe.start(&pool);
    EXPECT_EQ(probe.head, block) << "case (c) must hand out block 1 byte 0, not offset 0";
    EXPECT_NE(probe.head, 0U) << "case (c) must not alias block 0";
    EXPECT_EQ(pool.arena_bytes(), 2U * block) << "exactly two blocks after the probe";

    // The probe chain owns block 1 byte 0; write+read it to confirm it is live.
    probe.put(&pool, 0xEE);
    EXPECT_EQ(ReadChain(pool, probe.head, 1U), (std::vector<uint8_t> {0xEE}));

    // Every chain laid down in block 0 must still round-trip -- proving the case-(c)
    // allocation did NOT return offset 0 and overwrite block 0's live bytes.
    for (size_t i = 0; i < chains.size(); ++i) {
        EXPECT_EQ(ReadChain(pool, chains[i].head, data[i].size()), data[i])
                << "block-0 chain " << i << " corrupted across the exact block boundary";
    }
}

// reset() drops all blocks; a fresh chain after reset starts clean.
TEST(SniiCompactPostingPool, ResetClears) {
    CompactPostingPool pool;
    Chain ch;
    ch.start(&pool);
    for (int i = 0; i < 1000; ++i) {
        ch.put(&pool, static_cast<uint8_t>(i));
    }
    EXPECT_GT(pool.payload_bytes(), 0U);
    pool.reset();
    EXPECT_EQ(pool.payload_bytes(), 0U);
    Chain ch2;
    ch2.start(&pool);
    std::vector<uint8_t> data = {5, 6, 7};
    for (uint8_t b : data) {
        ch2.put(&pool, b);
    }
    EXPECT_EQ(ReadChain(pool, ch2.head, data.size()), data);
}
