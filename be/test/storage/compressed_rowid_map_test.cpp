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
#include "storage/compressed_rowid_map.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdio>
#include <random>
#include <utility>
#include <vector>

#include "runtime/thread_context.h"
#include "storage/index/inverted/snii/compaction/rowid_conversion.h"
#include "storage/rowid_conversion.h"

namespace doris {
namespace {
using Map = CompressedRowIdMap;
constexpr uint64_t MISSING = Map::MISSING;

void check_rows(const Map& map, uint32_t source, const std::vector<uint64_t>& expected,
                Map::Cursor& cursor, Map::DecodeWorkspace& workspace) {
    for (size_t row = 0; row < expected.size(); ++row) {
        uint64_t target = 0;
        ASSERT_TRUE(map.get(source, static_cast<uint32_t>(row), cursor, workspace, &target).ok());
        ASSERT_EQ(target, expected[row]) << "source=" << source << " row=" << row;
    }
}

TEST(CompressedRowIdMapTest, EmptyLinearAndVeryLargeSourceDoNotAllocateDenseRows) {
    Map map;
    ASSERT_TRUE(map.init_source(0).ok());
    // Row count, not a presumed 1 GiB segment-size limit, determines the directory.
    ASSERT_TRUE(map.init_source(1'000'000'000).ok());
    ASSERT_TRUE(map.init_source(4097).ok());
    for (uint32_t row = 0; row < 4097; ++row) {
        ASSERT_TRUE(map.append(2, row, static_cast<uint64_t>(UINT32_MAX) + row).ok());
    }
    ASSERT_TRUE(map.finish().ok());
    auto usage = map.memory_usage();
    EXPECT_EQ(usage.directory_bytes, (244141 + 2) * sizeof(Map::BlockMeta));
    EXPECT_EQ(usage.payload_bytes, 0);
    EXPECT_EQ(usage.builder_bytes, 0);
    EXPECT_EQ(usage.encoder_bytes, 0);
    Map::Cursor cursor;
    Map::DecodeWorkspace workspace;
    uint64_t target;
    ASSERT_TRUE(map.get(1, 999'999'999, cursor, workspace, &target).ok());
    EXPECT_EQ(target, MISSING);
    ASSERT_TRUE(map.get(2, 4096, cursor, workspace, &target).ok());
    EXPECT_EQ(target, static_cast<uint64_t>(UINT32_MAX) + 4096);
    EXPECT_EQ(cursor.memory_usage(), 0);
    EXPECT_EQ(workspace.memory_usage(), 0);
}

TEST(CompressedRowIdMapTest, MixedBlocksGapsWideOrdinalsAndBackwardSeeks) {
    constexpr uint32_t ROWS = 4096 * 3 + 17;
    Map map;
    ASSERT_TRUE(map.init_source(ROWS).ok());
    std::vector<uint64_t> expected(ROWS, MISSING);
    uint64_t target = (1ULL << 40);
    for (uint32_t row = 0; row < ROWS; ++row) {
        if (row % 9 == 0 || (row >= 4096 && row < 8192)) continue;
        target += row % 41 == 0 ? (1ULL << 35) : row % 13 + 1;
        expected[row] = target;
        ASSERT_TRUE(map.append(0, row, target).ok());
    }
    ASSERT_TRUE(map.finish().ok());
    Map::Cursor cursor;
    Map::DecodeWorkspace workspace;
    check_rows(map, 0, expected, cursor, workspace);
    std::mt19937 random(27);
    for (uint32_t i = 0; i < 2000; ++i) {
        uint32_t row = random() % ROWS;
        uint64_t result;
        ASSERT_TRUE(map.get(0, row, cursor, workspace, &result).ok());
        ASSERT_EQ(result, expected[row]);
    }
    EXPECT_GT(map.memory_usage().payload_bytes, 0);
    EXPECT_GT(cursor.decoded_blocks(), 100);
    EXPECT_EQ(cursor.memory_usage(), Map::BLOCK_ROWS * sizeof(uint64_t));
}

TEST(CompressedRowIdMapTest, InterleavedSourcesKeepIndependentCursorBlocks) {
    constexpr uint32_t SOURCES = 16;
    constexpr uint32_t ROWS = 8201;
    Map map;
    for (uint32_t source = 0; source < SOURCES; ++source) ASSERT_TRUE(map.init_source(ROWS).ok());
    for (uint32_t row = 0; row < ROWS; ++row) {
        for (uint32_t source = 0; source < SOURCES; ++source) {
            ASSERT_TRUE(
                    map.append(source, row, static_cast<uint64_t>(row) * SOURCES + source).ok());
        }
    }
    EXPECT_EQ(map.memory_usage().builder_bytes, SOURCES * 32768);
    ASSERT_TRUE(map.finish().ok());
    std::array<Map::Cursor, SOURCES> cursors;
    Map::DecodeWorkspace workspace;
    for (uint32_t row = 0; row < ROWS; ++row) {
        for (uint32_t source = 0; source < SOURCES; ++source) {
            uint64_t target;
            ASSERT_TRUE(map.get(source, row, cursors[source], workspace, &target).ok());
            ASSERT_EQ(target, static_cast<uint64_t>(row) * SOURCES + source);
        }
    }
    for (const auto& cursor : cursors) EXPECT_EQ(cursor.decoded_blocks(), 3);
    // Reusing a cursor across owners or sources must not return another block's values.
    Map other;
    ASSERT_TRUE(other.init_source(ROWS).ok());
    ASSERT_TRUE(other.append(0, 0, 800).ok());
    ASSERT_TRUE(other.append(0, 1, 900).ok());
    ASSERT_TRUE(other.finish().ok());
    uint64_t target;
    ASSERT_TRUE(other.get(0, 1, cursors[0], workspace, &target).ok());
    EXPECT_EQ(target, 900);
    ASSERT_TRUE(map.get(1, 1, cursors[0], workspace, &target).ok());
    EXPECT_EQ(target, 17);
}

TEST(CompressedRowIdMapTest, RejectsRevisitDuplicateAndReadsBeforeFinish) {
    Map map;
    ASSERT_TRUE(map.init_source(10000).ok());
    ASSERT_TRUE(map.append(0, 3, 7).ok());
    EXPECT_FALSE(map.append(0, 3, 8).ok());
    EXPECT_FALSE(map.append(0, 4, 7).ok());
    ASSERT_TRUE(map.append(0, 8194, 9).ok());
    EXPECT_FALSE(map.append(0, 4000, 10).ok());
    EXPECT_FALSE(map.append(0, 10000, 10).ok());
    EXPECT_FALSE(map.append(0, 9999, MISSING).ok());
    Map::Cursor cursor;
    Map::DecodeWorkspace workspace;
    uint64_t target;
    EXPECT_FALSE(map.get(0, 3, cursor, workspace, &target).ok());
    ASSERT_TRUE(map.finish().ok());
    EXPECT_FALSE(map.append(0, 9999, 10).ok());
    EXPECT_FALSE(map.init_source(1).ok());
    ASSERT_TRUE(map.get(0, 3, cursor, workspace, &target).ok());
    EXPECT_EQ(target, 7);
    ASSERT_TRUE(map.get(0, 8194, cursor, workspace, &target).ok());
    EXPECT_EQ(target, 9);
}

TEST(CompressedRowIdMapTest, LinearBitmapAndRawFallback) {
    Map map;
    ASSERT_TRUE(map.init_source(4096).ok());
    ASSERT_TRUE(map.init_source(16).ok());
    for (uint32_t row = 0; row < 4096; row += 2) ASSERT_TRUE(map.append(0, row, row / 2).ok());
    std::mt19937_64 random(4321);
    std::vector<uint64_t> raw(16);
    for (auto& value : raw) value = random();
    std::sort(raw.begin(), raw.end());
    for (uint32_t row = 0; row < raw.size(); ++row) ASSERT_TRUE(map.append(1, row, raw[row]).ok());
    ASSERT_TRUE(map.finish().ok());
    EXPECT_EQ(map.block_meta(0, 0).kind, Map::Kind::LINEAR);
    EXPECT_NE(map.block_meta(0, 0).flags & Map::flag_bits(Map::Flag::BITMAP), 0);
    EXPECT_NE(map.block_meta(0, 0).flags & Map::flag_bits(Map::Flag::ZSTD), 0);
    EXPECT_EQ(map.block_meta(1, 0).kind, Map::Kind::RAW64);
    Map::Cursor cursor;
    Map::DecodeWorkspace workspace;
    check_rows(map, 1, raw, cursor, workspace);
    for (uint32_t row = 0; row < 4096; ++row) {
        uint64_t target;
        ASSERT_TRUE(map.get(0, row, cursor, workspace, &target).ok());
        ASSERT_EQ(target, row % 2 == 0 ? row / 2 : MISSING);
    }
}

TEST(CompressedRowIdMapTest, CorruptDirectoryBitmapVarintAndZstdReturnErrors) {
    Map map;
    ASSERT_TRUE(map.init_source(4096).ok());
    for (uint32_t row = 0; row < 4096; row += 2) ASSERT_TRUE(map.append(0, row, row * 13).ok());
    ASSERT_TRUE(map.finish().ok());
    auto& meta = const_cast<Map::BlockMeta&>(map.block_meta(0, 0));
    Map::Cursor cursor;
    Map::DecodeWorkspace workspace;
    uint64_t target;
    auto payload_owner = std::move(meta.payload);
    EXPECT_FALSE(map.get(0, 0, cursor, workspace, &target).ok());
    meta.payload = std::move(payload_owner);
    meta.payload_bytes++;
    EXPECT_FALSE(map.get(0, 0, cursor, workspace, &target).ok());
    meta.payload_bytes--;
    meta.rows--;
    EXPECT_FALSE(map.get(0, 0, cursor, workspace, &target).ok());
    meta.rows++;
    meta.plain_bytes++;
    EXPECT_FALSE(map.get(0, 0, cursor, workspace, &target).ok());
    meta.plain_bytes--;
    std::span<const uint8_t> payload;
    ASSERT_TRUE(map.block_payload(meta, &payload).ok());
    ASSERT_NE(meta.flags & Map::flag_bits(Map::Flag::ZSTD), 0);
    const_cast<uint8_t*>(payload.data())[0] ^= 0xff;
    EXPECT_FALSE(map.get(0, 0, cursor, workspace, &target).ok());
    const_cast<uint8_t*>(payload.data())[0] ^= 0xff;
    ASSERT_TRUE(map.get(0, 0, cursor, workspace, &target).ok());
    EXPECT_EQ(target, 0);

    Map::BlockMeta malformed;
    malformed.kind = Map::Kind::DELTA;
    malformed.rows = 2;
    malformed.live = 2;
    malformed.payload_bytes = malformed.plain_bytes = 1;
    std::array<uint8_t, 1> truncated {0x80};
    EXPECT_FALSE(Map::decode(malformed, truncated, cursor, workspace).ok());
    malformed.first = UINT64_MAX - 2;
    std::array<uint8_t, 1> overflow {1};
    EXPECT_FALSE(Map::decode(malformed, overflow, cursor, workspace).ok());
    malformed.first = 0;
    malformed.payload_bytes = malformed.plain_bytes = 10;
    std::array<uint8_t, 10> too_wide {0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 2};
    EXPECT_FALSE(Map::decode(malformed, too_wide, cursor, workspace).ok());
    malformed.kind = Map::Kind::LINEAR;
    malformed.live = 1;
    malformed.flags = Map::flag_bits(Map::Flag::BITMAP);
    malformed.payload_bytes = malformed.plain_bytes = 1;
    std::array<uint8_t, 1> wrong_count {3};
    EXPECT_FALSE(Map::decode(malformed, wrong_count, cursor, workspace).ok());
    std::array<uint8_t, 1> padding {0x81};
    EXPECT_FALSE(Map::decode(malformed, padding, cursor, workspace).ok());
}

TEST(CompressedRowIdMapTest, OwnedPayloadsAndMetadataAccountForWholeMap) {
    Map map;
    constexpr uint32_t SOURCES = 8;
    constexpr uint32_t ROWS = 32768;
    std::mt19937 random(42);
    std::vector<std::vector<uint64_t>> oracle(SOURCES, std::vector<uint64_t>(ROWS));
    for (uint32_t source = 0; source < SOURCES; ++source) {
        ASSERT_TRUE(map.init_source(ROWS).ok());
        uint64_t target = 0;
        for (uint32_t row = 0; row < ROWS; ++row) {
            target += static_cast<uint64_t>(random()) + 1;
            oracle[source][row] = target;
            ASSERT_TRUE(map.append(source, row, target).ok());
        }
    }
    ASSERT_TRUE(map.finish().ok());
    auto usage = map.memory_usage();
    EXPECT_EQ(usage.directory_bytes, 64 * sizeof(Map::BlockMeta));
    EXPECT_GT(usage.payload_bytes, 1U << 20);
    EXPECT_GT(usage.metadata_bytes, 0);
    EXPECT_GE(usage.peak_bytes, usage.allocated_bytes());
    Map::Cursor cursor;
    Map::DecodeWorkspace workspace;
    for (uint32_t source = 0; source < SOURCES; ++source)
        check_rows(map, source, oracle[source], cursor, workspace);
}

TEST(CompressedRowIdMapTest, TinyPayloadUsesOnlyItsOwnAllocation) {
    MemTracker tracker("compressed-rowid-tiny-payload");
    SCOPED_CONSUME_MEM_TRACKER(&tracker);
    {
        Map map;
        ASSERT_TRUE(map.init_source(4).ok());
        ASSERT_TRUE(map.append(0, 0, 100).ok());
        ASSERT_TRUE(map.append(0, 2, 103).ok());
        ASSERT_TRUE(map.append(0, 3, 104).ok());
        ASSERT_TRUE(map.finish().ok());
        thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
        EXPECT_EQ(map.memory_usage().payload_bytes, 3);
        EXPECT_EQ(tracker.consumption(), map.memory_usage().allocated_bytes());
        EXPECT_LT(tracker.consumption(), 4096); // No minimum arena page remains.
        Map::Cursor cursor;
        Map::DecodeWorkspace workspace;
        check_rows(map, 0, {100, MISSING, 103, 104}, cursor, workspace);
    }
    thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
    EXPECT_EQ(tracker.consumption(), 0);
}

TEST(CompressedRowIdMapTest, SourceViewsMatchDenseAndValidateSniiCoverage) {
    RowsetId rowset;
    rowset.init(100);
    RowIdConversion compressed(RowIdConversion::Mode::COMPRESSED);
    RowIdConversion dense;
    const std::vector<uint32_t> ids {7, 42, 999};
    const std::vector<uint32_t> counts {8201, 8201, 0};
    for (auto* map : {&compressed, &dense}) {
        ASSERT_TRUE(map->init_segment_map(rowset, ids, counts).ok());
        ASSERT_TRUE(map->init_segment_map(rowset, ids, counts).ok());
    }
    std::vector<RowLocation> locations;
    for (uint32_t row = 0; row < counts[0]; ++row) {
        if (row % 5 != 0) locations.emplace_back(rowset, 7, row);
        locations.emplace_back(rowset, 42, row);
    }
    const std::vector<uint32_t> destinations {1000, 2000,
                                              static_cast<uint32_t>(locations.size() - 3000)};
    for (size_t begin = 0; begin < locations.size(); begin += 257) {
        std::vector<RowLocation> batch(locations.begin() + begin,
                                       locations.begin() + std::min(locations.size(), begin + 257));
        for (auto* map : {&compressed, &dense}) ASSERT_TRUE(map->add(batch, destinations).ok());
    }
    EXPECT_FALSE(compressed.finalize({1}).ok());
    ASSERT_TRUE(compressed.finalize(destinations).ok());
    Map::Cursor cursor;
    Map::DecodeWorkspace workspace;
    for (size_t source = 0; source < ids.size(); ++source) {
        auto view = compressed.find_source(rowset, ids[source]);
        for (uint32_t row = 0; row < counts[source]; ++row) {
            RowIdConversionSource::Pair result;
            ASSERT_TRUE(view.get(row, cursor, workspace, &result).ok());
            EXPECT_EQ(result, dense.get_rowid_conversion_map()[source][row]);
        }
    }
    std::unique_ptr<snii::compaction::ValidatedRowIdConversion> validated;
    auto status = snii::compaction::ValidatedRowIdConversion::create(&compressed, counts,
                                                                     destinations, &validated);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_TRUE(validated->source_has_deletions(0));
    EXPECT_FALSE(validated->source_has_deletions(1));
    EXPECT_FALSE(snii::compaction::ValidatedRowIdConversion::create(
                         &compressed, counts,
                         std::vector<uint32_t> {static_cast<uint32_t>(locations.size() + 1)},
                         &validated)
                         .ok());
    EXPECT_EQ(validated, nullptr);
}

TEST(CompressedRowIdMapTest, TrackedMemoryIsBalancedAndAllocationFailureIsAStatus) {
    auto tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER,
                                                    "compressed-rowid-test", 32 << 20);
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(tracker);
    const auto consumed = [&] {
        thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
        return tracker->consumption();
    };
    const int64_t baseline = consumed();
    {
        Map map;
        EXPECT_EQ(map.memory_usage().allocated_bytes(), 0);
        EXPECT_EQ(consumed(), baseline);
        ASSERT_TRUE(map.init_source(8192).ok());
        for (uint32_t row = 0; row < 8192; row += 2) ASSERT_TRUE(map.append(0, row, row * 7).ok());
        EXPECT_EQ(consumed() - baseline, map.memory_usage().allocated_bytes());
        ASSERT_TRUE(map.finish().ok());
        EXPECT_EQ(consumed() - baseline, map.memory_usage().allocated_bytes());
        Map::Cursor cursor;
        Map::DecodeWorkspace workspace;
        uint64_t value;
        ASSERT_TRUE(map.get(0, 3, cursor, workspace, &value).ok());
        EXPECT_EQ(consumed() - baseline, map.memory_usage().allocated_bytes() +
                                                 cursor.memory_usage() + workspace.memory_usage());
    }
    EXPECT_EQ(consumed(), baseline);
    {
        Map map;
        auto status = map.init_source(UINT32_MAX); // 40 MiB directory exceeds this tracker limit.
        EXPECT_TRUE(status.is<ErrorCode::MEM_LIMIT_EXCEEDED>()) << status;
    }
    EXPECT_EQ(consumed(), baseline);
}

TEST(CompressedRowIdMapTest, StorageAlternativesReleaseAllTrackedMemory) {
    auto tracker =
            MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER, "rowid-storage-test");
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(tracker);
    const auto consumed = [&] {
        thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
        return tracker->consumption();
    };
    int64_t baseline = consumed();
    RowsetId rowset;
    rowset.init(45);
    using Mode = RowIdConversion::Mode;
    for (auto mode : {Mode::DENSE, Mode::LAZY_CHUNKED, Mode::COMPRESSED}) {
        {
            RowIdConversion map;
            // Compaction selects storage before registration; replaced alternatives must be freed.
            map.set_mode(Mode::COMPRESSED);
            map.set_mode(mode);
            EXPECT_EQ(map.mode(), mode);
            ASSERT_TRUE(map.init_segment_map(rowset, {7, 91}, {8193, 4}).ok());
            ASSERT_TRUE(map.add({RowLocation(rowset, 7, 0), RowLocation(rowset, 91, 2)}, {2}).ok());
            ASSERT_TRUE(map.add({RowLocation(rowset, 7, 8192)}, {2, 1}).ok());
            ASSERT_TRUE(map.finalize({2, 1}).ok());
            ASSERT_TRUE(map.finalize({2, 1}).ok());
            Map::Cursor cursor;
            Map::DecodeWorkspace workspace;
            RowIdConversion::RowIdPair target;
            auto source = map.find_source(rowset, 7);
            EXPECT_EQ(source.is_dense(), mode == Mode::DENSE);
            ASSERT_TRUE(source.get(8192, cursor, workspace, &target).ok());
            EXPECT_EQ(target, (RowIdConversion::RowIdPair {1, 0}));
            ASSERT_TRUE(source.get(1, cursor, workspace, &target).ok());
            EXPECT_EQ(target, (RowIdConversion::RowIdPair {UINT32_MAX, UINT32_MAX}));
            ASSERT_TRUE(source.get(0, cursor, workspace, &target).ok());
            EXPECT_EQ(target, (RowIdConversion::RowIdPair {0, 0}));
            EXPECT_GT(consumed(), baseline);
        }
        EXPECT_EQ(consumed(), baseline);
    }
}

TEST(CompressedRowIdMapTest, EmptyOutputsSentinelAndZeroLengthDestinationSegments) {
    RowsetId rowset;
    rowset.init(123);
    RowIdConversion empty(RowIdConversion::Mode::COMPRESSED);
    ASSERT_TRUE(empty.init_segment_map(rowset, {10}, {3}).ok());
    ASSERT_TRUE(empty.finalize({}).ok());
    Map::Cursor cursor;
    Map::DecodeWorkspace workspace;
    RowIdConversionSource::Pair value;
    ASSERT_TRUE(empty.find_source(rowset, 10).get(1, cursor, workspace, &value).ok());
    EXPECT_EQ(value, (RowIdConversionSource::Pair {UINT32_MAX, UINT32_MAX}));
    RowIdConversion map(RowIdConversion::Mode::COMPRESSED);
    ASSERT_TRUE(map.init_segment_map(rowset, {10}, {4}).ok());
    ASSERT_TRUE(map.add({RowLocation(rowset, 10, UINT32_MAX), RowLocation(rowset, 10, 0),
                         RowLocation(rowset, 10, 2)},
                        {})
                        .ok());
    ASSERT_TRUE(map.finalize({0, 1, 0, 1, 0}).ok());
    ASSERT_TRUE(map.find_source(rowset, 10).get(0, cursor, workspace, &value).ok());
    EXPECT_EQ(value, (RowIdConversionSource::Pair {1, 0}));
    ASSERT_TRUE(map.find_source(rowset, 10).get(2, cursor, workspace, &value).ok());
    EXPECT_EQ(value, (RowIdConversionSource::Pair {3, 0}));
    ASSERT_TRUE(map.find_source(rowset, 42).get(0, cursor, workspace, &value).ok());
    EXPECT_EQ(value, (RowIdConversionSource::Pair {UINT32_MAX, UINT32_MAX}));
}

// Informational timings, not assertions. Run a Release UT build for meaningful
// throughput; ASAN runs are correctness checks. Oracle storage is excluded and
// reported separately. Dense and encoded paths use the same generated input.
TEST(CompressedRowIdMapBenchmark, ReportsWholeMapMemoryAndLookupCosts) {
    using Clock = std::chrono::steady_clock;
    auto ms = [](auto start) {
        return std::chrono::duration<double, std::milli>(Clock::now() - start).count();
    };
    constexpr uint32_t ROWS = 262144;
    for (uint32_t sources : {1U, 16U}) {
        for (uint32_t gap : {1U, 16U}) {
            const uint32_t per_source = ROWS / sources;
            std::mt19937 random(89);
            std::vector<std::vector<uint64_t>> oracle(sources,
                                                      std::vector<uint64_t>(per_source, MISSING));
            for (uint32_t source = 0; source < sources; ++source) {
                uint64_t value = source;
                for (uint32_t row = 0; row < per_source; ++row) {
                    if (random() % 10 == 0) continue;
                    value += (gap == 1 ? 1 : random() % (gap * 2) + 1) * sources;
                    oracle[source][row] = value;
                }
            }
            auto start = Clock::now();
            const auto dense = oracle;
            double dense_build_ms = ms(start);
            uint64_t expected_sum = 0;
            start = Clock::now();
            for (uint32_t row = 0; row < per_source; ++row) {
                for (uint32_t source = 0; source < sources; ++source)
                    expected_sum += dense[source][row];
            }
            double dense_sequential_ms = ms(start);
            // A consumer tracker records every allocation, including transient old/new
            // vector capacities. Limiter trackers batch small consumes and can miss peaks.
            MemTracker tracker("compressed-rowid-benchmark");
            SCOPED_CONSUME_MEM_TRACKER(&tracker);
            Map map;
            start = Clock::now();
            for (uint32_t source = 0; source < sources; ++source)
                ASSERT_TRUE(map.init_source(per_source).ok());
            for (uint32_t row = 0; row < per_source; ++row) {
                for (uint32_t source = 0; source < sources; ++source) {
                    if (oracle[source][row] != MISSING)
                        ASSERT_TRUE(map.append(source, row, oracle[source][row]).ok());
                }
            }
            const auto before_finish = map.memory_usage();
            ASSERT_TRUE(map.finish().ok());
            double build_ms = ms(start);
            thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
            const int64_t tracked_build_peak = tracker.peak_consumption();
            const int64_t tracked_retained = tracker.consumption();
            MemTracker read_tracker("compressed-rowid-read-benchmark");
            SCOPED_CONSUME_MEM_TRACKER(&read_tracker);
            Map::DecodeWorkspace workspace;
            std::vector<Map::Cursor> cursors(sources);
            uint64_t sum = 0;
            start = Clock::now();
            for (uint32_t row = 0; row < per_source; ++row) {
                for (uint32_t source = 0; source < sources; ++source) {
                    uint64_t value;
                    ASSERT_TRUE(map.get(source, row, cursors[source], workspace, &value).ok());
                    ASSERT_EQ(value, oracle[source][row]);
                    sum += value;
                }
            }
            double sequential_ms = ms(start);
            ASSERT_EQ(sum, expected_sum);
            uint64_t sequential_decodes = 0;
            size_t cursor_bytes = 0;
            for (const auto& cursor : cursors) {
                sequential_decodes += cursor.decoded_blocks();
                cursor_bytes += cursor.memory_usage();
            }
            start = Clock::now();
            for (uint32_t source = 0; source < sources; ++source) {
                cursors[source] = Map::Cursor();
                for (uint32_t row = 0; row < per_source; row += Map::BLOCK_ROWS) {
                    uint64_t value;
                    ASSERT_TRUE(map.get(source, row, cursors[source], workspace, &value).ok());
                    sum += value;
                }
            }
            double cold_decode_ms = ms(start);
            start = Clock::now();
            for (uint32_t group = 0; group < 64; ++group) {
                for (uint32_t i = 0; i < 64; ++i) {
                    uint32_t source = group % sources;
                    uint32_t row = i * (per_source / 64);
                    uint64_t value;
                    ASSERT_TRUE(map.get(source, row, cursors[source], workspace, &value).ok());
                    sum += value;
                }
            }
            double sparse_ms = ms(start);
            start = Clock::now();
            for (uint32_t i = 0; i < 4096; ++i) {
                uint32_t source = random() % sources;
                uint32_t row = random() % per_source;
                uint64_t value;
                ASSERT_TRUE(map.get(source, row, cursors[source], workspace, &value).ok());
                sum += value;
            }
            double random_ms = ms(start);
            thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
            const int64_t tracked_total_peak = tracker.peak_consumption();
            const auto memory = map.memory_usage();
            printf("ROWID_BENCH rows=%u sources=%u gap=%u dense_bytes=%zu oracle_bytes=%zu "
                   "directory=%zu payload=%zu metadata=%zu retained=%zu "
                   "owned_peak=%zu cursor_bytes=%zu workspace=%zu build_ms=%.3f sequential_ms=%.3f "
                   "sequential_decodes=%lu sparse_4096_ms=%.3f random_4096_ms=%.3f checksum=%lu "
                   "cold_decode_ms=%.3f dense_build_ms=%.3f dense_sequential_ms=%.3f "
                   "tracked_build_peak=%ld tracked_retained=%ld tracked_read_peak=%ld "
                   "tracked_total_peak=%ld "
                   "builders_before_finish=%zu encoder_before_finish=%zu\n",
                   ROWS, sources, gap, ROWS * sizeof(uint64_t), ROWS * sizeof(uint64_t),
                   memory.directory_bytes, memory.payload_bytes, memory.metadata_bytes,
                   memory.allocated_bytes(), memory.peak_bytes, cursor_bytes,
                   workspace.memory_usage(), build_ms, sequential_ms, sequential_decodes, sparse_ms,
                   random_ms, sum, cold_decode_ms, dense_build_ms, dense_sequential_ms,
                   tracked_build_peak, tracked_retained,
                   tracked_retained + read_tracker.peak_consumption(), tracked_total_peak,
                   before_finish.builder_bytes, before_finish.encoder_bytes);
        }
    }
}

TEST(CompressedRowIdMapBenchmark, ReportsLargeJointMappingMemoryAndLookupCosts) {
    using Clock = std::chrono::steady_clock;
    auto ms = [](auto start) {
        return std::chrono::duration<double, std::milli>(Clock::now() - start).count();
    };
    constexpr uint32_t SOURCES = 16;
    constexpr uint32_t PER_SOURCE = 1 << 20;
    constexpr uint32_t ROWS = SOURCES * PER_SOURCE;
    MemTracker tracker("compressed-rowid-large-benchmark");
    SCOPED_CONSUME_MEM_TRACKER(&tracker);
    Map map;
    auto start = Clock::now();
    for (uint32_t source = 0; source < SOURCES; ++source)
        ASSERT_TRUE(map.init_source(PER_SOURCE).ok());
    // Joint output ordinals cover [0, output_rows) exactly. Replay the seed when
    // reading, so no dense oracle is needed for this 128 MiB-equivalent mapping.
    std::mt19937 random(89);
    uint64_t output_rows = 0;
    for (uint32_t row = 0; row < PER_SOURCE; ++row) {
        for (uint32_t source = 0; source < SOURCES; ++source) {
            if (random() % 10 != 0) ASSERT_TRUE(map.append(source, row, output_rows++).ok());
        }
    }
    const auto before_finish = map.memory_usage();
    ASSERT_TRUE(map.finish().ok());
    const double build_ms = ms(start);
    thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
    const int64_t tracked_build_peak = tracker.peak_consumption();
    const int64_t tracked_retained = tracker.consumption();
    MemTracker read_tracker("compressed-rowid-large-read-benchmark");
    SCOPED_CONSUME_MEM_TRACKER(&read_tracker);

    Map::DecodeWorkspace workspace;
    std::vector<Map::Cursor> cursors(SOURCES);
    random.seed(89);
    uint64_t expected = 0;
    uint64_t checksum = 0;
    start = Clock::now();
    for (uint32_t row = 0; row < PER_SOURCE; ++row) {
        for (uint32_t source = 0; source < SOURCES; ++source) {
            const uint64_t target = random() % 10 != 0 ? expected++ : MISSING;
            uint64_t value;
            ASSERT_TRUE(map.get(source, row, cursors[source], workspace, &value).ok());
            ASSERT_EQ(value, target);
            if (value != MISSING) checksum += value;
        }
    }
    const double sequential_ms = ms(start);
    ASSERT_EQ(expected, output_rows);
    EXPECT_EQ(checksum, output_rows * (output_rows - 1) / 2);
    uint64_t sequential_decodes = 0;
    size_t cursor_bytes = 0;
    for (const auto& cursor : cursors) {
        sequential_decodes += cursor.decoded_blocks();
        cursor_bytes += cursor.memory_usage();
    }
    EXPECT_EQ(sequential_decodes, ROWS / Map::BLOCK_ROWS);
    thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
    const auto memory = map.memory_usage();
    EXPECT_EQ(tracker.consumption(),
              memory.allocated_bytes() + cursor_bytes + workspace.memory_usage());
    printf("ROWID_LARGE_BENCH rows=%u sources=%u output_rows=%lu dense_bytes=%zu "
           "directory=%zu payload=%zu metadata=%zu retained=%zu owned_peak=%zu "
           "builders_before_finish=%zu encoder_before_finish=%zu cursor_bytes=%zu workspace=%zu "
           "tracked_build_peak=%ld tracked_retained=%ld tracked_read_peak=%ld "
           "tracked_total_peak=%ld "
           "build_ms=%.3f sequential_ms=%.3f sequential_decodes=%lu checksum=%lu\n",
           ROWS, SOURCES, output_rows, ROWS * sizeof(uint64_t), memory.directory_bytes,
           memory.payload_bytes, memory.metadata_bytes, memory.allocated_bytes(), memory.peak_bytes,
           before_finish.builder_bytes, before_finish.encoder_bytes, cursor_bytes,
           workspace.memory_usage(), tracked_build_peak, tracked_retained,
           tracked_retained + read_tracker.peak_consumption(), tracker.peak_consumption(), build_ms,
           sequential_ms, sequential_decodes, checksum);
}

} // namespace
} // namespace doris
