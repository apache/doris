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

#include "storage/segment/condition_cache.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "format/generic_reader.h"
#include "format/orc/vorc_reader.h"
#include "format/parquet/vparquet_reader.h"
#include "format/table/transactional_hive_common.h"

namespace doris::vectorized {

constexpr int GS = ConditionCacheContext::GRANULE_SIZE; // 2048

class FilterRangesByCacheTest : public testing::Test {};

// Single contiguous range, first_row = 0, alternating true/false granules.
TEST_F(FilterRangesByCacheTest, SingleRangeAlternatingGranules) {
    // 4 full granules = 8192 rows, range [0, 8192)
    RowRanges ranges;
    ranges.add(RowRange(0, 4 * GS));
    // granule 0=true, 1=false, 2=true, 3=false
    std::vector<bool> cache = {true, false, true, false};

    auto result = RowGroupReader::filter_ranges_by_cache(ranges, cache, /*first_row=*/0);

    // Expect granules 0 and 2 kept: [0, 2048), [4096, 6144)
    EXPECT_EQ(result.range_size(), 2);
    EXPECT_EQ(result.count(), 2 * GS);
    EXPECT_EQ(result.get_range_from(0), 0);
    EXPECT_EQ(result.get_range_to(0), GS);
    EXPECT_EQ(result.get_range_from(1), 2 * GS);
    EXPECT_EQ(result.get_range_to(1), 3 * GS);
}

// All granules true -> ranges unchanged.
TEST_F(FilterRangesByCacheTest, AllGranulesTrue) {
    RowRanges ranges;
    ranges.add(RowRange(0, 3 * GS));
    std::vector<bool> cache = {true, true, true};

    auto result = RowGroupReader::filter_ranges_by_cache(ranges, cache, 0);

    EXPECT_EQ(result.range_size(), 1);
    EXPECT_EQ(result.count(), 3 * GS);
    EXPECT_EQ(result.get_range_from(0), 0);
    EXPECT_EQ(result.get_range_to(0), 3 * GS);
}

// All granules false -> empty ranges.
TEST_F(FilterRangesByCacheTest, AllGranulesFalse) {
    RowRanges ranges;
    ranges.add(RowRange(0, 3 * GS));
    std::vector<bool> cache = {false, false, false};

    auto result = RowGroupReader::filter_ranges_by_cache(ranges, cache, 0);

    EXPECT_EQ(result.range_size(), 0);
    EXPECT_EQ(result.count(), 0);
}

// first_row offset shifts granule boundaries.
TEST_F(FilterRangesByCacheTest, NonZeroFirstRow) {
    // first_row = 1024, range [0, 4096) -> 4096 rows
    // Sequential positions 0..4095, global_seq = 1024..5119
    // granule 0 (global 0..2047): seq 0..1023 -> range [0, 1024)
    // granule 1 (global 2048..4095): seq 1024..3071 -> range [1024, 3072)
    // granule 2 (global 4096..6143): seq 3072..4095 -> range [3072, 4096)
    RowRanges ranges;
    ranges.add(RowRange(0, 4096));
    // granule 0=false, 1=true, 2=false
    std::vector<bool> cache = {false, true, false};

    auto result = RowGroupReader::filter_ranges_by_cache(ranges, cache, /*first_row=*/1024);

    // Only granule 1 kept: rows with global_seq in [2048, 4096) -> range [1024, 3072)
    EXPECT_EQ(result.range_size(), 1);
    EXPECT_EQ(result.count(), 2048);
    EXPECT_EQ(result.get_range_from(0), 1024);
    EXPECT_EQ(result.get_range_to(0), 3072);
}

// Range that doesn't start at 0 (from page index filtering).
TEST_F(FilterRangesByCacheTest, RangeNotStartingAtZero) {
    // Range [2048, 6144) = 4096 rows, first_row = 0
    // Granule 0 (false): covers rg-relative [0, 2048) — no overlap with [2048, 6144)
    // Granule 1 (true): covers rg-relative [2048, 4096) — kept
    // Beyond cache: [4096, 6144) kept conservatively
    RowRanges ranges;
    ranges.add(RowRange(2048, 6144));
    std::vector<bool> cache = {false, true};

    auto result = RowGroupReader::filter_ranges_by_cache(ranges, cache, 0);

    // False granule [0, 2048) doesn't overlap [2048, 6144), so nothing is filtered
    EXPECT_EQ(result.range_size(), 1);
    EXPECT_EQ(result.count(), 4096);
    EXPECT_EQ(result.get_range_from(0), 2048);
    EXPECT_EQ(result.get_range_to(0), 6144);
}

// Multiple non-contiguous ranges (from page index filtering) with a single-entry cache.
TEST_F(FilterRangesByCacheTest, NonContiguousRangesGranuleSpansGap) {
    // Ranges: [0, 1000), [5000, 6000) = 2000 total rows, first_row = 0
    // Granule 0 covers rg-relative [0, 2048) — only overlaps [0, 1000)
    // [5000, 6000) is in granule 2 ([4096, 6144)) which is beyond cache -> kept conservatively
    RowRanges ranges;
    ranges.add(RowRange(0, 1000));
    ranges.add(RowRange(5000, 6000));

    // Granule 0 = false -> discard [0, 1000); [5000, 6000) kept (beyond cache)
    std::vector<bool> cache = {false};
    auto result = RowGroupReader::filter_ranges_by_cache(ranges, cache, 0);
    EXPECT_EQ(result.count(), 1000);
    EXPECT_EQ(result.range_size(), 1);
    EXPECT_EQ(result.get_range_from(0), 5000);
    EXPECT_EQ(result.get_range_to(0), 6000);

    // Granule 0 = true -> keep all
    std::vector<bool> cache2 = {true};
    result = RowGroupReader::filter_ranges_by_cache(ranges, cache2, 0);
    EXPECT_EQ(result.count(), 2000);
    EXPECT_EQ(result.range_size(), 2);
}

// Non-contiguous ranges where granule boundaries fall within ranges.
TEST_F(FilterRangesByCacheTest, NonContiguousRangesMultipleGranules) {
    // Ranges: [0, 3000), [8000, 11000) = 6000 total rows, first_row = 0
    // Granule 0 (false): rg-relative [0, 2048) — overlaps [0, 2048) of first range
    // Granule 1 (true):  rg-relative [2048, 4096) — overlaps [2048, 3000) of first range
    // Granule 2 (false): rg-relative [4096, 6144) — no overlap with either range
    // [8000, 11000) is in granules 3-5, all beyond cache -> kept conservatively
    RowRanges ranges;
    ranges.add(RowRange(0, 3000));
    ranges.add(RowRange(8000, 11000));

    // granule 0=false, 1=true, 2=false
    std::vector<bool> cache = {false, true, false};
    auto result = RowGroupReader::filter_ranges_by_cache(ranges, cache, 0);

    // Granule 0 removes [0, 2048) from [0, 3000) -> [2048, 3000) kept
    // Granule 2 [4096, 6144) doesn't overlap [8000, 11000) -> [8000, 11000) kept
    EXPECT_EQ(result.range_size(), 2);
    EXPECT_EQ(result.get_range_from(0), 2048);
    EXPECT_EQ(result.get_range_to(0), 3000);
    EXPECT_EQ(result.get_range_from(1), 8000);
    EXPECT_EQ(result.get_range_to(1), 11000);
    EXPECT_EQ(result.count(), (3000 - 2048) + (11000 - 8000)); // 952 + 3000 = 3952
}

// Cache smaller than the actual row range -> out-of-range granules kept conservatively.
TEST_F(FilterRangesByCacheTest, CacheSmallerThanRange) {
    // 4 granules of rows, cache only covers 2
    RowRanges ranges;
    ranges.add(RowRange(0, 4 * GS));
    std::vector<bool> cache = {false, true}; // only 2 entries

    auto result = RowGroupReader::filter_ranges_by_cache(ranges, cache, 0);

    // Granule 0 = false -> skip; granule 1 = true -> keep
    // Granule 2, 3 beyond cache -> kept conservatively
    EXPECT_EQ(result.range_size(), 1); // [GS, 4*GS) merged since granules 1,2,3 all kept
    EXPECT_EQ(result.count(), 3 * GS);
    EXPECT_EQ(result.get_range_from(0), GS);
    EXPECT_EQ(result.get_range_to(0), 4 * GS);
}

// Partial granule at the end of a range.
TEST_F(FilterRangesByCacheTest, PartialGranuleAtEnd) {
    // Range [0, 3000) = 3000 rows, first_row = 0
    // Granule 0: seq [0, 2048) -> [0, 2048)
    // Granule 1: seq [2048, 3000) -> [2048, 3000) (partial, only 952 rows)
    RowRanges ranges;
    ranges.add(RowRange(0, 3000));
    std::vector<bool> cache = {true, false};

    auto result = RowGroupReader::filter_ranges_by_cache(ranges, cache, 0);

    // Only granule 0 kept
    EXPECT_EQ(result.range_size(), 1);
    EXPECT_EQ(result.count(), GS);
    EXPECT_EQ(result.get_range_from(0), 0);
    EXPECT_EQ(result.get_range_to(0), GS);
}

// Empty ranges input.
TEST_F(FilterRangesByCacheTest, EmptyRanges) {
    RowRanges ranges;
    std::vector<bool> cache = {true, false, true};

    auto result = RowGroupReader::filter_ranges_by_cache(ranges, cache, 0);

    EXPECT_EQ(result.range_size(), 0);
    EXPECT_EQ(result.count(), 0);
}

// Large first_row offset (simulating second row group in file).
TEST_F(FilterRangesByCacheTest, LargeFirstRowOffset) {
    // first_row = 100000 (second row group starts here)
    // Range [0, 2048) = one full granule
    // global_seq = 100000 + 0 = 100000, granule = 100000/2048 = 48
    int64_t first_row = 100000;
    RowRanges ranges;
    ranges.add(RowRange(0, GS));
    std::vector<bool> cache(50, false); // 50 granules, all false
    cache[48] = true;                   // granule 48 = true

    auto result = RowGroupReader::filter_ranges_by_cache(ranges, cache, first_row);

    // global_seq for first chunk: 100000, granule 48 = true -> keep
    // But first chunk may not be the full range if 100000 is not aligned...
    // 100000 / 2048 = 48, 49*2048 = 100352, so rows_to_granule_end = 100352 - 100000 = 352
    // chunk 1: [0, 352) -> granule 48 = true -> keep
    // chunk 2: [352, 2048) -> global_seq = 100352 -> granule 49 = false -> discard
    EXPECT_EQ(result.count(), 352);
    EXPECT_EQ(result.range_size(), 1);
    EXPECT_EQ(result.get_range_from(0), 0);
    EXPECT_EQ(result.get_range_to(0), 352);
}

// ============================================================
// Tests for cache vector pre-allocation with +1 safety margin
// when first_row is not aligned to granule boundary.
// ============================================================

class CachePreAllocTest : public testing::Test {};

// When first_row is not aligned to granule boundary, pre-allocating
// ceil(total_rows / GS) + 1 guarantees coverage of all granules.
TEST_F(CachePreAllocTest, PlusOneCoversUnalignedFirstRow) {
    // Simulates: Scanner reads RG2+RG3 of a file:
    //   RG2: 5000 rows (row 20000~24999), RG3: 5000 rows (row 25000~29999)
    //   first_assigned_row = 20000, total_rows = 10000
    //
    // Pre-allocation: ceil(10000 / 2048) + 1 = 5 + 1 = 6
    // base_granule = 20000 / 2048 = 9
    // last_granule = ceil(30000 / 2048) = 15
    // needed = 15 - 9 = 6  <=  6 (pre-allocated)  → sufficient!
    constexpr int64_t GS = ConditionCacheContext::GRANULE_SIZE; // 2048
    int64_t first_assigned_row = 20000;
    int64_t total_rows = 10000;

    // Step 1: simulate pre-allocation with +1 (as FileScanner now does)
    int64_t pre_alloc = (total_rows + GS - 1) / GS + 1; // ceil(10000/2048) + 1 = 6
    EXPECT_EQ(pre_alloc, 6);
    std::vector<bool> cache(pre_alloc, false);

    // Step 2: compute base_granule (as set_condition_cache_context does)
    int64_t base_granule = first_assigned_row / GS; // 9
    EXPECT_EQ(base_granule, 9);

    // Step 3: verify all granules are coverable
    int64_t last_granule = (first_assigned_row + total_rows + GS - 1) / GS; // 15
    int64_t needed = last_granule - base_granule;                           // 6
    EXPECT_LE(static_cast<size_t>(needed), cache.size());

    // Step 4: mark all granules as true and verify HIT keeps all rows
    cache.assign(cache.size(), true);
    RowRanges ranges;
    ranges.add(RowRange(0, total_rows));
    auto result =
            RowGroupReader::filter_ranges_by_cache(ranges, cache, first_assigned_row, base_granule);
    EXPECT_EQ(result.count(), total_rows);
}

// Verify the last granule (cache_idx = needed-1) is reachable after +1 allocation.
TEST_F(CachePreAllocTest, LastGranuleIsReachable) {
    constexpr int64_t GS = ConditionCacheContext::GRANULE_SIZE;
    int64_t first_assigned_row = 20000;
    int64_t total_rows = 10000;

    int64_t pre_alloc = (total_rows + GS - 1) / GS + 1;
    std::vector<bool> cache(pre_alloc, false);
    int64_t base_granule = first_assigned_row / GS;

    // Simulate marking from _mark_condition_cache_granules for a row
    // in the last granule. E.g., global row 29000
    int64_t global_row = 29000;
    size_t granule = global_row / GS;          // 29000 / 2048 = 14
    size_t cache_idx = granule - base_granule; // 14 - 9 = 5

    // Without +1, cache.size() would be 5 and cache_idx=5 would be out of bounds
    // With +1, cache.size() is 6 and cache_idx=5 is valid
    EXPECT_LT(cache_idx, cache.size());
    cache[cache_idx] = true;
    EXPECT_TRUE(cache[cache_idx]);
}

// Verify +1 is sufficient for various misaligned first_row values.
TEST_F(CachePreAllocTest, PlusOneSufficientForVariousMisalignments) {
    constexpr int64_t GS = ConditionCacheContext::GRANULE_SIZE;

    struct TestCase {
        int64_t first_row;
        int64_t total_rows;
    };
    std::vector<TestCase> cases = {
            {.first_row = 20000, .total_rows = 10000},  // original example
            {.first_row = 1024, .total_rows = 4096},    // small offset
            {.first_row = 3000, .total_rows = 8000},    // another misalignment
            {.first_row = 2047, .total_rows = 2049},    // just before boundary, spans 3 granules
            {.first_row = 0, .total_rows = 10000},      // aligned (+1 is wasted but harmless)
            {.first_row = 1, .total_rows = 2048},       // off-by-one at start
            {.first_row = 100000, .total_rows = 10000}, // large offset (second row group)
    };

    for (auto& tc : cases) {
        int64_t last_row = tc.first_row + tc.total_rows;
        int64_t pre_alloc = (tc.total_rows + GS - 1) / GS + 1;
        int64_t base_granule = tc.first_row / GS;
        int64_t last_granule = (last_row + GS - 1) / GS;
        int64_t needed = last_granule - base_granule;

        EXPECT_LE(needed, pre_alloc)
                << "first_row=" << tc.first_row << " total_rows=" << tc.total_rows;

        // Verify HIT correctness
        std::vector<bool> cache(pre_alloc, true);
        RowRanges ranges;
        ranges.add(RowRange(0, tc.total_rows));
        auto result =
                RowGroupReader::filter_ranges_by_cache(ranges, cache, tc.first_row, base_granule);
        EXPECT_EQ(result.count(), tc.total_rows)
                << "first_row=" << tc.first_row << " total_rows=" << tc.total_rows;
    }
}

// Extra +1 element beyond actual data range doesn't cause incorrect filtering.
TEST_F(CachePreAllocTest, ExtraElementDoesNotCauseIncorrectFiltering) {
    // Aligned case: first_row=0, total_rows=4096 (exactly 2 granules)
    // Pre-alloc = 2 + 1 = 3. The 3rd element (cache[2]) is beyond data range.
    std::vector<bool> cache = {true, true, false}; // extra false at end

    RowRanges ranges;
    ranges.add(RowRange(0, 4096));
    auto result = RowGroupReader::filter_ranges_by_cache(ranges, cache, 0, 0);

    // The extra false granule covers rg-relative [4096, 6144) which doesn't
    // overlap [0, 4096), so all rows should be kept.
    EXPECT_EQ(result.count(), 4096);
}

// ============================================================
// Mock / Testable reader classes
// ============================================================

// GenericReader whose has_delete_operations() result is configurable,
// used to test condition cache skip logic for various delete scenarios.
class MockFileFormatReader : public GenericReader {
public:
    bool mock_has_deletes = false;
    Status _do_get_next_block(Block*, size_t*, bool*) override { return Status::OK(); }
    bool has_delete_operations() const override { return mock_has_deletes; }
};
// ============================================================
// These tests reproduce the logic from
// FileScanner::_init_reader_condition_cache() (file_scanner.cpp)
// using real ConditionCache + real reader instances.
// ============================================================

class ConditionCacheDeleteOpsTest : public testing::Test {
protected:
    void SetUp() override {
        _cache.reset(segment_v2::ConditionCache::create_global_cache(10 * 1024 * 1024, 4));
    }

    void TearDown() override { _cache.reset(); }

    // Reproduces the exact logic from FileScanner::_init_reader_condition_cache().
    // Returns whether the condition cache context was created (i.e. cache was not skipped).
    void simulate_init_condition_cache(GenericReader* reader, uint64_t digest,
                                       const std::string& path,
                                       /*out*/ bool& cache_hit,
                                       /*out*/ std::shared_ptr<std::vector<bool>>& cache,
                                       /*out*/ std::shared_ptr<ConditionCacheContext>& ctx) {
        cache_hit = false;
        cache = nullptr;
        ctx = nullptr;

        // Mirrors: if (_condition_cache_digest == 0 || _is_load) return;
        if (digest == 0) {
            return;
        }

        // Mirrors: if (_cur_reader && _cur_reader->has_delete_operations()) return;
        if (reader && reader->has_delete_operations()) {
            return;
        }

        auto* cc = _cache.get();
        if (cc == nullptr) {
            return;
        }

        segment_v2::ConditionCache::ExternalCacheKey key(path, -1, 0, digest, 0, -1);

        segment_v2::ConditionCacheHandle handle;
        cache_hit = cc->lookup(key, &handle);
        if (cache_hit) {
            cache = handle.get_filter_result();
        } else {
            cache = std::make_shared<std::vector<bool>>();
        }

        ctx = std::make_shared<ConditionCacheContext>();
        ctx->is_hit = cache_hit;
        ctx->filter_result = cache;
    }

    // Inserts a pre-populated entry into the cache for the given path/digest.
    void prepopulate_cache(const std::string& path, uint64_t digest) {
        segment_v2::ConditionCache::ExternalCacheKey key(path, -1, 0, digest, 0, -1);
        auto filter = std::make_shared<std::vector<bool>>(std::vector<bool> {true, false, true});
        _cache->insert(key, filter);
    }

    std::unique_ptr<segment_v2::ConditionCache> _cache;
};

// -- ParquetReader: no deletes -> cache populated (MISS) --
TEST_F(ConditionCacheDeleteOpsTest, ParquetNoDeletes_CachePopulated) {
    TFileScanRangeParams params;
    TFileRangeDesc range;
    auto reader = ParquetReader::create_unique(params, range, nullptr, nullptr);

    bool hit = false;
    std::shared_ptr<std::vector<bool>> cache;
    std::shared_ptr<ConditionCacheContext> ctx;
    simulate_init_condition_cache(reader.get(), 42, "/data/file.parquet", hit, cache, ctx);

    EXPECT_FALSE(hit);
    EXPECT_NE(ctx, nullptr);
    EXPECT_NE(cache, nullptr);
    EXPECT_FALSE(ctx->is_hit);
}

// -- ParquetReader: with position deletes -> cache skipped --
TEST_F(ConditionCacheDeleteOpsTest, ParquetWithPositionDeletes_CacheSkipped) {
    TFileScanRangeParams params;
    TFileRangeDesc range;
    auto reader = ParquetReader::create_unique(params, range, nullptr, nullptr);
    std::vector<int64_t> deletes = {1, 5, 10};
    reader->set_delete_rows(&deletes);

    bool hit = false;
    std::shared_ptr<std::vector<bool>> cache;
    std::shared_ptr<ConditionCacheContext> ctx;
    simulate_init_condition_cache(reader.get(), 42, "/data/file.parquet", hit, cache, ctx);

    EXPECT_EQ(ctx, nullptr);
    EXPECT_EQ(cache, nullptr);
}

// -- OrcReader: no deletes -> cache populated (MISS) --
TEST_F(ConditionCacheDeleteOpsTest, OrcNoDeletes_CachePopulated) {
    TFileScanRangeParams params;
    TFileRangeDesc range;
    auto reader = OrcReader::create_unique(params, range, 4064, "", nullptr);

    bool hit = false;
    std::shared_ptr<std::vector<bool>> cache;
    std::shared_ptr<ConditionCacheContext> ctx;
    simulate_init_condition_cache(reader.get(), 99, "/data/file.orc", hit, cache, ctx);

    EXPECT_FALSE(hit);
    EXPECT_NE(ctx, nullptr);
    EXPECT_NE(cache, nullptr);
    EXPECT_FALSE(ctx->is_hit);
}

// -- OrcReader: with position deletes -> cache skipped --
TEST_F(ConditionCacheDeleteOpsTest, OrcWithPositionDeletes_CacheSkipped) {
    TFileScanRangeParams params;
    TFileRangeDesc range;
    auto reader = OrcReader::create_unique(params, range, 4064, "", nullptr);
    std::vector<int64_t> pos_deletes = {0, 3, 7};
    reader->set_position_delete_rowids(&pos_deletes);

    bool hit = false;
    std::shared_ptr<std::vector<bool>> cache;
    std::shared_ptr<ConditionCacheContext> ctx;
    simulate_init_condition_cache(reader.get(), 99, "/data/file.orc", hit, cache, ctx);

    EXPECT_EQ(ctx, nullptr);
    EXPECT_EQ(cache, nullptr);
}

// -- OrcReader: with ACID deletes -> cache skipped --
TEST_F(ConditionCacheDeleteOpsTest, OrcWithAcidDeletes_CacheSkipped) {
    TFileScanRangeParams params;
    TFileRangeDesc range;
    auto reader = OrcReader::create_unique(params, range, 4064, "", nullptr);
    AcidRowIDSet acid_deletes;
    acid_deletes.insert({1, 0, 5});
    reader->set_delete_rows(&acid_deletes);

    bool hit = false;
    std::shared_ptr<std::vector<bool>> cache;
    std::shared_ptr<ConditionCacheContext> ctx;
    simulate_init_condition_cache(reader.get(), 99, "/data/file.orc", hit, cache, ctx);

    EXPECT_EQ(ctx, nullptr);
    EXPECT_EQ(cache, nullptr);
}

// -- MockReader: with deletes (simulating Iceberg/Hive with inner deletes) -> cache skipped --
// In the new architecture, Iceberg readers inherit ParquetReader/OrcReader directly (CRTP),
// so has_delete_operations() is resolved through the base reader. We use MockFileFormatReader
// to test the generic condition cache skip logic.
TEST_F(ConditionCacheDeleteOpsTest, ReaderWithDeletes_CacheSkipped) {
    auto reader = std::make_unique<MockFileFormatReader>();
    reader->mock_has_deletes = true;

    bool hit = false;
    std::shared_ptr<std::vector<bool>> cache;
    std::shared_ptr<ConditionCacheContext> ctx;
    simulate_init_condition_cache(reader.get(), 42, "/data/iceberg.parquet", hit, cache, ctx);

    EXPECT_EQ(ctx, nullptr);
    EXPECT_EQ(cache, nullptr);
}

// -- MockReader: no deletes -> cache populated --
TEST_F(ConditionCacheDeleteOpsTest, ReaderWithoutDeletes_CachePopulated) {
    auto reader = std::make_unique<MockFileFormatReader>();
    reader->mock_has_deletes = false;

    bool hit = false;
    std::shared_ptr<std::vector<bool>> cache;
    std::shared_ptr<ConditionCacheContext> ctx;
    simulate_init_condition_cache(reader.get(), 42, "/data/iceberg.parquet", hit, cache, ctx);

    EXPECT_FALSE(hit);
    EXPECT_NE(ctx, nullptr);
    EXPECT_NE(cache, nullptr);
    EXPECT_FALSE(ctx->is_hit);
}

// -- Pre-populated cache entry is NOT returned when deletes exist --
TEST_F(ConditionCacheDeleteOpsTest, CacheHitSkippedWhenDeletesExist) {
    const std::string path = "/data/cached_file.parquet";
    const uint64_t digest = 123;

    // Insert a cache entry
    prepopulate_cache(path, digest);

    // Verify it would be a hit without deletes
    {
        TFileScanRangeParams params;
        TFileRangeDesc range;
        auto reader = ParquetReader::create_unique(params, range, nullptr, nullptr);

        bool hit = false;
        std::shared_ptr<std::vector<bool>> cache;
        std::shared_ptr<ConditionCacheContext> ctx;
        simulate_init_condition_cache(reader.get(), digest, path, hit, cache, ctx);

        EXPECT_TRUE(hit);
        EXPECT_NE(ctx, nullptr);
        EXPECT_TRUE(ctx->is_hit);
        EXPECT_NE(cache, nullptr);
        EXPECT_EQ(cache->size(), 3);
    }

    // Now with deletes: cache entry should NOT be returned
    {
        TFileScanRangeParams params;
        TFileRangeDesc range;
        auto reader = ParquetReader::create_unique(params, range, nullptr, nullptr);
        std::vector<int64_t> deletes = {1, 2, 3};
        reader->set_delete_rows(&deletes);

        bool hit = false;
        std::shared_ptr<std::vector<bool>> cache;
        std::shared_ptr<ConditionCacheContext> ctx;
        simulate_init_condition_cache(reader.get(), digest, path, hit, cache, ctx);

        EXPECT_EQ(ctx, nullptr);
        EXPECT_EQ(cache, nullptr);
        EXPECT_FALSE(hit);
    }
}

// -- Zero digest always skips cache, even without deletes --
TEST_F(ConditionCacheDeleteOpsTest, ZeroDigest_CacheAlwaysSkipped) {
    TFileScanRangeParams params;
    TFileRangeDesc range;
    auto reader = ParquetReader::create_unique(params, range, nullptr, nullptr);

    bool hit = false;
    std::shared_ptr<std::vector<bool>> cache;
    std::shared_ptr<ConditionCacheContext> ctx;
    simulate_init_condition_cache(reader.get(), 0, "/data/file.parquet", hit, cache, ctx);

    EXPECT_EQ(ctx, nullptr);
    EXPECT_EQ(cache, nullptr);
    EXPECT_FALSE(hit);
}

} // namespace doris::vectorized
