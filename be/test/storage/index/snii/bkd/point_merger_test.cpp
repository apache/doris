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

#include "storage/index/snii/bkd/point_merger.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "storage/index/snii/bkd/point_run.h"
#include "storage/index/snii/bkd/point_sorter.h"
#include "storage/index/snii/writer/temp_dir.h"

namespace doris::snii::bkd {
namespace {

constexpr uint32_t kRecordSize = 12; // 8 value bytes + 4 doc id bytes

class Rng {
public:
    explicit Rng(uint64_t seed) : state_(seed) {}
    uint64_t next() {
        state_ = state_ * 6364136223846793005ULL + 1442695040888963407ULL;
        return state_ >> 11;
    }

private:
    uint64_t state_;
};

// A set of run files that all remove themselves together.
class ScopedRuns {
public:
    explicit ScopedRuns(const char* tag) : tag_(tag) {}
    ~ScopedRuns() {
        for (const std::string& path : paths_) {
            ::unlink(path.c_str());
        }
    }

    ScopedRuns(const ScopedRuns&) = delete;
    ScopedRuns& operator=(const ScopedRuns&) = delete;

    // Writes `records` (already sorted) as one more run and returns its path.
    Status add(const std::vector<uint8_t>& records) {
        const std::string path = writer::resolve_temp_dir() + "/bkd_point_merger_test_" + tag_ +
                                 "_" + std::to_string(::getpid()) + "_" +
                                 std::to_string(paths_.size()) + ".run";
        paths_.push_back(path);
        PointRunWriter run;
        RETURN_IF_ERROR(run.open(path));
        if (!records.empty()) {
            RETURN_IF_ERROR(run.append(Slice(records)));
        }
        return run.close();
    }

    const std::vector<std::string>& paths() const { return paths_; }

private:
    std::string tag_;
    std::vector<std::string> paths_;
};

std::vector<uint8_t> random_records(Rng* rng, uint32_t count) {
    std::vector<uint8_t> records(static_cast<size_t>(count) * kRecordSize);
    for (auto& byte : records) {
        byte = static_cast<uint8_t>(rng->next() & 0xFF);
    }
    return records;
}

std::vector<uint8_t> sorted_copy(std::vector<uint8_t> records) {
    point_sorter::sort(records.data(), records.size() / kRecordSize, kRecordSize);
    return records;
}

// Drains a source through next_block, concatenating the blocks. `max_points` is
// what the leaf-cutting loop would pass.
Status drain(PointSource* source, uint32_t max_points, std::vector<uint8_t>* out,
             std::vector<size_t>* block_sizes = nullptr) {
    while (true) {
        Slice block;
        RETURN_IF_ERROR(source->next_block(max_points, &block));
        if (block.size() == 0) {
            return Status::OK();
        }
        if (block_sizes != nullptr) {
            block_sizes->push_back(block.size() / kRecordSize);
        }
        out->insert(out->end(), block.data(), block.data() + block.size());
    }
}

} // namespace

// The whole point of the merge: k sorted runs in, one sorted stream out, holding
// exactly the same multiset of records. Sorting the concatenation independently
// is the oracle -- it never touches the merge's own comparison.
TEST(BkdPointMergerTest, MergesSortedRunsIntoOneSortedStream) {
    for (const uint32_t run_count : {1U, 2U, 3U, 8U, 17U}) {
        SCOPED_TRACE("run_count " + std::to_string(run_count));
        Rng rng(0x243F6A8885A308D3ULL ^ run_count);
        ScopedRuns runs("sorted");
        std::vector<uint8_t> all;

        for (uint32_t i = 0; i < run_count; ++i) {
            // Uneven run lengths, because equal ones would hide an off-by-one in
            // whichever cursor runs dry first.
            const uint32_t count = 1 + static_cast<uint32_t>(rng.next() % 200);
            const std::vector<uint8_t> run = sorted_copy(random_records(&rng, count));
            ASSERT_TRUE(runs.add(run).ok());
            all.insert(all.end(), run.begin(), run.end());
        }

        std::unique_ptr<MergingPointSource> source;
        ASSERT_TRUE(MergingPointSource::create(runs.paths(), kRecordSize, /*block_records=*/64,
                                               /*buffer_records_per_run=*/8, &source)
                            .ok());
        std::vector<uint8_t> merged;
        ASSERT_TRUE(drain(source.get(), 64, &merged).ok());

        EXPECT_EQ(merged, sorted_copy(all));
    }
}

// Duplicate records across runs are the dense-value shape the index has to be
// right about; the merge must emit every copy, not deduplicate.
TEST(BkdPointMergerTest, KeepsEveryDuplicate) {
    ScopedRuns runs("dupes");
    // Three runs, all identical, all a single repeated record.
    std::vector<uint8_t> run(static_cast<size_t>(5) * kRecordSize, 0x7F);
    for (int i = 0; i < 3; ++i) {
        ASSERT_TRUE(runs.add(run).ok());
    }

    std::unique_ptr<MergingPointSource> source;
    ASSERT_TRUE(MergingPointSource::create(runs.paths(), kRecordSize, /*block_records=*/4,
                                           /*buffer_records_per_run=*/2, &source)
                        .ok());
    std::vector<uint8_t> merged;
    ASSERT_TRUE(drain(source.get(), 4, &merged).ok());
    EXPECT_EQ(merged.size(), static_cast<size_t>(15) * kRecordSize);
    EXPECT_EQ(merged, std::vector<uint8_t>(merged.size(), 0x7F));
}

// An empty run is legal (the resident buffer can be empty at finish time) and
// must not end the merge early or contribute a phantom record.
TEST(BkdPointMergerTest, EmptyRunsAreSkipped) {
    Rng rng(0x13198A2E03707344ULL);
    ScopedRuns runs("empties");
    const std::vector<uint8_t> a = sorted_copy(random_records(&rng, 30));
    const std::vector<uint8_t> b = sorted_copy(random_records(&rng, 20));

    ASSERT_TRUE(runs.add({}).ok()); // leading empty
    ASSERT_TRUE(runs.add(a).ok());
    ASSERT_TRUE(runs.add({}).ok()); // middle empty
    ASSERT_TRUE(runs.add(b).ok());
    ASSERT_TRUE(runs.add({}).ok()); // trailing empty

    std::unique_ptr<MergingPointSource> source;
    ASSERT_TRUE(MergingPointSource::create(runs.paths(), kRecordSize, /*block_records=*/16,
                                           /*buffer_records_per_run=*/4, &source)
                        .ok());
    std::vector<uint8_t> merged;
    ASSERT_TRUE(drain(source.get(), 16, &merged).ok());

    std::vector<uint8_t> all = a;
    all.insert(all.end(), b.begin(), b.end());
    EXPECT_EQ(merged, sorted_copy(all));
}

// Blocking is the leaf-cutting contract: every block but the last is exactly
// max_points records, and the stream that comes out does not depend on it.
TEST(BkdPointMergerTest, BlockingDoesNotChangeTheStream) {
    Rng rng(0xA4093822299F31D0ULL);
    ScopedRuns runs("blocking");
    std::vector<uint8_t> all;
    for (int i = 0; i < 4; ++i) {
        const std::vector<uint8_t> run = sorted_copy(random_records(&rng, 101));
        ASSERT_TRUE(runs.add(run).ok());
        all.insert(all.end(), run.begin(), run.end());
    }
    const std::vector<uint8_t> expected = sorted_copy(all);
    const size_t total = expected.size() / kRecordSize;

    for (const uint32_t max_points : {1U, 3U, 64U, 404U, 1000U}) {
        SCOPED_TRACE("max_points " + std::to_string(max_points));
        std::unique_ptr<MergingPointSource> source;
        ASSERT_TRUE(MergingPointSource::create(runs.paths(), kRecordSize, max_points,
                                               /*buffer_records_per_run=*/8, &source)
                            .ok());
        std::vector<uint8_t> merged;
        std::vector<size_t> block_sizes;
        ASSERT_TRUE(drain(source.get(), max_points, &merged, &block_sizes).ok());
        EXPECT_EQ(merged, expected);

        ASSERT_FALSE(block_sizes.empty());
        for (size_t i = 0; i + 1 < block_sizes.size(); ++i) {
            EXPECT_EQ(block_sizes[i], max_points) << "block " << i << " is short";
        }
        EXPECT_EQ(block_sizes.back(), total - max_points * (block_sizes.size() - 1));
    }
}

// Exhaustion is sticky: once the stream ends, every later call keeps returning
// empty rather than restarting or reading past the end.
TEST(BkdPointMergerTest, ExhaustionIsSticky) {
    Rng rng(0x452821E638D01377ULL);
    ScopedRuns runs("sticky");
    ASSERT_TRUE(runs.add(sorted_copy(random_records(&rng, 3))).ok());

    std::unique_ptr<MergingPointSource> source;
    ASSERT_TRUE(MergingPointSource::create(runs.paths(), kRecordSize, /*block_records=*/8,
                                           /*buffer_records_per_run=*/2, &source)
                        .ok());
    Slice block;
    ASSERT_TRUE(source->next_block(8, &block).ok());
    EXPECT_EQ(block.size(), static_cast<size_t>(3) * kRecordSize);
    for (int i = 0; i < 3; ++i) {
        ASSERT_TRUE(source->next_block(8, &block).ok());
        EXPECT_EQ(block.size(), 0U);
    }
}

// No runs at all is the "everything fit in RAM" case reaching the merge by
// mistake; it must be an empty stream, not a crash.
TEST(BkdPointMergerTest, NoRunsIsAnEmptyStream) {
    std::unique_ptr<MergingPointSource> source;
    ASSERT_TRUE(MergingPointSource::create({}, kRecordSize, /*block_records=*/8,
                                           /*buffer_records_per_run=*/2, &source)
                        .ok());
    Slice block;
    ASSERT_TRUE(source->next_block(8, &block).ok());
    EXPECT_EQ(block.size(), 0U);
}

} // namespace doris::snii::bkd
