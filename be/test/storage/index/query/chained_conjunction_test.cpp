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

#include "storage/index/query/exec/chained_conjunction.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <limits>
#include <utility>
#include <vector>

#include "storage/index/query/spi/io_batch.h"

namespace doris::index_query {
namespace {

constexpr uint64_t kWindowBytes = 16;
constexpr uint64_t kUnlimited = std::numeric_limits<uint64_t>::max();

class RecordingReader final : public IoReader {
public:
    Status read_at(uint64_t offset, size_t len, std::vector<uint8_t>* out) override {
        reads.push_back({.offset = offset, .len = len});
        if (fail) {
            return Status::IOError("injected read failure");
        }
        out->assign(len, 0);
        return Status::OK();
    }
    uint64_t size() const override { return uint64_t {1} << 20; }

    std::vector<IoRange> reads;
    bool fail = false;
};

// A term stored as windows of documents; listing a window needs one read of it.
class WindowedTerm final : public ChainedPostings {
public:
    WindowedTerm(IoReader& reader, uint64_t base, std::vector<std::vector<uint32_t>> windows)
            : _reader(reader), _base(base), _windows(std::move(windows)) {}

    uint64_t doc_freq() const override {
        uint64_t count = 0;
        for (const auto& window : _windows) {
            count += window.size();
        }
        return count;
    }

    Status start(const std::vector<uint32_t>* candidates) override {
        ++starts;
        _candidates = candidates;
        _next = 0;
        return Status::OK();
    }

    Status prepare_wave(IoBatch& batch, bool* done) override {
        _wave.clear();
        while (_next < _windows.size()) {
            if (!_holds_candidates(_windows[_next])) {
                ++_next;
                continue;
            }
            bool accepted = false;
            size_t handle = 0;
            RETURN_IF_ERROR(batch.try_add(_reader, _base + _next * kWindowBytes, kWindowBytes,
                                          &accepted, &handle, _wave.empty()));
            if (!accepted) {
                break;
            }
            _wave.emplace_back(_next, handle);
            ++_next;
        }
        *done = _next == _windows.size();
        ++waves;
        return Status::OK();
    }

    Status collect_wave(const IoBatch& batch, std::vector<uint32_t>* out) override {
        for (const auto& [window, handle] : _wave) {
            if (batch.get(handle).size() != kWindowBytes) {
                return Status::InternalError("window {} was not fetched", window);
            }
            for (const uint32_t doc : _windows[window]) {
                if (_candidates == nullptr || std::ranges::binary_search(*_candidates, doc)) {
                    out->push_back(doc);
                }
            }
        }
        return Status::OK();
    }

    size_t starts = 0;
    size_t waves = 0;

private:
    bool _holds_candidates(const std::vector<uint32_t>& window) const {
        if (_candidates == nullptr) {
            return true;
        }
        const auto first = std::ranges::lower_bound(*_candidates, window.front());
        return first != _candidates->end() && *first <= window.back();
    }

    IoReader& _reader;
    uint64_t _base;
    std::vector<std::vector<uint32_t>> _windows;
    const std::vector<uint32_t>* _candidates = nullptr;
    size_t _next = 0;
    std::vector<std::pair<size_t, size_t>> _wave;
};

Status run(std::vector<ChainedPostings*> terms, const std::vector<uint32_t>* initial,
           IoBatch& batch, std::vector<uint32_t>* result, std::vector<size_t>* visited) {
    return chained_conjunction(terms, initial, batch, result, visited);
}

TEST(IndexQueryChainedConjunction, ListsTermsInAscendingDocumentFrequency) {
    RecordingReader reader;
    WindowedTerm common(reader, 0, {{1, 2, 3, 4, 5, 6, 7, 8}, {9, 10, 11, 12}});
    WindowedTerm rare(reader, 1024, {{2, 9, 12}});
    WindowedTerm middle(reader, 2048, {{2, 3, 9}, {11, 12}});
    MemoryBudget budget(kUnlimited);
    IoBatch batch(budget, {.bytes = kUnlimited, .ranges = 64});
    std::vector<uint32_t> result;
    std::vector<size_t> visited;
    ASSERT_TRUE(run({&common, &rare, &middle}, nullptr, batch, &result, &visited).ok());
    EXPECT_EQ(result, (std::vector<uint32_t> {2, 9, 12}));
    EXPECT_EQ(visited, (std::vector<size_t> {1, 2, 0}));
    EXPECT_EQ(batch.pending(), 0U);
    EXPECT_EQ(budget.used_bytes(), 0U);
}

TEST(IndexQueryChainedConjunction, ReadsOnlyWindowsThatHoldSurvivingCandidates) {
    RecordingReader reader;
    WindowedTerm rare(reader, 0, {{40, 41}});
    WindowedTerm wide(reader, 1024, {{1, 2, 3}, {10, 11, 12}, {40, 41, 42}, {60, 61, 62}});
    MemoryBudget budget(kUnlimited);
    IoBatch batch(budget, {.bytes = kUnlimited, .ranges = 64});
    std::vector<uint32_t> result;
    ASSERT_TRUE(run({&wide, &rare}, nullptr, batch, &result, nullptr).ok());
    EXPECT_EQ(result, (std::vector<uint32_t> {40, 41}));
    ASSERT_EQ(reader.reads.size(), 2U);
    EXPECT_EQ(reader.reads[1].offset, 1024 + 2 * kWindowBytes);
}

TEST(IndexQueryChainedConjunction, EmptyIntermediateResultStopsLaterTerms) {
    RecordingReader reader;
    WindowedTerm first(reader, 0, {{1, 3}});
    WindowedTerm disjoint(reader, 1024, {{2, 4, 6}});
    WindowedTerm large(reader, 2048, {{1, 2, 3, 4, 5, 6, 7, 8, 9}});
    MemoryBudget budget(kUnlimited);
    IoBatch batch(budget, {.bytes = kUnlimited, .ranges = 64});
    std::vector<uint32_t> result;
    std::vector<size_t> visited;
    ASSERT_TRUE(run({&large, &first, &disjoint}, nullptr, batch, &result, &visited).ok());
    EXPECT_TRUE(result.empty());
    EXPECT_EQ(visited, (std::vector<size_t> {1, 2}));
    EXPECT_EQ(large.starts, 0U);
    EXPECT_EQ(reader.reads.size(), 2U);
}

TEST(IndexQueryChainedConjunction, InitialCandidatesRestrictTheFirstTerm) {
    RecordingReader reader;
    WindowedTerm rare(reader, 0, {{5, 20}, {70, 90}});
    WindowedTerm wide(reader, 1024, {{5, 6, 7, 20}, {70, 71, 90, 91}});
    const std::vector<uint32_t> initial = {20, 70, 91};
    MemoryBudget budget(kUnlimited);
    IoBatch batch(budget, {.bytes = kUnlimited, .ranges = 64});
    std::vector<uint32_t> result;
    ASSERT_TRUE(run({&wide, &rare}, &initial, batch, &result, nullptr).ok());
    EXPECT_EQ(result, (std::vector<uint32_t> {20, 70}));
}

TEST(IndexQueryChainedConjunction, EmptyInitialCandidatesReadNothing) {
    RecordingReader reader;
    WindowedTerm term(reader, 0, {{1, 2}});
    const std::vector<uint32_t> initial;
    MemoryBudget budget(kUnlimited);
    IoBatch batch(budget, {.bytes = kUnlimited, .ranges = 64});
    std::vector<uint32_t> result = {7};
    std::vector<size_t> visited = {3};
    ASSERT_TRUE(run({&term}, &initial, batch, &result, &visited).ok());
    EXPECT_TRUE(result.empty());
    EXPECT_TRUE(visited.empty());
    EXPECT_EQ(term.starts, 0U);
    EXPECT_TRUE(reader.reads.empty());
}

TEST(IndexQueryChainedConjunction, NoTermsKeepTheInitialCandidates) {
    MemoryBudget budget(kUnlimited);
    IoBatch batch(budget, {.bytes = kUnlimited, .ranges = 64});
    const std::vector<uint32_t> initial = {3, 8};
    std::vector<uint32_t> result;
    ASSERT_TRUE(run({}, &initial, batch, &result, nullptr).ok());
    EXPECT_EQ(result, initial);
    ASSERT_TRUE(run({}, nullptr, batch, &result, nullptr).ok());
    EXPECT_TRUE(result.empty());
}

// One read per wave keeps at most one window's bytes admitted at a time.
TEST(IndexQueryChainedConjunction, ListsATermInBoundedWaves) {
    RecordingReader reader;
    WindowedTerm rare(reader, 0, {{2, 30, 50}});
    WindowedTerm wide(reader, 1024, {{1, 2}, {29, 30}, {50, 51}});
    MemoryBudget budget(kUnlimited);
    IoBatch batch(budget, {.bytes = kWindowBytes, .ranges = 1});
    std::vector<uint32_t> result;
    ASSERT_TRUE(run({&wide, &rare}, nullptr, batch, &result, nullptr).ok());
    EXPECT_EQ(result, (std::vector<uint32_t> {2, 30, 50}));
    EXPECT_EQ(wide.waves, 3U);
    EXPECT_EQ(reader.reads.size(), 4U);
    EXPECT_EQ(budget.peak_bytes(), kWindowBytes);
    EXPECT_EQ(budget.used_bytes(), 0U);
}

TEST(IndexQueryChainedConjunction, AWaveThatAdmitsNoReadFails) {
    RecordingReader reader;
    WindowedTerm term(reader, 0, {{1, 2}});
    MemoryBudget budget(kUnlimited);
    IoBatch batch(budget, {.bytes = kUnlimited, .ranges = 0});
    std::vector<uint32_t> result;
    const Status status = run({&term}, nullptr, batch, &result, nullptr);
    EXPECT_TRUE(status.is<ErrorCode::MEM_LIMIT_EXCEEDED>()) << status;
    EXPECT_TRUE(reader.reads.empty());
}

TEST(IndexQueryChainedConjunction, ReadFailureEndsTheChainAndReleasesTheWave) {
    RecordingReader reader;
    reader.fail = true;
    WindowedTerm first(reader, 0, {{1, 2}});
    WindowedTerm second(reader, 1024, {{1, 2, 3}});
    MemoryBudget budget(kUnlimited);
    IoBatch batch(budget, {.bytes = kUnlimited, .ranges = 64});
    std::vector<uint32_t> result;
    const Status status = run({&first, &second}, nullptr, batch, &result, nullptr);
    EXPECT_TRUE(status.is<ErrorCode::IO_ERROR>()) << status;
    EXPECT_EQ(second.starts, 0U);
    EXPECT_EQ(budget.used_bytes(), 0U);
}

} // namespace
} // namespace doris::index_query
