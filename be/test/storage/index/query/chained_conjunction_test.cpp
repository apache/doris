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
#include <utility>
#include <vector>

#include "storage/index/query/spi/io_read_batch.h"

namespace doris::index_query {
namespace {

constexpr uint64_t kWindowBytes = 16;

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

// A term stored as windows of documents; listing a window needs one read of it, and a
// listing reads its windows in one round.
class WindowedTerm final : public ChainedPostings {
public:
    WindowedTerm(IoReader* reader, uint64_t base, std::vector<std::vector<uint32_t>> windows)
            : _batch(reader), _base(base), _windows(std::move(windows)) {}

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
        _listed.clear();
        _batch.clear();
        for (size_t window = 0; window < _windows.size(); ++window) {
            if (_holds_candidates(_windows[window])) {
                _listed.emplace_back(window,
                                     _batch.add(_base + window * kWindowBytes, kWindowBytes));
            }
        }
        return _batch.pending() > 0 ? _batch.fetch() : Status::OK();
    }

    Status collect(std::vector<uint32_t>* out) override {
        for (const auto& [window, handle] : _listed) {
            if (_batch.get(handle).size() != kWindowBytes) {
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

private:
    bool _holds_candidates(const std::vector<uint32_t>& window) const {
        if (_candidates == nullptr) {
            return true;
        }
        const auto first = std::ranges::lower_bound(*_candidates, window.front());
        return first != _candidates->end() && *first <= window.back();
    }

    IoReadBatch _batch;
    uint64_t _base;
    std::vector<std::vector<uint32_t>> _windows;
    const std::vector<uint32_t>* _candidates = nullptr;
    std::vector<std::pair<size_t, size_t>> _listed;
};

Status run(std::vector<ChainedPostings*> terms, const std::vector<uint32_t>* initial,
           std::vector<uint32_t>* result, std::vector<size_t>* visited) {
    return chained_conjunction(terms, initial, result, visited);
}

TEST(IndexQueryChainedConjunction, ListsTermsInAscendingDocumentFrequency) {
    RecordingReader reader;
    WindowedTerm common(&reader, 0, {{1, 2, 3, 4, 5, 6, 7, 8}, {9, 10, 11, 12}});
    WindowedTerm rare(&reader, 1024, {{2, 9, 12}});
    WindowedTerm middle(&reader, 2048, {{2, 3, 9}, {11, 12}});
    std::vector<uint32_t> result;
    std::vector<size_t> visited;
    ASSERT_TRUE(run({&common, &rare, &middle}, nullptr, &result, &visited).ok());
    EXPECT_EQ(result, (std::vector<uint32_t> {2, 9, 12}));
    EXPECT_EQ(visited, (std::vector<size_t> {1, 2, 0}));
}

TEST(IndexQueryChainedConjunction, ReadsOnlyWindowsThatHoldSurvivingCandidates) {
    RecordingReader reader;
    WindowedTerm rare(&reader, 0, {{40, 41}});
    WindowedTerm wide(&reader, 1024, {{1, 2, 3}, {10, 11, 12}, {40, 41, 42}, {60, 61, 62}});
    std::vector<uint32_t> result;
    ASSERT_TRUE(run({&wide, &rare}, nullptr, &result, nullptr).ok());
    EXPECT_EQ(result, (std::vector<uint32_t> {40, 41}));
    ASSERT_EQ(reader.reads.size(), 2U);
    EXPECT_EQ(reader.reads[1].offset, 1024 + 2 * kWindowBytes);
}

TEST(IndexQueryChainedConjunction, ATermWithoutCandidateWindowsReadsNothing) {
    RecordingReader reader;
    WindowedTerm rare(&reader, 0, {{5}});
    WindowedTerm wide(&reader, 1024, {{1, 2}, {10, 11}});
    std::vector<uint32_t> result;
    ASSERT_TRUE(run({&wide, &rare}, nullptr, &result, nullptr).ok());
    EXPECT_TRUE(result.empty());
    EXPECT_EQ(wide.starts, 1U);
    EXPECT_EQ(reader.reads.size(), 1U);
}

TEST(IndexQueryChainedConjunction, EmptyIntermediateResultStopsLaterTerms) {
    RecordingReader reader;
    WindowedTerm first(&reader, 0, {{1, 3}});
    WindowedTerm disjoint(&reader, 1024, {{2, 4, 6}});
    WindowedTerm large(&reader, 2048, {{1, 2, 3, 4, 5, 6, 7, 8, 9}});
    std::vector<uint32_t> result;
    std::vector<size_t> visited;
    ASSERT_TRUE(run({&large, &first, &disjoint}, nullptr, &result, &visited).ok());
    EXPECT_TRUE(result.empty());
    EXPECT_EQ(visited, (std::vector<size_t> {1, 2}));
    EXPECT_EQ(large.starts, 0U);
    EXPECT_EQ(reader.reads.size(), 2U);
}

TEST(IndexQueryChainedConjunction, InitialCandidatesRestrictTheFirstTerm) {
    RecordingReader reader;
    WindowedTerm rare(&reader, 0, {{5, 20}, {70, 90}});
    WindowedTerm wide(&reader, 1024, {{5, 6, 7, 20}, {70, 71, 90, 91}});
    const std::vector<uint32_t> initial = {20, 70, 91};
    std::vector<uint32_t> result;
    ASSERT_TRUE(run({&wide, &rare}, &initial, &result, nullptr).ok());
    EXPECT_EQ(result, (std::vector<uint32_t> {20, 70}));
}

TEST(IndexQueryChainedConjunction, EmptyInitialCandidatesReadNothing) {
    RecordingReader reader;
    WindowedTerm term(&reader, 0, {{1, 2}});
    const std::vector<uint32_t> initial;
    std::vector<uint32_t> result = {7};
    std::vector<size_t> visited = {3};
    ASSERT_TRUE(run({&term}, &initial, &result, &visited).ok());
    EXPECT_TRUE(result.empty());
    EXPECT_TRUE(visited.empty());
    EXPECT_EQ(term.starts, 0U);
    EXPECT_TRUE(reader.reads.empty());
}

TEST(IndexQueryChainedConjunction, NoTermsKeepTheInitialCandidates) {
    const std::vector<uint32_t> initial = {3, 8};
    std::vector<uint32_t> result;
    ASSERT_TRUE(run({}, &initial, &result, nullptr).ok());
    EXPECT_EQ(result, initial);
    ASSERT_TRUE(run({}, nullptr, &result, nullptr).ok());
    EXPECT_TRUE(result.empty());
}

TEST(IndexQueryChainedConjunction, ReadFailureEndsTheChain) {
    RecordingReader reader;
    reader.fail = true;
    WindowedTerm first(&reader, 0, {{1, 2}});
    WindowedTerm second(&reader, 1024, {{1, 2, 3}});
    std::vector<uint32_t> result;
    const Status status = run({&first, &second}, nullptr, &result, nullptr);
    EXPECT_TRUE(status.is<ErrorCode::IO_ERROR>()) << status;
    EXPECT_EQ(second.starts, 0U);
}

} // namespace
} // namespace doris::index_query
