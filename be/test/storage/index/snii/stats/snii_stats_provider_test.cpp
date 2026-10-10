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

#include "storage/index/snii/stats/snii_stats_provider.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <atomic>
#include <barrier>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "common/status.h"
#include "runtime/thread_context.h"
#include "storage/index/snii/io/local_file.h"
#include "storage/index/snii/io/metered_file_reader.h"
#include "storage/index/snii/query/bm25_scorer.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii/writer/logical_index_writer.h"
#include "storage/index/snii/writer/snii_compound_writer.h"

using namespace doris::snii;
using namespace doris::snii::format;
using namespace doris::snii::writer;
using doris::snii::stats::SniiStatsProvider;

namespace {

std::string TempPath() {
    static int counter = 0;
    return "/tmp/snii_stats_test_" + std::to_string(getpid()) + "_" + std::to_string(counter++) +
           ".idx";
}

class ControllableFileReader final : public io::FileReader {
public:
    explicit ControllableFileReader(io::FileReader* inner) : inner_(inner) {}

    doris::Status read_at(uint64_t offset, size_t len, std::vector<uint8_t>* out) override {
        read_at_calls_.fetch_add(1, std::memory_order_relaxed);
        if (fail_next_read_.exchange(false, std::memory_order_acq_rel)) {
            return doris::Status::IOError<false>("injected norms read failure");
        }

        {
            std::unique_lock lock(mutex_);
            if (block_next_read_) {
                block_next_read_ = false;
                blocked_read_started_ = true;
                condition_.notify_all();
                condition_.wait(lock, [this] { return blocked_read_released_; });
            }
        }
        return inner_->read_at(offset, len, out);
    }

    uint64_t size() const override { return inner_->size(); }

    void reset_read_at_calls() { read_at_calls_.store(0, std::memory_order_relaxed); }
    uint64_t read_at_calls() const { return read_at_calls_.load(std::memory_order_relaxed); }
    void fail_next_read() { fail_next_read_.store(true, std::memory_order_release); }

    void block_next_read() {
        std::lock_guard lock(mutex_);
        block_next_read_ = true;
        blocked_read_started_ = false;
        blocked_read_released_ = false;
    }

    void wait_for_blocked_read() {
        std::unique_lock lock(mutex_);
        condition_.wait(lock, [this] { return blocked_read_started_; });
    }

    void release_blocked_read() {
        std::lock_guard lock(mutex_);
        blocked_read_released_ = true;
        condition_.notify_all();
    }

private:
    io::FileReader* inner_ = nullptr;
    std::atomic<uint64_t> read_at_calls_ = 0;
    std::atomic<bool> fail_next_read_ = false;
    std::mutex mutex_;
    std::condition_variable condition_;
    bool block_next_read_ = false;
    bool blocked_read_started_ = false;
    bool blocked_read_released_ = false;
};

// A small in-memory corpus: each doc is a bag of (term -> freq). Doc lengths vary so the norms
// differ.
struct Corpus {
    uint32_t doc_count = 0;
    // term -> (docid -> freq), docids ascending.
    std::map<std::string, std::map<uint32_t, uint32_t>> postings;
    std::vector<uint64_t> doc_len; // per-doc total token count
};

// Builds ~60 docs with varied lengths and a high-df + low-df term.
Corpus MakeCorpus() {
    Corpus c;
    c.doc_count = 60;
    c.doc_len.assign(c.doc_count, 0);

    auto add = [&](const std::string& term, uint32_t doc, uint32_t freq) {
        c.postings[term][doc] += freq;
        c.doc_len[doc] += freq;
    };

    for (uint32_t d = 0; d < c.doc_count; ++d) {
        if (d % 2 == 0) {
            add("common", d, 1 + (d % 4));
        }
        if (d == 3 || d == 17 || d == 42) {
            add("rare", d, 2);
        }
        add("filler", d, 1 + (d % 7) * 3);
        add("pad" + std::to_string(d % 11), d, (d % 5) + 1);
    }
    return c;
}

// Converts the corpus into a sorted SniiIndexInput with encoded norms.
SniiIndexInput ToInput(const Corpus& c) {
    SniiIndexInput in;
    in.index_id = 1;
    in.index_suffix = "body";
    in.config = IndexConfig::kDocsPositions;
    in.doc_count = c.doc_count;
    in.target_dict_block_bytes = 1; // one block per term

    in.encoded_norms.resize(c.doc_count);
    for (uint32_t d = 0; d < c.doc_count; ++d) {
        in.encoded_norms[d] = doris::snii::query::encode_norm(c.doc_len[d]);
    }

    for (const auto& [term, plist] : c.postings) {
        TermPostings tp;
        tp.term = term;
        for (const auto& [docid, freq] : plist) {
            tp.docids.push_back(docid);
            tp.freqs.push_back(freq);
            for (uint32_t k = 0; k < freq; ++k) {
                tp.positions_flat.push_back(k); // flat
            }
        }
        in.terms.push_back(std::move(tp));
    }
    return in;
}

} // namespace

TEST(SniiStatsProvider, SharesValidatedNormsAcrossQueries) {
    const Corpus corpus = MakeCorpus();
    const std::string path = TempPath();
    {
        io::LocalFileWriter writer;
        ASSERT_TRUE(writer.open(path).ok());
        SniiCompoundWriter compound_writer(&writer);
        ASSERT_TRUE(compound_writer.add_logical_index(ToInput(corpus)).ok());
        ASSERT_TRUE(compound_writer.finish().ok());
    }

    io::LocalFileReader file_reader;
    ASSERT_TRUE(file_reader.open(path).ok());
    io::MeteredFileReader metered_reader(&file_reader, /*block_size=*/64);
    reader::SniiSegmentReader segment_reader;
    ASSERT_TRUE(reader::SniiSegmentReader::open(&metered_reader, &segment_reader).ok());
    reader::LogicalIndexReader logical_reader;
    ASSERT_TRUE(segment_reader.open_index(1, "body", &logical_reader).ok());

    metered_reader.reset_metrics();
    const size_t memory_usage_before_load = logical_reader.memory_usage();
    SniiStatsProvider first;
    ASSERT_TRUE(SniiStatsProvider::open(&logical_reader, &first).ok());
    const io::IoMetrics after_first = metered_reader.metrics();
    EXPECT_GT(after_first.total_request_bytes, 0U);

    SniiStatsProvider second;
    ASSERT_TRUE(SniiStatsProvider::open(&logical_reader, &second).ok());
    EXPECT_EQ(metered_reader.metrics().read_at_calls, after_first.read_at_calls);
    EXPECT_EQ(metered_reader.metrics().total_request_bytes, after_first.total_request_bytes);
    EXPECT_EQ(logical_reader.memory_usage(), memory_usage_before_load);

    uint8_t first_norm = 0;
    uint8_t second_norm = 0;
    ASSERT_TRUE(first.encoded_norm(17, &first_norm).ok());
    ASSERT_TRUE(second.encoded_norm(17, &second_norm).ok());
    EXPECT_EQ(first_norm, second_norm);

    std::remove(path.c_str());
}

TEST(SniiStatsProvider, SharesOneConcurrentNormsLoad) {
    const Corpus corpus = MakeCorpus();
    const std::string path = TempPath();
    {
        io::LocalFileWriter writer;
        ASSERT_TRUE(writer.open(path).ok());
        SniiCompoundWriter compound_writer(&writer);
        ASSERT_TRUE(compound_writer.add_logical_index(ToInput(corpus)).ok());
        ASSERT_TRUE(compound_writer.finish().ok());
    }

    io::LocalFileReader local_reader;
    ASSERT_TRUE(local_reader.open(path).ok());
    ControllableFileReader controlled_reader(&local_reader);
    reader::SniiSegmentReader segment_reader;
    ASSERT_TRUE(reader::SniiSegmentReader::open(&controlled_reader, &segment_reader).ok());
    reader::LogicalIndexReader logical_reader;
    ASSERT_TRUE(segment_reader.open_index(1, "body", &logical_reader).ok());

    constexpr size_t kThreadCount = 16;
    std::barrier start(static_cast<std::ptrdiff_t>(kThreadCount + 1));
    std::vector<doris::Status> statuses(kThreadCount);
    std::vector<uint8_t> norms(kThreadCount);
    std::vector<std::thread> threads;
    threads.reserve(kThreadCount);
    controlled_reader.reset_read_at_calls();
    controlled_reader.block_next_read();
    for (size_t i = 0; i < kThreadCount; ++i) {
        threads.emplace_back([&, i] {
            SCOPED_INIT_THREAD_CONTEXT();
            start.arrive_and_wait();
            SniiStatsProvider provider;
            statuses[i] = SniiStatsProvider::open(&logical_reader, &provider);
            if (statuses[i].ok()) {
                statuses[i] = provider.encoded_norm(17, &norms[i]);
            }
        });
    }
    start.arrive_and_wait();
    controlled_reader.wait_for_blocked_read();
    controlled_reader.release_blocked_read();
    for (auto& thread : threads) {
        thread.join();
    }

    for (size_t i = 0; i < kThreadCount; ++i) {
        EXPECT_TRUE(statuses[i].ok()) << statuses[i].to_string();
        EXPECT_EQ(norms[i], norms[0]);
    }
    EXPECT_EQ(controlled_reader.read_at_calls(), 1U);

    std::remove(path.c_str());
}

TEST(SniiStatsProvider, RetriesTransientNormsReadFailure) {
    const Corpus corpus = MakeCorpus();
    const std::string path = TempPath();
    {
        io::LocalFileWriter writer;
        ASSERT_TRUE(writer.open(path).ok());
        SniiCompoundWriter compound_writer(&writer);
        ASSERT_TRUE(compound_writer.add_logical_index(ToInput(corpus)).ok());
        ASSERT_TRUE(compound_writer.finish().ok());
    }

    io::LocalFileReader local_reader;
    ASSERT_TRUE(local_reader.open(path).ok());
    ControllableFileReader controlled_reader(&local_reader);
    reader::SniiSegmentReader segment_reader;
    ASSERT_TRUE(reader::SniiSegmentReader::open(&controlled_reader, &segment_reader).ok());
    reader::LogicalIndexReader logical_reader;
    ASSERT_TRUE(segment_reader.open_index(1, "body", &logical_reader).ok());

    controlled_reader.reset_read_at_calls();
    controlled_reader.fail_next_read();
    SniiStatsProvider failed;
    const doris::Status first_status = SniiStatsProvider::open(&logical_reader, &failed);
    EXPECT_TRUE(first_status.is<doris::ErrorCode::IO_ERROR>()) << first_status.to_string();

    SniiStatsProvider retry;
    ASSERT_TRUE(SniiStatsProvider::open(&logical_reader, &retry).ok());
    uint8_t norm = 0;
    ASSERT_TRUE(retry.encoded_norm(17, &norm).ok());
    EXPECT_EQ(controlled_reader.read_at_calls(), 2U);

    std::remove(path.c_str());
}
