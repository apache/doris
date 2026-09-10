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

#include <sys/resource.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <climits>
#include <cstdio>
#include <cstring>
#include <limits>
#include <memory>
#include <queue>
#include <utility>

#include "storage/index/snii/encoding/crc32c.h"
#include "storage/index/snii/writer/encoded_spill_run.h"
#include "storage/index/snii/writer/spill_run_codec.h"
#include "storage/index/snii/writer/temp_dir.h"

namespace doris::snii::writer {
namespace {

Status invalid_run(const char* reason) {
    return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("spill merge: {}",
                                                                          reason);
}

std::atomic<uint64_t> g_run_compactions {0};

struct RunRange {
    std::string path;
    uint64_t begin = 0;
    uint64_t end = UINT64_MAX;
};

// Caller-owned fixture files, or a bounded directory of sealed ranges in one
// ingestion spool. The latter has no resident path/offset array proportional to
// the number of spills.
struct RunInputs {
    const std::vector<std::string>* files = nullptr;
    const std::string* spool = nullptr;
    PostingByteBuffer* ends = nullptr;
    size_t count = 0;
    size_t fan_in_limit = 0;
};

using Readers = std::vector<std::unique_ptr<StreamingRunReader>>;

std::string merge_run_path() {
    static std::atomic<uint64_t> sequence {0};
    return resolve_temp_dir() + "/snii_merge_" + std::to_string(::getpid()) + "_" +
           std::to_string(sequence.fetch_add(1, std::memory_order_relaxed)) + ".run";
}

// One-token lookahead survives a fill boundary, including a document split
// across any number of input fragments. Positions are appended incrementally
// into the consumer's replayable buffer, never accumulated per document here.
class RunTermSource final : public TermPostingSource {
public:
    RunTermSource(Readers* readers, const std::vector<size_t>* matching, bool positions)
            : readers_(readers), matching_(matching), positions_(positions) {}

    Status fill(uint32_t target_docs, TermPostingBuffer* out, bool* exhausted) override {
        if (target_docs == 0 || out == nullptr || exhausted == nullptr || !out->empty()) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "spill merge: invalid source fill arguments");
        }
        if (!pending_) {
            RETURN_IF_ERROR(next());
        }
        while (pending_ && out->document_count() < target_docs) {
            const uint32_t docid = docid_;
            MutableTermPostingSpan document;
            RETURN_IF_ERROR(out->grow_uninitialized(1, true, 0, &document));
            document.docids[0] = docid;
            uint64_t frequency = 0;
            do {
                if (positions_) {
                    RETURN_IF_ERROR(out->append_position(position_));
                }
                if (++frequency > std::numeric_limits<uint32_t>::max()) {
                    return invalid_run("document frequency overflows uint32");
                }
                RETURN_IF_ERROR(next());
            } while (pending_ && docid_ == docid);
            // Appending positions can replace the buffer's vectors. Acquire the
            // frequency slot again rather than retaining an invalidated span.
            out->set_last_frequency(static_cast<uint32_t>(frequency));
            if (pending_ && docid_ < docid) {
                return invalid_run("docids go backwards across run fragments");
            }
        }
        *exhausted = !pending_ && run_ == matching_->size();
        exhausted_ = *exhausted;
        return Status::OK();
    }

    bool exhausted() const { return exhausted_; }

private:
    Status next() {
        pending_ = false;
        while (run_ < matching_->size()) {
            bool end = false;
            RETURN_IF_ERROR((*readers_)[(*matching_)[run_]]->next_token(&docid_, &position_, &end));
            if (!end) {
                pending_ = true;
                break;
            }
            ++run_;
        }
        return Status::OK();
    }

    Readers* readers_;
    const std::vector<size_t>* matching_;
    const bool positions_;
    size_t run_ = 0;
    uint32_t docid_ = 0;
    uint32_t position_ = 0;
    bool pending_ = false;
    bool exhausted_ = false;
};

struct HeapItem {
    uint32_t id;
    size_t run;
};
struct Greater {
    const std::vector<uint32_t>* rank;
    bool operator()(const HeapItem& left, const HeapItem& right) const {
        const uint32_t a = (*rank)[left.id];
        const uint32_t b = (*rank)[right.id];
        return a == b ? left.run > right.run : a > b;
    }
};

using RunHeap = std::priority_queue<HeapItem, std::vector<HeapItem>, Greater>;

Status take_matching_runs(RunHeap* heap, const Readers& readers, EncodedRunTerm* term,
                          std::vector<size_t>* matching) {
    matching->clear();
    while (!heap->empty() && heap->top().id == term->term_id) {
        const size_t run = heap->top().run;
        heap->pop();
        const auto& input = readers[run]->current();
        if (input.has_positions != term->has_positions) {
            return invalid_run("posting shape differs");
        }
        if (input.document_groups > UINT64_MAX - term->document_groups ||
            input.tokens > UINT64_MAX - term->tokens) {
            return invalid_run("term counts overflow");
        }
        term->document_groups += input.document_groups;
        term->tokens += input.tokens;
        matching->push_back(run);
    }
    return Status::OK();
}

template <typename Consumer>
Status merge_group(const std::vector<RunRange>& paths, const std::vector<uint32_t>& rank,
                   bool positions, MemoryReporter* reporter, bool allow_legacy,
                   const Consumer& consume) {
    auto metadata = reporter == nullptr ? MemoryReporter::Reservation()
                                        : reporter->make_postings_reservation();
    if (reporter != nullptr) {
        // Reader objects own fixed, separately charged payload buffers. Reserve
        // their object storage plus both heap and matching-index capacities.
        RETURN_IF_ERROR(metadata.set_bytes(paths.size() * (StreamingRunReader::object_bytes() +
                                                           sizeof(HeapItem) + 2 * sizeof(size_t))));
    }
    Readers readers;
    readers.reserve(paths.size());
    std::vector<HeapItem> heap_storage;
    heap_storage.reserve(paths.size());
    RunHeap heap(Greater {&rank}, std::move(heap_storage));
    for (size_t run = 0; run < paths.size(); ++run) {
        auto reader = std::make_unique<StreamingRunReader>(reporter);
        RETURN_IF_ERROR(reader->open(paths[run].path, positions, allow_legacy, paths[run].begin,
                                     paths[run].end));
        if (!reader->exhausted()) {
            if (reader->current().term_id >= rank.size()) {
                return invalid_run("term id out of range");
            }
            heap.push({reader->current().term_id, run});
        }
        readers.push_back(std::move(reader));
    }
    std::vector<size_t> matching;
    matching.reserve(paths.size());
    while (!heap.empty()) {
        const uint32_t id = heap.top().id;
        EncodedRunTerm term {.term_id = id,
                             .has_positions = readers[heap.top().run]->current().has_positions};
        RETURN_IF_ERROR(take_matching_runs(&heap, readers, &term, &matching));
        RETURN_IF_ERROR(consume(term, &readers, matching));
        for (size_t run : matching) {
            RETURN_IF_ERROR(readers[run]->advance());
            if (!readers[run]->exhausted()) {
                const uint32_t next_id = readers[run]->current().term_id;
                if (next_id >= rank.size()) {
                    return invalid_run("term id out of range");
                }
                if (rank[next_id] <= rank[id]) {
                    return invalid_run("terms are not strictly ordered");
                }
                heap.push({next_id, run});
            }
        }
    }
    return Status::OK();
}

Status compact_group(const std::vector<RunRange>& paths, const std::vector<uint32_t>& rank,
                     bool positions, const std::string& output, MemoryReporter* reporter,
                     bool allow_legacy) {
    EncodedRunWriter writer(reporter);
    RETURN_IF_ERROR(writer.open(output));
    RETURN_IF_ERROR(merge_group(
            paths, rank, positions, reporter, allow_legacy,
            [&](const EncodedRunTerm& term, Readers* readers, const std::vector<size_t>& matching) {
                RETURN_IF_ERROR(writer.begin_term(term));
                for (size_t run : matching) {
                    RETURN_IF_ERROR((*readers)[run]->copy_fragments_to(&writer));
                }
                return writer.end_term();
            }));
    RETURN_IF_ERROR(writer.close());
    g_run_compactions.fetch_add(1, std::memory_order_relaxed);
    return Status::OK();
}

class ReducedRuns {
public:
    ~ReducedRuns() {
        // Manifests also retain names from failed/partially completed passes.
        // Cleanup reads directly into stack storage and cannot need a budget
        // reservation while unwinding a memory-limit failure.
        std::array<char, PATH_MAX> path {};
        for (auto& manifest : manifests_) {
            uint64_t offset = 0;
            while (offset < manifest->size()) {
                uint32_t length = 0;
                if (!manifest->read_at(offset, {reinterpret_cast<uint8_t*>(&length), 4}).ok()) {
                    break;
                }
                offset += 4;
                if (length >= path.size() ||
                    !manifest->read_at(offset, {reinterpret_cast<uint8_t*>(path.data()), length})
                             .ok()) {
                    break;
                }
                path[length] = 0;
                std::remove(path.data());
                offset += length;
            }
        }
    }

    Status prepare(const RunInputs& inputs, const std::vector<uint32_t>& rank, bool positions,
                   MemoryReporter* reporter, bool allow_legacy) {
        allow_legacy_ = allow_legacy;
        rank_ = &rank;
        positions_ = positions;
        reporter_ = reporter;
        // Four legacy raw cursors or two compact cursors per reader, plus room
        // for the output and the caller's final encoder. Contiguous groups keep
        // chronological run order and boundary-document concatenation intact.
        uint64_t budget =
                reporter == nullptr ? 32ULL * 1024 * 1024 : reporter->postings_available_bytes();
        fan_in_ = static_cast<size_t>(std::clamp<uint64_t>(budget / (512 * 1024), 2, 32));
        if (inputs.fan_in_limit != 0) {
            fan_in_ = std::min(fan_in_, std::max<size_t>(2, inputs.fan_in_limit));
        }
        struct rlimit descriptors {};
        if (::getrlimit(RLIMIT_NOFILE, &descriptors) != 0) {
            return Status::Error<ErrorCode::IO_ERROR, false>("spill merge: cannot read fd limit");
        }
        // Leave descriptors for the final output, manifests, codec temporaries,
        // and the caller. Other concurrent writers may still cause open() to fail.
        if (descriptors.rlim_cur != RLIM_INFINITY) {
            fan_in_ = std::min(fan_in_, std::max<size_t>(2, descriptors.rlim_cur / 4));
        }
        size_t passes = 0;
        for (size_t count = inputs.count; count > fan_in_; count = (count - 1) / fan_in_ + 1) {
            ++passes;
        }
        metadata_ = reporter == nullptr ? MemoryReporter::Reservation()
                                        : reporter->make_postings_reservation();
        if (reporter != nullptr) {
            RETURN_IF_ERROR(metadata_.set_bytes(3 * PATH_MAX));
        }
        size_t path_capacity = merge_run_path().size();
        if (inputs.files != nullptr) {
            for (const auto& path : *inputs.files) {
                path_capacity = std::max(path_capacity, path.size());
            }
        } else {
            path_capacity = std::max(path_capacity, inputs.spool->size());
        }
        if (reporter != nullptr) {
            RETURN_IF_ERROR(
                    metadata_.set_bytes(fan_in_ * (sizeof(RunRange) + 2 * (path_capacity + 32)) +
                                        passes * (sizeof(PostingByteBuffer) + sizeof(void*))));
        }
        paths_.reserve(fan_in_);
        manifests_.reserve(passes);
        size_t remaining = inputs.count;
        PostingByteBuffer* previous = nullptr;
        while (remaining > fan_in_) {
            auto manifest = std::make_unique<PostingByteBuffer>(reporter);
            auto* output_manifest = manifest.get();
            manifests_.push_back(std::move(manifest));
            RETURN_IF_ERROR(reduce_pass(inputs, previous, remaining, output_manifest));
            RETURN_IF_ERROR(output_manifest->spill_and_release_buffer());
            previous = output_manifest;
            remaining = (remaining - 1) / fan_in_ + 1;
        }
        std::optional<PostingByteCursor> cursor;
        if (previous != nullptr || inputs.ends != nullptr) {
            cursor.emplace(previous != nullptr ? previous : inputs.ends);
            RETURN_IF_ERROR(cursor->reset());
        }
        return read_paths(inputs, cursor ? &*cursor : nullptr, 0, remaining, previous == nullptr);
    }
    const std::vector<RunRange>& paths() const { return paths_; }

private:
    Status reduce_pass(const RunInputs& inputs, PostingByteBuffer* previous, size_t remaining,
                       PostingByteBuffer* output_manifest) {
        std::optional<PostingByteCursor> cursor;
        if (previous != nullptr || inputs.ends != nullptr) {
            cursor.emplace(previous != nullptr ? previous : inputs.ends);
            RETURN_IF_ERROR(cursor->reset());
        }
        for (size_t begin = 0; begin < remaining; begin += fan_in_) {
            const size_t count = std::min(fan_in_, remaining - begin);
            RETURN_IF_ERROR(read_paths(inputs, cursor ? &*cursor : nullptr, begin, count,
                                       previous == nullptr));
            const std::string output = merge_run_path();
            if (output.size() >= PATH_MAX) {
                return Status::Error<ErrorCode::IO_ERROR, false>("spill merge: path is too long");
            }
            const auto length = static_cast<uint32_t>(output.size());
            RETURN_IF_ERROR(output_manifest->append_u32(std::span(&length, 1)));
            RETURN_IF_ERROR(output_manifest->append(
                    {reinterpret_cast<const uint8_t*>(output.data()), output.size()}));
            RETURN_IF_ERROR(
                    compact_group(paths_, *rank_, positions_, output, reporter_, allow_legacy_));
            if (previous != nullptr) {
                for (const auto& consumed : paths_) {
                    std::remove(consumed.path.c_str());
                }
            }
        }
        return Status::OK();
    }

    Status read_spool_range(const RunInputs& inputs, PostingByteCursor* cursor, RunRange* range) {
        std::array<uint8_t, 12> record {};
        RETURN_IF_ERROR(cursor->read(record));
        uint64_t end = 0;
        uint32_t checksum = 0;
        std::memcpy(&end, record.data(), sizeof(end));
        std::memcpy(&checksum, record.data() + sizeof(end), sizeof(checksum));
        if (checksum != crc32c(Slice(record.data(), sizeof(end))) || end <= spool_offset_) {
            return invalid_run("invalid spool range or directory CRC");
        }
        *range = {.path = *inputs.spool, .begin = spool_offset_, .end = end};
        spool_offset_ = end;
        return Status::OK();
    }

    Status read_paths(const RunInputs& inputs, PostingByteCursor* cursor, size_t begin,
                      size_t count, bool original) {
        paths_.resize(count);
        for (size_t i = 0; i < count; ++i) {
            if (original && inputs.ends != nullptr) {
                RETURN_IF_ERROR(read_spool_range(inputs, cursor, &paths_[i]));
            } else if (cursor == nullptr) {
                paths_[i] = {.path = (*inputs.files)[begin + i]};
            } else {
                uint32_t length = 0;
                RETURN_IF_ERROR(cursor->read_u32(&length));
                if (length >= PATH_MAX) {
                    return invalid_run("invalid manifest path length");
                }
                paths_[i].begin = 0;
                paths_[i].end = UINT64_MAX;
                paths_[i].path.resize(length);
                RETURN_IF_ERROR(
                        cursor->read({reinterpret_cast<uint8_t*>(paths_[i].path.data()), length}));
            }
        }
        return Status::OK();
    }

    MemoryReporter::Reservation metadata_;
    std::vector<RunRange> paths_;
    uint64_t spool_offset_ = 0;
    // Only O(log_F(run_count)) tiny descriptors stay resident. Run names are
    // spilled; consumed intermediate posting files are removed after each group.
    std::vector<std::unique_ptr<PostingByteBuffer>> manifests_;
    const std::vector<uint32_t>* rank_ = nullptr;
    MemoryReporter* reporter_ = nullptr;
    bool positions_ = false;
    bool allow_legacy_ = false;
    size_t fan_in_ = 0;
};

Status merge_sources(const RunInputs& inputs, const std::vector<std::string>& vocab,
                     const std::vector<uint32_t>& string_rank, bool has_positions,
                     const StreamedTermConsumer& fn, MemoryReporter* memory_reporter,
                     bool allow_legacy) {
    if (vocab.size() != string_rank.size()) {
        return Status::Error<ErrorCode::INTERNAL_ERROR, false>(
                "merge_run_sources: string_rank/vocab size mismatch");
    }
    ReducedRuns runs;
    RETURN_IF_ERROR(
            runs.prepare(inputs, string_rank, has_positions, memory_reporter, allow_legacy));
    return merge_group(
            runs.paths(), string_rank, has_positions, memory_reporter, allow_legacy,
            [&](const EncodedRunTerm& term, Readers* readers, const std::vector<size_t>& matching) {
                RunTermSource source(readers, &matching, term.has_positions);
                StreamedTermPostings postings {.term = vocab[term.term_id],
                                               .retain_positions = term.has_positions,
                                               .source = &source};
                RETURN_IF_ERROR(fn(std::move(postings)));
                if (!source.exhausted()) {
                    return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                            "run posting source: consumer returned before term exhaustion");
                }
                return Status::OK();
            });
}

} // namespace

namespace testing {
uint64_t run_compactions() {
    return g_run_compactions.load(std::memory_order_relaxed);
}
void reset_run_compactions() {
    g_run_compactions.store(0, std::memory_order_relaxed);
}
} // namespace testing

Status merge_run_sources(const std::vector<std::string>& run_paths,
                         const std::vector<std::string>& vocab,
                         const std::vector<uint32_t>& string_rank, bool has_positions,
                         const StreamedTermConsumer& fn, MemoryReporter* memory_reporter,
                         bool allow_legacy) {
    return merge_sources({.files = &run_paths, .count = run_paths.size()}, vocab, string_rank,
                         has_positions, fn, memory_reporter, allow_legacy);
}

Status merge_spooled_run_sources(const std::string& spool, PostingByteBuffer* ends, size_t count,
                                 const std::vector<std::string>& vocab,
                                 const std::vector<uint32_t>& string_rank, bool has_positions,
                                 const StreamedTermConsumer& fn, MemoryReporter* reporter,
                                 size_t fan_in_limit) {
    return merge_sources(
            {.spool = &spool, .ends = ends, .count = count, .fan_in_limit = fan_in_limit}, vocab,
            string_rank, has_positions, fn, reporter, false);
}

Status compact_runs(const std::vector<std::string>& run_paths,
                    const std::vector<uint32_t>& string_rank, bool has_positions,
                    const std::string& out_path, MemoryReporter* memory_reporter,
                    bool allow_legacy) {
    ReducedRuns runs;
    RETURN_IF_ERROR(runs.prepare({.files = &run_paths, .count = run_paths.size()}, string_rank,
                                 has_positions, memory_reporter, allow_legacy));
    return compact_group(runs.paths(), string_rank, has_positions, out_path, memory_reporter,
                         allow_legacy);
}

} // namespace doris::snii::writer
