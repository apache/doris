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

#include "storage/index/snii/writer/posting_window_emitter.h"

#include <algorithm>
#include <atomic>
#include <limits>
#include <optional>
#include <utility>
#include <vector>

#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_sink.h"
#include "storage/index/snii/format/dict_entry.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/format/frq_pod.h"
#include "storage/index/snii/format/frq_prelude.h"
#include "storage/index/snii/io/file_writer.h"
#include "storage/index/snii/writer/spillable_byte_buffer.h"

namespace doris::snii::writer {

namespace {

constexpr int kEmitterRawFrqRegion = 0;
constexpr uint32_t kPreludeGroupSize = 64;

struct PostingWindowPlan {
    size_t doc_begin = 0;
    size_t doc_count = 0;
    uint64_t position_begin = 0;
    uint64_t position_count = 0;
};

bool emitter_fits_prx_window_shape(uint64_t doc_count, uint64_t position_count,
                                   const format::PrxWindowLimits& limits) {
    return doc_count <= limits.max_docs && position_count <= limits.max_positions;
}

// Five bytes is the maximum encoded width of every uint32 field in the raw
// payload. This gate is used only after an exact build requests a split.
bool conservatively_fits_prx_window(uint64_t doc_count, uint64_t position_count,
                                    const format::PrxWindowLimits& limits) {
    return emitter_fits_prx_window_shape(doc_count, position_count, limits) &&
           1 + doc_count + position_count <= limits.max_uncomp_bytes / 5;
}

Status build_prelude(const std::vector<format::WindowMeta>& windows, bool has_prx,
                     std::vector<uint8_t>* output) {
    format::FrqPreludeColumns columns;
    columns.has_prx = has_prx;
    columns.group_size = kPreludeGroupSize;
    columns.windows = windows;
    ByteSink sink;
    RETURN_IF_ERROR(format::build_frq_prelude(columns, &sink));
    *output = sink.take();
    return Status::OK();
}

Status checked_add(uint64_t increment, uint64_t* value) {
    if (increment > std::numeric_limits<uint64_t>::max() - *value) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "window emitter: term frequency overflow");
    }
    *value += increment;
    return Status::OK();
}

} // namespace

class WindowEmitter::Impl {
public:
    explicit Impl(WindowEmitterOptions options)
            : options_(options),
              dd_stager_(std::numeric_limits<uint64_t>::max(), "term_dd", options.memory_reporter) {
        if (options_.posting_out != nullptr &&
            options_.posting_out->bytes_written() >= options_.posting_region_offset) {
            prx_off_ = options_.posting_out->bytes_written() - options_.posting_region_offset;
            posting_offset_valid_ = true;
        }
    }

    Status emit_window(const PostingRunView& run) {
        if (phase_ != Phase::kActive) {
            return phase_error("emit_window");
        }
        Status status = emit_window_impl(run);
        if (!status.ok()) {
            phase_ = Phase::kFailed;
        }
        return status;
    }

    Status finish_term(format::DictEntry* entry, TermAggregateStats* stats) {
        if (phase_ != Phase::kActive) {
            return phase_error("finish_term");
        }
        if (entry == nullptr || stats == nullptr) {
            phase_ = Phase::kFailed;
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "window emitter: null finish output");
        }
        if (windows_.empty()) {
            phase_ = Phase::kFailed;
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "window emitter: cannot finish an empty term");
        }
        Status status = finish_term_impl(entry);
        if (!status.ok()) {
            phase_ = Phase::kFailed;
            return status;
        }
        *stats = stats_;
        phase_ = Phase::kFinished;
#ifdef BE_TEST
        finished_term_counter().fetch_add(1, std::memory_order_relaxed);
#endif
        return Status::OK();
    }

private:
    enum class Phase : uint8_t { kActive, kFinished, kFailed };

    Status phase_error(std::string_view operation) const {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "window emitter: {} after {}", operation,
                phase_ == Phase::kFailed ? "failure" : "finish");
    }

    Status posting_size(uint64_t* size) const {
        if (options_.posting_out == nullptr) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "window emitter: null posting sink");
        }
        if (!posting_offset_valid_ ||
            options_.posting_out->bytes_written() < options_.posting_region_offset) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "window emitter: invalid posting region offset");
        }
        *size = options_.posting_out->bytes_written() - options_.posting_region_offset;
        return Status::OK();
    }

    Status validate_run(const PostingRunView& run) const {
        if (options_.posting_out == nullptr) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "window emitter: null posting sink");
        }
        if (run.docids.empty()) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "window emitter: empty posting window");
        }
        if ((!run.freqs.empty() || options_.has_prx) && run.freqs.size() != run.docids.size()) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "window emitter: frequency shape must match documents");
        }
        if (options_.has_prx) {
            if (run.position_offsets.size() != run.docids.size() + 1) {
                return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                        "window emitter: position offsets must have docs plus one entries");
            }
            if (run.position_offsets.front() > run.position_offsets.back() ||
                run.position_offsets.back() - run.position_offsets.front() !=
                        run.positions_flat.size()) {
                return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                        "window emitter: position offsets differ from the position run");
            }
        } else if (!run.position_offsets.empty() || !run.positions_flat.empty()) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "window emitter: positions require a PRX term");
        }
        if (last_input_docid_.has_value() && run.docids.front() <= *last_input_docid_) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "window emitter: posting windows must be strictly ordered");
        }
        return Status::OK();
    }

    Status accumulate_constant_stats(const PostingRunView& run) {
        if (run.docids.size() > std::numeric_limits<uint32_t>::max() - stats_.df) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "window emitter: document frequency overflow");
        }
        stats_.df += static_cast<uint32_t>(run.docids.size());
        // 没有 freqs 的 docs-only 输入按每 doc 一次计入总词频；带 freqs 的输入在
        // emit_window_impl 的逐 doc 循环里累加。
        if (run.freqs.empty()) {
            return checked_add(run.docids.size(), &stats_.total_freq);
        }
        return Status::OK();
    }

    uint64_t position_count(const PostingRunView& run, size_t begin, size_t count) const {
        if (!options_.has_prx) {
            return 0;
        }
        return run.position_offsets[begin + count] - run.position_offsets[begin];
    }

    Status emit_window_impl(const PostingRunView& run) {
        RETURN_IF_ERROR(validate_run(run));
        RETURN_IF_ERROR(accumulate_constant_stats(run));

        const bool accumulate_frequencies = !run.freqs.empty();
        if (!options_.has_prx && !accumulate_frequencies) {
            RETURN_IF_ERROR(emit_planned(run, make_plan(run, 0, run.docids.size())));
            last_input_docid_ = run.docids.back();
            return Status::OK();
        }

        size_t window_begin = 0;
        for (size_t doc = 0; doc < run.docids.size(); ++doc) {
            const uint64_t document_positions = options_.has_prx ? position_count(run, doc, 1) : 0;
            if (options_.has_prx && (run.position_offsets[doc + 1] < run.position_offsets[doc] ||
                                     document_positions != run.freqs[doc])) {
                return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                        "window emitter: position offsets must match frequencies");
            }
            if (options_.has_prx && document_positions > options_.prx_window_limits.max_positions) {
                return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                        "window emitter: one document exceeds the PRX position limit");
            }
            if (accumulate_frequencies) {
                RETURN_IF_ERROR(checked_add(run.freqs[doc], &stats_.total_freq));
            }
            const uint64_t candidate_docs = doc - window_begin + 1;
            const uint64_t candidate_positions = position_count(run, window_begin, candidate_docs);
            if (doc != window_begin && options_.has_prx &&
                !emitter_fits_prx_window_shape(candidate_docs, candidate_positions,
                                               options_.prx_window_limits)) {
                RETURN_IF_ERROR(emit_planned(run, make_plan(run, window_begin, doc - window_begin)));
                window_begin = doc;
            }
        }
        RETURN_IF_ERROR(
                emit_planned(run, make_plan(run, window_begin, run.docids.size() - window_begin)));
        last_input_docid_ = run.docids.back();
        return Status::OK();
    }

    PostingWindowPlan make_plan(const PostingRunView& run, size_t begin, size_t count) const {
        return {
                .doc_begin = begin,
                .doc_count = count,
                .position_begin = options_.has_prx ? run.position_offsets[begin] -
                                                             run.position_offsets.front()
                                                   : uint64_t {0},
                .position_count = position_count(run, begin, count),
        };
    }

    Status emit_planned(const PostingRunView& run, const PostingWindowPlan& plan) {
        format::PrxWindowBuildOutcome outcome = format::PrxWindowBuildOutcome::kBuilt;
        RETURN_IF_ERROR(emit_physical_window(run, plan, &outcome));
        if (outcome == format::PrxWindowBuildOutcome::kBuilt) {
            return Status::OK();
        }

        std::vector<PostingWindowPlan> recut;
        recut_window(run, plan, &recut);
        for (const PostingWindowPlan& subplan : recut) {
            outcome = format::PrxWindowBuildOutcome::kBuilt;
            RETURN_IF_ERROR(emit_physical_window(run, subplan, &outcome));
            if (outcome == format::PrxWindowBuildOutcome::kNeedsSplit) {
                return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                        "window emitter: one document exceeds the PRX byte limit");
            }
        }
        return Status::OK();
    }

    void recut_window(const PostingRunView& run, const PostingWindowPlan& input,
                      std::vector<PostingWindowPlan>* output) const {
        size_t window_begin = input.doc_begin;
        const size_t input_end = input.doc_begin + input.doc_count;
        for (size_t doc = input.doc_begin; doc < input_end; ++doc) {
            const uint64_t candidate_docs = doc - window_begin + 1;
            const uint64_t candidate_positions = position_count(run, window_begin, candidate_docs);
            if (doc != window_begin &&
                !conservatively_fits_prx_window(candidate_docs, candidate_positions,
                                                options_.prx_window_limits)) {
                output->push_back(make_plan(run, window_begin, doc - window_begin));
                window_begin = doc;
            }
        }
        output->push_back(make_plan(run, window_begin, input_end - window_begin));
    }

    Status emit_physical_window(const PostingRunView& run, const PostingWindowPlan& plan,
                                format::PrxWindowBuildOutcome* outcome) {
        const auto docs = run.docids.subspan(plan.doc_begin, plan.doc_count);
        const auto freqs = run.freqs.empty() ? std::span<const uint32_t> {}
                                             : run.freqs.subspan(plan.doc_begin, plan.doc_count);
        format::WindowMeta window;
        window.last_docid = docs.back();
        window.win_base = window_base_;
        window.doc_count = static_cast<uint32_t>(docs.size());

        if (options_.has_prx) {
            const auto positions =
                    run.positions_flat.subspan(static_cast<size_t>(plan.position_begin),
                                               static_cast<size_t>(plan.position_count));
            prx_scratch_.clear();
            RETURN_IF_ERROR(format::try_build_prx_window_flat(
                    positions, freqs, -options_.prx_zstd_level, options_.prx_window_limits,
                    &prx_scratch_, outcome));
            if (*outcome == format::PrxWindowBuildOutcome::kNeedsSplit) {
                return Status::OK();
            }
            window.prx_off = prx_total_len_;
            window.prx_len = prx_scratch_.size();
            RETURN_IF_ERROR(options_.posting_out->append(prx_scratch_.view()));
            prx_total_len_ += window.prx_len;
        } else {
            *outcome = format::PrxWindowBuildOutcome::kBuilt;
        }

        ByteSink dd_sink;
        format::FrqRegionMeta dd_meta;
        window.dd_off = dd_stager_.size();
        RETURN_IF_ERROR(format::build_dd_region(docs, window_base_, kEmitterRawFrqRegion, &dd_sink,
                                                &dd_meta));
        window.dd_zstd = dd_meta.zstd;
        window.dd_disk_len = dd_meta.disk_len;
        window.dd_uncomp_len = dd_meta.uncomp_len;
        window.crc_dd = dd_meta.crc;
        RETURN_IF_ERROR(dd_stager_.append_move(dd_sink.take()));

        windows_.push_back(window);
        window_base_ = window.last_docid;
#ifdef BE_TEST
        physical_window_counter().fetch_add(1, std::memory_order_relaxed);
#endif
        return Status::OK();
    }

    Status finish_term_impl(format::DictEntry* entry) {
        std::vector<uint8_t> prelude;
        RETURN_IF_ERROR(build_prelude(windows_, options_.has_prx, &prelude));
        entry->kind = format::DictEntryKind::kPodRef;
        entry->enc = format::DictEntryEnc::kWindowed;
        entry->has_sb = true;
        entry->prelude_len = prelude.size();

        uint64_t frq_off = 0;
        RETURN_IF_ERROR(posting_size(&frq_off));
        RETURN_IF_ERROR(options_.posting_out->append(Slice(prelude)));
        RETURN_IF_ERROR(dd_stager_.seal());
        RETURN_IF_ERROR(dd_stager_.stream_into_and_release(options_.posting_out));
        entry->frq_off_delta = frq_off - options_.frq_base;
        uint64_t end = 0;
        RETURN_IF_ERROR(posting_size(&end));
        entry->frq_len = end - frq_off;
        if (options_.has_prx) {
            entry->prx_off_delta = prx_off_ - options_.prx_base;
            entry->prx_len = prx_total_len_;
        }
        return Status::OK();
    }

#ifdef BE_TEST
    static std::atomic<uint64_t>& finished_term_counter();
    static std::atomic<uint64_t>& physical_window_counter();
#endif

    WindowEmitterOptions options_;
    SpillableByteBuffer dd_stager_;
    std::vector<format::WindowMeta> windows_;
    ByteSink prx_scratch_;
    TermAggregateStats stats_;
    std::optional<uint32_t> last_input_docid_;
    uint64_t prx_off_ = 0;
    uint64_t prx_total_len_ = 0;
    uint64_t window_base_ = 0;
    bool posting_offset_valid_ = false;
    Phase phase_ = Phase::kActive;
};

#ifdef BE_TEST
namespace {
std::atomic<uint64_t>& emitter_finished_term_counter() {
    static std::atomic<uint64_t> counter {0};
    return counter;
}
std::atomic<uint64_t>& emitter_physical_window_counter() {
    static std::atomic<uint64_t> counter {0};
    return counter;
}
} // namespace

std::atomic<uint64_t>& WindowEmitter::Impl::finished_term_counter() {
    return emitter_finished_term_counter();
}

std::atomic<uint64_t>& WindowEmitter::Impl::physical_window_counter() {
    return emitter_physical_window_counter();
}
#endif

WindowEmitter::WindowEmitter(WindowEmitterOptions options)
        : impl_(std::make_unique<Impl>(options)) {}

WindowEmitter::~WindowEmitter() = default;

Status WindowEmitter::emit_window(const PostingRunView& window) {
    return impl_->emit_window(window);
}

Status WindowEmitter::finish_term(format::DictEntry* entry, TermAggregateStats* stats) {
    return impl_->finish_term(entry, stats);
}

namespace testing {

uint64_t window_emitter_finished_terms() {
#ifdef BE_TEST
    return emitter_finished_term_counter().load(std::memory_order_relaxed);
#else
    return 0;
#endif
}

uint64_t window_emitter_physical_windows() {
#ifdef BE_TEST
    return emitter_physical_window_counter().load(std::memory_order_relaxed);
#else
    return 0;
#endif
}

void reset_window_emitter_counters() {
#ifdef BE_TEST
    emitter_finished_term_counter().store(0, std::memory_order_relaxed);
    emitter_physical_window_counter().store(0, std::memory_order_relaxed);
#endif
}

} // namespace testing

} // namespace doris::snii::writer
