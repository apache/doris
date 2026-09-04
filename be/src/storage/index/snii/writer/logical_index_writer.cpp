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

#include "storage/index/snii/writer/logical_index_writer.h"

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <memory>
#include <span>
#include <utility>

#include "storage/index/inverted/common_grams/common_grams_key_codec.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/crc32c.h"
#include "storage/index/snii/encoding/varint.h"
#include "storage/index/snii/encoding/zstd_codec.h"
#include "storage/index/snii/format/bsbf.h"
#include "storage/index/snii/format/dict_block.h"
#include "storage/index/snii/format/dict_block_directory.h"
#include "storage/index/snii/format/frq_pod.h"
#include "storage/index/snii/format/norms_pod.h"
#include "storage/index/snii/format/null_bitmap.h"
#include "storage/index/snii/format/prx_pod.h"
#include "storage/index/snii/writer/posting_window_emitter.h"

namespace doris::snii::writer {

using format::BlockRef;
using format::DictBlockBuilder;
using format::DictBlockDirectoryBuilder;
using format::DictEntry;
using format::DictEntryEnc;
using format::DictEntryKind;
using format::SampledTermIndexBuilder;
using format::SectionRefs;
using segment_v2::inverted_index::CG_V1_MARKER;
using segment_v2::inverted_index::CommonGramsCoverage;
using segment_v2::inverted_index::ScoringCoverage;
using segment_v2::inverted_index::validate_common_grams_segment_metadata;

namespace {

// Target false-positive probability for the block-split bloom XFilter. Sizes
// the filter via Parquet OptimalNumOfBytes; L0 keeps the probe in memory and L1
// keeps the per-query cost at one 32-byte block.
constexpr double kBsbfFpp = 0.01;
// Force-raw level for .frq dd/freq regions. Their plaintext is PFOR-bit-packed
// doc-deltas/freqs -- already high-entropy, so zstd shrinks ~30 MB of input by
// <0.1 MiB while burning ~0.4s CPU (and an extra crc pass over the compressed
// bytes) at 5M. We force raw here and keep zstd only on .prx (which compresses
// ~77%). Output stays self-describing: the region meta records zstd=false.
constexpr int kRawFrqRegion = 0;
// zstd level for whole-DICT-block compression comes from
// SniiIndexInput::dict_block_zstd_level (default 3: ~40% on the 64KiB
// front-coded blocks at ~120 MiB/s encode / ~600 MiB/s decode; higher levels
// trade import CPU for size, decode speed unchanged). G16-h made it (and the
// .prx auto level) caller-tunable.

using format::FrqRegionMeta;

// Fused single-pass term-level freq statistics: total_freq (running sum) and
// max_freq (running max) in ONE scan, reused by validate_term (has_prx
// position-count budget), stats_.sum_total_term_freq, and the DictEntry
// ttf_delta/max_freq. Byte-identical to the former separate SumOf/MaxOf scans:
// same left-to-right accumulation order and the same max init of 0, so a freq of
// 0 never lowers the max. Complete CommonGrams entries bypass this helper:
// their ttf is the already-known PRX position count and max_freq is not stored.
FreqStats fuse_freq_stats(const std::vector<uint32_t>& freqs) {
#ifdef BE_TEST
    testing::note_term_freq_scan();
#endif
    FreqStats fs;
    for (uint32_t f : freqs) {
        fs.total_freq += f;
        fs.max_freq = std::max(f, fs.max_freq);
    }
    return fs;
}

// Default window doc count by df: high-df windowed terms combine kFrqBaseUnit
// units into larger (kAdaptiveWindowDocs) windows. PRX limits may subsequently
// recut one of these default windows at document boundaries.
uint32_t adaptive_window_docs(uint32_t df) {
    return df >= format::kAdaptiveWindowDfThreshold ? format::kAdaptiveWindowDocs
                                                    : format::kFrqBaseUnit;
}

bool fits_prx_window_shape(uint64_t doc_count, uint64_t position_count,
                           const format::PrxWindowLimits& limits) {
    return doc_count <= limits.max_docs && position_count <= limits.max_positions;
}

} // namespace

// The only encoder for TermPostingSource input. It borrows the writer's reusable
// posting buffer, streams PRX windows directly to the final sink, and stages grouped DD
// and frequency regions without retaining the complete term.
class StreamingTermEncoder {
public:
    StreamingTermEncoder(LogicalIndexWriter* writer, StreamedTermPostings* postings,
                         bool declared_common_gram, bool term_has_freq, bool term_has_prx,
                         TermPostingBuffer* buffer, uint64_t frq_base, uint64_t prx_base)
            : writer_(writer),
              postings_(postings),
              declared_common_gram_(declared_common_gram),
              term_has_freq_(term_has_freq),
              term_has_prx_(term_has_prx),
              buffer_(buffer),
              frq_base_(frq_base),
              prx_base_(prx_base),
              emitter_(WindowEmitterOptions {
                      .posting_out = writer->posting_out_,
                      .posting_region_offset = writer->posting_off0_,
                      .frq_base = frq_base,
                      .prx_base = prx_base,
                      .encoded_norms = writer->encoded_norms_,
                      .has_freq = term_has_freq,
                      .has_prx = term_has_prx,
                      .prx_zstd_level = writer->prx_zstd_level_,
                      .prx_window_limits = writer->prx_window_limits_,
                      .term_frequency_source =
                              declared_common_gram ? (postings->retain_positions
                                                              ? TermFrequencySource::kPositions
                                                              : TermFrequencySource::kDocuments)
                                                   : TermFrequencySource::kFrequenciesOrDocuments,
                      .memory_reporter = writer->memory_reporter_,
              }) {
        DCHECK(buffer_ != nullptr);
        DCHECK(buffer_->empty());
    }

    ~StreamingTermEncoder() {
        buffer_->clear_reuse_and_release_excess(format::kAdaptiveWindowDfThreshold);
    }

    Status encode(DictEntry* entry, FreqStats* stats) {
        if (postings_->source == nullptr) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "logical_index: streamed term has a null posting source");
        }
        if (writer_->has_prx_ && !postings_->retain_positions && !declared_common_gram_) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "logical_index: only a declared CommonGrams term may omit positions");
        }
#ifdef BE_TEST
        if (!declared_common_gram_) {
            testing::note_term_freq_scan();
        }
#endif
        bool exhausted = false;
        RETURN_IF_ERROR(fill(format::kAdaptiveWindowDfThreshold, &exhausted));
        if (exhausted && buffer_->document_count() < format::kAdaptiveWindowDfThreshold) {
            entry->term = std::move(postings_->term);
            entry->df = total_docs_;
            entry->ttf_delta = stats_.total_freq;
            entry->max_freq = stats_.max_freq;
            RETURN_IF_ERROR(encode_small(entry));
            *stats = stats_;
            return Status::OK();
        }

        if (!buffer_->empty()) {
            RETURN_IF_ERROR(encode_windowed_buffer(format::kAdaptiveWindowDocs));
        }
        while (!exhausted) {
            RETURN_IF_ERROR(fill(format::kAdaptiveWindowDocs, &exhausted));
            if (!buffer_->empty()) {
                RETURN_IF_ERROR(encode_windowed_buffer(format::kAdaptiveWindowDocs));
            }
        }

        entry->term = std::move(postings_->term);
        entry->df = total_docs_;
        entry->ttf_delta = stats_.total_freq;
        entry->max_freq = stats_.max_freq;
        RETURN_IF_ERROR(finish_windowed(entry));
        *stats = stats_;
        return Status::OK();
    }

private:
    Status fill(uint32_t target_docs, bool* exhausted) {
        buffer_->clear_reuse();
        position_offsets_.clear();
        *exhausted = false;
        RETURN_IF_ERROR(postings_->source->fill(target_docs, buffer_, exhausted));
        const size_t count = buffer_->document_count();
        if (count > target_docs) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "logical_index: posting source exceeded target_docs");
        }
        if (!*exhausted && count != target_docs) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "logical_index: posting source returned a short non-terminal fill");
        }
        if (count == 0 && !*exhausted) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "logical_index: posting source returned empty before EOF");
        }
        return validate_and_accumulate();
    }

    Status validate_and_accumulate() {
        const auto docids = buffer_->docids();
        const auto freqs = buffer_->freqs();
        const auto positions = buffer_->positions_flat();
        if (postings_->retain_positions && freqs.size() != docids.size()) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "logical_index: positioned source must provide one freq per docid");
        }
        if (!postings_->retain_positions && !freqs.empty() && freqs.size() != docids.size()) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "logical_index: docs-only source freqs must be empty or parallel");
        }
        if (postings_->retain_positions &&
            positions.size() > std::numeric_limits<uint32_t>::max()) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "logical_index: one source fill exceeds uint32 position offsets");
        }
        if (postings_->retain_positions) {
            position_offsets_.resize(freqs.size() + 1);
        }
        // One fused pass serves both the positions-count validation and the
        // frequency statistics below. fill() has already capped the buffer at
        // target_docs (at most the adaptive window sizes), so a uint64 sum of
        // uint32 frequencies cannot overflow within one fill; the per-term
        // accumulation below keeps its overflow check.
        uint64_t fill_freq_sum = 0;
        uint32_t fill_max_freq = 0;
        for (size_t doc = 0; doc < freqs.size(); ++doc) {
            const uint32_t freq = freqs[doc];
            fill_freq_sum += freq;
            fill_max_freq = std::max(fill_max_freq, freq);
            if (postings_->retain_positions) {
                position_offsets_[doc + 1] = static_cast<uint32_t>(fill_freq_sum);
            }
        }
        if (postings_->retain_positions) {
            if (fill_freq_sum != positions.size()) {
                return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                        "logical_index: source positions count must equal sum(freqs)");
            }
        } else {
            if (!positions.empty()) {
                return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                        "logical_index: docs-only source must not provide positions");
            }
        }

        for (uint32_t docid : docids) {
            if (last_docid_.has_value() && docid <= *last_docid_) {
                return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                        "logical_index: source docids must be strictly ascending");
            }
            if (docid >= writer_->doc_count_) {
                return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                        "logical_index: source docid must be less than doc_count");
            }
            last_docid_ = docid;
        }
        if (docids.size() > std::numeric_limits<uint32_t>::max() - total_docs_) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "logical_index: source document count exceeds uint32");
        }
        total_docs_ += static_cast<uint32_t>(docids.size());

        if (declared_common_gram_) {
            const uint64_t increment =
                    postings_->retain_positions ? positions.size() : docids.size();
            if (increment > std::numeric_limits<uint64_t>::max() - stats_.total_freq) {
                return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                        "logical_index: source total frequency overflow");
            }
            stats_.total_freq += increment;
        } else if (freqs.empty()) {
            if (docids.size() > std::numeric_limits<uint64_t>::max() - stats_.total_freq) {
                return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                        "logical_index: source total frequency overflow");
            }
            stats_.total_freq += docids.size();
        } else {
            if (fill_freq_sum > std::numeric_limits<uint64_t>::max() - stats_.total_freq) {
                return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                        "logical_index: source total frequency overflow");
            }
            stats_.total_freq += fill_freq_sum;
            stats_.max_freq = std::max(stats_.max_freq, fill_max_freq);
        }
        return Status::OK();
    }

    Status encode_small(DictEntry* entry) {
        if (total_docs_ >= format::kSlimDfThreshold ||
            (term_has_prx_ &&
             !fits_prx_window_shape(total_docs_, stats_.total_freq, writer_->prx_window_limits_))) {
            RETURN_IF_ERROR(encode_windowed_buffer(adaptive_window_docs(total_docs_)));
            return finish_windowed(entry);
        }

        std::vector<uint8_t> prx_window;
        if (term_has_prx_) {
            ByteSink sink;
            format::PrxWindowBuildOutcome outcome = format::PrxWindowBuildOutcome::kBuilt;
            RETURN_IF_ERROR(format::try_build_prx_window_flat(
                    buffer_->positions_flat(), buffer_->freqs(), -writer_->prx_zstd_level_,
                    writer_->prx_window_limits_, &sink, &outcome));
            if (outcome == format::PrxWindowBuildOutcome::kNeedsSplit) {
                RETURN_IF_ERROR(encode_windowed_buffer(format::kFrqBaseUnit));
                return finish_windowed(entry);
            }
            prx_window = sink.take();
        }

        ByteSink frq_sink;
        FrqRegionMeta dd_meta;
        FrqRegionMeta freq_meta {};
        RETURN_IF_ERROR(format::build_dd_region(buffer_->docids(), /*win_base=*/0, kRawFrqRegion,
                                                &frq_sink, &dd_meta));
        if (term_has_freq_) {
            RETURN_IF_ERROR(format::build_freq_region(buffer_->freqs(), kRawFrqRegion, &frq_sink,
                                                      &freq_meta));
        }
        std::vector<uint8_t> frq_window = frq_sink.take();
        entry->enc = DictEntryEnc::kSlim;
        entry->dd_meta = dd_meta;
        entry->freq_meta = freq_meta;
        if (frq_window.size() <= format::kDefaultInlineThreshold) {
            entry->kind = DictEntryKind::kInline;
            entry->inline_dd_disk_len = dd_meta.disk_len;
            entry->frq_bytes = std::move(frq_window);
            if (term_has_prx_) entry->prx_bytes = std::move(prx_window);
            return Status::OK();
        }

        entry->kind = DictEntryKind::kPodRef;
        entry->frq_docs_len = dd_meta.disk_len;
        if (term_has_prx_) {
            const uint64_t prx_off = writer_->posting_size();
            RETURN_IF_ERROR(writer_->posting_out_->append(Slice(prx_window)));
            entry->prx_off_delta = prx_off - prx_base_;
            entry->prx_len = writer_->posting_size() - prx_off;
        }
        const uint64_t frq_off = writer_->posting_size();
        RETURN_IF_ERROR(writer_->posting_out_->append(Slice(frq_window)));
        entry->frq_off_delta = frq_off - frq_base_;
        entry->frq_len = writer_->posting_size() - frq_off;
        return Status::OK();
    }

    Status encode_windowed_buffer(uint32_t unit) {
        DCHECK_GT(unit, 0);
        for (size_t begin = 0; begin < buffer_->document_count(); begin += unit) {
            const size_t count =
                    std::min(buffer_->document_count() - begin, static_cast<size_t>(unit));
            const auto offsets =
                    term_has_prx_
                            ? std::span<const uint32_t>(position_offsets_).subspan(begin, count + 1)
                            : std::span<const uint32_t> {};
            const auto positions =
                    term_has_prx_ ? buffer_->positions_flat().subspan(
                                            offsets.front(),
                                            static_cast<size_t>(offsets.back() - offsets.front()))
                                  : std::span<const uint32_t> {};
            RETURN_IF_ERROR(emitter_.emit_window(PostingRunView {
                    .docids = buffer_->docids().subspan(begin, count),
                    .freqs = buffer_->freqs().empty() ? std::span<const uint32_t> {}
                                                      : buffer_->freqs().subspan(begin, count),
                    .position_offsets = offsets,
                    .positions_flat = positions,
            }));
        }
        return Status::OK();
    }

    Status finish_windowed(DictEntry* entry) {
        TermAggregateStats emitted_stats;
        RETURN_IF_ERROR(emitter_.finish_term(entry, &emitted_stats));
        DCHECK_EQ(emitted_stats.df, total_docs_);
        DCHECK_EQ(emitted_stats.total_freq, stats_.total_freq);
        DCHECK_EQ(emitted_stats.max_freq, stats_.max_freq);
        return Status::OK();
    }

    LogicalIndexWriter* writer_;
    StreamedTermPostings* postings_;
    bool declared_common_gram_ = false;
    bool term_has_freq_ = false;
    bool term_has_prx_ = false;
    TermPostingBuffer* buffer_ = nullptr;
    uint64_t frq_base_ = 0;
    uint64_t prx_base_ = 0;
    WindowEmitter emitter_;
    std::vector<uint32_t> position_offsets_;
    FreqStats stats_;
    std::optional<uint32_t> last_docid_;
    uint32_t total_docs_ = 0;
};

namespace testing {
#ifdef BE_TEST
namespace {
// Function-local-static op-count seam backing term_freq_scans(). One atomic,
// relaxed: the writer build path is single-threaded, so only the COUNT matters,
// not ordering (the atomic keeps it race-clean if a test ever parallelizes).
std::atomic<uint64_t>& term_freq_scan_counter() {
    static std::atomic<uint64_t> counter {0};
    return counter;
}
} // namespace
#endif

void note_term_freq_scan() {
#ifdef BE_TEST
    term_freq_scan_counter().fetch_add(1, std::memory_order_relaxed);
#endif
}
uint64_t term_freq_scans() {
#ifdef BE_TEST
    return term_freq_scan_counter().load(std::memory_order_relaxed);
#else
    return 0;
#endif
}
void reset_term_freq_scans() {
#ifdef BE_TEST
    term_freq_scan_counter().store(0, std::memory_order_relaxed);
#endif
}

// Forwards to the real fused helper so pure boundary tests exercise production
// code (not a test-local re-implementation).
FreqStats fuse_freq_stats_for_test(const std::vector<uint32_t>& freqs) {
    return fuse_freq_stats(freqs);
}
} // namespace testing

LogicalIndexWriter::LogicalIndexWriter(const SniiIndexInput& in)
        : LogicalIndexWriter(in, TrackedNullDocids(std::vector<uint32_t>(in.null_docids))) {}

LogicalIndexWriter::LogicalIndexWriter(const SniiIndexInput& in, TrackedNullDocids null_docids)
        : index_id_(in.index_id),
          index_suffix_(in.index_suffix),
          index_config_(in.config),
          tier_(format::tier_of(in.config)),
          has_prx_(format::has_positions(in.config)),
          // G16-c: the caller can drop freq layout entirely (in.write_freq ==
          // false) on a freq-capable tier -- see SniiIndexInput::write_freq.
          has_freq_(format::tier_of(in.config) >= format::IndexTier::kT2 && in.write_freq),
          has_norms_(format::has_scoring(in.config)),
          doc_count_(in.doc_count),
          null_docids_(std::move(null_docids)),
          terms_(in.terms),
          term_source_(in.term_source),
          encoded_norms_(in.encoded_norms),
          common_grams_metadata_(in.common_grams_metadata),
          common_grams_posting_policy_(in.common_grams_posting_policy),
          target_dict_block_bytes_(in.target_dict_block_bytes != 0
                                           ? in.target_dict_block_bytes
                                           : format::kDefaultTargetDictBlockBytes),
          dict_block_zstd_level_(in.dict_block_zstd_level),
          prx_zstd_level_(in.prx_zstd_level),
          prx_window_limits_(in.prx_window_limits),
          memory_reporter_(in.mem_reporter),
          dict_buf_(in.dict_resident_cap_bytes, "dict", in.mem_reporter),
          norms_section_reservation_(in.mem_reporter == nullptr
                                             ? MemoryReporter::Reservation()
                                             : in.mem_reporter->make_reservation()),
          null_bitmap_section_reservation_(in.mem_reporter == nullptr
                                                   ? MemoryReporter::Reservation()
                                                   : in.mem_reporter->make_reservation()),
          term_hashes_reservation_(in.mem_reporter == nullptr
                                           ? MemoryReporter::Reservation()
                                           : in.mem_reporter->make_reservation()),
          bsbf_bytes_reservation_(in.mem_reporter == nullptr
                                          ? MemoryReporter::Reservation()
                                          : in.mem_reporter->make_reservation()) {}

Status LogicalIndexWriter::reserve_term_hash_for_append() {
    if (memory_reporter_ == nullptr || term_hashes_.size() < term_hashes_.capacity()) {
        return Status::OK();
    }
    size_t target_capacity = term_hashes_.capacity() == 0 ? 1 : term_hashes_.capacity() * 2;
    if (target_capacity < term_hashes_.capacity() ||
        target_capacity > std::numeric_limits<uint64_t>::max() / sizeof(uint64_t)) {
        return Status::Error<ErrorCode::MEM_LIMIT_EXCEEDED, false>(
                "logical_index: term hash capacity overflow");
    }
    MemoryReporter::Reservation replacement;
    RETURN_IF_ERROR(term_hashes_reservation_.prepare_replacement(target_capacity * sizeof(uint64_t),
                                                                 &replacement));
    term_hashes_.reserve(target_capacity);
    DCHECK_EQ(term_hashes_.capacity(), target_capacity);
    term_hashes_reservation_ = std::move(replacement);
    return Status::OK();
}

// Serializes the current open block, zstd-compresses it (the dict region is the
// single largest section -- term keys + entry meta + inline postings -- and the
// 64KiB blocks compress ~40%), streams the compressed bytes into the dict
// scratch file, and records a directory entry. The block-level crc32c
// (rec.checksum) covers the UNCOMPRESSED bytes, so DictBlockReader::open
// verifies integrity after the reader decompresses. A compressed block also
// shrinks the bytes a term lookup fetches from S3 -- aligning with the
// read-byte thesis. If zstd does not shrink a (tiny) block, it is stored raw so
// a lookup never pays a pointless decompress.
Status LogicalIndexWriter::flush_block(DictBlockBuilder* block, std::string first_term) {
    std::vector<uint8_t> plain_bytes = block->finish_owned();
    const Slice plain(plain_bytes);
    BlockRecord rec;
    rec.rel_offset = dict_buf_.size();
    rec.n_entries = block->n_entries();
    rec.checksum = crc32c(plain); // crc over UNCOMPRESSED block bytes
    rec.first_term = std::move(first_term);

    std::vector<uint8_t> comp;
    Status zs = zstd_compress(plain, dict_block_zstd_level_, &comp);
    if (zs.ok() && comp.size() < plain.size()) {
        rec.flags = format::block_ref_flags::kZstd;
        rec.uncomp_len = static_cast<uint64_t>(plain.size());
        rec.length = static_cast<uint64_t>(comp.size());
        RETURN_IF_ERROR(dict_buf_.append_move(std::move(comp)));
    } else {
        rec.flags = 0;
        rec.uncomp_len = 0;
        rec.length = static_cast<uint64_t>(plain.size());
        RETURN_IF_ERROR(dict_buf_.append_move(std::move(plain_bytes)));
    }
    blocks_.push_back(std::move(rec));
    return Status::OK();
}

// Running state for the in-flight DICT block while terms stream past.
struct LogicalIndexWriter::BlockState {
    explicit BlockState(MemoryReporter* memory_reporter) : transfer_buffer(memory_reporter) {}

    std::unique_ptr<DictBlockBuilder> block;
    std::string block_first_term;
    uint64_t frq_base = 0;
    uint64_t prx_base = 0;
    bool term_stats = true;
    TermPostingBuffer transfer_buffer;
};

// Out-of-line so unique_ptr<BlockState> sees the complete type (see header).
LogicalIndexWriter::~LogicalIndexWriter() = default;

Status LogicalIndexWriter::process_term(StreamedTermPostings& tp, BlockState* st) {
    const bool is_declared_common_gram =
            common_grams_metadata_.has_value() && tp.term.starts_with(CG_V1_MARKER) &&
            (common_grams_metadata_->common_grams_coverage == CommonGramsCoverage::kComplete ||
             common_grams_posting_policy_ == format::CommonGramsPostingPolicy::kHybridV1);
    const bool term_has_prx = has_prx_ && tp.retain_positions;
    const bool term_has_freq = has_freq_ && !is_declared_common_gram;

    if (st->block && st->term_stats != term_has_freq) {
        RETURN_IF_ERROR(flush_block(st->block.get(), st->block_first_term));
        st->block.reset();
    }
    if (!st->block) {
        const uint64_t base = posting_size();
        st->frq_base = base;
        st->prx_base = base;
        st->term_stats = term_has_freq;
        st->block = std::make_unique<DictBlockBuilder>(tier_, has_prx_, st->frq_base, st->prx_base,
                                                       /*anchor_interval=*/16,
                                                       /*term_stats=*/term_has_freq);
        st->block_first_term = tp.term;
    }

    RETURN_IF_ERROR(reserve_term_hash_for_append());
    const uint64_t term_hash = format::bsbf_hash(tp.term);
    DictEntry entry;
    FreqStats stats;
    StreamingTermEncoder encoder(this, &tp, is_declared_common_gram, term_has_freq, term_has_prx,
                                 &st->transfer_buffer, st->frq_base, st->prx_base);
    RETURN_IF_ERROR(encoder.encode(&entry, &stats));

    term_hashes_.push_back(term_hash);
    ++term_count_;
    stats_.sum_total_term_freq += stats.total_freq;
    st->block->add_entry(std::move(entry));
    if (st->block->estimated_bytes() >= target_dict_block_bytes_) {
        RETURN_IF_ERROR(flush_block(st->block.get(), st->block_first_term));
        st->block.reset();
    }
    return Status::OK();
}

Status LogicalIndexWriter::build_blocks() {
    BlockState st(memory_reporter_);
    if (term_source_ != nullptr) {
        RETURN_IF_ERROR(term_source_->for_each_term_sorted(
                [&](StreamedTermPostings&& tp) { return process_term(tp, &st); }));
    } else {
        for (const auto& tp : terms_) {
            SpanTermPostingSource source(tp.docids, tp.freqs, tp.positions_flat);
            StreamedTermPostings streamed {
                    .term = tp.term, .retain_positions = tp.retain_positions, .source = &source};
            RETURN_IF_ERROR(process_term(streamed, &st));
        }
    }
    if (st.block) RETURN_IF_ERROR(flush_block(st.block.get(), st.block_first_term));
    return Status::OK();
}

Status LogicalIndexWriter::prepare_build(io::FileWriter* posting_out) {
    if (posting_out == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "logical_index: null posting sink");
    }
    RETURN_IF_ERROR(format::validate_prx_window_limits(prx_window_limits_));
    if (has_norms_ && encoded_norms_.size() != doc_count_) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "logical_index: norms length must equal doc_count");
    }
    for (size_t i = 0; i < null_docids_.size(); ++i) {
        if (null_docids_[i] >= doc_count_) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "logical_index: null docid must be less than doc_count");
        }
        if (i != 0 && null_docids_[i] <= null_docids_[i - 1]) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "logical_index: null docids must be strictly ascending");
        }
    }
    if (common_grams_metadata_) {
        RETURN_IF_ERROR(validate_common_grams_segment_metadata(*common_grams_metadata_));
        if (common_grams_metadata_->common_grams_coverage == CommonGramsCoverage::kComplete &&
            !has_prx_) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "logical_index: complete CommonGrams metadata requires positions");
        }
        if (common_grams_metadata_->scoring_coverage == ScoringCoverage::kComplete) {
            if (!has_norms_ || !has_freq_) {
                return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                        "logical_index: complete scoring metadata requires frequencies and "
                        "semantic norms");
            }
            if (common_grams_metadata_->scoring_doc_count != doc_count_) {
                return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                        "logical_index: scoring doc count must equal doc_count");
            }
        }
    }
    if (common_grams_posting_policy_ == format::CommonGramsPostingPolicy::kHybridV1 &&
        (!common_grams_metadata_ ||
         common_grams_metadata_->common_grams_coverage != CommonGramsCoverage::kMixed ||
         !has_prx_)) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "logical_index: hybrid CommonGrams postings require mixed metadata and positions");
    }
    // The interleaved posting region streams STRAIGHT into the container output
    // (no temp round-trip): posting_size() is the region-relative byte count,
    // derived from the output offset advanced since this index's region began.
    // The DICT region is staged in dict_buf_ (tiered: RAM under the cap =
    // spill-only; spills above it) since it must land contiguously after the
    // concurrently-streamed posting region.
    posting_out_ = posting_out;
    posting_off0_ = posting_out->bytes_written();
    return Status::OK();
}

Status LogicalIndexWriter::finalize_build() {
    if (common_grams_metadata_ &&
        common_grams_metadata_->scoring_coverage == ScoringCoverage::kComplete) {
        if (common_grams_metadata_->scoring_token_count > stats_.sum_total_term_freq) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "logical_index: semantic scoring token count exceeds physical term frequency");
        }
        if (stats_.sum_total_term_freq != 0 && common_grams_metadata_->scoring_token_count == 0) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "logical_index: non-empty physical postings have zero semantic scoring tokens");
        }
        if (common_grams_metadata_->plain_term_key_version ==
                    ::doris::segment_v2::inverted_index::PlainTermKeyVersion::kRawNoInternal &&
            common_grams_metadata_->common_grams_coverage == CommonGramsCoverage::kNone &&
            common_grams_metadata_->scoring_token_count != stats_.sum_total_term_freq) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "logical_index: semantic plain token count must equal physical term frequency");
        }
    }
    // Seal the dict buffer so a spilled temp is flushed before
    // stream_dict_region_into reads it back. A no-op for a RAM-resident dict.
    RETURN_IF_ERROR(dict_buf_.seal());

    stats_.doc_count = doc_count_;
    stats_.indexed_doc_count = doc_count_ - static_cast<uint32_t>(null_docids_.size());
    stats_.term_count = term_count_;
    stats_.null_count = static_cast<uint32_t>(null_docids_.size());

    if (has_norms_) {
        const size_t payload_size = varint_len(encoded_norms_.size()) + encoded_norms_.size();
        const size_t section_size = 1 + varint_len(payload_size) + payload_size + sizeof(uint32_t);
        MemoryReporter::Reservation build_reservation =
                memory_reporter_ == nullptr ? MemoryReporter::Reservation()
                                            : memory_reporter_->make_reservation();
        if (memory_reporter_ != nullptr) {
            RETURN_IF_ERROR(build_reservation.set_bytes(payload_size));
            RETURN_IF_ERROR(norms_section_reservation_.set_bytes(section_size));
        }
        ByteSink nsink;
        format::NormsPodWriter::finish(encoded_norms_, &nsink);
        norms_section_ = nsink.take();
        DORIS_CHECK_EQ(norms_section_.capacity(), section_size);
        if (memory_reporter_ != nullptr) {
            DORIS_CHECK_EQ(norms_section_reservation_.bytes(), norms_section_.capacity());
        }
    }

    if (!null_docids_.empty()) {
        MemoryReporter::Reservation bitmap_build_reservation =
                memory_reporter_ == nullptr ? MemoryReporter::Reservation()
                                            : memory_reporter_->make_reservation();
        if (memory_reporter_ != nullptr) {
            RETURN_IF_ERROR(bitmap_build_reservation.set_bytes(
                    format::NullBitmapWriter::build_memory_upper_bound(
                            std::span<const uint32_t>(null_docids_.data(), null_docids_.size()))));
        }
        format::NullBitmapWriter null_writer;
        null_writer.add_many(std::span<const uint32_t>(null_docids_.data(), null_docids_.size()));
        null_docids_.release();

        format::NullBitmapSerializationSizes sizes;
        RETURN_IF_ERROR(null_writer.serialization_sizes(doc_count_, &sizes));
        if (sizes.roaring_bytes > std::numeric_limits<size_t>::max() - sizes.payload_bytes) {
            return Status::Error<ErrorCode::MEM_LIMIT_EXCEEDED, false>(
                    "logical_index: null bitmap scratch size overflows");
        }
        MemoryReporter::Reservation scratch_reservation =
                memory_reporter_ == nullptr ? MemoryReporter::Reservation()
                                            : memory_reporter_->make_reservation();
        if (memory_reporter_ != nullptr) {
            RETURN_IF_ERROR(
                    scratch_reservation.set_bytes(sizes.roaring_bytes + sizes.payload_bytes));
            RETURN_IF_ERROR(null_bitmap_section_reservation_.set_bytes(sizes.framed_bytes));
        }
        ByteSink null_sink;
        RETURN_IF_ERROR(null_writer.finish(doc_count_, &null_sink));
        null_bitmap_section_ = null_sink.take();
        DORIS_CHECK_EQ(null_bitmap_section_.size(), sizes.framed_bytes);
        if (memory_reporter_ != nullptr) {
            DORIS_CHECK_LE(null_bitmap_section_.capacity(),
                           null_bitmap_section_reservation_.bytes());
        }
    }
    null_docids_.release();

    // Build the absent-term filter (block-split bloom, Parquet-canonical) from
    // the per-term keys (no retained strings) as a [28B header][bitset] blob; the
    // compound writer places it as a PHYSICAL section probed one 32-byte block on
    // demand.
    bsbf_bytes_.clear();
    bsbf_built_ = false;
    if (!term_hashes_.empty()) {
        const uint32_t bitset_bytes = format::bsbf_optimal_num_bytes(
                static_cast<uint32_t>(term_hashes_.size()), kBsbfFpp);
        const size_t serialized_bytes = format::kBsbfHeaderSize + bitset_bytes;
        MemoryReporter::Reservation builder_reservation =
                memory_reporter_ == nullptr ? MemoryReporter::Reservation()
                                            : memory_reporter_->make_reservation();
        if (memory_reporter_ != nullptr) {
            RETURN_IF_ERROR(builder_reservation.set_bytes(bitset_bytes));
            RETURN_IF_ERROR(bsbf_bytes_reservation_.set_bytes(serialized_bytes));
        }
        format::BsbfBuilder bf;
        RETURN_IF_ERROR(format::BsbfBuilder::create(static_cast<uint32_t>(term_hashes_.size()),
                                                    kBsbfFpp, &bf));
        DCHECK_EQ(bf.resident_capacity_bytes(), bitset_bytes);
        for (uint64_t k : term_hashes_) bf.insert(k);
        ByteSink bsink;
        bsink.reserve(serialized_bytes);
        RETURN_IF_ERROR(bf.serialize(&bsink));
        bsbf_bytes_ = bsink.take();
        DCHECK_EQ(bsbf_bytes_.capacity(), serialized_bytes);
        bsbf_built_ = true;
    }
    std::vector<uint64_t>().swap(term_hashes_); // release
    term_hashes_reservation_.reset();

    return Status::OK();
}

void LogicalIndexWriter::release_bsbf_bytes() {
    std::vector<uint8_t>().swap(bsbf_bytes_);
    bsbf_bytes_reservation_.reset();
}

void LogicalIndexWriter::release_null_bitmap_bytes() {
    std::vector<uint8_t>().swap(null_bitmap_section_);
    null_bitmap_section_reservation_.reset();
}

void LogicalIndexWriter::release_norms_bytes() {
    std::vector<uint8_t>().swap(norms_section_);
    norms_section_reservation_.reset();
}

Status LogicalIndexWriter::build(io::FileWriter* posting_out) {
    // Single-session invariant: a writer that ran (or is running) a streamed
    // session must not also build() -- the posting sink anchor and the dict
    // buffer are one-shot.
    if (stream_phase_ != StreamPhase::kIdle) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "logical_index: build() on a writer with a streamed session");
    }
    // prepare_build is pure entry validation up to its final sink-anchor
    // assignments, so a failure there leaves the writer clean (still kIdle).
    RETURN_IF_ERROR(prepare_build(posting_out));
    // Poison-by-default past this point: build_blocks/finalize_build may fail
    // AFTER posting bytes hit the sink or term state advanced, and a dirty
    // writer must never accept a later begin_streamed/build (single-session +
    // crash-safety invariant 6). Only full success seals to kFinished.
    stream_phase_ = StreamPhase::kFailed;
    RETURN_IF_ERROR(build_blocks());
    RETURN_IF_ERROR(finalize_build());
    // Claim the (only) session so a later begin_streamed/push_term errors out.
    stream_phase_ = StreamPhase::kFinished;
    return Status::OK();
}

Status LogicalIndexWriter::begin_streamed(io::FileWriter* posting_out) {
    if (stream_phase_ == StreamPhase::kFailed) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "logical_index: begin_streamed on a failed writer (a prior session error left "
                "partial state; allocate a fresh writer)");
    }
    if (stream_phase_ != StreamPhase::kIdle) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "logical_index: begin_streamed on an already-claimed writer session");
    }
    RETURN_IF_ERROR(prepare_build(posting_out));
    stream_state_ = std::make_unique<BlockState>(memory_reporter_);
    stream_phase_ = StreamPhase::kActive;
    return Status::OK();
}

Status LogicalIndexWriter::push_term(StreamedTermPostings&& tp) {
    if (stream_phase_ != StreamPhase::kActive) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "logical_index: push_term without an active streamed session");
    }
    if (has_pushed_term_ && tp.term <= last_pushed_term_) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "logical_index: pushed terms must be strictly increasing ('{}' after '{}')",
                tp.term, last_pushed_term_);
    }
    last_pushed_term_ = tp.term;
    has_pushed_term_ = true;
    Status status = process_term(tp, stream_state_.get());
    if (!status.ok()) {
        stream_phase_ = StreamPhase::kFailed;
        stream_state_.reset();
    }
    return status;
}

Status LogicalIndexWriter::finish_streamed() {
    if (stream_phase_ == StreamPhase::kFinished) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "logical_index: finish_streamed on an already-finished session");
    }
    if (stream_phase_ == StreamPhase::kFailed) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "logical_index: finish_streamed on a failed session (a prior push/finish error "
                "poisoned the writer; the partial output must be discarded)");
    }
    if (stream_phase_ != StreamPhase::kActive) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "logical_index: finish_streamed without begin_streamed");
    }
    // Poison-by-default: a failed trailing flush or finalize leaves partial
    // output, so only full success below seals the session to kFinished.
    stream_phase_ = StreamPhase::kFailed;
    // Trailing-block flush mirrors the tail of build_blocks(); the shared
    // finalize_build() then seals the dict buffer and materializes the
    // stats/norms/null-bitmap/BSBF sections.
    if (stream_state_->block) {
        RETURN_IF_ERROR(flush_block(stream_state_->block.get(), stream_state_->block_first_term));
    }
    stream_state_.reset();
    RETURN_IF_ERROR(finalize_build());
    stream_phase_ = StreamPhase::kFinished;
    return Status::OK();
}

Status LogicalIndexWriter::finish_metadata(const SectionRefs& abs_refs, uint64_t dict_region_offset,
                                           SerializedMetadataGroup* out) const {
    if (out == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "logical_index: null metadata output");
    }
    *out = {};

    SampledTermIndexBuilder sti;
    for (const auto& b : blocks_) sti.add_block_first_term(b.first_term);
    ByteSink sti_sink;
    sti.finish(&sti_sink);

    DictBlockDirectoryBuilder dir;
    for (const auto& b : blocks_) {
        BlockRef ref;
        ref.offset = dict_region_offset + b.rel_offset;
        ref.length = b.length;
        ref.n_entries = b.n_entries;
        ref.flags = b.flags;
        ref.checksum = b.checksum;
        ref.uncomp_len = b.uncomp_len;
        dir.add(ref);
    }
    ByteSink dir_sink;
    dir.finish(&dir_sink);

    ByteSink sti_blob;
    RETURN_IF_ERROR(
            format::encode_metadata_blob(sti_sink.view(), format::SectionType::kSampledTermIndex,
                                         format::SectionType::kSampledTermIndexZstd, &sti_blob));
    out->sampled_term_index = sti_blob.take();

    ByteSink dbd_blob;
    RETURN_IF_ERROR(
            format::encode_metadata_blob(dir_sink.view(), format::SectionType::kDictBlockDirectory,
                                         format::SectionType::kDictBlockDirectoryZstd, &dbd_blob));
    out->dict_block_directory = dbd_blob.take();

    format::CoreMetadata core;
    core.index_config = index_config_;
    core.stats = stats_;
    core.section_refs = abs_refs;
    core.common_grams_metadata = common_grams_metadata_;
    core.common_grams_posting_policy = common_grams_posting_policy_;
    ByteSink core_sink;
    RETURN_IF_ERROR(format::encode_core_metadata(core, &core_sink));
    out->core = core_sink.take();
    return Status::OK();
}

} // namespace doris::snii::writer
