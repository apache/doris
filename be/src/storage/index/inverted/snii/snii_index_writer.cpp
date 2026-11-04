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

#include "storage/index/inverted/snii/snii_index_writer.h"

#include <algorithm>
#include <cstring>
#include <string_view>
#include <utility>

#include "common/cast_set.h"
#include "common/config.h"
#include "common/logging.h"
#include "storage/index/index_file_writer.h"
#include "storage/index/inverted/analysis/analyzer/analyzer.h"
#include "storage/index/inverted/analysis/gram/gram_density.h"
#include "storage/index/inverted/analysis/inverted_index_parser.h"
#include "storage/index/inverted/analysis/tokenizer/ngram/gram_tokenizer.h"
#include "storage/index/inverted/common/query_info.h"
#include "storage/index/inverted/snii/query/bm25_scorer.h"
#include "storage/index/inverted/snii/writer/global_memory_limiter.h"
#include "storage/index/inverted/snii/writer/snii_build_memory_tracker.h"
#include "storage/tablet/tablet_schema.h"

namespace doris::segment_v2 {
namespace {} // namespace

SniiIndexColumnWriter::SniiIndexColumnWriter(IndexFileWriter* index_file_writer,
                                             const TabletIndex* index_meta, FieldType value_type)
        : _index_file_writer(index_file_writer),
          _index_meta(index_meta),
          _is_char(value_type == FieldType::OLAP_FIELD_TYPE_CHAR) {}

// Gram-family detection (Rulings R21/R22): the scheme comes only from the single analyzer
// provider the writer created itself -- the same provider both produces the actual tokenizer and
// answers "am I gram family?", so the two cannot drift. A built-in analyzer
// (standard/english/...) goes through BuiltinAnalyzerProvider and the base class default, which
// is always nullopt, and the policy manager is never consulted (consulting it would throw
// "Policy not found" and make every built-in-analyzer index impossible to build).
void SniiIndexColumnWriter::_apply_gram_family_scheme(
        const inverted_index::AnalyzerProviderPtr& analyzer_provider) {
    if (analyzer_provider != nullptr) {
        _gram_scheme = analyzer_provider->gram_scheme();
    }
    // An index-level char_filter sits in front of the analyzer and is invisible to the scheme:
    // once one exists, the stored term is no longer equal to GramExtractor.extract(raw column
    // value), breaking the row invariant the query side (phase C) relies on, so this is treated
    // as "not gram family" (fail-safe, for the same reason as R22; the FE refuses the
    // combination).
    if (!_analyzer_config.char_filter_map.empty()) {
        _gram_scheme.reset();
    }
    DCHECK(!_gram_scheme.has_value() || _should_analyzer);
    if (!_gram_scheme.has_value() || !_has_positions) {
        return;
    }
    // R15: a gram-family hit forces a degradation to docs-only (the gram index does not support
    // phrase positions), and it has to happen before SpimiTermBuffer is fixed by _has_positions.
    LOG(INFO) << "gram-family analyzer forces docs-only index, ignoring support_phrase for index "
              << _index_meta->index_id();
    _has_positions = false;
    _config = ::doris::snii::format::IndexConfig::kDocsOnly;
}

// Arms the density solve for this segment. The configured density stays as the fallback: it is
// what a segment gets when the feature is off, when the sample carries no window of the
// promised length, or when nothing at all is written.
void SniiIndexColumnWriter::_arm_density_calibration() {
    // Density is a sparse-scheme parameter: a dense index cuts every position whatever the
    // rate, so holding its rows back would settle nothing and cost every dense writer the
    // sample, the histogram and a replay.
    if (!_gram_scheme.has_value() || _gram_scheme->mode != gram::GramMode::SPARSE ||
        !config::enable_gram_index_adaptive_density) {
        return;
    }
    // A literal shorter than a gram can never hold a whole one, so the promise cannot be
    // shorter than max_gram: a tokenizer with a longer max_gram solves for the shortest
    // literal it can actually keep instead of collecting no evidence at all.
    _density_promise_bytes = std::max<size_t>(
            static_cast<size_t>(config::gram_index_min_literal_bytes), _gram_scheme->max_len);
    _density_solver = std::make_unique<gram::DensitySolver>(
            _density_promise_bytes, _gram_scheme->max_len, _gram_scheme->lower_case);
    _density_calibrating = true;
}

// The whole path creates exactly one analyzer provider, and it must come before SpimiTermBuffer
// is constructed: the term buffer is fixed by _has_positions, and the gram-family decision that
// may reset _has_positions to false can only come from the provider. The second half of
// init() reuses the very provider created here.
Status SniiIndexColumnWriter::_create_analyzer_provider(
        inverted_index::AnalyzerProviderPtr* analyzer_provider) {
    try {
        if (_should_analyzer) {
            *analyzer_provider = inverted_index::InvertedIndexAnalyzer::create_analyzer_provider(
                    &_analyzer_config);
        }
    } catch (const Exception& e) {
        return Status::Error<ErrorCode::INVERTED_INDEX_ANALYZER_ERROR>(
                "SNII create analyzer failed: {}", e.what());
    }
    _apply_gram_family_scheme(*analyzer_provider);
    // After the scheme is settled and before any row arrives: the solve needs the scheme's
    // max_gram, and holding rows back only works if it starts with the first one.
    _arm_density_calibration();
    return Status::OK();
}

Status SniiIndexColumnWriter::init() {
    _should_analyzer =
            inverted_index::InvertedIndexAnalyzer::should_analyzer(_index_meta->properties());
    _has_positions = get_parser_phrase_support_string_from_properties(_index_meta->properties()) ==
                     INVERTED_INDEX_PARSER_PHRASE_SUPPORT_YES;
    _config = _has_positions ? ::doris::snii::format::IndexConfig::kDocsPositions
                             : ::doris::snii::format::IndexConfig::kDocsOnly;
    _analyzer_config.analyzer_name = get_analyzer_name_from_properties(_index_meta->properties());
    _analyzer_config.parser_type = get_inverted_index_parser_type_from_string(
            get_parser_string_from_properties(_index_meta->properties()));
    _analyzer_config.parser_mode =
            get_parser_mode_string_from_properties(_index_meta->properties());
    _analyzer_config.char_filter_map =
            get_parser_char_filter_map_from_properties(_index_meta->properties());
    _analyzer_config.lower_case =
            get_parser_lowercase_from_properties<true>(_index_meta->properties());
    _analyzer_config.stop_words = get_parser_stopwords_from_properties(_index_meta->properties());
    // The provider and the gram-family decision both happen here, before the term buffer is
    // constructed below.
    inverted_index::AnalyzerProviderPtr analyzer_provider;
    RETURN_IF_ERROR(_create_analyzer_provider(&analyzer_provider));
    auto ignore_above_value =
            get_parser_ignore_above_value_from_properties(_index_meta->properties());
    _ignore_above = cast_set<uint32_t>(std::stoul(ignore_above_value));
    const auto spill_threshold =
            static_cast<size_t>(config::inverted_index_ram_buffer_size * 1024 * 1024);
    // The consume_release callback mirrors this writer's live build bytes into
    // the process-wide SNII index-build observation tracker, so ingestion shows
    // up as its own line in Doris's memory picture (the allocation hook alone
    // only knows which THREAD allocated).
    _memory_reporter = std::make_unique<::doris::snii::writer::MemoryReporter>(
            ::doris::snii::writer::snii_build_consume_release(
                    ::doris::snii::writer::BuildMemoryPopulation::kRegistered),
            spill_threshold, ::doris::snii::writer::MemoryReporter::CapPolicy::kSpillThreshold,
            static_cast<uint64_t>(config::snii_postings_workspace_bytes));
    _null_docids_reservation = _memory_reporter->make_reservation();
    _term_buffer = std::make_unique<::doris::snii::writer::SpimiTermBuffer>(
            _has_positions, spill_threshold, _memory_reporter.get());
    // Register with the process-wide memory limiter so writers can honor advisory spill requests.
    // The arena floor and optional fan-in cap limit spill costs while workspace and fd limits still apply.
    _term_buffer->set_forced_spill_min_arena_bytes(
            static_cast<uint64_t>(std::max<int64_t>(config::snii_forced_spill_min_arena_bytes, 0)));
    _term_buffer->set_max_run_files(
            static_cast<size_t>(std::max<int32_t>(config::snii_spill_max_run_files_per_buffer, 0)));
    auto* global_limiter = ::doris::snii::writer::GlobalMemoryLimiter::instance();
    global_limiter->set_min_victim_arena_bytes(config::snii_forced_spill_min_arena_bytes);
    _term_buffer->attach_global_limiter(global_limiter);
    try {
        if (_should_analyzer) {
            _analyzer = analyzer_provider->get_analyzer();
            _stream = _analyzer->create_stream();
        }
    } catch (const Exception& e) {
        return Status::Error<ErrorCode::INVERTED_INDEX_ANALYZER_ERROR>(
                "SNII create analyzer failed: {}", e.what());
    }
    if (_gram_scheme.has_value()) {
        // A gram-family analyzer is a GramTextAnalyzer, whose stream is the GramTokenizer.
        _gram_tokenizer = dynamic_cast<inverted_index::GramTokenizer*>(_stream.get());
        DORIS_CHECK(_gram_tokenizer != nullptr);
    }
    // A2: Analyzed indexes with positions write norms (tokens per document, clamped to 1..255),
    // matching CLucene's scoring capabilities, unless the shared norms policy turns them off.
    // Keyword or positionless indexes omit them. Norms are an optional core-metadata region
    // ignored by older readers.
    _writes_norms = _should_analyzer && _has_positions && should_write_index_norms(*_index_meta);
    return Status::OK();
}

void SniiIndexColumnWriter::set_direct_load(bool is_direct_load) {
    // The PRX compression-tier hint must be stable for one index. The first
    // pre-row call wins; repeat or late calls are ignored and logged.
    DCHECK(!_direct_load_marked && _rid == 0);
    if (_direct_load_marked || _rid != 0) {
        LOG_EVERY_N(WARNING, 100) << "SNII set_direct_load(" << is_direct_load
                                  << ") ignored (already_marked=" << _direct_load_marked
                                  << ", rows_fed=" << _rid << ") for index "
                                  << (_index_meta != nullptr ? _index_meta->index_id() : -1)
                                  << "; keeping the first-captured PRX tier decision";
        return;
    }
    _direct_load_marked = true;
    _is_direct_load = is_direct_load;
}

Status SniiIndexColumnWriter::_add_value_tokens(const Slice& value, uint32_t docid,
                                                uint32_t position_base, uint32_t* max_position,
                                                uint32_t* token_count) {
    DCHECK(max_position != nullptr);
    DCHECK(token_count != nullptr);
    *max_position = position_base;
    *token_count = 0;
    const size_t logical_size = _is_char ? strnlen(value.data, value.size) : value.size;
    const std::string_view logical_value(value.data, logical_size);
    if ((!_should_analyzer && logical_value.size() > _ignore_above) ||
        (_should_analyzer && logical_value.empty())) {
        return Status::OK();
    }

    // Tokens stream from the analyzer straight into the SPIMI buffer as string_views (the
    // buffer interns the bytes into its own storage) -- no per-row vector<TermInfo> and no
    // per-token std::string. Golden-byte pins (snii_writer_golden_bytes_test.cpp) hold this path
    // byte-identical to the materializing one it replaced.
    auto consume_token = [&](std::string_view term, int32_t token_position, bool retain_positions) {
        const uint32_t position =
                _has_positions ? position_base + cast_set<uint32_t>(token_position) : 0;
        _term_buffer->add_token(term, docid, position, retain_positions);
        *max_position = std::max(*max_position, position);
        ++*token_count;
    };

    if (!_should_analyzer) {
        // Keyword lane: the whole value is one exact-match token at position 0
        // (an EMPTY value is a valid keyword token, mirrored from the old lane).
        consume_token(logical_value, 0, _has_positions);
    } else {
        try {
            _stream->reset(logical_value);
            // InvertedIndexAnalyzer::get_analyse_result semantics, including the subtle one: an
            // empty token's position increment is dropped WITH the token (not accumulated into
            // the next).
            int32_t position = 0;
            while (_stream->next_batch(_batch)) {
                for (const inverted_index::TokenRef& token : _batch.tokens()) {
                    if (token.size != 0) {
                        position += token.position_increment;
                        consume_token(token.term(), position, _has_positions);
                    }
                }
            }
            DCHECK_EQ(_batch.size(), 0);
            _batch.release_oversized();
            _report_analysis_capacity();
        } catch (const Exception& e) {
            return _latch_analysis_failure(Status::Error<ErrorCode::INVERTED_INDEX_ANALYZER_ERROR>(
                    "SNII analyze value failed: {}", e.what()));
        }
    }
    return Status::OK();
}

Status SniiIndexColumnWriter::add_values(const uint8_t* values, size_t count) {
    if (!_failure_status.ok()) {
        return _failure_status;
    }
    const auto* v = reinterpret_cast<const Slice*>(values);
    for (size_t i = 0; i < count; ++i) {
        if (_density_calibrating) {
            // Held back, not dropped: this row is tokenized once the rate is known. The
            // solver sees exactly the bytes the extractor will see -- a CHAR value ends at
            // its first NUL, as in _add_value_tokens -- and the sample is charged at what it
            // really retains: the payload plus the vector element and string header that
            // hold it, so a run of empty or tiny rows still reaches the cap.
            const size_t logical_size = _is_char ? strnlen(v->data, v->size) : v->size;
            _density_solver->observe(std::string_view(v->data, logical_size));
            _density_sample.emplace_back(_rid, std::string(v->data, logical_size));
            _density_sample_bytes += static_cast<int64_t>(logical_size) + kDensitySampleRowOverhead;
            _report_density_sample_capacity();
            ++v;
            ++_rid;
            if (_density_sample_bytes >= config::gram_index_density_sample_bytes) {
                RETURN_IF_ERROR(_finish_density_calibration());
            }
            continue;
        }
        uint32_t max_position = 0;
        uint32_t token_count = 0;
        RETURN_IF_ERROR(_add_value_tokens(*v, _rid, 0, &max_position, &token_count));
        if (_writes_norms) {
            _encoded_norms.push_back(::doris::snii::query::encode_norm(token_count));
            _report_encoded_norms_capacity();
        }
        ++v;
        ++_rid;
    }
    return Status::OK();
}

Status SniiIndexColumnWriter::add_array_values(size_t field_size, const uint8_t* value_ptr,
                                               const uint8_t* nested_null_map,
                                               const uint64_t* offsets_ptr, size_t count) {
    // Arrays are not sampled: settle the rate on whatever has been seen rather than hold array
    // rows back as well, which would complicate the docid bookkeeping for a shape the gram
    // family is not built for.
    RETURN_IF_ERROR(_finish_density_calibration());
    if (!_failure_status.ok()) {
        return _failure_status;
    }
    if (count == 0) {
        return Status::OK();
    }
    const auto* offsets = offsets_ptr;
    size_t start_off = 0;
    for (size_t i = 0; i < count; ++i) {
        auto array_elem_size = offsets[i + 1] - offsets[i];
        uint32_t position_base = 0;
        uint64_t row_token_count = 0;
        for (auto j = start_off; j < start_off + array_elem_size; ++j) {
            if (nested_null_map != nullptr && nested_null_map[j] == 1) {
                continue;
            }
            const auto* value = reinterpret_cast<const Slice*>(value_ptr + j * field_size);
            uint32_t max_position = position_base;
            uint32_t token_count = 0;
            RETURN_IF_ERROR(
                    _add_value_tokens(*value, _rid, position_base, &max_position, &token_count));
            position_base = max_position + 1;
            row_token_count += token_count;
        }
        if (_writes_norms) {
            // An ARRAY row's document length is the total token count across its elements.
            // NULL rows also pass here with length 0 and are marked by add_array_nulls.
            _encoded_norms.push_back(::doris::snii::query::encode_norm(row_token_count));
            _report_encoded_norms_capacity();
        }
        start_off += array_elem_size;
        ++_rid;
    }
    return Status::OK();
}

Status SniiIndexColumnWriter::_reserve_null_docids_for_append(size_t count) {
    // Uninitialized writers in unit tests have no reporter.
    return ::doris::snii::writer::reserve_docids_for_append(
            _memory_reporter.get(), &_null_docids_reservation, &_null_docids, count);
}

// Solves this segment's density from the rows held back for it, applies it, and tokenizes
// them. Idempotent, and a no-op unless a gram scheme is in force with the feature enabled.
//
// It runs from three places, which is the point: whichever of "the sample is full", "an array
// arrived" or "the segment is being flushed" comes first, the rate is settled before a single
// row is cut. Every row of a segment must be cut at one rate, because the query side
// reconstructs a segment's grams from the single rate its metadata records.
Status SniiIndexColumnWriter::_finish_density_calibration() {
    if (!_density_calibrating) {
        return Status::OK();
    }
    _density_calibrating = false;
    const auto coverage = cast_set<uint32_t>(config::gram_index_density_coverage_permille);
    const gram::DensitySolver::Solution solution = _density_solver->solve_detailed(coverage);
    if (_density_solver->observed_windows() > 0) {
        _gram_scheme->density_permille = solution.density_permille;
        // Before the replay below, which is the first row this tokenizer cuts.
        _gram_tokenizer->set_density_permille(solution.density_permille);
        if (solution.clamped && solution.achieved_coverage_permille < coverage) {
            // The sample asked for a rate outside the solver's bounds, so the recorded rate
            // does not keep the promise. Say so: a segment that silently misses its target
            // reads as an index that does not work on literals it was supposed to find.
            LOG(WARNING) << "gram index " << _index_meta->index_id()
                         << ": adaptive density clamped to " << solution.density_permille
                         << " permille; " << solution.required_permille
                         << " permille would be needed to cover " << coverage << " permille of "
                         << _density_promise_bytes << "-byte literals, the clamped rate covers "
                         << solution.achieved_coverage_permille << " permille";
        }
    }
    // Free the histogram before replaying: it is 256 KB that nothing needs again.
    _density_solver.reset();

    for (auto& [docid, value] : _density_sample) {
        uint32_t max_position = 0;
        uint32_t token_count = 0;
        const Slice slice(value.data(), value.size());
        RETURN_IF_ERROR(_add_value_tokens(slice, docid, 0, &max_position, &token_count));
        if (_writes_norms) {
            _encoded_norms.push_back(::doris::snii::query::encode_norm(token_count));
            _report_encoded_norms_capacity();
        }
    }
    std::vector<std::pair<uint32_t, std::string>>().swap(_density_sample);
    _density_sample_bytes = 0;
    _report_density_sample_capacity(/*release_all=*/true);
    return Status::OK();
}

// The held-back rows are real resident bytes between the first row and the solve, so the
// reporter that drives this writer's flush and spill decisions has to see them.
void SniiIndexColumnWriter::_report_density_sample_capacity(bool release_all) {
    if (_memory_reporter == nullptr) {
        return;
    }
    // The solver's histogram is resident for exactly as long as the sample is.
    const int64_t now =
            release_all ? 0
                        : _density_sample_bytes +
                                  (_density_solver != nullptr
                                           ? static_cast<int64_t>(gram::DensitySolver::heap_bytes())
                                           : 0);
    if (now != _density_sample_charged_bytes) {
        _memory_reporter->report(now - _density_sample_charged_bytes);
        _density_sample_charged_bytes = now;
    }
}

// What the analysis keeps between rows: the stream's scratch (for the gram family the fold
// buffer, boundary flags, gram list and dedupe table) and the batch. They are ordinary heap
// allocations, so the process-wide MemTrackerLimiter already sees them through the allocation
// hook -- but this writer's own MemoryReporter does not, and the number driving its flush/spill
// decision would understate real RSS. Rows on the analyzer lane are not bounded by ignore_above,
// so one long row can move it by about its own size, until the next row releases what is above
// the high water mark.
void SniiIndexColumnWriter::_report_analysis_capacity(bool release_all) {
    if (_memory_reporter == nullptr || _stream == nullptr) {
        return;
    }
    const int64_t now =
            release_all ? 0
                        : static_cast<int64_t>(_stream->reserved_bytes() + _batch.capacity_bytes());
    if (now != _analysis_charged_bytes) {
        _memory_reporter->report(now - _analysis_charged_bytes);
        _analysis_charged_bytes = now;
    }
}

void SniiIndexColumnWriter::_release_analysis() {
    _gram_tokenizer = nullptr;
    _stream.reset();
}

void SniiIndexColumnWriter::_report_encoded_norms_capacity(bool release_all) {
    if (_memory_reporter == nullptr) {
        return;
    }
    const int64_t now = release_all ? 0 : static_cast<int64_t>(_encoded_norms.capacity());
    if (now != _encoded_norms_charged_bytes) {
        _memory_reporter->report(now - _encoded_norms_charged_bytes);
        _encoded_norms_charged_bytes = now;
    }
}

Status SniiIndexColumnWriter::add_nulls(uint32_t count) {
    if (!_failure_status.ok()) {
        return _failure_status;
    }
    // GEOMETRIC BULK reserve -- never an exact one: ScalarColumnWriter::append calls
    // add_nulls once per NULL RUN (thousands to millions of calls on a large
    // interleaved-null segment), and an exact reserve(size()+count) caps
    // capacity at "just enough" -- the NEXT call then reallocates and memcpys
    // the WHOLE array, defeating geometric growth and turning total memcpy
    // quadratic: O(runs x array_bytes). On an agentlogs full-compaction segment
    // (12.4M rows, 22% interleaved nulls) that was TBs of memcpy per tablet --
    // the compaction ran 8+x slower than V3 (whose add_nulls is a roaring
    // addRange). Doubling on overflow keeps the O(count) amortization AND makes
    // one large run pay at most one reallocation.
    RETURN_IF_ERROR(_reserve_null_docids_for_append(count));
    for (uint32_t i = 0; i < count; ++i) {
        _null_docids.push_back(_rid + i);
    }
    _rid += count;
    if (_writes_norms) {
        _encoded_norms.insert(_encoded_norms.end(), count, ::doris::snii::query::encode_norm(0));
        _report_encoded_norms_capacity();
    }
    return Status::OK();
}

Status SniiIndexColumnWriter::add_array_nulls(const uint8_t* null_map, size_t num_rows) {
    if (!_failure_status.ok()) {
        return _failure_status;
    }
    DCHECK(_rid >= num_rows);
    if (num_rows == 0 || null_map == nullptr) {
        return Status::OK();
    }
    const size_t null_count = std::count(null_map, null_map + num_rows, static_cast<uint8_t>(1));
    RETURN_IF_ERROR(_reserve_null_docids_for_append(null_count));
    const auto first_row = _rid - num_rows;
    for (size_t i = 0; i < num_rows; ++i) {
        if (null_map[i] == 1) {
            _null_docids.push_back(cast_set<uint32_t>(first_row + i));
        }
    }
    return Status::OK();
}

Status SniiIndexColumnWriter::finish() {
    if (!_failure_status.ok()) {
        return _failure_status;
    }
    DCHECK(_term_buffer != nullptr);
    auto status = _term_buffer->status();
    if (!status.ok()) {
        return Status::InternalError("SNII term buffer error: {}", status.to_string());
    }
    // A segment smaller than the sample budget reaches here still holding its rows back. Solve
    // on what there is and tokenize them, so a short segment is indexed like any other -- and
    // so `options.gram_scheme` below carries the rate its rows were actually cut at.
    RETURN_IF_ERROR(_finish_density_calibration());
    IndexFileWriter::SniiAddIndexOptions options {};
    options.is_direct_load = _is_direct_load;
    options.gram_scheme = _gram_scheme;
    if (_writes_norms) {
        DORIS_CHECK_EQ(_encoded_norms.size(), _rid);
        options.encoded_norms = std::move(_encoded_norms);
    }
    status = _index_file_writer->add_snii_index(
            _index_meta, cast_set<uint32_t>(_rid),
            ::doris::snii::writer::TrackedNullDocids(std::move(_null_docids_reservation),
                                                     std::move(_null_docids)),
            _term_buffer.get(), _config, std::move(options), _memory_reporter.get());
    _report_encoded_norms_capacity(/*release_all=*/true);
    // The reporter is about to be handed to the index file writer and become a long-lived
    // process-wide counter, so its accumulation-phase charges must balance to zero first, and the
    // stream whose buffers are no longer charged goes.
    _report_analysis_capacity(/*release_all=*/true);
    _release_analysis();
    RETURN_IF_ERROR(status);
    _index_file_writer->retain_snii_memory_reporter(std::move(_memory_reporter));
    _term_buffer.reset();
    return Status::OK();
}

Status SniiIndexColumnWriter::_latch_analysis_failure(Status status) {
    DORIS_CHECK(!status.ok());
    DORIS_CHECK(_failure_status.ok());
    _failure_status = std::move(status);
    close_on_error();
    return _failure_status;
}

void SniiIndexColumnWriter::close_on_error() {
    _term_buffer.reset();
    std::vector<uint32_t>().swap(_null_docids);
    _null_docids_reservation.reset();
    _report_encoded_norms_capacity(/*release_all=*/true);
    _report_analysis_capacity(/*release_all=*/true);
    _release_analysis();
    _memory_reporter.reset();
    std::vector<uint8_t>().swap(_encoded_norms);
}

} // namespace doris::segment_v2
