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

#pragma once

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "storage/index/index_writer.h"
// AnalyzerProviderPtr is already in the include closure of inverted_index_parser.h (a path
// almost every TU pulls in), so spelling it out here only makes the dependency visible and adds
// no forward-closure cost.
#include "storage/index/inverted/analysis/analyzer/analyzer_provider.h"
#include "storage/index/inverted/analysis/gram/gram_density.h"
#include "storage/index/inverted/analysis/gram/gram_scheme.h"
#include "storage/index/inverted/analysis/inverted_index_parser.h"
#include "storage/index/inverted/analysis/text_analyzer.h"
#include "storage/index/inverted/common/query_info.h"
#include "storage/index/inverted/snii/format/format_constants.h"
#include "storage/index/inverted/snii/writer/memory_reporter.h"
#include "storage/index/inverted/snii/writer/spimi_term_buffer.h"
#include "util/slice.h"

namespace doris::segment_v2::inverted_index {
class GramTokenizer;
} // namespace doris::segment_v2::inverted_index

namespace doris::segment_v2 {

class SniiIndexColumnWriter final : public IndexColumnWriter {
public:
    SniiIndexColumnWriter(IndexFileWriter* index_file_writer, const TabletIndex* index_meta,
                          FieldType value_type);
    ~SniiIndexColumnWriter() override = default;

    Status init() override;
    void set_direct_load(bool is_direct_load) override;
    Status add_values(const uint8_t* values, size_t count) override;
    Status add_array_values(size_t field_size, const uint8_t* value_ptr,
                            const uint8_t* nested_null_map, const uint64_t* offsets_ptr,
                            size_t count) override;
    Status add_nulls(uint32_t count) override;
    Status add_array_nulls(const uint8_t* null_map, size_t num_rows) override;
    Status finish() override;
    int64_t size() const override { return 0; }
    void close_on_error() override;

#ifdef BE_TEST
    // TEST-ONLY view of the accumulated null docids: the growth-policy
    // regression pin asserts add_nulls keeps geometric growth (an exact
    // reserve(size+count) per null RUN made total memcpy quadratic -- the
    // agentlogs full-compaction pathology).
    const std::vector<uint32_t>& null_docids_for_test() const { return _null_docids; }
    ::doris::snii::writer::SpimiTermBuffer* term_buffer_for_test() const {
        return _term_buffer.get();
    }
    ::doris::snii::writer::MemoryReporter* memory_reporter_for_test() const {
        return _memory_reporter.get();
    }
    const std::vector<uint8_t>& encoded_norms_for_test() const { return _encoded_norms; }
    ::doris::snii::format::IndexConfig config_for_test() const { return _config; }
    bool writes_norms_for_test() const { return _writes_norms; }
    const std::optional<gram::GramScheme>& gram_scheme_for_test() const { return _gram_scheme; }
    bool density_calibrating_for_test() const { return _density_calibrating; }
    int64_t density_sample_bytes_for_test() const { return _density_sample_bytes; }
    size_t density_promise_bytes_for_test() const { return _density_promise_bytes; }
    // Replaces the analyzer of an initialized writer outside the gram family.
    void set_analysis_for_test(inverted_index::TextAnalyzerPtr analyzer) {
        DORIS_CHECK(!_gram_scheme.has_value());
        _should_analyzer = true;
        _stream.reset();
        _analyzer = std::move(analyzer);
        _stream = _analyzer->create_stream();
    }
    const inverted_index::TextTokenStream* stream_for_test() const { return _stream.get(); }
#endif

private:
    // The first half of init(): build (when _should_analyzer) the one analyzer provider, then let
    // _apply_gram_family_scheme settle the gram-family decision. Must be called before
    // SpimiTermBuffer is constructed. An Exception thrown here is turned into
    // INVERTED_INDEX_ANALYZER_ERROR.
    Status _create_analyzer_provider(inverted_index::AnalyzerProviderPtr* analyzer_provider);
    // Take the gram scheme from the provider and apply the consequences of the gram family
    // (forcing docs-only).
    void _apply_gram_family_scheme(const inverted_index::AnalyzerProviderPtr& analyzer_provider);
    Status _add_value_tokens(const Slice& value, uint32_t docid, uint32_t position_base,
                             uint32_t* max_position, uint32_t* token_count);
    Status _reserve_null_docids_for_append(size_t count);
    void _report_encoded_norms_capacity(bool release_all = false);
    // Mirrors what the analysis keeps between rows -- the stream's buffers and the batch -- into
    // _memory_reporter (delta-charged), same shape as the one above. A no-op for a writer that
    // does not analyze.
    void _report_analysis_capacity(bool release_all = false);
    // Destroys the stream and frees the batch once their capacity is no longer reported. After an
    // analyzer error the batch can still hold the tokens of the row that failed.
    void _release_analysis();
    // Solves the density from what has been held back, applies it, and tokenizes the held-back
    // rows. Idempotent, and a no-op when the scheme is not gram family or the feature is off.
    void _arm_density_calibration();
    Status _finish_density_calibration();
    void _report_density_sample_capacity(bool release_all = false);
    Status _latch_analysis_failure(Status status);

    IndexFileWriter* _index_file_writer = nullptr;
    const TabletIndex* _index_meta = nullptr;
    bool _should_analyzer = false;
    bool _has_positions = false;
    const bool _is_char;
    // A2: Analyzed indexes with positions write BM25 norms, matching CLucene, unless the shared
    // norms policy (should_write_index_norms) turns them off.
    bool _writes_norms = false;
    // Latch: set_direct_load() ran. The first call wins; a repeat or late call
    // is ignored (and logged) so one index keeps one stable compression-tier
    // decision.
    bool _direct_load_marked = false;
    // Captured by set_direct_load() under the same latch: this writer serves a
    // stream/broker load (DataWriteType::TYPE_DIRECT). Consumed at finish() to
    // route the prx region to the load-tier zstd level (patch C,
    // config::snii_prx_zstd_level_direct_load).
    bool _is_direct_load = false;
    uint32_t _ignore_above = 0;
    uint32_t _rid = 0;
    ::doris::snii::format::IndexConfig _config = ::doris::snii::format::IndexConfig::kDocsOnly;
    // Scheme parameters of a gram-family analyzer (an ngram tokenizer with a mode property);
    // obtained by _apply_gram_family_scheme, before the term buffer is constructed, from the
    // analyzer provider this writer created itself. Once it holds a value, docs-only is forced.
    // A built-in analyzer, an analyzer carrying filters, and an index carrying an index-level
    // char_filter all leave it nullopt (R21/R22).
    std::optional<gram::GramScheme> _gram_scheme;
    // Adaptive density. The rate a gram scheme cuts at is solved from this segment's own bytes
    // rather than taken from the tokenizer's configuration, because one configured rate cannot
    // mean the same thing on two columns -- measured, a nominal 0.25 produced 0.204 to 0.287
    // grams per byte and 91.7% to 98.9% coverage across three corpora.
    //
    // The first rows of a segment are held back rather than tokenized: every row of a segment
    // must be cut at ONE rate, since the query side reconstructs a segment's grams from the
    // single rate recorded in its metadata. Once the sample budget is reached the rate is
    // solved, applied, and the held-back rows are tokenized with it.
    std::unique_ptr<gram::DensitySolver> _density_solver;
    std::vector<std::pair<uint32_t, std::string>> _density_sample;
    // Per held-back row: the vector element and the string header it carries, on top of the
    // payload. Charged so that the sample cap is reached by row count as well as by bytes.
    static constexpr int64_t kDensitySampleRowOverhead =
            static_cast<int64_t>(sizeof(std::pair<uint32_t, std::string>));
    int64_t _density_sample_bytes = 0;
    // The literal length the solve promises, after the max_gram floor was applied.
    size_t _density_promise_bytes = 0;
    int64_t _density_sample_charged_bytes = 0;
    bool _density_calibrating = false;
    InvertedIndexAnalyzerConfig _analyzer_config;
    inverted_index::TextAnalyzerPtr _analyzer;
    // One stream for every row of the writer, made by init() and destroyed by finish() and
    // close_on_error(). Declared after _analyzer, which it must not outlive.
    std::unique_ptr<inverted_index::TextTokenStream> _stream;
    inverted_index::TokenBatch _batch;
    // Non-owning: _stream itself when the scheme is gram family, so that the solved density can
    // be set on it before it cuts the first row; null otherwise.
    inverted_index::GramTokenizer* _gram_tokenizer = nullptr;
    std::unique_ptr<::doris::snii::writer::MemoryReporter> _memory_reporter;
    ::doris::snii::writer::MemoryReporter::Reservation _null_docids_reservation;
    std::unique_ptr<::doris::snii::writer::SpimiTermBuffer> _term_buffer;
    std::vector<uint32_t> _null_docids;
    std::vector<uint8_t> _encoded_norms;
    int64_t _encoded_norms_charged_bytes = 0;
    int64_t _analysis_charged_bytes = 0;
    Status _failure_status = Status::OK();
};

} // namespace doris::segment_v2
