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
#include "storage/index/inverted/analyzer/analyzer_provider.h"
#include "storage/index/inverted/gram/gram_density.h"
#include "storage/index/inverted/gram/gram_scheme.h"
#include "storage/index/inverted/inverted_index_parser.h"
#include "storage/index/inverted/query/query_info.h"
#include "storage/index/inverted/util/reader.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/writer/memory_reporter.h"
#include "storage/index/snii/writer/spimi_term_buffer.h"
#include "util/slice.h"

namespace lucene::analysis {
class Analyzer;
class TokenStream;
} // namespace lucene::analysis

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
    Status add_values(const std::string name, const void* values, size_t count) override;
    Status add_array_values(size_t field_size, const void* value_ptr,
                            const uint8_t* nested_null_map, const uint8_t* offsets_ptr,
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
    void set_analysis_for_test(inverted_index::ReaderPtr reader,
                               std::shared_ptr<lucene::analysis::Analyzer> analyzer) {
        _should_analyzer = true;
        _char_string_reader = std::move(reader);
        _analyzer = std::move(analyzer);
    }
#endif

private:
    // The first half of init(): build the char filter reader and (when _should_analyzer) the one
    // analyzer provider, then let _apply_gram_family_scheme settle the gram-family decision. Must
    // be called before SpimiTermBuffer is constructed. A CLuceneError / Exception thrown here is
    // uniformly turned into INVERTED_INDEX_ANALYZER_ERROR.
    Status _create_analyzer_provider(inverted_index::AnalyzerProviderPtr* analyzer_provider);
    // Take the gram scheme from the provider and apply the consequences of the gram family
    // (forcing docs-only).
    void _apply_gram_family_scheme(const inverted_index::AnalyzerProviderPtr& analyzer_provider);
    Status _add_value_tokens(const Slice& value, uint32_t docid, uint32_t position_base,
                             uint32_t* max_position, uint32_t* token_count);
    // One row's token stream, reset and ready to drain. `owned` receives it only when this
    // writer must delete it; the gram lane's stream stays owned by _analyzer. See the
    // definition for the ownership/reset contract.
    lucene::analysis::TokenStream* _plain_lane_token_stream(
            std::unique_ptr<lucene::analysis::TokenStream>* owned);
    // Mirror of the above, run once the row's tokens are consumed: closes an owned stream, or
    // charges the reused gram tokenizer's settled buffer capacity to the memory reporter.
    void _finish_plain_lane_row(lucene::analysis::TokenStream* token_stream, bool owned);
    // Mirrors _null_docids' capacity into _memory_reporter (delta-charged);
    // release_all zeroes the charge (finish() handoff / close_on_error()).
    void _report_null_docids_capacity(bool release_all = false);
    void _report_encoded_norms_capacity(bool release_all = false);
    // Mirrors the reused gram tokenizer's buffer capacity into _memory_reporter
    // (delta-charged), same shape as the two above. A no-op until the reusable
    // lane has cached _gram_tokenizer.
    void _report_gram_buffers_capacity(bool release_all = false);
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
    // A2: Analyzed indexes with positions always write BM25 norms, matching CLucene.
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
    // Set when the rate has been solved but the tokenizer has not been obtained yet; applied
    // where the tokenizer is first cast, before it cuts anything.
    std::optional<uint16_t> _pending_density_permille;
    InvertedIndexAnalyzerConfig _analyzer_config;
    inverted_index::ReaderPtr _char_string_reader;
    std::shared_ptr<lucene::analysis::Analyzer> _analyzer;
    // Non-owning. The gram lane pulls its token stream from
    // Analyzer::reusableTokenStream, which hands back a pointer into the cached
    // components _analyzer owns, so this stays valid exactly as long as
    // _analyzer does and must be declared after it. Null until the first
    // analyzed row, and always null off the gram lane.
    inverted_index::GramTokenizer* _gram_tokenizer = nullptr;
    std::unique_ptr<::doris::snii::writer::MemoryReporter> _memory_reporter;
    std::unique_ptr<::doris::snii::writer::SpimiTermBuffer> _term_buffer;
    std::vector<uint32_t> _null_docids;
    std::vector<uint8_t> _encoded_norms;
    // Bytes of _null_docids capacity currently mirrored into _memory_reporter
    // (and through it the SNII index-build observation tracker). Re-charged on
    // growth in add_nulls / add_array_nulls, released in finish() / close_on_error() --
    // without it a large interleaved-null segment accumulates untracked RSS the
    // G09 limiter cannot see.
    int64_t _null_docids_charged_bytes = 0;
    int64_t _encoded_norms_charged_bytes = 0;
    int64_t _gram_buffers_charged_bytes = 0;
    Status _failure_status = Status::OK();
};

} // namespace doris::segment_v2
