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
#include <string>
#include <vector>

#include "storage/index/index_writer.h"
#include "storage/index/inverted/inverted_index_parser.h"
#include "storage/index/inverted/query/query_info.h"
#include "storage/index/inverted/util/reader.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/snii_phrase_bigram_build.h"
#include "storage/index/snii/writer/memory_reporter.h"
#include "storage/index/snii/writer/spimi_term_buffer.h"
#include "util/slice.h"

namespace lucene::analysis {
class Analyzer;
}

namespace doris::segment_v2 {

class SniiIndexColumnWriter final : public IndexColumnWriter {
public:
    SniiIndexColumnWriter(IndexFileWriter* index_file_writer, const TabletIndex* index_meta,
                          bool single_field);
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
#endif

private:
    Status _add_value_tokens(const Slice& value, uint32_t docid, uint32_t position_base,
                             uint32_t* max_position);
    // Emits the hidden phrase-bigram tokens for the row whose bigram-indexable
    // unigrams (as SPIMI term-ids + positions) _add_value_tokens collected into
    // _bigram_positioned.
    Status _add_phrase_bigram_tokens(uint32_t docid);
    // Mirrors _null_docids' capacity into _memory_reporter (delta-charged);
    // release_all zeroes the charge (finish() handoff / close_on_error()).
    void _report_null_docids_capacity(bool release_all = false);

    IndexFileWriter* _index_file_writer = nullptr;
    const TabletIndex* _index_meta = nullptr;
    bool _should_analyzer = false;
    bool _has_positions = false;
    // IndexColumnWriter::create() marks ARRAY item indexes as multi-field.
    // They retain the hidden bigram layout to preserve existing SNII full-build
    // phrase behavior at element boundaries.
    bool _single_field = true;
    // Latch: set_direct_load() ran. The first call wins; a repeat or late call
    // is ignored (and logged) so the pair feed can never desync from the
    // sentinel decision at finish().
    bool _direct_load_marked = false;
    // Captured by set_direct_load() under the same latch: this writer serves a
    // stream/broker load (DataWriteType::TYPE_DIRECT). Consumed at finish() to
    // route the prx region to the load-tier zstd level (patch C,
    // config::snii_prx_zstd_level_direct_load).
    bool _is_direct_load = false;
    // Defer the hidden phrase-bigram build to compaction for single-field
    // indexes only: no pair tokens, no sentinel, and a resident per-index
    // capability flag (kPhraseBigramsDeferred) routes fresh-segment phrases
    // directly to positions verification. ARRAY indexes retain their full build
    // to preserve existing SNII full-build phrase behavior at element
    // boundaries. Captured ONCE in set_direct_load() -- create() runs init()
    // first and the segment writer marks direct load before any row is fed --
    // so a mid-load flip of config::snii_bigram_defer_build_to_compaction can
    // never desync the pair feed from the sentinel decision at finish().
    bool _phrase_bigrams_deferred = false;
    uint32_t _ignore_above = 0;
    uint32_t _rid = 0;
    ::doris::snii::format::IndexConfig _config = ::doris::snii::format::IndexConfig::kDocsOnly;
    InvertedIndexAnalyzerConfig _analyzer_config;
    inverted_index::ReaderPtr _char_string_reader;
    std::shared_ptr<lucene::analysis::Analyzer> _analyzer;
    std::unique_ptr<::doris::snii::writer::MemoryReporter> _memory_reporter;
    std::unique_ptr<::doris::snii::writer::SpimiTermBuffer> _term_buffer;
    std::vector<uint32_t> _null_docids;
    // Bytes of _null_docids capacity currently mirrored into _memory_reporter
    // (and through it Doris's LOAD MemTracker). Re-charged on growth in
    // add_nulls / add_array_nulls, released in finish() / close_on_error() --
    // without it a large interleaved-null segment accumulates untracked RSS the
    // G09 limiter cannot see.
    int64_t _null_docids_charged_bytes = 0;
    // Reused across every _add_value_tokens call: clear() keeps the backing
    // capacity so the per-row phrase-bigram build stops re-allocating a fresh
    // positioned-term vector on each text row/array element. G05: carries the
    // SPIMI unigram TERM-IDS captured while adding the row's unigram tokens (not
    // term bytes) -- the bigram emission feeds the id-keyed
    // SpimiTermBuffer::add_bigram_token pair path. Single-threaded per-column
    // build state (see _add_phrase_bigram_tokens).
    std::vector<PhrasePositionedTermId> _bigram_positioned;
};

} // namespace doris::segment_v2
