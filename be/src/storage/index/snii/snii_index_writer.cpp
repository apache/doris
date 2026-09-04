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

#include "storage/index/snii/snii_index_writer.h"

#include <CLucene.h>

#include <algorithm>
#include <cstring>
#include <string_view>
#include <utility>

#include "common/cast_set.h"
#include "common/config.h"
#include "common/logging.h"
#include "storage/index/index_file_writer.h"
#include "storage/index/inverted/analyzer/analyzer.h"
#include "storage/index/inverted/query/query_info.h"
#include "storage/index/snii/query/bm25_scorer.h"
#include "storage/index/snii/writer/global_memory_limiter.h"
#include "storage/index/snii/writer/snii_build_memory_tracker.h"
#include "storage/tablet/tablet_schema.h"

namespace doris::segment_v2 {
namespace {

} // namespace

SniiIndexColumnWriter::SniiIndexColumnWriter(
        IndexFileWriter* index_file_writer, const TabletIndex* index_meta, FieldType value_type)
        : _index_file_writer(index_file_writer),
          _index_meta(index_meta),
          _is_char(value_type == FieldType::OLAP_FIELD_TYPE_CHAR) {}

Status SniiIndexColumnWriter::init() {
    _should_analyzer =
            inverted_index::InvertedIndexAnalyzer::should_analyzer(_index_meta->properties());
    _has_positions = get_parser_phrase_support_string_from_properties(_index_meta->properties()) ==
                     INVERTED_INDEX_PARSER_PHRASE_SUPPORT_YES;
    _config = _has_positions ? ::doris::snii::format::IndexConfig::kDocsPositions
                             : ::doris::snii::format::IndexConfig::kDocsOnly;
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
            spill_threshold, ::doris::snii::writer::MemoryReporter::CapPolicy::kSpillThreshold);
    _term_buffer = std::make_unique<::doris::snii::writer::SpimiTermBuffer>(
            _has_positions, spill_threshold, _memory_reporter.get());
    // G09: join the PROCESS-WIDE build-RAM limiter. The per-writer spill threshold above
    // bounds one writer; a load keeps (tablets x concurrency) writers alive at
    // once, none of which may ever reach it -- the global registry bounds their
    // SUM by asking the largest buffers to spill early (advisory flags honored
    // on each writer's own thread; byte-identical output). Registration is
    // UNCONDITIONAL: the limiter re-reads its trigger (SNII's share of the
    // process limit, plus the process-level backstops) at every decision, so an
    // admin enabling or disabling the share mid-load takes effect for writers
    // that are already running -- it is not latched here.
    // G09 anti-storm knobs (see the config comments): the forced-spill floor
    // gates both the owner-side honor (a request is a pending no-op until the
    // reclaimable arena regrows past it) and the limiter's victim eligibility,
    // and the run-file cap merge-compacts a writer's spill runs so the final
    // k-way merge's fd fan-in stays bounded. Applied unconditionally -- the
    // floor also protects test-seam requests, and the cap also bounds
    // per-writer gate-2 runs when the global limiter is off.
    _term_buffer->set_forced_spill_min_arena_bytes(
            static_cast<uint64_t>(std::max<int64_t>(config::snii_forced_spill_min_arena_bytes, 0)));
    _term_buffer->set_max_run_files(
            static_cast<size_t>(std::max<int32_t>(config::snii_spill_max_run_files_per_buffer, 0)));
    auto* global_limiter = ::doris::snii::writer::GlobalMemoryLimiter::instance();
    global_limiter->set_min_victim_arena_bytes(config::snii_forced_spill_min_arena_bytes);
    _term_buffer->attach_global_limiter(global_limiter);
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
    try {
        _char_string_reader = inverted_index::InvertedIndexAnalyzer::create_reader(
                _analyzer_config.char_filter_map);
        if (_should_analyzer) {
            auto analyzer_provider =
                    inverted_index::InvertedIndexAnalyzer::create_analyzer_provider(
                            &_analyzer_config);
            _analyzer = analyzer_provider->get_analyzer(inverted_index::AnalysisPurpose::kIndex);
        }
    } catch (const CLuceneError& e) {
        return Status::Error<ErrorCode::INVERTED_INDEX_ANALYZER_ERROR>(
                "SNII create analyzer failed: {}", e.what());
    } catch (const Exception& e) {
        return Status::Error<ErrorCode::INVERTED_INDEX_ANALYZER_ERROR>(
                "SNII create analyzer failed: {}", e.what());
    }
    // A2：分词 + 带位置的索引一律写 norms（每 doc 的词元数，clamp 到 1..255），与 CLucene 的
    // 打分能力对齐；keyword 或不带位置的索引不写。norms 是 core 元数据里的可选 region，
    // 不认识它的老 reader 会原样忽略。
    _writes_norms = _should_analyzer && _has_positions;
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

    // T1a: tokens STREAM from the analyzer straight into the SPIMI buffer as
    // string_views (the buffer interns the bytes into its own storage) -- no
    // per-row vector<TermInfo> and no per-token std::string materialization
    // (the old get_analyse_result lane; profile: 3.4-4.7% of import CPU burned
    // in token realloc). Golden-byte pins (snii_writer_golden_bytes_test.cpp)
    // hold this path byte-identical to the materializing one it replaced.
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
            _char_string_reader->init(logical_value.data(), cast_set<int32_t>(logical_value.size()),
                                      false);
            {
                std::unique_ptr<lucene::analysis::TokenStream> owned_token_stream(
                        _analyzer->tokenStream(L"", _char_string_reader));
                auto* token_stream = owned_token_stream.get();
                // EXACT InvertedIndexAnalyzer::get_analyse_result semantics,
                // including the subtle one: an empty token's position increment is
                // dropped WITH the token (not accumulated into the next).
                lucene::analysis::Token token;
                int32_t position = 0;
                while (token_stream->next(&token)) {
                    if (token.termLength<char>() != 0) {
                        const std::string_view term(token.termBuffer<char>(),
                                                    token.termLength<char>());
                        position += token.getPositionIncrement();
                        consume_token(term, position, _has_positions);
                    }
                }
                token_stream->close();
            }
        } catch (const CLuceneError& e) {
            return _latch_analysis_failure(Status::Error<ErrorCode::INVERTED_INDEX_ANALYZER_ERROR>(
                    "SNII analyze value failed: {}", e.what()));
        } catch (const Exception& e) {
            return _latch_analysis_failure(Status::Error<ErrorCode::INVERTED_INDEX_ANALYZER_ERROR>(
                    "SNII analyze value failed: {}", e.what()));
        }
    }
    return Status::OK();
}

Status SniiIndexColumnWriter::add_values(const std::string /*name*/, const void* values,
                                         size_t count) {
    if (!_failure_status.ok()) {
        return _failure_status;
    }
    const auto* v = reinterpret_cast<const Slice*>(values);
    for (size_t i = 0; i < count; ++i) {
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

Status SniiIndexColumnWriter::add_array_values(size_t field_size, const void* value_ptr,
                                               const uint8_t* nested_null_map,
                                               const uint8_t* offsets_ptr, size_t count) {
    if (!_failure_status.ok()) {
        return _failure_status;
    }
    if (count == 0) {
        return Status::OK();
    }
    const auto* offsets = reinterpret_cast<const uint64_t*>(offsets_ptr);
    size_t start_off = 0;
    for (size_t i = 0; i < count; ++i) {
        auto array_elem_size = offsets[i + 1] - offsets[i];
        uint32_t position_base = 0;
        uint64_t row_token_count = 0;
        for (auto j = start_off; j < start_off + array_elem_size; ++j) {
            if (nested_null_map != nullptr && nested_null_map[j] == 1) {
                continue;
            }
            const auto* value = reinterpret_cast<const Slice*>(
                    reinterpret_cast<const uint8_t*>(value_ptr) + j * field_size);
            uint32_t max_position = position_base;
            uint32_t token_count = 0;
            RETURN_IF_ERROR(_add_value_tokens(*value, _rid, position_base, &max_position,
                                              &token_count));
            position_base = max_position + 1;
            row_token_count += token_count;
        }
        if (_writes_norms) {
            // 一行 ARRAY 的文档长度 = 所有元素的词元数之和（NULL 行由 add_array_nulls 声明，
            // 但同样经过这里，长度为 0）。
            _encoded_norms.push_back(::doris::snii::query::encode_norm(row_token_count));
            _report_encoded_norms_capacity();
        }
        start_off += array_elem_size;
        ++_rid;
    }
    return Status::OK();
}

void SniiIndexColumnWriter::_report_null_docids_capacity(bool release_all) {
    if (_memory_reporter == nullptr) {
        return;
    }
    const int64_t now =
            release_all ? 0 : static_cast<int64_t>(_null_docids.capacity() * sizeof(uint32_t));
    if (now != _null_docids_charged_bytes) {
        _memory_reporter->report(now - _null_docids_charged_bytes);
        _null_docids_charged_bytes = now;
    }
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
    // GEOMETRIC BULK reserve -- never an exact one: append_nullable calls
    // add_nulls once per NULL RUN (thousands to millions of calls on a large
    // interleaved-null segment), and an exact reserve(size()+count) caps
    // capacity at "just enough" -- the NEXT call then reallocates and memcpys
    // the WHOLE array, defeating geometric growth and turning total memcpy
    // quadratic: O(runs x array_bytes). On an agentlogs full-compaction segment
    // (12.4M rows, 22% interleaved nulls) that was TBs of memcpy per tablet --
    // the compaction ran 8+x slower than V3 (whose add_nulls is a roaring
    // addRange). Doubling on overflow keeps the O(count) amortization AND makes
    // one large run pay at most one reallocation.
    const size_t need = _null_docids.size() + count;
    if (need > _null_docids.capacity()) {
        _null_docids.reserve(std::max(need, _null_docids.capacity() * 2));
    }
    for (uint32_t i = 0; i < count; ++i) {
        _null_docids.push_back(_rid + i);
    }
    _rid += count;
    if (_writes_norms) {
        _encoded_norms.insert(_encoded_norms.end(), count, ::doris::snii::query::encode_norm(0));
        _report_encoded_norms_capacity();
    }
    _report_null_docids_capacity();
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
    const auto first_row = _rid - num_rows;
    for (size_t i = 0; i < num_rows; ++i) {
        if (null_map[i] == 1) {
            _null_docids.push_back(cast_set<uint32_t>(first_row + i));
        }
    }
    _report_null_docids_capacity();
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
    // Ownership of _null_docids hands off to the flush below (transient,
    // flush-scoped); release the accumulation-phase charge so the retained
    // reporter (and the observation tracker behind it) balances to zero.
    _report_null_docids_capacity(/*release_all=*/true);
    IndexFileWriter::SniiAddIndexOptions options {};
    options.is_direct_load = _is_direct_load;
    if (_writes_norms) {
        DORIS_CHECK_EQ(_encoded_norms.size(), _rid);
        options.encoded_norms = std::move(_encoded_norms);
    }
    status = _index_file_writer->add_snii_index(
            _index_meta, cast_set<uint32_t>(_rid), std::move(_null_docids), _term_buffer.get(),
            _config, std::move(options), _memory_reporter.get());
    _report_encoded_norms_capacity(/*release_all=*/true);
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
    // Balance the observation-tracker mirror before dropping the reporter.
    _report_null_docids_capacity(/*release_all=*/true);
    _report_encoded_norms_capacity(/*release_all=*/true);
    _memory_reporter.reset();
    _null_docids.clear();
    std::vector<uint8_t>().swap(_encoded_norms);
}

} // namespace doris::segment_v2
