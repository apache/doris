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

#include "common/cast_set.h"
#include "common/config.h"
#include "storage/index/index_file_writer.h"
#include "storage/index/inverted/analyzer/analyzer.h"
#include "storage/index/inverted/query/query_info.h"
#include "storage/index/snii/format/phrase_bigram.h"
#include "storage/tablet/tablet_schema.h"

namespace doris::segment_v2 {

SniiIndexColumnWriter::SniiIndexColumnWriter(IndexFileWriter* index_file_writer,
                                             const TabletIndex* index_meta, bool /*single_field*/)
        : _index_file_writer(index_file_writer), _index_meta(index_meta) {}

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
    _memory_reporter =
            std::make_unique<::doris::snii::writer::MemoryReporter>(nullptr, spill_threshold);
    _term_buffer = std::make_unique<::doris::snii::writer::SpimiTermBuffer>(
            _has_positions, spill_threshold, _memory_reporter.get());
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
            _analyzer = inverted_index::InvertedIndexAnalyzer::create_analyzer(&_analyzer_config);
        }
    } catch (const CLuceneError& e) {
        return Status::Error<ErrorCode::INVERTED_INDEX_ANALYZER_ERROR>(
                "SNII create analyzer failed: {}", e.what());
    } catch (const Exception& e) {
        return Status::Error<ErrorCode::INVERTED_INDEX_ANALYZER_ERROR>(
                "SNII create analyzer failed: {}", e.what());
    }
    return Status::OK();
}

Status SniiIndexColumnWriter::_analyze(const Slice& value, std::vector<TermInfo>* terms) {
    terms->clear();
    if (!_should_analyzer) {
        TermInfo term;
        term.term = std::string(value.data, value.size);
        term.position = 0;
        terms->emplace_back(std::move(term));
        return Status::OK();
    }
    try {
        _char_string_reader->init(value.data, cast_set<int32_t>(value.size), false);
        *terms = inverted_index::InvertedIndexAnalyzer::get_analyse_result(_char_string_reader,
                                                                           _analyzer.get());
    } catch (const CLuceneError& e) {
        return Status::Error<ErrorCode::INVERTED_INDEX_ANALYZER_ERROR>(
                "SNII analyze value failed: {}", e.what());
    } catch (const Exception& e) {
        return Status::Error<ErrorCode::INVERTED_INDEX_ANALYZER_ERROR>(
                "SNII analyze value failed: {}", e.what());
    }
    return Status::OK();
}

Status SniiIndexColumnWriter::_add_phrase_bigram_tokens(const std::vector<TermInfo>& terms,
                                                        uint32_t docid, uint32_t position_base) {
    if (!_has_positions || terms.size() < 2) {
        return Status::OK();
    }

    struct PositionedTerm {
        std::string_view term;
        uint32_t position = 0;
    };

    std::vector<PositionedTerm> positioned;
    positioned.reserve(terms.size());
    for (const auto& term_info : terms) {
        DCHECK(term_info.is_single_term());
        const std::string_view term = term_info.get_single_term();
        if (!::doris::snii::format::is_phrase_bigram_indexable_term(term)) {
            continue;
        }
        positioned.push_back({term, position_base + cast_set<uint32_t>(term_info.position)});
    }
    if (positioned.size() < 2) {
        return Status::OK();
    }
    std::ranges::sort(positioned, [](const PositionedTerm& lhs, const PositionedTerm& rhs) {
        if (lhs.position != rhs.position) {
            return lhs.position < rhs.position;
        }
        return lhs.term < rhs.term;
    });

    size_t left_begin = 0;
    while (left_begin < positioned.size()) {
        size_t left_end = left_begin + 1;
        while (left_end < positioned.size() &&
               positioned[left_end].position == positioned[left_begin].position) {
            ++left_end;
        }

        size_t right_begin = left_end;
        while (right_begin < positioned.size() &&
               positioned[right_begin].position <= positioned[left_begin].position) {
            ++right_begin;
        }
        if (right_begin == positioned.size() ||
            positioned[right_begin].position != positioned[left_begin].position + 1) {
            left_begin = left_end;
            continue;
        }
        size_t right_end = right_begin + 1;
        while (right_end < positioned.size() &&
               positioned[right_end].position == positioned[right_begin].position) {
            ++right_end;
        }

        for (size_t l = left_begin; l < left_end; ++l) {
            for (size_t r = right_begin; r < right_end; ++r) {
                _term_buffer->add_token(::doris::snii::format::make_phrase_bigram_term(
                                                positioned[l].term, positioned[r].term),
                                        docid, positioned[l].position);
            }
        }
        left_begin = left_end;
    }
    return Status::OK();
}

Status SniiIndexColumnWriter::_add_value_tokens(const Slice& value, uint32_t docid,
                                                uint32_t position_base, uint32_t* max_position) {
    DCHECK(max_position != nullptr);
    *max_position = position_base;
    if ((!_should_analyzer && value.size > _ignore_above) || (_should_analyzer && value.empty())) {
        return Status::OK();
    }

    std::vector<TermInfo> terms;
    RETURN_IF_ERROR(_analyze(value, &terms));
    for (const auto& term_info : terms) {
        DCHECK(term_info.is_single_term());
        const auto& term = term_info.get_single_term();
        const uint32_t position =
                _has_positions ? position_base + cast_set<uint32_t>(term_info.position) : 0;
        _term_buffer->add_token(term, docid, position);
        *max_position = std::max(*max_position, position);
    }
    RETURN_IF_ERROR(_add_phrase_bigram_tokens(terms, docid, position_base));
    return Status::OK();
}

Status SniiIndexColumnWriter::add_values(const std::string /*name*/, const void* values,
                                         size_t count) {
    const auto* v = reinterpret_cast<const Slice*>(values);
    for (size_t i = 0; i < count; ++i) {
        uint32_t max_position = 0;
        RETURN_IF_ERROR(_add_value_tokens(*v, _rid, 0, &max_position));
        ++v;
        ++_rid;
    }
    return Status::OK();
}

Status SniiIndexColumnWriter::add_array_values(size_t field_size, const void* value_ptr,
                                               const uint8_t* nested_null_map,
                                               const uint8_t* offsets_ptr, size_t count) {
    if (count == 0) {
        return Status::OK();
    }
    const auto* offsets = reinterpret_cast<const uint64_t*>(offsets_ptr);
    size_t start_off = 0;
    for (size_t i = 0; i < count; ++i) {
        auto array_elem_size = offsets[i + 1] - offsets[i];
        uint32_t position_base = 0;
        for (auto j = start_off; j < start_off + array_elem_size; ++j) {
            if (nested_null_map != nullptr && nested_null_map[j] == 1) {
                continue;
            }
            const auto* value = reinterpret_cast<const Slice*>(
                    reinterpret_cast<const uint8_t*>(value_ptr) + j * field_size);
            uint32_t max_position = position_base;
            RETURN_IF_ERROR(_add_value_tokens(*value, _rid, position_base, &max_position));
            position_base = max_position + 1;
        }
        start_off += array_elem_size;
        ++_rid;
    }
    return Status::OK();
}

Status SniiIndexColumnWriter::add_nulls(uint32_t count) {
    _null_docids.reserve(_null_docids.size() + count);
    for (uint32_t i = 0; i < count; ++i) {
        _null_docids.push_back(_rid + i);
    }
    _rid += count;
    return Status::OK();
}

Status SniiIndexColumnWriter::add_array_nulls(const uint8_t* null_map, size_t num_rows) {
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
    return Status::OK();
}

Status SniiIndexColumnWriter::finish() {
    DCHECK(_term_buffer != nullptr);
    if (_has_positions && _rid > 0) {
        _term_buffer->add_token(::doris::snii::format::make_phrase_bigram_sentinel_term(), 0, 0);
    }
    auto status = _term_buffer->status();
    if (!status.ok()) {
        return Status::InternalError("SNII term buffer error: {}", status.to_string());
    }
    RETURN_IF_ERROR(_index_file_writer->add_snii_index(_index_meta, cast_set<uint32_t>(_rid),
                                                       std::move(_null_docids), _term_buffer.get(),
                                                       _config, _memory_reporter.get()));
    _index_file_writer->retain_snii_memory_reporter(std::move(_memory_reporter));
    _term_buffer.reset();
    return Status::OK();
}

void SniiIndexColumnWriter::close_on_error() {
    _term_buffer.reset();
    _memory_reporter.reset();
    _null_docids.clear();
}

} // namespace doris::segment_v2
