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
    Status add_values(const std::string name, const void* values, size_t count) override;
    Status add_array_values(size_t field_size, const void* value_ptr,
                            const uint8_t* nested_null_map, const uint8_t* offsets_ptr,
                            size_t count) override;
    Status add_nulls(uint32_t count) override;
    Status add_array_nulls(const uint8_t* null_map, size_t num_rows) override;
    Status finish() override;
    int64_t size() const override { return 0; }
    void close_on_error() override;

private:
    Status _add_value_tokens(const Slice& value, uint32_t docid, uint32_t position_base,
                             uint32_t* max_position);
    Status _add_phrase_bigram_tokens(const std::vector<TermInfo>& terms, uint32_t docid,
                                     uint32_t position_base);
    Status _analyze(const Slice& value, std::vector<TermInfo>* terms);

    IndexFileWriter* _index_file_writer = nullptr;
    const TabletIndex* _index_meta = nullptr;
    bool _should_analyzer = false;
    bool _has_positions = false;
    uint32_t _ignore_above = 0;
    uint32_t _rid = 0;
    ::doris::snii::format::IndexConfig _config = ::doris::snii::format::IndexConfig::kDocsOnly;
    InvertedIndexAnalyzerConfig _analyzer_config;
    inverted_index::ReaderPtr _char_string_reader;
    std::shared_ptr<lucene::analysis::Analyzer> _analyzer;
    std::unique_ptr<::doris::snii::writer::MemoryReporter> _memory_reporter;
    std::unique_ptr<::doris::snii::writer::SpimiTermBuffer> _term_buffer;
    std::vector<uint32_t> _null_docids;
    // Reused across every _add_phrase_bigram_tokens call: clear() keeps the
    // backing capacity so the per-row phrase-bigram build stops re-allocating a
    // fresh positioned-term vector on each text row/array element. Single-threaded
    // per-column build state (see _add_phrase_bigram_tokens).
    std::vector<PhrasePositionedTerm> _bigram_positioned;
};

} // namespace doris::segment_v2
