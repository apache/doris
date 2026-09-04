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
// AnalyzerProviderPtr 已经在 inverted_index_parser.h 的闭包里（那条路径几乎所有 TU 都会
// 拉到），这里显式写出来只是让依赖可见，不增加任何前向闭包成本。
#include "storage/index/inverted/analyzer/analyzer_provider.h"
#include "storage/index/inverted/common_grams/common_grams_segment_metadata.h"
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
}

namespace doris::segment_v2::inverted_index {
class CommonGramsFilter;
}

namespace doris::segment_v2 {

class SniiIndexColumnWriter final : public IndexColumnWriter {
public:
    SniiIndexColumnWriter(IndexFileWriter* index_file_writer, const TabletIndex* index_meta,
                          FieldType value_type,
                          std::optional<inverted_index::CommonGramsSegmentMetadata>
                                  common_grams_metadata_seed = std::nullopt);
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
    uint64_t scoring_token_count_for_test() const { return _scoring_token_count; }
    ::doris::snii::format::IndexConfig config_for_test() const { return _config; }
    const std::optional<gram::GramScheme>& gram_scheme_for_test() const { return _gram_scheme; }
    bool has_common_grams_metadata_seed_for_test() const {
        return _common_grams_metadata_seed.has_value();
    }
    inverted_index::CommonGramsSegmentMetadata common_grams_metadata_for_test() const {
        return _build_common_grams_metadata();
    }
    void set_analysis_for_test(inverted_index::ReaderPtr reader,
                               std::shared_ptr<lucene::analysis::Analyzer> analyzer) {
        _should_analyzer = true;
        _char_string_reader = std::move(reader);
        _analyzer = std::move(analyzer);
    }
#endif

private:
    // init() 的前半段：建好 char filter reader 与（_should_analyzer 时）唯一的 analyzer
    // provider，再由 _apply_gram_family_scheme 定下 gram 族判定。必须先于 SpimiTermBuffer
    // 构造调用。抛出的 CLuceneError / Exception 统一转成 INVERTED_INDEX_ANALYZER_ERROR。
    Status _create_analyzer_provider(inverted_index::AnalyzerProviderPtr* analyzer_provider);
    // 从 provider 取 gram 方案，并施加 gram 族的后果（强制 docs-only）。
    void _apply_gram_family_scheme(const inverted_index::AnalyzerProviderPtr& analyzer_provider);
    Status _add_value_tokens(const Slice& value, uint32_t docid, uint32_t position_base,
                             uint32_t* max_position, uint32_t* semantic_length);
    inverted_index::CommonGramsSegmentMetadata _build_common_grams_metadata() const;
    // Mirrors _null_docids' capacity into _memory_reporter (delta-charged);
    // release_all zeroes the charge (finish() handoff / close_on_error()).
    void _report_null_docids_capacity(bool release_all = false);
    void _report_encoded_norms_capacity(bool release_all = false);
    Status _latch_analysis_failure(Status status);

    IndexFileWriter* _index_file_writer = nullptr;
    const TabletIndex* _index_meta = nullptr;
    bool _should_analyzer = false;
    bool _has_positions = false;
    const bool _is_char;
    const bool _common_grams_build_enabled;
    bool _uses_common_grams = false;
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
    // gram 族（ngram tokenizer + mode 属性）analyzer 的方案参数；由
    // _apply_gram_family_scheme 在 term buffer 构造之前、从本写入器自己创建的 analyzer
    // provider 上取得，一旦有值就强制 docs-only。内置 analyzer、带 filter 的 analyzer、
    // 带索引级 char_filter 的索引一律为 nullopt（R21/R22）。
    std::optional<gram::GramScheme> _gram_scheme;
    InvertedIndexAnalyzerConfig _analyzer_config;
    inverted_index::ReaderPtr _char_string_reader;
    std::shared_ptr<lucene::analysis::Analyzer> _analyzer;
    inverted_index::CommonGramsFilter* _common_grams_filter = nullptr;
    std::unique_ptr<::doris::snii::writer::MemoryReporter> _memory_reporter;
    std::unique_ptr<::doris::snii::writer::SpimiTermBuffer> _term_buffer;
    std::vector<uint32_t> _null_docids;
    std::vector<uint8_t> _encoded_norms;
    uint64_t _scoring_token_count = 0;
    std::optional<inverted_index::CommonGramsSegmentMetadata> _common_grams_metadata_seed;
    // Bytes of _null_docids capacity currently mirrored into _memory_reporter
    // (and through it the SNII index-build observation tracker). Re-charged on
    // growth in add_nulls / add_array_nulls, released in finish() / close_on_error() --
    // without it a large interleaved-null segment accumulates untracked RSS the
    // G09 limiter cannot see.
    int64_t _null_docids_charged_bytes = 0;
    int64_t _encoded_norms_charged_bytes = 0;
    Status _failure_status = Status::OK();
};

} // namespace doris::segment_v2
