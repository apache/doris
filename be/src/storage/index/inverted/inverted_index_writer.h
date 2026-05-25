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

#include <CLucene.h> // IWYU pragma: keep
#include <CLucene/util/bkd/bkd_writer.h>

#include <roaring/roaring.hh>

#include "CLucene/search/Similarity.h"
#include "storage/index/index_file_writer.h"
#include "storage/index/index_writer.h"
#include "storage/index/inverted/inverted_index_parser.h"
#include "storage/index/inverted/spimi/posting_buffer.h"
#include "storage/index/inverted/spimi/tee_token_stream.h"
#include "storage/index/inverted/util/reader.h"
#include "storage/olap_common.h"
#include "storage/segment/common.h"

namespace doris {

class KeyCoder;

template <FieldType field_type>
struct CppTypeTraits;

namespace segment_v2 {

using namespace doris::segment_v2::inverted_index;

template <FieldType field_type>
class InvertedIndexColumnWriter : public IndexColumnWriter {
public:
    using CppType = typename CppTypeTraits<field_type>::CppType;

    InvertedIndexColumnWriter(const std::string& field_name, IndexFileWriter* index_file_writer,
                              const TabletIndex* index_meta, const bool single_field = true);
    ~InvertedIndexColumnWriter() override;

    Status init() override;
    void close_on_error() override;
    Status init_bkd_index();
    Result<ReaderPtr> create_char_string_reader(CharFilterMap& char_filter_map);
    Status open_index_directory();
    std::unique_ptr<lucene::index::IndexWriter> create_index_writer();
    Status create_field(lucene::document::Field** field);
    Result<std::shared_ptr<lucene::analysis::Analyzer>> create_analyzer(
            const InvertedIndexAnalyzerConfig& analyzer_config);
    Status init_fulltext_index();
    Status add_document();
    Status add_null_document();
    Status add_nulls(uint32_t count) override;
    Status add_array_nulls(const uint8_t* null_map, size_t num_rows) override;
    Status new_inverted_index_field(const char* field_value_data, size_t field_value_size);
    void new_char_token_stream(const char* s, size_t len, lucene::document::Field* field);
    void new_field_value(const char* s, size_t len, lucene::document::Field* field);
    void new_field_char_value(const char* s, size_t len, lucene::document::Field* field);
    Status add_values(const std::string fn, const void* values, size_t count) override;
    Status add_array_values(size_t field_size, const void* value_ptr,
                            const uint8_t* nested_null_map, const uint8_t* offsets_ptr,
                            size_t count) override;
    Status add_numeric_values(const void* values, size_t count);
    Status add_value(const CppType& value);

    int64_t size() const override;

    // For tests: returns the resident bytes of the SPIMI shadow buffer
    // (12 B/record + arena + intern slots) when the flag is on, or 0
    // when the buffer was never allocated.
    size_t spimi_buffer_memory_usage() const override {
        return _spimi_buffer == nullptr ? 0 : _spimi_buffer->MemoryUsage();
    }
    bool has_spimi_buffer() const override { return _spimi_buffer != nullptr; }
    void write_null_bitmap(lucene::store::IndexOutput* null_bitmap_out);
    Status finish() override;

private:
    // C2 — release the SPIMI shadow buffer at the entry of any array
    // overload. Array<string> joins per-element strings into one
    // tokenized stream (CollectionValue overload) or wraps each element
    // in its own non-reusable TokenStream (void-ptr overload); in both
    // cases the SPIMI tee is NOT installed for the per-element stream
    // and a live buffer would emit an empty/wrong-position shadow
    // segment alongside the full CLucene segment. The helper centralises
    // the latch (was duplicated as inline comments across both overloads,
    // and the second site missed the "release before the nullptr check"
    // requirement that the first site implements).
    //
    // Idempotent: a second call after `_spimi_buffer == nullptr` is a
    // no-op. Logs at most once per writer instance via LOG_FIRST_N.
    void release_spimi_shadow_for_array_path();

    rowid_t _rid = 0;
    uint32_t _row_ids_seen_for_bkd = 0;
    roaring::Roaring _null_bitmap;
    uint64_t _reverted_index_size;

    std::unique_ptr<lucene::document::Document> _doc = nullptr;
    lucene::document::Field* _field = nullptr;
    bool _single_field = true;
    // Since _index_writer's write.lock is created by _dir.lockFactory,
    // _dir must destruct after _index_writer, so _dir must be defined before _index_writer.
    std::shared_ptr<DorisFSDirectory> _dir = nullptr;
    std::unique_ptr<lucene::index::IndexWriter> _index_writer = nullptr;
    std::shared_ptr<lucene::analysis::Analyzer> _analyzer = nullptr;
    std::unique_ptr<lucene::search::Similarity> _similarity = nullptr;
    ReaderPtr _char_string_reader = nullptr;
    std::shared_ptr<lucene::util::bkd::bkd_writer> _bkd_writer = nullptr;
    InvertedIndexAnalyzerConfig _analyzer_config;
    const KeyCoder* _value_key_coder;
    const TabletIndex* _index_meta;
    std::wstring _field_name;
    IndexFileWriter* _index_file_writer;
    uint32_t _ignore_above;
    bool _should_analyzer = false;

    // SPIMI shadow-mode accumulator. Populated alongside the CLucene
    // IndexWriter path when `config::inverted_index_fulltext_spimi` is true
    // at init_fulltext_index() time. At finish() the buffer is emitted into
    // a sibling segment (`_spimi_0.tis` / `.tii` / `.frq` / `.prx` / `.fnm`
    // + `spimi_segments_1` + `spimi_segments.gen`) so the segment can be
    // compared against CLucene's primary output without disrupting the
    // existing write path. `nullptr` when the flag is off.
    std::unique_ptr<segment_v2::inverted_index::spimi::SpimiPostingBuffer> _spimi_buffer = nullptr;
    segment_v2::inverted_index::spimi::TeeTokenStream _spimi_tee;
    int32_t _spimi_doc_count = 0;

    // V4 storage format = pure SPIMI write path. When true, the
    // writer does NOT create a CLucene IndexWriter / Document /
    // Field; tokens flow directly from the analyzer's
    // reusableTokenStream into `_spimi_buffer`. This is what
    // delivers the SPIMI project's 50%+ memory savings target —
    // running CLucene alongside (the legacy shadow mode) doubles
    // RAM. V4 makes SPIMI standalone.
    bool _is_v4 = false;
};

} // namespace segment_v2
} // namespace doris