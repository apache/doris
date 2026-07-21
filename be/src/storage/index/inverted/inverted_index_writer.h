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
#include "storage/index/inverted/util/reader.h"
#include "storage/olap_common.h"
#include "storage/segment/common.h"

// SPIMI write types are held only as unique_ptr members (the SpimiIndexWriter
// facade and its TeeTokenStream), dereferenced only in the .cpp. Forward-
// declaring them here — instead of #include-ing spimi_index_writer.h /
// tee_token_stream.h (both of which transitively pull in the heavy
// posting_buffer.h) — keeps those SPIMI headers OUT of this header. This header
// is pulled in transitively by exec_env.h (→ effectively all of BE), so a
// posting_buffer.h edit used to recompile ~95 translation units; with the
// forward declarations a posting_buffer.h change only rebuilds the handful of
// SPIMI .cpp files plus inverted_index_writer.cpp.
namespace doris::segment_v2::inverted_index::spimi {
class SpimiIndexWriter;
class TeeTokenStream;
} // namespace doris::segment_v2::inverted_index::spimi

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

    // For tests: returns the resident bytes of the V4 SPIMI posting
    // buffer (arena + intern slots + per-term state + slice pool), or 0 when
    // this writer is on the V1/V2/V3 (CLucene) path. Defined out-of-line in the
    // .cpp because SpimiIndexWriter is only forward-declared in this header.
    size_t spimi_buffer_memory_usage() const override;
    void write_null_bitmap(lucene::store::IndexOutput* null_bitmap_out);
    Status finish() override;

private:
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

    // SPIMI write facade. Encapsulates the posting buffer, spill
    // manager, and segment emission logic. Created in
    // init_fulltext_index() for V4 (pure SPIMI) or shadow mode.
    // `nullptr` when SPIMI is not active.
    std::unique_ptr<segment_v2::inverted_index::spimi::SpimiIndexWriter> _spimi_writer = nullptr;
    // unique_ptr (not by-value) so this header only needs a forward declaration
    // of TeeTokenStream; constructed in init_fulltext_index() alongside the
    // facade. Non-owning over the upstream analyser stream it tees from.
    std::unique_ptr<segment_v2::inverted_index::spimi::TeeTokenStream> _spimi_tee;
    int32_t _spimi_doc_count = 0;
    bool _is_v4 = false;
    // Per-writer backstop: force a spill once the SPIMI buffer alone exceeds
    // this many resident bytes, independent of process-global pressure. Caps a
    // single column writer from hoarding memory when the process limit is huge.
    // Cached once at writer init; only meaningful in the V4 (_spimi_writer)
    // branch. min(2GiB, MemInfo::mem_limit()/20).
    int64_t _spimi_backstop_bytes = 0;

    // Row counter throttling the EXPENSIVE per-row spill gate (process memory
    // watermarks + MemoryUsage + reserve). The cheap config-driven
    // (inverted_index_ram_buffer_size) ShouldFlush() latch is still checked
    // every row; the expensive checks run only every
    // inverted_index_spimi_spill_check_interval_rows rows. Persists across the
    // batched add_values calls.
    int64_t _spimi_gate_counter = 0;

    // True when process memory PRESSURE warrants a spill (the expensive half of
    // the gate, throttled to every N rows): OR of (1) process hard-mem-limit
    // exceeded (force), (2) process soft pressure + buffer past the opportunistic
    // min, (3) per-writer backstop. The cheap config-driven
    // (inverted_index_ram_buffer_size) ShouldFlush() budget floor is checked
    // separately every row. Only the V4 branch calls this; _spimi_writer
    // must be non-null.
    bool ShouldSpillUnderPressure() const;
};

} // namespace segment_v2
} // namespace doris