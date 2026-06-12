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

#include "storage/index/inverted/inverted_index_writer.h"

#include <cstdlib>

#include "common/config.h"
#include "runtime/memory/global_memory_arbitrator.h"
#include "runtime/memory/thread_mem_tracker_mgr.h"
#include "runtime/thread_context.h"
#include "storage/index/inverted/analyzer/analyzer.h"
#include "storage/index/inverted/inverted_index_common.h"
#include "storage/index/inverted/inverted_index_fs_directory.h"
#include "storage/index/inverted/spimi/spimi_index_writer.h"
#include "storage/index/inverted/spimi/tee_token_stream.h"
#include "storage/key_coder.h"
#include "storage/tablet/tablet_schema.h"
#include "util/faststring.h"
#include "util/mem_info.h"

namespace doris::segment_v2 {

const int32_t MAX_FIELD_LEN = 0x7FFFFFFFL;
const int32_t MERGE_FACTOR = 100000000;
const int32_t MAX_LEAF_COUNT = 1024;
const float MAXMBSortInHeap = 512.0 * 8;
const int DIMS = 1;

template <FieldType field_type>
InvertedIndexColumnWriter<field_type>::InvertedIndexColumnWriter(const std::string& field_name,
                                                                 IndexFileWriter* index_file_writer,
                                                                 const TabletIndex* index_meta,
                                                                 const bool single_field)
        : _single_field(single_field),
          _index_meta(index_meta),
          _index_file_writer(index_file_writer) {
    _should_analyzer =
            inverted_index::InvertedIndexAnalyzer::should_analyzer(_index_meta->properties());
    _value_key_coder = get_key_coder(field_type);
    _field_name = StringUtil::string_to_wstring(field_name);
}

template <FieldType field_type>
InvertedIndexColumnWriter<field_type>::~InvertedIndexColumnWriter() {
    // V4 holds `_dir` directly without `_index_writer`. If the
    // writer dies before `finish()` runs (exception in
    // `add_values`, cancelled load), we still need to delete the
    // partial files on disk — without this branch, the V4 path
    // leaked the directory and its partial outputs on every error
    // path.
    if (_index_writer != nullptr || (_is_v4 && _dir != nullptr)) {
        close_on_error();
    }
}

template <FieldType field_type>
size_t InvertedIndexColumnWriter<field_type>::spimi_buffer_memory_usage() const {
    // Out-of-line because SpimiIndexWriter is only forward-declared in the
    // header (so a posting_buffer.h edit doesn't recompile every TU that
    // transitively includes inverted_index_writer.h via exec_env.h).
    return _spimi_writer == nullptr ? 0 : _spimi_writer->MemoryUsage();
}

template <FieldType field_type>
Status InvertedIndexColumnWriter<field_type>::init() {
    try {
        DBUG_EXECUTE_IF("InvertedIndexColumnWriter::init_field_type_not_supported", {
            return Status::Error<doris::ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                    "Field type not supported");
        })
        DBUG_EXECUTE_IF("InvertedIndexColumnWriter::init_inverted_index_writer_init_error",
                        { _CLTHROWA(CL_ERR_IO, "debug point: init index error"); })
        if constexpr (field_is_slice_type(field_type)) {
            return init_fulltext_index();
        } else if constexpr (field_is_numeric_type(field_type)) {
            return init_bkd_index();
        }
        return Status::Error<doris::ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                "Field type not supported");
    } catch (const CLuceneError& e) {
        LOG(WARNING) << "Inverted index writer init error occurred: " << e.what();
        return Status::Error<doris::ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                "Inverted index writer init error occurred: {}", e.what());
    }
}

template <FieldType field_type>
void InvertedIndexColumnWriter<field_type>::close_on_error() {
    try {
        DBUG_EXECUTE_IF("InvertedIndexColumnWriter::close_on_error_throw_exception",
                        { _CLTHROWA(CL_ERR_IO, "debug point: close on error"); })
        // delete directory must be done before index_writer close
        // because index_writer will close the directory
        if (_dir) {
            _dir->deleteDirectory();
            // Null `_dir` so a second call (e.g. dtor after the V4
            // add_values catch already called close_on_error) is a
            // true no-op rather than producing a spurious
            // `delete_directory NOT_FOUND` log line.
            _dir.reset();
        }
        if (_index_writer) {
            _index_writer->close();
            _index_writer.reset();
        }
        // V4: release in-memory spill data and buffer.
        if (_spimi_writer) {
            _spimi_writer->Cleanup();
        }
    } catch (CLuceneError& e) {
        LOG(ERROR) << "InvertedIndexWriter close_on_error failure: " << e.what();
    } catch (const std::exception& e) {
        // close_on_error MUST be no-throw so callers in catch
        // handlers (V4 add_values, dtor) can rely on it. Catch
        // any non-CLucene exception too and log instead of
        // re-throwing.
        LOG(ERROR) << "InvertedIndexWriter close_on_error unexpected: " << e.what();
    } catch (...) {
        LOG(ERROR) << "InvertedIndexWriter close_on_error unknown exception";
    }
}

template <FieldType field_type>
Status InvertedIndexColumnWriter<field_type>::init_bkd_index() {
    size_t value_length = sizeof(CppType);
    // NOTE: initialize with 0, set to max_row_id when finished.
    int32_t max_doc = 0;
    int32_t total_point_count = std::numeric_limits<std::int32_t>::max();
    _bkd_writer = std::make_shared<lucene::util::bkd::bkd_writer>(
            max_doc, DIMS, DIMS, value_length, MAX_LEAF_COUNT, MAXMBSortInHeap, total_point_count,
            true, config::max_depth_in_bkd_tree);
    DBUG_EXECUTE_IF("InvertedIndexColumnWriter::init_bkd_index_throw_error",
                    { _CLTHROWA(CL_ERR_IllegalArgument, "debug point: create bkd_writer error"); })
    return open_index_directory();
}

template <FieldType field_type>
Result<ReaderPtr> InvertedIndexColumnWriter<field_type>::create_char_string_reader(
        CharFilterMap& char_filter_map) {
    try {
        return inverted_index::InvertedIndexAnalyzer::create_reader(char_filter_map);
    } catch (CLuceneError& e) {
        return ResultError(Status::Error<doris::ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                "inverted index create string reader failed: {}", e.what()));
    }
}

template <FieldType field_type>
Status InvertedIndexColumnWriter<field_type>::open_index_directory() {
    DBUG_EXECUTE_IF("InvertedIndexColumnWriter::open_index_directory_error", {
        return Status::Error<ErrorCode::INTERNAL_ERROR>("debug point: open_index_directory_error");
    })
    _dir = DORIS_TRY(_index_file_writer->open(_index_meta));
    return Status::OK();
}

template <FieldType field_type>
std::unique_ptr<lucene::index::IndexWriter>
InvertedIndexColumnWriter<field_type>::create_index_writer() {
    bool create_index = true;
    bool close_dir_on_shutdown = true;
    auto index_writer = std::make_unique<lucene::index::IndexWriter>(
            _dir.get(), _analyzer.get(), create_index, close_dir_on_shutdown);
    DBUG_EXECUTE_IF("InvertedIndexColumnWriter::create_index_writer_setRAMBufferSizeMB_error",
                    { index_writer->setRAMBufferSizeMB(-100); })
    DBUG_EXECUTE_IF("InvertedIndexColumnWriter::create_index_writer_setMaxBufferedDocs_error",
                    { index_writer->setMaxBufferedDocs(1); })
    DBUG_EXECUTE_IF("InvertedIndexColumnWriter::create_index_writer_setMergeFactor_error",
                    { index_writer->setMergeFactor(1); })
    index_writer->setRAMBufferSizeMB(static_cast<float>(config::inverted_index_ram_buffer_size));
    index_writer->setMaxBufferedDocs(config::inverted_index_max_buffered_docs);
    index_writer->setMaxFieldLength(MAX_FIELD_LEN);
    index_writer->setMergeFactor(MERGE_FACTOR);
    index_writer->setUseCompoundFile(false);
    index_writer->setEnableCorrectTermWrite(config::enable_inverted_index_correct_term_write);
    index_writer->setSimilarity(_similarity.get());

    return index_writer;
}

template <FieldType field_type>
Status InvertedIndexColumnWriter<field_type>::create_field(lucene::document::Field** field) {
    int field_config =
            int(lucene::document::Field::STORE_NO) | int(lucene::document::Field::INDEX_NONORMS);
    field_config |= _should_analyzer ? int32_t(lucene::document::Field::INDEX_TOKENIZED)
                                     : int32_t(lucene::document::Field::INDEX_UNTOKENIZED);
    *field = new lucene::document::Field(_field_name.c_str(), field_config);
    const bool omit_term_freq_and_positions =
            !(get_parser_phrase_support_string_from_properties(_index_meta->properties()) ==
              INVERTED_INDEX_PARSER_PHRASE_SUPPORT_YES);
    (*field)->setOmitTermFreqAndPositions(omit_term_freq_and_positions);
    // Norms (length normalization) are only consumed by relevance scoring, which
    // needs term frequencies. A DOCS_ONLY field (omit_term_freq_and_positions,
    // i.e. support_phrase off) can never be scored, so its per-doc norms are pure
    // dead weight on disk -- the SPIMI/V4 path omits them unconditionally. Keep
    // norms only for analyzed fields that still carry freq+positions.
    if (_should_analyzer && !omit_term_freq_and_positions) {
        (*field)->setOmitNorms(false);
    }

    DBUG_EXECUTE_IF("InvertedIndexColumnWriter::always_omit_norms",
                    { (*field)->setOmitNorms(false); });

    DBUG_EXECUTE_IF("InvertedIndexColumnWriter::create_field_v3", {
        if (_index_file_writer->get_storage_format() != InvertedIndexStorageFormatPB::V3) {
            return Status::Error<doris::ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                    "debug point: InvertedIndexColumnWriter::create_field_v3 error");
        }
    })
    if (_index_file_writer->get_storage_format() >= InvertedIndexStorageFormatPB::V3) {
        (*field)->setIndexVersion(IndexVersion::kV4);
        // Only effective in v3
        std::string dict_compression =
                get_parser_dict_compression_from_properties(_index_meta->properties());
        DBUG_EXECUTE_IF("InvertedIndexColumnWriter::create_field_dic_compression", {
            if (dict_compression != INVERTED_INDEX_PARSER_TRUE) {
                return Status::Error<doris::ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                        "debug point: "
                        "InvertedIndexColumnWriter::create_field_dic_compression error");
            }
        })
        if (dict_compression == INVERTED_INDEX_PARSER_TRUE) {
            (*field)->updateFlag(FlagBits::DICT_COMPRESS);
        }
    }
    return Status::OK();
}

template <FieldType field_type>
Result<std::shared_ptr<lucene::analysis::Analyzer>>
InvertedIndexColumnWriter<field_type>::create_analyzer(
        const InvertedIndexAnalyzerConfig& analyzer_config) {
    try {
        return inverted_index::InvertedIndexAnalyzer::create_analyzer(&analyzer_config);
    } catch (CLuceneError& e) {
        return ResultError(Status::Error<doris::ErrorCode::INVERTED_INDEX_ANALYZER_ERROR>(
                "inverted index create analyzer failed: {}", e.what()));
    } catch (Exception& e) {
        return ResultError(Status::Error<doris::ErrorCode::INVERTED_INDEX_ANALYZER_ERROR>(
                "inverted index create analyzer failed: {}", e.what()));
    }
}

template <FieldType field_type>
Status InvertedIndexColumnWriter<field_type>::init_fulltext_index() {
    // SPIMI is the new write path that replaces CLucene's per-term Posting +
    // byte-pool slice machinery with a flat record buffer. It is currently
    // opt-in (default off) and partially landed: the writer infrastructure
    // (posting buffer, term dictionary, skip-list, freq/prox encoder, field
    // infos, segments_N writer, file-backed adapter) is all in place and
    // unit-tested under be/{src,test}/storage/index/inverted/spimi/. The
    // final wiring — tapping the analyzer's token stream into
    // SpimiFulltextWriter::AddOccurrence and replacing IndexWriter::close()
    // with SpimiFulltextWriter::Finish — is the next step.
    // V4 storage format = pure SPIMI write path. No CLucene
    // IndexWriter/Document/Field — tokens go directly from the
    // analyzer into `_spimi_buffer`. This is where SPIMI's memory-
    // savings target lives (running CLucene alongside doubles RAM,
    // which is exactly what shadow mode does and what V4 explicitly
    // does NOT do).
    _is_v4 = (_index_file_writer != nullptr &&
              _index_file_writer->get_storage_format() == InvertedIndexStorageFormatPB::V4);
    if (_is_v4 && !_should_analyzer) {
        // V4 today only implements the analyzed-fulltext path. A
        // non-analyzed (keyword) string column on a V4 tablet would
        // load successfully via add_values's "keyword" branch but
        // every query against it would error at read time
        // (column_reader.cpp routes V4+!should_analyzer to
        // INVERTED_INDEX_NOT_SUPPORTED). Fail fast at writer init
        // so the user sees the misconfiguration BEFORE loading
        // data; mirrors the read-side rejection.
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                "V4 inverted index storage format only supports analyzed fulltext columns "
                "(parser != 'none' / 'unknown'); use V1/V2/V3 for keyword columns.");
    }
    if (_is_v4) {
        const std::string field_name_utf8(_field_name.begin(), _field_name.end());
        // Same flag Finish() passes via SpimiFinishConfig (see ~L997). Captured
        // here so spill segments omit freq+positions in lockstep with the final
        // segment when the field has no phrase support.
        const bool omit_term_freq_and_positions =
                !(get_parser_phrase_support_string_from_properties(_index_meta->properties()) ==
                  INVERTED_INDEX_PARSER_PHRASE_SUPPORT_YES);
        _spimi_writer = std::make_unique<segment_v2::inverted_index::spimi::SpimiIndexWriter>(
                field_name_utf8, /*is_v4=*/true, omit_term_freq_and_positions);
        _spimi_tee = std::make_unique<segment_v2::inverted_index::spimi::TeeTokenStream>();
        // Per-writer backstop: cap a single column writer at min(2GiB,
        // mem_limit/20) of SPIMI buffer before forcing a spill, independent of
        // process-global pressure. Cached once here (mem_limit is effectively
        // fixed for the process lifetime).
        _spimi_backstop_bytes = std::min<int64_t>(int64_t {2} << 30, MemInfo::mem_limit() / 20);
        LOG_FIRST_N(INFO, 1) << "V4 storage format: pure SPIMI write path (no CLucene)";
    }
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
    RETURN_IF_ERROR(open_index_directory());
    _char_string_reader = DORIS_TRY(create_char_string_reader(_analyzer_config.char_filter_map));
    if (_should_analyzer) {
        _analyzer = DORIS_TRY(create_analyzer(_analyzer_config));
    }
    _similarity = std::make_unique<lucene::search::LengthSimilarity>();
    if (!_is_v4) {
        // V1/V2/V3: classic CLucene write path. V4 skips all of
        // these — no IndexWriter, no Document, no Field. Saves the
        // CLucene write-side allocations that the SPIMI project
        // exists to eliminate.
        _index_writer = create_index_writer();
        _doc = std::make_unique<lucene::document::Document>();
        if (_single_field) {
            RETURN_IF_ERROR(create_field(&_field));
            _doc->add(*_field);
        } else {
            // array's inverted index do need create field first
            _doc->setNeedResetFieldData(true);
        }
    }
    auto ignore_above_value =
            get_parser_ignore_above_value_from_properties(_index_meta->properties());
    _ignore_above = std::stoi(ignore_above_value);
    return Status::OK();
}

template <FieldType field_type>
Status InvertedIndexColumnWriter<field_type>::add_document() {
    DBUG_EXECUTE_IF("inverted_index_writer.add_document", { return Status::OK(); });

    try {
        _index_writer->addDocument(_doc.get());
        DBUG_EXECUTE_IF("InvertedIndexColumnWriter::add_document_throw_error",
                        { _CLTHROWA(CL_ERR_IO, "debug point: add_document io error"); })
    } catch (const CLuceneError& e) {
        close_on_error();
        return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                "CLuceneError add_document: {}", e.what());
    }
    return Status::OK();
}

template <FieldType field_type>
Status InvertedIndexColumnWriter<field_type>::add_null_document() {
    try {
        _index_writer->addNullDocument(_doc.get());
        DBUG_EXECUTE_IF("InvertedIndexColumnWriter::add_null_document_throw_error",
                        { _CLTHROWA(CL_ERR_IO, "debug point: add_null_document io error"); })
    } catch (const CLuceneError& e) {
        close_on_error();
        return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                "CLuceneError add_null_document: {}", e.what());
    }
    return Status::OK();
}

template <FieldType field_type>
Status InvertedIndexColumnWriter<field_type>::add_nulls(uint32_t count) {
    _null_bitmap.addRange(_rid, _rid + count);
    _rid += count;
    if constexpr (field_is_slice_type(field_type)) {
        if (_is_v4) {
            // V4: null bitmap already updated above; no CLucene
            // call needed. Update _spimi_doc_count so finish() emits
            // the right segment.doc_count for downstream readers.
            // V4 is "pure SPIMI" — a null `_spimi_buffer` here is a
            // programmer error in the init flow (e.g. add_nulls
            // called after a saturation-triggered reset). DORIS_CHECK
            // surfaces the bug rather than silently producing a
            // segment with a doc_count that the buffer never saw.
            DORIS_CHECK(_spimi_writer->HasBuffer());
            if (static_cast<int32_t>(_rid) > _spimi_doc_count) {
                _spimi_doc_count = static_cast<int32_t>(_rid);
            }
            return Status::OK();
        }
        DBUG_EXECUTE_IF("InvertedIndexColumnWriter::add_nulls_field_nullptr", { _field = nullptr; })
        DBUG_EXECUTE_IF("InvertedIndexColumnWriter::add_nulls_index_writer_nullptr",
                        { _index_writer = nullptr; })
        if (_field == nullptr || _index_writer == nullptr) {
            LOG(ERROR) << "field or index writer is null in inverted index writer.";
            return Status::InternalError("field or index writer is null in inverted index writer");
        }

        for (int i = 0; i < count; ++i) {
            RETURN_IF_ERROR(add_null_document());
        }
    }
    return Status::OK();
}

template <FieldType field_type>
Status InvertedIndexColumnWriter<field_type>::add_array_nulls(const uint8_t* null_map,
                                                              size_t num_rows) {
    DCHECK(_rid >= num_rows);
    if (num_rows == 0 || null_map == nullptr) {
        return Status::OK();
    }
    std::vector<uint32_t> null_indices;
    null_indices.reserve(num_rows / 8);

    // because _rid is the row id in block, not segment, and we add data before we add nulls,
    // so we need to subtract num_rows to get the row id in segment
    for (size_t i = 0; i < num_rows; i++) {
        if (null_map[i] == 1) {
            null_indices.push_back(cast_set<uint32_t>(_rid - num_rows + i));
        }
    }

    if (!null_indices.empty()) {
        _null_bitmap.addMany(null_indices.size(), null_indices.data());
    }

    return Status::OK();
}

template <FieldType field_type>
Status InvertedIndexColumnWriter<field_type>::new_inverted_index_field(const char* field_value_data,
                                                                       size_t field_value_size) {
    try {
        if (_should_analyzer) {
            new_char_token_stream(field_value_data, field_value_size, _field);
        } else {
            new_field_char_value(field_value_data, field_value_size, _field);
        }
    } catch (const CLuceneError& e) {
        return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                "CLuceneError create new index field error: {}", e.what());
    }
    return Status::OK();
}

template <FieldType field_type>
void InvertedIndexColumnWriter<field_type>::new_char_token_stream(const char* s, size_t len,
                                                                  lucene::document::Field* field) {
    _char_string_reader->init(s, cast_set<int32_t>(len), false);
    DBUG_EXECUTE_IF(
            "InvertedIndexColumnWriter::new_char_token_stream__char_string_reader_init_"
            "error",
            {
                _CLTHROWA(CL_ERR_UnsupportedOperation,
                          "UnsupportedOperationException: CLStream::init");
            })
    auto* stream = _analyzer->reusableTokenStream(field->name(), _char_string_reader);
    if (_spimi_writer != nullptr && _spimi_writer->HasBuffer()) {
        // SPIMI shadow mode: wrap the analyser's reusable stream in a tee
        // so every token CLucene's addDocument observes is also copied
        // into the SPIMI buffer with the right doc id and position. This
        // guarantees the two segments share an identical token sequence
        // without re-tokenising. The tee is a non-owning member; the
        // upstream stream's lifetime is managed by the analyser.
        _spimi_tee->Configure(stream, _spimi_writer->buffer(), static_cast<uint32_t>(_rid));
        field->setValue(_spimi_tee.get(), /*own_stream=*/false);
    } else {
        field->setValue(stream);
    }
}

template <FieldType field_type>
void InvertedIndexColumnWriter<field_type>::new_field_value(const char* s, size_t len,
                                                            lucene::document::Field* field) {
    auto* field_value = lucene::util::Misc::_charToWide(s, len);
    field->setValue(field_value, false);
    // setValue did not duplicate value, so we don't have to delete
    //_CLDELETE_ARRAY(field_value)
}

template <FieldType field_type>
void InvertedIndexColumnWriter<field_type>::new_field_char_value(const char* s, size_t len,
                                                                 lucene::document::Field* field) {
    field->setValue((char*)s, len);
}

template <FieldType field_type>
bool InvertedIndexColumnWriter<field_type>::ShouldSpillUnderPressure() const {
    DCHECK(_spimi_writer != nullptr);
    // Process-pressure spill triggers ONLY (the cheap 256MiB ShouldFlush() hard
    // floor is checked separately every row in add_values; this is the expensive
    // half, throttled to every inverted_index_spimi_spill_check_interval_rows).
    // (1) Process hard mem limit exceeded → force a spill regardless of size.
    if (GlobalMemoryArbitrator::is_exceed_hard_mem_limit()) {
        return true;
    }
    const int64_t mem = _spimi_writer->MemoryUsage();
    // (2) Soft pressure + buffer past the opportunistic min. Avoids tiny
    //     segments under transient soft pressure.
    if (GlobalMemoryArbitrator::is_exceed_soft_mem_limit() &&
        mem >= (config::inverted_index_spimi_min_spill_mem_mb << 20)) {
        return true;
    }
    // (3) Per-writer backstop.
    return mem >= _spimi_backstop_bytes;
}

template <FieldType field_type>
Status InvertedIndexColumnWriter<field_type>::add_values(const std::string fn, const void* values,
                                                         size_t count) {
    if constexpr (field_is_slice_type(field_type)) {
        if (_is_v4) {
            // V4 = pure SPIMI. No CLucene Document/Field/IndexWriter
            // touched. Tokens flow direct: analyzer.reusableTokenStream
            // → next() loop → SpimiPostingBuffer::Append. This is the
            // path that delivers the SPIMI memory-savings target.
            //
            // The analyzer / token-stream calls below can throw
            // CLuceneError (custom tokenizers, filter exceptions);
            // wrap the whole loop in try/catch so CLucene exceptions
            // and DORIS_CHECK-thrown doris::Exception convert to
            // Status rather than escaping `add_values` past the
            // segment writer's caller. Matches the catch placement
            // in `add_document()` for the V1/V2/V3 path.
            try {
                // P2: return any unused growth reservation at scope end. The
                // reserve below only ever charges incremental growth granules
                // (never the full MemoryUsage()), so this DEFER is the matching
                // release for whatever try_reserve left outstanding.
                DEFER_RELEASE_RESERVED();
                // SPIMI write memory is ALWAYS tracked under the synchronous
                // attached limiter (LOAD / COMPACTION / SCHEMA_CHANGE depending
                // on the caller) — never Orphan. Cheap debug guard catches a
                // future thread-move that would silently mis-account / make the
                // process-global watermark gate and try_reserve no-ops. Assert
                // NON-Orphan (any valid Type), not LOAD specifically.
                DCHECK(thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker()->label() !=
                       "Orphan")
                        << "V4 SPIMI add_values running under Orphan mem tracker";
                const int64_t kSpimiReserveGranule = config::inverted_index_spimi_reserve_granule_mb
                                                     << 20;
                const int64_t kSpimiMinSpillBytes = config::inverted_index_spimi_min_spill_mem_mb
                                                    << 20;
                const auto* v = (Slice*)values;
                // Hot path: fetch the posting buffer once. AppendToken/Saturated
                // are thin facade forwarders living in a separate TU (BE builds
                // without LTO), so calling the buffer directly drops a cross-TU
                // call per token — Saturated() then inlines to a single load from
                // posting_buffer.h, and Append() loses its facade trampoline.
                auto* const spimi_buf = _spimi_writer->buffer();
                DCHECK(spimi_buf != nullptr);
                for (size_t i = 0; i < count; ++i) {
                    if ((!_should_analyzer && v->get_size() > _ignore_above) ||
                        (_should_analyzer && v->empty())) {
                        // Empty / over-limit value → no tokens to index,
                        // but the ROW IS NOT NULL. `_null_bitmap` must
                        // mirror only true upstream nulls (added by
                        // `add_nulls`); empty strings have a non-null
                        // column value of "" and `IS NULL` must return
                        // false for them. Mirrors V2's behavior at
                        // `add_values:615` which calls `add_null_document`
                        // (a CLucene "null-doc" marker that does NOT
                        // touch `_null_bitmap`). The earlier V4 impl
                        // added these to `_null_bitmap`, causing
                        // `WHERE body IS NULL` to incorrectly return
                        // empty-string rows.
                    } else if (_should_analyzer) {
                        // Tokenize. Mirror TeeTokenStream::next: position
                        // accumulator clamped to 0 on first token to
                        // handle the synonym-overlay-as-first-token edge
                        // case CLucene's DocumentsWriter normalises.
                        _char_string_reader->init(v->get_data(), cast_set<int32_t>(v->get_size()),
                                                  false);
                        auto* stream = _analyzer->reusableTokenStream(_field_name.c_str(),
                                                                      _char_string_reader);
                        // `reusableTokenStream` returning null means the
                        // analyzer is mis-configured at init time — a
                        // programmer error, not a runtime input. Per
                        // CLAUDE.md's "assert correctness, no defensive
                        // if" rule, crash via DORIS_CHECK rather than
                        // silently dropping the row's tokens.
                        DORIS_CHECK(stream != nullptr);
                        stream->reset();
                        lucene::analysis::Token tok;
                        int32_t pos = -1;
                        bool first_token = true;
                        while (stream->next(&tok) != nullptr) {
                            pos += tok.getPositionIncrement();
                            if (first_token && pos < 0) {
                                pos = 0;
                            }
                            first_token = false;
                            const char* term_buf = tok.template termBuffer<char>();
                            const size_t term_len = tok.template termLength<char>();
                            // Skip zero-length tokens (legitimate output of
                            // some filters). `term_buf` is non-null by
                            // analyzer contract when term_len > 0 — no
                            // defensive guard.
                            if (term_len > 0) {
                                spimi_buf->Append(std::string_view(term_buf, term_len),
                                                  static_cast<uint32_t>(_rid),
                                                  static_cast<uint32_t>(pos));
                                // Mid-row saturation check. The buffer's
                                // `Append` is silently no-op once
                                // saturated; without polling here the
                                // remaining tokens of THIS row would be
                                // silently dropped before the outer row-
                                // boundary poll catches it. Throw inside
                                // the try so the existing catch records
                                // context + calls close_on_error.
                                if (spimi_buf->Saturated()) [[unlikely]] {
                                    _CLTHROWA(CL_ERR_IO,
                                              "V4 SPIMI buffer saturated mid-row: "
                                              "subsequent tokens would be dropped");
                                }
                            }
                        }
                    } else {
                        // Non-analyzed (keyword) string: append whole
                        // value at position 0 — same semantics CLucene's
                        // setValue(char*, len) produces.
                        spimi_buf->Append(std::string_view(v->get_data(), v->get_size()),
                                          static_cast<uint32_t>(_rid), 0);
                    }
                    // Poll saturation after each row's worth of Appends.
                    // The buffer's `Append` is void / silent on
                    // saturation — under V4 we must surface the error
                    // immediately instead of letting subsequent rows
                    // silently drop their tokens before `finish()`
                    // ultimately fails. Shadow mode (V1/V2/V3) keeps the
                    // existing silent-drop behaviour: CLucene is the
                    // primary, the shadow buffer is best-effort.
                    if (spimi_buf->Saturated()) [[unlikely]] {
                        return Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>(
                                "V4 SPIMI buffer saturated mid-batch for field {}: subsequent "
                                "tokens "
                                "would be dropped silently",
                                std::string(_field_name.begin(), _field_name.end()));
                    }
                    // Spill gate. The cheap 256MiB ShouldFlush() latch (set
                    // inside Append when the buffer crosses the budget) is checked
                    // EVERY row so the hard floor stays responsive and the
                    // no-pressure default behaviour + .idx output are unchanged.
                    // The EXPENSIVE checks — process memory watermarks, MemoryUsage
                    // and the growth reserve — run only every
                    // inverted_index_spimi_spill_check_interval_rows rows, keeping
                    // the per-row hot path lean (a 512-row window cannot cross the
                    // 256MiB budget for any realistic doc size).
                    if (_spimi_writer->ShouldFlush()) {
                        _spimi_writer->FlushPending(_spimi_doc_count);
                    } else if (++_spimi_gate_counter >=
                               std::max<int64_t>(
                                       1, config::inverted_index_spimi_spill_check_interval_rows)) {
                        _spimi_gate_counter = 0;
                        // Expensive process hard/soft pressure + per-writer
                        // backstop (ShouldFlush already false here).
                        if (ShouldSpillUnderPressure()) {
                            _spimi_writer->FlushPending(_spimi_doc_count);
                        } else if (_spimi_writer->MemoryUsage() >= kSpimiMinSpillBytes &&
                                   thread_context()->thread_mem_tracker_mgr->reserved_mem() == 0) {
                            // P2 reserve-before-growth: reserve a fixed incremental
                            // growth granule (CHECK_PROCESS only — the write_tracker
                            // task limit is -1). ONLY the granule, never the full
                            // MemoryUsage() (would double-charge the disabled
                            // flush-level reserve at memtable_flush_executor.cpp).
                            Status rst = thread_context()->thread_mem_tracker_mgr->try_reserve(
                                    kSpimiReserveGranule,
                                    ThreadMemTrackerMgr::TryReserveChecker::CHECK_PROCESS);
                            if (!rst.ok()) {
                                // PROCESS_MEMORY_EXCEEDED: spill to relieve, retry
                                // ONCE, else proceed best-effort (SPIMI must NEVER
                                // fail the load).
                                _spimi_writer->FlushPending(_spimi_doc_count);
                                rst = thread_context()->thread_mem_tracker_mgr->try_reserve(
                                        kSpimiReserveGranule,
                                        ThreadMemTrackerMgr::TryReserveChecker::CHECK_PROCESS);
                                (void)rst;
                            }
                        }
                    }
                    if (static_cast<int32_t>(_rid) + 1 > _spimi_doc_count) {
                        _spimi_doc_count = static_cast<int32_t>(_rid) + 1;
                    }
                    ++v;
                    _rid++;
                }
            } catch (const CLuceneError& e) {
                close_on_error();
                return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                        "V4 SPIMI add_values CLucene error: {}", e.what());
            } catch (const Exception& e) {
                // doris::Exception from DORIS_CHECK or downstream
                // (e.g. SpimiPostingBuffer's DCHECK paths).
                close_on_error();
                return Status::Error<ErrorCode::INTERNAL_ERROR>("V4 SPIMI add_values error: {}",
                                                                e.what());
            }
            return Status::OK();
        }
        // V1/V2/V3 classic CLucene path follows.
        DBUG_EXECUTE_IF("InvertedIndexColumnWriter::add_values_field_is_nullptr",
                        { _field = nullptr; })
        DBUG_EXECUTE_IF("InvertedIndexColumnWriter::add_values_index_writer_is_nullptr",
                        { _index_writer = nullptr; })
        if (_field == nullptr || _index_writer == nullptr) {
            LOG(ERROR) << "field or index writer is null in inverted index writer.";
            return Status::InternalError("field or index writer is null in inverted index writer");
        }
        const auto* v = (Slice*)values;
        for (size_t i = 0; i < count; ++i) {
            // only ignore_above UNTOKENIZED strings and empty strings not tokenized
            if ((!_should_analyzer && v->get_size() > _ignore_above) ||
                (_should_analyzer && v->empty())) {
                RETURN_IF_ERROR(add_null_document());
            } else {
                RETURN_IF_ERROR(new_inverted_index_field(v->get_data(), v->get_size()));
                RETURN_IF_ERROR(add_document());
                if (_spimi_writer != nullptr && _spimi_writer->HasBuffer()) {
                    // The tee installed by new_inverted_index_field already
                    // tapped this value's tokens into _spimi_writer during
                    // add_document; for non-analyzed fields the tee is not
                    // used (field->setValue takes the raw bytes), so we
                    // append the whole value at position 0 here.
                    if (!_should_analyzer) {
                        _spimi_writer->AppendToken(std::string_view(v->get_data(), v->get_size()),
                                                   static_cast<uint32_t>(_rid), 0);
                    }
                    if (static_cast<int32_t>(_rid) + 1 > _spimi_doc_count) {
                        _spimi_doc_count = static_cast<int32_t>(_rid) + 1;
                    }
                }
            }
            ++v;
            _rid++;
        }
    } else if constexpr (field_is_numeric_type(field_type)) {
        RETURN_IF_ERROR(add_numeric_values(values, count));
    }
    return Status::OK();
}

template <FieldType field_type>
Status InvertedIndexColumnWriter<field_type>::add_array_values(size_t field_size,
                                                               const void* value_ptr,
                                                               const uint8_t* nested_null_map,
                                                               const uint8_t* offsets_ptr,
                                                               size_t count) {
    // V4 does not yet support array<string> fulltext columns. Both
    // overloads of `add_array_values` go through the CLucene
    // Document path which we deliberately don't build in V4 mode.
    // Without this guard the caller would hit `_field == nullptr`
    // and get a misleading "field or index writer is null"
    // InternalError. Surface a clear NOT_SUPPORTED instead.
    if constexpr (field_is_slice_type(field_type)) {
        if (_is_v4) {
            return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                    "V4 storage format does not yet support ARRAY<string> inverted index "
                    "columns; use V1/V2/V3 for arrays.");
        }
    }
    if (count == 0) {
        // no values to add inverted index
        return Status::OK();
    }
    const auto* offsets = reinterpret_cast<const uint64_t*>(offsets_ptr);
    if constexpr (field_is_slice_type(field_type)) {
        DBUG_EXECUTE_IF("InvertedIndexColumnWriter::add_array_values_index_writer_is_nullptr",
                        { _index_writer = nullptr; })
        if (_index_writer == nullptr) {
            LOG(ERROR) << "index writer is null in inverted index writer.";
            return Status::InternalError("index writer is null in inverted index writer");
        }
        size_t start_off = 0;
        std::vector<ReaderPtr> keep_readers;
        for (size_t i = 0; i < count; ++i) {
            // nullmap & value ptr-array may not from offsets[i] because olap_convertor make offsets accumulate from _base_offset which may not is 0, but nullmap & value in this segment is from 0, we only need
            // every single array row element size to go through the nullmap & value ptr-array, and also can go through the every row in array to keep with _rid++
            auto array_elem_size = offsets[i + 1] - offsets[i];
            // TODO(Amory).later we use object pool to avoid field creation
            std::unique_ptr<lucene::document::Field> new_field;
            CL_NS(analysis)::TokenStream* ts = nullptr;
            for (auto j = start_off; j < start_off + array_elem_size; ++j) {
                if (nested_null_map && nested_null_map[j] == 1) {
                    continue;
                }
                auto* v = (Slice*)((const uint8_t*)value_ptr + j * field_size);
                if ((!_should_analyzer && v->get_size() > _ignore_above) ||
                    (_should_analyzer && v->empty())) {
                    // is here a null value?
                    // TODO. Maybe here has performance problem for large size string.
                    continue;
                } else {
                    // now we temp create field . later make a pool
                    lucene::document::Field* tmp_field = nullptr;
                    Status st = create_field(&tmp_field);
                    new_field.reset(tmp_field);
                    DBUG_EXECUTE_IF(
                            "InvertedIndexColumnWriter::add_array_values_create_field_"
                            "error",
                            {
                                st = Status::Error<ErrorCode::INTERNAL_ERROR>(
                                        "debug point: add_array_values_create_field_error");
                            })
                    if (st != Status::OK()) {
                        LOG(ERROR) << "create field "
                                   << std::string(_field_name.begin(), _field_name.end())
                                   << " error:" << st;
                        return st;
                    }
                    if (_should_analyzer) {
                        // in this case stream need to delete after add_document, because the
                        // stream can not reuse for different field
                        bool own_token_stream = true;
                        ReaderPtr char_string_reader = DORIS_TRY(
                                create_char_string_reader(_analyzer_config.char_filter_map));
                        char_string_reader->init(v->get_data(), cast_set<int32_t>(v->get_size()),
                                                 false);
                        ts = _analyzer->tokenStream(new_field->name(), char_string_reader);
                        new_field->setValue(ts, own_token_stream);
                        keep_readers.emplace_back(std::move(char_string_reader));
                    } else {
                        new_field_char_value(v->get_data(), v->get_size(), new_field.get());
                    }
                    // NOTE: new_field is managed by doc now, so we need to use release() to get the pointer
                    _doc->add(*new_field.release());
                }
            }
            start_off += array_elem_size;
            // here to make debug for array field with current doc which should has expected number of fields
            DBUG_EXECUTE_IF("array_inverted_index.write_index", {
                auto single_array_field_count =
                        DebugPoints::instance()->get_debug_param_or_default<int32_t>(
                                "array_inverted_index.write_index", "single_array_field_count", 0);
                if (single_array_field_count < 0) {
                    return Status::Error<ErrorCode::INTERNAL_ERROR>(
                            "indexes count cannot be negative");
                }
                if (_doc->getFields()->size() != single_array_field_count) {
                    return Status::Error<ErrorCode::INTERNAL_ERROR>(
                            "array field has fields count {} not equal to expected {}",
                            _doc->getFields()->size(), single_array_field_count);
                }
            })

            if (!_doc->getFields()->empty()) {
                // if this array is null, we just ignore to write inverted index
                RETURN_IF_ERROR(add_document());
                _doc->clear();
            } else {
                // avoid to add doc which without any field which may make threadState init skip
                // init fieldDataArray, then will make error with next doc with fields in
                // resetCurrentFieldData
                lucene::document::Field* tmp_field = nullptr;
                Status st = create_field(&tmp_field);
                new_field.reset(tmp_field);
                DBUG_EXECUTE_IF("InvertedIndexColumnWriter::add_array_values_create_field_error_2",
                                {
                                    st = Status::Error<ErrorCode::INTERNAL_ERROR>(
                                            "debug point: add_array_values_create_field_error_2");
                                })
                if (st != Status::OK()) {
                    LOG(ERROR) << "create field "
                               << std::string(_field_name.begin(), _field_name.end())
                               << " error:" << st;
                    return st;
                }
                _doc->add(*new_field.release());
                RETURN_IF_ERROR(add_null_document());
                _doc->clear();
            }
            _rid++;
            keep_readers.clear();
        }
    } else if constexpr (field_is_numeric_type(field_type)) {
        size_t start_off = 0;
        for (int i = 0; i < count; ++i) {
            auto array_elem_size = offsets[i + 1] - offsets[i];
            for (size_t j = start_off; j < start_off + array_elem_size; ++j) {
                if (nested_null_map && nested_null_map[j] == 1) {
                    continue;
                }
                const CppType* p = &reinterpret_cast<const CppType*>(value_ptr)[j];
                RETURN_IF_ERROR(add_value(*p));
            }
            start_off += array_elem_size;
            _row_ids_seen_for_bkd++;
            _rid++;
        }
    }
    return Status::OK();
}

template <FieldType field_type>
Status InvertedIndexColumnWriter<field_type>::add_numeric_values(const void* values, size_t count) {
    auto p = reinterpret_cast<const CppType*>(values);
    for (size_t i = 0; i < count; ++i) {
        RETURN_IF_ERROR(add_value(*p));
        _rid++;
        p++;
        _row_ids_seen_for_bkd++;
    }
    return Status::OK();
}

template <FieldType field_type>
Status InvertedIndexColumnWriter<field_type>::add_value(const CppType& value) {
    try {
        std::string new_value;
        uint32_t value_length = sizeof(CppType);

        DBUG_EXECUTE_IF(
                "InvertedIndexColumnWriter::add_value_bkd_writer_add_throw_"
                "error",
                { _CLTHROWA(CL_ERR_IllegalArgument, ("packedValue should be length=xxx")); });

        _value_key_coder->full_encode_ascending(&value, &new_value);
        _bkd_writer->add((const uint8_t*)new_value.c_str(), value_length, _rid);
    } catch (const CLuceneError& e) {
        return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>("CLuceneError add_value: {}",
                                                                      e.what());
    }
    return Status::OK();
}

template <FieldType field_type>
int64_t InvertedIndexColumnWriter<field_type>::size() const {
    // V4 path reports the SPIMI posting buffer's resident bytes; this is
    // where the segment memory estimate sees the V4 writer's working
    // set. V1/V2/V3 keep master's "TODO 0" behaviour — the CLucene-side
    // memory accounting + cap is the precursor PR's job, not this one.
    if constexpr (field_is_slice_type(field_type)) {
        if (_is_v4) {
            return _spimi_writer != nullptr ? _spimi_writer->MemoryUsage() : 0;
        }
    }
    //TODO: get memory size of inverted index
    return 0;
}

template <FieldType field_type>
void InvertedIndexColumnWriter<field_type>::write_null_bitmap(
        lucene::store::IndexOutput* null_bitmap_out) {
    // write null_bitmap file
    _null_bitmap.runOptimize();
    size_t size = _null_bitmap.getSizeInBytes(false);
    if (size > 0) {
        faststring buf;
        buf.resize(size);
        _null_bitmap.write(reinterpret_cast<char*>(buf.data()), false);
        null_bitmap_out->writeBytes(buf.data(), cast_set<int32_t>(size));
    }
}

template <FieldType field_type>
Status InvertedIndexColumnWriter<field_type>::finish() {
    if (_dir != nullptr) {
        std::unique_ptr<lucene::store::IndexOutput> null_bitmap_out = nullptr;
        std::unique_ptr<lucene::store::IndexOutput> data_out = nullptr;
        std::unique_ptr<lucene::store::IndexOutput> index_out = nullptr;
        std::unique_ptr<lucene::store::IndexOutput> meta_out = nullptr;
        ErrorContext error_context;
        try {
            // write bkd file
            if constexpr (field_is_numeric_type(field_type)) {
                _bkd_writer->max_doc_ = _rid;
                _bkd_writer->docs_seen_ = _row_ids_seen_for_bkd;
                null_bitmap_out = std::unique_ptr<lucene::store::IndexOutput>(_dir->createOutput(
                        InvertedIndexDescriptor::get_temporary_null_bitmap_file_name()));
                data_out = std::unique_ptr<lucene::store::IndexOutput>(_dir->createOutput(
                        InvertedIndexDescriptor::get_temporary_bkd_index_data_file_name()));
                meta_out = std::unique_ptr<lucene::store::IndexOutput>(_dir->createOutput(
                        InvertedIndexDescriptor::get_temporary_bkd_index_meta_file_name()));
                index_out = std::unique_ptr<lucene::store::IndexOutput>(_dir->createOutput(
                        InvertedIndexDescriptor::get_temporary_bkd_index_file_name()));
                write_null_bitmap(null_bitmap_out.get());

                DBUG_EXECUTE_IF("InvertedIndexWriter._set_bkd_data_out_nullptr",
                                { data_out = nullptr; });
                if (data_out != nullptr && meta_out != nullptr && index_out != nullptr) {
                    _bkd_writer->meta_finish(meta_out.get(),
                                             _bkd_writer->finish(data_out.get(), index_out.get()),
                                             int(field_type));
                } else {
                    LOG(WARNING) << "Inverted index writer create output error "
                                    "occurred: nullptr";
                    _CLTHROWA(CL_ERR_IO, "Create output error with nullptr");
                }
            } else if constexpr (field_is_slice_type(field_type)) {
                null_bitmap_out = std::unique_ptr<lucene::store::IndexOutput>(_dir->createOutput(
                        InvertedIndexDescriptor::get_temporary_null_bitmap_file_name()));
                write_null_bitmap(null_bitmap_out.get());
                DBUG_EXECUTE_IF(
                        "InvertedIndexWriter._throw_clucene_error_in_fulltext_"
                        "writer_close",
                        {
                            _CLTHROWA(CL_ERR_IO,
                                      "debug point: test throw error in fulltext "
                                      "index writer");
                        });
                if (_spimi_writer != nullptr && _spimi_writer->HasBuffer()) {
                    // Delegate the entire SPIMI segment emission to the
                    // SpimiIndexWriter facade. It handles saturated-buffer
                    // checks, file naming, IndexOutput creation, direct
                    // emit vs spill-merge path selection, FINALLY_CLOSE,
                    // and on-disk byte-count validation internally.
                    namespace spimi_ns = segment_v2::inverted_index::spimi;
                    spimi_ns::SpimiFinishConfig spimi_config;
                    spimi_config.is_v4 = _is_v4;
                    spimi_config.omit_term_freq_and_positions =
                            !(get_parser_phrase_support_string_from_properties(
                                      _index_meta->properties()) ==
                              INVERTED_INDEX_PARSER_PHRASE_SUPPORT_YES);
                    spimi_config.field_name_utf8 =
                            std::string(_field_name.begin(), _field_name.end());
                    spimi_config.doc_count = _spimi_doc_count;
                    _spimi_writer->Finish(_dir.get(), spimi_config);
                }
                // SPIMI segment emission is now fully delegated to
                // SpimiIndexWriter::Finish() above. Nothing else to do
                // here for the SPIMI path.
            }
            // catch order note: CLuceneError and doris::Exception are
            // unrelated types (neither inherits from the other), so the
            // order below is correct as-is. If a future change introduces a
            // common base (e.g. `std::exception`) the more-derived catch
            // MUST stay first.
        } catch (CLuceneError& e) {
            error_context.eptr = std::current_exception();
            error_context.err_msg.append("Inverted index writer finish error occurred: ");
            error_context.err_msg.append(e.what());
            LOG(ERROR) << error_context.err_msg;
        } catch (const Exception& e) {
            // Phase 27 — `DORIS_CHECK` throws `doris::Exception`, and the
            // SPIMI emit path now contains DORIS_CHECKs (NoteDocId,
            // WriteSkipEntry). Without this branch the exception would
            // propagate past the FINALLY block, leaking the seven
            // SPIMI IndexOutputs and the primary CLucene writer's
            // handles. Funnel it through the same error_context path
            // so cleanup runs uniformly.
            error_context.eptr = std::current_exception();
            error_context.err_msg.append("Inverted index writer finish error occurred: ");
            error_context.err_msg.append(e.what());
            LOG(ERROR) << error_context.err_msg;
        } catch (...) {
            // Catch-all so the FINALLY cleanup runs for ANY exception type, not
            // just the two named above. SpimiIndexWriter::Finish() rethrows the
            // original exception type (e.g. a std::bad_alloc surfaced while
            // building a segment); without this branch such a type would escape
            // past the FINALLY block, leaking the seven SPIMI IndexOutputs and
            // skipping the eptr->Status conversion. The FINALLY macro converts
            // the captured eptr into an INVERTED_INDEX_CLUCENE_ERROR Status.
            error_context.eptr = std::current_exception();
            error_context.err_msg.append("Inverted index writer finish unknown exception");
            LOG(ERROR) << error_context.err_msg;
        }
        FINALLY({
            FINALLY_CLOSE(null_bitmap_out);
            FINALLY_CLOSE(meta_out);
            FINALLY_CLOSE(data_out);
            FINALLY_CLOSE(index_out);
            if constexpr (field_is_numeric_type(field_type)) {
                FINALLY_CLOSE(_dir);
            } else if constexpr (field_is_slice_type(field_type)) {
                if (_is_v4) {
                    // V4 owns the directory directly (no CLucene
                    // IndexWriter held it). Close the dir so the
                    // SPIMI outputs are durably flushed.
                    FINALLY_CLOSE(_dir);
                    if (!error_context.eptr) {
                        // Success: the directory's files must survive until
                        // IndexFileWriter packs them into the combined .idx (it
                        // keeps its own reference via _indices_dirs). Drop OUR
                        // reference WITHOUT deleting, so the success path no
                        // longer satisfies ~InvertedIndexColumnWriter()'s
                        // `_is_v4 && _dir != nullptr` guard and therefore does
                        // NOT call close_on_error()->_dir->deleteDirectory(),
                        // which would wipe the temp dir before it is packed.
                        // Mirrors the V1/V2/V3 `_index_writer.reset()` success
                        // signal. Only observable with an on-disk
                        // DorisFSDirectory (inverted_index_ram_dir_enable=false);
                        // with the RAM dir deleteDirectory() is a no-op, which is
                        // why this was hidden. On the error path _dir is kept so
                        // the destructor still cleans up the partial temp files.
                        _dir.reset();
                    }
                } else {
                    FINALLY_CLOSE(_index_writer);
                    // After closing the _index_writer, it needs to be reset to null to prevent issues of not closing it or closing it multiple times.
                    _index_writer.reset();
                }
            }
        })

        return Status::OK();
    }
    LOG(WARNING) << "Inverted index writer finish error occurred: dir is nullptr";
    return Status::Error<doris::ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
            "Inverted index writer finish error occurred: dir is nullptr");
}

// Helper member template definition placed BEFORE the explicit instantiation
// list. Per `[temp.explicit]/12`, an explicit instantiation only instantiates
// members whose definitions are visible at that point in the translation
// unit. The slice-type instantiations (CHAR/VARCHAR/STRING) below are
// exactly the specializations whose `add_array_values` overloads call this
// helper; ordering the definition before them ensures the body is emitted
// for those specializations rather than being left as a declaration-only
// reference (which would silently rely on a later implicit instantiation
// from the call sites, a build-fragility landmine if the call sites ever
// move to another TU).
template class InvertedIndexColumnWriter<FieldType::OLAP_FIELD_TYPE_CHAR>;
template class InvertedIndexColumnWriter<FieldType::OLAP_FIELD_TYPE_VARCHAR>;
template class InvertedIndexColumnWriter<FieldType::OLAP_FIELD_TYPE_STRING>;

template class InvertedIndexColumnWriter<FieldType::OLAP_FIELD_TYPE_TINYINT>;
template class InvertedIndexColumnWriter<FieldType::OLAP_FIELD_TYPE_SMALLINT>;
template class InvertedIndexColumnWriter<FieldType::OLAP_FIELD_TYPE_INT>;
template class InvertedIndexColumnWriter<FieldType::OLAP_FIELD_TYPE_UNSIGNED_INT>;
template class InvertedIndexColumnWriter<FieldType::OLAP_FIELD_TYPE_BIGINT>;
template class InvertedIndexColumnWriter<FieldType::OLAP_FIELD_TYPE_LARGEINT>;
template class InvertedIndexColumnWriter<FieldType::OLAP_FIELD_TYPE_DATE>;
template class InvertedIndexColumnWriter<FieldType::OLAP_FIELD_TYPE_DATETIME>;
template class InvertedIndexColumnWriter<FieldType::OLAP_FIELD_TYPE_DECIMAL>;
template class InvertedIndexColumnWriter<FieldType::OLAP_FIELD_TYPE_DATEV2>;
template class InvertedIndexColumnWriter<FieldType::OLAP_FIELD_TYPE_DATETIMEV2>;
template class InvertedIndexColumnWriter<FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ>;
template class InvertedIndexColumnWriter<FieldType::OLAP_FIELD_TYPE_DECIMAL32>;
template class InvertedIndexColumnWriter<FieldType::OLAP_FIELD_TYPE_DECIMAL64>;
template class InvertedIndexColumnWriter<FieldType::OLAP_FIELD_TYPE_DECIMAL128I>;
template class InvertedIndexColumnWriter<FieldType::OLAP_FIELD_TYPE_DECIMAL256>;
template class InvertedIndexColumnWriter<FieldType::OLAP_FIELD_TYPE_BOOL>;
template class InvertedIndexColumnWriter<FieldType::OLAP_FIELD_TYPE_IPV4>;
template class InvertedIndexColumnWriter<FieldType::OLAP_FIELD_TYPE_IPV6>;
template class InvertedIndexColumnWriter<FieldType::OLAP_FIELD_TYPE_FLOAT>;
template class InvertedIndexColumnWriter<FieldType::OLAP_FIELD_TYPE_DOUBLE>;

} // namespace doris::segment_v2