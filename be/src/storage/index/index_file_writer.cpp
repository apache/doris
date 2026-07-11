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

#include "storage/index/index_file_writer.h"

#include <glog/logging.h>

#include <algorithm>
#include <atomic>
#include <filesystem>

#include "common/cast_set.h"
#include "common/config.h"
#include "common/status.h"
#include "io/fs/packed_file_writer.h"
#include "io/fs/s3_file_writer.h"
#include "io/fs/stream_sink_file_writer.h"
#include "storage/index/ann/ann_index_files.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/index_storage_format_v1.h"
#include "storage/index/index_storage_format_v2.h"
#include "storage/index/inverted/inverted_index_compound_reader.h"
#include "storage/index/inverted/inverted_index_desc.h"
#include "storage/index/inverted/inverted_index_fs_directory.h"
#include "storage/index/inverted/inverted_index_reader.h"
#include "storage/index/snii/format/phrase_bigram.h"
#include "storage/index/snii/snii_doris_adapter.h"
#include "storage/tablet/tablet_schema.h"

namespace doris::segment_v2 {

namespace {

// Resolves the EFFECTIVE SNII phrase-bigram df-prune threshold for one segment
// index (G01): 0 for non-positional configs (no bigrams exist there) and when
// config::snii_bigram_prune_min_df == 0 (pruning disabled, legacy layout); the
// fixed config value when > 0; otherwise (< 0, the default) the auto formula
// max(64, doc_count / 10000). The resolved value is what the writer applies AND
// records in the per-index segment meta.
uint32_t snii_effective_bigram_prune_min_df(uint32_t doc_count,
                                            doris::snii::format::IndexConfig index_config) {
    if (!doris::snii::format::has_positions(index_config)) {
        return 0;
    }
    const int32_t conf = config::snii_bigram_prune_min_df;
    if (conf == 0) {
        return 0;
    }
    if (conf > 0) {
        return static_cast<uint32_t>(conf);
    }
    return doris::snii::format::default_phrase_bigram_prune_min_df(doc_count);
}

// Resolves the EFFECTIVE SNII phrase-bigram df-prune UPPER bound for one
// segment index (G15): 0 (no upper gate) for non-positional configs and when
// config::snii_bigram_prune_max_df_ratio is outside (0, 1); otherwise
// ratio * doc_count, floored at 2 * effective_min_df when the min-df gate is
// active so the two gates never overlap. A rounded-to-0 result (tiny segment,
// no min floor) stays 0: recording 0 means "gate inactive" to the reader, so
// pruning with it would break the dict-miss fallback contract. Resolved ONLY
// here, at flush, where the final doc count is known -- the bound scales with
// the doc count, so no mid-feed partial-df comparison against it is ever
// valid.
uint64_t snii_effective_bigram_prune_max_df(uint32_t doc_count, uint32_t effective_min_df,
                                            doris::snii::format::IndexConfig index_config) {
    if (!doris::snii::format::has_positions(index_config)) {
        return 0;
    }
    const double ratio = config::snii_bigram_prune_max_df_ratio;
    // Gate only for ratio in (0, 1): df can never exceed doc_count, so a
    // ratio >= 1.0 is a provably-dead gate -- resolving it to 0 keeps the meta
    // from arming the dict-miss fallback (turning fast "pair absent == empty"
    // answers into pointless positions-verification fallbacks) for a bound
    // that can never prune. The (0, 1) window also keeps ratio * doc_count
    // below 2^32, so the uint64 cast below is always defined (an absurd ratio
    // like 1e18, or NaN -- which fails the conjunction -- could otherwise
    // overflow the cast into UB).
    if (!(ratio > 0.0 && ratio < 1.0)) {
        return 0;
    }
    auto max_df = static_cast<uint64_t>(ratio * doc_count);
    if (effective_min_df > 0) {
        max_df = std::max<uint64_t>(max_df, 2ULL * effective_min_df);
    }
    return max_df;
}

} // namespace

// Resolves whether one segment index lays out freq regions (G16-c). Freq
// serves ONLY BM25 scoring: a scoring config always keeps it; a plain
// positions config keeps it only when the escape-hatch config asks for the
// full T2 layout. NOT in the anonymous namespace on purpose -- the UT covers
// this production policy line directly (a flipped operator or inverted flag
// here would otherwise stay green: no BE test drives add_snii_index).
bool snii_effective_write_freq(doris::snii::format::IndexConfig index_config) {
    return doris::snii::format::has_scoring(index_config) ||
           config::snii_positions_index_write_freq;
}

IndexFileWriter::IndexFileWriter(io::FileSystemSPtr fs, std::string index_path_prefix,
                                 std::string rowset_id, int64_t seg_id,
                                 InvertedIndexStorageFormatPB storage_format,
                                 io::FileWriterPtr file_writer, bool can_use_ram_dir,
                                 int64_t tablet_id)
        : _fs(std::move(fs)),
          _index_path_prefix(std::move(index_path_prefix)),
          _rowset_id(std::move(rowset_id)),
          _seg_id(seg_id),
          _storage_format(storage_format),
          _local_fs(io::global_local_filesystem()),
          _idx_v2_writer(std::move(file_writer)),
          _can_use_ram_dir(can_use_ram_dir),
          _tablet_id(tablet_id) {
    auto tmp_file_dir = ExecEnv::GetInstance()->get_tmp_file_dirs()->get_tmp_file_dir();
    _tmp_dir = tmp_file_dir.native();
    if (_storage_format == InvertedIndexStorageFormatPB::V1) {
        _index_storage_format = std::make_unique<IndexStorageFormatV1>(this);
    } else if (_storage_format != InvertedIndexStorageFormatPB::SNII) {
        _index_storage_format = std::make_unique<IndexStorageFormatV2>(this);
    }
}

Status IndexFileWriter::initialize(InvertedIndexDirectoryMap& indices_dirs) {
    _indices_dirs = std::move(indices_dirs);
    return Status::OK();
}

Status IndexFileWriter::_insert_directory_into_map(int64_t index_id,
                                                   const std::string& index_suffix,
                                                   std::shared_ptr<DorisFSDirectory> dir) {
    auto key = std::make_pair(index_id, index_suffix);
    auto [it, inserted] = _indices_dirs.emplace(key, std::move(dir));
    if (!inserted) {
        LOG(ERROR) << "IndexFileWriter::open attempted to insert a duplicate key: (" << key.first
                   << ", " << key.second << ")";
        LOG(ERROR) << "Directories already in map: ";
        for (const auto& entry : _indices_dirs) {
            LOG(ERROR) << "Key: (" << entry.first.first << ", " << entry.first.second << ")";
        }
        return Status::InternalError("IndexFileWriter::open attempted to insert a duplicate dir");
    }
    return Status::OK();
}

Result<std::shared_ptr<DorisFSDirectory>> IndexFileWriter::open(const TabletIndex* index_meta) {
    if (_storage_format == InvertedIndexStorageFormatPB::SNII) {
        return ResultError(Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                "SNII format does not open CLucene directories"));
    }
    auto local_fs_index_path = InvertedIndexDescriptor::get_temporary_index_path(
            _tmp_dir, _rowset_id, _seg_id, index_meta->index_id(), index_meta->get_index_suffix());
    auto dir = std::shared_ptr<DorisFSDirectory>(DorisFSDirectoryFactory::getDirectory(
            _local_fs, local_fs_index_path.c_str(), _can_use_ram_dir));
    auto st =
            _insert_directory_into_map(index_meta->index_id(), index_meta->get_index_suffix(), dir);
    if (!st.ok()) {
        return ResultError(st);
    }

    return dir;
}

Status IndexFileWriter::add_snii_index(const TabletIndex* index_meta, uint32_t doc_count,
                                       std::vector<uint32_t> null_docids,
                                       doris::snii::writer::SpimiTermBuffer* const term_buffer,
                                       doris::snii::format::IndexConfig index_config,
                                       bool phrase_bigrams_deferred,
                                       doris::snii::writer::MemoryReporter* const mem_reporter) {
    DCHECK(_storage_format == InvertedIndexStorageFormatPB::SNII);
    DCHECK(index_meta != nullptr);
    DCHECK(term_buffer != nullptr);
    if (_idx_v2_writer == nullptr) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND>(
                "SNII index file writer is null for {}", _index_path_prefix);
    }
    if (_snii_file_writer == nullptr) {
        _snii_file_writer = std::make_unique<snii_doris::DorisSniiFileWriter>(_idx_v2_writer.get());
        _snii_compound_writer =
                std::make_unique<doris::snii::writer::SniiCompoundWriter>(_snii_file_writer.get());
    }

    doris::snii::writer::SniiIndexInput input;
    input.index_id = cast_set<uint64_t>(index_meta->index_id());
    input.index_suffix = index_meta->get_index_suffix();
    input.config = index_config;
    input.doc_count = doc_count;
    input.null_docids = std::move(null_docids);
    input.term_source = term_buffer;
    input.mem_reporter = mem_reporter;
    // NOTE: deferred segments persist these thresholds too, although zero
    // bigram terms were fed. On OLD readers (flag-blind) a non-zero threshold
    // is the SECOND, redundant fallback trigger alongside the omitted sentinel
    // -- either alone routes their pair-dict miss to positions verification --
    // so neither mechanism may be removed independently.
    input.bigram_prune_min_df = snii_effective_bigram_prune_min_df(doc_count, index_config);
    input.bigram_prune_max_df =
            snii_effective_bigram_prune_max_df(doc_count, input.bigram_prune_min_df, index_config);
    input.phrase_bigrams_deferred = phrase_bigrams_deferred;
    // G16-c: freq regions serve only BM25 scoring; a plain positions index
    // drops them unless the escape hatch asks for the full T2 layout.
    input.write_freq = snii_effective_write_freq(index_config);
    // G16-h: zstd levels. dict blocks accept zstd's full sane range; the prx
    // level floor is 3 because the writer passes -level into the prx builders
    // and -1 is the historic "auto at default level 3" sentinel -- a
    // configured level 1 would silently resolve to 3 anyway (levels 1-2 buy
    // nothing over 3 on these payloads).
    input.dict_block_zstd_level = std::clamp(config::snii_dict_block_zstd_level, 1, 19);
    input.prx_zstd_level = std::clamp(config::snii_prx_zstd_level, 3, 19);
    // G16-d: dict block size experiment knob; <= 0 keeps the format default.
    if (config::snii_target_dict_block_bytes > 0) {
        input.target_dict_block_bytes = static_cast<uint32_t>(config::snii_target_dict_block_bytes);
    }
    // G04: hand the flush the buffer's ever-dropped bloom so vocab-cap-evicted
    // bigram pairs that reappeared (incomplete postings) are dropped in addition
    // to the df threshold. Null when no eviction ever fired (zero probe cost).
    input.bigram_ever_dropped = term_buffer->bigram_dropped_filter();
    // GUARD against a mid-import config flip: the buffer suppressed bigram
    // positions / evicted terms because pruning was enabled at init, but the
    // mutable config now resolves the threshold to 0 (legacy layout). Flushing a
    // legacy layout from a dieted buffer would either fail validation (missing
    // positions) or -- worse -- materialize incomplete evicted pairs on a segment
    // whose meta declares no fallback. Fail loudly instead. DEFERRED segments
    // are exempt: they fed zero pair tokens, so the armed diet is provably
    // inert (empty pair map, no evicted-incomplete pairs possible) and the
    // resident deferred flag already routes readers to positions verification.
    if (input.bigram_prune_min_df == 0 && !phrase_bigrams_deferred &&
        term_buffer->bigram_diet_configured()) {
        return Status::InternalError(
                "SNII: snii_bigram_prune_min_df was disabled while an import with the bigram "
                "diet active was in flight for {}; keep the config stable during a load",
                _index_path_prefix);
    }
    RETURN_IF_ERROR(_snii_compound_writer->add_logical_index(input));
    ++_snii_index_count;
    return Status::OK();
}

void IndexFileWriter::retain_snii_memory_reporter(
        std::unique_ptr<doris::snii::writer::MemoryReporter> mem_reporter) {
    DCHECK(mem_reporter != nullptr);
    _snii_memory_reporters.push_back(std::move(mem_reporter));
}

Status IndexFileWriter::delete_index(const TabletIndex* index_meta) {
    DBUG_EXECUTE_IF("IndexFileWriter::delete_index_index_meta_nullptr", { index_meta = nullptr; });
    if (!index_meta) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT>("Index metadata is null.");
    }

    auto index_id = index_meta->index_id();
    const auto& index_suffix = index_meta->get_index_suffix();

    // Check if the specified index exists
    auto index_it = _indices_dirs.find(std::make_pair(index_id, index_suffix));
    DBUG_EXECUTE_IF("IndexFileWriter::delete_index_indices_dirs_reach_end",
                    { index_it = _indices_dirs.end(); })
    if (index_it == _indices_dirs.end()) {
        std::ostringstream errMsg;
        errMsg << "No inverted index with id " << index_id << " and suffix " << index_suffix
               << " found.";
        LOG(WARNING) << errMsg.str();
        return Status::OK();
    }

    _indices_dirs.erase(index_it);
    return Status::OK();
}

Status IndexFileWriter::add_into_searcher_cache() {
    if (_storage_format == InvertedIndexStorageFormatPB::SNII) {
        return Status::OK();
    }
    auto index_file_reader = std::make_unique<IndexFileReader>(
            _fs, _index_path_prefix, _storage_format, InvertedIndexFileInfo(), _tablet_id);
    auto st = index_file_reader->init();
    if (!st.ok()) {
        if (dynamic_cast<io::StreamSinkFileWriter*>(_idx_v2_writer.get()) != nullptr) {
            // StreamSinkFileWriter not found file is normal.
            return Status::OK();
        }
        if (dynamic_cast<io::PackedFileWriter*>(_idx_v2_writer.get()) != nullptr) {
            // PackedFileWriter: file may be merged, skip cache for now.
            // The cache will be populated on first read.
            return Status::OK();
        }
        LOG(WARNING) << "IndexFileWriter::add_into_searcher_cache for " << _index_path_prefix
                     << ", error " << st.msg();
        return st;
    }
    for (const auto& entry : _indices_dirs) {
        auto index_meta = entry.first;
        auto dir = DORIS_TRY(index_file_reader->_open(index_meta.first, index_meta.second));
        std::vector<std::string> file_names;
        dir->list(&file_names);
        // Skip ANN indexes – they use FAISS files (ann.faiss, ann.ivfdata) instead of
        // CLucene segments, so building an inverted-index searcher would fail.
        // HNSW/IVF produces 1 file (ann.faiss); IVF_ON_DISK produces 2 (ann.faiss + ann.ivfdata).
        bool is_ann_index =
                std::any_of(file_names.begin(), file_names.end(), [](const std::string& f) {
                    return f == faiss_index_fila_name || f == faiss_ivfdata_file_name;
                });
        if (is_ann_index) {
            continue;
        }
        auto index_file_key = InvertedIndexDescriptor::get_index_file_cache_key(
                _index_path_prefix, index_meta.first, index_meta.second);
        InvertedIndexSearcherCache::CacheKey searcher_cache_key(index_file_key);
        InvertedIndexCacheHandle inverted_index_cache_handle;
        if (InvertedIndexSearcherCache::instance()->lookup(searcher_cache_key,
                                                           &inverted_index_cache_handle)) {
            st = InvertedIndexSearcherCache::instance()->erase(searcher_cache_key.index_file_path);
            if (!st.ok()) {
                LOG(WARNING) << "IndexFileWriter::add_into_searcher_cache for "
                             << _index_path_prefix << ", error " << st.msg();
            }
        }
        IndexSearcherPtr searcher;
        size_t reader_size = 0;
        auto index_searcher_builder = DORIS_TRY(_construct_index_searcher_builder(dir.get()));
        RETURN_IF_ERROR(InvertedIndexReader::create_index_searcher(
                index_searcher_builder.get(), dir.get(), &searcher, reader_size));
        auto* cache_value = new InvertedIndexSearcherCache::CacheValue(std::move(searcher),
                                                                       reader_size, UnixMillis());
        InvertedIndexSearcherCache::instance()->insert(searcher_cache_key, cache_value);
    }
    return Status::OK();
}

Result<std::unique_ptr<IndexSearcherBuilder>> IndexFileWriter::_construct_index_searcher_builder(
        const DorisCompoundReader* dir) {
    std::vector<std::string> files;
    dir->list(&files);
    auto reader_type = InvertedIndexReaderType::FULLTEXT;
    bool found_bkd = std::any_of(files.begin(), files.end(), [](const std::string& file) {
        return file == InvertedIndexDescriptor::get_temporary_bkd_index_data_file_name();
    });
    if (found_bkd) {
        reader_type = InvertedIndexReaderType::BKD;
    }
    return IndexSearcherBuilder::create_index_searcher_builder(reader_type);
}

Status IndexFileWriter::begin_close() {
    DCHECK(!_closed) << debug_string();
    _closed = true;
    if (_storage_format == InvertedIndexStorageFormatPB::SNII) {
        if (_snii_compound_writer == nullptr) {
            if (_idx_v2_writer == nullptr) {
                return Status::OK();
            }
            _snii_file_writer =
                    std::make_unique<snii_doris::DorisSniiFileWriter>(_idx_v2_writer.get());
            _snii_compound_writer = std::make_unique<doris::snii::writer::SniiCompoundWriter>(
                    _snii_file_writer.get());
        }
        RETURN_IF_ERROR(_snii_compound_writer->finish());
        _total_file_size = _idx_v2_writer == nullptr ? 0 : _idx_v2_writer->bytes_appended();
        _file_info.set_index_size(_total_file_size);
        return Status::OK();
    }
    if (_indices_dirs.empty()) {
        // An empty file must still be created even if there are no indexes to write
        if (dynamic_cast<io::StreamSinkFileWriter*>(_idx_v2_writer.get()) != nullptr ||
            dynamic_cast<io::S3FileWriter*>(_idx_v2_writer.get()) != nullptr ||
            dynamic_cast<io::PackedFileWriter*>(_idx_v2_writer.get()) != nullptr) {
            return _idx_v2_writer->close(true);
        }
        return Status::OK();
    }
    DBUG_EXECUTE_IF("inverted_index_storage_format_must_be_v2", {
        if (_storage_format != InvertedIndexStorageFormatPB::V2) {
            return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                    "IndexFileWriter::close fault injection:inverted index storage format "
                    "must be v2");
        }
    })
    try {
        RETURN_IF_ERROR(_index_storage_format->write());
        for (const auto& entry : _indices_dirs) {
            const auto& dir = entry.second;
            // delete index path, which contains separated inverted index files
            if (std::strcmp(dir->getObjectName(), "DorisFSDirectory") == 0) {
                auto* compound_dir = static_cast<DorisFSDirectory*>(dir.get());
                compound_dir->deleteDirectory();
            }
        }
    } catch (CLuceneError& err) {
        if (_storage_format == InvertedIndexStorageFormatPB::V1) {
            return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                    "CLuceneError occur when close, error msg: {}", err.what());
        } else {
            return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                    "CLuceneError occur when close idx file {}, error msg: {}",
                    InvertedIndexDescriptor::get_index_file_path_v2(_index_path_prefix),
                    err.what());
        }
    }
    return Status::OK();
}

Status IndexFileWriter::finish_close() {
    DCHECK(_closed) << debug_string();
    if (_storage_format == InvertedIndexStorageFormatPB::SNII) {
        if (_idx_v2_writer != nullptr && _idx_v2_writer->state() != io::FileWriter::State::CLOSED) {
            RETURN_IF_ERROR(_idx_v2_writer->close(false));
        }
        return Status::OK();
    }
    if (_indices_dirs.empty()) {
        // An empty file must still be created even if there are no indexes to write
        if (dynamic_cast<io::StreamSinkFileWriter*>(_idx_v2_writer.get()) != nullptr ||
            dynamic_cast<io::S3FileWriter*>(_idx_v2_writer.get()) != nullptr ||
            dynamic_cast<io::PackedFileWriter*>(_idx_v2_writer.get()) != nullptr) {
            return _idx_v2_writer->close(false);
        }
        return Status::OK();
    }
    if (_idx_v2_writer != nullptr && _idx_v2_writer->state() != io::FileWriter::State::CLOSED) {
        RETURN_IF_ERROR(_idx_v2_writer->close(false));
    }

    Status st = Status::OK();
    if (config::enable_write_index_searcher_cache) {
        st = add_into_searcher_cache();
    }
    _indices_dirs.clear();
    return st;
}

std::vector<std::string> IndexFileWriter::get_index_file_names() const {
    std::vector<std::string> file_names;
    if (_storage_format == InvertedIndexStorageFormatPB::V1) {
        if (_closed && _file_info.index_info_size() > 0) {
            for (const auto& index_info : _file_info.index_info()) {
                file_names.emplace_back(InvertedIndexDescriptor::get_index_file_name_v1(
                        _rowset_id, _seg_id, index_info.index_id(), index_info.index_suffix()));
            }
        } else {
            for (const auto& [index_info, _] : _indices_dirs) {
                file_names.emplace_back(InvertedIndexDescriptor::get_index_file_name_v1(
                        _rowset_id, _seg_id, index_info.first, index_info.second));
            }
        }
    } else {
        file_names.emplace_back(
                InvertedIndexDescriptor::get_index_file_name_v2(_rowset_id, _seg_id));
    }
    return file_names;
}

std::string IndexFileWriter::debug_string() const {
    std::stringstream indices_dirs;
    for (const auto& [index, dir] : _indices_dirs) {
        indices_dirs << "index id is: " << index.first << " , index suffix is: " << index.second
                     << " , index dir is: " << dir->toString();
    }
    return fmt::format(
            "inverted index file writer debug string: index storage format is: {}, index path "
            "prefix is: {}, rowset id is: {}, seg id is: {}, closed is: {}, total file size "
            "is: {}, index dirs is: {}",
            _storage_format, _index_path_prefix, _rowset_id, _seg_id, _closed, _total_file_size,
            indices_dirs.str());
}

} // namespace doris::segment_v2
