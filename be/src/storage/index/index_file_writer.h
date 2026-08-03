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
#include <CLucene/store/IndexInput.h>
#include <gen_cpp/olap_common.pb.h>
#include <gen_cpp/olap_file.pb.h>

#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "common/be_mock_util.h"
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "storage/index/index_storage_format.h"
#include "storage/index/inverted/common_grams/common_grams_segment_metadata.h"
#include "storage/index/inverted/inverted_index_common.h"
#include "storage/index/inverted/inverted_index_compound_reader.h"
#include "storage/index/inverted/inverted_index_searcher.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/snii_doris_adapter.h"
#include "storage/index/snii/writer/snii_compound_writer.h"

namespace doris::snii::writer {
class MemoryReporter;
class SpimiTermBuffer;
class SniiCompoundWriter;
} // namespace doris::snii::writer

namespace doris {
class TabletIndex;

namespace segment_v2 {
class DorisFSDirectory;
namespace snii_doris {
class DorisSniiFileWriter;
} // namespace snii_doris

using InvertedIndexDirectoryMap =
        std::map<std::pair<int64_t, std::string>, std::shared_ptr<lucene::store::Directory>>;

class IndexFileWriter;
using IndexFileWriterPtr = std::unique_ptr<IndexFileWriter>;

class IndexFileWriter {
public:
    IndexFileWriter(io::FileSystemSPtr fs, std::string index_path_prefix, std::string rowset_id,
                    int64_t seg_id, InvertedIndexStorageFormatPB storage_format,
                    io::FileWriterPtr file_writer = nullptr, bool can_use_ram_dir = true,
                    int64_t tablet_id = -1);
    virtual ~IndexFileWriter() = default;

    MOCK_FUNCTION Result<std::shared_ptr<DorisFSDirectory>> open(const TabletIndex* index_meta);
    // Write-path facts for one SNII index flush.
    struct SniiAddIndexOptions {
        // This flush serves a stream/broker load (DataWriteType::TYPE_DIRECT):
        // the prx region compresses at snii_prx_zstd_level_direct_load;
        // compaction / schema change / ADD INDEX keep snii_prx_zstd_level.
        bool is_direct_load = false;
        // Present only for a CommonGrams writer that has a complete immutable
        // capability identity. These are semantic BM25 inputs; physical TTF is
        // still derived from every emitted unigram and gram posting.
        std::vector<uint8_t> encoded_norms;
        std::optional<inverted_index::CommonGramsSegmentMetadata> common_grams_metadata;
        snii::format::CommonGramsPostingPolicy common_grams_posting_policy =
                snii::format::CommonGramsPostingPolicy::kNone;
    };
    Status add_snii_index(const TabletIndex* index_meta, uint32_t doc_count,
                          std::vector<uint32_t> null_docids,
                          doris::snii::writer::SpimiTermBuffer* const term_buffer,
                          doris::snii::format::IndexConfig index_config,
                          SniiAddIndexOptions options,
                          doris::snii::writer::MemoryReporter* const mem_reporter);
    // T2.2 compaction index merge fast path: begins a STREAMED SNII index
    // session on this compound. Unlike add_snii_index (which drains a SPIMI
    // term buffer), the caller pushes pre-merged, lexicographically sorted
    // terms through *session and seals the index with (*session)->finish().
    // Write parameters resolve through the SAME helper as add_snii_index
    // (write_freq / zstd levels / dict block size), always at the COMPACTION
    // prx tier (a merge is never a direct load). CommonGrams T3 callers transfer
    // a precharged destination norm vector and a validated static metadata seed;
    // the streamed session late-binds semantic token_count before finish. Only ONE
    // session may be active per compound at a time, and begin_close() with an
    // unfinished session fails instead of sealing a half-fed container. The
    // handle is owned by this writer and valid until it is destroyed.
    Status add_snii_index_streamed(
            const TabletIndex* index_meta, uint32_t doc_count,
            doris::snii::writer::TrackedNullDocids null_docids,
            doris::snii::format::IndexConfig index_config,
            std::shared_ptr<doris::snii::writer::MemoryReporter> mem_reporter,
            doris::snii::writer::SniiStreamedIndexSession** session);
    Status add_snii_index_streamed(
            const TabletIndex* index_meta, uint32_t doc_count,
            doris::snii::writer::TrackedNullDocids null_docids,
            doris::snii::writer::TrackedEncodedNorms encoded_norms,
            std::optional<inverted_index::CommonGramsSegmentMetadata> common_grams_metadata,
            doris::snii::format::CommonGramsPostingPolicy common_grams_posting_policy,
            doris::snii::format::IndexConfig index_config,
            std::shared_ptr<doris::snii::writer::MemoryReporter> mem_reporter,
            doris::snii::writer::SniiStreamedIndexSession** session);
    // Registers one opaque BLOB logical index (a numeric BKD, an ANN graph, ...)
    // on this SNII compound. Unlike add_snii_index it feeds the writer no terms:
    // the sub-file bytes are pulled through the BlobFileSource callbacks at
    // finish(), which is what lets the container -- not the producer -- decide
    // cold/hot placement. Registration writes no byte, so a rejected call leaves
    // the writer clean.
    Status add_snii_blob_index(const TabletIndex* index_meta,
                               doris::snii::format::LogicalIndexKind kind,
                               std::vector<doris::snii::writer::BlobFileSource> cold_files,
                               std::vector<doris::snii::writer::BlobFileSource> hot_files);
    void retain_snii_memory_reporter(
            std::unique_ptr<doris::snii::writer::MemoryReporter> mem_reporter);
    // SNII only, BUILD INDEX rewrite: copies the source container's valid
    // physical prefix and registers the inherited metadata groups so begin_close
    // re-emits them without decoding a posting. Must precede every
    // add_snii_index on this writer (the copied prefix owns the container
    // front).
    Status inherit_snii(const doris::snii::reader::SniiRewriteSnapshot& snapshot,
                        doris::snii::io::FileReader* source);
    Status delete_index(const TabletIndex* index_meta);
    Status initialize(InvertedIndexDirectoryMap& indices_dirs);
    Status add_into_searcher_cache();
    // Begin the close process. This mainly triggers the asynchronous close operation of
    // _idx_v2_writer by calling close(true), which starts the close process but returns
    // immediately without waiting for completion.
    Status begin_close();
    // Finish the close process. This waits for the close operation to complete by calling
    // _idx_v2_writer->close(false), which blocks until the close is fully done.
    Status finish_close();
    const InvertedIndexFileInfo* get_index_file_info() const {
        DCHECK(_closed) << debug_string();
        return &_file_info;
    }
    int64_t get_index_file_total_size() const {
        DCHECK(_closed) << debug_string();
        return _total_file_size;
    }
    const io::FileSystemSPtr& get_fs() const { return _fs; }
    InvertedIndexStorageFormatPB get_storage_format() const { return _storage_format; }
    void set_file_writer_opts(const io::FileWriterOptions& opts) { _opts = opts; }
    std::vector<std::string> get_index_file_names() const;
    std::string debug_string() const;

    // Get internal file writer (for merge file index collection)
    io::FileWriter* get_file_writer() const { return _idx_v2_writer.get(); }

private:
    Status _insert_directory_into_map(int64_t index_id, const std::string& index_suffix,
                                      std::shared_ptr<DorisFSDirectory> dir);
    virtual Result<std::unique_ptr<IndexSearcherBuilder>> _construct_index_searcher_builder(
            const DorisCompoundReader* dir);
    // SNII only: turns every CLucene directory opened through open() into a blob
    // logical index in the container. Runs once, from begin_close(), before the
    // compound writer is sealed.
    Status _seal_snii_blob_directories();
    // Drops the directories harvested by the above once the container owns their
    // bytes. Only the SNII path needs this: the V1/V2 branch of begin_close()
    // deletes its own directories inline.
    void _release_snii_blob_directories();

    // Member variables...
    InvertedIndexDirectoryMap _indices_dirs;
    // SNII only: the index metadata behind each entry of _indices_dirs. Owned a
    // copy rather than borrowed, because the harvest happens in begin_close(),
    // long after open() returned. Held by shared_ptr so this header keeps
    // TabletIndex incomplete -- it is included nearly everywhere.
    std::map<std::pair<int64_t, std::string>, std::shared_ptr<TabletIndex>> _snii_blob_dir_metas;
    const io::FileSystemSPtr _fs;
    std::string _index_path_prefix;
    std::string _rowset_id;
    int64_t _seg_id;
    InvertedIndexStorageFormatPB _storage_format;
    std::string _tmp_dir;
    const std::shared_ptr<io::LocalFileSystem>& _local_fs;

    // write to disk or stream
    io::FileWriterPtr _idx_v2_writer = nullptr;
    io::FileWriterOptions _opts;

    // v1: all file size
    // v2: file size
    int64_t _total_file_size = 0;
    InvertedIndexFileInfo _file_info;

    // only once
    bool _closed = false;
    bool _can_use_ram_dir = true;

    IndexStorageFormatPtr _index_storage_format;
    int64_t _tablet_id = -1;
    std::unique_ptr<snii_doris::DorisSniiFileWriter> _snii_file_writer;
    std::vector<std::shared_ptr<doris::snii::writer::MemoryReporter>> _snii_memory_reporters;
    std::unique_ptr<doris::snii::writer::SniiCompoundWriter> _snii_compound_writer;
    size_t _snii_index_count = 0;

    friend class IndexStorageFormatV1;
    friend class IndexStorageFormatV2;
    friend class IndexFileWriterTest;
};

} // namespace segment_v2
} // namespace doris
