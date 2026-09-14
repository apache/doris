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

#include "storage/index/index_disk_usage.h"

#include <algorithm>
#include <utility>

#include "common/cast_set.h"
#include "common/check.h"
#include "io/io_common.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/inverted/inverted_index_desc.h"
#include "storage/index/snii/format/dict_entry.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/reader/logical_index_reader.h"

namespace doris::segment_v2 {

namespace {

bool is_bkd_file(std::string_view name) {
    return name == InvertedIndexDescriptor::get_temporary_bkd_index_data_file_name() ||
           name == InvertedIndexDescriptor::get_temporary_bkd_index_meta_file_name() ||
           name == InvertedIndexDescriptor::get_temporary_bkd_index_file_name();
}

bool is_wanted(const IndexDiskUsageOptions& options, int64_t index_id) {
    return options.index_ids.empty() || options.index_ids.contains(index_id);
}

// Classifies every sub-file of a CLucene directory into `record` and adds their total length to
// `files_bytes`.
Status add_directory_files(const lucene::store::Directory& dir, IndexDiskUsageRecord* record,
                           int64_t* files_bytes) {
    try {
        std::vector<std::string> names;
        if (!dir.list(&names)) {
            return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                    "failed to list inverted index sub-files");
        }
        for (const auto& name : names) {
            const int64_t length = dir.fileLength(name.c_str());
            classify_clucene_file(name, length, record);
            *files_bytes += length;
        }
    } catch (CLuceneError& e) {
        return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                "failed to read inverted index sub-files: {}", e.what());
    }
    return Status::OK();
}

// Sums the position bytes of the dictionary entries whose postings live in the posting region.
// Inline postings stay in the dictionary region and are not counted.
Status sum_snii_position_bytes(const IndexFileReader& reader, uint64_t index_id,
                               std::string_view suffix, int64_t* position_bytes) {
    // A full dictionary scan should not evict blocks that queries keep in the file cache.
    io::IOContext io_ctx;
    io_ctx.is_disposable = true;
    io_ctx.is_inverted_index = true;
    auto logical = DORIS_TRY(reader.open_snii_logical_index(
            index_id, suffix, &io_ctx, snii::reader::LogicalIndexOpenMode::kCompaction));
    snii_doris::DorisSniiFileReader::ScopedIOContext io_context_scope(&io_ctx);
    std::vector<snii::format::DictEntry> entries;
    for (uint32_t block = 0; block < logical->n_dict_blocks(); ++block) {
        uint64_t frq_base = 0;
        uint64_t prx_base = 0;
        RETURN_IF_ERROR(logical->decode_dict_block(block, &entries, &frq_base, &prx_base));
        for (const auto& entry : entries) {
            if (entry.kind == snii::format::DictEntryKind::kPodRef) {
                *position_bytes += cast_set<int64_t>(entry.prx_len);
            }
        }
    }
    return Status::OK();
}

} // namespace

void classify_clucene_file(std::string_view name, int64_t length, IndexDiskUsageRecord* record) {
    record->total_bytes += length;
    if (is_bkd_file(name)) {
        record->structure = IndexDiskUsageStructure::kBkd;
        return;
    }
    const size_t dot = name.rfind('.');
    const std::string_view extension =
            dot == std::string_view::npos ? std::string_view() : name.substr(dot + 1);
    if (extension == "tis" || extension == "tii") {
        record->dict_bytes += length;
    } else if (extension == "frq") {
        record->posting_bytes += length;
    } else if (extension == "prx") {
        record->position_bytes += length;
    } else if (extension == "nrm") {
        record->stats_bytes += length;
    } else {
        record->other_bytes += length;
    }
}

IndexDiskUsageCollector::IndexDiskUsageCollector(io::FileSystemSPtr fs,
                                                 std::string index_path_prefix,
                                                 TabletSchemaSPtr schema,
                                                 InvertedIndexStorageFormatPB format,
                                                 int64_t tablet_id)
        : _fs(std::move(fs)),
          _index_path_prefix(std::move(index_path_prefix)),
          _schema(std::move(schema)),
          _format(format),
          _tablet_id(tablet_id) {}

Status IndexDiskUsageCollector::collect(const IndexDiskUsageOptions& options,
                                        std::vector<IndexDiskUsageRecord>* out) {
    DORIS_CHECK(out != nullptr);
    switch (_format) {
    case InvertedIndexStorageFormatPB::V1:
        return _collect_v1(options, out);
    case InvertedIndexStorageFormatPB::V2:
    case InvertedIndexStorageFormatPB::V3:
        return _collect_compound(options, out);
    case InvertedIndexStorageFormatPB::SNII:
        return _collect_snii(options, out);
    default:
        return Status::NotSupported("index disk usage does not support inverted index format {}",
                                    InvertedIndexStorageFormatPB_Name(_format));
    }
}

Status IndexDiskUsageCollector::_collect_v1(const IndexDiskUsageOptions& options,
                                            std::vector<IndexDiskUsageRecord>* out) {
    IndexFileReader reader(_fs, _index_path_prefix, _format, InvertedIndexFileInfo(), _tablet_id);
    RETURN_IF_ERROR(reader.init());
    for (const TabletIndex* index : _schema->inverted_indexes()) {
        if (!is_wanted(options, index->index_id())) {
            continue;
        }
        const std::string path = InvertedIndexDescriptor::get_index_file_path_v1(
                _index_path_prefix, index->index_id(), index->get_index_suffix());
        int64_t file_size = 0;
        RETURN_IF_ERROR(_fs->file_size(path, &file_size));
        auto directory = DORIS_TRY(reader.open(index));

        IndexDiskUsageRecord record;
        record.index_id = index->index_id();
        record.index_suffix = index->get_index_suffix();
        int64_t files_bytes = 0;
        RETURN_IF_ERROR(add_directory_files(*directory, &record, &files_bytes));
        // Each V1 index owns its file, so its compound header is part of the index.
        record.other_bytes += file_size - files_bytes;
        record.total_bytes += file_size - files_bytes;
        out->push_back(std::move(record));
    }
    return Status::OK();
}

Status IndexDiskUsageCollector::_collect_compound(const IndexDiskUsageOptions& options,
                                                  std::vector<IndexDiskUsageRecord>* out) {
    IndexFileReader reader(_fs, _index_path_prefix, _format, InvertedIndexFileInfo(), _tablet_id);
    RETURN_IF_ERROR(reader.init());
    auto directories = DORIS_TRY(reader.get_all_directories());

    // Every index counts toward the attributed bytes, even the filtered ones, so the container
    // record only holds the shared header.
    int64_t attributed_bytes = 0;
    for (const auto& [key, directory] : directories) {
        IndexDiskUsageRecord record;
        record.index_id = key.first;
        record.index_suffix = key.second;
        RETURN_IF_ERROR(add_directory_files(*directory, &record, &attributed_bytes));
        if (is_wanted(options, record.index_id)) {
            out->push_back(std::move(record));
        }
    }
    if (options.index_ids.empty()) {
        IndexDiskUsageRecord container;
        container.structure = IndexDiskUsageStructure::kContainer;
        container.total_bytes = reader.get_inverted_file_size() - attributed_bytes;
        container.other_bytes = container.total_bytes;
        out->push_back(std::move(container));
    }
    return Status::OK();
}

Status IndexDiskUsageCollector::_collect_snii(const IndexDiskUsageOptions& options,
                                              std::vector<IndexDiskUsageRecord>* out) {
    IndexFileReader reader(_fs, _index_path_prefix, _format, InvertedIndexFileInfo(), _tablet_id);
    RETURN_IF_ERROR(reader.init());
    const auto entries = DORIS_TRY(reader.snii_logical_indexes());

    int64_t attributed_bytes = 0;
    for (const auto& entry : entries) {
        const auto index_id = cast_set<int64_t>(entry.index_id);
        // The container record is only reported without a filter, so filtered-out indexes
        // need no metadata read.
        if (!is_wanted(options, index_id)) {
            continue;
        }
        IndexDiskUsageRecord record;
        record.index_id = index_id;
        record.index_suffix = entry.index_suffix;
        if (entry.kind == snii::format::LogicalIndexKind::kInverted) {
            snii::format::CoreMetadata core;
            RETURN_IF_ERROR(reader.snii_core_metadata(entry.index_id, entry.index_suffix, &core));
            const auto& refs = core.section_refs;
            record.structure = IndexDiskUsageStructure::kTerm;
            record.dict_bytes = cast_set<int64_t>(refs.dict_region.length +
                                                  entry.sampled_term_index.length +
                                                  entry.dict_block_directory.length +
                                                  refs.bsbf.length);
            record.posting_bytes = cast_set<int64_t>(refs.posting_region.length);
            record.stats_bytes = cast_set<int64_t>(entry.core_metadata.length + refs.norms.length);
            record.other_bytes = cast_set<int64_t>(refs.null_bitmap.length);
            if (!snii::format::has_positions(core.index_config)) {
                record.position_bytes = 0;
            } else if (options.position_detail) {
                int64_t position_bytes = 0;
                RETURN_IF_ERROR(sum_snii_position_bytes(reader, entry.index_id, entry.index_suffix,
                                                        &position_bytes));
                record.position_bytes = position_bytes;
                record.posting_bytes -= position_bytes;
            } else {
                record.position_bytes = -1;
            }
            record.total_bytes = record.dict_bytes + record.posting_bytes +
                                 std::max<int64_t>(record.position_bytes, 0) +
                                 record.stats_bytes + record.other_bytes;
        } else {
            record.structure = entry.kind == snii::format::LogicalIndexKind::kBkd
                                       ? IndexDiskUsageStructure::kBkd
                                       : IndexDiskUsageStructure::kAnn;
            for (const auto& file : entry.files) {
                record.total_bytes += cast_set<int64_t>(file.length);
            }
        }
        attributed_bytes += record.total_bytes;
        out->push_back(std::move(record));
    }
    if (options.index_ids.empty()) {
        IndexDiskUsageRecord container;
        container.structure = IndexDiskUsageStructure::kContainer;
        container.total_bytes = reader.get_inverted_file_size() - attributed_bytes;
        container.other_bytes = container.total_bytes;
        out->push_back(std::move(container));
    }
    return Status::OK();
}

} // namespace doris::segment_v2
