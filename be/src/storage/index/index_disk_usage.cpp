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
#include <map>
#include <memory>
#include <tuple>
#include <utility>

#include "common/cast_set.h"
#include "common/check.h"
#include "io/io_common.h"
#include "storage/index/ann/ann_index_files.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/inverted/inverted_index_desc.h"
#include "storage/index/snii/format/dict_entry.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/olap_common.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/segment/segment.h"

namespace doris::segment_v2 {

namespace {

bool is_bkd_file(std::string_view name) {
    return name == InvertedIndexDescriptor::get_temporary_bkd_index_data_file_name() ||
           name == InvertedIndexDescriptor::get_temporary_bkd_index_meta_file_name() ||
           name == InvertedIndexDescriptor::get_temporary_bkd_index_file_name();
}

bool is_ann_file(std::string_view name) {
    return name == faiss_index_fila_name || name == faiss_ivfdata_file_name;
}

bool is_wanted(const IndexDiskUsageOptions& options, int64_t index_id) {
    return options.index_ids.empty() || options.index_ids.contains(index_id);
}

Status check_cancelled(const IndexDiskUsageOptions& options) {
    return options.check_cancelled ? options.check_cancelled() : Status::OK();
}

// A segment may have no index file on purpose, for example when every ANN index skipped a segment
// too small to train, or when a legacy table skipped writing indexes on load. Such a file holds
// no index bytes, while any other error is still reported.
bool is_absent_index_file(const Status& status) {
    return status.is<ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND>() ||
           status.is<ErrorCode::INVERTED_INDEX_BYPASS>() || status.is<ErrorCode::NOT_FOUND>();
}

// A V1 index file and its size persisted in the rowset meta, or -1 when the size is not recorded.
struct V1IndexFile {
    const TabletIndex* index;
    int64_t persisted_size;
};

// Lists the V1 index files of a segment. The rowset meta records every file, including the
// extracted VARIANT paths that the schema does not list; rowsets written without that record fall
// back to the schema indexes. `owned` keeps the indexes built from the rowset meta.
std::vector<V1IndexFile> list_v1_index_files(const TabletSchema& schema,
                                             const InvertedIndexFileInfo& file_info,
                                             std::vector<TabletIndex>* owned) {
    std::vector<V1IndexFile> files;
    if (file_info.index_info_size() == 0) {
        for (const TabletIndex* index : schema.inverted_indexes()) {
            files.push_back({.index = index, .persisted_size = -1});
        }
        return files;
    }
    owned->reserve(file_info.index_info_size());
    for (const auto& index_info : file_info.index_info()) {
        TabletIndexPB index_pb;
        index_pb.set_index_type(IndexType::INVERTED);
        index_pb.set_index_id(index_info.index_id());
        index_pb.set_index_suffix_name(index_info.index_suffix());
        owned->emplace_back().init_from_pb(index_pb);
        files.push_back({.index = &owned->back(),
                         .persisted_size = index_info.index_file_size() > 0
                                                   ? index_info.index_file_size()
                                                   : -1});
    }
    return files;
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
                               std::string_view suffix, const IndexDiskUsageOptions& options,
                               int64_t* position_bytes) {
    // A full dictionary scan should not evict blocks that queries keep in the file cache.
    io::IOContext io_ctx;
    io_ctx.is_disposable = true;
    io_ctx.is_inverted_index = true;
    auto logical = DORIS_TRY(reader.open_snii_logical_index(
            index_id, suffix, &io_ctx, snii::reader::LogicalIndexOpenMode::kCompaction));
    snii_doris::DorisSniiFileReader::ScopedIOContext io_context_scope(&io_ctx);
    std::vector<snii::format::DictEntry> entries;
    for (uint32_t block = 0; block < logical->n_dict_blocks(); ++block) {
        RETURN_IF_ERROR(check_cancelled(options));
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

int64_t merge_component(int64_t lhs, int64_t rhs) {
    return lhs < 0 || rhs < 0 ? -1 : lhs + rhs;
}

} // namespace

std::vector<IndexDiskUsageRow> aggregate_index_disk_usage(std::vector<IndexDiskUsageRow> rows,
                                                          IndexDiskUsageLevel level) {
    if (level == IndexDiskUsageLevel::kSegment) {
        return rows;
    }
    using Key = std::tuple<std::string, int64_t, std::string, int, int>;
    std::map<Key, size_t> positions;
    std::vector<IndexDiskUsageRow> merged;
    for (auto& row : rows) {
        row.segment_id = -1;
        if (level == IndexDiskUsageLevel::kTablet) {
            row.rowset_id.clear();
        }
        Key key {row.rowset_id, row.record.index_id, row.record.index_suffix,
                 static_cast<int>(row.format), static_cast<int>(row.record.structure)};
        auto [it, inserted] = positions.emplace(std::move(key), merged.size());
        if (inserted) {
            merged.push_back(std::move(row));
            continue;
        }
        IndexDiskUsageRow& target = merged[it->second];
        target.segment_count += row.segment_count;
        target.row_count += row.row_count;
        IndexDiskUsageRecord& dst = target.record;
        const IndexDiskUsageRecord& src = row.record;
        dst.total_bytes += src.total_bytes;
        dst.dict_bytes = merge_component(dst.dict_bytes, src.dict_bytes);
        dst.posting_bytes = merge_component(dst.posting_bytes, src.posting_bytes);
        dst.position_bytes = merge_component(dst.position_bytes, src.position_bytes);
        dst.stats_bytes = merge_component(dst.stats_bytes, src.stats_bytes);
        dst.other_bytes = merge_component(dst.other_bytes, src.other_bytes);
    }
    return merged;
}

void classify_clucene_file(std::string_view name, int64_t length, IndexDiskUsageRecord* record) {
    record->total_bytes += length;
    if (is_bkd_file(name)) {
        record->structure = IndexDiskUsageStructure::kBkd;
        return;
    }
    if (is_ann_file(name)) {
        record->structure = IndexDiskUsageStructure::kAnn;
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
                                                 int64_t tablet_id,
                                                 InvertedIndexFileInfo index_file_info)
        : _fs(std::move(fs)),
          _index_path_prefix(std::move(index_path_prefix)),
          _schema(std::move(schema)),
          _format(format),
          _tablet_id(tablet_id),
          _index_file_info(std::move(index_file_info)) {}

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
    IndexFileReader reader(_fs, _index_path_prefix, _format, _index_file_info, _tablet_id);
    RETURN_IF_ERROR(reader.init());
    std::vector<TabletIndex> file_indexes;
    for (const V1IndexFile& file : list_v1_index_files(*_schema, _index_file_info, &file_indexes)) {
        const TabletIndex& index = *file.index;
        if (!is_wanted(options, index.index_id())) {
            continue;
        }
        RETURN_IF_ERROR(check_cancelled(options));
        int64_t file_size = file.persisted_size;
        if (file_size < 0) {
            const std::string path = InvertedIndexDescriptor::get_index_file_path_v1(
                    _index_path_prefix, index.index_id(), index.get_index_suffix());
            const Status size_status = _fs->file_size(path, &file_size);
            if (is_absent_index_file(size_status)) {
                continue;
            }
            RETURN_IF_ERROR(size_status);
        }
        auto directory = reader.open(&index);
        if (!directory.has_value()) {
            if (is_absent_index_file(directory.error())) {
                continue;
            }
            return directory.error();
        }

        IndexDiskUsageRecord record;
        record.index_id = index.index_id();
        record.index_suffix = index.get_index_suffix();
        int64_t files_bytes = 0;
        RETURN_IF_ERROR(add_directory_files(*directory.value(), &record, &files_bytes));
        // Each V1 index owns its file, so its compound header is part of the index.
        record.other_bytes += file_size - files_bytes;
        record.total_bytes += file_size - files_bytes;
        out->push_back(std::move(record));
    }
    return Status::OK();
}

Status IndexDiskUsageCollector::_collect_compound(const IndexDiskUsageOptions& options,
                                                  std::vector<IndexDiskUsageRecord>* out) {
    IndexFileReader reader(_fs, _index_path_prefix, _format, _index_file_info, _tablet_id);
    if (const Status st = reader.init(); !st.ok()) {
        return is_absent_index_file(st) ? Status::OK() : st;
    }
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
    IndexFileReader reader(_fs, _index_path_prefix, _format, _index_file_info, _tablet_id);
    if (const Status st = reader.init(); !st.ok()) {
        return is_absent_index_file(st) ? Status::OK() : st;
    }
    const auto entries = DORIS_TRY(reader.snii_logical_indexes());

    int64_t attributed_bytes = 0;
    for (const auto& entry : entries) {
        const auto index_id = cast_set<int64_t>(entry.index_id);
        // The container record is only reported without a filter, so filtered-out indexes
        // need no metadata read.
        if (!is_wanted(options, index_id)) {
            continue;
        }
        RETURN_IF_ERROR(check_cancelled(options));
        IndexDiskUsageRecord record;
        record.index_id = index_id;
        record.index_suffix = entry.index_suffix;
        if (entry.kind == snii::format::LogicalIndexKind::kInverted) {
            snii::format::CoreMetadata core;
            RETURN_IF_ERROR(reader.snii_core_metadata(entry.index_id, entry.index_suffix, &core));
            const auto& refs = core.section_refs;
            record.structure = IndexDiskUsageStructure::kTerm;
            record.dict_bytes =
                    cast_set<int64_t>(refs.dict_region.length + entry.sampled_term_index.length +
                                      entry.dict_block_directory.length + refs.bsbf.length);
            record.posting_bytes = cast_set<int64_t>(refs.posting_region.length);
            record.stats_bytes = cast_set<int64_t>(entry.core_metadata.length + refs.norms.length);
            record.other_bytes = cast_set<int64_t>(refs.null_bitmap.length);
            if (!snii::format::has_positions(core.index_config)) {
                record.position_bytes = 0;
            } else if (options.position_detail) {
                int64_t position_bytes = 0;
                RETURN_IF_ERROR(sum_snii_position_bytes(reader, entry.index_id, entry.index_suffix,
                                                        options, &position_bytes));
                record.position_bytes = position_bytes;
                record.posting_bytes -= position_bytes;
            } else {
                record.position_bytes = -1;
            }
            record.total_bytes = record.dict_bytes + record.posting_bytes +
                                 std::max<int64_t>(record.position_bytes, 0) + record.stats_bytes +
                                 record.other_bytes;
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

const TabletIndex* resolve_disk_usage_index(const TabletSchema& schema, int64_t index_id,
                                            std::string_view suffix) {
    // An extracted VARIANT path keeps its parent index id under a path suffix that the current
    // schema may not list, so an unmatched suffix resolves to the index of the same id.
    const TabletIndex* same_id = nullptr;
    for (const TabletIndex* index : schema.inverted_and_ann_indexes()) {
        if (index->index_id() != index_id) {
            continue;
        }
        if (index->get_index_suffix() == suffix) {
            return index;
        }
        if (same_id == nullptr || index->get_index_suffix().empty()) {
            same_id = index;
        }
    }
    return same_id;
}

Status collect_rowset_index_disk_usage(const RowsetSharedPtr& rowset,
                                       const IndexDiskUsageOptions& options, int64_t tablet_id,
                                       std::vector<IndexDiskUsageRow>* rows) {
    const TabletSchemaSPtr schema = rowset->tablet_schema();
    if (!schema->has_inverted_or_ann_index()) {
        return Status::OK();
    }
    const InvertedIndexStorageFormatPB format = schema->get_inverted_index_storage_format();
    const std::string rowset_id = rowset->rowset_id().to_string();
    for (auto segment : rowset->segments()) {
        RETURN_IF_ERROR(check_cancelled(options));
        const std::string segment_path = DORIS_TRY(segment.path());
        IndexDiskUsageCollector collector(
                rowset->rowset_meta()->fs(),
                std::string(InvertedIndexDescriptor::get_index_file_path_prefix(segment_path)),
                schema, format, tablet_id, segment.inverted_index_file_info());
        std::vector<IndexDiskUsageRecord> records;
        RETURN_IF_ERROR(collector.collect(options, &records));
        if (records.empty()) {
            continue;
        }
        int64_t row_count = 0;
        if (segment.has_num_rows()) {
            row_count = segment.num_rows();
        } else {
            // Rowsets without persisted row counts read them from the segment footer. Loading the
            // segment directly keeps a transient failure out of the rowset's run-once row cache.
            SegmentSharedPtr loaded;
            OlapReaderStatistics stats;
            RETURN_IF_ERROR(std::static_pointer_cast<BetaRowset>(rowset)->load_segment(
                    segment.ref(), &stats, &loaded));
            row_count = loaded->num_rows();
        }
        for (auto& record : records) {
            IndexDiskUsageRow row;
            row.rowset_id = rowset_id;
            row.segment_id = cast_set<int32_t>(segment.id());
            row.segment_count = 1;
            row.row_count = row_count;
            row.format = format;
            row.record = std::move(record);
            rows->push_back(std::move(row));
        }
    }
    return Status::OK();
}

} // namespace doris::segment_v2
