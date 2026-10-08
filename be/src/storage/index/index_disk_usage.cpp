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
#include <charconv>
#include <deque>
#include <filesystem>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <system_error>
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

// Whether `status` says that the segment has no index container, which is only expected when the
// rowset meta records no container size: the size is recorded after the container is written.
bool is_skipped_container(const Status& status, const InvertedIndexFileInfo& file_info) {
    const bool recorded = file_info.has_index_size() && file_info.index_size() > 0;
    return !recorded && is_absent_index_file(status);
}

// A V1 index file and its size persisted in the rowset meta, or -1 when the size is not recorded.
struct V1IndexFile {
    const TabletIndex* index;
    int64_t persisted_size;
};

// Lists the V1 index files of a segment from the rowset meta, which records every written file,
// including the extracted VARIANT paths that the schema does not list. Rowsets without that record
// fall back to the inverted and ANN indexes of their own schema.
// `owned` keeps the indexes built from the rowset meta.
std::vector<V1IndexFile> list_v1_index_files(const TabletSchema& schema,
                                             const InvertedIndexFileInfo& file_info,
                                             std::deque<TabletIndex>* owned) {
    std::vector<V1IndexFile> files;
    if (file_info.index_info_size() == 0) {
        for (const TabletIndex* index : schema.inverted_and_ann_indexes()) {
            files.push_back({.index = index, .persisted_size = -1});
        }
        return files;
    }
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

// Parses the index id and suffix out of `{segment}_{index_id}[@{suffix}].idx`, the name of a V1
// index file of the segment named `segment`.
std::optional<std::pair<int64_t, std::string>> parse_v1_index_file_name(std::string_view name,
                                                                        std::string_view segment) {
    constexpr std::string_view extension = InvertedIndexDescriptor::index_suffix;
    if (name.size() < segment.size() + extension.size() + 2 || !name.starts_with(segment) ||
        !name.ends_with(extension) || name[segment.size()] != '_') {
        return std::nullopt;
    }
    std::string_view rest = name.substr(segment.size() + 1);
    rest.remove_suffix(extension.size());
    uint64_t index_id = 0;
    const auto [digits_end, error] =
            std::from_chars(rest.data(), rest.data() + rest.size(), index_id);
    if (error != std::errc() || index_id > std::numeric_limits<int64_t>::max()) {
        return std::nullopt;
    }
    std::string_view tail = rest.substr(digits_end - rest.data());
    if (tail.empty()) {
        return std::make_pair(static_cast<int64_t>(index_id), std::string());
    }
    if (tail.front() != '@') {
        return std::nullopt;
    }
    return std::make_pair(static_cast<int64_t>(index_id), std::string(tail.substr(1)));
}

// A legacy local VARIANT rowset has no index file info, and its schema may not name every file
// that was written for an extracted path. Adds the V1 index files of the segment that exist in its
// directory and are not in `files` yet, in index id and suffix order.
Status add_v1_index_files_on_disk(const std::string& index_path_prefix,
                                  DirectoryFileNames* directory_files,
                                  std::deque<TabletIndex>* owned, std::vector<V1IndexFile>* files) {
    const std::filesystem::path prefix(index_path_prefix);
    const std::vector<std::string>* names = nullptr;
    RETURN_IF_ERROR(directory_files->list(prefix.parent_path().string(), &names));
    std::set<std::pair<int64_t, std::string>> listed;
    for (const V1IndexFile& file : *files) {
        listed.emplace(file.index->index_id(), file.index->get_index_suffix());
    }
    // The names are sorted, so the files of this segment are adjacent.
    const std::string segment_name = prefix.filename().string();
    const std::string name_prefix = segment_name + "_";
    std::set<std::pair<int64_t, std::string>> on_disk;
    for (auto it = std::lower_bound(names->begin(), names->end(), name_prefix);
         it != names->end() && it->starts_with(name_prefix); ++it) {
        if (auto parsed = parse_v1_index_file_name(*it, segment_name);
            parsed.has_value() && !listed.contains(*parsed)) {
            on_disk.insert(std::move(*parsed));
        }
    }
    for (const auto& [index_id, suffix] : on_disk) {
        TabletIndexPB index_pb;
        index_pb.set_index_type(IndexType::INVERTED);
        index_pb.set_index_id(index_id);
        index_pb.set_index_suffix_name(suffix);
        owned->emplace_back().init_from_pb(index_pb);
        files->push_back({.index = &owned->back(), .persisted_size = -1});
    }
    return Status::OK();
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
    io::IOContext io_ctx = options.io_ctx != nullptr ? *options.io_ctx : io::IOContext {};
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

Status DirectoryFileNames::list(const std::string& dir, const std::vector<std::string>** names) {
    if (!_listed || _dir != dir) {
        // The file sizes are not read: unused rowsets delete their files from the tablet directory
        // while it is listed, and the stat of a vanished file would fail the listing. An entry whose
        // type cannot be read is gone and is skipped.
        std::error_code ec;
        std::filesystem::directory_iterator it(dir, ec);
        if (ec && ec != std::errc::no_such_file_or_directory) {
            return Status::IOError("failed to list {}: {}", dir, ec.message());
        }
        _names.clear();
        for (const std::filesystem::directory_iterator end; !ec && it != end; it.increment(ec)) {
            std::error_code type_ec;
            if (it->is_regular_file(type_ec)) {
                _names.push_back(it->path().filename().string());
            }
        }
        if (ec && ec != std::errc::no_such_file_or_directory) {
            return Status::IOError("failed to list {}: {}", dir, ec.message());
        }
        std::sort(_names.begin(), _names.end());
        _dir = dir;
        _listed = true;
        ++_listings;
    }
    *names = &_names;
    return Status::OK();
}

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
    // A rowset returns no file system when its tablet or storage resource cannot be resolved.
    if (_fs == nullptr) {
        return Status::Error<ErrorCode::INIT_FAILED>("no file system for inverted index files {}",
                                                     _index_path_prefix);
    }
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
    RETURN_IF_ERROR(reader.init(config::inverted_index_read_buffer_size, options.io_ctx));
    std::deque<TabletIndex> file_indexes;
    std::vector<V1IndexFile> v1_files =
            list_v1_index_files(*_schema, _index_file_info, &file_indexes);
    if (_index_file_info.index_info_size() == 0 && _schema->num_variant_columns() > 0 &&
        _fs->type() == io::FileSystemType::LOCAL) {
        DirectoryFileNames own_listing;
        RETURN_IF_ERROR(add_v1_index_files_on_disk(
                _index_path_prefix,
                options.directory_files != nullptr ? options.directory_files : &own_listing,
                &file_indexes, &v1_files));
    }
    for (const V1IndexFile& file : v1_files) {
        const TabletIndex& index = *file.index;
        if (!is_wanted(options, index.index_id())) {
            continue;
        }
        RETURN_IF_ERROR(check_cancelled(options));
        // The rowset meta records only files that were written, so only a file listed from the
        // schema may be absent.
        const bool recorded = file.persisted_size > 0;
        int64_t file_size = file.persisted_size;
        if (!recorded) {
            const std::string path = InvertedIndexDescriptor::get_index_file_path_v1(
                    _index_path_prefix, index.index_id(), index.get_index_suffix());
            const Status size_status = _fs->file_size(path, &file_size);
            if (is_absent_index_file(size_status)) {
                continue;
            }
            RETURN_IF_ERROR(size_status);
        }
        auto directory = reader.open(&index, options.io_ctx);
        if (!directory.has_value()) {
            if (!recorded && is_absent_index_file(directory.error())) {
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
    if (const Status st = reader.init(config::inverted_index_read_buffer_size, options.io_ctx);
        !st.ok()) {
        return is_skipped_container(st, _index_file_info) ? Status::OK() : st;
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
    if (const Status st = reader.init(config::inverted_index_read_buffer_size, options.io_ctx);
        !st.ok()) {
        return is_skipped_container(st, _index_file_info) ? Status::OK() : st;
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
            RETURN_IF_ERROR(reader.snii_core_metadata(entry.index_id, entry.index_suffix, &core,
                                                      options.io_ctx));
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
                    segment.ref(), &stats, &loaded, options.io_ctx));
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
