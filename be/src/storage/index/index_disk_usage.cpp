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

#include <utility>

#include "common/check.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/inverted/inverted_index_desc.h"

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

// Classifies every sub-file of a CLucene directory into `record` and returns their total length.
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
                                                 TabletSchemaSPtr /*schema*/,
                                                 InvertedIndexStorageFormatPB format,
                                                 int64_t tablet_id)
        : _fs(std::move(fs)),
          _index_path_prefix(std::move(index_path_prefix)),
          _format(format),
          _tablet_id(tablet_id) {}

Status IndexDiskUsageCollector::collect(const IndexDiskUsageOptions& options,
                                        std::vector<IndexDiskUsageRecord>* out) {
    DORIS_CHECK(out != nullptr);
    switch (_format) {
    case InvertedIndexStorageFormatPB::V2:
    case InvertedIndexStorageFormatPB::V3:
        return _collect_compound(options, out);
    default:
        return Status::NotSupported("index disk usage does not support inverted index format {}",
                                    InvertedIndexStorageFormatPB_Name(_format));
    }
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

} // namespace doris::segment_v2
