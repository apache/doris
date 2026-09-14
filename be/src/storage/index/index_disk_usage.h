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

#include <gen_cpp/olap_file.pb.h>

#include <cstdint>
#include <set>
#include <string>
#include <string_view>
#include <vector>

#include "common/status.h"
#include "io/fs/file_system.h"
#include "storage/tablet/tablet_schema.h"

namespace doris::segment_v2 {

enum class IndexDiskUsageStructure : uint8_t { kTerm, kBkd, kAnn, kContainer };

// Physical bytes of one index, or of a container's shared overhead, in one segment.
// A component of -1 means it is unknown.
struct IndexDiskUsageRecord {
    int64_t index_id = -1;
    std::string index_suffix;
    IndexDiskUsageStructure structure = IndexDiskUsageStructure::kTerm;
    int64_t total_bytes = 0;
    int64_t dict_bytes = 0;
    int64_t posting_bytes = 0;
    int64_t position_bytes = 0;
    int64_t stats_bytes = 0;
    int64_t other_bytes = 0;
};

struct IndexDiskUsageOptions {
    // Scan SNII dictionary blocks to split positions out of postings.
    bool position_detail = false;
    // Empty means all indexes.
    std::set<int64_t> index_ids;
};

enum class IndexDiskUsageLevel : uint8_t { kTablet, kRowset, kSegment };

// A record tagged with the rowset and segment it was collected from.
struct IndexDiskUsageRow {
    std::string rowset_id;
    int32_t segment_id = -1;
    int64_t segment_count = 0;
    int64_t row_count = 0;
    InvertedIndexStorageFormatPB format = InvertedIndexStorageFormatPB::V2;
    IndexDiskUsageRecord record;
};

// Merges rows of the same index, format and structure within the granularity of `level`.
// Merged rows keep first-appearance order; a component unknown in any input stays unknown.
std::vector<IndexDiskUsageRow> aggregate_index_disk_usage(std::vector<IndexDiskUsageRow> rows,
                                                          IndexDiskUsageLevel level);

// Adds a CLucene sub-file to the component it belongs to. BKD sub-files only count toward the
// total and mark the record as a BKD structure.
void classify_clucene_file(std::string_view name, int64_t length, IndexDiskUsageRecord* record);

// Reads the index file metadata of one segment and reports the bytes of each index.
class IndexDiskUsageCollector {
public:
    IndexDiskUsageCollector(io::FileSystemSPtr fs, std::string index_path_prefix,
                            TabletSchemaSPtr schema, InvertedIndexStorageFormatPB format,
                            int64_t tablet_id);

    // Appends one record per index, plus a container record for a shared file when no index
    // filter is given.
    Status collect(const IndexDiskUsageOptions& options, std::vector<IndexDiskUsageRecord>* out);

private:
    Status _collect_v1(const IndexDiskUsageOptions& options,
                       std::vector<IndexDiskUsageRecord>* out);
    Status _collect_compound(const IndexDiskUsageOptions& options,
                             std::vector<IndexDiskUsageRecord>* out);
    Status _collect_snii(const IndexDiskUsageOptions& options,
                         std::vector<IndexDiskUsageRecord>* out);

    io::FileSystemSPtr _fs;
    std::string _index_path_prefix;
    TabletSchemaSPtr _schema;
    InvertedIndexStorageFormatPB _format;
    int64_t _tablet_id;
};

} // namespace doris::segment_v2
