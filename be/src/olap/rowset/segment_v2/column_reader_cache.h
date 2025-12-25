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

#include "agent/be_exec_version_manager.h"
#include "io/fs/file_reader.h"
#include "olap/rowset/segment_v2/stream_reader.h"
#include "olap/tablet_fwd.h"
#include "vec/json/path_in_data.h"

namespace doris::segment_v2 {

class ColumnReader;
class ColumnReaderCache;
class ColumnMetaAccessor;

// Key: pair of column uid and variant path (empty for normal column)
using ColumnReaderCacheKey = std::pair<int32_t, vectorized::PathInData>;

// This node holds the cached ColumnReader and its key.
struct CacheNode {
    ColumnReaderCacheKey key; // key: (column uid, column path)
    std::shared_ptr<ColumnReader> reader;
    std::chrono::steady_clock::time_point last_access; // optional if needed
};

// This cache is used to cache column readers for columns in segment.
// Limit the column reader count to config::max_segment_partial_column_cache_size
class ColumnReaderCache {
public:
    // Main constructor used in production: cache is bound to a specific segment's
    // ColumnMetaAccessor, TabletSchema, file reader and row count, plus a footer
    // getter callback (Segment::_get_segment_footer).
    ColumnReaderCache(
            ColumnMetaAccessor* accessor, TabletSchemaSPtr tablet_schema,
            io::FileReaderSPtr file_reader, uint64_t num_rows,
            std::function<Status(std::shared_ptr<SegmentFooterPB>&, OlapReaderStatistics*)>
                    get_footer_cb);
    virtual ~ColumnReaderCache();
    // Get all available readers
    // if include_subcolumns is true, return all available readers, including subcolumn readers
    // otherwise, return only none variant subcolumn readers
    std::map<int32_t, std::shared_ptr<ColumnReader>> get_available_readers(bool include_subcolumns);

    // Get column reader by column unique id
    Status get_column_reader(int32_t col_uid, std::shared_ptr<ColumnReader>* column_reader,
                             OlapReaderStatistics* stats);

    // Get column reader by column unique id and path(leaf node of variant's subcolumn)
    virtual Status get_path_column_reader(int32_t col_uid, vectorized::PathInData relative_path,
                                          std::shared_ptr<ColumnReader>* column_reader,
                                          OlapReaderStatistics* stats,
                                          const SubcolumnColumnMetaInfo::Node* node_hint = nullptr);

private:
    // Lookup function remains similar
    std::shared_ptr<ColumnReader> _lookup(const ColumnReaderCacheKey& key);
    // Insert helper: assumes _cache_mutex already locked, will evict if needed
    void _insert_locked_nocheck(const ColumnReaderCacheKey& key,
                                const std::shared_ptr<ColumnReader>& reader);

    // Insert an already-created reader directly into cache
    void _insert_direct(const ColumnReaderCacheKey& key,
                        const std::shared_ptr<ColumnReader>& column_reader);
    // keep _lru_list and _cache_map thread safe
    std::mutex _cache_mutex;
    // Doubly-linked list to maintain LRU order
    std::list<CacheNode> _lru_list;
    // Map from key to list iterator for O(1) access
    std::unordered_map<ColumnReaderCacheKey, std::list<CacheNode>::iterator> _cache_map;
    int _be_exec_version = BeExecVersionManager::get_newest_version();

    // Non-owning pointer to the segment's ColumnMetaAccessor.
    ColumnMetaAccessor* _accessor = nullptr;
    // Segment-level context needed to construct ColumnReader.
    TabletSchemaSPtr _tablet_schema;
    io::FileReaderSPtr _file_reader;
    uint64_t _num_rows = 0;
    // Callback to get footer, usually bound to Segment::_get_segment_footer.
    std::function<Status(std::shared_ptr<SegmentFooterPB>&, OlapReaderStatistics*)> _get_footer_cb;
};

} // namespace doris::segment_v2