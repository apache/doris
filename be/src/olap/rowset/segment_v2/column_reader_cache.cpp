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

#include "olap/rowset/segment_v2/column_reader_cache.h"

#include "olap/rowset/segment_v2/column_meta_accessor.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/rowset/segment_v2/variant/variant_column_reader.h"
#

bvar::Adder<int64_t> g_segment_column_reader_cache_count("segment_column_cache_count");
bvar::Adder<int64_t> g_segment_column_cache_hit_count("segment_column_cache_hit_count");
bvar::Adder<int64_t> g_segment_column_cache_miss_count("segment_column_cache_miss_count");
bvar::Adder<int64_t> g_segment_column_cache_evict_count("segment_column_cache_evict_count");

namespace doris::segment_v2 {

#include "common/compile_check_begin.h"

ColumnReaderCache::ColumnReaderCache(
        ColumnMetaAccessor* accessor, TabletSchemaSPtr tablet_schema,
        io::FileReaderSPtr file_reader, uint64_t num_rows,
        std::function<Status(std::shared_ptr<SegmentFooterPB>&, OlapReaderStatistics*)>
                get_footer_cb)
        : _accessor(accessor),
          _tablet_schema(std::move(tablet_schema)),
          _file_reader(std::move(file_reader)),
          _num_rows(num_rows),
          _get_footer_cb(std::move(get_footer_cb)) {}

ColumnReaderCache::~ColumnReaderCache() {
    g_segment_column_reader_cache_count << -_cache_map.size();
}

std::shared_ptr<ColumnReader> ColumnReaderCache::_lookup(const ColumnReaderCacheKey& key) {
    std::lock_guard<std::mutex> lock(_cache_mutex);
    auto it = _cache_map.find(key);
    if (it == _cache_map.end()) {
        g_segment_column_cache_miss_count << 1;
        return nullptr;
    }
    // Move the accessed node to the front of the linked list
    _lru_list.splice(_lru_list.begin(), _lru_list, it->second);
    DCHECK_EQ(it->second->key.first, key.first);
    g_segment_column_cache_hit_count << 1;
    return it->second->reader;
}

void ColumnReaderCache::_insert_locked_nocheck(const ColumnReaderCacheKey& key,
                                               const std::shared_ptr<ColumnReader>& reader) {
    // If capacity exceeded, remove least recently used (tail)
    if (_cache_map.size() >= config::max_segment_partial_column_cache_size) {
        g_segment_column_reader_cache_count << -1;
        g_segment_column_cache_evict_count << 1;
        auto last_it = _lru_list.end();
        --last_it;
        _cache_map.erase(last_it->key);
        _lru_list.pop_back();
    }
    g_segment_column_reader_cache_count << 1;
    _lru_list.push_front(CacheNode {
            .key = key, .reader = reader, .last_access = std::chrono::steady_clock::now()});
    _cache_map[key] = _lru_list.begin();
}

void ColumnReaderCache::_insert_direct(const ColumnReaderCacheKey& key,
                                       const std::shared_ptr<ColumnReader>& column_reader) {
    std::lock_guard<std::mutex> lock(_cache_mutex);
    _insert_locked_nocheck(key, column_reader);
}

std::map<int32_t, std::shared_ptr<ColumnReader>> ColumnReaderCache::get_available_readers(
        bool include_subcolumns) {
    std::lock_guard<std::mutex> lock(_cache_mutex);
    std::map<int32_t, std::shared_ptr<ColumnReader>> readers;
    for (const auto& node : _lru_list) {
        if (include_subcolumns || node.key.second.empty()) {
            readers.insert({node.key.first, node.reader});
        }
    }
    return readers;
}

Status ColumnReaderCache::get_column_reader(int32_t col_uid,
                                            std::shared_ptr<ColumnReader>* column_reader,
                                            OlapReaderStatistics* stats) {
    // Attempt to find in cache
    if (auto cached = _lookup({col_uid, {}})) {
        *column_reader = cached;
        return Status::OK();
    }
    // Load footer once under cache mutex (not thread-safe otherwise)
    std::shared_ptr<SegmentFooterPB> footer_pb_shared;
    {
        std::lock_guard<std::mutex> lock(_cache_mutex);
        RETURN_IF_ERROR(_get_footer_cb(footer_pb_shared, stats));
    }

    // Lookup column meta by uid via ColumnMetaAccessor. If not initialized or not found, return NOT_FOUND.
    ColumnMetaPB meta;
    Status st_meta = _accessor->get_column_meta_by_uid(*footer_pb_shared, col_uid, &meta);
    if (st_meta.is<ErrorCode::NOT_FOUND>()) {
        *column_reader = nullptr;
        return st_meta;
    }
    RETURN_IF_ERROR(st_meta);

    ColumnReaderOptions opts {.kept_in_memory = _tablet_schema->is_in_memory(),
                              .be_exec_version = _be_exec_version,
                              .tablet_schema = _tablet_schema};

    std::shared_ptr<ColumnReader> reader;
    if ((FieldType)meta.type() == FieldType::OLAP_FIELD_TYPE_VARIANT) {
        // Variant root columns require VariantColumnReader, which encapsulates
        // subcolumn layout, sparse columns and external meta.
        std::unique_ptr<VariantColumnReader> variant_reader(new VariantColumnReader());
        RETURN_IF_ERROR(variant_reader->init(opts, _accessor, footer_pb_shared, col_uid, _num_rows,
                                             _file_reader));
        reader.reset(variant_reader.release());
        VLOG_DEBUG << "insert cache (variant): uid=" << col_uid << " col_id=" << meta.column_id();
    } else {
        // For non-variant columns, we can create reader directly from ColumnMetaPB.
        VLOG_DEBUG << "insert cache: uid=" << col_uid << " col_id=" << meta.column_id();
        RETURN_IF_ERROR(ColumnReader::create(opts, meta, _num_rows, _file_reader, &reader));
    }

    _insert_direct({col_uid, {}}, reader);
    *column_reader = std::move(reader);
    return Status::OK();
}

Status ColumnReaderCache::get_path_column_reader(int32_t col_uid,
                                                 vectorized::PathInData relative_path,
                                                 std::shared_ptr<ColumnReader>* column_reader,
                                                 OlapReaderStatistics* stats,
                                                 const SubcolumnColumnMetaInfo::Node* node_hint) {
    // Attempt to find in cache first
    if (auto cached = _lookup({col_uid, relative_path})) {
        *column_reader = cached;
        return Status::OK();
    }

    if (!_accessor->has_column_uid(col_uid)) {
        *column_reader = nullptr;
        return Status::Error<ErrorCode::NOT_FOUND, false>("column not found in segment, col_uid={}",
                                                          col_uid);
    }

    // Load footer once under cache mutex (not thread-safe otherwise)
    std::shared_ptr<SegmentFooterPB> footer_pb_shared;
    {
        std::lock_guard<std::mutex> lock(_cache_mutex);
        RETURN_IF_ERROR(_get_footer_cb(footer_pb_shared, stats));
    }

    // Ensure variant root reader is available in cache.
    ColumnReaderOptions opts {.kept_in_memory = _tablet_schema->is_in_memory(),
                              .be_exec_version = _be_exec_version,
                              .tablet_schema = _tablet_schema};
    std::shared_ptr<ColumnReader> variant_column_reader;
    RETURN_IF_ERROR(get_column_reader(col_uid, &variant_column_reader, stats));

    if (relative_path.empty()) {
        *column_reader = std::move(variant_column_reader);
        return Status::OK();
    }

    // Delegate path-level reader creation to VariantColumnReader, which hides
    // inline vs external meta details.
    std::shared_ptr<ColumnReader> path_reader;
    auto* vreader = static_cast<VariantColumnReader*>(variant_column_reader.get());
    Status st = vreader->create_path_reader(relative_path, opts, _accessor, *footer_pb_shared,
                                            _file_reader, _num_rows, &path_reader);
    if (st.is<ErrorCode::NOT_FOUND>()) {
        *column_reader = nullptr;
        return st;
    }
    RETURN_IF_ERROR(st);

    // Cache and return
    _insert_direct({col_uid, relative_path}, path_reader);
    *column_reader = std::move(path_reader);
    return Status::OK();
}

#include "common/compile_check_end.h"

} // namespace doris::segment_v2