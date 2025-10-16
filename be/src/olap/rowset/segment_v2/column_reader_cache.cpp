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

#include "olap/rowset/segment_v2/segment.h"
#include "olap/rowset/segment_v2/variant/variant_column_reader.h"

bvar::Adder<int64_t> g_segment_column_reader_cache_count("segment_column_cache_count");
bvar::Adder<int64_t> g_segment_column_cache_hit_count("segment_column_cache_hit_count");
bvar::Adder<int64_t> g_segment_column_cache_miss_count("segment_column_cache_miss_count");
bvar::Adder<int64_t> g_segment_column_cache_evict_count("segment_column_cache_evict_count");

namespace doris::segment_v2 {

#include "common/compile_check_begin.h"

ColumnReaderCache::ColumnReaderCache(Segment* segment) : _segment(segment) {}

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

Status ColumnReaderCache::_insert(const ColumnReaderCacheKey& key, const ColumnReaderOptions& opts,
                                  const SegmentFooterPB& footer, int32_t column_id,
                                  const io::FileReaderSPtr& file_reader, size_t num_rows,
                                  std::shared_ptr<ColumnReader>* column_reader) {
    std::lock_guard<std::mutex> lock(_cache_mutex);
    // If already exists, and move it to the front
    if (_cache_map.find(key) != _cache_map.end()) {
        g_segment_column_cache_hit_count << 1;
        auto it = _cache_map[key];
        DCHECK_EQ(it->key.first, key.first);
        _lru_list.splice(_lru_list.begin(), _lru_list, it);
        *column_reader = it->reader;
        return Status::OK();
    }
    // If capacity exceeded, remove least recently used (tail)
    if (_cache_map.size() >= config::max_segment_partial_column_cache_size) {
        g_segment_column_reader_cache_count << -1;
        g_segment_column_cache_evict_count << 1;
        auto last_it = _lru_list.end();
        --last_it;
        _cache_map.erase(last_it->key);
        _lru_list.pop_back();
    }
    std::shared_ptr<ColumnReader> reader;
    RETURN_IF_ERROR(ColumnReader::create(opts, footer, column_id, num_rows, file_reader, &reader));
    // Insert new node at the front
    std::shared_ptr<ColumnReader> reader_ptr = std::move(reader);
    g_segment_column_reader_cache_count << 1;
    DCHECK(key.first >= 0) << " col_uid: " << key.first
                           << " relative_path: " << key.second.get_path();
    _lru_list.push_front(CacheNode {key, reader_ptr, std::chrono::steady_clock::now()});
    _cache_map[key] = _lru_list.begin();
    *column_reader = reader_ptr;
    VLOG_DEBUG << "insert cache: " << key.first << " " << key.second.get_path()
               << ", type: " << (int)reader_ptr->get_meta_type()
               << ", cache_size: " << _cache_map.size() << ", list_size: " << _lru_list.size()
               << ", cache_map: " << _cache_map.size() << ", lru_list: " << _lru_list.size();
    return Status::OK();
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
    auto it = _segment->_column_uid_to_footer_ordinal.find(col_uid);
    if (it == _segment->_column_uid_to_footer_ordinal.end()) {
        // no such column in this segment, return nullptr
        *column_reader = nullptr;
        return Status::Error<ErrorCode::NOT_FOUND, false>("column not found in segment, col_uid={}",
                                                          col_uid);
    }
    std::shared_ptr<SegmentFooterPB> footer_pb_shared;
    {
        std::lock_guard<std::mutex> lock(_cache_mutex);
        // keep the lock until the footer is loaded, since _get_segment_footer is not thread safe
        RETURN_IF_ERROR(_segment->_get_segment_footer(footer_pb_shared, stats));
    }
    // lazy create column reader from footer
    const auto& col_footer_pb = footer_pb_shared->columns(static_cast<int>(it->second));
    ColumnReaderOptions opts {
            .kept_in_memory = _segment->tablet_schema()->is_in_memory(),
            .be_exec_version = _be_exec_version,
            .tablet_schema = _segment->tablet_schema(),
    };
    VLOG_DEBUG << "insert cache: " << col_uid << " "
               << ""
               << ", type: " << (int)col_footer_pb.type() << ", footer_ordinal: " << it->second;
    return _insert({col_uid, {}}, opts, *footer_pb_shared, static_cast<int>(it->second),
                   _segment->_file_reader, _segment->num_rows(), column_reader);
}

Status ColumnReaderCache::get_path_column_reader(uint32_t col_uid,
                                                 vectorized::PathInData relative_path,
                                                 std::shared_ptr<ColumnReader>* column_reader,
                                                 OlapReaderStatistics* stats,
                                                 const SubcolumnColumnMetaInfo::Node* node_hint) {
    // Attempt to find in cache
    if (auto cached = _lookup({col_uid, relative_path})) {
        *column_reader = cached;
        return Status::OK();
    }
    const SubcolumnColumnMetaInfo::Node* node = node_hint;
    std::shared_ptr<ColumnReader> variant_column_reader;
    if (node == nullptr) {
        RETURN_IF_ERROR(get_column_reader(col_uid, &variant_column_reader, stats));
        node = variant_column_reader
                       ? static_cast<VariantColumnReader*>(variant_column_reader.get())
                                 ->get_subcolumn_meta_by_path(relative_path)
                       : nullptr;
    }
    if (node != nullptr) {
        // lazy create column reader from footer
        DCHECK_GE(node->data.footer_ordinal, 0);
        std::shared_ptr<SegmentFooterPB> footer_pb_shared;
        {
            std::lock_guard<std::mutex> lock(_cache_mutex);
            // keep the lock until the footer is loaded, since _get_segment_footer is not thread safe
            RETURN_IF_ERROR(_segment->_get_segment_footer(footer_pb_shared, stats));
        }
        const auto& col_footer_pb = footer_pb_shared->columns(node->data.footer_ordinal);
        ColumnReaderOptions opts {
                .kept_in_memory = _segment->tablet_schema()->is_in_memory(),
                .be_exec_version = _be_exec_version,
                .tablet_schema = _segment->tablet_schema(),
        };
        VLOG_DEBUG << "insert cache: " << col_uid << " "
                   << ""
                   << ", type: " << (int)col_footer_pb.type()
                   << ", footer_ordinal: " << node->data.footer_ordinal;
        RETURN_IF_ERROR(_insert({col_uid, node->path}, opts, *footer_pb_shared,
                                node->data.footer_ordinal, _segment->_file_reader,
                                _segment->num_rows(), column_reader));
        return Status::OK();
    }
    // no such column in this segment, return nullptr
    *column_reader = nullptr;
    return Status::Error<ErrorCode::NOT_FOUND, false>("column not found in segment, col_uid={}",
                                                      col_uid);
}

#include "common/compile_check_end.h"

} // namespace doris::segment_v2