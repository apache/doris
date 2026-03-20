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

#include "service/http/action/clear_cache_action.h"

#include <sstream>
#include <string>

#include "runtime/memory/cache_manager.h"
#include "service/http/http_channel.h"
#include "service/http/http_headers.h"
#include "service/http/http_request.h"
#include "service/http/http_status.h"

namespace doris {

void ClearCacheAction::handle(HttpRequest* req) {
    req->add_output_header(HttpHeaders::CONTENT_TYPE, "text/plain; version=0.0.4");
    std::string cache_type_str = req->param("type");
    fmt::memory_buffer return_string_buffer;
    int64_t freed_size = 0;
    if (cache_type_str == "all_cache") {
        freed_size = CacheManager::instance()->for_each_cache_prune_all(nullptr, true);
    } else if (cache_type_str == "all" || cache_type_str == "data_page") {
        freed_size = CacheManager::instance()->cache_prune_all(
                CachePolicy::CacheType::DATA_PAGE_CACHE, true);
    } else if (cache_type_str == "IndexPageCache") {
        freed_size = CacheManager::instance()->cache_prune_all(
                CachePolicy::CacheType::INDEXPAGE_CACHE, true);
    } else if (cache_type_str == "PKIndexPageCache") {
        freed_size = CacheManager::instance()->cache_prune_all(
                CachePolicy::CacheType::PK_INDEX_PAGE_CACHE, true);
    } else if (cache_type_str == "SchemaCache") {
        freed_size = CacheManager::instance()->cache_prune_all(CachePolicy::CacheType::SCHEMA_CACHE,
                                                               true);
    } else if (cache_type_str == "SegmentCache") {
        freed_size = CacheManager::instance()->cache_prune_all(
                CachePolicy::CacheType::SEGMENT_CACHE, true);
    } else if (cache_type_str == "InvertedIndexSearcherCache") {
        freed_size = CacheManager::instance()->cache_prune_all(
                CachePolicy::CacheType::INVERTEDINDEX_SEARCHER_CACHE, true);
    } else if (cache_type_str == "InvertedIndexQueryCache") {
        freed_size = CacheManager::instance()->cache_prune_all(
                CachePolicy::CacheType::INVERTEDINDEX_QUERY_CACHE, true);
    } else if (cache_type_str == "PointQueryLookupConnectionCache") {
        freed_size = CacheManager::instance()->cache_prune_all(
                CachePolicy::CacheType::LOOKUP_CONNECTION_CACHE, true);
    } else if (cache_type_str == "PointQueryRowCache") {
        freed_size = CacheManager::instance()->cache_prune_all(
                CachePolicy::CacheType::POINT_QUERY_ROW_CACHE, true);
    } else if (cache_type_str == "MowDeleteBitmapAggCache") {
        freed_size = CacheManager::instance()->cache_prune_all(
                CachePolicy::CacheType::DELETE_BITMAP_AGG_CACHE, true);
    } else if (cache_type_str == "MowTabletVersionCache") {
        freed_size = CacheManager::instance()->cache_prune_all(
                CachePolicy::CacheType::TABLET_VERSION_CACHE, true);
    } else if (cache_type_str == "LoadStateChannelCache") {
        freed_size = CacheManager::instance()->cache_prune_all(
                CachePolicy::CacheType::LOAD_STATE_CHANNEL_CACHE, true);
    } else if (cache_type_str == "CommonObjLRUCache") {
        freed_size = CacheManager::instance()->cache_prune_all(
                CachePolicy::CacheType::COMMON_OBJ_LRU_CACHE, true);
    } else if (cache_type_str == "TabletSchemaCache") {
        freed_size = CacheManager::instance()->cache_prune_all(
                CachePolicy::CacheType::TABLET_SCHEMA_CACHE, true);
    } else if (cache_type_str == "CreateTabletRRIdxCache") {
        freed_size = CacheManager::instance()->cache_prune_all(
                CachePolicy::CacheType::CREATE_TABLET_RR_IDX_CACHE, true);
    } else if (cache_type_str == "CloudTabletCache") {
        freed_size = CacheManager::instance()->cache_prune_all(
                CachePolicy::CacheType::CLOUD_TABLET_CACHE, true);
    } else if (cache_type_str == "CloudTxnDeleteBitmapCache") {
        freed_size = CacheManager::instance()->cache_prune_all(
                CachePolicy::CacheType::CLOUD_TXN_DELETE_BITMAP_CACHE, true);
    } else if (cache_type_str == "QueryCache") {
        freed_size = CacheManager::instance()->cache_prune_all(CachePolicy::CacheType::QUERY_CACHE,
                                                               true);
    } else if (cache_type_str == "TabletColumnObjectPool") {
        freed_size = CacheManager::instance()->cache_prune_all(
                CachePolicy::CacheType::TABLET_COLUMN_OBJECT_POOL, true);
    } else if (cache_type_str == "SchemaCloudDictionaryCache") {
        freed_size = CacheManager::instance()->cache_prune_all(
                CachePolicy::CacheType::SCHEMA_CLOUD_DICTIONARY_CACHE, true);
    } else {
        CachePolicy::CacheType cache_type = CachePolicy::string_to_type(cache_type_str);
        if (cache_type == CachePolicy::CacheType::NONE) {
            fmt::format_to(return_string_buffer,
                           "ClearCacheAction not match type:{} of cache policy", cache_type_str);
            LOG(WARNING) << fmt::to_string(return_string_buffer);
            HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR,
                                    fmt::to_string(return_string_buffer));
            return;
        }
        freed_size = CacheManager::instance()->cache_prune_all(cache_type, true);
        if (freed_size == -1) {
            fmt::format_to(return_string_buffer,
                           "ClearCacheAction cache:{} is not allowed to be pruned", cache_type_str);
            LOG(WARNING) << fmt::to_string(return_string_buffer);
            HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR,
                                    fmt::to_string(return_string_buffer));
            return;
        }
    }
    fmt::format_to(return_string_buffer, "ClearCacheAction cache:{} prune win, freed size {}",
                   cache_type_str, freed_size);
    LOG(WARNING) << fmt::to_string(return_string_buffer);
    HttpChannel::send_reply(req, HttpStatus::OK, fmt::to_string(return_string_buffer));
}

} // end namespace doris
