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

#include "storage/index/inverted/inverted_index_reader.h"

#include <roaring/roaring.hh>

#include "common/config.h"
#include "common/logging.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"
#include "storage/index/index_file_reader.h"

namespace doris::segment_v2 {

std::string InvertedIndexReader::get_index_file_path() {
    return _index_file_reader->get_index_file_path(&_index_meta);
}

Status InvertedIndexReader::query_with_null_bitmap(
        const IndexQueryContextPtr& context, const std::string& column_name,
        const Field& query_value, InvertedIndexQueryType query_type,
        std::shared_ptr<roaring::Roaring>& bit_map,
        InvertedIndexQueryCacheHandle* null_bitmap_cache_handle,
        const InvertedIndexAnalyzerCtx* analyzer_ctx) {
    DORIS_CHECK(null_bitmap_cache_handle != nullptr);
    RETURN_IF_ERROR(query(context, column_name, query_value, query_type, bit_map, analyzer_ctx));
    if (!has_null()) {
        return Status::OK();
    }
    return read_null_bitmap(context, null_bitmap_cache_handle);
}

bool InvertedIndexReader::handle_query_cache(const IndexQueryContextPtr& context,
                                             InvertedIndexQueryCache* cache,
                                             const InvertedIndexQueryCache::CacheKey& cache_key,
                                             InvertedIndexQueryCacheHandle* cache_handler,
                                             std::shared_ptr<roaring::Roaring>& bit_map,
                                             bool enabled) {
    const auto& query_options = context->runtime_state->query_options();
    if (!enabled || !query_options.enable_inverted_index_query_cache) {
        return false;
    }

    context->stats->inverted_index_query_cache_lookup++;
    SCOPED_RAW_TIMER(&context->stats->inverted_index_lookup_timer);
    const bool cache_hit = cache->lookup(cache_key, cache_handler);
    if (cache_hit) {
        DBUG_EXECUTE_IF("InvertedIndexReader.handle_query_cache_hit", {
            return Status::Error<ErrorCode::INTERNAL_ERROR>("handle query cache hit");
        });
        context->stats->inverted_index_query_cache_hit++;
        SCOPED_RAW_TIMER(&context->stats->inverted_index_query_bitmap_copy_timer);
        bit_map = cache_handler->get_bitmap();
        return true;
    }

    DBUG_EXECUTE_IF("InvertedIndexReader.handle_query_cache_miss", {
        return Status::Error<ErrorCode::INTERNAL_ERROR>("handle query cache miss");
    });
    context->stats->inverted_index_query_cache_miss++;
    return false;
}

void InvertedIndexReader::insert_query_cache(const IndexQueryContextPtr& context,
                                             InvertedIndexQueryCache* cache,
                                             const InvertedIndexQueryCache::CacheKey& cache_key,
                                             std::shared_ptr<roaring::Roaring> bit_map,
                                             InvertedIndexQueryCacheHandle* cache_handler,
                                             bool enabled) {
    if (!enabled || !context->runtime_state->query_options().enable_inverted_index_query_cache) {
        return;
    }
    cache->insert(cache_key, std::move(bit_map), cache_handler);
    context->stats->inverted_index_query_cache_insert++;
}

} // namespace doris::segment_v2
