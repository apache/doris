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

#include "storage/index/snii/snii_bkd_index_reader.h"

#include "common/check.h"
#include "runtime/runtime_state.h"
#include "storage/index/bkd_field_encoding.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/inverted/inverted_index_iterator.h"
#include "storage/index/snii/format/null_bitmap.h"
#include "storage/index/snii/snii_bkd_query.h"
#include "storage/index/snii/snii_doris_adapter.h"
#include "storage/key_coder.h"
#include "util/time.h"

namespace doris::segment_v2 {

namespace {
::doris::snii::Slice slice_of(const std::string& bytes) {
    return ::doris::snii::Slice(reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size());
}
} // namespace

Status SniiBkdIndexReader::new_iterator(std::unique_ptr<IndexIterator>* iterator) {
    if (*iterator == nullptr) {
        *iterator = InvertedIndexIterator::create_unique();
    }
    dynamic_cast<InvertedIndexIterator*>(iterator->get())
            ->add_reader(InvertedIndexReaderType::BKD,
                         std::dynamic_pointer_cast<InvertedIndexReader>(shared_from_this()));
    return Status::OK();
}

Status SniiBkdIndexReader::_get_searcher(const IndexQueryContextPtr& context,
                                         InvertedIndexCacheHandle* cache_handle,
                                         std::unique_ptr<::doris::snii::bkd::BkdSearcher>* uncached,
                                         const ::doris::snii::bkd::BkdSearcher** searcher) {
    DORIS_CHECK(cache_handle != nullptr);
    DORIS_CHECK(uncached != nullptr);
    DORIS_CHECK(searcher != nullptr);

    const bool enable_searcher_cache =
            context->runtime_state != nullptr &&
            context->runtime_state->query_options().enable_inverted_index_searcher_cache;
    const auto index_file_key = _index_file_reader->get_index_file_cache_key(&_index_meta);
    InvertedIndexSearcherCache::CacheKey searcher_cache_key(index_file_key);

    if (enable_searcher_cache) {
        SCOPED_RAW_TIMER(&context->stats->inverted_index_lookup_timer);
        if (InvertedIndexSearcherCache::instance()->lookup(searcher_cache_key, cache_handle)) {
            context->stats->inverted_index_searcher_cache_hit++;
            *searcher = cache_handle->get_snii_bkd_searcher();
            if (*searcher == nullptr) {
                return Status::InternalError("SNII searcher cache entry has no BKD searcher");
            }
            return Status::OK();
        }
    }

    SCOPED_RAW_TIMER(&context->stats->inverted_index_searcher_open_timer);
    context->stats->inverted_index_searcher_cache_miss++;
    RETURN_IF_ERROR(
            _index_file_reader->init(config::inverted_index_read_buffer_size, context->io_ctx));
    auto opened = DORIS_TRY(_index_file_reader->open_snii_bkd_index(&_index_meta, context->io_ctx));

    if (!enable_searcher_cache) {
        *searcher = opened.get();
        *uncached = std::move(opened);
        return Status::OK();
    }

    const size_t reader_size = std::max<size_t>(opened->memory_usage(), 1);
    auto* cache_value = new InvertedIndexSearcherCache::CacheValue(
            std::move(opened), reader_size, UnixMillis(), _index_file_reader);
    InvertedIndexSearcherCache::instance()->insert(searcher_cache_key, cache_value, cache_handle);
    *searcher = cache_handle->get_snii_bkd_searcher();
    if (*searcher == nullptr) {
        return Status::InternalError("SNII searcher cache insert produced empty BKD searcher");
    }
    return Status::OK();
}

Status SniiBkdIndexReader::_encode_query_value(const ::doris::snii::bkd::BkdSearcher& searcher,
                                               const Field& query_value, std::string* out) {
    // The type comes out of the index HEADER, so the coder that reads is the one
    // that wrote. Taking it from the query's own type instead would compare
    // correctly-encoded bytes in the wrong order (INV-1).
    const FieldType type = searcher.reader->header().field_type;
    if (!is_scalar_type(type)) {
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                "unsupported bkd index type {}", static_cast<int>(type));
    }
    return encode_bkd_field_ascending(type, query_value, get_key_coder(type), out);
}

Status SniiBkdIndexReader::query(const IndexQueryContextPtr& context,
                                 const std::string& column_name, const Field& query_value,
                                 InvertedIndexQueryType query_type,
                                 std::shared_ptr<roaring::Roaring>& bit_map,
                                 const InvertedIndexAnalyzerCtx* /*analyzer_ctx*/) {
    SCOPED_RAW_TIMER(&context->stats->inverted_index_query_timer);
    snii_doris::DorisSniiFileReader::ScopedIOContext io_context_scope(context->io_ctx);

    InvertedIndexCacheHandle searcher_cache_handle;
    std::unique_ptr<::doris::snii::bkd::BkdSearcher> uncached;
    const ::doris::snii::bkd::BkdSearcher* searcher = nullptr;
    RETURN_IF_ERROR(_get_searcher(context, &searcher_cache_handle, &uncached, &searcher));

    std::string query_str;
    RETURN_IF_ERROR(_encode_query_value(*searcher, query_value, &query_str));

    // The interval is resolved BEFORE the query cache is consulted: an
    // unsupported shape must be refused, not answered from a cache entry some
    // other predicate left behind under the same key.
    BkdQueryBounds bounds;
    RETURN_IF_ERROR(build_bkd_query_bounds(query_type, slice_of(query_str), &bounds));

    auto index_file_key = _index_file_reader->get_index_file_cache_key(&_index_meta);
    InvertedIndexQueryCache::CacheKey cache_key {index_file_key, column_name, query_type,
                                                 query_str};
    auto* cache = InvertedIndexQueryCache::instance();
    InvertedIndexQueryCacheHandle cache_handler;
    if (handle_query_cache(context, cache, cache_key, &cache_handler, bit_map)) {
        return Status::OK();
    }

    RETURN_IF_ERROR(searcher->reader->range(bounds.lower, bounds.lower_inclusive, bounds.upper,
                                            bounds.upper_inclusive, bit_map.get()));
    bit_map->runOptimize();
    // insert_query_cache, not cache->insert: it is the one that honours
    // enable_inverted_index_query_cache, so a query that opted out of the cache
    // does not populate it anyway.
    insert_query_cache(context, cache, cache_key, bit_map, &cache_handler);
    return Status::OK();
}

Status SniiBkdIndexReader::try_query(const IndexQueryContextPtr& context,
                                     const std::string& /*column_name*/, const Field& query_value,
                                     InvertedIndexQueryType query_type, size_t* count) {
    DORIS_CHECK(count != nullptr);
    snii_doris::DorisSniiFileReader::ScopedIOContext io_context_scope(context->io_ctx);

    InvertedIndexCacheHandle searcher_cache_handle;
    std::unique_ptr<::doris::snii::bkd::BkdSearcher> uncached;
    const ::doris::snii::bkd::BkdSearcher* searcher = nullptr;
    RETURN_IF_ERROR(_get_searcher(context, &searcher_cache_handle, &uncached, &searcher));

    std::string query_str;
    RETURN_IF_ERROR(_encode_query_value(*searcher, query_value, &query_str));
    BkdQueryBounds bounds;
    RETURN_IF_ERROR(build_bkd_query_bounds(query_type, slice_of(query_str), &bounds));

    uint64_t estimate = 0;
    RETURN_IF_ERROR(searcher->reader->estimate_cardinality(
            bounds.lower, bounds.lower_inclusive, bounds.upper, bounds.upper_inclusive, &estimate));
    *count = estimate;
    return Status::OK();
}

Status SniiBkdIndexReader::read_null_bitmap(const IndexQueryContextPtr& context,
                                            InvertedIndexQueryCacheHandle* cache_handle,
                                            lucene::store::Directory* /*dir*/) {
    SCOPED_RAW_TIMER(&context->stats->inverted_index_query_null_bitmap_timer);
    auto index_file_key = _index_file_reader->get_index_file_cache_key(&_index_meta);
    InvertedIndexQueryCache::CacheKey cache_key {
            index_file_key, "", InvertedIndexQueryType::UNKNOWN_QUERY, "null_bitmap"};
    auto* cache = InvertedIndexQueryCache::instance();
    if (cache->lookup(cache_key, cache_handle)) {
        return Status::OK();
    }

    snii_doris::DorisSniiFileReader::ScopedIOContext io_context_scope(context->io_ctx);
    InvertedIndexCacheHandle searcher_cache_handle;
    std::unique_ptr<::doris::snii::bkd::BkdSearcher> uncached;
    const ::doris::snii::bkd::BkdSearcher* searcher = nullptr;
    RETURN_IF_ERROR(_get_searcher(context, &searcher_cache_handle, &uncached, &searcher));

    auto null_bitmap = std::make_shared<roaring::Roaring>();
    if (searcher->null_bitmap_length > 0) {
        std::vector<uint8_t> bytes;
        // Through the SEARCHER's own file, mirroring the text reader
        // (snii_index_reader.cpp uses logical_reader->reader()->read_at). Going
        // via _index_file_reader reads through THIS object's member, which on a
        // searcher-cache hit may belong to a different, never-init()-ed
        // IndexFileReader -- the Segment cache and the searcher cache evict on
        // different axes, so a surviving searcher paired with a reloaded segment
        // is ordinary under memory pressure, and it produced
        // INVERTED_INDEX_FILE_NOT_FOUND for a file that is open right there.
        RETURN_IF_ERROR(searcher->reader->reader()->read_at(searcher->null_bitmap_offset,
                                                            searcher->null_bitmap_length, &bytes));
        ::doris::snii::format::NullBitmapReader reader;
        RETURN_IF_ERROR(::doris::snii::format::NullBitmapReader::open(::doris::snii::Slice(bytes),
                                                                      &reader));
        reader.copy_to(null_bitmap.get());
        null_bitmap->runOptimize();
    }
    cache->insert(cache_key, null_bitmap, cache_handle);
    return Status::OK();
}

} // namespace doris::segment_v2
