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

#include "storage/index/snii/snii_index_reader.h"

#include <CLucene.h>
#include <CLucene/util/stringUtil.h>
#include <fmt/format.h>

#include <algorithm>
#include <atomic>
#include <memory>
#include <optional>
#include <roaring/roaring.hh>
#include <string>
#include <string_view>
#include <utility>

#include "common/config.h"
#include "common/exception.h"
#include "runtime/exec_env.h"
#include "runtime/query_context.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/index_reader_helper.h"
#include "storage/index/inverted/analyzer/analyzer.h"
#include "storage/index/inverted/inverted_index_cache.h"
#include "storage/index/inverted/inverted_index_iterator.h"
#include "storage/index/query/docid_sink.h"
#include "storage/index/query/logical/search_lowering.h"
#include "storage/index/query/roaring_docid_sink.h"
#include "storage/index/query/term_pattern.h"
#include "storage/index/snii/format/null_bitmap.h"
#include "storage/index/snii/query/count_query.h"
#include "storage/index/snii/query/internal/plain_term_routing.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_index_source.h"
#include "storage/index/snii/snii_doris_adapter.h"
#include "storage/index/snii/snii_prx_profile.h"
#include "util/time.h"

namespace doris::segment_v2 {

Status SniiIndexReader::_get_logical_reader(
        const IndexQueryContextPtr& context, InvertedIndexCacheHandle* searcher_cache_handle,
        std::unique_ptr<::doris::snii::reader::LogicalIndexReader>* uncached_reader,
        const ::doris::snii::reader::LogicalIndexReader** logical_reader) {
    DCHECK(searcher_cache_handle != nullptr);
    DCHECK(uncached_reader != nullptr);
    DCHECK(logical_reader != nullptr);

    const bool enable_searcher_cache =
            context->runtime_state != nullptr &&
            context->runtime_state->query_options().enable_inverted_index_searcher_cache;
    const auto index_file_key = _index_file_reader->get_index_file_cache_key(&_index_meta);
    InvertedIndexSearcherCache::CacheKey searcher_cache_key(index_file_key);

    bool cache_hit = false;
    if (enable_searcher_cache) {
        SCOPED_RAW_TIMER(&context->stats->inverted_index_lookup_timer);
        cache_hit = InvertedIndexSearcherCache::instance()->lookup(searcher_cache_key,
                                                                   searcher_cache_handle);
    }

    if (cache_hit) {
        context->stats->inverted_index_searcher_cache_hit++;
        *logical_reader = searcher_cache_handle->get_snii_logical_reader();
        if (*logical_reader == nullptr) {
            return Status::InternalError("SNII searcher cache entry has no logical reader");
        }
        return Status::OK();
    }

    SCOPED_RAW_TIMER(&context->stats->inverted_index_searcher_open_timer);
    context->stats->inverted_index_searcher_cache_miss++;
#ifdef BE_TEST
    if (_searcher_open_observer != nullptr) {
        _searcher_open_observer(_searcher_open_opaque);
    }
#endif
    RETURN_IF_ERROR(
            _index_file_reader->init(config::inverted_index_read_buffer_size, context->io_ctx));
    auto opened_reader =
            DORIS_TRY(_index_file_reader->open_snii_index(&_index_meta, context->io_ctx));

    if (!enable_searcher_cache) {
        *logical_reader = opened_reader.get();
        *uncached_reader = std::move(opened_reader);
        return Status::OK();
    }

    const size_t reader_size = std::max<size_t>(opened_reader->memory_usage(), 1);
    auto* cache_value = new InvertedIndexSearcherCache::CacheValue(
            std::move(opened_reader), reader_size, UnixMillis(), _index_file_reader);
    InvertedIndexSearcherCache::instance()->insert(searcher_cache_key, cache_value,
                                                   searcher_cache_handle);
    *logical_reader = searcher_cache_handle->get_snii_logical_reader();
    if (*logical_reader == nullptr) {
        return Status::InternalError("SNII searcher cache insert produced empty logical reader");
    }
    return Status::OK();
}

namespace {

struct SniiOpenedIndex : OpenedIndex {
    SniiOpenedIndex(const io::IOContext* io_ctx, OlapReaderStatistics* query_stats)
            : io_scope(io_ctx), stats(query_stats) {}
    // The PRX frames the cursors of the index's sources decoded join the query's statistics.
    ~SniiOpenedIndex() override { ::doris::snii::add_prx_decode_stats(stats, prx_decode_stats); }

    snii_doris::DorisSniiFileReader::ScopedIOContext io_scope;
    InvertedIndexCacheHandle searcher_cache_handle;
    std::unique_ptr<::doris::snii::reader::LogicalIndexReader> uncached_reader;
    const ::doris::snii::reader::LogicalIndexReader* reader = nullptr;
    OlapReaderStatistics* stats;
    ::doris::snii::format::PrxDecodeStats prx_decode_stats;
};

} // namespace

Status SniiIndexReader::_open_index(const IndexQueryContextPtr& context,
                                    std::unique_ptr<OpenedIndex>* out) {
    auto opened = std::make_unique<SniiOpenedIndex>(context->io_ctx, context->stats);
    RETURN_IF_ERROR(_get_logical_reader(context, &opened->searcher_cache_handle,
                                        &opened->uncached_reader, &opened->reader));
    *out = std::move(opened);
    return Status::OK();
}

index_query::IndexSourcePtr SniiIndexReader::_bind_source(const IndexQueryContextPtr& /*context*/,
                                                          const std::wstring& /*field*/,
                                                          OpenedIndex& index) {
    auto& opened = static_cast<SniiOpenedIndex&>(index);
    return std::make_shared<::doris::snii::reader::SniiIndexSource>(*opened.reader,
                                                                    &opened.prx_decode_stats);
}

Status SniiIndexReader::_term_document_frequency(const std::string& /*column_name*/,
                                                 OpenedIndex& index, const std::string& term,
                                                 uint64_t* df, uint64_t* document_count) {
    const auto& reader = *static_cast<SniiOpenedIndex&>(index).reader;
    RETURN_IF_ERROR(::doris::snii::query::internal::check_term_outside_internal_namespace(term));
    RETURN_IF_ERROR(::doris::snii::query::count_only_term_df(reader, term, df));
    // The image's document count and its count of documents holding terms both bound df.
    const auto& stats = reader.stats();
    *document_count = std::min(stats.doc_count, stats.indexed_doc_count);
    return Status::OK();
}

// The leaf runs on the shared engine over the index's source, keeping the codes of what it
// throws, so a bypass or a corrupted image still downgrades to rows.
Status SniiIndexReader::_run_leaf(const IndexQueryContextPtr& context,
                                  const std::string& column_name, OpenedIndex& index,
                                  const index_query::logical::Node& leaf,
                                  const roaring::Roaring* candidates, bool scoring,
                                  std::shared_ptr<roaring::Roaring>* out) {
    const std::wstring field = StringUtil::string_to_wstring(column_name);
    auto source = _bind_source(context, field, index);
    const uint32_t doc_count = source->doc_count();
    auto result = std::make_shared<roaring::Roaring>();
    RETURN_IF_ERROR_OR_CATCH_EXCEPTION(run_leaf(context, field, leaf, candidates, scoring,
                                                std::move(source), doc_count, result));
    result->runOptimize();
    *out = std::move(result);
    return Status::OK();
}

// Keep the complete count-only eligibility and null-safe fabrication contract in one linear path.
// NOLINTNEXTLINE(readability-function-size)
#ifdef BE_TEST
Status SniiIndexReader::_try_count_only_fastpath(
        const IndexQueryContextPtr& context, InvertedIndexQueryType /*query_type*/,
        const InvertedIndexQueryInfo& /*query_info*/, const std::vector<std::string>& terms,
        bool* handled, std::shared_ptr<roaring::Roaring>* out,
        const ::doris::snii::reader::LogicalIndexReader* preopened_reader) {
    *handled = false;
    if (terms.size() != 1) {
        return Status::OK();
    }
    std::unique_ptr<OpenedIndex> index;
    if (preopened_reader != nullptr) {
        auto opened = std::make_unique<SniiOpenedIndex>(context->io_ctx, context->stats);
        opened->reader = preopened_reader;
        index = std::move(opened);
    } else {
        RETURN_IF_ERROR(_open_index(context, &index));
    }
    return _count_from_df(context, "", *index, terms.front(), handled, out);
}
#endif

Status SniiIndexReader::_read_null_bitmap(const IndexQueryContextPtr& context,
                                          InvertedIndexQueryCacheHandle* cache_handle,
                                          OpenedIndex* index) {
    return _read_snii_null_bitmap(
            context, cache_handle,
            index == nullptr ? nullptr : static_cast<SniiOpenedIndex*>(index)->reader);
}

Status SniiIndexReader::read_null_bitmap(const IndexQueryContextPtr& context,
                                         InvertedIndexQueryCacheHandle* cache_handle,
                                         lucene::store::Directory* /*dir*/) {
    return _read_snii_null_bitmap(context, cache_handle, nullptr);
}

Status SniiIndexReader::_read_snii_null_bitmap(
        const IndexQueryContextPtr& context, InvertedIndexQueryCacheHandle* cache_handle,
        const ::doris::snii::reader::LogicalIndexReader* preopened_reader) {
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
    std::unique_ptr<::doris::snii::reader::LogicalIndexReader> uncached_reader;
    const ::doris::snii::reader::LogicalIndexReader* logical_reader = preopened_reader;
    if (logical_reader == nullptr) {
        RETURN_IF_ERROR(_get_logical_reader(context, &searcher_cache_handle, &uncached_reader,
                                            &logical_reader));
    }
    auto null_bitmap = std::make_shared<roaring::Roaring>();
    const auto& ref = logical_reader->section_refs().null_bitmap;
    if (ref.length > 0) {
        std::vector<uint8_t> bytes;
        RETURN_IF_ERROR(logical_reader->reader()->read_at(ref.offset, ref.length, &bytes));
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
