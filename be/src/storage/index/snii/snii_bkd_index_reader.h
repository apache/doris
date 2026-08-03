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

#include <memory>
#include <string>

#include "common/status.h"
#include "storage/index/inverted/inverted_index_reader.h"
#include "storage/index/snii/snii_bkd_searcher.h"

namespace doris::segment_v2 {

// Doris read-path adapter for the SNII-native BKD (design 10 / task P3-2b): the
// numeric counterpart of SniiIndexReader, and the drop-in replacement for the
// CLucene-backed BkdIndexReader on SNII segments.
//
// It reports type() == BKD on purpose. The predicate layer routes on exactly
// that (comparison_predicate.h and in_list_predicate.h both refuse to push a
// numeric comparison down unless the iterator has a BKD reader), so a distinct
// reader type would silently disable index acceleration for every numeric
// column in the format rather than fail loudly.
//
// Nothing here catches a CLuceneError, because nothing under it can throw one:
// this reader reaches the SNII-native core and no third-party index library.
class SniiBkdIndexReader final : public InvertedIndexReader {
    ENABLE_FACTORY_CREATOR(SniiBkdIndexReader);

public:
    SniiBkdIndexReader(const TabletIndex* index_meta,
                       const std::shared_ptr<IndexFileReader>& index_file_reader)
            : InvertedIndexReader(index_meta, index_file_reader) {}
    ~SniiBkdIndexReader() override = default;

    Status new_iterator(std::unique_ptr<IndexIterator>* iterator) override;
    Status query(const IndexQueryContextPtr& context, const std::string& column_name,
                 const Field& query_value, InvertedIndexQueryType query_type,
                 std::shared_ptr<roaring::Roaring>& bit_map,
                 const InvertedIndexAnalyzerCtx* analyzer_ctx = nullptr) override;
    Status try_query(const IndexQueryContextPtr& context, const std::string& column_name,
                     const Field& query_value, InvertedIndexQueryType query_type,
                     size_t* count) override;
    Status read_null_bitmap(const IndexQueryContextPtr& context,
                            InvertedIndexQueryCacheHandle* cache_handle,
                            lucene::store::Directory* dir = nullptr) override;
    InvertedIndexReaderType type() override { return InvertedIndexReaderType::BKD; }

private:
    // Resolves the opened index, from the searcher cache when the query allows
    // it. `uncached` owns the reader when caching is off, `cache_handle` owns it
    // otherwise; either way *searcher points at whichever is alive.
    Status _get_searcher(const IndexQueryContextPtr& context,
                         InvertedIndexCacheHandle* cache_handle,
                         std::unique_ptr<::doris::snii::bkd::BkdSearcher>* uncached,
                         const ::doris::snii::bkd::BkdSearcher** searcher);

    // Encodes `query_value` with the key coder of the INDEX's OWN field type
    // (INV-1) -- the one read out of the header, never the query's own type.
    static Status _encode_query_value(const ::doris::snii::bkd::BkdSearcher& searcher,
                                      const Field& query_value, std::string* out);
};

} // namespace doris::segment_v2
