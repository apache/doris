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

#include "common/factory_creator.h"
#include "storage/index/inverted/inverted_index_reader.h"

namespace doris::segment_v2 {

// Doris-side adapter for V4 SPIMI segments. Inherits the
// production `FullTextIndexReader` and overrides ONLY `type()`.
// The base class's `query()` calls `handle_searcher_cache()` which
// calls `create_index_searcher_builder(this->type())` — virtual
// dispatch on `this` resolves to `SPIMI_FULLTEXT`, routing the
// searcher build to `SpimiSearcherBuilder`. Everything else —
// analyzer context, phrase parser, query cache, scoring,
// `match_index_search` — runs unchanged through the base class.
//
// Why inheritance, not delegation: an earlier attempt constructed
// a stack-local `FullTextIndexReader proxy` and forwarded
// `query()` to it. Virtual `type()` then resolved on the proxy
// (returning `FULLTEXT`) instead of on the SPIMI adapter, so the
// searcher cache dispatch landed at `FulltextIndexSearcherBuilder`
// which tried to read CLucene compound files that don't exist for
// V4. The entire V4 read path was broken until this was fixed.
//
// `column_reader.cpp` instantiates this when the tablet's
// `inverted_index_storage_format == V4`.
class SpimiFulltextIndexReader : public FullTextIndexReader {
    ENABLE_FACTORY_CREATOR(SpimiFulltextIndexReader);

public:
    SpimiFulltextIndexReader(const TabletIndex* index_meta,
                             const std::shared_ptr<IndexFileReader>& index_file_reader)
            : FullTextIndexReader(index_meta, index_file_reader) {}

    ~SpimiFulltextIndexReader() override = default;

    // Registers this reader under the SPIMI_FULLTEXT iterator slot.
    // Distinct from the base class's `new_iterator` so that
    // downstream `InvertedIndexIterator::query()` dispatch keeps the
    // SPIMI/CLucene paths apart in profiling.
    Status new_iterator(std::unique_ptr<IndexIterator>* iterator) override;

    // Type override is THE behavioural difference vs the base. The
    // base class's `query()` reaches `create_index_searcher_builder
    // (type())` via virtual dispatch on `this`, so this single
    // override redirects the entire search-time path to
    // `SpimiSearcherBuilder` without any further code duplication.
    InvertedIndexReaderType type() override { return InvertedIndexReaderType::SPIMI_FULLTEXT; }

    // Override the inherited `try_query` so the error message
    // identifies V4/SPIMI to the user. Without this, the message
    // says "FullTextIndexReader not support try_query" which
    // misattributes the operation to the CLucene path in profiling
    // and user-facing diagnostics. Signature mirrors the base
    // class — note `const Field&` (master refactor changed from
    // `const void*` to a typed reference).
    Status try_query(const IndexQueryContextPtr& /*context*/, const std::string& /*column_name*/,
                     const Field& /*query_value*/, InvertedIndexQueryType /*query_type*/,
                     size_t* /*count*/) override {
        return Status::Error<ErrorCode::NOT_IMPLEMENTED_ERROR>(
                "SpimiFulltextIndexReader (V4) does not support try_query");
    }
};

} // namespace doris::segment_v2
