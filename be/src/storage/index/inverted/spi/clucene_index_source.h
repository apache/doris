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

#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "common/status.h"
#include "storage/index/query/spi/index_source.h"

namespace lucene::index {
class IndexReader;
}

namespace doris::io {
struct IOContext;
}

namespace doris::segment_v2 {

// A CLucene reader as the engine's source for one field. A reader split by segment exposes
// its sub-readers as sources of their own.
class CluceneIndexSource final : public index_query::IndexSource {
public:
    // `owner` keeps `reader` alive: the reader itself, or the reader whose segment it is.
    CluceneIndexSource(std::shared_ptr<lucene::index::IndexReader> owner,
                       lucene::index::IndexReader* reader, std::wstring field,
                       const io::IOContext* io_ctx);

    uint32_t doc_count() const override;
    Status open_term(std::string_view term, bool positions, bool scoring,
                     std::unique_ptr<index_query::PostingsCursor>* out) override;
    Status expand_terms(index_query::TermPattern& pattern, int32_t max_expansions,
                        std::vector<std::string>* out) override;
    std::span<const float> norm_lengths() const override;
    bool is_live(uint32_t doc) const override;
    std::vector<index_query::IndexSegment> segments() const override;

    lucene::index::IndexReader* reader() const { return _reader; }
    const std::wstring& field() const { return _field; }

private:
    std::shared_ptr<lucene::index::IndexReader> _owner;
    lucene::index::IndexReader* _reader;
    std::wstring _field;
    const io::IOContext* _io_ctx;
    // The sub-reader sources, built on the first request.
    mutable std::optional<std::vector<index_query::IndexSegment>> _segments;
};

// A reader owned elsewhere, as the shared pointer the sources take.
inline std::shared_ptr<lucene::index::IndexReader> non_owning_reader(
        lucene::index::IndexReader* reader) {
    return {reader, [](lucene::index::IndexReader*) {}};
}

// The source for `field` over a reader owned elsewhere.
std::shared_ptr<CluceneIndexSource> clucene_index_source(
        std::shared_ptr<lucene::index::IndexReader> reader, std::wstring field,
        const io::IOContext* io_ctx);

} // namespace doris::segment_v2
