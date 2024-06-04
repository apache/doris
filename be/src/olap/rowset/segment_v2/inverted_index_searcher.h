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

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wshadow-field"
#endif
#include <CLucene.h> // IWYU pragma: keep
#ifdef __clang__
#pragma clang diagnostic pop
#endif

#include <memory>
#include <optional>
#include <variant>

#include "common/status.h"
#include "inverted_index_query_type.h"

namespace lucene {
namespace search {
class IndexSearcher;
} // namespace search

namespace util::bkd {
class bkd_reader;
}

} // namespace lucene

namespace doris::segment_v2 {
using FulltextIndexSearcherPtr = std::shared_ptr<lucene::search::IndexSearcher>;
using BKDIndexSearcherPtr = std::shared_ptr<lucene::util::bkd::bkd_reader>;
using IndexSearcherPtr = std::variant<FulltextIndexSearcherPtr, BKDIndexSearcherPtr>;
using OptionalIndexSearcherPtr = std::optional<IndexSearcherPtr>;

class InvertedIndexCacheHandle;
class DorisCompoundReader;

class IndexSearcherBuilder {
public:
    virtual Status build(lucene::store::Directory* directory,
                         OptionalIndexSearcherPtr& output_searcher) = 0;
    virtual ~IndexSearcherBuilder() = default;
    virtual Result<IndexSearcherPtr> get_index_searcher(lucene::store::Directory* directory);
    static Result<std::unique_ptr<IndexSearcherBuilder>> create_index_searcher_builder(
            InvertedIndexReaderType reader_type);
    int64_t get_reader_size() const { return reader_size; }

protected:
    int64_t reader_size = 0;
};

class FulltextIndexSearcherBuilder : public IndexSearcherBuilder {
public:
    Status build(lucene::store::Directory* directory,
                 OptionalIndexSearcherPtr& output_searcher) override;
};

class BKDIndexSearcherBuilder : public IndexSearcherBuilder {
public:
    Status build(lucene::store::Directory* directory,
                 OptionalIndexSearcherPtr& output_searcher) override;
};
} // namespace doris::segment_v2