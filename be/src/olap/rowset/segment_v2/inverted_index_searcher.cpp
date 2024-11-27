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

#include "olap/rowset/segment_v2/inverted_index_searcher.h"

#include <CLucene/search/IndexSearcher.h>
#include <CLucene/util/bkd/bkd_reader.h>

#include "common/config.h"
#include "olap/rowset/segment_v2/inverted_index_compound_reader.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/rowset/segment_v2/inverted_index_fs_directory.h"

namespace doris::segment_v2 {
Status FulltextIndexSearcherBuilder::build(lucene::store::Directory* directory,
                                           OptionalIndexSearcherPtr& output_searcher) {
    auto close_directory = true;
    lucene::index::IndexReader* reader = nullptr;
    try {
        reader = lucene::index::IndexReader::open(
                directory, config::inverted_index_read_buffer_size, close_directory);
    } catch (const CLuceneError& e) {
        return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                "FulltextIndexSearcherBuilder build error: {}", e.what());
    }
    bool close_reader = true;
    auto index_searcher = std::make_shared<lucene::search::IndexSearcher>(reader, close_reader);
    if (!index_searcher) {
        output_searcher = std::nullopt;
        return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                "FulltextIndexSearcherBuilder build index_searcher error.");
    }
    reader_size = reader->getTermInfosRAMUsed();
    // NOTE: need to cl_refcount-- here, so that directory will be deleted when
    // index_searcher is destroyed
    _CLDECDELETE(directory)
    output_searcher = index_searcher;
    return Status::OK();
}

Status BKDIndexSearcherBuilder::build(lucene::store::Directory* directory,
                                      OptionalIndexSearcherPtr& output_searcher) {
    try {
        auto close_directory = true;
        auto bkd_reader =
                std::make_shared<lucene::util::bkd::bkd_reader>(directory, close_directory);
        if (!bkd_reader->open()) {
            LOG(INFO) << "bkd index file " << directory->toString() << " is empty";
        }
        output_searcher = bkd_reader;
        _CLDECDELETE(directory)
        return Status::OK();
    } catch (const CLuceneError& e) {
        return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                "BKDIndexSearcherBuilder build error: {}", e.what());
    }
}

Result<std::unique_ptr<IndexSearcherBuilder>> IndexSearcherBuilder::create_index_searcher_builder(
        InvertedIndexReaderType reader_type) {
    std::unique_ptr<IndexSearcherBuilder> index_builder;
    switch (reader_type) {
    case InvertedIndexReaderType::STRING_TYPE:
    case InvertedIndexReaderType::FULLTEXT: {
        index_builder = std::make_unique<FulltextIndexSearcherBuilder>();
        break;
    }
    case InvertedIndexReaderType::BKD: {
        index_builder = std::make_unique<BKDIndexSearcherBuilder>();
        break;
    }

    default:
        LOG(ERROR) << "InvertedIndexReaderType:" << reader_type_to_string(reader_type)
                   << " is not support for InvertedIndexSearcherCache";
        return ResultError(Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                "InvertedIndexSearcherCache do not support reader type."));
    }

    return index_builder;
}

Result<IndexSearcherPtr> IndexSearcherBuilder::get_index_searcher(
        lucene::store::Directory* directory) {
    OptionalIndexSearcherPtr result;
    auto st = build(directory, result);
    if (!st.ok()) {
        _CLDECDELETE(directory)
        return ResultError(st);
    }
    if (!result.has_value()) {
        _CLDECDELETE(directory)
        return ResultError(Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                "InvertedIndexSearcherCache build error."));
    }
    return *result;
}
} // namespace doris::segment_v2