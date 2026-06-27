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
#include <vector>

#include "storage/index/inverted/inverted_index_query_type.h"
#include "storage/index/inverted/inverted_index_reader.h"

namespace doris::segment_v2 {

class SniiIndexReader final : public InvertedIndexReader {
    ENABLE_FACTORY_CREATOR(SniiIndexReader);

public:
    SniiIndexReader(const TabletIndex* index_meta,
                    const std::shared_ptr<IndexFileReader>& index_file_reader,
                    InvertedIndexReaderType reader_type)
            : InvertedIndexReader(index_meta, index_file_reader), _reader_type(reader_type) {}

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
    InvertedIndexReaderType type() override { return _reader_type; }

private:
    Status _parse_query_terms(const IndexQueryContextPtr& context, std::string search_str,
                              InvertedIndexQueryType query_type,
                              const InvertedIndexAnalyzerCtx* analyzer_ctx,
                              InvertedIndexQueryInfo* query_info);

    InvertedIndexReaderType _reader_type;
};

} // namespace doris::segment_v2
