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

#include "olap/rowset/segment_v2/index_iterator.h"
#include "olap/rowset/segment_v2/inverted_index_reader.h"

namespace doris::segment_v2 {

struct InvertedIndexParam {
    std::string column_name;
    vectorized::DataTypePtr column_type;
    const void* query_value;
    InvertedIndexQueryType query_type;
    uint32_t num_rows;
    std::shared_ptr<roaring::Roaring> roaring;
    bool skip_try = false;
};

class InvertedIndexIterator : public IndexIterator {
public:
    InvertedIndexIterator();
    ~InvertedIndexIterator() override = default;

    void add_reader(InvertedIndexReaderType type, const InvertedIndexReaderPtr& reader);

    Status read_from_index(const IndexParam& param) override;

    Status read_null_bitmap(InvertedIndexQueryCacheHandle* cache_handle) override;

    [[nodiscard]] Result<bool> has_null() override;

    IndexReaderPtr get_reader(IndexReaderType reader_type) const override;

private:
    ENABLE_FACTORY_CREATOR(InvertedIndexIterator);

    Status try_read_from_inverted_index(const InvertedIndexReaderPtr& reader,
                                        const std::string& column_name, const void* query_value,
                                        InvertedIndexQueryType query_type, size_t* count);
    Result<InvertedIndexReaderPtr> _select_best_reader(const vectorized::DataTypePtr& column_type,
                                                       InvertedIndexQueryType query_type);
    Result<InvertedIndexReaderPtr> _select_best_reader();

    std::unordered_map<IndexReaderType, InvertedIndexReaderPtr> _readers;
};

} // namespace doris::segment_v2