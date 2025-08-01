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
    const void* query_value;
    InvertedIndexQueryType query_type;
    uint32_t num_rows;
    std::shared_ptr<roaring::Roaring> roaring;
    bool skip_try = false;
};

class InvertedIndexIterator : public IndexIterator {
public:
    InvertedIndexIterator(const io::IOContext& io_ctx, OlapReaderStatistics* stats,
                          RuntimeState* runtime_state, const IndexReaderPtr& reader);
    ~InvertedIndexIterator() override = default;

    IndexType type() override { return IndexType::INVERTED; }
    IndexReaderPtr get_reader() override { return _index_reader; }

    Status read_from_index(const IndexParam& param) override;
    Status read_null_bitmap(InvertedIndexQueryCacheHandle* cache_handle) override;
    bool has_null() override;

private:
    Status try_read_from_inverted_index(const std::string& column_name, const void* query_value,
                                        InvertedIndexQueryType query_type, size_t* count);

    InvertedIndexReaderPtr _index_reader;

    ENABLE_FACTORY_CREATOR(InvertedIndexIterator);

    friend class InvertedIndexReaderTest;
};

} // namespace doris::segment_v2