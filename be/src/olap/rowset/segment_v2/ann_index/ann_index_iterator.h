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

#include "olap/rowset/segment_v2/ann_index/ann_index_reader.h"
#include "olap/rowset/segment_v2/index_iterator.h"
#include "runtime/runtime_state.h"

namespace doris::segment_v2 {
struct AnnRangeSearchParams;
struct AnnRangeSearchResult;
#include "common/compile_check_begin.h"
class AnnIndexIterator : public IndexIterator {
public:
    AnnIndexIterator(const IndexReaderPtr& reader);
    ~AnnIndexIterator() override = default;

    IndexReaderPtr get_reader(IndexReaderType reader_type) const override {
        return std::static_pointer_cast<IndexReader>(_ann_reader);
    }
    MOCK_FUNCTION Status read_from_index(const IndexParam& param) override;

    Status read_null_bitmap(InvertedIndexQueryCacheHandle* cache_handle) override {
        return Status::OK();
    }

    Result<bool> has_null() override { return true; }

    MOCK_FUNCTION Status range_search(const AnnRangeSearchParams& params,
                                      const VectorSearchUserParams& custom_params,
                                      AnnRangeSearchResult* result, AnnIndexStats* stats);

private:
    std::shared_ptr<AnnIndexReader> _ann_reader;

    ENABLE_FACTORY_CREATOR(AnnIndexIterator);
};
#include "common/compile_check_end.h"
} // namespace doris::segment_v2