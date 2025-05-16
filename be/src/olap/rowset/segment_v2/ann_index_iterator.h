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

#include "gutil/integral_types.h"
#include "olap/rowset/segment_v2/ann_index_reader.h"
#include "olap/rowset/segment_v2/index_iterator.h"

namespace doris::segment_v2 {

struct AnnIndexParam {
    const float* query_value;
    const size_t query_value_size;
    size_t limit;
    roaring::Roaring* roaring;
    std::unique_ptr<std::vector<float>> distance = nullptr;
    std::unique_ptr<std::vector<uint64_t>> row_ids = nullptr;
};

struct RangeSearchParams {
    bool is_le_or_lt = true;
    float* query_value = nullptr;
    float radius = -1;
    roaring::Roaring* roaring; // roaring from segment_iterator
    std::string to_string() const {
        DCHECK(roaring != nullptr);
        return fmt::format("is_le_or_lt: {}, radius: {}, input rows {}", is_le_or_lt, radius,
                           roaring->cardinality());
    }
    virtual ~RangeSearchParams() = default;
};

struct CustomSearchParams {
    int ef_search = 16;
};

struct RangeSearchResult {
    std::shared_ptr<roaring::Roaring> roaring;
    std::unique_ptr<std::vector<uint64_t>> row_ids;
    std::unique_ptr<float[]> distance;
};

// IndexIterator 与 IndexReader 的角色似乎有点重复，未来可以重构后删除一层概念
class AnnIndexIterator : public IndexIterator {
public:
    AnnIndexIterator(const io::IOContext& io_ctx, OlapReaderStatistics* stats,
                     RuntimeState* runtime_state, const IndexReaderPtr& reader);
    ~AnnIndexIterator() override = default;

    IndexType type() override { return IndexType::ANN; }

    IndexReaderPtr get_reader() override {
        return std::static_pointer_cast<IndexReader>(_ann_reader);
    }

    MOCK_FUNCTION Status read_from_index(const IndexParam& param) override;

    Status read_null_bitmap(InvertedIndexQueryCacheHandle* cache_handle) override {
        return Status::OK();
    }

    bool has_null() override { return true; }

    MOCK_FUNCTION Status range_search(const RangeSearchParams& params,
                                      const CustomSearchParams& custom_params,
                                      RangeSearchResult* result);

private:
    std::shared_ptr<AnnIndexReader> _ann_reader;

    ENABLE_FACTORY_CREATOR(AnnIndexIterator);
};

} // namespace doris::segment_v2