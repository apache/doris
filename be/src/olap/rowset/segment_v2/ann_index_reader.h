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

#include "olap/rowset/segment_v2/index_reader.h"
#include "olap/tablet_schema.h"
#include "runtime/runtime_state.h"
#include "vector/vector_index.h"

namespace doris::segment_v2 {

struct AnnIndexParam;
struct RangeSearchParams;
struct RangeSearchResult;

class IndexFileReader;
class IndexIterator;

class AnnIndexReader : public IndexReader {
public:
    AnnIndexReader(const TabletIndex* index_meta,
                   std::shared_ptr<IndexFileReader> index_file_reader);
    ~AnnIndexReader() override = default;

    static void update_result(const IndexSearchResult&, std::vector<float>& distance,
                              roaring::Roaring& row_id);

    Status load_index(io::IOContext* io_ctx);

    Status query(io::IOContext* io_ctx, AnnIndexParam* param);

    Status range_search(const RangeSearchParams& params,
                        const VectorSearchUserParams& custom_params, RangeSearchResult* result,
                        io::IOContext* io_ctx = nullptr);

    uint64_t get_index_id() const override { return _index_meta.index_id(); }

    Status new_iterator(const io::IOContext& io_ctx, OlapReaderStatistics* stats,
                        RuntimeState* runtime_state,
                        std::unique_ptr<IndexIterator>* iterator) override;
    VectorIndex::Metric get_metric_type() const { return _metric_type; }

private:
    TabletIndex _index_meta;
    std::shared_ptr<IndexFileReader> _index_file_reader;
    std::unique_ptr<VectorIndex> _vector_index;
    // TODO: Use integer.
    std::string _index_type;
    VectorIndex::Metric _metric_type;
};

using AnnIndexReaderPtr = std::shared_ptr<AnnIndexReader>;

} // namespace doris::segment_v2
