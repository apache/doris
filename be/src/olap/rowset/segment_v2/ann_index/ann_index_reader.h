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

#include "olap/rowset/segment_v2/ann_index/ann_index.h"
#include "olap/rowset/segment_v2/ann_index/ann_search_params.h"
#include "olap/rowset/segment_v2/index_reader.h"
#include "olap/tablet_schema.h"
#include "runtime/runtime_state.h"
#include "util/once.h"
namespace doris::segment_v2 {
#include "common/compile_check_begin.h"

struct AnnTopNParam;
struct AnnRangeSearchParams;
struct AnnRangeSearchResult;
struct IndexSearchResult;

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

    Status query(io::IOContext* io_ctx, AnnTopNParam* param, AnnIndexStats* stats);

    Status range_search(const AnnRangeSearchParams& params,
                        const VectorSearchUserParams& custom_params, AnnRangeSearchResult* result,
                        AnnIndexStats* stats, io::IOContext* io_ctx = nullptr);

    IndexType index_type() override { return IndexType::ANN; }

    uint64_t get_index_id() const override { return _index_meta.index_id(); }

    Status new_iterator(std::unique_ptr<IndexIterator>* iterator) override;

    AnnIndexMetric get_metric_type() const { return _metric_type; }

private:
    TabletIndex _index_meta;
    std::shared_ptr<IndexFileReader> _index_file_reader;
    std::unique_ptr<VectorIndex> _vector_index;
    // TODO: Use integer.
    std::string _index_type;
    AnnIndexMetric _metric_type;

    DorisCallOnce<Status> _load_index_once;
};

using AnnIndexReaderPtr = std::shared_ptr<AnnIndexReader>;
#include "common/compile_check_end.h"
} // namespace doris::segment_v2
