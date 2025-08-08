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

#include <CLucene.h>
#include <CLucene/store/IndexInput.h>
#include <CLucene/store/IndexOutput.h>
#include <faiss/Index.h>
#include <gen_cpp/olap_file.pb.h>

#include <string>

#include "common/status.h"
#include "vec/core/types.h"
#include "vector_index.h"

namespace doris::vectorized {
struct IndexSearchParameters;
struct IndexSearchResult;
} // namespace doris::vectorized

namespace doris::segment_v2 {
#include "common/compile_check_begin.h"
struct FaissBuildParameter {
    enum class IndexType { HNSW };

    enum class MetricType {
        L2, // Euclidean distance
        IP, // Inner product
    };

    static IndexType string_to_index_type(const std::string& type) {
        if (type == "hnsw") {
            return IndexType::HNSW;
        } else {
            throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT, "Unsupported index type: {}",
                                   type);
        }
    }

    static MetricType string_to_metric_type(const std::string& type) {
        if (type == "l2_distance") {
            return MetricType::L2;
        } else if (type == "inner_product") {
            return MetricType::IP;
        } else {
            throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT,
                                   "Unsupported metric type: {}", type);
        }
    }

    // HNSW
    int dim = 0;
    int max_degree = 0;
    IndexType index_type = IndexType::HNSW;
    MetricType metric_type = MetricType::L2;
};

class FaissVectorIndex : public VectorIndex {
public:
    static std::unique_ptr<faiss::IDSelector> roaring_to_faiss_selector(
            const roaring::Roaring& bitmap);

    static void update_roaring(const vectorized::Int64* labels, const size_t n,
                               roaring::Roaring& roaring);

    FaissVectorIndex();

    doris::Status add(int n, const float* vec) override;

    void set_build_params(const FaissBuildParameter& params);

    doris::Status ann_topn_search(const float* query_vec, int k,
                                  const vectorized::IndexSearchParameters& params,
                                  vectorized::IndexSearchResult& result) override;

    doris::Status range_search(const float* query_vec, const float& radius,
                               const vectorized::IndexSearchParameters& params,
                               vectorized::IndexSearchResult& result) override;

    doris::Status save(lucene::store::Directory*) override;

    doris::Status load(lucene::store::Directory*) override;

private:
    std::unique_ptr<faiss::Index> _index = nullptr;
};
#include "common/compile_check_end.h"
} // namespace doris::segment_v2