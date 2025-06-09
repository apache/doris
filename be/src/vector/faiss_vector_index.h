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

#include <memory>
#include <string>

#include "common/status.h"
#include "vector_index.h"

namespace doris::vectorized {
struct IndexSearchParameters;
struct IndexSearchResult;
} // namespace doris::vectorized

namespace doris::segment_v2 {
struct FaissBuildParameter {
    enum class IndexType { BruteForce, IVF, HNSW };

    enum class Quantilizer { FLAT, PQ };

    enum class MetricType {
        L2, // Euclidean distance
        IP, // Inner product
    };

    static IndexType string_to_index_type(const std::string& type) {
        if (type == "brute_force") {
            return IndexType::BruteForce;
        } else if (type == "ivf") {
            return IndexType::IVF;
        } else if (type == "hnsw") {
            return IndexType::HNSW;
        }
        return IndexType::HNSW; // default
    }

    static Quantilizer string_to_quantilizer(const std::string& type) {
        if (type == "flat") {
            return Quantilizer::FLAT;
        } else if (type == "pq") {
            return Quantilizer::PQ;
        }
        return Quantilizer::FLAT; // default
    }

    static MetricType string_to_metric_type(const std::string& type) {
        if (type == "l2") {
            return MetricType::L2;
        } else if (type == "ip") {
            return MetricType::IP;
        }

        return MetricType::L2; // default
    }

    // HNSW
    int d = 0;
    int m = 0;
    int pq_m = -1; // Only used for PQ quantilizer
    IndexType index_type;
    Quantilizer quantilizer;
    MetricType metric_type = MetricType::L2;
};

class FaissVectorIndex : public VectorIndex {
public:
    static std::unique_ptr<faiss::IDSelector> roaring_to_faiss_selector(
            const roaring::Roaring& bitmap);

    static void update_roaring(const faiss::idx_t* labels, const size_t n,
                               roaring::Roaring& roaring);

    FaissVectorIndex() = default;

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

} // namespace doris::segment_v2