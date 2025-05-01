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
#include <roaring/roaring.hh>

#include "common/status.h"

namespace lucene::store {
class Directory;
}

namespace doris::segment_v2 {
struct SearchResult {
    SearchResult() = default;

    float get_distance(int idx) {
        if (idx < 0 || idx >= static_cast<int>(row_ids->cardinality())) {
            throw std::out_of_range("Invalid distance index");
        }
        return distances[idx];
    }

    std::unique_ptr<float[]> distances;
    std::shared_ptr<roaring::Roaring> row_ids;
};

struct SearchParameters {
    roaring::Roaring* row_ids = nullptr;
};

class VectorIndex {
public:
    enum class Metric { L2, COSINE, INNER_PRODUCT, UNKNOWN };

    /** Add n vectors of dimension d to the index.
     *
     * Vectors are implicitly assigned labels ntotal .. ntotal + n - 1
     * This function slices the input vectors in chunks smaller than
     * blocksize_add and calls add_core.
     * @param n      number of vectors
     * @param x      input matrix, size n * d
     */
    virtual doris::Status add(int n, const float* x) = 0;

    virtual doris::Status search(const float* query_vec, int k, const SearchParameters& params,
                                 SearchResult& result) = 0;

    virtual doris::Status save(lucene::store::Directory*) = 0;

    virtual doris::Status load(lucene::store::Directory*) = 0;

    static std::string metric_to_string(Metric metric) {
        switch (metric) {
        case Metric::L2:
            return "L2";
        case Metric::COSINE:
            return "COSINE";
        case Metric::INNER_PRODUCT:
            return "INNER_PRODUCT";
        default:
            return "UNKNOWN";
        }
    }
    static Metric string_to_metric(const std::string& metric) {
        if (metric == "l2") {
            return Metric::L2;
        } else if (metric == "cosine") {
            return Metric::COSINE;
        } else if (metric == "inner_product") {
            return Metric::INNER_PRODUCT;
        } else {
            return Metric::UNKNOWN;
        }
    }
    virtual ~VectorIndex() = default;

    size_t get_dimension() const { return _dimension; }

protected:
    size_t _dimension = 0;
};

} // namespace doris::segment_v2