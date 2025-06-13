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
#include "vec/functions/array/function_array_distance.h"

namespace lucene::store {
class Directory;
}
namespace doris::segment_v2 {
/*
This struct is used to wrap the search result of a vector index.
roaring is a bitmap that contains the row ids that satisfy the search condition.
row_ids is a vector of row ids that are returned by the search, it could be used by virtual_column_iterator to do column filter.
distances is a vector of distances that are returned by the search.
For range search, is condition is not le_or_lt, the row_ids and distances will be nullptr.
*/
struct IndexSearchResult {
    IndexSearchResult() = default;

    std::unique_ptr<float[]> distances = nullptr;
    std::unique_ptr<std::vector<uint64_t>> row_ids = nullptr;
    std::shared_ptr<roaring::Roaring> roaring = nullptr;
};

struct IndexSearchParameters {
    roaring::Roaring* roaring = nullptr;
    bool is_le_or_lt = true;
    virtual ~IndexSearchParameters() = default;
};

struct HNSWSearchParameters : public IndexSearchParameters {
    int ef_search = 16;
    bool check_relative_distance = true;
    bool bounded_queue = true;
};

class VectorIndex {
public:
    enum class Metric { L2, INNER_PRODUCT, UNKNOWN };

    static std::string metric_to_string(Metric metric) {
        switch (metric) {
        case Metric::L2:
            return vectorized::L2Distance::name;
        case Metric::INNER_PRODUCT:
            return vectorized::InnerProduct::name;
        default:
            return "UNKNOWN";
        }
    }
    static Metric string_to_metric(const std::string& metric) {
        if (metric == vectorized::L2Distance::name) {
            return Metric::L2;
        } else if (metric == vectorized::InnerProduct::name) {
            return Metric::INNER_PRODUCT;
        } else {
            return Metric::UNKNOWN;
        }
    }

    virtual ~VectorIndex() = default;

    /** Add n vectors of dimension d vectors to the index.
     *
     * Vectors are implicitly assigned labels ntotal .. ntotal + n - 1
     * This function slices the input vectors in chunks smaller than
     * blocksize_add and calls add_core.
     * @param n      number of vectors
     * @param x      input matrix, size n * d
     */
    virtual doris::Status add(int n, const float* x) = 0;

    /** Return approximate nearest neighbors of a query vector.
     * The result is stored in the result object.
     * @param query_vec  input vector, size d
     * @param k          number of nearest neighbors to return
     * @param params     search parameters
     * @param result     output search result
     * @return          status of the operation
    */
    virtual doris::Status ann_topn_search(const float* query_vec, int k,
                                          const IndexSearchParameters& params,
                                          IndexSearchResult& result) = 0;
    /**
    * Search for the nearest neighbors of a query vector within a given radius.
    * @param query_vec  input vector, size d
    * @param radius  search radius
    * @param result  output search result
    * @return       status of the operation
    */
    virtual doris::Status range_search(const float* query_vec, const float& radius,
                                       const IndexSearchParameters& params,
                                       IndexSearchResult& result) = 0;

    virtual doris::Status save(lucene::store::Directory*) = 0;

    virtual doris::Status load(lucene::store::Directory*) = 0;

    size_t get_dimension() const { return _dimension; }

protected:
    // When adding vectors to the index, use this variable to check the dimension of the vectors.
    size_t _dimension = 0;
};

} // namespace doris::segment_v2