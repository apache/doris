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

#include <roaring/roaring.hh>

#include "common/status.h"
#include "vector/metric.h"

namespace lucene::store {
class Directory;
}

namespace doris::vectorized {
struct IndexSearchParameters;
struct IndexSearchResult;
} // namespace doris::vectorized
namespace doris::segment_v2 {

class VectorIndex {
public:
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
                                          const vectorized::IndexSearchParameters& params,
                                          vectorized::IndexSearchResult& result) = 0;
    /**
    * Search for the nearest neighbors of a query vector within a given radius.
    * @param query_vec  input vector, size d
    * @param radius  search radius
    * @param result  output search result
    * @return       status of the operation
    */
    virtual doris::Status range_search(const float* query_vec, const float& radius,
                                       const vectorized::IndexSearchParameters& params,
                                       vectorized::IndexSearchResult& result) = 0;

    virtual doris::Status save(lucene::store::Directory*) = 0;

    virtual doris::Status load(lucene::store::Directory*) = 0;

    size_t get_dimension() const { return _dimension; }

protected:
    // When adding vectors to the index, use this variable to check the dimension of the vectors.
    size_t _dimension = 0;
    Metric _metric = Metric::L2; // Default metric is L2 distance
};

} // namespace doris::segment_v2