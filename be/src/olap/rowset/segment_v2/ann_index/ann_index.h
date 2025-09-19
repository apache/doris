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

/**
 * @file ann_index.h
 * @brief Abstract interface for vector similarity search indexes in Doris.
 * 
 * This file defines the abstract VectorIndex interface that provides a unified
 * API for different vector index implementations (FAISS, etc.). The interface
 * supports both approximate k-nearest neighbor search and range search operations.
 * 
 * Key operations supported:
 * - Adding vectors to the index during build phase
 * - K-nearest neighbor search for Top-N queries
 * - Range search for finding vectors within a distance threshold
 * - Persistence to/from storage for index durability
 * 
 * This abstraction allows Doris to support multiple vector index libraries
 * through a consistent interface.
 */

#pragma once

#include <roaring/roaring.hh>

#include "common/status.h"
#include "vec/core/types.h"

namespace lucene::store {
class Directory;
}

#include "common/compile_check_begin.h"
namespace doris::segment_v2 {
struct IndexSearchParameters;
struct IndexSearchResult;

enum class AnnIndexMetric { L2, IP, UNKNOWN };

std::string metric_to_string(AnnIndexMetric metric);

AnnIndexMetric string_to_metric(const std::string& metric);

enum class AnnIndexType { UNKNOWN, HNSW };

std::string ann_index_type_to_string(AnnIndexType type);

AnnIndexType string_to_ann_index_type(const std::string& type);

/**
 * @brief Abstract base class for vector similarity search indexes.
 * 
 * This class defines the interface that all vector index implementations
 * must follow. It provides the core operations needed for vector similarity
 * search in Doris, including index building, searching, and persistence.
 * 
 * Implementations of this interface (like FaissVectorIndex) handle the
 * specifics of different vector index libraries while providing a consistent
 * API for the Doris query execution engine.
 */
class VectorIndex {
public:
    virtual ~VectorIndex() = default;

    virtual void train(vectorized::Int64 n, const float* x) = 0;

    /** Add n vectors of dimension d vectors to the index.
     *
     * Vectors are implicitly assigned labels ntotal .. ntotal + n - 1
     * This function slices the input vectors in chunks smaller than
     * blocksize_add and calls add_core.
     * @param n      number of vectors
     * @param x      input matrix, size n * d
     */
    virtual doris::Status add(vectorized::Int64 n, const float* x) = 0;

    /** Return approximate nearest neighbors of a query vector.
     * The result is stored in the result object.
     * @param query_vec  input vector, size d
     * @param k          number of nearest neighbors to return
     * @param params     search parameters
     * @param result     output search result
     * @return          status of the operation
    */
    virtual doris::Status ann_topn_search(const float* query_vec, int k,
                                          const segment_v2::IndexSearchParameters& params,
                                          segment_v2::IndexSearchResult& result) = 0;
    /**
    * Search for the nearest neighbors of a query vector within a given radius.
    * @param query_vec  input vector, size d
    * @param radius  search radius
    * @param result  output search result
    * @return       status of the operation
    */
    virtual doris::Status range_search(const float* query_vec, const float& radius,
                                       const segment_v2::IndexSearchParameters& params,
                                       segment_v2::IndexSearchResult& result) = 0;

    virtual doris::Status save(lucene::store::Directory*) = 0;

    virtual doris::Status load(lucene::store::Directory*) = 0;

    size_t get_dimension() const { return _dimension; }

    void set_metric(AnnIndexMetric metric) { _metric = metric; }

protected:
    // When adding vectors to the index, use this variable to check the dimension of the vectors.
    size_t _dimension = 0;
    AnnIndexMetric _metric = AnnIndexMetric::L2; // Default metric is L2 distance
};
#include "common/compile_check_end.h"
} // namespace doris::segment_v2