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
#include "olap/rowset/segment_v2/ann_index/ann_index.h"
#include "vec/core/types.h"

namespace doris::segment_v2 {
#include "common/compile_check_begin.h"
struct IndexSearchParameters;
struct IndexSearchResult;
/**
 * @brief Build parameters for constructing FAISS-based vector indexes.
 * 
 * This structure encapsulates all configuration parameters needed to build
 * various types of FAISS indexes. It supports different index types and
 * distance metrics commonly used in vector similarity search.
 */
struct FaissBuildParameter {
    /**
     * @brief Supported vector index types.
     */
    enum class IndexType {
        HNSW ///< Hierarchical Navigable Small World (HNSW) index for high performance
    };

    /**
     * @brief Supported distance metrics for vector similarity.
     */
    enum class MetricType {
        L2, ///< Euclidean distance (L2 norm)
        IP, ///< Inner product (cosine similarity when vectors are normalized)
    };

    /**
     * @brief Converts string representation to IndexType enum.
     * @param type String representation of index type (e.g., "hnsw")
     * @return Corresponding IndexType enum value
     * @throws doris::Exception for unsupported index types
     */
    static IndexType string_to_index_type(const std::string& type) {
        if (type == "hnsw") {
            return IndexType::HNSW;
        } else {
            throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT, "Unsupported index type: {}",
                                   type);
        }
    }

    /**
     * @brief Converts string representation to MetricType enum.
     * @param type String representation of metric type (e.g., "l2_distance", "inner_product")
     * @return Corresponding MetricType enum value
     * @throws doris::Exception for unsupported metric types
     */
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

    // HNSW-specific parameters
    int dim = 0;        ///< Vector dimensionality (must match data vectors)
    int max_degree = 0; ///< Maximum number of connections per node in HNSW graph
    IndexType index_type = IndexType::HNSW;  ///< Type of index to build
    MetricType metric_type = MetricType::L2; ///< Distance metric to use
    int ef_construction = 40; ///< Size of dynamic list for nearest neighbors during construction
};

/**
 * @brief FAISS-based implementation of vector index for approximate nearest neighbor search.
 * 
 * This class provides a concrete implementation of the VectorIndex interface using
 * the FAISS library. It supports various index types (currently HNSW) and distance
 * metrics (L2, Inner Product) for efficient vector similarity search.
 * 
 * Key features:
 * - High-performance approximate nearest neighbor search
 * - Support for both exact and range search queries
 * - Integration with Doris storage and query execution
 * - Persistence to/from Lucene directory storage
 * - Bitmap-based result filtering
 * 
 * Thread safety: This class is NOT thread-safe. Concurrent access should be
 * synchronized externally.
 */
class FaissVectorIndex : public VectorIndex {
public:
    /**
     * @brief Converts a Roaring bitmap to a FAISS IDSelector for filtered search.
     * 
     * This utility method creates a FAISS IDSelector that can be used to filter
     * search results to only include vectors whose IDs are present in the bitmap.
     * This is essential for supporting WHERE clause filtering in vector queries.
     * 
     * @param bitmap Roaring bitmap containing valid vector IDs
     * @return Unique pointer to a FAISS IDSelector for the given bitmap
     */
    static std::unique_ptr<faiss::IDSelector> roaring_to_faiss_selector(
            const roaring::Roaring& bitmap);

    /**
     * @brief Updates a Roaring bitmap with the given labels/IDs.
     * 
     * This method is used to update result bitmaps with the vector IDs
     * returned from FAISS search operations.
     * 
     * @param labels Array of vector IDs returned from search
     * @param n Number of labels in the array
     * @param roaring Reference to the Roaring bitmap to update
     */
    static void update_roaring(const faiss::idx_t* labels, const size_t n,
                               roaring::Roaring& roaring);

    /**
     * @brief Default constructor.
     */
    FaissVectorIndex();

    /**
     * @brief Adds vectors to the index for future searches.
     * 
     * This method is used during index building to add vectors to the FAISS index.
     * The vectors must have the same dimensionality as specified in build parameters.
     * 
     * @param n Number of vectors to add
     * @param vec Pointer to vector data (n * dim float values)
     * @return Status indicating success or failure
     */
    doris::Status add(int n, const float* vec) override;

    /**
     * @brief Sets the build parameters for the index.
     * 
     * This method must be called before adding vectors or performing searches.
     * It configures the underlying FAISS index with the specified parameters.
     * 
     * @param params Build parameters including index type, metric, and dimensions
     */
    void build(const FaissBuildParameter& params);

    /**
     * @brief Performs approximate k-nearest neighbor search.
     * 
     * Finds the k most similar vectors to the query vector using the configured
     * distance metric. Results are ordered by similarity (closest first for L2,
     * highest score first for inner product).
     * 
     * @param query_vec Query vector (must be same dimensionality as index)
     * @param k Number of nearest neighbors to find
     * @param params Search parameters including any filtering criteria
     * @param result Output structure containing distances and vector IDs
     * @return Status indicating success or failure
     */
    doris::Status ann_topn_search(const float* query_vec, int k,
                                  const segment_v2::IndexSearchParameters& params,
                                  segment_v2::IndexSearchResult& result) override;

    /**
     * @brief Performs range search to find all vectors within a distance threshold.
     * 
     * Finds all vectors within the specified radius from the query vector.
     * This is useful for similarity queries where you want all "similar enough"
     * vectors rather than a fixed number of nearest neighbors.
     * 
     * @param query_vec Query vector (must be same dimensionality as index)
     * @param radius Maximum distance threshold for results
     * @param params Search parameters including any filtering criteria
     * @param result Output structure containing distances and vector IDs
     * @return Status indicating success or failure
     */
    doris::Status range_search(const float* query_vec, const float& radius,
                               const segment_v2::IndexSearchParameters& params,
                               segment_v2::IndexSearchResult& result) override;

    /**
     * @brief Saves the index to persistent storage.
     * 
     * Serializes the complete FAISS index to the provided Lucene directory
     * for later loading. This enables index persistence across restarts.
     * 
     * @param directory Lucene directory for writing index data
     * @return Status indicating success or failure
     */
    doris::Status save(lucene::store::Directory*) override;

    /**
     * @brief Loads the index from persistent storage.
     * 
     * Deserializes a previously saved FAISS index from the provided Lucene
     * directory. The loaded index is ready for search operations.
     * 
     * @param directory Lucene directory containing saved index data
     * @return Status indicating success or failure
     */
    doris::Status load(lucene::store::Directory*) override;

private:
    std::unique_ptr<faiss::Index> _index = nullptr; ///< Underlying FAISS index instance
};
#include "common/compile_check_end.h"
} // namespace doris::segment_v2