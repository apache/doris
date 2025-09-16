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

#include <gen_cpp/Opcodes_types.h>

#include <string>

#include "olap/rowset/segment_v2/ann_index/ann_index.h"
#include "vec/runtime/vector_search_user_params.h"

namespace doris::segment_v2 {
struct AnnRangeSearchParams;
#include "common/compile_check_begin.h"

/**
 * @brief Runtime information structure for ANN (Approximate Nearest Neighbor) range search operations.
 * 
 * This structure encapsulates all the necessary runtime parameters required for performing
 * range search queries on ANN indexes. Range search finds all vectors within a specified
 * distance radius from a query vector, which is different from traditional K-NN search
 * that finds a fixed number of nearest neighbors.
 * 
 * The structure supports:
 * - L2 distance and inner product metrics
 * - Configurable search radius for distance thresholds
 * - Deep copy semantics for query vectors
 * - Integration with Doris vectorized execution engine
 */
struct AnnRangeSearchRuntime {
    /**
     * @brief Default constructor initializing all fields to safe default values.
     * 
     * Initializes the structure with:
     * - Range search disabled by default
     * - Less-than-or-equal comparison mode
     * - Zero radius and invalid metric type
     * - Null query vector pointer
     */
    // DefaultConstructor
    AnnRangeSearchRuntime()
            : is_ann_range_search(false),
              is_le_or_lt(true),
              src_col_idx(0),
              dst_col_idx(-1),
              radius(0.0),
              metric_type(AnnIndexMetric::UNKNOWN) {
        query_value = nullptr;
    }

    /**
     * @brief Copy constructor with deep copy semantics for query vector.
     * 
     * Performs deep copying of all fields including the query_value array.
     * This is crucial for thread safety and preventing memory corruption
     * when the runtime info is passed between different execution contexts.
     * 
     * @param other The source RangeSearchRuntimeInfo to copy from
     */
    // CopyConstructor
    AnnRangeSearchRuntime(const AnnRangeSearchRuntime& other)
            : is_ann_range_search(other.is_ann_range_search),
              is_le_or_lt(other.is_le_or_lt),
              src_col_idx(other.src_col_idx),
              dim(other.dim),
              dst_col_idx(other.dst_col_idx),
              radius(other.radius),
              metric_type(other.metric_type),
              user_params(other.user_params) {
        // Do deep copy to query_value.
        if (other.query_value) {
            query_value = std::make_unique<float[]>(other.dim);
            std::copy(other.query_value.get(), other.query_value.get() + other.dim,
                      query_value.get());
        } else {
            query_value = nullptr;
        }
    }

    /**
     * @brief Assignment operator with deep copy semantics.
     * 
     * Ensures proper assignment of all fields with deep copying of the query vector.
     * Maintains the same memory safety guarantees as the copy constructor.
     * 
     * @param other The source RangeSearchRuntimeInfo to assign from
     * @return Reference to this object for chaining
     */
    AnnRangeSearchRuntime& operator=(const AnnRangeSearchRuntime& other) {
        is_ann_range_search = other.is_ann_range_search;
        is_le_or_lt = other.is_le_or_lt;
        src_col_idx = other.src_col_idx;
        dst_col_idx = other.dst_col_idx;
        radius = other.radius;
        metric_type = other.metric_type;
        user_params = other.user_params;
        dim = other.dim;
        // Do deep copy to query_value.
        if (other.query_value) {
            query_value = std::make_unique<float[]>(other.dim);
            std::copy(other.query_value.get(), other.query_value.get() + other.dim,
                      query_value.get());
        } else {
            query_value = nullptr;
        }
        return *this;
    }

    /**
     * @brief Converts the runtime info to AnnRangeSearchParams for actual search execution.
     * @return AnnRangeSearchParams structure suitable for index operations
     */
    AnnRangeSearchParams to_range_search_params() const;

    /**
     * @brief Generates a string representation for debugging and logging.
     * @return String containing all relevant runtime information
     */
    std::string to_string() const;

    // Core search configuration
    bool is_ann_range_search = false;          ///< Flag indicating if ANN range search is enabled
    bool is_le_or_lt = true;                   ///< Comparison mode: true for <=, false for <
    size_t src_col_idx = 0;                    ///< Source column index in the schema
    size_t dim = 0;                            ///< Dimensionality of the vector space
    int64_t dst_col_idx = -1;                  ///< Destination column index (-1 if not applicable)
    double radius = 0.0;                       ///< Search radius/distance threshold
    AnnIndexMetric metric_type;                ///< Distance metric (L2, Inner Product, etc.)
    doris::VectorSearchUserParams user_params; ///< User-defined search parameters
    std::unique_ptr<float[]> query_value;      ///< Query vector data (deep copied)
};
#include "common/compile_check_end.h"
} // namespace doris::segment_v2