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
 * @file ann_topn_runtime.h
 * @brief Runtime execution engine for ANN (Approximate Nearest Neighbor) Top-N queries in Doris.
 * 
 * This file contains the runtime infrastructure for executing ANN Top-N queries efficiently
 * using vector indexes. It provides the bridge between Doris's SQL execution engine and
 * underlying vector similarity search libraries like FAISS.
 * 
 * The main class AnnTopNRuntime handles:
 * - SQL expression analysis to extract vector search parameters
 * - Integration with segment-level ANN indexes  
 * - Result collection and sorting for Top-K nearest neighbor queries
 * - Performance statistics and monitoring
 * 
 * This is used internally by the query execution engine when processing SQL queries like:
 * SELECT * FROM table ORDER BY l2_distance(vector_column, [1,2,3]) LIMIT 10;
 */

#pragma once

#include "runtime/runtime_state.h"
#include "vec/columns/column.h"
#include "vec/exprs/varray_literal.h"
#include "vec/exprs/vcast_expr.h"
#include "vec/exprs/vectorized_fn_call.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/exprs/vexpr_fwd.h"
#include "vec/exprs/vslot_ref.h"

namespace doris::segment_v2 {
#include "common/compile_check_begin.h"
struct AnnIndexStats;

/**
 * @brief Runtime execution engine for ANN (Approximate Nearest Neighbor) Top-N queries.
 * 
 * This class implements the runtime execution logic for ANN Top-N queries, which find
 * the K nearest neighbors to a given query vector. It integrates with Doris's vectorized
 * execution framework and supports various distance metrics and search parameters.
 * 
 * Key features:
 * - Supports both ascending and descending order results
 * - Configurable K (limit) parameter for top-N results
 * - Integration with segment-level ANN indexes
 * - Performance statistics collection
 * - Thread-safe execution in parallel query contexts
 * 
 * Typical usage in SQL:
 * SELECT * FROM table ORDER BY l2_distance(vec_column, [1,2,3]) LIMIT 10;
 */
class AnnTopNRuntime {
    ENABLE_FACTORY_CREATOR(AnnTopNRuntime);

public:
    /**
     * @brief Constructs an AnnTopNRuntime instance.
     * 
     * @param asc Sort order: true for ascending (smallest distances first), false for descending
     * @param limit Maximum number of results to return (K in K-NN)
     * @param order_by_expr_ctx Expression context for the distance function (e.g., l2_distance)
     */
    AnnTopNRuntime(bool asc, size_t limit, vectorized::VExprContextSPtr order_by_expr_ctx)
            : _asc(asc), _limit(limit), _order_by_expr_ctx(order_by_expr_ctx) {};

    /**
     * @brief Prepares the runtime for execution by analyzing the distance expression.
     * 
     * This method analyzes the ORDER BY expression to extract:
     * - Source column index (the vector column being searched)
     * - Distance metric type (L2, Inner Product, etc.)
     * - Query vector from the literal array
     * - User-defined search parameters
     * 
     * @param state Runtime state containing session and query context
     * @param row_desc Row descriptor for the input schema
     * @return Status indicating success or failure
     */
    Status prepare(RuntimeState* state, const RowDescriptor& row_desc);

    vectorized::VExprContextSPtr get_order_by_expr_ctx() const { return _order_by_expr_ctx; }

    /**
     * @brief Executes the ANN search on the given index iterator.
     * 
     * This is the core method that performs the actual ANN search by:
     * 1. Calling the underlying index search method (e.g., HNSW, IVF)
     * 2. Filtering results based on the provided row bitmap
     * 3. Collecting performance statistics
     * 4. Returning the top-K results with their distances and row IDs
     * 
     * @param ann_index_iterator Iterator for the ANN index on the segment
     * @param row_bitmap Bitmap indicating which rows are valid for the search
     * @param result_column Output column containing the computed distances
     * @param row_ids Output vector containing the row IDs of matching results
     * @param ann_index_stats Statistics collector for performance monitoring
     * @return Status indicating success or failure
     */
    Status evaluate_vector_ann_search(segment_v2::IndexIterator* ann_index_iterator,
                                      roaring::Roaring* row_bitmap, size_t rows_of_segment,
                                      vectorized::IColumn::MutablePtr& result_column,
                                      std::unique_ptr<std::vector<uint64_t>>& row_ids,
                                      segment_v2::AnnIndexStats& ann_index_stats);

    /**
     * @brief Gets the distance metric type used by this runtime.
     * @return The metric type (L2_DISTANCE, INNER_PRODUCT, etc.)
     */
    AnnIndexMetric get_metric_type() const { return _metric_type; }

    /**
     * @brief Returns a debug string representation of this runtime.
     * @return String containing runtime configuration and state information
     */
    std::string debug_string() const;

    /**
     * @brief Gets the source column index (vector column being searched).
     * @return Column index in the table schema
     */
    size_t get_src_column_idx() const { return _src_column_idx; }

    /**
     * @brief Gets the destination column index for distance results.
     * @return Column index where distance values will be stored
     */
    size_t get_dest_column_idx() const { return _dest_column_idx; }

    /**
     * @brief Gets the sort order for results.
     * @return true for ascending order (smallest distances first), false for descending
     */
    bool is_asc() const { return _asc; }

private:
    // Core configuration
    const bool _asc;     ///< Sort order for results
    const size_t _limit; ///< Maximum number of results (K in K-NN)
    vectorized::VExprContextSPtr
            _order_by_expr_ctx; ///< Expression context for distance calculation

    // Runtime metadata
    std::string _name = "ann_topn_runtime";     ///< Runtime identifier for logging
    size_t _src_column_idx = -1;                ///< Source vector column index
    size_t _dest_column_idx = -1;               ///< Destination distance column index
    segment_v2::AnnIndexMetric _metric_type;    ///< Distance metric type
    vectorized::IColumn::Ptr _query_array;      ///< Query vector data
    doris::VectorSearchUserParams _user_params; ///< User-defined search parameters
};
#include "common/compile_check_end.h"
} // namespace doris::segment_v2