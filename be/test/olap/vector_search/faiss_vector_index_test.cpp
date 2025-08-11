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

#include <faiss/IndexHNSW.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstddef>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "olap/rowset/segment_v2/ann_index/ann_index.h"
#include "olap/rowset/segment_v2/ann_index/ann_search_params.h"
#include "olap/rowset/segment_v2/ann_index/faiss_ann_index.h"
// metrics.h not used directly here
#include "vector_search_utils.h"

using namespace doris::segment_v2;

namespace doris::vectorized {

// Test saving and loading an index
TEST_F(VectorSearchTest, TestSaveAndLoad) {
    // Step 1: Create first index instance
    auto index1 = std::make_unique<FaissVectorIndex>();

    // Step 2: Set build parameters
    FaissBuildParameter params;
    params.dim = 128;       // Vector dimension
    params.max_degree = 16; // HNSW max connections
    params.index_type = FaissBuildParameter::IndexType::HNSW;
    index1->set_build_params(params);

    // Step 3: Add vectors to the index
    const int num_vectors = 100;
    std::vector<float> vectors;
    for (int i = 0; i < num_vectors; i++) {
        auto tmp = vector_search_utils::generate_random_vector(params.dim);
        vectors.insert(vectors.end(), tmp.begin(), tmp.end());
    }

    std::ignore = index1->add(num_vectors, vectors.data());

    // Step 4: Save the index
    auto save_status = index1->save(_ram_dir.get());
    ASSERT_TRUE(save_status.ok()) << "Failed to save index: " << save_status.to_string();

    // Step 5: Create a new index instance
    auto index2 = std::make_unique<FaissVectorIndex>();

    // Step 6: Load the index
    auto load_status = index2->load(_ram_dir.get());
    ASSERT_TRUE(load_status.ok()) << "Failed to load index: " << load_status.to_string();

    // Step 7: Verify the loaded index works by searching
    auto query_vec = vector_search_utils::generate_random_vector(params.dim);
    const int top_k = 10;

    // TopN search requires a candidate roaring and rows_of_segment now.
    HNSWSearchParameters topn_params;
    auto topn_roaring = std::make_unique<roaring::Roaring>();
    for (int i = 0; i < num_vectors; ++i) topn_roaring->add(i);
    topn_params.roaring = topn_roaring.get();
    topn_params.rows_of_segment = num_vectors;

    IndexSearchResult search_result1;
    IndexSearchResult search_result2;

    std::ignore = index1->ann_topn_search(query_vec.data(), top_k, topn_params, search_result1);

    std::ignore = index2->ann_topn_search(query_vec.data(), top_k, topn_params, search_result2);

    // Compare the results
    EXPECT_EQ(search_result1.roaring->cardinality(), search_result2.roaring->cardinality())
            << "Row ID cardinality mismatch";
    for (size_t i = 0; i < search_result1.roaring->cardinality(); ++i) {
        EXPECT_EQ(search_result1.distances[i], search_result2.distances[i])
                << "Distance mismatch at index " << i;
    }

    HNSWSearchParameters hnsw_params;
    auto roaring_bitmap = std::make_unique<roaring::Roaring>();
    hnsw_params.roaring = roaring_bitmap.get();
    for (size_t i = 0; i < num_vectors; ++i) {
        hnsw_params.roaring->add(i);
    }
    IndexSearchResult range_search_result1;
    std::ignore = index1->range_search(vectors.data(), 10, hnsw_params, range_search_result1);
    IndexSearchResult range_search_result2;
    std::ignore = index2->range_search(vectors.data(), 10, hnsw_params, range_search_result2);
    EXPECT_EQ(range_search_result1.roaring->cardinality(),
              range_search_result2.roaring->cardinality())
            << "Row ID cardinality mismatch";
    for (size_t i = 0; i < range_search_result1.roaring->cardinality(); ++i) {
        EXPECT_EQ(range_search_result1.distances[i], range_search_result2.distances[i])
                << "Distance mismatch at index " << i;
    }
}

TEST_F(VectorSearchTest, UpdateRoaring) {
    // Create a roaring bitmap
    roaring::Roaring roaring_bitmap;
    // Create some dummy labels
    const size_t n = 5;
    faiss::idx_t labels[n] = {1, 2, 3, 4, 5};

    // Call the update_roaring function
    FaissVectorIndex::update_roaring(labels, n, roaring_bitmap);

    EXPECT_EQ(roaring_bitmap.cardinality(), n) << "Roaring bitmap size mismatch";

    for (size_t i = 0; i < n; ++i) {
        EXPECT_EQ(roaring_bitmap.contains(labels[i]), true)
                << "Label " << labels[i] << " not found";
    }
}

TEST_F(VectorSearchTest, CompareResultWithNativeFaiss1) {
    const size_t iterations = 3;
    // Create random number generator
    std::random_device rd;
    std::mt19937 gen(rd());
    // Define fixed parameter sets to choose from
    const std::vector<int> dimensions = {32, 64, 128, 256};
    const std::vector<int> max_connections = {8, 16, 32, 64};
    const std::vector<int> vector_counts = {100, 200, 500, 1000};

    for (size_t iter = 0; iter < iterations; ++iter) {
        // Randomly select parameters from the fixed sets
        const int dimension =
                dimensions[std::uniform_int_distribution<>(0, dimensions.size() - 1)(gen)];
        const int max_connection = max_connections[std::uniform_int_distribution<>(
                0, max_connections.size() - 1)(gen)];
        const int num_vectors =
                vector_counts[std::uniform_int_distribution<>(0, vector_counts.size() - 1)(gen)];

        // Step 1: Create indexes
        auto doris_index = doris::vector_search_utils::create_doris_index(
                doris::vector_search_utils::IndexType::HNSW, dimension, max_connection);
        auto native_index = doris::vector_search_utils::create_native_index(
                doris::vector_search_utils::IndexType::HNSW, dimension, max_connection);

        // Step 2: Generate vectors and add to indexes
        auto vectors =
                doris::vector_search_utils::generate_test_vectors_matrix(num_vectors, dimension);
        doris::vector_search_utils::add_vectors_to_indexes_serial_mode(doris_index.get(),
                                                                       native_index.get(), vectors);

        // Step 3: Search
        int query_idx = num_vectors / 2;
        const float* query_vec = vectors[query_idx].data();
        const int top_k = 10;

        // Search in Doris index
        HNSWSearchParameters search_params;
        auto roaring = std::make_unique<roaring::Roaring>();
        for (int i = 0; i < num_vectors; ++i) roaring->add(i);
        search_params.roaring = roaring.get();
        search_params.rows_of_segment = num_vectors;
        IndexSearchResult doris_results;
        auto search_status =
                doris_index->ann_topn_search(query_vec, top_k, search_params, doris_results);
        ASSERT_EQ(search_status.ok(), true)
                << "Search failed with dimension=" << dimension
                << ", max_connections=" << max_connection << ", num_vectors=" << num_vectors;

        // Search in native Faiss index
        std::vector<float> native_distances(top_k);
        std::vector<faiss::idx_t> native_indices(top_k);
        native_index->search(1, query_vec, top_k, native_distances.data(), native_indices.data());
        size_t cnt = std::count_if(native_indices.begin(), native_indices.end(),
                                   [](faiss::idx_t idx) { return idx != -1; });
        for (size_t i = 0; i < cnt; ++i) {
            native_distances[i] = std::sqrt(native_distances[i]);
        }
        // Step 4: Compare results
        vector_search_utils::compare_search_results(doris_results, native_distances,
                                                    native_indices);
    }
}

TEST_F(VectorSearchTest, CompareResultWithNativeFaiss2) {
    const size_t iterations = 2;
    // Create random number generator
    std::random_device rd;
    std::mt19937 gen(rd());
    // Define fixed parameter sets to choose from
    const std::vector<int> dimensions = {32, 64, 128, 256};
    const std::vector<int> max_connections = {8, 16, 32, 64};
    const std::vector<int> vector_counts = {100, 200, 500, 1000};

    for (size_t i = 0; i < iterations; ++i) {
        // Randomly select parameters from the fixed sets
        const int dimension =
                dimensions[std::uniform_int_distribution<>(0, dimensions.size() - 1)(gen)];
        const int max_connection = max_connections[std::uniform_int_distribution<>(
                0, max_connections.size() - 1)(gen)];
        const int num_vectors =
                vector_counts[std::uniform_int_distribution<>(0, vector_counts.size() - 1)(gen)];

        // Step 1: Create indexes
        auto doris_index = doris::vector_search_utils::create_doris_index(
                doris::vector_search_utils::IndexType::HNSW, dimension, max_connection);
        auto native_index = doris::vector_search_utils::create_native_index(
                doris::vector_search_utils::IndexType::HNSW, dimension, max_connection);

        // Step 2: Generate vectors and add to indexes
        std::vector<std::vector<float>> vectors =
                doris::vector_search_utils::generate_test_vectors_matrix(num_vectors, dimension);
        doris::vector_search_utils::add_vectors_to_indexes_serial_mode(doris_index.get(),
                                                                       native_index.get(), vectors);

        // Step 3: Search
        int query_idx = num_vectors / 2;
        const float* query_vec = vectors[query_idx].data();
        const int top_k = num_vectors;
        HNSWSearchParameters search_params;
        auto roaring = std::make_unique<roaring::Roaring>();
        for (int i = 0; i < num_vectors; ++i) roaring->add(i);
        search_params.roaring = roaring.get();
        search_params.rows_of_segment = num_vectors;
        IndexSearchResult doris_results;
        std::ignore = doris_index->ann_topn_search(query_vec, top_k, search_params, doris_results);

        // Search in native Faiss index
        std::vector<float> native_distances(top_k, -1);
        std::vector<faiss::idx_t> native_indices(top_k, -1);
        native_index->search(1, query_vec, top_k, native_distances.data(), native_indices.data());
        size_t cnt = std::count_if(native_indices.begin(), native_indices.end(),
                                   [](faiss::idx_t idx) { return idx != -1; });
        for (size_t i = 0; i < cnt; ++i) {
            native_distances[i] = std::sqrt(native_distances[i]);
        }
        // Step 4: Compare results
        doris::vector_search_utils::compare_search_results(doris_results, native_distances,
                                                           native_indices);
    }
}

TEST_F(VectorSearchTest, SearchAllVectors) {
    size_t iterations = 5;
    for (size_t i = 0; i < iterations; ++i) {
        // Step 1: Create and build index
        auto index1 = std::make_unique<FaissVectorIndex>();

        FaissBuildParameter params;
        params.dim = 64;
        params.max_degree = 32;
        params.index_type = FaissBuildParameter::IndexType::HNSW;
        index1->set_build_params(params);

        // Add 500 vectors
        const int num_vectors = 500;
        std::vector<float> vectors;
        for (int i = 0; i < num_vectors; i++) {
            auto vec = doris::vector_search_utils::generate_random_vector(params.dim);
            vectors.insert(vectors.end(), vec.begin(), vec.end());
        }

        ASSERT_EQ(index1->add(500, vectors.data()).ok(), true);

        // Save index
        ASSERT_TRUE(index1->save(_ram_dir.get()).ok());

        // Step 2: Load index
        auto index2 = std::make_unique<FaissVectorIndex>();
        ASSERT_TRUE(index2->load(_ram_dir.get()).ok());

        // Step 3: Search all vectors
        HNSWSearchParameters search_params;
        auto roaring = std::make_unique<roaring::Roaring>();
        for (int i = 0; i < num_vectors; ++i) roaring->add(i);
        search_params.roaring = roaring.get();
        search_params.rows_of_segment = num_vectors;
        IndexSearchResult search_result;

        // Search for all vectors - use a vector we know is in the index
        std::vector<float> query_vec {
                vectors.begin(), vectors.begin() + params.dim}; // Use the first vector we added
        const int top_k = num_vectors;                          // Get all vectors

        ASSERT_EQ(
                index2->ann_topn_search(query_vec.data(), top_k, search_params, search_result).ok(),
                true);
        // Step 4: Verify we got all vectors back
        // Note: In practical ANN search with approximate algorithms like HNSW,
        // we might not get exactly all vectors due to the nature of approximate search.
        // So we verify we got a reasonable number back.
        EXPECT_GE(search_result.roaring->cardinality(), num_vectors * 0.60)
                << "Expected to find at least 60% of all vectors";

        // Also verify the first result is the query vector itself (it should be an exact match)
        ASSERT_EQ(search_result.roaring->isEmpty(), false) << "Search result should not be empty";
        size_t first = search_result.roaring->getIndex(0);
        std::vector<float> first_result_vec(vectors.begin() + first * params.dim,
                                            vectors.begin() + (first + 1) * params.dim);
        std::string query_vec_str = fmt::format("[{}]", fmt::join(query_vec, ","));
        std::string first_result_vec_str = fmt::format("[{}]", fmt::join(first_result_vec, ","));
        EXPECT_EQ(first_result_vec, query_vec) << "First result should be the query vector itself";
    }
}

TEST_F(VectorSearchTest, CompRangeSearch) {
    size_t iterations = 5;
    std::vector<faiss::MetricType> metrics = {faiss::METRIC_L2, faiss::METRIC_INNER_PRODUCT};
    for (size_t i = 0; i < iterations; ++i) {
        for (auto metric : metrics) {
            // Random parameters for each test iteration
            std::random_device rd;
            std::mt19937 gen(rd());
            size_t random_d = std::uniform_int_distribution<>(1, 512)(gen);
            size_t random_m = 4 << std::uniform_int_distribution<>(1, 4)(gen);
            size_t random_n = std::uniform_int_distribution<>(10, 200)(gen);

            // Step 1: Create and build index
            auto doris_index = std::make_unique<FaissVectorIndex>();
            FaissBuildParameter params;
            params.dim = random_d;
            params.max_degree = random_m;
            params.index_type = FaissBuildParameter::IndexType::HNSW;
            if (metric == faiss::METRIC_L2) {
                params.metric_type = FaissBuildParameter::MetricType::L2;
            } else if (metric == faiss::METRIC_INNER_PRODUCT) {
                params.metric_type = FaissBuildParameter::MetricType::IP;
            } else {
                throw std::runtime_error(fmt::format("Unsupported metric type: {}", metric));
            }
            doris_index->set_build_params(params);

            const int num_vectors = random_n;
            std::vector<std::vector<float>> vectors;
            for (int i = 0; i < num_vectors; i++) {
                auto vec = vector_search_utils::generate_random_vector(params.dim);
                vectors.push_back(vec);
            }

            std::unique_ptr<faiss::Index> native_index;
            if (metric == faiss::METRIC_L2) {
                native_index = std::make_unique<faiss::IndexHNSWFlat>(params.dim, params.max_degree,
                                                                      faiss::METRIC_L2);
            } else if (metric == faiss::METRIC_INNER_PRODUCT) {
                native_index = std::make_unique<faiss::IndexHNSWFlat>(params.dim, params.max_degree,
                                                                      faiss::METRIC_INNER_PRODUCT);
            } else {
                throw std::runtime_error(fmt::format("Unsupported metric type: {}", metric));
            }

            doris::vector_search_utils::add_vectors_to_indexes_serial_mode(
                    doris_index.get(), native_index.get(), vectors);

            std::vector<float> query_vec = vectors.front();
            float radius = 0;
            radius = doris::vector_search_utils::get_radius_from_matrix(
                    query_vec.data(), params.dim, vectors, 0.4f, metric);

            HNSWSearchParameters hnsw_params;
            hnsw_params.ef_search = 16;
            // Search on all rows;
            auto roaring = std::make_unique<roaring::Roaring>();
            hnsw_params.roaring = roaring.get();
            for (size_t i = 0; i < vectors.size(); i++) {
                hnsw_params.roaring->add(i);
            }
            hnsw_params.is_le_or_lt = metric == faiss::METRIC_L2;
            IndexSearchResult doris_result;
            std::ignore =
                    doris_index->range_search(query_vec.data(), radius, hnsw_params, doris_result);

            faiss::SearchParametersHNSW search_params_native;
            search_params_native.efSearch = hnsw_params.ef_search;
            faiss::RangeSearchResult search_result_native(1, true);
            // 对于L2，radius要平方；对于IP，直接用
            float faiss_radius = (metric == faiss::METRIC_L2) ? radius * radius : radius;
            native_index->range_search(1, query_vec.data(), faiss_radius, &search_result_native,
                                       &search_params_native);

            std::vector<std::pair<int, float>> native_results;
            size_t begin = search_result_native.lims[0];
            size_t end = search_result_native.lims[1];
            for (size_t i = begin; i < end; i++) {
                native_results.push_back(
                        {search_result_native.labels[i], search_result_native.distances[i]});
            }

            // Make sure result is same
            ASSERT_NEAR(doris_result.roaring->cardinality(), native_results.size(), 1)
                    << fmt::format("\nd: {}, m: {}, n: {}, metric: {}", random_d, random_m,
                                   random_n, metric);
            ASSERT_EQ(doris_result.distances != nullptr, true);
            if (doris_result.roaring->cardinality() == native_results.size()) {
                for (size_t i = 0; i < native_results.size(); i++) {
                    const size_t rowid = native_results[i].first;
                    const float dis = native_results[i].second;
                    ASSERT_EQ(doris_result.roaring->contains(rowid), true)
                            << "Row ID mismatch at rank " << i;
                    if (metric == faiss::METRIC_L2) {
                        ASSERT_FLOAT_EQ(doris_result.distances[i], sqrt(dis))
                                << "Distance mismatch at rank " << i;
                    } else {
                        ASSERT_FLOAT_EQ(doris_result.distances[i], dis)
                                << "Distance mismatch at rank " << i;
                    }
                }
            }
        }
    }
}

TEST_F(VectorSearchTest, RangeSearchAllRowsAsCandidates) {
    size_t iterations = 5;
    // Random parameters for each test iteration

    for (size_t i = 0; i < iterations; ++i) {
        std::random_device rd;
        std::mt19937 gen(rd());
        size_t random_d =
                std::uniform_int_distribution<>(1, 512)(gen); // Random dimension from 32 to 256
        size_t random_m =
                4 << std::uniform_int_distribution<>(1, 4)(gen); // Random M (4, 8, 16, 32, 64)
        size_t random_n = std::uniform_int_distribution<>(10, 200)(gen); // Random number of vectors
        // Step 1: Create and build index
        auto index1 = std::make_unique<FaissVectorIndex>();

        FaissBuildParameter params;
        params.dim = random_d;
        params.max_degree = random_m;
        params.index_type = FaissBuildParameter::IndexType::HNSW;
        index1->set_build_params(params);

        const int num_vectors = random_n;
        std::vector<std::vector<float>> vectors;
        for (int i = 0; i < num_vectors; i++) {
            auto vec = vector_search_utils::generate_random_vector(params.dim);
            vectors.push_back(vec);
        }
        std::unique_ptr<faiss::Index> native_index =
                std::make_unique<faiss::IndexHNSWFlat>(params.dim, params.max_degree);
        doris::vector_search_utils::add_vectors_to_indexes_serial_mode(index1.get(),
                                                                       native_index.get(), vectors);

        std::vector<float> query_vec = vectors.front();

        std::vector<std::pair<size_t, float>> distances(num_vectors);
        for (int i = 0; i < num_vectors; i++) {
            double sum = 0;
            auto& vec = vectors[i];
            for (int j = 0; j < params.dim; j++) {
                accumulate(vec[j], query_vec[j], sum);
            }
            distances[i] = std::make_pair(i, finalize(sum));
        }
        std::sort(distances.begin(), distances.end(),
                  [](const auto& a, const auto& b) { return a.second < b.second; });

        float radius = distances[num_vectors / 4].second;
        // Save index
        ASSERT_TRUE(index1->save(_ram_dir.get()).ok());

        // Step 2: Load index
        auto index2 = std::make_unique<FaissVectorIndex>();
        ASSERT_TRUE(index2->load(_ram_dir.get()).ok());

        // Step 3: Range search
        // Use a vector we know is in the index

        faiss::SearchParametersHNSW search_params;
        std::unique_ptr<roaring::Roaring> all_rows = std::make_unique<roaring::Roaring>();
        for (size_t i = 0; i < num_vectors; ++i) {
            all_rows->add(i);
        }
        auto sel = FaissVectorIndex::roaring_to_faiss_selector(*all_rows);
        search_params.sel = sel.get();
        search_params.efSearch = 16; // Set efSearch for better accuracy
        faiss::RangeSearchResult native_search_result(1, true);
        native_index->range_search(1, query_vec.data(), radius * radius, &native_search_result,
                                   &search_params);

        std::vector<std::pair<int, float>> native_results;
        size_t begin = native_search_result.lims[0];
        size_t end = native_search_result.lims[1];
        for (size_t i = begin; i < end; i++) {
            native_results.push_back(
                    {native_search_result.labels[i], native_search_result.distances[i]});
        }

        HNSWSearchParameters doris_search_params;
        doris_search_params.ef_search = 16; // Set efSearch for better accuracy
        doris_search_params.roaring = all_rows.get();
        IndexSearchResult search_result1;
        IndexSearchResult search_result2;

        ASSERT_EQ(
                index1->range_search(query_vec.data(), radius, doris_search_params, search_result1)
                        .ok(),
                true);
        ASSERT_EQ(
                index2->range_search(query_vec.data(), radius, doris_search_params, search_result2)
                        .ok(),
                true);

        ASSERT_EQ(search_result1.roaring->cardinality(), search_result2.roaring->cardinality());
        for (size_t i = 0; i < search_result1.roaring->cardinality(); i++) {
            ASSERT_EQ(search_result1.distances[i], search_result2.distances[i])
                    << "Distance mismatch at rank " << i;
        }

        ASSERT_EQ(search_result2.roaring->cardinality(), native_results.size());

        ASSERT_EQ(search_result2.distances != nullptr, true);
        for (size_t i = 0; i < native_results.size(); i++) {
            const size_t rowid = native_results[i].first;
            const float dis = native_results[i].second;
            ASSERT_EQ(search_result2.roaring->contains(rowid), true)
                    << "Row ID mismatch at rank " << i;
            ASSERT_FLOAT_EQ(search_result2.distances[i], sqrt(dis))
                    << "Distance mismatch at rank " << i;
        }

        doris_search_params.is_le_or_lt = false;
        doris_search_params.roaring = all_rows.get();
        for (size_t i = 0; i < num_vectors; ++i) {
            doris_search_params.roaring->add(i);
        }
        IndexSearchResult search_result3;
        std::ignore =
                index1->range_search(query_vec.data(), radius, doris_search_params, search_result3);
        roaring::Roaring ge_rows;
        ASSERT_EQ(search_result3.distances == nullptr, true);
        for (size_t i = 0; i < native_results.size(); ++i) {
            ge_rows.add(native_results[i].first);
        }
        roaring::Roaring and_row_id = ge_rows & *search_result3.roaring;
        roaring::Roaring or_row_id = ge_rows | *search_result3.roaring;
        ASSERT_EQ(and_row_id.cardinality(), 0);
        ASSERT_EQ(or_row_id.cardinality(), num_vectors);
    }
}

TEST_F(VectorSearchTest, RangeSearchWithSelector1) {
    size_t iterations = 2;
    for (size_t i = 0; i < iterations; ++i) {
        // Step 1: Create and build index
        auto index1 = std::make_unique<FaissVectorIndex>();

        FaissBuildParameter params;
        params.dim = 100;
        params.max_degree = 32;
        params.index_type = FaissBuildParameter::IndexType::HNSW;
        index1->set_build_params(params);

        const int num_vectors = 100;
        std::vector<std::vector<float>> vectors;
        for (int i = 0; i < num_vectors; i++) {
            auto vec = vector_search_utils::generate_random_vector(params.dim);
            vectors.push_back(vec);
        }

        // Use a vector we know is in the index
        std::vector<float> query_vec = vectors.front();
        std::vector<std::pair<size_t, float>> distances(num_vectors);
        for (int i = 0; i < num_vectors; i++) {
            double sum = 0;
            auto& vec = vectors[i];
            for (int j = 0; j < params.dim; j++) {
                accumulate(vec[j], query_vec[j], sum);
            }
            distances[i] = std::make_pair(i, finalize(sum));
        }
        std::sort(distances.begin(), distances.end(),
                  [](const auto& a, const auto& b) { return a.second < b.second; });
        // Use the median distance as the radius
        float radius = distances[num_vectors / 2].second;

        std::unique_ptr<faiss::Index> native_index = std::make_unique<faiss::IndexHNSWFlat>(
                params.dim, params.max_degree, faiss::METRIC_L2);
        doris::vector_search_utils::add_vectors_to_indexes_serial_mode(index1.get(),
                                                                       native_index.get(), vectors);

        std::unique_ptr<roaring::Roaring> all_rows = std::make_unique<roaring::Roaring>();
        std::unique_ptr<roaring::Roaring> sel_rows = std::make_unique<roaring::Roaring>();
        for (size_t i = 0; i < num_vectors; ++i) {
            all_rows->add(i);
            if (i % 2 == 0) {
                sel_rows->add(i);
            }
        }

        // Step 3: Range search
        faiss::SearchParametersHNSW search_params;
        search_params.efSearch = 16; // Set efSearch for better accuracy
        auto faiss_selector = segment_v2::FaissVectorIndex::roaring_to_faiss_selector(*sel_rows);
        search_params.sel = faiss_selector.get();
        faiss::RangeSearchResult native_search_result(1, true);
        native_index->range_search(1, query_vec.data(), radius * radius, &native_search_result,
                                   &search_params);
        // labels and distance
        std::vector<std::pair<int, float>> native_results;
        size_t begin = native_search_result.lims[0];
        size_t end = native_search_result.lims[1];
        for (size_t i = begin; i < end; i++) {
            native_results.push_back(
                    {native_search_result.labels[i], native_search_result.distances[i]});
        }

        HNSWSearchParameters doris_search_params;
        doris_search_params.ef_search = search_params.efSearch;
        doris_search_params.is_le_or_lt = true;
        doris_search_params.roaring = sel_rows.get();
        IndexSearchResult doris_search_result;

        ASSERT_EQ(index1->range_search(query_vec.data(), radius, doris_search_params,
                                       doris_search_result)
                          .ok(),
                  true);

        ASSERT_EQ(native_results.size(), doris_search_result.roaring->cardinality());

        ASSERT_EQ(doris_search_result.distances != nullptr, true);
        for (size_t i = 0; i < native_results.size(); i++) {
            const size_t rowid = native_results[i].first;
            const float dis = native_results[i].second;
            ASSERT_EQ(doris_search_result.roaring->contains(rowid), true)
                    << "Row ID mismatch at rank " << i;
            ASSERT_FLOAT_EQ(doris_search_result.distances[i], sqrt(dis))
                    << "Distance mismatch at rank " << i;
        }

        doris_search_params.is_le_or_lt = false;
        IndexSearchResult doris_search_result2;
        ASSERT_EQ(index1->range_search(query_vec.data(), radius, doris_search_params,
                                       doris_search_result2)
                          .ok(),
                  true);
        roaring::Roaring ge_rows = *doris_search_result2.roaring;
        roaring::Roaring less_rows;
        for (size_t i = 0; i < native_results.size(); ++i) {
            less_rows.add(native_results[i].first);
        }
        // result2 contains all rows that not included by result1
        roaring::Roaring and_row_id = ge_rows & less_rows;
        roaring::Roaring or_row_id = ge_rows | less_rows;
        ASSERT_NEAR(and_row_id.cardinality(), 0, 1);
        ASSERT_EQ(or_row_id.cardinality(), sel_rows->cardinality());
        ASSERT_EQ(or_row_id, *sel_rows);
    }
}

TEST_F(VectorSearchTest, InnerProductTopKSearch) {
    const size_t iterations = 1;
    const std::vector<int> dimensions = {32, 64};
    const std::vector<int> vector_counts = {100, 500};
    const std::vector<int> k_values = {5, 10, 20};

    for (size_t iter = 0; iter < iterations; ++iter) {
        for (int dim : dimensions) {
            for (int n : vector_counts) {
                for (int k : k_values) {
                    if (k > n) continue;

                    // Create Doris index
                    auto doris_index = std::make_unique<FaissVectorIndex>();
                    FaissBuildParameter params;
                    params.dim = dim;
                    params.max_degree = 32;
                    params.index_type = FaissBuildParameter::IndexType::HNSW;
                    params.metric_type = FaissBuildParameter::MetricType::IP;
                    doris_index->set_build_params(params);

                    // Generate normalized vectors (important for inner product)
                    std::vector<float> flat_vectors;
                    for (int i = 0; i < n; ++i) {
                        auto vec = doris::vector_search_utils::generate_random_vector(dim);
                        // Normalize the vector
                        float norm = 0.0f;
                        for (float val : vec) {
                            norm += val * val;
                        }
                        norm = std::sqrt(norm);
                        if (norm > 0) {
                            for (float& val : vec) {
                                val /= norm;
                            }
                        }
                        flat_vectors.insert(flat_vectors.end(), vec.begin(), vec.end());
                    }

                    // Add vectors to index
                    doris_index->add(n, flat_vectors.data());

                    // Create query vector (also normalized)
                    auto query_vec = doris::vector_search_utils::generate_random_vector(dim);
                    float norm = 0.0f;
                    for (float val : query_vec) {
                        norm += val * val;
                    }
                    norm = std::sqrt(norm);
                    if (norm > 0) {
                        for (float& val : query_vec) {
                            val /= norm;
                        }
                    }

                    // Perform search (top-N requires a candidate roaring and rows_of_segment)
                    HNSWSearchParameters search_params;
                    auto roaring = std::make_unique<roaring::Roaring>();
                    for (int i = 0; i < n; ++i) roaring->add(i);
                    search_params.roaring = roaring.get();
                    search_params.rows_of_segment = n;
                    IndexSearchResult search_result;
                    auto status = doris_index->ann_topn_search(query_vec.data(), k, search_params,
                                                               search_result);

                    ASSERT_TRUE(status.ok()) << "Inner product search failed";
                    ASSERT_GE(search_result.roaring->cardinality(), static_cast<size_t>(k * 0.7))
                            << "Expected to find at least 70% of requested results";

                    // Verify distances are in descending order (higher inner product is better)
                    for (size_t i = 1; i < search_result.roaring->cardinality(); ++i) {
                        ASSERT_GE(search_result.distances[i - 1], search_result.distances[i])
                                << "Inner product distances should be in descending order";
                    }

                    // Verify all distances are valid (between -1 and 1 for normalized vectors)
                    for (size_t i = 0; i < search_result.roaring->cardinality(); ++i) {
                        ASSERT_GE(search_result.distances[i], -1.1f)
                                << "Inner product distance should be >= -1";
                        ASSERT_LE(search_result.distances[i], 1.1f)
                                << "Inner product distance should be <= 1";
                    }
                }
            }
        }
    }
}

TEST_F(VectorSearchTest, InnerProductRangeSearchBasic) {
    const size_t iterations = 3;

    for (size_t iter = 0; iter < iterations; ++iter) {
        const int dim = 64;
        const int n = 500;
        const int m = 32;

        // Create Doris index
        auto doris_index = std::make_unique<FaissVectorIndex>();
        FaissBuildParameter params;
        params.dim = dim;
        params.max_degree = m;
        params.index_type = FaissBuildParameter::IndexType::HNSW;
        params.metric_type = FaissBuildParameter::MetricType::IP;
        doris_index->set_build_params(params);

        // Create native index for comparison
        faiss::IndexFlatIP native_index(dim);

        // Generate vectors
        std::vector<std::vector<float>> vectors;
        std::vector<float> flat_vectors;
        for (int i = 0; i < n; ++i) {
            auto vec = doris::vector_search_utils::generate_random_vector(dim);
            vectors.push_back(vec);
            flat_vectors.insert(flat_vectors.end(), vec.begin(), vec.end());
        }

        // Add vectors to both indexes
        doris_index->add(n, flat_vectors.data());
        native_index.add(n, flat_vectors.data());

        // Use first vector as query
        std::vector<float> query_vec = vectors[0];

        // Calculate radius based on inner product distribution
        float radius = doris::vector_search_utils::get_radius_from_matrix(
                query_vec.data(), dim, vectors, 0.5f, faiss::METRIC_INNER_PRODUCT);

        // Perform Doris range search
        HNSWSearchParameters doris_params;
        doris_params.ef_search = 100;
        doris_params.is_le_or_lt = false; // For inner product, we want values >= radius
        auto roaring = std::make_unique<roaring::Roaring>();
        for (int i = 0; i < n; ++i) {
            roaring->add(i);
        }
        doris_params.roaring = roaring.get();

        IndexSearchResult doris_result;
        auto status =
                doris_index->range_search(query_vec.data(), radius, doris_params, doris_result);
        ASSERT_TRUE(status.ok()) << "Doris range search failed";

        // Perform native range search
        faiss::RangeSearchResult native_result(1, true);
        native_index.range_search(1, query_vec.data(), radius, &native_result);

        // Compare results
        size_t native_count = native_result.lims[1] - native_result.lims[0];
        ASSERT_NEAR(doris_result.roaring->cardinality(), native_count, 1)
                << "Result count mismatch for inner product range search";

        // Verify all returned distances are >= radius
        for (size_t i = 0; i < doris_result.roaring->cardinality(); ++i) {
            ASSERT_GE(doris_result.distances[i], radius - 1e-6)
                    << "Distance should be >= radius for inner product range search";
        }
    }
}

TEST_F(VectorSearchTest, InnerProductVsL2Comparison) {
    const int dim = 32;
    const int n = 100;
    const int k = 10;

    // Generate the same set of vectors
    std::vector<float> flat_vectors;
    for (int i = 0; i < n; ++i) {
        auto vec = doris::vector_search_utils::generate_random_vector(dim);
        flat_vectors.insert(flat_vectors.end(), vec.begin(), vec.end());
    }

    // Create L2 index
    auto l2_index = std::make_unique<FaissVectorIndex>();
    FaissBuildParameter l2_params;
    l2_params.dim = dim;
    l2_params.max_degree = 32;
    l2_params.index_type = FaissBuildParameter::IndexType::HNSW;
    l2_params.metric_type = FaissBuildParameter::MetricType::L2;
    l2_index->set_build_params(l2_params);
    l2_index->add(n, flat_vectors.data());

    // Create Inner Product index
    auto ip_index = std::make_unique<FaissVectorIndex>();
    FaissBuildParameter ip_params;
    ip_params.dim = dim;
    ip_params.max_degree = 32;
    ip_params.index_type = FaissBuildParameter::IndexType::HNSW;
    ip_params.metric_type = FaissBuildParameter::MetricType::IP;
    ip_index->set_build_params(ip_params);
    ip_index->add(n, flat_vectors.data());

    // Use first vector as query
    std::vector<float> query_vec(flat_vectors.begin(), flat_vectors.begin() + dim);

    // Search with L2
    HNSWSearchParameters search_params;
    auto roaring = std::make_unique<roaring::Roaring>();
    for (int i = 0; i < n; ++i) roaring->add(i);
    search_params.roaring = roaring.get();
    search_params.rows_of_segment = n;
    IndexSearchResult l2_result;
    auto l2_status = l2_index->ann_topn_search(query_vec.data(), k, search_params, l2_result);
    ASSERT_EQ(l2_status.ok(), true) << "L2 search failed";

    // Search with Inner Product
    IndexSearchResult ip_result;
    auto ip_status = ip_index->ann_topn_search(query_vec.data(), k, search_params, ip_result);
    ASSERT_EQ(ip_status.ok(), true) << "Inner Product search failed";

    // Both should find results
    ASSERT_GT(l2_result.roaring->cardinality(), 0) << "L2 search should find results";
    ASSERT_GT(ip_result.roaring->cardinality(), 0) << "Inner Product search should find results";

    // Results should be different (different metrics lead to different rankings)
    // We'll check that at least some results are different
    std::set<size_t> l2_ids, ip_ids;
    for (size_t i = 0; i < l2_result.roaring->cardinality(); ++i) {
        l2_ids.insert(l2_result.roaring->getIndex(i));
    }
    for (size_t i = 0; i < ip_result.roaring->cardinality(); ++i) {
        ip_ids.insert(ip_result.roaring->getIndex(i));
    }

    // At least verify that both metrics return valid results
    ASSERT_GT(l2_ids.size(), 0) << "L2 search should return valid IDs";
    ASSERT_GT(ip_ids.size(), 0) << "Inner Product search should return valid IDs";

    // Verify distance ranges make sense
    // L2 distances should be positive
    for (size_t i = 0; i < l2_result.roaring->cardinality(); ++i) {
        ASSERT_GE(l2_result.distances[i], 0.0f) << "L2 distance should be non-negative";
    }

    // Inner product distances can be negative or positive
    bool has_valid_ip_distance = false;
    for (size_t i = 0; i < ip_result.roaring->cardinality(); ++i) {
        if (std::isfinite(ip_result.distances[i])) {
            has_valid_ip_distance = true;
            break;
        }
    }
    ASSERT_EQ(has_valid_ip_distance, true) << "Inner Product should return valid distances";
}

TEST_F(VectorSearchTest, TestIdSelectorWithEmptyRoaring) {
    auto roaring = std::make_unique<roaring::Roaring>();
    auto sel = FaissVectorIndex::roaring_to_faiss_selector(*roaring);
    for (size_t i = 0; i < 10000; ++i) {
        ASSERT_EQ(sel->is_member(i), false) << "Selector should be empty";
    }
}

// New tests: radius == 0 or < 0
TEST_F(VectorSearchTest, L2RangeSearchZeroAndNegativeRadius) {
    const int dim = 32;
    const int m = 32;
    const int n = 200;

    auto index = std::make_unique<FaissVectorIndex>();
    FaissBuildParameter params;
    params.dim = dim;
    params.max_degree = m;
    params.index_type = FaissBuildParameter::IndexType::HNSW;
    params.metric_type = FaissBuildParameter::MetricType::L2;
    index->set_build_params(params);

    // Generate data
    std::vector<float> flat_vectors;
    flat_vectors.reserve(static_cast<size_t>(n) * dim);
    for (int i = 0; i < n; ++i) {
        auto v = doris::vector_search_utils::generate_random_vector(dim);
        flat_vectors.insert(flat_vectors.end(), v.begin(), v.end());
    }
    ASSERT_EQ(index->add(n, flat_vectors.data()).ok(), true);

    // Query uses the first vector -> exact match at distance 0
    std::vector<float> query(flat_vectors.begin(), flat_vectors.begin() + dim);

    HNSWSearchParameters sp;
    sp.ef_search = 64;
    sp.is_le_or_lt = false; // Only test the ">=" branch (complement for L2)
    auto all_rows = std::make_unique<roaring::Roaring>();
    for (int i = 0; i < n; ++i) all_rows->add(i);
    sp.roaring = all_rows.get();

    // radius == 0, ">=" for L2 means all rows except those with distance <= 0.
    // With approximate HNSW range_search, the exact 0-distance self may or may not be found,
    // so the complement size could be n (if none found) or n-1 (if self found).
    IndexSearchResult res_ge0;
    ASSERT_EQ(index->range_search(query.data(), 0.0f, sp, res_ge0).ok(), true);
    ASSERT_EQ(res_ge0.distances, nullptr);
    ASSERT_EQ(res_ge0.row_ids, nullptr);
    ASSERT_TRUE(res_ge0.roaring->cardinality() == static_cast<size_t>(n) ||
                res_ge0.roaring->cardinality() == static_cast<size_t>(n - 1));

    // radius < 0 (e.g., -1.0f) -> no vector has distance <= -1, so ">=" branch should return all rows
    IndexSearchResult res_ge_neg;
    ASSERT_EQ(index->range_search(query.data(), -1.0f, sp, res_ge_neg).ok(), true);
    ASSERT_EQ(res_ge_neg.distances, nullptr);
    ASSERT_EQ(res_ge_neg.roaring->cardinality(), static_cast<size_t>(n));
}

TEST_F(VectorSearchTest, InnerProductRangeSearchZeroAndNegativeRadius) {
    const int dim = 32;
    const int m = 32;
    const int n = 200;

    auto index = std::make_unique<FaissVectorIndex>();
    FaissBuildParameter params;
    params.dim = dim;
    params.max_degree = m;
    params.index_type = FaissBuildParameter::IndexType::HNSW;
    params.metric_type = FaissBuildParameter::MetricType::IP;
    index->set_build_params(params);

    // Generate normalized vectors to keep IP in [-1, 1]
    std::vector<float> flat_vectors;
    flat_vectors.reserve(static_cast<size_t>(n) * dim);
    for (int i = 0; i < n; ++i) {
        auto v = doris::vector_search_utils::generate_random_vector(dim);
        float norm = 0.0f;
        for (float x : v) norm += x * x;
        norm = std::sqrt(norm);
        if (norm > 0)
            for (float& x : v) x /= norm;
        flat_vectors.insert(flat_vectors.end(), v.begin(), v.end());
    }
    ASSERT_EQ(index->add(n, flat_vectors.data()).ok(), true);

    std::vector<float> query(flat_vectors.begin(), flat_vectors.begin() + dim);
    // normalize query as well
    float qn = 0.0f;
    for (float x : query) qn += x * x;
    qn = std::sqrt(qn);
    if (qn > 0)
        for (float& x : query) x /= qn;

    HNSWSearchParameters sp;
    sp.ef_search = 100;
    auto origin = std::make_unique<roaring::Roaring>();
    for (int i = 0; i < n; ++i) origin->add(i);
    sp.roaring = origin.get();

    // radius == 0, is_le_or_lt = false -> inner product >= 0
    sp.is_le_or_lt = false;
    IndexSearchResult res_ge0;
    ASSERT_EQ(index->range_search(query.data(), 0.0f, sp, res_ge0).ok(), true);
    ASSERT_NE(res_ge0.distances, nullptr);
    ASSERT_GT(res_ge0.roaring->cardinality(), 0u);
    for (size_t i = 0; i < res_ge0.roaring->cardinality(); ++i) {
        ASSERT_GE(res_ge0.distances[i], -1e-6f);
    }

    // radius < 0, e.g., -1.0f -> almost all should satisfy IP >= -1
    sp.is_le_or_lt = false;
    IndexSearchResult res_gen;
    ASSERT_EQ(index->range_search(query.data(), -1.0f, sp, res_gen).ok(), true);
    ASSERT_GE(res_gen.roaring->cardinality(), static_cast<size_t>(n * 0.9));
}

} // namespace doris::vectorized