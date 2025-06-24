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

#include "faiss_vector_index.h"

#include <faiss/IndexHNSW.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstddef>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "olap/rowset/segment_v2/ann_index/ann_search_params.h"
#include "util/metrics.h"
#include "vector_index.h"
#include "vector_search_utils.h"

using namespace doris::segment_v2;

namespace doris::vectorized {

// Test saving and loading an index
TEST_F(VectorSearchTest, TestSaveAndLoad) {
    // Step 1: Create first index instance
    auto index1 = std::make_unique<FaissVectorIndex>();

    // Step 2: Set build parameters
    FaissBuildParameter params;
    params.d = 128; // Vector dimension
    params.m = 16;  // HNSW max connections
    params.index_type = FaissBuildParameter::IndexType::HNSW;
    params.quantilizer = FaissBuildParameter::Quantilizer::FLAT;
    index1->set_build_params(params);

    // Step 3: Add vectors to the index
    const int num_vectors = 100;
    std::vector<float> vectors;
    for (int i = 0; i < num_vectors; i++) {
        auto tmp = vector_search_utils::generate_random_vector(params.d);
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
    auto query_vec = vector_search_utils::generate_random_vector(params.d);
    const int top_k = 10;

    IndexSearchParameters search_params;
    IndexSearchResult search_result1;
    IndexSearchResult search_result2;

    std::ignore = index1->ann_topn_search(query_vec.data(), top_k, search_params, search_result1);

    std::ignore = index2->ann_topn_search(query_vec.data(), top_k, search_params, search_result2);

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
    const size_t iterations = 50;
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
        IndexSearchParameters search_params;
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
    const size_t iterations = 50;
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
        IndexSearchParameters search_params;
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
    size_t iterations = 25;
    for (size_t i = 0; i < iterations; ++i) {
        // Step 1: Create and build index
        auto index1 = std::make_unique<FaissVectorIndex>();

        FaissBuildParameter params;
        params.d = 64;
        params.m = 32;
        params.index_type = FaissBuildParameter::IndexType::HNSW;
        params.quantilizer = FaissBuildParameter::Quantilizer::FLAT;
        index1->set_build_params(params);

        // Add 500 vectors
        const int num_vectors = 500;
        std::vector<float> vectors;
        for (int i = 0; i < num_vectors; i++) {
            auto vec = doris::vector_search_utils::generate_random_vector(params.d);
            vectors.insert(vectors.end(), vec.begin(), vec.end());
        }

        ASSERT_EQ(index1->add(500, vectors.data()).ok(), true);

        // Save index
        ASSERT_TRUE(index1->save(_ram_dir.get()).ok());

        // Step 2: Load index
        auto index2 = std::make_unique<FaissVectorIndex>();
        ASSERT_TRUE(index2->load(_ram_dir.get()).ok());

        // Step 3: Search all vectors
        IndexSearchParameters search_params;
        IndexSearchResult search_result;

        // Search for all vectors - use a vector we know is in the index
        std::vector<float> query_vec {vectors.begin(),
                                      vectors.begin() + params.d}; // Use the first vector we added
        const int top_k = num_vectors;                             // Get all vectors

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
        std::vector<float> first_result_vec(vectors.begin() + first * params.d,
                                            vectors.begin() + (first + 1) * params.d);
        std::string query_vec_str = fmt::format("[{}]", fmt::join(query_vec, ","));
        std::string first_result_vec_str = fmt::format("[{}]", fmt::join(first_result_vec, ","));
        EXPECT_EQ(first_result_vec, query_vec) << "First result should be the query vector itself";
    }
}

TEST_F(VectorSearchTest, CompRangeSearch) {
    size_t iterations = 10;
    // std::vector<faiss::MetricType> metrics = {faiss::METRIC_L2, faiss::METRIC_INNER_PRODUCT};
    std::vector<faiss::MetricType> metrics = {faiss::METRIC_INNER_PRODUCT};
    for (size_t i = 0; i < iterations; ++i) {
        for (auto metric : metrics) {
            // Random parameters for each test iteration
            std::random_device rd;
            std::mt19937 gen(rd());
            size_t random_d = std::uniform_int_distribution<>(1, 1024)(gen);
            size_t random_m = 4 << std::uniform_int_distribution<>(1, 4)(gen);
            size_t random_n = std::uniform_int_distribution<>(500, 2000)(gen);

            // Step 1: Create and build index
            auto doris_index = std::make_unique<FaissVectorIndex>();
            FaissBuildParameter params;
            params.d = random_d;
            params.m = random_m;
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
                auto vec = vector_search_utils::generate_random_vector(params.d);
                vectors.push_back(vec);
            }

            std::unique_ptr<faiss::Index> native_index = nullptr;
            if (metric == faiss::METRIC_L2) {
                native_index = std::make_unique<faiss::IndexHNSWFlat>(params.d, params.m,
                                                                      faiss::METRIC_L2);
            } else if (metric == faiss::METRIC_INNER_PRODUCT) {
                native_index = std::make_unique<faiss::IndexHNSWFlat>(params.d, params.m,
                                                                      faiss::METRIC_INNER_PRODUCT);
            } else {
                throw std::runtime_error(fmt::format("Unsupported metric type: {}", metric));
            }

            doris::vector_search_utils::add_vectors_to_indexes_serial_mode(
                    doris_index.get(), native_index.get(), vectors);

            std::vector<float> query_vec = vectors.front();
            float radius = 0;
            radius = doris::vector_search_utils::get_radius_from_matrix(query_vec.data(), params.d,
                                                                        vectors, 0.4f, metric);

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
                std::uniform_int_distribution<>(1, 1024)(gen); // Random dimension from 32 to 256
        size_t random_m =
                4 << std::uniform_int_distribution<>(1, 4)(gen); // Random M (4, 8, 16, 32, 64)
        size_t random_n =
                std::uniform_int_distribution<>(500, 2000)(gen); // Random number of vectors
        // Step 1: Create and build index
        auto index1 = std::make_unique<FaissVectorIndex>();

        FaissBuildParameter params;
        params.d = random_d;
        params.m = random_m;
        params.index_type = FaissBuildParameter::IndexType::HNSW;
        index1->set_build_params(params);

        const int num_vectors = random_n;
        std::vector<std::vector<float>> vectors;
        for (int i = 0; i < num_vectors; i++) {
            auto vec = vector_search_utils::generate_random_vector(params.d);
            vectors.push_back(vec);
        }
        std::unique_ptr<faiss::Index> native_index =
                std::make_unique<faiss::IndexHNSWFlat>(params.d, params.m);
        doris::vector_search_utils::add_vectors_to_indexes_serial_mode(index1.get(),
                                                                       native_index.get(), vectors);

        std::vector<float> query_vec = vectors.front();

        std::vector<std::pair<size_t, float>> distances(num_vectors);
        for (int i = 0; i < num_vectors; i++) {
            double sum = 0;
            auto& vec = vectors[i];
            for (int j = 0; j < params.d; j++) {
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
    size_t iterations = 5;
    for (size_t i = 0; i < iterations; ++i) {
        // Step 1: Create and build index
        auto index1 = std::make_unique<FaissVectorIndex>();

        FaissBuildParameter params;
        params.d = 100;
        params.m = 32;
        params.index_type = FaissBuildParameter::IndexType::HNSW;
        index1->set_build_params(params);

        const int num_vectors = 1000;
        std::vector<std::vector<float>> vectors;
        for (int i = 0; i < num_vectors; i++) {
            auto vec = vector_search_utils::generate_random_vector(params.d);
            vectors.push_back(vec);
        }

        // Use a vector we know is in the index
        std::vector<float> query_vec = vectors.front();
        std::vector<std::pair<size_t, float>> distances(num_vectors);
        for (int i = 0; i < num_vectors; i++) {
            double sum = 0;
            auto& vec = vectors[i];
            for (int j = 0; j < params.d; j++) {
                accumulate(vec[j], query_vec[j], sum);
            }
            distances[i] = std::make_pair(i, finalize(sum));
        }
        std::sort(distances.begin(), distances.end(),
                  [](const auto& a, const auto& b) { return a.second < b.second; });
        // Use the median distance as the radius
        float radius = distances[num_vectors / 2].second;

        std::unique_ptr<faiss::Index> native_index =
                std::make_unique<faiss::IndexHNSWFlat>(params.d, params.m, faiss::METRIC_L2);
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

TEST_F(VectorSearchTest, RangeSearchEmptyResult) {
    for (size_t i = 0; i < 5; ++i) {
        const size_t d = 10;
        const size_t m = 32;
        const int num_vectors = 1000;
        auto index1 =
                vector_search_utils::create_doris_index(vector_search_utils::IndexType::HNSW, d, m);

        std::vector<float> vectors;
        // Create 1000 vectors and make sure their l2_distance_approximate with [1,2,3,4,5,6,7,8,9,10] is less than 100
        // [1,2,3,4,5,6,7,8,9,10]
        // [2,3,4,5,6,7,8,9,10,1]
        // [3,4,5,6,7,8,9,10,1,2]
        // ...
        for (int i = 0; i < num_vectors; i++) {
            int rowid = i;
            while (rowid >= 10) {
                rowid = rowid % 10;
            }

            std::vector<float> vec(d);
            for (int colid = 0; colid < d; colid++) {
                vec[colid] = (rowid + colid) % 10 + 1;
            }
            vectors.insert(vectors.end(), vec.begin(), vec.end());
        }
        std::unique_ptr<faiss::Index> native_index = vector_search_utils::create_native_index(
                vector_search_utils::IndexType::HNSW, d, m);
        vector_search_utils::add_vectors_to_indexes_batch_mode(index1.get(), native_index.get(),
                                                               num_vectors, vectors);

        std::vector<float> query_vec;
        for (int i = 0; i < d; i++) {
            query_vec.push_back(5.0f);
        }

        // L2 distance between [5,5,5,5,5,5,5,5,5,5] with any other vector is large than 5 and less than 250.
        // Find the min
        float radius = 5.0f;
        doris::vectorized::HNSWSearchParameters search_params;
        search_params.ef_search = 1000; // Set efSearch for better accuracy
        std::unique_ptr<roaring::Roaring> sel_rows = std::make_unique<roaring::Roaring>();
        for (size_t i = 0; i < num_vectors; ++i) {
            sel_rows->add(i);
        }
        search_params.roaring = sel_rows.get();
        auto doris_search_result = vector_search_utils::perform_doris_index_range_search(
                index1.get(), query_vec.data(), radius, search_params);
        auto native_search_result = vector_search_utils::perform_native_index_range_search(
                native_index.get(), query_vec.data(), radius);

        ASSERT_EQ(doris_search_result->roaring->cardinality(), 0);
        ASSERT_EQ(native_search_result.size(), 0);

        // Search all rows.
        doris::vectorized::HNSWSearchParameters search_params_all_rows;
        search_params_all_rows.ef_search = 1000; // Set efSearch for better accuracy
        search_params_all_rows.is_le_or_lt = true;
        search_params_all_rows.roaring = sel_rows.get();
        doris_search_result = vector_search_utils::perform_doris_index_range_search(
                index1.get(), query_vec.data(), radius, search_params_all_rows);
        ASSERT_EQ(doris_search_result->distances != nullptr, true);

        native_search_result = vector_search_utils::perform_native_index_range_search(
                native_index.get(), query_vec.data(), radius);

        // Make sure result is same
        for (size_t i = 0; i < native_search_result.size(); i++) {
            const size_t rowid = native_search_result[i].first;
            const float dis = native_search_result[i].second;
            ASSERT_EQ(doris_search_result->roaring->contains(rowid), true)
                    << "Row ID mismatch at rank " << i;
            ASSERT_FLOAT_EQ(doris_search_result->distances[i], sqrt(dis))
                    << "Distance mismatch at rank " << i;
        }

        search_params_all_rows.is_le_or_lt = false;
        roaring::Roaring ge_rows = *search_params_all_rows.roaring;
        roaring::Roaring less_rows;
        for (size_t i = 0; i < native_search_result.size(); ++i) {
            less_rows.add(native_search_result[i].first);
        }
        roaring::Roaring and_row_id = ge_rows & less_rows;
        roaring::Roaring or_row_id = ge_rows | less_rows;
        ASSERT_NEAR(and_row_id.cardinality(), 0, 1);
        ASSERT_EQ(or_row_id.cardinality(), sel_rows->cardinality());
        ASSERT_EQ(or_row_id, *sel_rows);
    }
}

TEST_F(VectorSearchTest, TestIdSelectorWithEmptyRoaring) {
    auto roaring = std::make_unique<roaring::Roaring>();
    auto sel = FaissVectorIndex::roaring_to_faiss_selector(*roaring);
    for (size_t i = 0; i < 10000; ++i) {
        ASSERT_EQ(sel->is_member(i), false) << "Selector should be empty";
    }
}

} // namespace doris::vectorized