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
#include <faiss/MetricType.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <omp.h> // Add this header for OpenMP functions

#include <cstddef>
#include <iomanip>
#include <roaring/roaring.hh>

#include "olap/vector_search/vector_search_utils.h"

namespace doris::vector_search_utils {
std::vector<float> generate_random_vector(int dim);
}

class NativeFaissTest : public ::testing::Test {
public:
    static void accumulate(double x, double y, double& sum) { sum += (x - y) * (x - y); }
    static double finalize(double sum) { return sqrt(sum); }

    // Function for brute force range search calculation
    static std::vector<std::pair<int, float>> calculate_brute_force_range_search(
            const std::vector<float>& data_vectors, const std::vector<float>& query_vector,
            float radius, int dim) {
        std::vector<std::pair<int, float>> results;
        int n = data_vectors.size() / dim;
        const double target_radius = radius * radius;

        for (int i = 0; i < n; ++i) {
            double sum = 0;
            for (int j = 0; j < dim; ++j) {
                accumulate(data_vectors[i * dim + j], query_vector[j], sum);
            }

            if (sum < target_radius && finalize(sum) < radius) {
                results.push_back({i, finalize(sum)});
            }
        }

        std::sort(results.begin(), results.end(),
                  [](const auto& a, const auto& b) { return a.first < b.first; });
        return results;
    }

    // Enum for different index types
    enum class IndexType {
        FLAT_L2,
        HNSW,
        // Add more index types as needed
    };

    // Function for index-based range search
    static std::vector<std::pair<int, float>> perform_index_range_search(
            const std::vector<float>& data_vectors, const std::vector<float>& query_vector,
            float radius, int dim, IndexType index_type) {
        int n = data_vectors.size() / dim;
        std::unique_ptr<faiss::Index> index = nullptr;

        // Create the appropriate index based on the type
        switch (index_type) {
        case IndexType::FLAT_L2:
            index = std::make_unique<faiss::IndexFlatL2>(dim);
            break;
        case IndexType::HNSW: {
            index = std::make_unique<faiss::IndexHNSWFlat>(dim, 32, faiss::METRIC_L2);
            // // Disable multi-threading to avoid memory leaks with OpenMP
            // hnsw_index->hnsw.efSearch = 32;
            // hnsw_index->hnsw.efConstruction = 40;
            //  = std::move(hnsw_index);
            break;
        }
            // Add more cases for other index types
        }

        if (!index) {
            throw std::runtime_error("Invalid index type");
        }

        // Add vectors to the index
        index->add(n, data_vectors.data());

        // Perform range search
        std::vector<std::pair<int, float>> results;
        {
            faiss::RangeSearchResult result(1);
            if (index_type == IndexType::HNSW) {
                // Use HNSW-specific search parameters
                faiss::SearchParametersHNSW search_params;
                search_params.efSearch = 100000; // Set efSearch for better accuracy
                index->range_search(1, query_vector.data(), radius * radius, &result,
                                    &search_params);
            } else {
                // Use default search parameters for other index types
                index->range_search(1, query_vector.data(), radius * radius, &result);
            }

            // Extract results
            size_t begin = result.lims[0];
            size_t end = result.lims[1];
            results.reserve(end - begin);
            for (size_t j = begin; j < end; ++j) {
                results.push_back({result.labels[j], sqrt(result.distances[j])});
            }
        }

        // Sort results by ID
        std::sort(results.begin(), results.end(),
                  [](const auto& a, const auto& b) { return a.first < b.first; });

        return results;
    }

    // Calculate precision and recall for search results
    static std::pair<double, double> calculate_precision_recall(
            const std::vector<std::pair<int, float>>& expected_results,
            const std::vector<std::pair<int, float>>& actual_results) {
        // Calculate the number of true positives (TP)
        size_t tp = 0;
        for (const auto& expected_result : expected_results) {
            for (const auto& actual_result : actual_results) {
                if (expected_result.first == actual_result.first) {
                    tp++;
                    break;
                }
            }
        }
        // Calculate the number of false positives (FP)
        size_t fp = 0;
        for (const auto& actual_result : actual_results) {
            bool found = false;
            for (const auto& expected_result : expected_results) {
                if (actual_result.first == expected_result.first) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                fp++;
            }
        }
        // Calculate the number of false negatives (FN)
        size_t fn = 0;
        for (const auto& expected_result : expected_results) {
            bool found = false;
            for (const auto& actual_result : actual_results) {
                if (expected_result.first == actual_result.first) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                fn++;
            }
        }

        // Calculate precision and recall
        double precision = (tp + fp) > 0 ? static_cast<double>(tp) / (tp + fp) : 1.0;
        double recall = (tp + fn) > 0 ? static_cast<double>(tp) / (tp + fn) : 1.0;

        return {precision, recall};
    }

    // Calculate average precision and recall across multiple iterations
    static std::pair<double, double> calculate_average_metrics(
            const std::vector<std::pair<double, double>>& metrics) {
        double total_precision = 0.0;
        double total_recall = 0.0;

        for (const auto& metric : metrics) {
            total_precision += metric.first;
            total_recall += metric.second;
        }

        size_t count = metrics.size();
        return {count > 0 ? total_precision / count : 0.0, count > 0 ? total_recall / count : 0.0};
    }
};

TEST_F(NativeFaissTest, RangeSearchBasic) {
    // Test setup
    int dim = 10;
    int n = 10;
    std::vector<float> data_vectors(n * dim);
    for (int i = 0; i < n; ++i) {
        for (int j = 0; j < dim; ++j) {
            data_vectors[i * dim + j] = static_cast<float>(i + j);
        }
    }

    // Create query vector [0, 1, 2, ..., 9]
    std::vector<float> query_vector(dim);
    for (int j = 0; j < dim; ++j) {
        query_vector[j] = static_cast<float>(j);
    }

    float radius = 15.0f;

    // Get brute force results
    auto expected_results =
            calculate_brute_force_range_search(data_vectors, query_vector, radius, dim);

    // Get index-based results using HNSW index
    auto actual_results =
            perform_index_range_search(data_vectors, query_vector, radius, dim, IndexType::HNSW);

    // Assert that brute force and index search results match
    ASSERT_EQ(expected_results.size(), actual_results.size());
    for (size_t i = 0; i < expected_results.size(); ++i) {
        ASSERT_EQ(expected_results[i].first, actual_results[i].first);
        ASSERT_EQ(expected_results[i].second, actual_results[i].second);
    }
}

TEST_F(NativeFaissTest, TestRangeSearch10000Random) {
    // Test setup
    int dim = 100;
    int n = 1000;
    std::vector<float> data_vectors;
    for (int i = 0; i < n; ++i) {
        auto random_vector = doris::vector_search_utils::generate_random_vector(dim);
        data_vectors.insert(data_vectors.end(), random_vector.begin(), random_vector.end());
    }

    // Create query vector [0, 1, 2, ..., 9]
    std::vector<float> query_vector(dim);
    for (int j = 0; j < dim; ++j) {
        query_vector[j] = static_cast<float>(j);
    }

    float radius = 15.0f;

    // Get brute force results
    auto expected_results =
            calculate_brute_force_range_search(data_vectors, query_vector, radius, dim);

    // Get index-based results using HNSW index
    auto actual_results =
            perform_index_range_search(data_vectors, query_vector, radius, dim, IndexType::HNSW);

    // Assert that brute force and index search results match
    ASSERT_NEAR(expected_results.size(), actual_results.size(), 1);
    if (expected_results.size() == actual_results.size()) {
        for (size_t i = 0; i < expected_results.size(); ++i) {
            ASSERT_EQ(expected_results[i].first, actual_results[i].first);
            ASSERT_NEAR(expected_results[i].second, actual_results[i].second, 1e-5)
                    << "Distance mismatch at rank " << i;
        }
    }
}

TEST_F(NativeFaissTest, TestRangeSearchRandomVectorsSearchMedian) {
    // Define the dimensions and vector counts to test
    std::vector<int> dimensions = {16, 64, 128, 512, 1024};
    std::vector<int> vector_counts = {10, 100, 1000, 10000};

    // Number of iterations per configuration
    const size_t iterations_per_config = 5;

    std::cout << "=== Precision/Recall Test Results ===" << std::endl;
    std::cout << "Dim\tVectors\tPrecis\tRecall" << std::endl;

    // Iterate over all combinations of dimensions and vector counts
    for (int dim : dimensions) {
        for (int n : vector_counts) {
            // Skip the largest combinations to avoid excessive memory usage
            if (dim == 1024 && n == 10000) continue;

            std::vector<std::pair<double, double>> metrics;

            for (size_t iteration = 0; iteration < iterations_per_config; iteration++) {
                // Generate random vectors
                std::vector<float> data_vectors;
                for (int i = 0; i < n; ++i) {
                    auto random_vector = doris::vector_search_utils::generate_random_vector(dim);
                    data_vectors.insert(data_vectors.end(), random_vector.begin(),
                                        random_vector.end());
                }

                // Use first vector as query
                std::vector<float> query_vector(data_vectors.begin(), data_vectors.begin() + dim);

                // Calculate distances to all vectors
                std::vector<std::pair<size_t, float>> distances(n);
                for (int i = 0; i < n; i++) {
                    double sum = 0;
                    for (int j = 0; j < dim; j++) {
                        accumulate(data_vectors[i * dim + j], query_vector[j], sum);
                    }
                    distances[i] = std::make_pair(i, finalize(sum));
                }

                // Sort distances and use median as radius
                std::sort(distances.begin(), distances.end(),
                          [](const auto& a, const auto& b) { return a.second < b.second; });
                float radius = distances[n / 2].second;

                // Get brute force results
                auto expected_results =
                        calculate_brute_force_range_search(data_vectors, query_vector, radius, dim);

                // Get HNSW index results
                auto actual_results = perform_index_range_search(data_vectors, query_vector, radius,
                                                                 dim, IndexType::HNSW);

                // Calculate precision and recall
                auto [precision, recall] =
                        calculate_precision_recall(expected_results, actual_results);
                metrics.push_back({precision, recall});

                // Validate that all returned results are within radius
                for (size_t i = 0; i < actual_results.size(); ++i) {
                    ASSERT_LE(actual_results[i].second, radius)
                            << "Distance to result " << actual_results[i].first
                            << " exceeds radius";
                }
            }

            // Calculate and print average metrics for this configuration
            auto [avg_precision, avg_recall] = calculate_average_metrics(metrics);
            std::cout << dim << "\t" << n << "\t" << std::fixed << std::setprecision(3)
                      << avg_precision << "\t" << avg_recall << std::endl;
        }
    }
}

TEST_F(NativeFaissTest, TestTopNRandomVectorSearchMedian) {
    // Define the dimensions and vector counts to test
    std::vector<int> dimensions = {16, 64, 128, 512, 1024};
    std::vector<int> vector_counts = {10, 100, 1000, 10000};
    std::vector<int> k_values = {5, 10, 20, 50, 100};

    // Number of iterations per configuration
    const size_t iterations_per_config = 3;

    std::cout << "=== TopN Search Test Results ===" << std::endl;
    std::cout << "Dim\tVectors\tK\tPrecis\tRecall" << std::endl;

    // Iterate over all combinations of dimensions, vector counts, and k values
    for (int dim : dimensions) {
        for (int n : vector_counts) {
            // Skip the largest combinations to avoid excessive memory usage
            if (dim == 1024 && n == 10000) continue;

            for (int k : k_values) {
                // Skip invalid k values (k > n)
                if (k > n) continue;

                std::vector<std::pair<double, double>> metrics;

                for (size_t iteration = 0; iteration < iterations_per_config; iteration++) {
                    // Generate random vectors
                    std::vector<float> data_vectors;
                    for (int i = 0; i < n; ++i) {
                        auto random_vector =
                                doris::vector_search_utils::generate_random_vector(dim);
                        data_vectors.insert(data_vectors.end(), random_vector.begin(),
                                            random_vector.end());
                    }

                    // Use a random vector as query
                    std::vector<float> query_vector =
                            doris::vector_search_utils::generate_random_vector(dim);

                    // Brute force search to find ground truth top-k
                    std::vector<std::pair<int, float>> all_distances(n);
                    for (int i = 0; i < n; i++) {
                        double sum = 0;
                        for (int j = 0; j < dim; j++) {
                            double diff = data_vectors[i * dim + j] - query_vector[j];
                            sum += diff * diff;
                        }
                        all_distances[i] = {i, sqrt(sum)};
                    }

                    // Sort by distance to get top-k
                    std::sort(all_distances.begin(), all_distances.end(),
                              [](const auto& a, const auto& b) { return a.second < b.second; });

                    // Take top-k as ground truth
                    std::vector<std::pair<int, float>> expected_results(all_distances.begin(),
                                                                        all_distances.begin() + k);

                    // HNSW index-based search
                    std::unique_ptr<faiss::Index> index =
                            std::make_unique<faiss::IndexHNSWFlat>(dim, 32, faiss::METRIC_L2);
                    index->add(n, data_vectors.data());

                    // Perform search
                    std::vector<float> distances(k);
                    std::vector<faiss::idx_t> indices(k);
                    faiss::SearchParametersHNSW search_params;
                    search_params.efSearch = 100000; // Set efSearch for better accuracy
                    index->search(1, query_vector.data(), k, distances.data(), indices.data());

                    // Format results
                    std::vector<std::pair<int, float>> actual_results;
                    for (int i = 0; i < k; i++) {
                        if (indices[i] != -1) { // -1 indicates not enough results
                            actual_results.push_back({indices[i], sqrt(distances[i])});
                        }
                    }

                    // Calculate precision and recall between brute force and HNSW results
                    auto [precision, recall] =
                            calculate_precision_recall(expected_results, actual_results);
                    metrics.push_back({precision, recall});
                }

                // Calculate and print average metrics for this configuration
                auto [avg_precision, avg_recall] = calculate_average_metrics(metrics);
                std::cout << dim << "\t" << n << "\t" << k << "\t" << std::fixed
                          << std::setprecision(3) << avg_precision << "\t" << avg_recall
                          << std::endl;
            }
        }
    }
}

TEST_F(NativeFaissTest, SameTypeIndexDiffObject) {
    // Test setup
    int dim = 100;
    int n = 1000;
    std::vector<float> data_vectors;
    for (int i = 0; i < n; ++i) {
        auto random_vector = doris::vector_search_utils::generate_random_vector(dim);
        data_vectors.insert(data_vectors.end(), random_vector.begin(), random_vector.end());
    }

    // Create query vector [0, 1, 2, ..., 9]
    std::vector<float> query_vector(data_vectors.begin(),
                                    data_vectors.begin() + dim); // Use the first vector we added

    std::vector<std::pair<size_t, float>> distances(n);
    for (int i = 0; i < n; i++) {
        double sum = 0;
        for (int j = 0; j < dim; j++) {
            accumulate(data_vectors[i * dim + j], query_vector[j], sum);
        }
        distances[i] = std::make_pair(i, finalize(sum));
    }
    std::sort(distances.begin(), distances.end(),
              [](const auto& a, const auto& b) { return a.second < b.second; });
    // Use the median distance as the radius
    float radius = distances[n / 2].second;

    // Get index-based results using HNSW index
    auto actual_results1 =
            perform_index_range_search(data_vectors, query_vector, radius, dim, IndexType::HNSW);

    auto actual_results2 =
            perform_index_range_search(data_vectors, query_vector, radius, dim, IndexType::HNSW);
    auto actual_results3 =
            perform_index_range_search(data_vectors, query_vector, radius, dim, IndexType::HNSW);

    // ASSERT three result is same
    ASSERT_EQ(actual_results1.size(), actual_results2.size());
    ASSERT_EQ(actual_results1.size(), actual_results3.size());
    for (size_t i = 0; i < actual_results1.size(); ++i) {
        ASSERT_EQ(actual_results1[i].first, actual_results2[i].first);
        ASSERT_EQ(actual_results1[i].first, actual_results3[i].first);
        ASSERT_FLOAT_EQ(actual_results1[i].second, actual_results2[i].second);
        ASSERT_FLOAT_EQ(actual_results1[i].second, actual_results3[i].second);
    }
}

TEST_F(NativeFaissTest, BatchInsert) {
    size_t iterations = 5;
    for (size_t i = 0; i < iterations; ++i) {
        const size_t d = 100;
        const size_t m = 32;
        const int num_vectors = 1000;

        auto index1 = doris::vector_search_utils::create_native_index(
                doris::vector_search_utils::IndexType::HNSW, d, m);

        auto index2 = doris::vector_search_utils::create_native_index(
                doris::vector_search_utils::IndexType::HNSW, d, m);

        auto flatten_vectors =
                doris::vector_search_utils::generate_test_vectors_flatten(num_vectors, d);

        doris::vector_search_utils::add_vectors_to_indexes_batch_mode(nullptr, index1.get(),
                                                                      num_vectors, flatten_vectors);
        doris::vector_search_utils::add_vectors_to_indexes_batch_mode(nullptr, index2.get(),
                                                                      num_vectors, flatten_vectors);

        // Use a random vector as query
        std::vector<float> query_vector = doris::vector_search_utils::generate_random_vector(d);
        float radius = doris::vector_search_utils::get_radius_from_flatten(query_vector.data(), d,
                                                                           flatten_vectors, 0.4);

        // Get index-based results using HNSW index
        auto res1 = doris::vector_search_utils::perform_native_index_range_search(
                index1.get(), query_vector.data(), radius);
        auto res2 = doris::vector_search_utils::perform_native_index_range_search(
                index2.get(), query_vector.data(), radius);
        // Insert vector using batch insert will make result is different.
        std::cout << "res1 size: " << res1.size() << std::endl;
        std::cout << "res2 size: " << res2.size() << std::endl;
    }
}