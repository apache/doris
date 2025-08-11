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

#include "vector_search_utils.h"

#include <faiss/IndexHNSW.h>

#include <cstddef>
#include <memory>

#include "olap/rowset/segment_v2/ann_index/ann_index.h"
#include "olap/rowset/segment_v2/ann_index/ann_search_params.h"
#include "olap/rowset/segment_v2/ann_index/faiss_ann_index.h"

namespace doris::vector_search_utils {
static void accumulate(double x, double y, double& sum) {
    sum += (x - y) * (x - y);
}

static double finalize(double sum) {
    return sqrt(sum);
}

// Generate random vectors for testing
std::vector<float> generate_random_vector(int dim) {
    std::vector<float> vector(dim);
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<float> dis(-1.0f, 1.0f);

    for (int i = 0; i < dim; i++) {
        vector[i] = dis(gen);
    }
    return vector;
}

// Helper function to create and configure a Doris Vector index
std::unique_ptr<doris::segment_v2::VectorIndex> create_doris_index(IndexType index_type,
                                                                   int dimension, int m) {
    auto index = std::make_unique<doris::segment_v2::FaissVectorIndex>();
    segment_v2::FaissBuildParameter params;
    params.dim = dimension;
    params.max_degree = m;
    switch (index_type) {
    case IndexType::HNSW:
        params.index_type = segment_v2::FaissBuildParameter::IndexType::HNSW;
        break;
    default:
        throw std::invalid_argument("Unsupported index type");
    }
    index->set_build_params(params);
    return std::move(index);
}

// Helper function to create a native Faiss index
std::unique_ptr<faiss::Index> create_native_index(IndexType type, int dimension, int m) {
    std::unique_ptr<faiss::Index> index;

    switch (type) {
    case IndexType::FLAT_L2:
        index = std::make_unique<faiss::IndexFlatL2>(dimension);
        break;
    case IndexType::HNSW:
        index = std::make_unique<faiss::IndexHNSWFlat>(dimension, m, faiss::METRIC_L2);
        break;
    default:
        throw std::invalid_argument("Unsupported index type");
    }

    return index;
}

// Removed: create_native_hnsw_index_with_metric (not needed)

// Helper function to generate a batch of random vectors
std::vector<std::vector<float>> generate_test_vectors_matrix(int num_vectors, int dimension) {
    std::vector<std::vector<float>> vectors;
    vectors.reserve(num_vectors);

    for (int i = 0; i < num_vectors; i++) {
        vectors.push_back(generate_random_vector(dimension));
    }

    return vectors;
}

std::vector<float> generate_test_vectors_flatten(int num_vectors, int dimension) {
    std::vector<float> vectors;
    vectors.reserve(num_vectors * dimension);

    for (int i = 0; i < num_vectors; i++) {
        auto tmp = generate_random_vector(dimension);
        vectors.insert(vectors.end(), tmp.begin(), tmp.end());
    }

    return vectors;
}

// Helper function to add vectors to both Doris and native indexes
void add_vectors_to_indexes_serial_mode(segment_v2::VectorIndex* doris_index,
                                        faiss::Index* native_index,
                                        const std::vector<std::vector<float>>& vectors) {
    for (size_t i = 0; i < vectors.size(); i++) {
        if (doris_index) {
            auto status = doris_index->add(1, vectors[i].data());
            ASSERT_TRUE(status.ok())
                    << "Failed to add vector to Doris index: " << status.to_string();
        }
        if (native_index) {
            // Add vector to native Faiss index
            native_index->add(1, vectors[i].data());
        }
    }
}

void add_vectors_to_indexes_batch_mode(segment_v2::VectorIndex* doris_index,
                                       faiss::Index* native_index, size_t num_vectors,
                                       const std::vector<float>& flatten_vectors) {
    if (doris_index) {
        auto status = doris_index->add(num_vectors, flatten_vectors.data());
        ASSERT_TRUE(status.ok()) << "Failed to add vectors to Doris index: " << status.to_string();
    }

    if (native_index) {
        // Add vectors to native Faiss index
        native_index->add(num_vectors, flatten_vectors.data());
    }
}

// Helper function to print search results for comparison
void print_search_results(const segment_v2::IndexSearchResult& doris_results,
                          const std::vector<float>& native_distances,
                          const std::vector<faiss::idx_t>& native_indices, int query_idx) {
    std::cout << "Query vector index: " << query_idx << std::endl;

    std::cout << "Doris Index Results:" << std::endl;
    for (int i = 0; i < doris_results.roaring->cardinality(); i++) {
        std::cout << "ID: " << doris_results.roaring->getIndex(i)
                  << ", Distance: " << doris_results.distances[i] << std::endl;
    }

    std::cout << "Native Faiss Results:" << std::endl;
    for (size_t i = 0; i < native_indices.size(); i++) {
        if (native_indices[i] == -1) continue;
        std::cout << "ID: " << native_indices[i] << ", Distance: " << native_distances[i]
                  << std::endl;
    }
}

// Helper function to compare search results between Doris and native Faiss
void compare_search_results(const segment_v2::IndexSearchResult& doris_results,
                            const std::vector<float>& native_distances,
                            const std::vector<faiss::idx_t>& native_indices, float abs_error) {
    EXPECT_EQ(doris_results.roaring->cardinality(),
              std::count_if(native_indices.begin(), native_indices.end(),
                            [](faiss::idx_t id) { return id != -1; }));

    for (size_t i = 0; i < native_indices.size(); i++) {
        if (native_indices[i] == -1) continue;

        EXPECT_TRUE(doris_results.roaring->contains(native_indices[i]))
                << "ID mismatch at rank " << i;
        EXPECT_NEAR(doris_results.distances[i], native_distances[i], abs_error)
                << "Distance mismatch at rank " << i;
    }
}

// result is a vector of pairs, where each pair contains the labels and distance
// result is sorted by labels
std::vector<std::pair<int, float>> perform_native_index_range_search(faiss::Index* index,
                                                                     const float* query_vector,
                                                                     float radius) {
    std::vector<std::pair<int, float>> results;
    faiss::RangeSearchResult result(1);
    index->range_search(1, query_vector, radius * radius, &result);
    size_t begin = result.lims[0];
    size_t end = result.lims[1];
    results.reserve(end - begin);
    for (size_t j = begin; j < end; ++j) {
        results.push_back({result.labels[j], sqrt(result.distances[j])});
    }
    std::sort(results.begin(), results.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; });
    return results;
}

std::unique_ptr<doris::segment_v2::IndexSearchResult> perform_doris_index_range_search(
        segment_v2::VectorIndex* index, const float* query_vector, float radius,
        const segment_v2::IndexSearchParameters& params) {
    auto result = std::make_unique<doris::segment_v2::IndexSearchResult>();
    std::ignore = index->range_search(query_vector, radius, params, *result);
    return result;
}

float get_radius_from_flatten(const float* vector, int dim,
                              const std::vector<float>& flatten_vectors, float percentile) {
    size_t n = flatten_vectors.size() / dim;
    std::vector<std::pair<size_t, float>> distances(n);
    for (int i = 0; i < n; i++) {
        double sum = 0;
        for (int j = 0; j < dim; j++) {
            accumulate(flatten_vectors[i * dim + j], flatten_vectors[j], sum);
        }
        distances[i] = std::make_pair(i, finalize(sum));
    }
    std::sort(distances.begin(), distances.end(),
              [](const auto& a, const auto& b) { return a.second < b.second; });
    // Use the median distance as the radius
    size_t percentile_index = static_cast<size_t>(n * percentile);
    float radius = distances[percentile_index].second;

    return radius;
}

float get_radius_from_matrix(const float* vector, int dim,
                             const std::vector<std::vector<float>>& matrix_vectors,
                             float percentile,
                             faiss::MetricType metric_type /* = faiss::METRIC_L2 */) {
    size_t n = matrix_vectors.size();
    std::vector<std::pair<size_t, float>> distances(n);
    for (size_t i = 0; i < n; i++) {
        double sum = 0;
        if (metric_type == faiss::METRIC_L2) {
            for (int j = 0; j < dim; j++) {
                accumulate(matrix_vectors[i][j], vector[j], sum);
            }
            distances[i] = std::make_pair(i, finalize(sum));
        } else if (metric_type == faiss::METRIC_INNER_PRODUCT) {
            for (int j = 0; j < dim; j++) {
                sum += matrix_vectors[i][j] * vector[j];
            }
            distances[i] = std::make_pair(i, static_cast<float>(sum));
        } else {
            throw std::invalid_argument("Unsupported metric type in get_radius_from_matrix");
        }
    }
    if (metric_type == faiss::METRIC_L2) {
        std::sort(distances.begin(), distances.end(),
                  [](const auto& a, const auto& b) { return a.second < b.second; });
    } else if (metric_type == faiss::METRIC_INNER_PRODUCT) {
        std::sort(distances.begin(), distances.end(),
                  [](const auto& a, const auto& b) { return a.second > b.second; });
    }
    // Use the percentile distance as the radius
    size_t percentile_index = static_cast<size_t>(n * percentile);
    if (percentile_index >= n) percentile_index = n - 1;
    float radius = distances[percentile_index].second;

    return radius;
}

std::pair<std::unique_ptr<MockTabletIndex>, std::shared_ptr<segment_v2::AnnIndexReader>>
create_tmp_ann_index_reader(std::map<std::string, std::string> properties) {
    auto mock_tablet_index = std::make_unique<MockTabletIndex>();
    mock_tablet_index->_properties = properties;
    auto mock_index_file_reader = std::make_shared<MockIndexFileReader>();
    auto ann_reader = std::make_shared<segment_v2::AnnIndexReader>(mock_tablet_index.get(),
                                                                   mock_index_file_reader);
    return std::make_pair(std::move(mock_tablet_index), ann_reader);
}
} // namespace doris::vector_search_utils