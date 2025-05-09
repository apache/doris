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

// Add CLucene RAM Directory header
#include <CLucene/store/RAMDirectory.h>

namespace doris::segment_v2 {
// Generate random vectors for testing
static std::vector<float> generate_random_vector(int dim) {
    std::vector<float> vector(dim);
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<float> dis(-1.0f, 1.0f);

    for (int i = 0; i < dim; i++) {
        vector[i] = dis(gen);
    }
    return vector;
}

class FaissVectorIndexTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create CLucene RAM directory instead of mock
        _ram_dir = std::make_shared<lucene::store::RAMDirectory>();

        // Optional: Create test file to simulate index presence
        auto output = _ram_dir->createOutput("index_file");
        // Write some dummy data
        const char* dummy_data = "dummy data";
        output->writeBytes((const uint8_t*)dummy_data, strlen(dummy_data));
        output->close();
        delete output; // CLucene requires manual delete
    }
    std::shared_ptr<lucene::store::RAMDirectory> _ram_dir;
};

// Test saving and loading an index
TEST_F(FaissVectorIndexTest, TestSaveAndLoad) {
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
    for (int i = 0; i < num_vectors; i++) {
        auto vec = generate_random_vector(params.d);
        auto status = index1->add(1, vec.data());
        EXPECT_TRUE(status.ok()) << "Failed to add vector: " << status.to_string();
    }

    // Step 4: Save the index
    auto save_status = index1->save(_ram_dir.get());
    ASSERT_TRUE(save_status.ok()) << "Failed to save index: " << save_status.to_string();

    // Step 5: Create a new index instance
    auto index2 = std::make_unique<FaissVectorIndex>();

    // Step 6: Load the index
    auto load_status = index2->load(_ram_dir.get());
    ASSERT_TRUE(load_status.ok()) << "Failed to load index: " << load_status.to_string();

    // Step 7: Verify the loaded index works by searching
    auto query_vec = generate_random_vector(params.d);
    const int top_k = 10;

    SearchParameters search_params;
    SearchResult search_result;

    auto search_status = index2->search(query_vec.data(), top_k, search_params, search_result);
    ASSERT_TRUE(search_status.ok())
            << "Failed to search loaded index: " << search_status.to_string();

    // Verify search results - at minimum, we should have results
    ASSERT_GT(search_result.row_ids->cardinality(), 0) << "Search returned no results";
}

TEST_F(FaissVectorIndexTest, TestUpdateRoaring) {
    // Create a roaring bitmap
    roaring::Roaring roaring_bitmap;
    // Create some dummy labels
    const size_t n = 5;
    faiss::idx_t labels[n] = {1, 2, 3, 4, 5};

    // Call the update_roaring function
    FaissVectorIndex::update_roaring(labels, n, roaring_bitmap);

    EXPECT_EQ(roaring_bitmap.cardinality(), n) << "Roaring bitmap size mismatch";

    for (size_t i = 0; i < n; ++i) {
        EXPECT_TRUE(roaring_bitmap.contains(labels[i])) << "Label " << labels[i] << " not found";
    }
}

TEST_F(FaissVectorIndexTest, CompareResultWithNativeFaiss1) {
    // Step1: Create and build doris faiss index & faiss native index
    auto doris_index = std::make_unique<FaissVectorIndex>();

    FaissBuildParameter params;
    params.d = 64; // Vector dimension
    params.m = 16; // HNSW max connections
    params.index_type = FaissBuildParameter::IndexType::HNSW;
    params.quantilizer = FaissBuildParameter::Quantilizer::FLAT;
    doris_index->set_build_params(params);

    // Create a native Faiss index with ID mapping
    faiss::IndexHNSWFlat native_index(params.d, params.m);

    // Step2: Create random vectors
    const int num_vectors = 200;
    std::vector<std::vector<float>> vectors;

    for (int i = 0; i < num_vectors; i++) {
        vectors.push_back(generate_random_vector(params.d));
    }

    // Step3: Add vectors to both indexes
    for (int i = 0; i < num_vectors; i++) {
        // Add to Doris index with row_id = i
        auto status = doris_index->add(1, vectors[i].data());
        ASSERT_TRUE(status.ok()) << "Failed to add vector to Doris index: " << status.to_string();

        // Add to native Faiss index with the same ID
        native_index.add(1, vectors[i].data());
    }

    // Step4: Search for a vector in both indexes
    // Use one of the vectors we added as the query
    int query_idx = num_vectors / 2;
    const float* query_vec = vectors[query_idx].data();
    const int top_k = 10;

    // Search in Doris index
    SearchParameters search_params;
    SearchResult doris_results;
    auto search_status = doris_index->search(query_vec, top_k, search_params, doris_results);
    ASSERT_TRUE(search_status.ok());

    // Search in native Faiss index
    std::vector<float> native_distances(top_k);
    std::vector<faiss::idx_t> native_indices(top_k);
    native_index.search(1, query_vec, top_k, native_distances.data(), native_indices.data());

    // Step5: Compare results
    std::cout << "Query vector index: " << query_idx << std::endl;

    std::cout << "Doris Index Results:" << std::endl;
    for (int i = 0; i < doris_results.row_ids->cardinality(); i++) {
        std::cout << "ID: " << doris_results.row_ids->getIndex(i)
                  << ", Distance: " << doris_results.distances[i] << std::endl;
    }

    std::cout << "Native Faiss Results:" << std::endl;
    for (int i = 0; i < top_k; i++) {
        std::cout << "ID: " << native_indices[i] << ", Distance: " << native_distances[i]
                  << std::endl;
    }

    EXPECT_EQ(doris_results.row_ids->cardinality(), native_indices.size());

    for (int i = 0; i < native_indices.size(); i++) {
        EXPECT_EQ(doris_results.row_ids->contains(native_indices[i]), true)
                << "ID mismatch at rank " << i;
        EXPECT_NEAR(doris_results.distances[i], native_distances[i], 1e-5)
                << "Distance mismatch at rank " << i;
    }
}

TEST_F(FaissVectorIndexTest, CompareResultWithNativeFaiss2) {
    // Step1: Create and build doris faiss index & faiss native index
    auto doris_index = std::make_unique<FaissVectorIndex>();

    FaissBuildParameter params;
    params.d = 64; // Vector dimension
    params.m = 16; // HNSW max connections
    params.index_type = FaissBuildParameter::IndexType::HNSW;
    params.quantilizer = FaissBuildParameter::Quantilizer::FLAT;
    doris_index->set_build_params(params);

    // Create a native Faiss index with ID mapping
    faiss::IndexHNSWFlat native_index(params.d, params.m);

    // Step2: Create random vectors
    const int num_vectors = 500;
    std::vector<std::vector<float>> vectors;

    for (int i = 0; i < num_vectors; i++) {
        vectors.push_back(generate_random_vector(params.d));
    }

    // Step3: Add vectors to both indexes
    for (int i = 0; i < num_vectors; i++) {
        // Add to Doris index with row_id = i
        auto status = doris_index->add(1, vectors[i].data());
        ASSERT_TRUE(status.ok()) << "Failed to add vector to Doris index: " << status.to_string();

        // Add to native Faiss index with the same ID
        native_index.add(1, vectors[i].data());
    }

    // Step4: Search for a vector in both indexes
    // Use one of the vectors we added as the query
    int query_idx = num_vectors / 2;
    const float* query_vec = vectors[query_idx].data();
    const int top_k = num_vectors;

    // Search in Doris index
    SearchParameters search_params;
    SearchResult doris_results;
    auto search_status = doris_index->search(query_vec, top_k, search_params, doris_results);
    ASSERT_TRUE(search_status.ok());

    // Search in native Faiss index
    std::vector<float> native_distances(top_k, -1);
    std::vector<faiss::idx_t> native_indices(top_k, -1);

    native_index.search(1, query_vec, top_k, native_distances.data(), native_indices.data());

    // Step5: Compare results
    size_t cardinality_of_native_index = std::count_if(native_indices.begin(), native_indices.end(),
                                                       [](faiss::idx_t id) { return id != -1; });
    EXPECT_EQ(doris_results.row_ids->cardinality(), cardinality_of_native_index);

    for (int i = 0; i < native_indices.size(); i++) {
        if (native_indices[i] == -1) {
            continue; // Skip invalid indices
        }
        EXPECT_EQ(doris_results.row_ids->contains(native_indices[i]), true)
                << "ID mismatch at rank " << i;
        EXPECT_EQ(doris_results.distances[i], native_distances[i])
                << "Distance mismatch at rank " << i;
    }
}

TEST_F(FaissVectorIndexTest, TestSearchAllVectors) {
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
    for (int i = 0; i < num_vectors * params.m; i++) {
        auto vec = generate_random_vector(params.d);
        vectors.insert(vectors.end(), vec.begin(), vec.end());
    }

    ASSERT_TRUE(index1->add(500, vectors.data()).ok());

    // Save index
    ASSERT_TRUE(index1->save(_ram_dir.get()).ok());

    // Step 2: Load index
    auto index2 = std::make_unique<FaissVectorIndex>();
    ASSERT_TRUE(index2->load(_ram_dir.get()).ok());

    // Step 3: Search all vectors
    SearchParameters search_params;
    SearchResult search_result;

    // Search for all vectors - use a vector we know is in the index
    std::vector<float> query_vec {vectors.begin(),
                                  vectors.begin() + params.d}; // Use the first vector we added
    const int top_k = num_vectors;                             // Get all vectors

    ASSERT_TRUE(index2->search(query_vec.data(), top_k, search_params, search_result).ok());
    std::cout << "Search returned " << search_result.row_ids->cardinality() << " results\n";
    // Step 4: Verify we got all vectors back
    // Note: In practical ANN search with approximate algorithms like HNSW,
    // we might not get exactly all vectors due to the nature of approximate search.
    // So we verify we got a reasonable number back.
    EXPECT_GE(search_result.row_ids->cardinality(), num_vectors * 0.60)
            << "Expected to find at least 60% of all vectors";

    // Also verify the first result is the query vector itself (it should be an exact match)
    ASSERT_FALSE(search_result.row_ids->isEmpty()) << "Search result should not be empty";
    size_t first = search_result.row_ids->getIndex(0);
    std::cout << "First result ID: " << first << "\n";
    std::vector<float> first_result_vec(vectors.begin() + first * params.d,
                                        vectors.begin() + (first + 1) * params.d);
    std::string query_vec_str = fmt::format("[{}]", fmt::join(query_vec, ","));
    std::string first_result_vec_str = fmt::format("[{}]", fmt::join(first_result_vec, ","));
    std::cout << query_vec_str << "\n" << first_result_vec_str << "\n";
    EXPECT_EQ(first_result_vec, query_vec) << "First result should be the query vector itself";
}

} // namespace doris::segment_v2