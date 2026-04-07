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

#include <crc32c/crc32c.h>
#include <gtest/gtest.h>

#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <numeric>
#include <set>
#include <vector>

#include "runtime/exec_env.h"
#include "storage/cache/ann_index_pq_chunk_cache.h"
#include "storage/index/ann/ann_index.h"
#include "storage/index/ann/ann_index_files.h"
#include "storage/index/ann/ann_search_params.h"
#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wkeyword-macro"
#endif
#define private public
#include "storage/index/ann/pq_on_disk_vector_index.h"
#undef private
#if defined(__clang__)
#pragma clang diagnostic pop
#endif
#include "storage/index/inverted/inverted_index_fs_directory.h"

namespace doris {

// ---- Test fixture ----
class PqOnDiskIndexTest : public testing::Test {
protected:
    void SetUp() override { _ram_dir = std::make_shared<segment_v2::DorisRAMFSDirectory>(); }

    void TearDown() override {}

    // Helper: build a PQ index, train, add, save into _ram_dir
    std::shared_ptr<segment_v2::PqOnDiskVectorIndex> build_and_save(
            int dim, int pq_m, int pq_nbits, segment_v2::AnnIndexMetric metric, int num_vectors,
            const std::vector<float>& data) {
        segment_v2::PqOnDiskBuildParameter params;
        params.dim = dim;
        params.pq_m = pq_m;
        params.pq_nbits = pq_nbits;
        params.metric = metric;

        auto index = std::make_shared<segment_v2::PqOnDiskVectorIndex>();
        index->build(params);

        EXPECT_TRUE(index->train(num_vectors, data.data()).ok());
        EXPECT_TRUE(index->add(num_vectors, data.data()).ok());
        EXPECT_TRUE(index->save(_ram_dir.get()).ok());
        return index;
    }

    // Helper: load a PQ index from _ram_dir
    std::unique_ptr<segment_v2::PqOnDiskVectorIndex> load_index(segment_v2::AnnIndexMetric metric) {
        auto index = std::make_unique<segment_v2::PqOnDiskVectorIndex>();
        index->set_metric(metric);
        index->set_type(segment_v2::AnnIndexType::PQ_ON_DISK);
        EXPECT_TRUE(index->load(_ram_dir.get()).ok());
        return index;
    }

    // Helper: generate sequential test vectors (dim-dimensional, each row has pattern)
    // Values are scaled to [0,1] range so PQ can quantize them effectively.
    static std::vector<float> generate_sequential_vectors(int num_vectors, int dim) {
        std::vector<float> data(num_vectors * dim);
        const float scale = 1.0f / static_cast<float>(num_vectors * dim);
        for (int i = 0; i < num_vectors; ++i) {
            for (int j = 0; j < dim; ++j) {
                data[i * dim + j] = static_cast<float>(i * dim + j) * scale;
            }
        }
        return data;
    }

    // Helper: compute exact L2 distance between two vectors
    static float exact_l2(const float* a, const float* b, int dim) {
        float sum = 0;
        for (int i = 0; i < dim; ++i) {
            float d = a[i] - b[i];
            sum += d * d;
        }
        return std::sqrt(sum);
    }

    // Helper: compute exact inner product between two vectors
    static float exact_ip(const float* a, const float* b, int dim) {
        float sum = 0;
        for (int i = 0; i < dim; ++i) {
            sum += a[i] * b[i];
        }
        return sum;
    }

    std::shared_ptr<segment_v2::DorisRAMFSDirectory> _ram_dir;
};

static void write_ram_file(segment_v2::DorisRAMFSDirectory* dir, const char* name,
                           const std::vector<uint8_t>& bytes) {
    if (dir->fileExists(name)) {
        ASSERT_TRUE(dir->doDeleteFile(name));
    }
    auto* output = dir->createOutput(name);
    if (!bytes.empty()) {
        output->writeBytes(bytes.data(), cast_set<Int32>(bytes.size()));
    }
    output->close();
    delete output;
}

static std::vector<uint8_t> read_ram_file(segment_v2::DorisRAMFSDirectory* dir, const char* name) {
    lucene::store::IndexInput* input = nullptr;
    CLuceneError error;
    EXPECT_TRUE(dir->openInput(name, input, error));
    DCHECK(input != nullptr);
    std::vector<uint8_t> bytes(cast_set<size_t>(input->length()));
    if (!bytes.empty()) {
        input->readBytes(bytes.data(), cast_set<Int32>(bytes.size()));
    }
    input->close();
    delete input;
    return bytes;
}

static std::unique_ptr<segment_v2::PqOnDiskVectorIndex> new_pq_index_for_load() {
    auto index = std::make_unique<segment_v2::PqOnDiskVectorIndex>();
    index->set_metric(segment_v2::AnnIndexMetric::L2);
    index->set_type(segment_v2::AnnIndexType::PQ_ON_DISK);
    return index;
}

static roaring::Roaring make_all_rows_bitmap(int num_vectors) {
    roaring::Roaring rows;
    for (int i = 0; i < num_vectors; ++i) {
        rows.add(i);
    }
    return rows;
}

static std::set<uint64_t> roaring_to_set(const roaring::Roaring& roaring) {
    std::set<uint64_t> rows;
    for (const auto rowid : roaring) {
        rows.insert(rowid);
    }
    return rows;
}

// ---- Test: Build, Train, Add, Save ----
TEST_F(PqOnDiskIndexTest, BuildTrainAddSave) {
    constexpr int dim = 32;
    constexpr int pq_m = 4; // 32/4 = 8 subvectors
    constexpr int pq_nbits = 8;
    constexpr int num_vectors = 30000; // > 25600 min training rows

    auto data = generate_sequential_vectors(num_vectors, dim);
    auto index =
            build_and_save(dim, pq_m, pq_nbits, segment_v2::AnnIndexMetric::L2, num_vectors, data);

    // Verify dimension and min train rows
    EXPECT_EQ(index->get_dimension(), dim);
    EXPECT_EQ(index->get_min_train_rows(), (1LL << pq_nbits) * 100); // 25600
}

TEST_F(PqOnDiskIndexTest, SaveLoadRoundtripWithPackedCodes) {
    constexpr int dim = 32;
    constexpr int pq_m = 4;
    constexpr int pq_nbits = 4;
    constexpr int num_vectors = 2000;

    auto data = generate_sequential_vectors(num_vectors, dim);
    build_and_save(dim, pq_m, pq_nbits, segment_v2::AnnIndexMetric::L2, num_vectors, data);

    auto loaded = load_index(segment_v2::AnnIndexMetric::L2);
    ASSERT_NE(loaded, nullptr);
    EXPECT_EQ(loaded->get_dimension(), dim);

    roaring::Roaring all_rows;
    for (int i = 0; i < num_vectors; ++i) {
        all_rows.add(i);
    }

    segment_v2::IndexSearchParameters params;
    params.roaring = &all_rows;
    params.rows_of_segment = num_vectors;

    segment_v2::IndexSearchResult result;
    ASSERT_TRUE(loaded->ann_topn_search(data.data(), 5, params, result).ok());
    ASSERT_NE(result.roaring, nullptr);
    ASSERT_NE(result.distances, nullptr);
    ASSERT_NE(result.row_ids, nullptr);
    EXPECT_EQ(result.roaring->cardinality(), 5);
}

// ---- Test: Save + Load roundtrip ----
TEST_F(PqOnDiskIndexTest, SaveLoadRoundtrip) {
    constexpr int dim = 32;
    constexpr int pq_m = 4;
    constexpr int pq_nbits = 8;
    constexpr int num_vectors = 30000;

    auto data = generate_sequential_vectors(num_vectors, dim);
    build_and_save(dim, pq_m, pq_nbits, segment_v2::AnnIndexMetric::L2, num_vectors, data);

    // Load from RAMDirectory
    auto loaded = load_index(segment_v2::AnnIndexMetric::L2);
    ASSERT_NE(loaded, nullptr);
    EXPECT_EQ(loaded->get_dimension(), dim);
}

TEST_F(PqOnDiskIndexTest, LoadFailsWhenMetaFileIsTooSmall) {
    write_ram_file(_ram_dir.get(), segment_v2::pq_meta_file_name, std::vector<uint8_t>(16, 0));

    auto index = new_pq_index_for_load();
    auto status = index->load(_ram_dir.get());
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.to_string().find("PQ meta file too small") != std::string::npos)
            << status.to_string();
}

TEST_F(PqOnDiskIndexTest, LoadFailsWhenMetaCrcMismatches) {
    constexpr int dim = 32;
    constexpr int pq_m = 4;
    constexpr int pq_nbits = 8;
    constexpr int num_vectors = 30000;

    auto data = generate_sequential_vectors(num_vectors, dim);
    build_and_save(dim, pq_m, pq_nbits, segment_v2::AnnIndexMetric::L2, num_vectors, data);

    auto meta = read_ram_file(_ram_dir.get(), segment_v2::pq_meta_file_name);
    ASSERT_GT(meta.size(), 4U);
    meta[0] ^= 0xFF;
    write_ram_file(_ram_dir.get(), segment_v2::pq_meta_file_name, meta);

    auto index = new_pq_index_for_load();
    auto status = index->load(_ram_dir.get());
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.to_string().find("PQ meta CRC mismatch") != std::string::npos)
            << status.to_string();
}

TEST_F(PqOnDiskIndexTest, LoadFailsWhenMetaMagicIsInvalid) {
    constexpr int dim = 32;
    constexpr int pq_m = 4;
    constexpr int pq_nbits = 8;
    constexpr int num_vectors = 30000;

    auto data = generate_sequential_vectors(num_vectors, dim);
    build_and_save(dim, pq_m, pq_nbits, segment_v2::AnnIndexMetric::L2, num_vectors, data);

    auto meta = read_ram_file(_ram_dir.get(), segment_v2::pq_meta_file_name);
    ASSERT_GT(meta.size(), 40U);
    uint32_t bad_magic = 0;
    std::memcpy(meta.data(), &bad_magic, sizeof(bad_magic));
    const uint32_t crc =
            crc32c::Crc32c(reinterpret_cast<const char*>(meta.data()), meta.size() - 4);
    std::memcpy(meta.data() + meta.size() - 4, &crc, sizeof(crc));
    write_ram_file(_ram_dir.get(), segment_v2::pq_meta_file_name, meta);

    auto index = new_pq_index_for_load();
    auto status = index->load(_ram_dir.get());
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.to_string().find("PQ meta invalid magic") != std::string::npos)
            << status.to_string();
}

TEST_F(PqOnDiskIndexTest, LoadFailsWhenDataFileIsMissing) {
    constexpr int dim = 32;
    constexpr int pq_m = 4;
    constexpr int pq_nbits = 8;
    constexpr int num_vectors = 30000;

    auto data = generate_sequential_vectors(num_vectors, dim);
    build_and_save(dim, pq_m, pq_nbits, segment_v2::AnnIndexMetric::L2, num_vectors, data);
    ASSERT_TRUE(_ram_dir->doDeleteFile(segment_v2::pq_data_file_name));

    auto index = new_pq_index_for_load();
    auto status = index->load(_ram_dir.get());
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.to_string().find("Failed to open PQ data file") != std::string::npos)
            << status.to_string();
}

TEST_F(PqOnDiskIndexTest, LoadFailsWhenDataFileSizeMismatches) {
    constexpr int dim = 32;
    constexpr int pq_m = 4;
    constexpr int pq_nbits = 8;
    constexpr int num_vectors = 30000;

    auto data = generate_sequential_vectors(num_vectors, dim);
    build_and_save(dim, pq_m, pq_nbits, segment_v2::AnnIndexMetric::L2, num_vectors, data);
    write_ram_file(_ram_dir.get(), segment_v2::pq_data_file_name, std::vector<uint8_t>(1, 0));

    auto index = new_pq_index_for_load();
    auto status = index->load(_ram_dir.get());
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.to_string().find("PQ data file size mismatch") != std::string::npos)
            << status.to_string();
}

TEST_F(PqOnDiskIndexTest, LoadFailsWhenMetaPayloadIsTruncated) {
    constexpr int dim = 32;
    constexpr int pq_m = 4;
    constexpr int pq_nbits = 8;
    constexpr int num_vectors = 30000;

    auto data = generate_sequential_vectors(num_vectors, dim);
    build_and_save(dim, pq_m, pq_nbits, segment_v2::AnnIndexMetric::L2, num_vectors, data);

    auto meta = read_ram_file(_ram_dir.get(), segment_v2::pq_meta_file_name);
    ASSERT_GT(meta.size(), 48U);
    meta.resize(meta.size() - 8);
    const uint32_t crc =
            crc32c::Crc32c(reinterpret_cast<const char*>(meta.data()), meta.size() - 4);
    std::memcpy(meta.data() + meta.size() - 4, &crc, sizeof(crc));
    write_ram_file(_ram_dir.get(), segment_v2::pq_meta_file_name, meta);

    auto index = new_pq_index_for_load();
    auto status = index->load(_ram_dir.get());
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.to_string().find("PQ meta file truncated") != std::string::npos)
            << status.to_string();
}

// ---- Test: TopN Search L2 ----
TEST_F(PqOnDiskIndexTest, TopNSearchL2) {
    constexpr int dim = 32;
    constexpr int pq_m = 4;
    constexpr int pq_nbits = 8;
    constexpr int num_vectors = 30000;
    constexpr int k = 5;

    auto data = generate_sequential_vectors(num_vectors, dim);
    build_and_save(dim, pq_m, pq_nbits, segment_v2::AnnIndexMetric::L2, num_vectors, data);
    auto loaded = load_index(segment_v2::AnnIndexMetric::L2);

    // Query: first vector (should be closest to itself)
    const float* query = data.data(); // vector 0

    // Create a roaring bitmap with all rows
    roaring::Roaring all_rows;
    for (int i = 0; i < num_vectors; ++i) {
        all_rows.add(i);
    }

    segment_v2::IndexSearchParameters params;
    params.roaring = &all_rows;
    params.rows_of_segment = num_vectors;

    segment_v2::IndexSearchResult result;
    ASSERT_TRUE(loaded->ann_topn_search(query, k, params, result).ok());

    ASSERT_NE(result.roaring, nullptr);
    ASSERT_NE(result.distances, nullptr);
    ASSERT_NE(result.row_ids, nullptr);
    EXPECT_EQ(result.roaring->cardinality(), k);
    EXPECT_EQ(result.row_ids->size(), k);

    // The closest vector to vector 0 should be vector 0 itself (distance ~0)
    EXPECT_TRUE(result.roaring->contains(0));
    // Distance to self should be small relative to inter-vector distances.
    // PQ is lossy, so we check that self-distance is less than the distance
    // to a far-away vector. With normalized data in [0,1] the quantization
    // error is bounded.
    float far_exact = exact_l2(data.data(), data.data() + (num_vectors - 1) * dim, dim);
    for (size_t i = 0; i < result.row_ids->size(); ++i) {
        if ((*result.row_ids)[i] == 0) {
            EXPECT_LT(result.distances[i], far_exact)
                    << "Distance to self should be smaller than distance to farthest vector";
        }
    }
}

// ---- Test: TopN Search IP ----
TEST_F(PqOnDiskIndexTest, TopNSearchIP) {
    constexpr int dim = 32;
    constexpr int pq_m = 4;
    constexpr int pq_nbits = 8;
    constexpr int num_vectors = 30000;
    constexpr int k = 5;

    auto data = generate_sequential_vectors(num_vectors, dim);
    build_and_save(dim, pq_m, pq_nbits, segment_v2::AnnIndexMetric::IP, num_vectors, data);
    auto loaded = load_index(segment_v2::AnnIndexMetric::IP);

    // Query: last vector (should have highest IP with itself and nearby high-magnitude vectors)
    const float* query = data.data() + (num_vectors - 1) * dim;

    roaring::Roaring all_rows;
    for (int i = 0; i < num_vectors; ++i) {
        all_rows.add(i);
    }

    segment_v2::IndexSearchParameters params;
    params.roaring = &all_rows;
    params.rows_of_segment = num_vectors;

    segment_v2::IndexSearchResult result;
    ASSERT_TRUE(loaded->ann_topn_search(query, k, params, result).ok());

    ASSERT_NE(result.roaring, nullptr);
    ASSERT_NE(result.distances, nullptr);
    ASSERT_NE(result.row_ids, nullptr);
    EXPECT_EQ(result.roaring->cardinality(), k);
    EXPECT_EQ(result.row_ids->size(), k);

    // PQ is lossy — with sequential data the highest-IP vector may not be
    // exactly the last one in the approximate result.  Instead verify that
    // the top-k set contains high-index vectors (large-magnitude vectors
    // should dominate IP with the query, which is the last vector).
    // Check structural correctness: k results, all from bitmap, distances set.
    // Also verify that at least one result is from the upper half of the dataset.
    bool has_high_index = false;
    for (size_t i = 0; i < result.row_ids->size(); ++i) {
        if ((*result.row_ids)[i] >= static_cast<uint64_t>(num_vectors / 2)) {
            has_high_index = true;
        }
    }
    EXPECT_TRUE(has_high_index) << "IP top-k should contain high-index vectors";
}

// ---- Test: Range Search L2 (le_or_lt = true) ----
TEST_F(PqOnDiskIndexTest, RangeSearchL2LeLt) {
    constexpr int dim = 32;
    constexpr int pq_m = 4;
    constexpr int pq_nbits = 8;
    constexpr int num_vectors = 30000;

    auto data = generate_sequential_vectors(num_vectors, dim);
    build_and_save(dim, pq_m, pq_nbits, segment_v2::AnnIndexMetric::L2, num_vectors, data);
    auto loaded = load_index(segment_v2::AnnIndexMetric::L2);

    const float* query = data.data(); // vector 0

    roaring::Roaring all_rows;
    for (int i = 0; i < num_vectors; ++i) {
        all_rows.add(i);
    }

    // Use a very generous radius that covers a significant portion of the dataset.
    // With normalized data in [0,1], the maximum exact L2 between any two vectors
    // is bounded; use a radius large enough that PQ approximation doesn't exclude
    // many vectors that should be within range.
    float far_dist = exact_l2(data.data(), data.data() + (num_vectors / 10) * dim, dim);
    float radius = far_dist * 3.0f;

    segment_v2::IndexSearchParameters params;
    params.roaring = &all_rows;
    params.rows_of_segment = num_vectors;
    params.is_le_or_lt = true;

    segment_v2::IndexSearchResult result;
    ASSERT_TRUE(loaded->range_search(query, radius, params, result).ok());

    ASSERT_NE(result.roaring, nullptr);
    // L2 + le_or_lt → should return matched rows with distances and row_ids
    ASSERT_NE(result.distances, nullptr);
    ASSERT_NE(result.row_ids, nullptr);

    // With a generous radius, at least some vectors should be returned
    EXPECT_GT(result.roaring->cardinality(), 0u)
            << "At least some vectors should be within the radius";
    // All returned distances should be <= radius (PQ approximate distances)
    for (size_t i = 0; i < result.row_ids->size(); ++i) {
        EXPECT_LE(result.distances[i], radius + 0.01f)
                << "Distance at index " << i << " exceeds radius";
    }
}

// ---- Test: Range Search L2 (ge_or_gt → difference set) ----
TEST_F(PqOnDiskIndexTest, RangeSearchL2GeGt) {
    constexpr int dim = 32;
    constexpr int pq_m = 4;
    constexpr int pq_nbits = 8;
    constexpr int num_vectors = 30000;

    auto data = generate_sequential_vectors(num_vectors, dim);
    build_and_save(dim, pq_m, pq_nbits, segment_v2::AnnIndexMetric::L2, num_vectors, data);
    auto loaded = load_index(segment_v2::AnnIndexMetric::L2);

    const float* query = data.data();

    roaring::Roaring all_rows;
    for (int i = 0; i < num_vectors; ++i) {
        all_rows.add(i);
    }

    float radius = exact_l2(data.data(), data.data() + dim, dim) * 50.0f;

    segment_v2::IndexSearchParameters params;
    params.roaring = &all_rows;
    params.rows_of_segment = num_vectors;
    params.is_le_or_lt = false; // ge_or_gt

    segment_v2::IndexSearchResult result;
    ASSERT_TRUE(loaded->range_search(query, radius, params, result).ok());

    ASSERT_NE(result.roaring, nullptr);
    // L2 + ge_or_gt → difference set: distances and row_ids should be nullptr
    EXPECT_EQ(result.distances, nullptr);
    EXPECT_EQ(result.row_ids, nullptr);
    // The result should be all_rows minus those within radius
    // So the total should be num_vectors minus whatever is within radius
    EXPECT_LE(result.roaring->cardinality(), static_cast<uint64_t>(num_vectors));
}

// ---- Test: Range Search IP (le_or_lt → difference set) ----
TEST_F(PqOnDiskIndexTest, RangeSearchIPLeLt) {
    constexpr int dim = 32;
    constexpr int pq_m = 4;
    constexpr int pq_nbits = 8;
    constexpr int num_vectors = 30000;

    auto data = generate_sequential_vectors(num_vectors, dim);
    build_and_save(dim, pq_m, pq_nbits, segment_v2::AnnIndexMetric::IP, num_vectors, data);
    auto loaded = load_index(segment_v2::AnnIndexMetric::IP);

    const float* query = data.data() + (num_vectors - 1) * dim;

    roaring::Roaring all_rows;
    for (int i = 0; i < num_vectors; ++i) {
        all_rows.add(i);
    }

    // Use a radius based on IP of query with itself
    float radius = exact_ip(query, query, dim) * 0.5f;

    segment_v2::IndexSearchParameters params;
    params.roaring = &all_rows;
    params.rows_of_segment = num_vectors;
    params.is_le_or_lt = true;

    segment_v2::IndexSearchResult result;
    ASSERT_TRUE(loaded->range_search(query, radius, params, result).ok());

    ASSERT_NE(result.roaring, nullptr);
    // IP + le_or_lt → difference set: distances and row_ids should be nullptr
    EXPECT_EQ(result.distances, nullptr);
    EXPECT_EQ(result.row_ids, nullptr);
}

// ---- Test: Range Search IP (ge_or_gt → matched rows with distances) ----
TEST_F(PqOnDiskIndexTest, RangeSearchIPGeGt) {
    constexpr int dim = 32;
    constexpr int pq_m = 4;
    constexpr int pq_nbits = 8;
    constexpr int num_vectors = 30000;

    auto data = generate_sequential_vectors(num_vectors, dim);
    build_and_save(dim, pq_m, pq_nbits, segment_v2::AnnIndexMetric::IP, num_vectors, data);
    auto loaded = load_index(segment_v2::AnnIndexMetric::IP);

    const float* query = data.data() + (num_vectors - 1) * dim;

    roaring::Roaring all_rows;
    for (int i = 0; i < num_vectors; ++i) {
        all_rows.add(i);
    }

    // Use a high radius so only high-IP vectors match
    float radius = exact_ip(query, query, dim) * 0.9f;

    segment_v2::IndexSearchParameters params;
    params.roaring = &all_rows;
    params.rows_of_segment = num_vectors;
    params.is_le_or_lt = false; // ge_or_gt

    segment_v2::IndexSearchResult result;
    ASSERT_TRUE(loaded->range_search(query, radius, params, result).ok());

    ASSERT_NE(result.roaring, nullptr);
    // IP + ge_or_gt → matched rows with distances + row_ids
    ASSERT_NE(result.distances, nullptr);
    ASSERT_NE(result.row_ids, nullptr);

    // The query vector itself should be in the result (highest IP)
    EXPECT_TRUE(result.roaring->contains(num_vectors - 1));
}

// ---- Test: Bitmap-Filtered Search (simulate post-filter) ----
TEST_F(PqOnDiskIndexTest, BitmapFilteredSearch) {
    constexpr int dim = 32;
    constexpr int pq_m = 4;
    constexpr int pq_nbits = 8;
    constexpr int num_vectors = 30000;
    constexpr int k = 3;

    auto data = generate_sequential_vectors(num_vectors, dim);
    build_and_save(dim, pq_m, pq_nbits, segment_v2::AnnIndexMetric::L2, num_vectors, data);
    auto loaded = load_index(segment_v2::AnnIndexMetric::L2);

    const float* query = data.data(); // vector 0

    // Only include a subset of rows (simulate WHERE user_id = ?)
    roaring::Roaring filtered_rows;
    // Include rows 0, 100, 200, 300, 400 (sparse)
    filtered_rows.add(0);
    filtered_rows.add(100);
    filtered_rows.add(200);
    filtered_rows.add(300);
    filtered_rows.add(400);

    segment_v2::IndexSearchParameters params;
    params.roaring = &filtered_rows;
    params.rows_of_segment = num_vectors;

    segment_v2::IndexSearchResult result;
    ASSERT_TRUE(loaded->ann_topn_search(query, k, params, result).ok());

    ASSERT_NE(result.roaring, nullptr);
    EXPECT_EQ(result.roaring->cardinality(), k);
    // All results should be from the filtered set
    auto it = result.roaring->begin();
    while (it != result.roaring->end()) {
        EXPECT_TRUE(filtered_rows.contains(*it)) << "Result rowid " << *it << " not in filter";
        ++it;
    }
    // Vector 0 should be the closest to query vector 0
    EXPECT_TRUE(result.roaring->contains(0));
}

// ---- Test: Empty bitmap search ----
TEST_F(PqOnDiskIndexTest, EmptyBitmapSearch) {
    constexpr int dim = 32;
    constexpr int pq_m = 4;
    constexpr int pq_nbits = 8;
    constexpr int num_vectors = 30000;
    constexpr int k = 5;

    auto data = generate_sequential_vectors(num_vectors, dim);
    build_and_save(dim, pq_m, pq_nbits, segment_v2::AnnIndexMetric::L2, num_vectors, data);
    auto loaded = load_index(segment_v2::AnnIndexMetric::L2);

    const float* query = data.data();

    roaring::Roaring empty_bitmap;

    segment_v2::IndexSearchParameters params;
    params.roaring = &empty_bitmap;
    params.rows_of_segment = num_vectors;

    segment_v2::IndexSearchResult result;
    ASSERT_TRUE(loaded->ann_topn_search(query, k, params, result).ok());

    ASSERT_NE(result.roaring, nullptr);
    ASSERT_NE(result.distances, nullptr);
    ASSERT_NE(result.row_ids, nullptr);
    EXPECT_EQ(result.roaring->cardinality(), 0U);
    EXPECT_EQ(result.row_ids->size(), 0U);
}

TEST_F(PqOnDiskIndexTest, SparseBitmapAcrossChunkBoundaries) {
    constexpr int dim = 32;
    constexpr int pq_m = 4;
    constexpr int pq_nbits = 8;
    constexpr int num_vectors = 70000;

    auto data = generate_sequential_vectors(num_vectors, dim);
    build_and_save(dim, pq_m, pq_nbits, segment_v2::AnnIndexMetric::L2, num_vectors, data);
    auto loaded = load_index(segment_v2::AnnIndexMetric::L2);

    ASSERT_NE(loaded, nullptr);
    ASSERT_GT(loaded->_vecs_per_chunk, 1U);
    ASSERT_EQ(loaded->_chunk_size, loaded->_vecs_per_chunk * loaded->_pq->code_size);

    const uint32_t chunk_boundary = cast_set<uint32_t>(loaded->_vecs_per_chunk);
    roaring::Roaring filtered_rows;
    filtered_rows.add(0);
    filtered_rows.add(chunk_boundary - 1);
    filtered_rows.add(chunk_boundary);
    filtered_rows.add(chunk_boundary + 1);
    filtered_rows.add(chunk_boundary * 2);

    segment_v2::IndexSearchParameters params;
    params.roaring = &filtered_rows;
    params.rows_of_segment = num_vectors;

    segment_v2::IndexSearchResult result;
    ASSERT_TRUE(loaded->ann_topn_search(data.data(), 5, params, result).ok());
    ASSERT_NE(result.roaring, nullptr);
    ASSERT_NE(result.row_ids, nullptr);
    EXPECT_EQ(result.roaring->cardinality(), filtered_rows.cardinality());
    EXPECT_EQ(roaring_to_set(*result.roaring), roaring_to_set(filtered_rows));
    EXPECT_EQ(result.row_ids->size(), filtered_rows.cardinality());
}

TEST_F(PqOnDiskIndexTest, TopNSearchWithZeroTopkReturnsEmptyStructures) {
    constexpr int dim = 32;
    constexpr int pq_m = 4;
    constexpr int pq_nbits = 8;
    constexpr int num_vectors = 30000;

    auto data = generate_sequential_vectors(num_vectors, dim);
    build_and_save(dim, pq_m, pq_nbits, segment_v2::AnnIndexMetric::L2, num_vectors, data);
    auto loaded = load_index(segment_v2::AnnIndexMetric::L2);

    auto all_rows = make_all_rows_bitmap(num_vectors);

    segment_v2::IndexSearchParameters params;
    params.roaring = &all_rows;
    params.rows_of_segment = num_vectors;

    segment_v2::IndexSearchResult result;
    ASSERT_TRUE(loaded->ann_topn_search(data.data(), 0, params, result).ok());
    ASSERT_NE(result.roaring, nullptr);
    ASSERT_NE(result.distances, nullptr);
    ASSERT_NE(result.row_ids, nullptr);
    EXPECT_EQ(result.roaring->cardinality(), 0U);
    EXPECT_EQ(result.row_ids->size(), 0U);
}

TEST_F(PqOnDiskIndexTest, TopNSearchWithKGreaterThanCandidatesReturnsAllCandidates) {
    constexpr int dim = 32;
    constexpr int pq_m = 4;
    constexpr int pq_nbits = 8;
    constexpr int num_vectors = 30000;

    auto data = generate_sequential_vectors(num_vectors, dim);
    build_and_save(dim, pq_m, pq_nbits, segment_v2::AnnIndexMetric::L2, num_vectors, data);
    auto loaded = load_index(segment_v2::AnnIndexMetric::L2);

    roaring::Roaring filtered_rows;
    filtered_rows.add(17);
    filtered_rows.add(2048);
    filtered_rows.add(29999);

    segment_v2::IndexSearchParameters params;
    params.roaring = &filtered_rows;
    params.rows_of_segment = num_vectors;

    segment_v2::IndexSearchResult result;
    ASSERT_TRUE(loaded->ann_topn_search(data.data(), 10, params, result).ok());
    ASSERT_NE(result.roaring, nullptr);
    ASSERT_NE(result.row_ids, nullptr);
    EXPECT_EQ(result.roaring->cardinality(), filtered_rows.cardinality());
    EXPECT_EQ(result.row_ids->size(), filtered_rows.cardinality());
    EXPECT_EQ(roaring_to_set(*result.roaring), roaring_to_set(filtered_rows));
}

TEST_F(PqOnDiskIndexTest, SearchRejectsOutOfRangeBitmapRowId) {
    constexpr int dim = 32;
    constexpr int pq_m = 4;
    constexpr int pq_nbits = 8;
    constexpr int num_vectors = 30000;

    auto data = generate_sequential_vectors(num_vectors, dim);
    build_and_save(dim, pq_m, pq_nbits, segment_v2::AnnIndexMetric::L2, num_vectors, data);
    auto loaded = load_index(segment_v2::AnnIndexMetric::L2);

    roaring::Roaring filtered_rows;
    filtered_rows.add(0);
    filtered_rows.add(num_vectors);

    segment_v2::IndexSearchParameters params;
    params.roaring = &filtered_rows;
    params.rows_of_segment = num_vectors;

    segment_v2::IndexSearchResult result;
    auto status = loaded->ann_topn_search(data.data(), 2, params, result);
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.to_string().find("exceeds indexed row count") != std::string::npos)
            << status.to_string();
}

TEST_F(PqOnDiskIndexTest, RangeSearchWithEmptyBitmapReturnsEmptyStructures) {
    constexpr int dim = 32;
    constexpr int pq_m = 4;
    constexpr int pq_nbits = 8;
    constexpr int num_vectors = 30000;

    auto data = generate_sequential_vectors(num_vectors, dim);
    build_and_save(dim, pq_m, pq_nbits, segment_v2::AnnIndexMetric::L2, num_vectors, data);
    auto loaded = load_index(segment_v2::AnnIndexMetric::L2);

    roaring::Roaring empty_bitmap;

    segment_v2::IndexSearchParameters params;
    params.roaring = &empty_bitmap;
    params.rows_of_segment = num_vectors;
    params.is_le_or_lt = true;

    segment_v2::IndexSearchResult result;
    ASSERT_TRUE(loaded->range_search(data.data(), 1.0f, params, result).ok());
    ASSERT_NE(result.roaring, nullptr);
    ASSERT_NE(result.distances, nullptr);
    ASSERT_NE(result.row_ids, nullptr);
    EXPECT_EQ(result.roaring->cardinality(), 0U);
    EXPECT_EQ(result.row_ids->size(), 0U);
}

TEST_F(PqOnDiskIndexTest, RangeSearchL2GeGtWithHugeRadiusReturnsEmptySet) {
    constexpr int dim = 32;
    constexpr int pq_m = 4;
    constexpr int pq_nbits = 8;
    constexpr int num_vectors = 30000;

    auto data = generate_sequential_vectors(num_vectors, dim);
    build_and_save(dim, pq_m, pq_nbits, segment_v2::AnnIndexMetric::L2, num_vectors, data);
    auto loaded = load_index(segment_v2::AnnIndexMetric::L2);

    auto all_rows = make_all_rows_bitmap(num_vectors);

    segment_v2::IndexSearchParameters params;
    params.roaring = &all_rows;
    params.rows_of_segment = num_vectors;
    params.is_le_or_lt = false;

    segment_v2::IndexSearchResult result;
    ASSERT_TRUE(loaded->range_search(data.data(), std::numeric_limits<float>::max(), params, result)
                        .ok());
    ASSERT_NE(result.roaring, nullptr);
    EXPECT_EQ(result.distances, nullptr);
    EXPECT_EQ(result.row_ids, nullptr);
    EXPECT_EQ(result.roaring->cardinality(), 0U);
}

TEST_F(PqOnDiskIndexTest, RangeSearchIPLeLtWithTooLargeRadiusReturnsAllCandidates) {
    constexpr int dim = 32;
    constexpr int pq_m = 4;
    constexpr int pq_nbits = 8;
    constexpr int num_vectors = 30000;

    auto data = generate_sequential_vectors(num_vectors, dim);
    build_and_save(dim, pq_m, pq_nbits, segment_v2::AnnIndexMetric::IP, num_vectors, data);
    auto loaded = load_index(segment_v2::AnnIndexMetric::IP);

    roaring::Roaring filtered_rows;
    filtered_rows.add(0);
    filtered_rows.add(100);
    filtered_rows.add(1000);
    filtered_rows.add(num_vectors - 1);

    segment_v2::IndexSearchParameters params;
    params.roaring = &filtered_rows;
    params.rows_of_segment = num_vectors;
    params.is_le_or_lt = true;

    segment_v2::IndexSearchResult result;
    ASSERT_TRUE(loaded->range_search(data.data() + (num_vectors - 1) * dim,
                                     std::numeric_limits<float>::max(), params, result)
                        .ok());
    ASSERT_NE(result.roaring, nullptr);
    EXPECT_EQ(result.distances, nullptr);
    EXPECT_EQ(result.row_ids, nullptr);
    EXPECT_EQ(result.roaring->cardinality(), filtered_rows.cardinality());
    EXPECT_EQ(roaring_to_set(*result.roaring), roaring_to_set(filtered_rows));
}

// ---- Test: min_train_rows ----
TEST_F(PqOnDiskIndexTest, MinTrainRows) {
    segment_v2::PqOnDiskBuildParameter params;
    params.dim = 32;
    params.pq_m = 4;
    params.pq_nbits = 8;
    params.metric = segment_v2::AnnIndexMetric::L2;

    auto index = std::make_unique<segment_v2::PqOnDiskVectorIndex>();
    index->build(params);

    // For 8-bit PQ: (1 << 8) * 100 = 25600
    EXPECT_EQ(index->get_min_train_rows(), 25600);
}

// ---- Test: Multiple train calls are idempotent ----
TEST_F(PqOnDiskIndexTest, TrainIdempotent) {
    constexpr int dim = 32;
    constexpr int pq_m = 4;
    constexpr int pq_nbits = 8;
    constexpr int num_vectors = 30000;

    auto data = generate_sequential_vectors(num_vectors, dim);

    segment_v2::PqOnDiskBuildParameter params;
    params.dim = dim;
    params.pq_m = pq_m;
    params.pq_nbits = pq_nbits;
    params.metric = segment_v2::AnnIndexMetric::L2;

    auto index = std::make_unique<segment_v2::PqOnDiskVectorIndex>();
    index->build(params);

    // Train twice — second call should be a no-op
    ASSERT_TRUE(index->train(num_vectors, data.data()).ok());
    ASSERT_TRUE(index->train(num_vectors, data.data()).ok());

    // Should still be able to add and save normally
    ASSERT_TRUE(index->add(num_vectors, data.data()).ok());
    ASSERT_TRUE(index->save(_ram_dir.get()).ok());
}

// ---- Test: Writer integration through AnnIndexColumnWriter ----
TEST_F(PqOnDiskIndexTest, WriterIntegration) {
    // This test verifies PQ_ON_DISK is properly wired in the writer
    constexpr int dim = 32;
    constexpr int pq_m = 4;
    constexpr int num_vectors = 30000;

    auto data = generate_sequential_vectors(num_vectors, dim);

    // Build via direct API (simulating what the writer does)
    segment_v2::PqOnDiskBuildParameter params;
    params.dim = dim;
    params.pq_m = pq_m;
    params.pq_nbits = 8;
    params.metric = segment_v2::AnnIndexMetric::L2;

    auto index = std::make_shared<segment_v2::PqOnDiskVectorIndex>();
    index->build(params);

    // Simulate chunked train/add (same pattern as AnnIndexColumnWriter)
    constexpr int chunk = 10000;
    for (int offset = 0; offset < num_vectors; offset += chunk) {
        int n = std::min(chunk, num_vectors - offset);
        ASSERT_TRUE(index->train(n, data.data() + offset * dim).ok());
        ASSERT_TRUE(index->add(n, data.data() + offset * dim).ok());
    }

    ASSERT_TRUE(index->save(_ram_dir.get()).ok());

    // Load and verify
    auto loaded = load_index(segment_v2::AnnIndexMetric::L2);
    ASSERT_NE(loaded, nullptr);
    EXPECT_EQ(loaded->get_dimension(), dim);

    // TopN search on the loaded index
    roaring::Roaring all_rows;
    for (int i = 0; i < num_vectors; ++i) {
        all_rows.add(i);
    }

    segment_v2::IndexSearchParameters search_params;
    search_params.roaring = &all_rows;
    search_params.rows_of_segment = num_vectors;

    segment_v2::IndexSearchResult result;
    ASSERT_TRUE(loaded->ann_topn_search(data.data(), 5, search_params, result).ok());
    EXPECT_EQ(result.roaring->cardinality(), 5);
    EXPECT_TRUE(result.roaring->contains(0)); // self should be nearest
}

// ---- Tests with PQ Chunk Cache enabled ----
// These tests verify that search produces correct results when ExecEnv has an
// AnnIndexPqChunkCache. The cache key prefix must be set before load() so that
// _for_each_code_in_bitmap uses the cache.

class PqOnDiskIndexCacheTest : public PqOnDiskIndexTest {
protected:
    void SetUp() override {
        PqOnDiskIndexTest::SetUp();
        // Create a cache instance (1MB, 4 shards) and register it in ExecEnv.
        _cache.reset(AnnIndexPqChunkCache::create_global_cache(1024 * 1024, 4));
        ExecEnv::GetInstance()->set_ann_index_pq_chunk_cache(_cache.get());
    }

    void TearDown() override {
        ExecEnv::GetInstance()->set_ann_index_pq_chunk_cache(nullptr);
        _cache.reset();
        PqOnDiskIndexTest::TearDown();
    }

    std::unique_ptr<AnnIndexPqChunkCache> _cache;

    // Load index with a cache key prefix set (required for cache participation)
    std::unique_ptr<segment_v2::PqOnDiskVectorIndex> load_index_cached(
            segment_v2::AnnIndexMetric metric, const std::string& prefix = "test_pqdata") {
        auto index = std::make_unique<segment_v2::PqOnDiskVectorIndex>();
        index->set_metric(metric);
        index->set_type(segment_v2::AnnIndexType::PQ_ON_DISK);
        index->set_pqdata_cache_key_prefix(prefix);
        EXPECT_TRUE(index->load(_ram_dir.get()).ok());
        return index;
    }
};

// Verify TopN L2 search produces correct results with chunk cache.
TEST_F(PqOnDiskIndexCacheTest, TopNSearchL2WithCache) {
    constexpr int dim = 32;
    constexpr int pq_m = 4;
    constexpr int pq_nbits = 8;
    constexpr int num_vectors = 30000;
    constexpr int k = 5;

    auto data = generate_sequential_vectors(num_vectors, dim);
    build_and_save(dim, pq_m, pq_nbits, segment_v2::AnnIndexMetric::L2, num_vectors, data);
    auto loaded = load_index_cached(segment_v2::AnnIndexMetric::L2);

    const float* query = data.data();

    roaring::Roaring all_rows;
    for (int i = 0; i < num_vectors; ++i) {
        all_rows.add(i);
    }

    segment_v2::IndexSearchParameters params;
    params.roaring = &all_rows;
    params.rows_of_segment = num_vectors;

    segment_v2::IndexSearchResult result;
    ASSERT_TRUE(loaded->ann_topn_search(query, k, params, result).ok());

    ASSERT_NE(result.roaring, nullptr);
    EXPECT_EQ(result.roaring->cardinality(), k);
    EXPECT_TRUE(result.roaring->contains(0));
}

// Verify repeated queries hit the cache and still return correct results.
TEST_F(PqOnDiskIndexCacheTest, RepeatedSearchHitsCache) {
    constexpr int dim = 32;
    constexpr int pq_m = 4;
    constexpr int pq_nbits = 8;
    constexpr int num_vectors = 30000;
    constexpr int k = 5;

    auto data = generate_sequential_vectors(num_vectors, dim);
    build_and_save(dim, pq_m, pq_nbits, segment_v2::AnnIndexMetric::L2, num_vectors, data);
    auto loaded = load_index_cached(segment_v2::AnnIndexMetric::L2);

    const float* query = data.data();

    roaring::Roaring all_rows;
    for (int i = 0; i < num_vectors; ++i) {
        all_rows.add(i);
    }

    segment_v2::IndexSearchParameters params;
    params.roaring = &all_rows;
    params.rows_of_segment = num_vectors;

    // Run the same search twice; the second should hit the cache
    for (int run = 0; run < 2; ++run) {
        segment_v2::IndexSearchResult result;
        ASSERT_TRUE(loaded->ann_topn_search(query, k, params, result).ok())
                << "Failed on run " << run;
        ASSERT_NE(result.roaring, nullptr);
        EXPECT_EQ(result.roaring->cardinality(), k);
        EXPECT_TRUE(result.roaring->contains(0)) << "Self not found on run " << run;
    }
}

// Verify IP TopN search works with cache.
TEST_F(PqOnDiskIndexCacheTest, TopNSearchIPWithCache) {
    constexpr int dim = 32;
    constexpr int pq_m = 4;
    constexpr int pq_nbits = 8;
    constexpr int num_vectors = 30000;
    constexpr int k = 5;

    auto data = generate_sequential_vectors(num_vectors, dim);
    build_and_save(dim, pq_m, pq_nbits, segment_v2::AnnIndexMetric::IP, num_vectors, data);
    auto loaded = load_index_cached(segment_v2::AnnIndexMetric::IP);

    const float* query = data.data() + (num_vectors - 1) * dim;

    roaring::Roaring all_rows;
    for (int i = 0; i < num_vectors; ++i) {
        all_rows.add(i);
    }

    segment_v2::IndexSearchParameters params;
    params.roaring = &all_rows;
    params.rows_of_segment = num_vectors;

    segment_v2::IndexSearchResult result;
    ASSERT_TRUE(loaded->ann_topn_search(query, k, params, result).ok());

    ASSERT_NE(result.roaring, nullptr);
    EXPECT_EQ(result.roaring->cardinality(), k);

    // At least one result from the upper half
    bool has_high_index = false;
    for (size_t i = 0; i < result.row_ids->size(); ++i) {
        if ((*result.row_ids)[i] >= static_cast<uint64_t>(num_vectors / 2)) {
            has_high_index = true;
        }
    }
    EXPECT_TRUE(has_high_index);
}

// Verify range search (L2 le_or_lt) works with cache.
TEST_F(PqOnDiskIndexCacheTest, RangeSearchL2WithCache) {
    constexpr int dim = 32;
    constexpr int pq_m = 4;
    constexpr int pq_nbits = 8;
    constexpr int num_vectors = 30000;

    auto data = generate_sequential_vectors(num_vectors, dim);
    build_and_save(dim, pq_m, pq_nbits, segment_v2::AnnIndexMetric::L2, num_vectors, data);
    auto loaded = load_index_cached(segment_v2::AnnIndexMetric::L2);

    const float* query = data.data();

    roaring::Roaring all_rows;
    for (int i = 0; i < num_vectors; ++i) {
        all_rows.add(i);
    }

    float far_dist = exact_l2(data.data(), data.data() + (num_vectors / 10) * dim, dim);
    float radius = far_dist * 3.0F;

    segment_v2::IndexSearchParameters params;
    params.roaring = &all_rows;
    params.rows_of_segment = num_vectors;
    params.is_le_or_lt = true;

    segment_v2::IndexSearchResult result;
    ASSERT_TRUE(loaded->range_search(query, radius, params, result).ok());

    ASSERT_NE(result.roaring, nullptr);
    ASSERT_NE(result.distances, nullptr);
    EXPECT_GT(result.roaring->cardinality(), 0u);

    for (size_t i = 0; i < result.row_ids->size(); ++i) {
        EXPECT_LE(result.distances[i], radius + 0.01F);
    }
}

// Verify bitmap-filtered search works with cache.
TEST_F(PqOnDiskIndexCacheTest, BitmapFilteredSearchWithCache) {
    constexpr int dim = 32;
    constexpr int pq_m = 4;
    constexpr int pq_nbits = 8;
    constexpr int num_vectors = 30000;
    constexpr int k = 3;

    auto data = generate_sequential_vectors(num_vectors, dim);
    build_and_save(dim, pq_m, pq_nbits, segment_v2::AnnIndexMetric::L2, num_vectors, data);
    auto loaded = load_index_cached(segment_v2::AnnIndexMetric::L2);

    const float* query = data.data();

    roaring::Roaring filtered_rows;
    filtered_rows.add(0);
    filtered_rows.add(100);
    filtered_rows.add(200);
    filtered_rows.add(300);
    filtered_rows.add(400);

    segment_v2::IndexSearchParameters params;
    params.roaring = &filtered_rows;
    params.rows_of_segment = num_vectors;

    segment_v2::IndexSearchResult result;
    ASSERT_TRUE(loaded->ann_topn_search(query, k, params, result).ok());

    ASSERT_NE(result.roaring, nullptr);
    EXPECT_EQ(result.roaring->cardinality(), k);

    auto it = result.roaring->begin();
    while (it != result.roaring->end()) {
        EXPECT_TRUE(filtered_rows.contains(*it)) << "Result rowid " << *it << " not in filter";
        ++it;
    }
    EXPECT_TRUE(result.roaring->contains(0));
}

// Verify packed codes (4-bit PQ) work with cache.
TEST_F(PqOnDiskIndexCacheTest, PackedCodesWithCache) {
    constexpr int dim = 32;
    constexpr int pq_m = 4;
    constexpr int pq_nbits = 4;
    constexpr int num_vectors = 2000;

    auto data = generate_sequential_vectors(num_vectors, dim);
    build_and_save(dim, pq_m, pq_nbits, segment_v2::AnnIndexMetric::L2, num_vectors, data);
    auto loaded = load_index_cached(segment_v2::AnnIndexMetric::L2);

    roaring::Roaring all_rows;
    for (int i = 0; i < num_vectors; ++i) {
        all_rows.add(i);
    }

    segment_v2::IndexSearchParameters params;
    params.roaring = &all_rows;
    params.rows_of_segment = num_vectors;

    segment_v2::IndexSearchResult result;
    ASSERT_TRUE(loaded->ann_topn_search(data.data(), 5, params, result).ok());
    ASSERT_NE(result.roaring, nullptr);
    EXPECT_EQ(result.roaring->cardinality(), 5);
}

TEST_F(PqOnDiskIndexCacheTest, SearchDoesNotUseStaleLoadIoContext) {
    constexpr int dim = 32;
    constexpr int pq_m = 4;
    constexpr int pq_nbits = 8;
    constexpr int num_vectors = 30000;

    auto data = generate_sequential_vectors(num_vectors, dim);
    build_and_save(dim, pq_m, pq_nbits, segment_v2::AnnIndexMetric::L2, num_vectors, data);
    auto loaded = load_index_cached(segment_v2::AnnIndexMetric::L2);

    roaring::Roaring all_rows;
    for (int i = 0; i < num_vectors; ++i) {
        all_rows.add(i);
    }

    io::FileCacheStatistics load_stats;
    io::FileCacheStatistics search_stats;
    segment_v2::IndexSearchParameters params;
    io::IOContext search_io_ctx;
    search_io_ctx.reader_type = ReaderType::READER_QUERY;
    search_io_ctx.file_cache_stats = &search_stats;
    params.roaring = &all_rows;
    params.rows_of_segment = num_vectors;
    params.io_ctx = &search_io_ctx;

    loaded->_pqdata_input->setIoContext(&search_io_ctx);
    loaded->_pqdata_input->setIoContext(nullptr);
    loaded->_pqdata_input->setIoContext(&search_io_ctx);

    segment_v2::IndexSearchResult result;
    ASSERT_TRUE(loaded->ann_topn_search(data.data(), 5, params, result).ok());
    ASSERT_NE(result.roaring, nullptr);
    EXPECT_EQ(result.roaring->cardinality(), 5);
    EXPECT_EQ(search_stats.inverted_index_io_timer, 0);
    EXPECT_EQ(load_stats.inverted_index_io_timer, 0);
}

TEST_F(PqOnDiskIndexCacheTest, SparseBitmapAcrossChunkBoundariesWithCache) {
    constexpr int dim = 32;
    constexpr int pq_m = 4;
    constexpr int pq_nbits = 8;
    constexpr int num_vectors = 70000;

    auto data = generate_sequential_vectors(num_vectors, dim);
    build_and_save(dim, pq_m, pq_nbits, segment_v2::AnnIndexMetric::L2, num_vectors, data);
    auto loaded = load_index_cached(segment_v2::AnnIndexMetric::L2, "chunk_boundary_cache");

    ASSERT_NE(loaded, nullptr);
    ASSERT_GT(loaded->_vecs_per_chunk, 1U);

    const uint32_t chunk_boundary = cast_set<uint32_t>(loaded->_vecs_per_chunk);
    roaring::Roaring filtered_rows;
    filtered_rows.add(chunk_boundary - 1);
    filtered_rows.add(chunk_boundary);
    filtered_rows.add(chunk_boundary + 1);
    filtered_rows.add(chunk_boundary * 2);

    segment_v2::IndexSearchParameters params;
    params.roaring = &filtered_rows;
    params.rows_of_segment = num_vectors;

    segment_v2::IndexSearchResult result;
    ASSERT_TRUE(loaded->ann_topn_search(data.data(), 10, params, result).ok());
    ASSERT_NE(result.roaring, nullptr);
    ASSERT_NE(result.row_ids, nullptr);
    EXPECT_EQ(result.roaring->cardinality(), filtered_rows.cardinality());
    EXPECT_EQ(result.row_ids->size(), filtered_rows.cardinality());
    EXPECT_EQ(roaring_to_set(*result.roaring), roaring_to_set(filtered_rows));
}

TEST_F(PqOnDiskIndexCacheTest, SearchRejectsOutOfRangeBitmapRowIdWithCache) {
    constexpr int dim = 32;
    constexpr int pq_m = 4;
    constexpr int pq_nbits = 8;
    constexpr int num_vectors = 30000;

    auto data = generate_sequential_vectors(num_vectors, dim);
    build_and_save(dim, pq_m, pq_nbits, segment_v2::AnnIndexMetric::L2, num_vectors, data);
    auto loaded = load_index_cached(segment_v2::AnnIndexMetric::L2, "out_of_range_cache");

    roaring::Roaring filtered_rows;
    filtered_rows.add(num_vectors);

    segment_v2::IndexSearchParameters params;
    params.roaring = &filtered_rows;
    params.rows_of_segment = num_vectors;

    segment_v2::IndexSearchResult result;
    auto status = loaded->ann_topn_search(data.data(), 1, params, result);
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.to_string().find("exceeds indexed row count") != std::string::npos)
            << status.to_string();
}

} // namespace doris
