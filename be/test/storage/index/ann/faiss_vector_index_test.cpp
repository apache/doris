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

#include <faiss/IndexFlat.h>
#include <faiss/IndexHNSW.h>
#include <faiss/IndexIDMap.h>
#include <faiss/IndexIVFFlat.h>
#include <faiss/impl/io.h>
#include <faiss/index_io.h>
#include <faiss/invlists/PreadInvertedLists.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <omp.h>

#include <algorithm>
#include <atomic>
#include <barrier>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstring>
#include <future>
#include <limits>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "common/config.h"
#include "common/metrics/doris_metrics.h"
#include "faiss/impl/FaissException.h"
#include "storage/index/ann/ann_index.h"
#include "storage/index/ann/ann_index_files.h"
#include "storage/index/ann/ann_search_params.h"
#include "storage/index/ann/faiss_ann_index.h"
// metrics.h not used directly here
#include "storage/cache/ann_index_ivf_list_cache.h"
#include "storage/index/ann/vector_search_utils.h"
#include "util/cpu_info.h"
#include "util/defer_op.h"

using namespace doris::segment_v2;

namespace doris {

// Verify native IVF labels and HNSW IDMap through serialization and query-time filtering.
TEST_F(VectorSearchTest, SparseLabelsSurviveReloadAndDeleteBitmapFiltering) {
    const bool previous = config::enable_ann_index_empty_array;
    config::enable_ann_index_empty_array = false;
    Defer restore_config([&] { config::enable_ann_index_empty_array = previous; });
    // FAISS serialization and readers do not consult the index-writer feature flag.
    AnnIndexIVFListCache::create_global_cache(64 * 1024 * 1024);
    Defer destroy_cache([] { AnnIndexIVFListCache::destroy_global_cache(); });
    for (auto type : {FaissBuildParameter::IndexType::HNSW, FaissBuildParameter::IndexType::IVF,
                      FaissBuildParameter::IndexType::IVF_ON_DISK}) {
        for (auto metric :
             {FaissBuildParameter::MetricType::L2, FaissBuildParameter::MetricType::IP}) {
            SCOPED_TRACE(static_cast<int>(type));
            SCOPED_TRACE(static_cast<int>(metric));
            const bool l2 = metric == FaissBuildParameter::MetricType::L2;
            FaissBuildParameter build;
            build.dim = 2;
            build.max_degree = 16;
            build.ivf_nlist = 1;
            build.index_type = type;
            build.metric_type = metric;
            FaissVectorIndex writer;
            writer.build(build);
            std::vector<float> vectors;
            std::vector<Int64> labels;
            constexpr int rows = 193;
            for (int i = 0; i < 64; ++i) {
                vectors.insert(vectors.end(), {static_cast<float>(i), 1.0f});
                labels.push_back(3 * i + 1);
            }
            ASSERT_TRUE(writer.train(64, vectors.data()).ok());
            ASSERT_TRUE(writer.add_with_ids(64, vectors.data(), labels.data()).ok());
            auto dir = std::make_shared<lucene::store::RAMDirectory>();
            ASSERT_TRUE(writer.save(dir.get()).ok());
            EXPECT_TRUE(dir->fileExists(faiss_index_fila_name));
            EXPECT_FALSE(dir->fileExists("ann.sparse.v1.faiss"));

            // A native FAISS reader must understand the file without a Doris bitmap header.
            faiss::VectorIOReader native_bytes;
            auto input = std::unique_ptr<lucene::store::IndexInput>(
                    dir->openInput(faiss_index_fila_name));
            native_bytes.data.resize(cast_set<size_t>(input->length()));
            input->readBytes(native_bytes.data.data(), cast_set<int32_t>(native_bytes.data.size()));
            input->close();
            auto native = std::unique_ptr<faiss::Index>(faiss::read_index(
                    &native_bytes,
                    faiss::IO_FLAG_SKIP_IVF_DATA | faiss::IO_FLAG_SKIP_PRECOMPUTE_TABLE));
            EXPECT_EQ(native->ntotal, 64);
            EXPECT_EQ(dynamic_cast<faiss::IndexIDMap*>(native.get()) != nullptr,
                      type == FaissBuildParameter::IndexType::HNSW);

            FaissVectorIndex reader;
            reader.set_type(type == FaissBuildParameter::IndexType::HNSW ? AnnIndexType::HNSW
                            : type == FaissBuildParameter::IndexType::IVF
                                    ? AnnIndexType::IVF
                                    : AnnIndexType::IVF_ON_DISK);
            reader.set_metric(l2 ? AnnIndexMetric::L2 : AnnIndexMetric::IP);
            reader.set_ivfdata_cache_key_prefix("sparse_labels_" +
                                                std::to_string(static_cast<int>(metric)));
            ASSERT_TRUE(reader.load(dir.get()).ok());
            EXPECT_EQ(reader._indexed_rows, nullptr);
            // Concurrent first complements share one complete domain, including IDs-only disk reads.
            std::barrier start(4);
            std::vector<std::future<Status>> initializations;
            for (int i = 0; i < 4; ++i) {
                initializations.emplace_back(std::async(std::launch::async, [&] {
                    start.arrive_and_wait();
                    return reader._init_indexed_rows();
                }));
            }
            for (auto& initialization : initializations) {
                ASSERT_TRUE(initialization.get().ok());
            }
            roaring::Roaring indexed;
            for (auto id : labels) {
                indexed.add(cast_set<uint32_t>(id));
            }
            EXPECT_EQ(*reader._indexed_rows, indexed);
            HNSWSearchParameters hnsw;
            hnsw.ef_search = 256;
            IVFSearchParameters ivf;
            ivf.nprobe = 1;
            IndexSearchParameters& params = type == FaissBuildParameter::IndexType::HNSW
                                                    ? static_cast<IndexSearchParameters&>(hnsw)
                                                    : static_cast<IndexSearchParameters&>(ivf);
            const float query[] = {l2 ? 0.0f : 1.0f, 0.0f};
            for (bool delete_best : {false, true}) {
                roaring::Roaring allowed;
                allowed.addRange(0, rows);
                const int best = l2 ? 0 : 63;
                if (delete_best) {
                    allowed.remove(labels[best]);
                }
                params.roaring = &allowed;
                params.rows_of_segment = rows;
                IndexSearchResult topn;
                ASSERT_TRUE(reader.ann_topn_search(query, 1, params, topn).ok());
                const int winner = delete_best ? (l2 ? 1 : 62) : best;
                ASSERT_EQ(topn.row_ids->size(), 1);
                EXPECT_EQ((*topn.row_ids)[0], labels[winner]);
                EXPECT_FLOAT_EQ(topn.distances[0], l2 ? std::sqrt(winner * winner + 1.0f)
                                                      : static_cast<float>(winner));
                for (bool less : {false, true}) {
                    params.is_le_or_lt = less;
                    IndexSearchResult range;
                    ASSERT_TRUE(reader.range_search(query, 10.5f, params, range).ok());
                    roaring::Roaring expected;
                    for (int i = 0; i < 64; ++i) {
                        if ((i <= 10) == less && (!delete_best || i != best)) {
                            expected.add(labels[i]);
                        }
                    }
                    EXPECT_EQ(*range.roaring, expected);
                }
            }
        }
    }
}

TEST_F(VectorSearchTest, FailedIvfIdReadDoesNotPublishPartialDomain) {
    struct FailOnceIdsReader : faiss::RandomAccessReader {
        mutable int reads = 0;
        void read_at(size_t offset, void* output, size_t bytes) const override {
            ++reads;
            if (reads == 2) {
                throw Exception(ErrorCode::IO_ERROR, "injected second-list failure");
            }
            const faiss::idx_t id = offset == 8 ? 0 : 3;
            EXPECT_EQ(bytes, sizeof(id));
            std::memcpy(output, &id, sizeof(id));
        }
    };
    FaissBuildParameter build;
    build.dim = 2;
    build.index_type = FaissBuildParameter::IndexType::IVF_ON_DISK;
    build.ivf_nlist = 2;
    FaissVectorIndex index;
    index.build(build);
    auto* ivf = dynamic_cast<faiss::IndexIVF*>(index._index.get());
    ASSERT_NE(ivf, nullptr);
    std::vector<faiss::OnDiskOneList> lists(2);
    for (size_t i = 0; i < lists.size(); ++i) {
        lists[i].size = lists[i].capacity = 1;
        lists[i].offset = i * 16;
    }
    auto pread = std::make_unique<faiss::PreadInvertedLists>(2, 8, std::move(lists));
    auto reader = std::make_unique<FailOnceIdsReader>();
    auto* reader_ptr = reader.get();
    pread->set_reader(std::move(reader));
    ivf->replace_invlists(pread.release(), true);
    auto status = index._init_indexed_rows();
    EXPECT_TRUE(status.is<ErrorCode::IO_ERROR>());
    EXPECT_EQ(index._indexed_rows, nullptr);
    ASSERT_TRUE(index._init_indexed_rows().ok());
    EXPECT_EQ(*index._indexed_rows, roaring::Roaring::bitmapOf(2, 0, 3));
    EXPECT_EQ(reader_ptr->reads, 4);
    ASSERT_TRUE(index._init_indexed_rows().ok());
    EXPECT_EQ(reader_ptr->reads, 4);
}

TEST_F(VectorSearchTest, LegacyDenseHnswNeedsNoLabelCollection) {
    FaissBuildParameter build;
    build.dim = 2;
    build.max_degree = 8;
    FaissVectorIndex writer;
    writer.build(build);
    const float vectors[] = {0, 0, 1, 0, 2, 0, 3, 0};
    ASSERT_TRUE(writer.add(4, vectors).ok());
    auto dir = std::make_shared<lucene::store::RAMDirectory>();
    ASSERT_TRUE(writer.save(dir.get()).ok());
    FaissVectorIndex reader;
    reader.set_type(AnnIndexType::HNSW);
    reader.set_metric(AnnIndexMetric::L2);
    ASSERT_TRUE(reader.load(dir.get()).ok());
    EXPECT_NE(dynamic_cast<faiss::IndexHNSW*>(reader._index.get()), nullptr);
    EXPECT_EQ(dynamic_cast<faiss::IndexIDMap*>(reader._index.get()), nullptr);
    roaring::Roaring allowed = roaring::Roaring::bitmapOf(3, 0, 1, 2);
    HNSWSearchParameters params;
    params.ef_search = 32;
    params.roaring = &allowed;
    params.rows_of_segment = 4;
    const float query[] = {0, 0};
    IndexSearchResult result;
    params.is_le_or_lt = true;
    ASSERT_TRUE(reader.range_search(query, 1.5f, params, result).ok());
    EXPECT_EQ(*result.roaring, roaring::Roaring::bitmapOf(2, 0, 1));
    EXPECT_EQ(reader._indexed_rows, nullptr);
    params.is_le_or_lt = false;
    ASSERT_TRUE(reader.range_search(query, 1.5f, params, result).ok());
    EXPECT_EQ(*result.roaring, roaring::Roaring::bitmapOf(1, 2));
    EXPECT_EQ(reader._indexed_rows, nullptr);
}

TEST_F(VectorSearchTest, DenseIvfComplementsDoNotCollectAllListIds) {
    AnnIndexIVFListCache::create_global_cache(16 * 1024 * 1024);
    Defer destroy_cache([] { AnnIndexIVFListCache::destroy_global_cache(); });
    for (auto type :
         {FaissBuildParameter::IndexType::IVF, FaissBuildParameter::IndexType::IVF_ON_DISK}) {
        FaissBuildParameter build;
        build.dim = 2;
        build.index_type = type;
        build.ivf_nlist = 1;
        FaissVectorIndex writer;
        writer.build(build);
        std::vector<float> vectors;
        for (int i = 0; i < 64; ++i) {
            vectors.insert(vectors.end(), {static_cast<float>(i), 0.0f});
        }
        ASSERT_TRUE(writer.train(64, vectors.data()).ok());
        ASSERT_TRUE(writer.add(64, vectors.data()).ok());
        auto dir = std::make_shared<lucene::store::RAMDirectory>();
        ASSERT_TRUE(writer.save(dir.get()).ok());
        FaissVectorIndex reader;
        reader.set_type(type == FaissBuildParameter::IndexType::IVF ? AnnIndexType::IVF
                                                                    : AnnIndexType::IVF_ON_DISK);
        reader.set_ivfdata_cache_key_prefix("dense_complement");
        ASSERT_TRUE(reader.load(dir.get()).ok());
        EXPECT_EQ(reader.get_num_rows_in_index(), 64);
        roaring::Roaring allowed;
        allowed.addRange(0, 64);
        IVFSearchParameters params;
        params.roaring = &allowed;
        params.rows_of_segment = 64;
        params.is_le_or_lt = false;
        const float query[] = {0, 0};
        IndexSearchResult result;
        ASSERT_TRUE(reader.range_search(query, 10.5f, params, result).ok());
        roaring::Roaring expected;
        expected.addRange(11, 64);
        EXPECT_EQ(*result.roaring, expected);
        EXPECT_EQ(reader._indexed_rows, nullptr);
    }
}

TEST_F(VectorSearchTest, AllEmptySparseIndexNeedsNoTraining) {
    for (auto type : {FaissBuildParameter::IndexType::HNSW, FaissBuildParameter::IndexType::IVF,
                      FaissBuildParameter::IndexType::IVF_ON_DISK}) {
        FaissBuildParameter build;
        build.dim = 2;
        build.max_degree = 16;
        build.ivf_nlist = 1;
        build.index_type = type;
        FaissVectorIndex writer;
        writer.build(build);
        roaring::Roaring empty_rows;
        empty_rows.addRange(0, 3);
        ASSERT_TRUE(writer.add_with_ids(0, nullptr, nullptr).ok());
        auto dir = std::make_shared<lucene::store::RAMDirectory>();
        ASSERT_TRUE(writer.save(dir.get()).ok());
        FaissVectorIndex reader;
        reader.set_type(type == FaissBuildParameter::IndexType::HNSW  ? AnnIndexType::HNSW
                        : type == FaissBuildParameter::IndexType::IVF ? AnnIndexType::IVF
                                                                      : AnnIndexType::IVF_ON_DISK);
        ASSERT_TRUE(reader.load(dir.get()).ok());
        HNSWSearchParameters params;
        params.roaring = &empty_rows;
        params.rows_of_segment = 3;
        const float query[] = {0, 0};
        IndexSearchResult result;
        ASSERT_TRUE(reader.ann_topn_search(query, 1, params, result).ok());
        EXPECT_TRUE(result.roaring->isEmpty());
        for (bool less : {false, true}) {
            params.is_le_or_lt = less;
            ASSERT_TRUE(reader.range_search(query, 1, params, result).ok());
            EXPECT_TRUE(result.roaring->isEmpty());
        }
    }
}

// Test saving and loading an index
TEST_F(VectorSearchTest, TestSaveAndLoad) {
    std::vector<FaissBuildParameter::Quantizer> quantizers = {
            FaissBuildParameter::Quantizer::FLAT, FaissBuildParameter::Quantizer::SQ4,
            FaissBuildParameter::Quantizer::SQ8, FaissBuildParameter::Quantizer::PQ};

    for (auto quantizer : quantizers) {
        // Step 1: Create first index instance
        auto index1 = std::make_unique<FaissVectorIndex>();

        // Step 2: Set build parameters
        FaissBuildParameter params;
        params.dim = 128;       // Vector dimension
        params.max_degree = 16; // HNSW max connections
        params.pq_m = 4;
        params.pq_nbits = 8;
        params.index_type = FaissBuildParameter::IndexType::HNSW;
        params.quantizer = quantizer;
        index1->build(params);

        // Step 3: Add vectors to the index
        const int num_vectors = 100;
        std::vector<float> vectors;
        for (int i = 0; i < num_vectors; i++) {
            auto tmp = vector_search_utils::generate_random_vector(params.dim);
            vectors.insert(vectors.end(), tmp.begin(), tmp.end());
        }

        std::ignore = index1->train(num_vectors, vectors.data());
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
}

TEST_F(VectorSearchTest, TestSaveAndLoadIVF) {
    std::vector<FaissBuildParameter::Quantizer> quantizers = {
            FaissBuildParameter::Quantizer::FLAT, FaissBuildParameter::Quantizer::SQ4,
            FaissBuildParameter::Quantizer::SQ8, FaissBuildParameter::Quantizer::PQ};

    for (auto quantizer : quantizers) {
        // Step 1: Create first index instance
        auto index1 = std::make_unique<FaissVectorIndex>();

        // Step 2: Set build parameters
        FaissBuildParameter params;
        params.dim = 128; // Vector dimension
        params.ivf_nlist = 4;
        params.pq_m = 4;
        params.pq_nbits = 8;
        params.quantizer = quantizer;
        params.index_type = FaissBuildParameter::IndexType::IVF;
        index1->build(params);

        // Step 3: Add vectors to the index
        const int num_vectors = 100;
        std::vector<float> vectors;
        for (int i = 0; i < num_vectors; i++) {
            auto tmp = vector_search_utils::generate_random_vector(params.dim);
            vectors.insert(vectors.end(), tmp.begin(), tmp.end());
        }

        std::ignore = index1->train(num_vectors, vectors.data());
        std::ignore = index1->add(num_vectors, vectors.data());

        // Step 4: Save the index
        auto save_status = index1->save(_ram_dir.get());
        ASSERT_TRUE(save_status.ok()) << "Failed to save index: " << save_status.to_string();

        // Step 5: Create a new index instance
        auto index2 = std::make_unique<FaissVectorIndex>();
        index2->set_type(segment_v2::AnnIndexType::IVF);

        // Step 6: Load the index
        auto load_status = index2->load(_ram_dir.get());
        ASSERT_TRUE(load_status.ok()) << "Failed to load index: " << load_status.to_string();

        // Step 7: Verify the loaded index works by searching
        auto query_vec = vector_search_utils::generate_random_vector(params.dim);
        const int top_k = 10;

        // TopN search requires a candidate roaring and rows_of_segment now.
        IVFSearchParameters topn_params;
        auto topn_roaring = std::make_unique<roaring::Roaring>();
        for (int i = 0; i < num_vectors; ++i) topn_roaring->add(i);
        topn_params.roaring = topn_roaring.get();
        topn_params.rows_of_segment = num_vectors;
        topn_params.nprobe = 4;

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

        IVFSearchParameters ivf_params;
        auto roaring_bitmap = std::make_unique<roaring::Roaring>();
        ivf_params.roaring = roaring_bitmap.get();
        for (size_t i = 0; i < num_vectors; ++i) {
            ivf_params.roaring->add(i);
        }
        IndexSearchResult range_search_result1;
        std::ignore = index1->range_search(vectors.data(), 10, ivf_params, range_search_result1);
        IndexSearchResult range_search_result2;
        std::ignore = index2->range_search(vectors.data(), 10, ivf_params, range_search_result2);
        EXPECT_EQ(range_search_result1.roaring->cardinality(),
                  range_search_result2.roaring->cardinality())
                << "Row ID cardinality mismatch";
        for (size_t i = 0; i < range_search_result1.roaring->cardinality(); ++i) {
            EXPECT_EQ(range_search_result1.distances[i], range_search_result2.distances[i])
                    << "Distance mismatch at index " << i;
        }
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

TEST_F(VectorSearchTest, OmpThreadBudgetNeverExceedsLimit) {
    constexpr int kWorkers = 2;
    constexpr int kDim = 64;
    // Keep this workload small to avoid long-running BE UT under ASAN.
    constexpr int kNumVectors = 500;

    const auto old_omp_threads_limit = config::omp_threads_limit;
    config::omp_threads_limit = 1;
    Defer reset_omp_threads_limit(
            [&old_omp_threads_limit]() { config::omp_threads_limit = old_omp_threads_limit; });

    auto* budget_metric = DorisMetrics::instance()->ann_index_build_index_threads;
    std::atomic<bool> start {false};
    std::atomic<int> finished {0};
    std::vector<std::thread> workers;
    workers.reserve(kWorkers);

    for (int worker_id = 0; worker_id < kWorkers; ++worker_id) {
        workers.emplace_back([&start, &finished, worker_id]() {
            auto index = std::make_unique<FaissVectorIndex>();
            FaissBuildParameter params;
            params.dim = kDim;
            params.max_degree = 8;
            params.ef_construction = 20;
            params.index_type = FaissBuildParameter::IndexType::HNSW;
            index->build(params);

            std::vector<float> vectors(static_cast<size_t>(kNumVectors) * kDim,
                                       static_cast<float>(worker_id + 1));
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }

            auto st = index->add(kNumVectors, vectors.data());
            EXPECT_TRUE(st.ok()) << st.to_string();
            finished.fetch_add(1, std::memory_order_acq_rel);
        });
    }

    start.store(true, std::memory_order_release);

    int64_t observed_peak = 0;
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
    while (finished.load(std::memory_order_acquire) < kWorkers &&
           std::chrono::steady_clock::now() < deadline) {
        observed_peak = std::max<int64_t>(observed_peak, budget_metric->value());
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    for (auto& worker : workers) {
        worker.join();
    }

    observed_peak = std::max<int64_t>(observed_peak, budget_metric->value());
    EXPECT_EQ(finished.load(std::memory_order_acquire), kWorkers);
    EXPECT_LE(observed_peak, 1);
    EXPECT_EQ(budget_metric->value(), 0);
}

namespace {

class MockFaissBuildIndex : public faiss::IndexFlatL2 {
public:
    MockFaissBuildIndex() : faiss::IndexFlatL2(1) {}
    MOCK_METHOD(void, train, (faiss::idx_t, const float*), (override));
    MOCK_METHOD(void, add, (faiss::idx_t, const float*), (override));
};

class AnnOmpThreadBudgetTest : public VectorSearchTest {
public:
    void SetUp() override {
        VectorSearchTest::SetUp();
        _old_limit = config::omp_threads_limit;
        _old_threads_per_task = config::ann_index_build_threads_per_task;
        _old_num_cores = CpuInfo::num_cores_;
        _old_omp_threads = omp_get_max_threads();
    }

    void TearDown() override {
        EXPECT_EQ(DorisMetrics::instance()->ann_index_build_index_threads->value(), 0);
        config::omp_threads_limit = _old_limit;
        config::ann_index_build_threads_per_task = _old_threads_per_task;
        CpuInfo::num_cores_ = _old_num_cores;
        omp_set_num_threads(_old_omp_threads);
        VectorSearchTest::TearDown();
    }

private:
    int _old_limit = 0;
    int _old_threads_per_task = 0;
    int _old_num_cores = 0;
    int _old_omp_threads = 0;
};

} // namespace

TEST_F(AnnOmpThreadBudgetTest, ConfigurationRejectsInvalidReservations) {
    const auto& field = config::Register::_s_field_map->at("ann_index_build_threads_per_task");
    EXPECT_FALSE(field.valmutable);
    const auto& validate = config::RegisterConfValidator::_s_field_validator->at(
            "ann_index_build_threads_per_task");
    for (int value : {-2, 0}) {
        config::ann_index_build_threads_per_task = value;
        EXPECT_FALSE(validate()) << value;
    }
    for (int value : {-1, 1, 4}) {
        config::ann_index_build_threads_per_task = value;
        EXPECT_TRUE(validate()) << value;
    }
}

TEST_F(AnnOmpThreadBudgetTest, FixedAndAutomaticReservations) {
    struct TestCase {
        int total;
        int per_task;
        int expected;
    };
    const int auto_threads = CpuInfo::num_cores() < 32 ? 1 : 4;
    const TestCase cases[] = {{10, 4, 4},
                              {2, 8, 2},
                              {100, -1, auto_threads},
                              {2, -1, std::min(2, auto_threads)},
                              {-1, -1, auto_threads}};
    float value = 0;
    for (const auto& test : cases) {
        SCOPED_TRACE(testing::Message()
                     << "total=" << test.total << ", per_task=" << test.per_task);
        config::omp_threads_limit = test.total;
        config::ann_index_build_threads_per_task = test.per_task;
        FaissVectorIndex index;
        auto mock = std::make_unique<testing::StrictMock<MockFaissBuildIndex>>();
        auto check_budget = [&](faiss::idx_t, const float*) {
            EXPECT_EQ(omp_get_max_threads(), test.expected);
            EXPECT_EQ(DorisMetrics::instance()->ann_index_build_index_threads->value(),
                      test.expected);
        };
        EXPECT_CALL(*mock, train(0, &value)).WillOnce(check_budget);
        EXPECT_CALL(*mock, add(0, &value)).WillOnce(check_budget);
        index._index = std::move(mock);
        EXPECT_TRUE(index.train(0, &value).ok());
        EXPECT_EQ(DorisMetrics::instance()->ann_index_build_index_threads->value(), 0);
        EXPECT_TRUE(index.add(0, &value).ok());
        EXPECT_EQ(DorisMetrics::instance()->ann_index_build_index_threads->value(), 0);
    }
}

TEST_F(AnnOmpThreadBudgetTest, AutomaticReservationsKeepFirstCpuSnapshot) {
    config::omp_threads_limit = -1;
    config::ann_index_build_threads_per_task = -1;
    const int expected_threads = CpuInfo::num_cores() < 32 ? 1 : 4;
    const int expected_limit = std::max(1, CpuInfo::num_cores() * 4 / 5);
    float value = 0;
    FaissVectorIndex index;
    auto mock = std::make_unique<testing::StrictMock<MockFaissBuildIndex>>();
    EXPECT_CALL(*mock, train(0, &value)).Times(3).WillRepeatedly([&](faiss::idx_t, const float*) {
        EXPECT_EQ(omp_get_max_threads(), expected_threads);
        EXPECT_EQ(DorisMetrics::instance()->ann_index_build_index_threads->value(),
                  expected_threads);
    });
    EXPECT_CALL(*mock, add(0, &value)).Times(2).WillRepeatedly([&](faiss::idx_t, const float*) {
        EXPECT_EQ(omp_get_max_threads(), expected_limit);
        EXPECT_EQ(DorisMetrics::instance()->ann_index_build_index_threads->value(), expected_limit);
    });
    index._index = std::move(mock);
    ASSERT_TRUE(index.train(0, &value).ok());
    // Cross the 32-core default boundary in both directions after the first reservation.
    // Also request the whole budget to verify that auto total sizing uses the same snapshot.
    for (int cores : {1, 64}) {
        CpuInfo::num_cores_ = cores;
        config::ann_index_build_threads_per_task = -1;
        EXPECT_TRUE(index.train(0, &value).ok());
        config::ann_index_build_threads_per_task = std::numeric_limits<int>::max();
        EXPECT_TRUE(index.add(0, &value).ok());
    }
}

TEST_F(AnnOmpThreadBudgetTest, ConcurrentCallsKeepFixedReservations) {
    config::ann_index_build_threads_per_task = 4;
    float value = 0;
    auto* metric = DorisMetrics::instance()->ann_index_build_index_threads;
    // Two calls fit in 10 slots. With 7 slots, the second must wait rather than take 3.
    for (int total : {10, 7}) {
        SCOPED_TRACE(total);
        config::omp_threads_limit = total;
        std::promise<void> first_entered;
        auto first_entered_future = first_entered.get_future();
        std::promise<void> release_first;
        auto release_first_future = release_first.get_future();
        std::promise<void> second_started;
        auto second_started_future = second_started.get_future();
        std::promise<void> second_entered;
        auto second_entered_future = second_entered.get_future();

        FaissVectorIndex first;
        auto first_mock = std::make_unique<testing::StrictMock<MockFaissBuildIndex>>();
        EXPECT_CALL(*first_mock, train(0, &value)).WillOnce([&](faiss::idx_t, const float*) {
            EXPECT_EQ(omp_get_max_threads(), 4);
            EXPECT_EQ(metric->value(), 4);
            first_entered.set_value();
            release_first_future.wait();
        });
        first._index = std::move(first_mock);
        auto first_result = std::async(std::launch::async, [&] { return first.train(0, &value); });
        EXPECT_EQ(first_entered_future.wait_for(std::chrono::seconds(5)),
                  std::future_status::ready);

        FaissVectorIndex second;
        auto second_mock = std::make_unique<testing::StrictMock<MockFaissBuildIndex>>();
        EXPECT_CALL(*second_mock, add(0, &value)).WillOnce([&](faiss::idx_t, const float*) {
            EXPECT_EQ(omp_get_max_threads(), 4);
            EXPECT_EQ(metric->value(), total == 10 ? 8 : 4);
            second_entered.set_value();
        });
        second._index = std::move(second_mock);
        auto second_result = std::async(std::launch::async, [&] {
            second_started.set_value();
            return second.add(0, &value);
        });
        second_started_future.wait();
        if (total == 10) {
            EXPECT_EQ(second_entered_future.wait_for(std::chrono::seconds(5)),
                      std::future_status::ready);
        } else {
            EXPECT_EQ(second_entered_future.wait_for(std::chrono::milliseconds(100)),
                      std::future_status::timeout);
            EXPECT_EQ(metric->value(), 4);
        }
        release_first.set_value();
        EXPECT_TRUE(first_result.get().ok());
        EXPECT_TRUE(second_result.get().ok());
        EXPECT_EQ(metric->value(), 0);
    }
}

TEST_F(AnnOmpThreadBudgetTest, FailedTrainingReleasesReservation) {
    config::omp_threads_limit = 4;
    config::ann_index_build_threads_per_task = 4;
    float value = 0;
    FaissVectorIndex index;
    auto mock = std::make_unique<testing::StrictMock<MockFaissBuildIndex>>();
    EXPECT_CALL(*mock, train(0, &value)).WillOnce([](faiss::idx_t, const float*) {
        throw faiss::FaissException("injected training failure");
    });
    EXPECT_CALL(*mock, add(0, &value)).WillOnce([](faiss::idx_t, const float*) {
        EXPECT_EQ(omp_get_max_threads(), 4);
        EXPECT_EQ(DorisMetrics::instance()->ann_index_build_index_threads->value(), 4);
    });
    index._index = std::move(mock);
    EXPECT_FALSE(index.train(0, &value).ok());
    EXPECT_EQ(DorisMetrics::instance()->ann_index_build_index_threads->value(), 0);
    EXPECT_TRUE(index.add(0, &value).ok());
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

        // Search in native Faiss index using the same efSearch that Doris applies:
        // ann_topn_search clamps to max(ef_search, k), so native must match.
        std::vector<float> native_distances(top_k, -1);
        std::vector<faiss::idx_t> native_indices(top_k, -1);
        faiss::SearchParametersHNSW native_params;
        native_params.efSearch = std::max(search_params.ef_search, top_k);
        native_index->search(1, query_vec, top_k, native_distances.data(), native_indices.data(),
                             &native_params);
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
        params.max_degree = 128;
        params.index_type = FaissBuildParameter::IndexType::HNSW;
        index1->build(params);

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
        search_params.ef_search = 128;
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
            doris_index->build(params);

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

TEST_F(VectorSearchTest, CompRangeSearchIVF) {
    size_t iterations = 5;
    std::vector<faiss::MetricType> metrics = {faiss::METRIC_L2, faiss::METRIC_INNER_PRODUCT};
    for (size_t i = 0; i < iterations; ++i) {
        for (auto metric : metrics) {
            // Random parameters for each test iteration
            std::random_device rd;
            std::mt19937 gen(rd());
            size_t random_d = std::uniform_int_distribution<>(1, 512)(gen);
            size_t random_n = std::uniform_int_distribution<>(10, 200)(gen);

            // Step 1: Create and build index
            auto doris_index = std::make_unique<FaissVectorIndex>();
            FaissBuildParameter params;
            params.dim = random_d;
            params.ivf_nlist = 4;
            params.index_type = FaissBuildParameter::IndexType::IVF;
            if (metric == faiss::METRIC_L2) {
                params.metric_type = FaissBuildParameter::MetricType::L2;
            } else if (metric == faiss::METRIC_INNER_PRODUCT) {
                params.metric_type = FaissBuildParameter::MetricType::IP;
            } else {
                throw std::runtime_error(fmt::format("Unsupported metric type: {}", metric));
            }
            doris_index->build(params);

            const int num_vectors = random_n;
            std::vector<float> flat_vector;
            std::vector<float> query_vec;
            for (int i = 0; i < num_vectors; i++) {
                auto vec = vector_search_utils::generate_random_vector(params.dim);
                if (i == 0) {
                    query_vec = vec;
                }
                flat_vector.insert(flat_vector.end(), vec.begin(), vec.end());
            }

            std::unique_ptr<faiss::Index> native_index;
            std::unique_ptr<faiss::IndexFlat> quantizer = nullptr;
            if (metric == faiss::METRIC_L2) {
                quantizer = std::make_unique<faiss::IndexFlat>(params.dim, faiss::METRIC_L2);
                native_index = std::make_unique<faiss::IndexIVFFlat>(
                        quantizer.get(), params.dim, params.ivf_nlist, faiss::METRIC_L2);
            } else if (metric == faiss::METRIC_INNER_PRODUCT) {
                quantizer =
                        std::make_unique<faiss::IndexFlat>(params.dim, faiss::METRIC_INNER_PRODUCT);
                native_index = std::make_unique<faiss::IndexIVFFlat>(
                        quantizer.get(), params.dim, params.ivf_nlist, faiss::METRIC_INNER_PRODUCT);
            } else {
                throw std::runtime_error(fmt::format("Unsupported metric type: {}", metric));
            }

            doris::vector_search_utils::add_vectors_to_indexes_batch_mode(
                    doris_index.get(), native_index.get(), num_vectors, flat_vector);

            float radius = 0;
            radius = doris::vector_search_utils::get_radius_from_flatten(
                    query_vec.data(), params.dim, flat_vector, 0.4f);

            IVFSearchParameters ivf_params;
            ivf_params.nprobe = 4;
            // Search on all rows;
            auto roaring = std::make_unique<roaring::Roaring>();
            ivf_params.roaring = roaring.get();
            for (size_t i = 0; i < num_vectors; i++) {
                ivf_params.roaring->add(i);
            }
            ivf_params.is_le_or_lt = metric == faiss::METRIC_L2;
            IndexSearchResult doris_result;
            std::ignore =
                    doris_index->range_search(query_vec.data(), radius, ivf_params, doris_result);

            faiss::SearchParametersIVF search_params_native;
            search_params_native.nprobe = ivf_params.nprobe;
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
                    << fmt::format("\nd: {}, n: {}, metric: {}", random_d, random_n, metric);
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
        index1->build(params);

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
        index1->build(params);

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
                    doris_index->build(params);

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
        doris_index->build(params);

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

TEST_F(VectorSearchTest, InnerProductRangeSearchBasicIVF) {
    const size_t iterations = 3;

    for (size_t iter = 0; iter < iterations; ++iter) {
        const int dim = 64;
        const int n = 500;
        const int nlist = 4;

        // Create Doris IVF index
        auto doris_index = std::make_unique<FaissVectorIndex>();
        FaissBuildParameter params;
        params.dim = dim;
        params.index_type = FaissBuildParameter::IndexType::IVF;
        params.ivf_nlist = nlist;
        params.quantizer = FaissBuildParameter::Quantizer::FLAT;
        params.metric_type = FaissBuildParameter::MetricType::IP;
        doris_index->build(params);

        // Generate vectors
        std::vector<std::vector<float>> vectors;
        std::vector<float> flat_vectors;
        for (int i = 0; i < n; ++i) {
            auto vec = doris::vector_search_utils::generate_random_vector(dim);
            vectors.push_back(vec);
            flat_vectors.insert(flat_vectors.end(), vec.begin(), vec.end());
        }

        // Add vectors to index
        doris_index->train(n, flat_vectors.data());
        doris_index->add(n, flat_vectors.data());

        // Use first vector as query
        std::vector<float> query_vec = vectors[0];

        // Calculate radius based on inner product distribution
        float radius = doris::vector_search_utils::get_radius_from_matrix(
                query_vec.data(), dim, vectors, 0.5f, faiss::METRIC_INNER_PRODUCT);

        // Perform Doris range search
        IVFSearchParameters doris_params;
        doris_params.nprobe = 4;          // Search in 4 clusters
        doris_params.is_le_or_lt = false; // For inner product, we want values >= radius
        auto roaring = std::make_unique<roaring::Roaring>();
        for (int i = 0; i < n; ++i) {
            roaring->add(i);
        }
        doris_params.roaring = roaring.get();

        IndexSearchResult doris_result;
        auto status =
                doris_index->range_search(query_vec.data(), radius, doris_params, doris_result);
        ASSERT_TRUE(status.ok()) << "Doris IVF range search failed";

        // Verify basic properties
        ASSERT_GT(doris_result.roaring->cardinality(), 0u) << "Should find some results";
        ASSERT_NE(doris_result.distances, nullptr) << "Distances should be provided";

        // Verify all returned distances are >= radius
        for (size_t i = 0; i < doris_result.roaring->cardinality(); ++i) {
            ASSERT_GE(doris_result.distances[i], radius - 1e-6)
                    << "Distance should be >= radius for IVF inner product range search";
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
    l2_index->build(l2_params);
    l2_index->add(n, flat_vectors.data());

    // Create Inner Product index
    auto ip_index = std::make_unique<FaissVectorIndex>();
    FaissBuildParameter ip_params;
    ip_params.dim = dim;
    ip_params.max_degree = 32;
    ip_params.index_type = FaissBuildParameter::IndexType::HNSW;
    ip_params.metric_type = FaissBuildParameter::MetricType::IP;
    ip_index->build(ip_params);
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

TEST_F(VectorSearchTest, TestIdSelectorRoaringBasicMembership) {
    auto roaring = std::make_unique<roaring::Roaring>();
    for (uint32_t i : {0u, 2u, 4u, 1000u}) {
        roaring->add(i);
    }
    auto sel = FaissVectorIndex::roaring_to_faiss_selector(*roaring);
    for (uint32_t i : {0u, 2u, 4u, 1000u}) {
        ASSERT_TRUE(sel->is_member(static_cast<faiss::idx_t>(i)))
                << "Expected id " << i << " present";
    }
    for (uint32_t i : {1u, 3u, 5u, 999u, 1001u}) {
        ASSERT_FALSE(sel->is_member(static_cast<faiss::idx_t>(i)))
                << "Unexpected id " << i << " present";
    }
    ASSERT_FALSE(sel->is_member(-1)) << "Negative ids should never match";
}

TEST_F(VectorSearchTest, TestIdSelectorRoaringReflectsBitmapUpdates) {
    auto roaring = std::make_unique<roaring::Roaring>();
    roaring->add(10);
    auto sel = FaissVectorIndex::roaring_to_faiss_selector(*roaring);

    ASSERT_TRUE(sel->is_member(10));
    ASSERT_FALSE(sel->is_member(20));

    roaring->add(20);
    roaring->remove(10);

    ASSERT_FALSE(sel->is_member(10)) << "Selector should track removals";
    ASSERT_TRUE(sel->is_member(20)) << "Selector should track additions";
}

TEST_F(VectorSearchTest, TestIdSelectorRoaringHandlesUInt32Max) {
    auto roaring = std::make_unique<roaring::Roaring>();
    constexpr uint32_t kMax = std::numeric_limits<uint32_t>::max();
    roaring->add(kMax);
    auto sel = FaissVectorIndex::roaring_to_faiss_selector(*roaring);

    ASSERT_TRUE(sel->is_member(static_cast<faiss::idx_t>(kMax)))
            << "Expected uint32_t max to be present";
    bool exception_thrown = false;
    try {
        sel->is_member(static_cast<faiss::idx_t>(kMax) + 1);
    } catch (const std::exception& e) {
        exception_thrown = true;
    }
    ASSERT_TRUE(exception_thrown) << "Expected exception for value beyond uint32_t max";
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
    index->build(params);

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
    index->build(params);

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

TEST_F(VectorSearchTest, IVFOnDiskSaveLoadAndSearch) {
    AnnIndexIVFListCache::create_global_cache(64 * 1024 * 1024);
    Defer destroy_cache([] { AnnIndexIVFListCache::destroy_global_cache(); });

    auto index1 = std::make_unique<FaissVectorIndex>();
    FaissBuildParameter params;
    params.dim = 32;
    params.ivf_nlist = 4;
    params.quantizer = FaissBuildParameter::Quantizer::FLAT;
    params.index_type = FaissBuildParameter::IndexType::IVF_ON_DISK;
    index1->build(params);

    const int num_vectors = 200;
    std::vector<float> vectors;
    vectors.reserve(static_cast<size_t>(num_vectors) * params.dim);
    for (int i = 0; i < num_vectors; i++) {
        auto tmp = vector_search_utils::generate_random_vector(params.dim);
        vectors.insert(vectors.end(), tmp.begin(), tmp.end());
    }
    ASSERT_TRUE(index1->train(num_vectors, vectors.data()).ok());
    ASSERT_TRUE(index1->add(num_vectors, vectors.data()).ok());

    auto dir = std::make_shared<lucene::store::RAMDirectory>();
    ASSERT_TRUE(index1->save(dir.get()).ok());

    auto index2 = std::make_unique<FaissVectorIndex>();
    index2->set_type(AnnIndexType::IVF_ON_DISK);
    index2->set_ivfdata_cache_key_prefix("ut_stampede_test");
    ASSERT_TRUE(index2->load(dir.get()).ok());

    auto query_vec = vector_search_utils::generate_random_vector(params.dim);
    IVFSearchParameters search_params;
    search_params.nprobe = 4;
    auto roaring = std::make_unique<roaring::Roaring>();
    for (int i = 0; i < num_vectors; ++i) roaring->add(i);
    search_params.roaring = roaring.get();
    search_params.rows_of_segment = num_vectors;

    IndexSearchResult result;
    ASSERT_TRUE(index2->ann_topn_search(query_vec.data(), 10, search_params, result).ok());
    EXPECT_GT(result.roaring->cardinality(), 0u);
    EXPECT_GT(result.ivf_on_disk_cache_miss_cnt, 0);
}

// Searching WITHOUT a list cache has to read, not recurse.
//
// faiss documents RandomAccessReader::borrow()'s default as "allocates a buffer
// and calls read_at()", and CachedRandomAccessReader::read_at() is implemented by
// calling borrow(). Falling back to the base borrow() when
// AnnIndexIVFListCache::instance() is null is therefore unbounded mutual
// recursion. Every other IVF-on-disk test above installs the cache first, which
// is exactly why nothing has ever hit it.
//
// exec_env_init installs the cache unconditionally at BE startup, so a running BE
// does not reach this today. It is a trap for any unit test that does not know to
// install one -- it does not fail, it never returns -- and for any change that
// makes the cache optional.
//
// The search runs on a worker with a deadline because before the fix it does not
// return at all: called inline it would hang the whole suite instead of failing
// this one case. Everything the worker touches is shared-owned so a hung thread
// cannot outlive its arguments.
TEST_F(VectorSearchTest, IVFOnDiskSearchWithoutTheListCacheStillReads) {
    ASSERT_EQ(AnnIndexIVFListCache::instance(), nullptr)
            << "this case is about the no-cache path, but something installed a cache";

    auto builder = std::make_unique<FaissVectorIndex>();
    FaissBuildParameter params;
    params.dim = 32;
    params.ivf_nlist = 4;
    params.quantizer = FaissBuildParameter::Quantizer::FLAT;
    params.index_type = FaissBuildParameter::IndexType::IVF_ON_DISK;
    builder->build(params);

    const int num_vectors = 200;
    std::vector<float> vectors;
    vectors.reserve(static_cast<size_t>(num_vectors) * params.dim);
    for (int i = 0; i < num_vectors; i++) {
        auto tmp = vector_search_utils::generate_random_vector(params.dim);
        vectors.insert(vectors.end(), tmp.begin(), tmp.end());
    }
    ASSERT_TRUE(builder->train(num_vectors, vectors.data()).ok());
    ASSERT_TRUE(builder->add(num_vectors, vectors.data()).ok());

    auto dir = std::make_shared<lucene::store::RAMDirectory>();
    ASSERT_TRUE(builder->save(dir.get()).ok());

    auto index = std::make_shared<FaissVectorIndex>();
    index->set_type(AnnIndexType::IVF_ON_DISK);
    index->set_ivfdata_cache_key_prefix("ut_no_list_cache");
    ASSERT_TRUE(index->load(dir.get()).ok());

    auto query_vec = std::make_shared<std::vector<float>>(
            vector_search_utils::generate_random_vector(params.dim));
    auto roaring = std::make_shared<roaring::Roaring>();
    for (int i = 0; i < num_vectors; ++i) {
        roaring->add(i);
    }
    auto result = std::make_shared<IndexSearchResult>();

    auto done = std::make_shared<std::promise<Status>>();
    auto ready = done->get_future();
    std::thread worker([index, dir, query_vec, roaring, result, done, num_vectors] {
        IVFSearchParameters search_params;
        search_params.nprobe = 4;
        search_params.roaring = roaring.get();
        search_params.rows_of_segment = num_vectors;
        done->set_value(index->ann_topn_search(query_vec->data(), 10, search_params, *result));
    });
    const bool returned = ready.wait_for(std::chrono::seconds(60)) == std::future_status::ready;
    if (!returned) {
        worker.detach();
        FAIL() << "the search never returned: borrow() recurses when there is no list cache";
    }
    worker.join();
    ASSERT_TRUE(ready.get().ok());
    EXPECT_GT(result->roaring->cardinality(), 0U);
}

// All threads share a single FaissVectorIndex (and thus a single
// CachedRandomAccessReader with its _io_mutex).  On the first round
// (cold cache) threads contend on the same _io_mutex; the first thread
// through loads from "disk" while the rest hit the double-check cache
// lookup path (lines 372-375 in faiss_ann_index.cpp).
TEST_F(VectorSearchTest, IVFOnDiskConcurrentSearchStampedeProtection) {
    AnnIndexIVFListCache::create_global_cache(64 * 1024 * 1024);
    Defer destroy_cache([] { AnnIndexIVFListCache::destroy_global_cache(); });

    auto index_builder = std::make_unique<FaissVectorIndex>();
    FaissBuildParameter params;
    params.dim = 32;
    params.ivf_nlist = 4;
    params.quantizer = FaissBuildParameter::Quantizer::FLAT;
    params.index_type = FaissBuildParameter::IndexType::IVF_ON_DISK;
    index_builder->build(params);

    const int num_vectors = 200;
    std::vector<float> vectors;
    vectors.reserve(static_cast<size_t>(num_vectors) * params.dim);
    for (int i = 0; i < num_vectors; i++) {
        auto tmp = vector_search_utils::generate_random_vector(params.dim);
        vectors.insert(vectors.end(), tmp.begin(), tmp.end());
    }
    ASSERT_TRUE(index_builder->train(num_vectors, vectors.data()).ok());
    ASSERT_TRUE(index_builder->add(num_vectors, vectors.data()).ok());

    auto dir = std::make_shared<lucene::store::RAMDirectory>();
    ASSERT_TRUE(index_builder->save(dir.get()).ok());

    auto shared_index = std::make_shared<FaissVectorIndex>();
    shared_index->set_type(AnnIndexType::IVF_ON_DISK);
    shared_index->set_ivfdata_cache_key_prefix("ut_stampede_shared");
    ASSERT_TRUE(shared_index->load(dir.get()).ok());

    constexpr int kThreads = 8;
    auto query_vec = vector_search_utils::generate_random_vector(params.dim);

    std::barrier sync_point(kThreads);
    std::vector<IndexSearchResult> results(kThreads);
    std::vector<Status> statuses(kThreads);

    auto search_fn = [&](int tid) {
        IVFSearchParameters search_params;
        search_params.nprobe = 4;
        auto thread_roaring = std::make_unique<roaring::Roaring>();
        for (int i = 0; i < num_vectors; ++i) thread_roaring->add(i);
        search_params.roaring = thread_roaring.get();
        search_params.rows_of_segment = num_vectors;

        sync_point.arrive_and_wait();

        statuses[tid] =
                shared_index->ann_topn_search(query_vec.data(), 10, search_params, results[tid]);
    };

    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int i = 0; i < kThreads; ++i) {
        threads.emplace_back(search_fn, i);
    }
    for (auto& t : threads) {
        t.join();
    }

    int64_t total_hits = 0;
    int64_t total_misses = 0;
    for (int i = 0; i < kThreads; ++i) {
        ASSERT_TRUE(statuses[i].ok()) << "Thread " << i << ": " << statuses[i].to_string();
        EXPECT_GT(results[i].roaring->cardinality(), 0u) << "Thread " << i;
        total_hits += results[i].ivf_on_disk_cache_hit_cnt;
        total_misses += results[i].ivf_on_disk_cache_miss_cnt;
    }

    EXPECT_GT(total_hits, 0) << "Expected cache hits from stampede double-check path";
    EXPECT_GT(total_misses, 0) << "Expected cache misses from first-thread disk reads";
}

// NprobeClampedToNlist (name kept for history): after the fix nprobe only gets a
// lower-bound guard. FAISS asserts nprobe > 0, so nprobe < 1 (e.g. 0) throws
// "nprobe > 0 failed". The upper bound is intentionally NOT clamped: FAISS caps
// nprobe at the index's real nlist internally, and _params.ivf_nlist is unreliable
// after load() (stays at the default 1024). With nlist=4 this test cannot expose the
// stale-nlist upper-bound bug (that needs nlist>1024); it covers the lower-bound
// guard and that a huge nprobe is handled safely by FAISS.
TEST_F(VectorSearchTest, NprobeClampedToNlist) {
    const int dim = 32;
    const int nlist = 4;
    const int num_vectors = 200;

    auto index = std::make_unique<FaissVectorIndex>();
    FaissBuildParameter params;
    params.dim = dim;
    params.ivf_nlist = nlist;
    params.index_type = FaissBuildParameter::IndexType::IVF;
    params.quantizer = FaissBuildParameter::Quantizer::FLAT;
    index->build(params);

    std::vector<float> vecs;
    vecs.reserve(num_vectors * dim);
    for (int i = 0; i < num_vectors; i++) {
        auto v = vector_search_utils::generate_random_vector(dim);
        vecs.insert(vecs.end(), v.begin(), v.end());
    }
    ASSERT_TRUE(index->train(num_vectors, vecs.data()).ok());
    ASSERT_TRUE(index->add(num_vectors, vecs.data()).ok());

    ASSERT_TRUE(index->save(_ram_dir.get()).ok());
    auto loaded = std::make_unique<FaissVectorIndex>();
    loaded->set_type(AnnIndexType::IVF);
    ASSERT_TRUE(loaded->load(_ram_dir.get()).ok());

    auto roaring = std::make_unique<roaring::Roaring>();
    for (int i = 0; i < num_vectors; ++i) roaring->add(i);

    auto query = vector_search_utils::generate_random_vector(dim);
    IndexSearchResult result;

    // nprobe far above nlist: FAISS caps it at the real nlist on its own, so this
    // is safe with no upper-bound clamp at all. Regression guard for results.
    IVFSearchParameters search_params;
    search_params.roaring = roaring.get();
    search_params.rows_of_segment = num_vectors;
    search_params.nprobe = 9999;
    EXPECT_TRUE(loaded->ann_topn_search(query.data(), 5, search_params, result).ok())
            << "nprobe > nlist is safe (FAISS caps at nlist) and must return results";

    // nprobe = 0 — THIS is the real bug: FAISS asserts nprobe > 0, so without the
    // lower-bound clamp this would throw "nprobe > 0 failed".
    IndexSearchResult result2;
    search_params.nprobe = 0;
    EXPECT_TRUE(loaded->ann_topn_search(query.data(), 5, search_params, result2).ok())
            << "nprobe = 0 should succeed after clamping to 1";
}

// efSearch < k silently returns fewer results; after fix efSearch is
// boosted to max(ef_search, k) so we always get k valid results.
TEST_F(VectorSearchTest, EfSearchLinkedToK) {
    const int dim = 16;
    const int num_vectors = 200;
    const int k = 50;

    auto index = std::make_unique<FaissVectorIndex>();
    FaissBuildParameter params;
    params.dim = dim;
    params.max_degree = 16;
    params.ef_construction = 64;
    params.index_type = FaissBuildParameter::IndexType::HNSW;
    index->build(params);

    std::vector<float> vecs;
    vecs.reserve(num_vectors * dim);
    for (int i = 0; i < num_vectors; i++) {
        auto v = vector_search_utils::generate_random_vector(dim);
        vecs.insert(vecs.end(), v.begin(), v.end());
    }
    // HNSW does not need explicit train
    ASSERT_TRUE(index->add(num_vectors, vecs.data()).ok());

    ASSERT_TRUE(index->save(_ram_dir.get()).ok());
    auto loaded = std::make_unique<FaissVectorIndex>();
    loaded->set_type(AnnIndexType::HNSW);
    ASSERT_TRUE(loaded->load(_ram_dir.get()).ok());

    auto roaring = std::make_unique<roaring::Roaring>();
    for (int i = 0; i < num_vectors; ++i) roaring->add(i);

    auto query = vector_search_utils::generate_random_vector(dim);
    IndexSearchResult result;

    HNSWSearchParameters search_params;
    search_params.roaring = roaring.get();
    search_params.rows_of_segment = num_vectors;
    // ef_search deliberately set below k; after fix should be raised to k
    search_params.ef_search = 1;

    ASSERT_TRUE(loaded->ann_topn_search(query.data(), k, search_params, result).ok());
    // With the fix, efSearch = max(1, k) = k, so all k results must be valid (no -1 labels)
    EXPECT_EQ(static_cast<int>(result.roaring->cardinality()), k)
            << "efSearch boosted to max(ef_search, k): should return exactly k results";
}

} // namespace doris
