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

#include <faiss/index_io.h>

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "CLucene/store/IndexInput.h"
#include "CLucene/store/IndexOutput.h"
#include "common/exception.h"
#include "common/logging.h"
#include "common/status.h"
#include "faiss/IndexHNSW.h"
#include "faiss/impl/io.h"
#include "olap/rowset/segment_v2/ann_index/ann_search_params.h"
#include "util/metrics.h"
#include "vector/vector_index.h"

namespace doris::segment_v2 {

std::unique_ptr<faiss::IDSelector> FaissVectorIndex::roaring_to_faiss_selector(
        const roaring::Roaring& roaring) {
    std::vector<faiss::idx_t> ids;
    ids.reserve(roaring.cardinality());

    for (roaring::Roaring::const_iterator it = roaring.begin(); it != roaring.end(); ++it) {
        ids.push_back(static_cast<faiss::idx_t>(*it));
    }

    return std::make_unique<faiss::IDSelectorBatch>(ids.size(), ids.data());
}

void FaissVectorIndex::update_roaring(const faiss::idx_t* labels, const size_t n,
                                      roaring::Roaring& roaring) {
    // make sure roaring is empty before adding new elements
    DCHECK(roaring.cardinality() == 0);
    for (size_t i = 0; i < n; ++i) {
        if (labels[i] >= 0) {
            roaring.add(labels[i]);
        }
    }
}

struct FaissIndexWriter : faiss::IOWriter {
public:
    FaissIndexWriter() = default;
    FaissIndexWriter(lucene::store::IndexOutput* output) : _output(output) {}
    ~FaissIndexWriter() override {
        if (_output != nullptr) {
            _output->close();
            delete _output;
        }
    }

    size_t operator()(const void* ptr, size_t size, size_t nitems) override {
        size_t bytes = size * nitems;
        if (bytes > 0) {
            try {
                _output->writeBytes(reinterpret_cast<const uint8_t*>(ptr), bytes);
            } catch (const std::exception& e) {
                throw doris::Exception(doris::ErrorCode::IO_ERROR,
                                       "Failed to write vector index {}", e.what());
            }
        }
        return nitems;
    };

    lucene::store::IndexOutput* _output = nullptr;
};

struct FaissIndexReader : faiss::IOReader {
public:
    FaissIndexReader() = default;
    FaissIndexReader(lucene::store::IndexInput* input) : _input(input) {}
    ~FaissIndexReader() override {
        if (_input != nullptr) {
            _input->close();
            delete _input;
        }
    }
    size_t operator()(void* ptr, size_t size, size_t nitems) override {
        size_t bytes = size * nitems;
        if (bytes > 0) {
            try {
                _input->readBytes(reinterpret_cast<uint8_t*>(ptr), bytes);
            } catch (const std::exception& e) {
                throw doris::Exception(doris::ErrorCode::IO_ERROR, "Failed to read vector index {}",
                                       e.what());
            }
        }
        return nitems;
    };

    lucene::store::IndexInput* _input = nullptr;
};

/** Add n vectors of dimension d to the index.
*
* Vectors are implicitly assigned labels ntotal .. ntotal + n - 1
* This function slices the input vectors in chunks smaller than
* blocksize_add and calls add_core.
* @param n      number of vectors
* @param x      input matrix, size n * d
*/
doris::Status FaissVectorIndex::add(int n, const float* vec) {
    DCHECK(vec != nullptr);
    DCHECK(_index != nullptr);
    _index->add(n, vec);
    return doris::Status::OK();
}

void FaissVectorIndex::set_build_params(const FaissBuildParameter& params) {
    _dimension = params.d;
    switch (params.metric_type) {
    case FaissBuildParameter::MetricType::L2:
        _metric = Metric::L2;
        break;
    case FaissBuildParameter::MetricType::IP:
        _metric = Metric::IP;
        break;
    default:
        throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT, "Unsupported metric type: {}",
                               static_cast<int>(params.metric_type));
        break;
    }
    if (params.index_type == FaissBuildParameter::IndexType::BruteForce) {
        if (params.metric_type == FaissBuildParameter::MetricType::L2) {
            _index = std::make_unique<faiss::IndexFlatL2>(params.d);
        } else if (params.metric_type == FaissBuildParameter::MetricType::IP) {
            _index = std::make_unique<faiss::IndexFlatIP>(params.d);
        } else {
            throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT,
                                   "Unsupported metric type: {}",
                                   static_cast<int>(params.metric_type));
        }
    } else if (params.index_type == FaissBuildParameter::IndexType::HNSW) {
        if (params.quantilizer == FaissBuildParameter::Quantilizer::FLAT) {
            if (params.metric_type == FaissBuildParameter::MetricType::L2) {
                _index = std::make_unique<faiss::IndexHNSWFlat>(params.d, params.m);
            } else if (params.metric_type == FaissBuildParameter::MetricType::IP) {
                _index = std::make_unique<faiss::IndexHNSWFlat>(params.d, params.m,
                                                                faiss::METRIC_INNER_PRODUCT);
            } else {
                throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT,
                                       "Unsupported metric type: {}",
                                       static_cast<int>(params.metric_type));
            }
        } else if (params.quantilizer == FaissBuildParameter::Quantilizer::PQ) {
            if (params.pq_m <= 0) {
                throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT,
                                       "pq_m should be greater than 0 for PQ quantilizer");
            }

            if (params.metric_type == FaissBuildParameter::MetricType::L2) {
                _index = std::make_unique<faiss::IndexHNSWPQ>(params.d, params.m, params.pq_m);
            } else if (params.metric_type == FaissBuildParameter::MetricType::IP) {
                _index = std::make_unique<faiss::IndexHNSWPQ>(params.d, params.m, params.pq_m,
                                                              faiss::METRIC_INNER_PRODUCT);
            } else {
                throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT,
                                       "Unsupported metric type: {}",
                                       static_cast<int>(params.metric_type));
            }
        } else {
            throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT,
                                   "Unsupported quantilizer type: {}",
                                   static_cast<int>(params.quantilizer));
        }
    } else {
        throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT, "Unsupported index type: {}",
                               static_cast<int>(params.index_type));
    }
}

// TODO: Support batch search
doris::Status FaissVectorIndex::ann_topn_search(const float* query_vec, int k,
                                                const vectorized::IndexSearchParameters& params,
                                                vectorized::IndexSearchResult& result) {
    std::unique_ptr<float[]> distances_ptr = std::make_unique<float[]>(k);
    float* distances = distances_ptr.get();

    // Initialize labels with -1
    // Even if there are N vectors in the index, limit N search in faiss could return less than N(eg, HNSW)
    // so we need to initialize labels with -1 to tell the end of the result ids.
    std::unique_ptr<std::vector<faiss::idx_t>> labels_ptr =
            std::make_unique<std::vector<faiss::idx_t>>(k, -1);
    faiss::idx_t* labels = (*labels_ptr).data();

    if (params.roaring == nullptr) {
        _index->search(1, query_vec, k, distances, labels);
    } else {
        std::unique_ptr<faiss::IDSelector> id_sel = nullptr;
        id_sel = roaring_to_faiss_selector(*params.roaring);
        faiss::SearchParametersHNSW param;
        const vectorized::HNSWSearchParameters* hnsw_params =
                dynamic_cast<const vectorized::HNSWSearchParameters*>(&params);
        if (hnsw_params == nullptr) {
            return doris::Status::InvalidArgument(
                    "HNSW search parameters should not be null for HNSW index");
        }
        param.sel = id_sel.get();
        param.efSearch = hnsw_params->ef_search;
        param.check_relative_distance = hnsw_params->check_relative_distance;
        param.bounded_queue = hnsw_params->bounded_queue;

        _index->search(1, query_vec, k, distances, labels, &param);
    }

    result.roaring = std::make_shared<roaring::Roaring>();
    update_roaring(labels, k, *result.roaring);
    size_t roaring_cardinality = result.roaring->cardinality();
    result.distances = std::make_unique<float[]>(roaring_cardinality);
    result.row_ids = std::make_unique<std::vector<uint64_t>>();

    if (_metric == Metric::L2) {
        // For l2_distance, we need to convert the distance to the actual distance.
        // The distance returned by Faiss is actually the squared distance.
        // So we need to take the square root of the squared distance.
        for (size_t i = 0; i < roaring_cardinality; ++i) {
            result.row_ids->push_back(labels[i]);
            result.distances[i] = std::sqrt(distances[i]);
        }
    } else if (_metric == Metric::IP) {
        // For inner product, we can use the distance directly.
        for (size_t i = 0; i < roaring_cardinality; ++i) {
            result.row_ids->push_back(labels[i]);
            result.distances[i] = distances[i]; // Convert squared distance to actual distance
        }
    } else {
        throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT, "Unsupported metric type: {}",
                               static_cast<int>(_metric));
    }

    DCHECK(result.row_ids->size() == result.roaring->cardinality())
            << "Row ids size: " << result.row_ids->size()
            << ", roaring size: " << result.roaring->cardinality();
    return doris::Status::OK();
}

// For l2 distance, range search radius is the squared distance.
// For inner product, range search radius is the actual distance.
// range search on inner product returns all vectors with inner product greater than or equal to the radius.
// For l2 distance, range search returns all vectors with squared distance less than or equal to the radius.
doris::Status FaissVectorIndex::range_search(const float* query_vec, const float& radius,
                                             const vectorized::IndexSearchParameters& params,
                                             vectorized::IndexSearchResult& result) {
    DCHECK(_index != nullptr);
    DCHECK(query_vec != nullptr);
    DCHECK(params.roaring != nullptr)
            << "Roaring should not be null for range search, please set roaring in params";
    std::unique_ptr<faiss::IDSelector> sel = roaring_to_faiss_selector(*params.roaring);

    faiss::RangeSearchResult native_search_result(1, true);
    const vectorized::HNSWSearchParameters* hnsw_params =
            dynamic_cast<const vectorized::HNSWSearchParameters*>(&params);
    // Currently only support HNSW index for range search.
    DCHECK(hnsw_params != nullptr) << "HNSW search parameters should not be null for HNSW index";

    faiss::SearchParametersHNSW param;
    param.efSearch = hnsw_params->ef_search;
    param.check_relative_distance = hnsw_params->check_relative_distance;
    param.bounded_queue = hnsw_params->bounded_queue;
    param.sel = sel.get();
    if (_metric == Metric::L2) {
        _index->range_search(1, query_vec, radius * radius, &native_search_result, &param);
    } else if (_metric == Metric::IP) {
        _index->range_search(1, query_vec, radius, &native_search_result, &param);
    }

    size_t begin = native_search_result.lims[0];
    size_t end = native_search_result.lims[1];
    auto row_ids = std::make_unique<std::vector<uint64_t>>();
    row_ids->resize(end - begin);
    LOG_INFO("Range search result: begin {}, end {}", begin, end);
    if (params.is_le_or_lt) {
        if (_metric == Metric::L2) {
            std::unique_ptr<float[]> distances_ptr = std::make_unique<float[]>(end - begin);
            float* distances = distances_ptr.get();
            auto roaring = std::make_shared<roaring::Roaring>();
            // The distance returned by Faiss is actually the squared distance.
            // So we need to take the square root of the squared distance.
            for (size_t i = begin; i < end; ++i) {
                (*row_ids)[i] = native_search_result.labels[i];
                roaring->add(native_search_result.labels[i]);
                distances[i - begin] = sqrt(native_search_result.distances[i]);
            }
            result.distances = std::move(distances_ptr);
            result.row_ids = std::move(row_ids);
            result.roaring = roaring;

            DCHECK(result.row_ids->size() == result.roaring->cardinality())
                    << "row_ids size: " << result.row_ids->size()
                    << ", roaring size: " << result.roaring->cardinality();
        } else if (_metric == Metric::IP) {
            // For IP, we can use the distance directly.
            // range search on ip gets all vectors with inner product greater than or equal to the radius.
            // so we need to do a convertion.
            const roaring::Roaring& origin_row_ids = *params.roaring;
            std::shared_ptr<roaring::Roaring> roaring = std::make_shared<roaring::Roaring>();
            for (size_t i = begin; i < end; ++i) {
                roaring->add(native_search_result.labels[i]);
            }
            result.roaring = std::make_shared<roaring::Roaring>();
            // remove all rows that should not be included.
            *(result.roaring) = origin_row_ids - *roaring;
            // Just update the roaring. distance can not be used.
        } else {
            throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT,
                                   "Unsupported metric type: {}", static_cast<int>(_metric));
        }
    } else {
        if (_metric == Metric::L2) {
            // Faiss can only return labels in the range of radius.
            // If the precidate is not less than, we need to to a convertion.
            const roaring::Roaring& origin_row_ids = *params.roaring;
            std::shared_ptr<roaring::Roaring> roaring = std::make_shared<roaring::Roaring>();
            for (size_t i = begin; i < end; ++i) {
                roaring->add(native_search_result.labels[i]);
            }
            result.roaring = std::make_shared<roaring::Roaring>();
            *(result.roaring) = origin_row_ids - *roaring;
            result.distances = nullptr;
            result.row_ids = nullptr;
        } else if (_metric == Metric::IP) {
            // For inner product, we can use the distance directly.
            // range search on ip gets all vectors with inner product greater than or equal to the radius.
            // when query condition is not le_or_lt, we can use the roaring and distance directly.
            std::unique_ptr<float[]> distances_ptr = std::make_unique<float[]>(end - begin);
            float* distances = distances_ptr.get();
            auto roaring = std::make_shared<roaring::Roaring>();
            // The distance returned by Faiss is actually the squared distance.
            // So we need to take the square root of the squared distance.
            for (size_t i = begin; i < end; ++i) {
                (*row_ids)[i] = native_search_result.labels[i];
                roaring->add(native_search_result.labels[i]);
                distances[i - begin] = native_search_result.distances[i];
            }
            result.distances = std::move(distances_ptr);
            result.row_ids = std::move(row_ids);
            result.roaring = roaring;

            DCHECK(result.row_ids->size() == result.roaring->cardinality())
                    << "row_ids size: " << result.row_ids->size()
                    << ", roaring size: " << result.roaring->cardinality();

        } else {
            throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT,
                                   "Unsupported metric type: {}", static_cast<int>(_metric));
        }
    }

    return Status::OK();
}

doris::Status FaissVectorIndex::save(lucene::store::Directory* dir) {
    auto start_time = std::chrono::high_resolution_clock::now();

    lucene::store::IndexOutput* idx_output = dir->createOutput("faiss.idx");
    auto writer = std::make_unique<FaissIndexWriter>(idx_output);
    faiss::write_index(_index.get(), writer.get());

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    LOG_INFO(fmt::format("Faiss index saved to {}, {}, rows {}, cost {} ms", dir->toString(),
                         "faiss.idx", _index->ntotal, duration.count()));
    return doris::Status::OK();
}

doris::Status FaissVectorIndex::load(lucene::store::Directory* dir) {
    LOG_INFO("Loading Faiss index from: {}", dir->getObjectName());
    auto start_time = std::chrono::high_resolution_clock::now();
    lucene::store::IndexInput* idx_input = dir->openInput("faiss.idx");
    auto reader = std::make_unique<FaissIndexReader>(idx_input);
    faiss::Index* idx = faiss::read_index(reader.get());
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    LOG_INFO("Load index from {} costs {} ms, rows {}", dir->getObjectName(), duration.count(),
             idx->ntotal);
    _index.reset(idx);
    return doris::Status::OK();
}

} // namespace doris::segment_v2