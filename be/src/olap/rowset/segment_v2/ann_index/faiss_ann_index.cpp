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

#include "faiss_ann_index.h"

#include <faiss/index_io.h>
#include <omp.h>

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>

#include "CLucene/store/IndexInput.h"
#include "CLucene/store/IndexOutput.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/logging.h"
#include "common/status.h"
#include "faiss/IndexHNSW.h"
#include "faiss/MetricType.h"
#include "faiss/impl/IDSelector.h"
#include "faiss/impl/io.h"
#include "olap/rowset/segment_v2/ann_index/ann_index.h"
#include "olap/rowset/segment_v2/ann_index/ann_index_files.h"
#include "olap/rowset/segment_v2/ann_index/ann_search_params.h"
#include "util/time.h"
#include "vec/core/types.h"

namespace doris::segment_v2 {
#include "common/compile_check_begin.h"
std::unique_ptr<faiss::IDSelector> FaissVectorIndex::roaring_to_faiss_selector(
        const roaring::Roaring& roaring) {
    std::vector<faiss::idx_t> ids;
    ids.resize(roaring.cardinality());

    size_t i = 0;
    for (roaring::Roaring::const_iterator it = roaring.begin(); it != roaring.end(); ++it, ++i) {
        ids[i] = cast_set<faiss::idx_t>(*it);
    }
    // construct derived and wrap into base unique_ptr explicitly
    return std::unique_ptr<faiss::IDSelector>(new faiss::IDSelectorBatch(ids.size(), ids.data()));
}

void FaissVectorIndex::update_roaring(const faiss::idx_t* labels, const size_t n,
                                      roaring::Roaring& roaring) {
    // make sure roaring is empty before adding new elements
    DCHECK(roaring.cardinality() == 0);
    for (size_t i = 0; i < n; ++i) {
        if (labels[i] >= 0) {
            roaring.add(cast_set<vectorized::UInt32>(labels[i]));
        }
    }
}

FaissVectorIndex::FaissVectorIndex() : _index(nullptr) {}

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
            const auto* data = reinterpret_cast<const uint8_t*>(ptr);
            // CLucene IndexOutput::writeBytes accepts at most Int32 bytes at a time.
            const size_t kMaxChunk =
                    static_cast<size_t>(std::numeric_limits<vectorized::Int32>::max());
            size_t written = 0;
            while (written < bytes) {
                size_t to_write = bytes - written;
                if (to_write > kMaxChunk) to_write = kMaxChunk;
                try {
                    _output->writeBytes(data + written, cast_set<vectorized::Int32>(to_write));
                } catch (const std::exception& e) {
                    throw doris::Exception(doris::ErrorCode::IO_ERROR,
                                           "Failed to write vector index {}", e.what());
                }
                written += to_write;
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
            auto* data = reinterpret_cast<uint8_t*>(ptr);
            const size_t kMaxChunk =
                    static_cast<size_t>(std::numeric_limits<vectorized::Int32>::max());
            size_t read = 0;
            while (read < bytes) {
                size_t to_read = bytes - read;
                if (to_read > kMaxChunk) to_read = kMaxChunk;
                try {
                    _input->readBytes(data + read, cast_set<vectorized::Int32>(to_read));
                } catch (const std::exception& e) {
                    throw doris::Exception(doris::ErrorCode::IO_ERROR,
                                           "Failed to read vector index {}", e.what());
                }
                read += to_read;
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
    omp_set_num_threads(config::omp_threads_limit);
    _index->add(n, vec);
    return doris::Status::OK();
}

void FaissVectorIndex::build(const FaissBuildParameter& params) {
    _dimension = params.dim;
    switch (params.metric_type) {
    case FaissBuildParameter::MetricType::L2:
        _metric = AnnIndexMetric::L2;
        break;
    case FaissBuildParameter::MetricType::IP:
        _metric = AnnIndexMetric::IP;
        break;
    default:
        throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT, "Unsupported metric type: {}",
                               static_cast<int>(params.metric_type));
        break;
    }

    if (params.index_type == FaissBuildParameter::IndexType::HNSW) {
        if (params.metric_type == FaissBuildParameter::MetricType::L2) {
            _index = std::make_unique<faiss::IndexHNSWFlat>(params.dim, params.max_degree,
                                                            faiss::METRIC_L2);
        } else if (params.metric_type == FaissBuildParameter::MetricType::IP) {
            _index = std::make_unique<faiss::IndexHNSWFlat>(params.dim, params.max_degree,
                                                            faiss::METRIC_INNER_PRODUCT);
        } else {
            throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT,
                                   "Unsupported metric type: {}",
                                   static_cast<int>(params.metric_type));
        }
    } else {
        throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT, "Unsupported index type: {}",
                               static_cast<int>(params.index_type));
    }
}

// TODO: Support batch search
doris::Status FaissVectorIndex::ann_topn_search(const float* query_vec, int k,
                                                const segment_v2::IndexSearchParameters& params,
                                                segment_v2::IndexSearchResult& result) {
    std::unique_ptr<float[]> distances_ptr;
    std::unique_ptr<std::vector<faiss::idx_t>> labels_ptr;
    {
        SCOPED_RAW_TIMER(&result.engine_prepare_ns);
        distances_ptr = std::make_unique<float[]>(k);
        // Initialize labels with -1
        // Even if there are N vectors in the index, limit N search in faiss could return less than N(eg, HNSW)
        // so we need to initialize labels with -1 to tell the end of the result ids.
        labels_ptr = std::make_unique<std::vector<faiss::idx_t>>(k, -1);
    }
    float* distances = distances_ptr.get();
    faiss::idx_t* labels = (*labels_ptr).data();
    DCHECK(params.roaring != nullptr)
            << "Roaring should not be null for topN search, please set roaring in params";

    faiss::SearchParametersHNSW param;
    const HNSWSearchParameters* hnsw_params = dynamic_cast<const HNSWSearchParameters*>(&params);
    if (hnsw_params == nullptr) {
        return doris::Status::InvalidArgument(
                "HNSW search parameters should not be null for HNSW index");
    }
    param.efSearch = hnsw_params->ef_search;
    param.check_relative_distance = hnsw_params->check_relative_distance;
    param.bounded_queue = hnsw_params->bounded_queue;
    param.sel = nullptr;
    std::unique_ptr<faiss::IDSelector> id_sel = nullptr;
    // Costs of roaring to faiss selector is very high especially when the cardinality is very high.
    if (params.roaring->cardinality() != params.rows_of_segment) {
        LOG_INFO("Roaring to faiss selector, roaring {} rows, segment {} rows",
                 params.roaring->cardinality(), params.rows_of_segment);
        {
            SCOPED_RAW_TIMER(&result.engine_prepare_ns);
            id_sel = roaring_to_faiss_selector(*params.roaring);
        }
        param.sel = id_sel.get();
    }
    {
        SCOPED_RAW_TIMER(&result.engine_search_ns);
        _index->search(1, query_vec, k, distances, labels, &param);
    }
    {
        SCOPED_RAW_TIMER(&result.engine_convert_ns);
        result.roaring = std::make_shared<roaring::Roaring>();
        update_roaring(labels, k, *result.roaring);
        size_t roaring_cardinality = result.roaring->cardinality();
        result.distances = std::make_unique<float[]>(roaring_cardinality);
        result.row_ids = std::make_unique<std::vector<uint64_t>>();
        result.row_ids->resize(roaring_cardinality);

        if (_metric == AnnIndexMetric::L2) {
            // For l2_distance, we need to convert the distance to the actual distance.
            // The distance returned by Faiss is actually the squared distance.
            // So we need to take the square root of the squared distance.
            for (size_t i = 0; i < roaring_cardinality; ++i) {
                (*result.row_ids)[i] = labels[i];
                result.distances[i] = std::sqrt(distances[i]);
            }
        } else if (_metric == AnnIndexMetric::IP) {
            // For inner product, we can use the distance directly.
            for (size_t i = 0; i < roaring_cardinality; ++i) {
                (*result.row_ids)[i] = labels[i];
                result.distances[i] = distances[i];
            }
        } else {
            throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT,
                                   "Unsupported metric type: {}", static_cast<int>(_metric));
        }

        DCHECK(result.row_ids->size() == result.roaring->cardinality())
                << "Row ids size: " << result.row_ids->size()
                << ", roaring size: " << result.roaring->cardinality();
    }
    // distance/row_ids conversion above already timed via SCOPED_RAW_TIMER
    return doris::Status::OK();
}

// For l2 distance, range search radius is the squared distance.
// For inner product, range search radius is the actual distance.
// range search on inner product returns all vectors with inner product greater than or equal to the radius.
// For l2 distance, range search returns all vectors with squared distance less than or equal to the radius.
doris::Status FaissVectorIndex::range_search(const float* query_vec, const float& radius,
                                             const segment_v2::IndexSearchParameters& params,
                                             segment_v2::IndexSearchResult& result) {
    DCHECK(_index != nullptr);
    DCHECK(query_vec != nullptr);
    DCHECK(params.roaring != nullptr)
            << "Roaring should not be null for range search, please set roaring in params";
    std::unique_ptr<faiss::IDSelector> sel;
    {
        // Engine prepare: convert roaring bitmap to FAISS selector
        SCOPED_RAW_TIMER(&result.engine_prepare_ns);
        sel = roaring_to_faiss_selector(*params.roaring);
    }
    faiss::RangeSearchResult native_search_result(1, true);
    const HNSWSearchParameters* hnsw_params = dynamic_cast<const HNSWSearchParameters*>(&params);
    // Currently only support HNSW index for range search.
    DCHECK(hnsw_params != nullptr) << "HNSW search parameters should not be null for HNSW index";

    faiss::SearchParametersHNSW param;
    {
        // Engine prepare: set search parameters and bind selector
        SCOPED_RAW_TIMER(&result.engine_prepare_ns);
        param.efSearch = hnsw_params->ef_search;
        param.check_relative_distance = hnsw_params->check_relative_distance;
        param.bounded_queue = hnsw_params->bounded_queue;
        param.sel = sel.get();
    }
    {
        // Engine search: FAISS range_search
        SCOPED_RAW_TIMER(&result.engine_search_ns);
        if (_metric == AnnIndexMetric::L2) {
            if (radius <= 0) {
                _index->range_search(1, query_vec, 0.0f, &native_search_result, &param);
            } else {
                _index->range_search(1, query_vec, radius * radius, &native_search_result, &param);
            }
        } else if (_metric == AnnIndexMetric::IP) {
            _index->range_search(1, query_vec, radius, &native_search_result, &param);
        }
    }

    size_t begin = native_search_result.lims[0];
    size_t end = native_search_result.lims[1];
    auto row_ids = std::make_unique<std::vector<uint64_t>>();
    row_ids->resize(end - begin);
    if (params.is_le_or_lt) {
        if (_metric == AnnIndexMetric::L2) {
            std::unique_ptr<float[]> distances_ptr;
            float* distances = nullptr;
            auto roaring = std::make_shared<roaring::Roaring>();
            {
                // Engine convert: build roaring, row_ids, distances from FAISS result
                SCOPED_RAW_TIMER(&result.engine_convert_ns);
                distances_ptr = std::make_unique<float[]>(end - begin);
                distances = distances_ptr.get();
                // The distance returned by Faiss is actually the squared distance.
                // So we need to take the square root of the squared distance.
                for (size_t i = begin; i < end; ++i) {
                    (*row_ids)[i] = native_search_result.labels[i];
                    roaring->add(cast_set<vectorized::UInt32>(native_search_result.labels[i]));
                    distances[i - begin] = sqrt(native_search_result.distances[i]);
                }
            }
            result.distances = std::move(distances_ptr);
            result.row_ids = std::move(row_ids);
            result.roaring = roaring;

            DCHECK(result.row_ids->size() == result.roaring->cardinality())
                    << "row_ids size: " << result.row_ids->size()
                    << ", roaring size: " << result.roaring->cardinality();
        } else if (_metric == AnnIndexMetric::IP) {
            // For IP, we can use the distance directly.
            // range search on ip gets all vectors with inner product greater than or equal to the radius.
            // so we need to do a convertion.
            const roaring::Roaring& origin_row_ids = *params.roaring;
            std::shared_ptr<roaring::Roaring> roaring = std::make_shared<roaring::Roaring>();
            {
                // Engine convert: compute roaring difference
                SCOPED_RAW_TIMER(&result.engine_convert_ns);
                for (size_t i = begin; i < end; ++i) {
                    roaring->add(cast_set<vectorized::UInt32>(native_search_result.labels[i]));
                }
                result.roaring = std::make_shared<roaring::Roaring>();
                // remove all rows that should not be included.
                *(result.roaring) = origin_row_ids - *roaring;
                // Just update the roaring. distance can not be used.
            }
        } else {
            throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT,
                                   "Unsupported metric type: {}", static_cast<int>(_metric));
        }
    } else {
        if (_metric == AnnIndexMetric::L2) {
            // Faiss can only return labels in the range of radius.
            // If the precidate is not less than, we need to to a convertion.
            const roaring::Roaring& origin_row_ids = *params.roaring;
            std::shared_ptr<roaring::Roaring> roaring = std::make_shared<roaring::Roaring>();
            {
                // Engine convert: compute roaring difference
                SCOPED_RAW_TIMER(&result.engine_convert_ns);
                for (size_t i = begin; i < end; ++i) {
                    roaring->add(cast_set<vectorized::UInt32>(native_search_result.labels[i]));
                }
                result.roaring = std::make_shared<roaring::Roaring>();
                *(result.roaring) = origin_row_ids - *roaring;
                result.distances = nullptr;
                result.row_ids = nullptr;
            }
        } else if (_metric == AnnIndexMetric::IP) {
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
                roaring->add(cast_set<vectorized::UInt32>(native_search_result.labels[i]));
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

    lucene::store::IndexOutput* idx_output = dir->createOutput(faiss_index_fila_name);
    auto writer = std::make_unique<FaissIndexWriter>(idx_output);
    faiss::write_index(_index.get(), writer.get());

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    LOG_INFO(fmt::format("Faiss index saved to {}, {}, rows {}, cost {} ms", dir->toString(),
                         faiss_index_fila_name, _index->ntotal, duration.count()));
    return doris::Status::OK();
}

doris::Status FaissVectorIndex::load(lucene::store::Directory* dir) {
    auto start_time = std::chrono::high_resolution_clock::now();
    lucene::store::IndexInput* idx_input = nullptr;
    try {
        idx_input = dir->openInput(faiss_index_fila_name);
    } catch (const CLuceneError& e) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                "Failed to open index file: {}, error: {}", faiss_index_fila_name, e.what());
    }

    auto reader = std::make_unique<FaissIndexReader>(idx_input);
    faiss::Index* idx = faiss::read_index(reader.get());
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    VLOG_DEBUG << fmt::format("Load index from {} costs {} ms, rows {}", dir->getObjectName(),
                              duration.count(), idx->ntotal);
    _index.reset(idx);
    return doris::Status::OK();
}

} // namespace doris::segment_v2