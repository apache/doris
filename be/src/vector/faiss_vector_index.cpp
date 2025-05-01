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

#include <array>
#include <cstdint>
#include <memory>

#include "CLucene/store/IndexInput.h"
#include "CLucene/store/IndexOutput.h"
#include "common/exception.h"
#include "common/logging.h"
#include "common/status.h"
#include "faiss/IndexHNSW.h"
#include "faiss/impl/io.h"

namespace doris::segment_v2 {

std::unique_ptr<faiss::IDSelector> FaissVectorIndex::roaring_to_faiss_selector(
        const roaring::Roaring& bitmap) {
    std::vector<faiss::idx_t> ids;
    ids.reserve(bitmap.cardinality());

    for (roaring::Roaring::const_iterator it = bitmap.begin(); it != bitmap.end(); ++it) {
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
    if (params.index_type == FaissBuildParameter::IndexType::BruteForce) {
        _index = std::make_unique<faiss::IndexFlatL2>(params.d);
    } else if (params.index_type == FaissBuildParameter::IndexType::HNSW) {
        _index = std::make_unique<faiss::IndexHNSWFlat>(params.d, params.m);
    } else {
        throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT, "Unsupported index type: {}",
                               static_cast<int>(params.index_type));
    }
}

doris::Status FaissVectorIndex::search(const float* query_vec, int k,
                                       const SearchParameters& params, SearchResult& result) {
    std::unique_ptr<float[]> distances_ptr = std::make_unique<float[]>(k);
    float* distances = distances_ptr.get();

    // Initialize labels with -1
    // Even if there are N vectors in the index, limit N search in faiss could return less than N(eg, HNSW)
    // so we need to initialize labels with -1 to tell the end of the result ids.
    std::unique_ptr<std::vector<faiss::idx_t>> labels_ptr =
            std::make_unique<std::vector<faiss::idx_t>>(k, -1);
    faiss::idx_t* labels = (*labels_ptr).data();

    if (params.row_ids == nullptr) {
        _index->search(1, query_vec, k, distances, labels);
    } else {
        std::unique_ptr<faiss::IDSelector> id_sel = nullptr;
        id_sel = roaring_to_faiss_selector(*params.row_ids);
        faiss::SearchParameters param;
        param.sel = id_sel.get();

        _index->search(1, query_vec, k, distances, labels, &param);
    }

    result.row_ids = std::make_shared<roaring::Roaring>();
    update_roaring(labels, k, *result.row_ids);

    result.distances = std::move(distances_ptr);

    return doris::Status::OK();
}

doris::Status FaissVectorIndex::save(lucene::store::Directory* dir) {
    lucene::store::IndexOutput* idx_output = dir->createOutput("faiss.idx");
    auto writer = std::make_unique<FaissIndexWriter>(idx_output);
    faiss::write_index(_index.get(), writer.get());
    VLOG_DEBUG << fmt::format("Faiss index saved to faiss.idx, rows {}", _index->ntotal);
    return doris::Status::OK();
}

doris::Status FaissVectorIndex::load(lucene::store::Directory* dir) {
    lucene::store::IndexInput* idx_input = dir->openInput("faiss.idx");
    auto reader = std::make_unique<FaissIndexReader>(idx_input);
    faiss::Index* idx = faiss::read_index(reader.get());
    _index.reset(idx);
    return doris::Status::OK();
}

} // namespace doris::segment_v2