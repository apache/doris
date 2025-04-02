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

#include <memory>

#include "CLucene/store/IndexOutput.h"
#include "common/exception.h"
#include "common/logging.h"
#include "common/status.h"
#include "faiss/IndexHNSW.h"
#include "faiss/impl/io.h"

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

doris::Status FaissVectorIndex::add(int n, const float* vec) {
    DCHECK(vec != nullptr);

    _index->add(n, vec);

    return doris::Status::OK();
}

void FaissVectorIndex::set_build_params(const FaissBuildParameter& params) {
    if (params.index_type == FaissBuildParameter::IndexType::BruteForce) {
        _index = std::make_shared<faiss::IndexFlatL2>(params.d);
    } else if (params.index_type == FaissBuildParameter::IndexType::HNSW) {
        _index = std::make_shared<faiss::IndexHNSWFlat>(params.d, params.m);
    } else {
        throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT, "Unsupported index type: {}",
                               static_cast<int>(params.index_type));
    }
}

doris::Status FaissVectorIndex::search(const float* query_vec, int k, SearchResult* result,
                                       const SearchParameters* params) {
    return doris::Status::OK();
}

doris::Status FaissVectorIndex::save() {
    lucene::store::IndexOutput* idx_output = _dir->createOutput("faiss.idx");
    auto writer = std::make_unique<FaissIndexWriter>(idx_output);
    faiss::write_index(_index.get(), writer.get());
    VLOG_DEBUG << fmt::format("Faiss index saved to faiss.idx, rows {}", _index->ntotal);
    return doris::Status::OK();
}
doris::Status FaissVectorIndex::load(Metric type) {
    // Load the index from the directory
    // This is a placeholder for actual implementation
    return doris::Status::OK();
}