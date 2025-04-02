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

#pragma once

#include <CLucene.h>
#include <CLucene/store/IndexInput.h>
#include <CLucene/store/IndexOutput.h>
#include <faiss/Index.h>

#include <memory>
#include <string>

#include "common/status.h"
#include "vector_index.h"

struct FaissBuildParameter {
    enum class IndexType { BruteForce, IVF, HNSW };

    enum class Quantilizer { FLAT, SQ, PQ };

    static IndexType string_to_index_type(const std::string& type) {
        if (type == "brute_force") {
            return IndexType::BruteForce;
        } else if (type == "ivf") {
            return IndexType::IVF;
        } else if (type == "hnsw") {
            return IndexType::HNSW;
        }
        return IndexType::HNSW; // default
    }

    static Quantilizer string_to_quantilizer(const std::string& type) {
        if (type == "flat") {
            return Quantilizer::FLAT;
        } else if (type == "sq") {
            return Quantilizer::SQ;
        } else if (type == "pq") {
            return Quantilizer::PQ;
        }
        return Quantilizer::FLAT; // default
    }

    // HNSW
    int d = 0;
    int m = 0;
    IndexType index_type;
    Quantilizer quantilizer;
};

class FaissVectorIndex : public VectorIndex {
public:
    FaissVectorIndex(std::shared_ptr<lucene::store::Directory> dir) : _index(nullptr), _dir(dir) {}

    doris::Status add(int n, const float* vec) override;

    void set_build_params(const FaissBuildParameter& params);

    doris::Status search(const float* query_vec, int k, SearchResult* result,
                         const SearchParameters* params = nullptr) override;

    doris::Status save() override;

    doris::Status load(Metric type) override;

private:
    std::shared_ptr<faiss::Index> _index;

    std::shared_ptr<lucene::store::Directory> _dir;
};
