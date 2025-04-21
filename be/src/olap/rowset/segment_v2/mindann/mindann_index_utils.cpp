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

#include "mindann_index_utils.h"

namespace doris {
namespace segment_v2 {

static tenann::IndexType convert_to_index_type(const std::string& type_string) {
    const std::string standard_type_string = boost::algorithm::to_lower_copy(type_string);
    if (standard_type_string == "hnsw") {
        return tenann::IndexType::kFaissHnsw;
    } else if (standard_type_string == "ivfflat") {
        return tenann::IndexType::kFaissIvfFlat;
    } else if (standard_type_string == "ivfpq") {
        return tenann::IndexType::kFaissIvfPq;
    } else if (standard_type_string == "hnswpq") {
        return tenann::IndexType::kFaissHnswPq;
    } else {
        return tenann::IndexType::kFaissHnsw;
    }
}

static tenann::MetricType convert_to_metric_type(const std::string& type_string) {
    const std::string standard_type_string = boost::algorithm::to_lower_copy(type_string);
    if (standard_type_string == "cosine_distance") {
        return tenann::MetricType::kCosineDistance;
    } else if (standard_type_string == "cosine_similarity") {
        return tenann::MetricType::kCosineSimilarity;
    } else if (standard_type_string == "inner_product") {
        return tenann::MetricType::kInnerProduct;
    } else if (standard_type_string == "l2_distance") {
        return tenann::MetricType::kL2Distance;
    } else {
       return tenann::MetricType::kL2Distance;
    }
}

tenann::IndexMeta get_vector_meta(const TabletIndex* tablet_index,
                                  const std::map<std::string, std::string>& query_params) {
    tenann::IndexMeta meta;
    meta.SetIndexFamily(tenann::IndexFamily::kVectorIndex);

    std::string param_value;

    const std::map<string, string>& index_params = tablet_index->properties();

    //index_type
    auto it = index_params.find(INDEX_TYPE);
    if (it != index_params.end()) {
        param_value = it->second;
        meta.SetIndexType(convert_to_index_type(param_value));
    }

    if (meta.index_type() == tenann::IndexType::kFaissIvfPq) {
        //nlist
        it = index_params.find(NLIST);
        if (it != index_params.end()) {
            param_value = it->second;
            meta.index_params()[NLIST] = std::atoi(param_value.c_str());
        }

        //M
        it = index_params.find(M);
        if (it != index_params.end()) {
            param_value = it->second;
            meta.index_params()[M] = std::atoi(param_value.c_str());
        }

        //nbits
        it = index_params.find(NBITS);
        if (it != index_params.end()) {
            param_value = it->second;
            meta.index_params()[NBITS] = std::atoi(param_value.c_str());
        }

        // -------- search params --------
        // nprobe
        it = index_params.find(NPROBE);
        if (it != index_params.end()) {
            param_value = it->second;
            meta.search_params()[NPROBE] = std::atoi(param_value.c_str());
        }

    } else if (meta.index_type() == tenann::IndexType::kFaissHnsw || meta.index_type() == tenann::IndexType::kFaissHnswPq) {
        //efConstruction
        it = index_params.find(EF_CONSTRUCTION);
        if (it != index_params.end()) {
            param_value = it->second;
            meta.index_params()[EF_CONSTRUCTION] = std::atoi(param_value.c_str());
        }

        //M
        it = index_params.find(M);
        if (it != index_params.end()) {
            param_value = it->second;
            meta.index_params()[M] = std::atoi(param_value.c_str());
        }

        //efSearch
        it = index_params.find(EF_SEARCH);
        if (it != index_params.end()) {
            param_value = it->second;
            meta.search_params()[EF_SEARCH] = std::atoi(param_value.c_str());
        }

        if (meta.index_type() == tenann::IndexType::kFaissHnswPq) {
            // pqM
            it = index_params.find(PQM);
            if (it != index_params.end()) {
                param_value = it->second;
                meta.index_params()[PQM] = std::atoi(param_value.c_str());
            }
        }
    }

    //metric_type
    it = index_params.find(METRIC_TYPE);
    if (it != index_params.end()) {
        param_value = it->second;
        meta.common_params()[METRIC_TYPE] = convert_to_metric_type(param_value);
    }

    //dim
    it = index_params.find(DIM);
    if (it != index_params.end()) {
        param_value = it->second;
        meta.common_params()[DIM] = std::atoi(param_value.c_str());
    }

    //is_vector_normed
    it = index_params.find(IS_VECTOR_NORMED);
    if (it != index_params.end()) {
        param_value = it->second;
    } else {
        param_value = "false";
    }
    meta.common_params()[IS_VECTOR_NORMED] = boost::algorithm::to_lower_copy(param_value) == "true";

    // support dynamic setting of query parameters
    for (const auto& entry : query_params) {
        if (entry.first == RANGE_SEARCH_CONFIDENCE) {
            meta.search_params()[entry.first] = std::stof(entry.second.c_str());
        } else {
            meta.search_params()[entry.first] = std::atoi(entry.second.c_str());
        }
    }

    return meta;
}

bool is_pq_index(int index_type) {
    return index_type == tenann::IndexType::kFaissHnswPq || index_type == tenann::IndexType::kFaissIvfPq;
}

int64_t get_start_build_threshold_for_pq_index(tenann::IndexMeta index_meta) {
    if (!is_pq_index(index_meta.index_type())) {
        return 0;
    }

    int64_t threshold = 0;
    if (index_meta.index_type() == tenann::IndexType::kFaissHnswPq) {
        // calculate build threshold for hnswpq
        threshold = build_threshold_for_hnswpq;
    } else if (index_meta.index_type() == tenann::IndexType::kFaissIvfPq) {
        // calculate build threshold for ivfpq
        auto nlist_size = int(index_meta.index_params()[NLIST]);
        auto nbits_size = 1 << int(index_meta.index_params()[NBITS]);
        threshold = std::max(nlist_size, nbits_size);
    }
    
    return threshold;
}

}
}