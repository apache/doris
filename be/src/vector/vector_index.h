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

#include "common/status.h"

struct SearchResult {
    int rows;
    std::vector<float> distances;
    std::vector<int64_t> ids;
    void* stat; //统计分析

    SearchResult() {
        rows = 0;
        stat = nullptr;
    }
    float get_distance(int idx) {
        if (idx < 0 || idx >= static_cast<int>(distances.size())) {
            throw std::out_of_range("Invalid distance index");
        }
        return distances[idx];
    }
    int64_t get_id(int idx) {
        if (idx < 0 || idx >= static_cast<int>(ids.size())) {
            throw std::out_of_range("Invalid ID index");
        }
        return ids[idx];
    }
    void reset() {
        rows = 0;
        distances.clear();
        ids.clear();
    }
    bool has_rows() { return rows > 0; }
    int row_count() { return rows; }
};

struct SearchParameters {
    virtual ~SearchParameters() {}
};

class VectorIndex {
public:
    enum class Metric { L2, COSINE, INNER_PRODUCT, UNKNOWN };

    virtual doris::Status add(int n, const float* vec) = 0;

    virtual doris::Status search(const float* query_vec, int k, SearchResult* result,
                                 const SearchParameters* params = nullptr) = 0;
    //virtual Status save(FileWriter* writer);
    virtual doris::Status save() = 0;

    //virtual Status load(FileReader* reader);
    virtual doris::Status load(Metric type) = 0;
    //void reset();
    static std::string metric_to_string(Metric metric) {
        switch (metric) {
        case Metric::L2:
            return "L2";
        case Metric::COSINE:
            return "COSINE";
        case Metric::INNER_PRODUCT:
            return "INNER_PRODUCT";
        default:
            return "UNKNOWN";
        }
    }
    static Metric string_to_metric(const std::string& metric) {
        if (metric == "l2") {
            return Metric::L2;
        } else if (metric == "cosine") {
            return Metric::COSINE;
        } else if (metric == "inner_product") {
            return Metric::INNER_PRODUCT;
        } else {
            return Metric::UNKNOWN;
        }
    }
    virtual ~VectorIndex() = default;
};