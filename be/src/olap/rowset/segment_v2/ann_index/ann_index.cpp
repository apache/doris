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

#include "olap/rowset/segment_v2/ann_index/ann_index.h"

#include "vec/functions/array/function_array_distance.h"

namespace doris::segment_v2 {

std::string metric_to_string(AnnIndexMetric metric) {
    switch (metric) {
    case AnnIndexMetric::L2:
        return vectorized::L2Distance::name;
    case AnnIndexMetric::IP:
        return vectorized::InnerProduct::name;
    default:
        return "UNKNOWN";
    }
}

AnnIndexMetric string_to_metric(const std::string& metric) {
    if (metric == vectorized::L2Distance::name) {
        return AnnIndexMetric::L2;
    } else if (metric == vectorized::InnerProduct::name) {
        return AnnIndexMetric::IP;
    } else {
        return AnnIndexMetric::UNKNOWN;
    }
}

std::string ann_index_type_to_string(AnnIndexType type) {
    switch (type) {
    case AnnIndexType::UNKNOWN:
        return "unknown";
    case AnnIndexType::HNSW:
        return "hnsw";
    default:
        return "unknown";
    }
}

AnnIndexType string_to_ann_index_type(const std::string& type) {
    if (type == "hnsw") {
        return AnnIndexType::HNSW;
    } else {
        return AnnIndexType::UNKNOWN;
    }
}

} // namespace doris::segment_v2