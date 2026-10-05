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

#include "storage/index/ann/ann_index.h"

#include "common/metrics/doris_metrics.h"
#include "exprs/function/array/function_array_distance.h"

namespace doris::segment_v2 {

std::string metric_to_string(AnnIndexMetric metric) {
    switch (metric) {
    case AnnIndexMetric::L2:
        return L2Distance::name;
    case AnnIndexMetric::IP:
        return InnerProduct::name;
    default:
        return "UNKNOWN";
    }
}

AnnIndexMetric string_to_metric(const std::string& metric) {
    if (metric == L2Distance::name) {
        return AnnIndexMetric::L2;
    } else if (metric == InnerProduct::name) {
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
    case AnnIndexType::IVF:
        return "ivf";
    case AnnIndexType::IVF_ON_DISK:
        return "ivf_on_disk";
    default:
        return "unknown";
    }
}

AnnIndexType string_to_ann_index_type(const std::string& type) {
    if (type == "hnsw") {
        return AnnIndexType::HNSW;
    } else if (type == "ivf") {
        return AnnIndexType::IVF;
    } else if (type == "ivf_on_disk") {
        return AnnIndexType::IVF_ON_DISK;
    } else {
        return AnnIndexType::UNKNOWN;
    }
}

VectorIndex::VectorIndex() {
    DorisMetrics::instance()->ann_index_in_memory_cnt->increment(1);
}

VectorIndex::~VectorIndex() {
    DorisMetrics::instance()->ann_index_in_memory_cnt->increment(-1);
}

} // namespace doris::segment_v2
