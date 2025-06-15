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

#include <map>
#include <string>

namespace doris {
namespace segment_v2 {

struct VariantStatistics {
    // If reached the size of this, we should stop writing statistics for sparse data
    std::map<std::string, int64_t> subcolumns_non_null_size;
    std::map<std::string, int64_t> sparse_column_non_null_size;

    void to_pb(VariantStatisticsPB* stats) const {
        for (const auto& [path, value] : sparse_column_non_null_size) {
            stats->mutable_sparse_column_non_null_size()->emplace(path, value);
        }
        LOG(INFO) << "num subcolumns " << subcolumns_non_null_size.size() << ", num sparse columns "
                  << sparse_column_non_null_size.size();
    }

    void from_pb(const VariantStatisticsPB& stats) {
        for (const auto& [path, value] : stats.sparse_column_non_null_size()) {
            sparse_column_non_null_size[path] = value;
        }
    }
};
} // namespace segment_v2
} // namespace doris