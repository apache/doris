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

#include "gen_cpp/segment_v2.pb.h"

namespace doris {
namespace segment_v2 {

#include "common/compile_check_begin.h"

struct VariantStatistics {
    std::map<std::string, uint32_t> subcolumns_non_null_size;
    // sparse column non-null size for all buckets
    std::map<std::string, uint32_t> sparse_column_non_null_size;

    // doc snapshot column non-null size for each bucket
    std::unordered_map<uint32_t, std::set<std::string, std::less<>>> doc_snapshot_column_paths;

    void to_pb(VariantStatisticsPB* stats) const {
        auto* sparse_map = stats->mutable_sparse_column_non_null_size();
        for (const auto& [path, value] : sparse_column_non_null_size) {
            (*sparse_map)[path] = value;
        }
    }
};
#include "common/compile_check_end.h"

} // namespace segment_v2
} // namespace doris