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

#include <gen_cpp/segment_v2.pb.h>

#include <cstdint>
#include <map>
#include <string>

namespace doris::segment_v2 {

#include "common/compile_check_begin.h"

struct VariantStatistics {
    std::map<std::string, int64_t> subcolumns_non_null_size;
    std::map<std::string, uint32_t> sparse_column_non_null_size;
    std::map<std::string, uint32_t> doc_value_column_non_null_size;
    bool has_nested_group = false;

    void to_pb(VariantStatisticsPB* stats) const {
        if (stats == nullptr) {
            return;
        }
        stats->Clear();
        auto* sparse_map = stats->mutable_sparse_column_non_null_size();
        for (const auto& [path, value] : sparse_column_non_null_size) {
            (*sparse_map)[path] = value;
        }
        auto* doc_value_map = stats->mutable_doc_value_column_non_null_size();
        for (const auto& [path, value] : doc_value_column_non_null_size) {
            (*doc_value_map)[path] = value;
        }
        stats->set_has_nested_group(has_nested_group);
    }

    void from_pb(const VariantStatisticsPB& stats) {
        subcolumns_non_null_size.clear();
        sparse_column_non_null_size.clear();
        doc_value_column_non_null_size.clear();
        has_nested_group = stats.has_nested_group();
        for (const auto& [path, value] : stats.sparse_column_non_null_size()) {
            sparse_column_non_null_size[path] = value;
        }
        for (const auto& [path, value] : stats.doc_value_column_non_null_size()) {
            doc_value_column_non_null_size[path] = value;
        }
    }

    bool has_sparse_column_non_null_size() const { return !sparse_column_non_null_size.empty(); }
    bool has_doc_value_column_non_null_size() const {
        return !doc_value_column_non_null_size.empty();
    }

    bool existed_in_sparse_column(const std::string& relative_path) const {
        return sparse_column_non_null_size.contains(relative_path);
    }

    bool existed_in_doc_value_column(const std::string& relative_path) const {
        return doc_value_column_non_null_size.contains(relative_path);
    }

    bool has_prefix_path_in_sparse_column(const std::string& dot_prefix) const {
        auto it = sparse_column_non_null_size.lower_bound(dot_prefix);
        return it != sparse_column_non_null_size.end() && it->first.starts_with(dot_prefix);
    }

    bool has_prefix_path_in_doc_value_column(const std::string& dot_prefix) const {
        auto it = doc_value_column_non_null_size.lower_bound(dot_prefix);
        return it != doc_value_column_non_null_size.end() && it->first.starts_with(dot_prefix);
    }

    bool has_doc_column_non_null_size() const { return has_doc_value_column_non_null_size(); }
    bool existed_in_doc_column(const std::string& relative_path) const {
        return existed_in_doc_value_column(relative_path);
    }
    bool has_prefix_path_in_doc_column(const std::string& dot_prefix) const {
        return has_prefix_path_in_doc_value_column(dot_prefix);
    }
};
#include "common/compile_check_end.h"

} // namespace doris::segment_v2
