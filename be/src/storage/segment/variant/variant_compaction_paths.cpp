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

#include "storage/segment/variant/variant_compaction_paths.h"

#include "storage/index/index_writer.h"

namespace doris {

namespace {

const TabletIndexes* indexes_of_path(const VariantCompactionPaths& paths, const TabletColumn& col,
                                     const std::string& relative_path) {
    if (col.path_info_ptr()->get_is_typed()) {
        auto it = paths.typed_path_set.find(relative_path);
        return it == paths.typed_path_set.end() ? nullptr : &it->second.indexes;
    }
    auto it = paths.subcolumn_indexes.find(relative_path);
    return it == paths.subcolumn_indexes.end() ? nullptr : &it->second;
}

} // namespace

std::vector<const TabletIndex*> variant_subcolumn_indexes(const VariantCompactionPathsMap* paths,
                                                          const TabletColumn& col) {
    // Some extracted types (JSONB, a nested variant, an array of them) cannot carry one.
    if (paths == nullptr || !col.is_extracted_column() ||
        !segment_v2::IndexColumnWriter::check_support_inverted_index(col)) {
        return {};
    }
    auto column_paths = paths->find(col.parent_unique_id());
    if (column_paths == paths->end()) {
        return {};
    }
    const TabletIndexes* indexes = indexes_of_path(
            column_paths->second, col, col.path_info_ptr()->copy_pop_front().get_path());
    if (indexes == nullptr) {
        return {};
    }
    std::vector<const TabletIndex*> result;
    result.reserve(indexes->size());
    for (const auto& index : *indexes) {
        result.push_back(index.get());
    }
    return result;
}

} // namespace doris
