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

#include <cstdint>
#include <map>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "core/data_type/data_type.h"
#include "storage/index/inverted/inverted_index_query_type.h"
#include "storage/olap_common.h"

namespace doris::segment_v2 {

struct InvertedIndexSelectionCandidate {
    int64_t index_id;
    InvertedIndexReaderType reader_type;
    std::string analyzer_key;
};

using InvertedIndexSelectionKeyIndex = std::unordered_map<std::string, std::vector<size_t>>;

Status add_inverted_index_selection_candidate(
        InvertedIndexSelectionCandidate candidate,
        std::vector<InvertedIndexSelectionCandidate>* candidates,
        InvertedIndexSelectionKeyIndex* key_index);

[[nodiscard]] Result<size_t> select_best_inverted_index_candidate(
        const std::vector<InvertedIndexSelectionCandidate>& candidates,
        const InvertedIndexSelectionKeyIndex& key_index, FieldType field_type,
        InvertedIndexQueryType query_type, std::string_view normalized_analyzer_key);

FieldType get_inverted_index_leaf_field_type(const DataTypePtr& column_type);

InvertedIndexReaderType infer_inverted_index_reader_type(
        FieldType field_type, const std::map<std::string, std::string>& properties);

} // namespace doris::segment_v2
