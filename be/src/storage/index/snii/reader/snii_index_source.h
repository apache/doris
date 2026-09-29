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
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "storage/index/query/spi/index_source.h"
#include "storage/index/snii/format/norms_pod.h"
#include "storage/index/snii/reader/logical_index_reader.h"

namespace doris::snii::reader {

// A logical SNII index as the engine's source: terms resolved through the dictionary, a batch
// ahead when prepared, and postings read by SniiPostingsCursor.
class SniiIndexSource final : public index_query::IndexSource {
public:
    explicit SniiIndexSource(const LogicalIndexReader& idx);

    uint32_t doc_count() const override;
    Status prepare_terms(std::span<const std::string> terms) override;
    Status open_term(std::string_view term, bool positions, bool scoring,
                     std::unique_ptr<index_query::PostingsCursor>* out) override;
    Status expand_terms(index_query::TermPattern& pattern, int32_t max_expansions,
                        std::vector<std::string>* out) override;

    const LogicalIndexReader& index() const { return _idx; }

private:
    Status _resolve(std::string_view term, LogicalIndexReader::BatchLookupResult* out);
    Status _open_norms(const format::NormsPodReader** out);

    const LogicalIndexReader& _idx;
    std::unordered_map<std::string, LogicalIndexReader::BatchLookupResult> _prepared;
    format::NormsPodReader _norms;
    bool _norms_opened = false;
};

} // namespace doris::snii::reader
