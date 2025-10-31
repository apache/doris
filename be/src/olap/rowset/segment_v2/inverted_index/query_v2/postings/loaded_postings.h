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

#include <memory>
#include <vector>

#include "olap/rowset/segment_v2/inverted_index/query_v2/segment_postings.h"

namespace doris::segment_v2::inverted_index::query_v2 {

class LoadedPostings;
using LoadedPostingsPtr = std::shared_ptr<LoadedPostings>;

class LoadedPostings final : public Postings {
public:
    LoadedPostings() = default;
    LoadedPostings(std::vector<uint32_t> doc_ids, std::vector<std::vector<uint32_t>> positions);
    ~LoadedPostings() override = default;

    template <typename TPostings>
    static LoadedPostingsPtr load(TPostings& segment_postings);

    uint32_t advance() override;
    uint32_t seek(uint32_t target) override;
    uint32_t doc() const override;
    uint32_t size_hint() const override;
    uint32_t freq() const override;
    uint32_t norm() const override;

    void append_positions_with_offset(uint32_t offset, std::vector<uint32_t>& output) override;

private:
    std::vector<uint32_t> _doc_ids;
    std::vector<uint32_t> _position_offsets;
    std::vector<uint32_t> _positions;
    size_t _cursor = 0;
};

} // namespace doris::segment_v2::inverted_index::query_v2