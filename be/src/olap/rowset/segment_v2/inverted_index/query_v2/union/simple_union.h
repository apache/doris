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

template <typename TDocSet>
class SimpleUnion;

template <typename TDocSet>
using SimpleUnionPtr = std::shared_ptr<SimpleUnion<TDocSet>>;

template <typename TDocSet>
class SimpleUnion final : public Postings {
public:
    explicit SimpleUnion(std::vector<TDocSet> docsets);
    ~SimpleUnion() override = default;

    static SimpleUnionPtr<TDocSet> create(std::vector<TDocSet> docsets);

    uint32_t advance() override;
    uint32_t seek(uint32_t target) override;
    uint32_t doc() const override;
    uint32_t size_hint() const override;
    uint32_t freq() const override;
    uint32_t norm() const override;

    void append_positions_with_offset(uint32_t offset, std::vector<uint32_t>& output) override;

    size_t num_docsets() const { return _docsets.size(); }

private:
    void initialize_first_doc_id();
    uint32_t advance_to_next();

    std::vector<TDocSet> _docsets;
    uint32_t _doc;
};

template <typename TDocSet>
auto make_simple_union(std::vector<TDocSet> docsets) {
    return SimpleUnion<TDocSet>::create(std::move(docsets));
}

} // namespace doris::segment_v2::inverted_index::query_v2