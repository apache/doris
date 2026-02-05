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

#include "olap/rowset/segment_v2/inverted_index/query_v2/bit_set_query/bit_set_weight.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/query.h"
#include "roaring/roaring.hh"

namespace doris::segment_v2::inverted_index::query_v2 {

class BitSetQuery : public Query {
public:
    explicit BitSetQuery(std::shared_ptr<roaring::Roaring> bitmap) : _bitmap(std::move(bitmap)) {}
    BitSetQuery(const roaring::Roaring& bitmap)
            : _bitmap(std::make_shared<roaring::Roaring>(bitmap)) {}
    ~BitSetQuery() override = default;

    WeightPtr weight(bool /*enable_scoring*/) override {
        return std::make_shared<BitSetWeight>(_bitmap);
    }

private:
    std::shared_ptr<roaring::Roaring> _bitmap;
};

using BitSetQueryPtr = std::shared_ptr<BitSetQuery>;

} // namespace doris::segment_v2::inverted_index::query_v2