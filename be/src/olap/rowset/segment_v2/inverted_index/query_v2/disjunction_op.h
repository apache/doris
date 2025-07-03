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

#include <queue>

#include "olap/rowset/segment_v2/inverted_index/query_v2/operator.h"

namespace doris::segment_v2::idx_query_v2 {

class DisjunctionOp : public Operator {
public:
    DisjunctionOp() = default;
    ~DisjunctionOp() override;

    Status init();

    int32_t doc_id() const;
    int32_t next_doc() const;
    int32_t advance(int32_t target) const;
    int64_t cost() const;

private:
    class DisiWrapper {
    public:
        DisiWrapper() = default;
        ~DisiWrapper() = default;

        int32_t _doc = -1;
        int64_t _cost = 0;
        const Node* _iter = nullptr;
    };

    struct CompareDisiWrapper {
        bool operator()(DisiWrapper* lhs, DisiWrapper* rhs) const { return lhs->_doc > rhs->_doc; }
    };

    int64_t _cost = 0;
    mutable std::priority_queue<DisiWrapper*, std::vector<DisiWrapper*>, CompareDisiWrapper> _pq;
};

using DisjunctionOpPtr = std::shared_ptr<DisjunctionOp>;

} // namespace doris::segment_v2::idx_query_v2