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

#include "olap/rowset/segment_v2/inverted_index/query_v2/disjunction_op.h"

#include "olap/rowset/segment_v2/inverted_index/query_v2/boolean_query.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/conjunction_op.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/node.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/roaring_query.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/term_query.h"

namespace doris::segment_v2::idx_query_v2 {

DisjunctionOp::~DisjunctionOp() {
    while (!_pq.empty()) {
        auto* w = _pq.top();
        if (w != nullptr) {
            delete w;
            w = nullptr;
        }
        _pq.pop();
    }
}

Status DisjunctionOp::init() {
    if (_childrens.size() < 2) {
        return Status::InternalError("_childrens must contain more than 2 elements.");
    }

    for (const auto& child : _childrens) {
        auto* w = new DisiWrapper();
        w->_iter = &child;
        w->_cost = visit_node(*w->_iter, Cost {});
        this->_cost += w->_cost;
        _pq.push(w);
    }

    return Status::OK();
}

int32_t DisjunctionOp::doc_id() const {
    return visit_node(*_pq.top()->_iter, DocId {});
}

int32_t DisjunctionOp::next_doc() const {
    auto* top = _pq.top();
    int32_t doc = top->_doc;
    do {
        _pq.pop();
        top->_doc = visit_node(*top->_iter, NextDoc {});
        _pq.push(top);
        top = _pq.top();
    } while (top->_doc == doc);
    return top->_doc;
}

int32_t DisjunctionOp::advance(int32_t target) const {
    auto* top = _pq.top();
    do {
        _pq.pop();
        top->_doc = visit_node(*top->_iter, Advance {}, target);
        _pq.push(top);
        top = _pq.top();
    } while (top->_doc < target);
    return top->_doc;
}

int64_t DisjunctionOp::cost() const {
    return _cost;
}

} // namespace doris::segment_v2::idx_query_v2