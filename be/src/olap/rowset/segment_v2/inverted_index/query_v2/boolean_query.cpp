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

#include "olap/rowset/segment_v2/inverted_index/query_v2/boolean_query.h"

#include "olap/rowset/segment_v2/inverted_index/query_v2/conjunction_op.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/disjunction_op.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/factory.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/node.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/operator.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/roaring_query.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/term_query.h"

namespace doris::segment_v2::idx_query_v2 {

Status BooleanQuery::Builder::set_op(OperatorType type) {
    _op = DORIS_TRY(OperatorFactory::create(type));
    return Status::OK();
}

Status BooleanQuery::Builder::add(const Node& clause) {
    return visit_node(_op, OpAddChild {}, clause);
}

Result<Node> BooleanQuery::Builder::build() {
    auto query_ptr = std::make_shared<BooleanQuery>(std::move(_op));
    auto status = visit_node(query_ptr->_op, OpInit {});
    if (!status.ok()) {
        return ResultError(std::move(status));
    }
    return query_ptr;
}

BooleanQuery::BooleanQuery(Node op) : _op(std::move(op)) {}

void BooleanQuery::execute(const std::shared_ptr<roaring::Roaring>& result) {
    bool use_skip = should_use_skip();

    if (use_skip) {
        search_by_skiplist(result);
    } else {
        search_by_bitmap(result);
    }
}

int32_t BooleanQuery::doc_id() const {
    return visit_node(_op, DocId {});
}

int32_t BooleanQuery::next_doc() const {
    return visit_node(_op, NextDoc {});
}

int32_t BooleanQuery::advance(int32_t target) const {
    return visit_node(_op, Advance {}, target);
}

int64_t BooleanQuery::cost() const {
    return visit_node(_op, Cost {});
}

bool BooleanQuery::should_use_skip() {
    return visit_node(_op, Cost {});
}

void BooleanQuery::search_by_skiplist(const std::shared_ptr<roaring::Roaring>& result) {
    auto _next_doc = [](const auto& node) { return node->next_doc(); };

    int32_t doc = 0;
    while ((doc = visit_node(_op, _next_doc)) != INT32_MAX) {
        result->add(doc);
    }
}

void BooleanQuery::search_by_bitmap(const std::shared_ptr<roaring::Roaring>& result) {}

} // namespace doris::segment_v2::idx_query_v2