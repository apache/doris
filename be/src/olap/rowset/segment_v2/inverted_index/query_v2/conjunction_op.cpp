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

#include "olap/rowset/segment_v2/inverted_index/query_v2/conjunction_op.h"

#include "olap/rowset/segment_v2/inverted_index/query_v2/boolean_query.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/disjunction_op.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/roaring_query.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/term_query.h"

namespace doris::segment_v2::idx_query_v2 {

Status ConjunctionOp::init() {
    if (_childrens.size() < 2) {
        return Status::InternalError("_childrens must contain more than 2 elements.");
    }

    std::sort(_childrens.begin(), _childrens.end(), [](const Node& a, const Node& b) {
        return visit_node(a, Cost {}) < visit_node(b, Cost {});
    });

    _lead1 = _childrens.data();
    _lead2 = _childrens.data() + 1;

    for (int32_t i = 2; i < _childrens.size(); i++) {
        _others.push_back(_childrens.data() + i);
    }

    return Status::OK();
}

int32_t ConjunctionOp::doc_id() const {
    return visit_node(*_lead1, DocId {});
}

int32_t ConjunctionOp::next_doc() const {
    return do_next(visit_node(*_lead1, NextDoc {}));
}

int32_t ConjunctionOp::advance(int32_t target) const {
    return do_next(visit_node(*_lead1, Advance {}, target));
}

int32_t ConjunctionOp::do_next(int32_t doc) const {
    while (true) {
        assert(doc == visit_node(*_lead1, DocId {}));

        int32_t next2 = visit_node(*_lead2, Advance {}, doc);
        if (next2 != doc) {
            doc = visit_node(*_lead1, Advance {}, next2);
            if (next2 != doc) {
                continue;
            }
        }

        bool advance_head = false;
        for (const auto& other : _others) {
            int32_t other_doc_id = visit_node(*other, DocId {});
            if (other_doc_id < doc) {
                int32_t next = visit_node(*other, Advance {}, doc);
                if (next > doc) {
                    doc = visit_node(*_lead1, Advance {}, next);
                    advance_head = true;
                    break;
                }
            }
        }
        if (advance_head) {
            continue;
        }

        return doc;
    }
}

int64_t ConjunctionOp::cost() const {
    return visit_node(*_lead1, Cost {});
}

} // namespace doris::segment_v2::idx_query_v2