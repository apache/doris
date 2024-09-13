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

#include "olap/rowset/segment_v2/inverted_index/query_v2/node.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/query.h"

namespace doris::segment_v2::idx_query_v2 {

enum class OperatorType;
class BooleanQuery : public Query {
public:
    BooleanQuery(Node op);
    ~BooleanQuery() override = default;

    void execute(const std::shared_ptr<roaring::Roaring>& result);

    int32_t doc_id() const;
    int32_t next_doc() const;
    int32_t advance(int32_t target) const;
    int64_t cost() const;

    class Builder {
    public:
        Status set_op(OperatorType type);
        Status add(const Node& clause);
        Result<Node> build();

    private:
        Node _op;
    };

private:
    bool should_use_skip();
    void search_by_skiplist(const std::shared_ptr<roaring::Roaring>& result);
    void search_by_bitmap(const std::shared_ptr<roaring::Roaring>& result);

    Node _op;
};

} // namespace doris::segment_v2::idx_query_v2