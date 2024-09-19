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

#include <vector>

#include "olap/rowset/segment_v2/inverted_index/query_v2/node.h"

namespace doris::segment_v2::idx_query_v2 {

enum class OperatorType { OP_AND = 0, OP_OR };

class Operator;
using OperatorPtr = std::shared_ptr<Operator>;

class Operator {
public:
    Operator() = default;
    virtual ~Operator() = default;

    Status add_child(const Node& clause) {
        _childrens.emplace_back(clause);
        return Status::OK();
    }

protected:
    std::vector<Node> _childrens;
};

} // namespace doris::segment_v2::idx_query_v2