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

#include "olap/rowset/segment_v2/inverted_index/query_v2/factory.h"

#include "olap/rowset/segment_v2/inverted_index/query_v2/conjunction_op.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/disjunction_op.h"

namespace doris::segment_v2::idx_query_v2 {

Result<Node> OperatorFactory::create(OperatorType query_type) {
    switch (query_type) {
    case OperatorType::OP_AND:
        return std::make_shared<ConjunctionOp>();
    case OperatorType::OP_OR:
        return std::make_shared<DisjunctionOp>();
    default:
        return ResultError(Status::InternalError("failed create operator: {}", query_type));
    }
}

} // namespace doris::segment_v2::idx_query_v2