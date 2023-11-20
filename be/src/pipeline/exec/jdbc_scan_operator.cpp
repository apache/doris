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

#include "pipeline/exec/jdbc_scan_operator.h"

#include "common/object_pool.h"
#include "vec/exec/scan/new_jdbc_scanner.h"

namespace doris::pipeline {

Status JDBCScanLocalState::_init_scanners(std::list<vectorized::VScannerSPtr>* scanners) {
    auto& p = _parent->cast<JDBCScanOperatorX>();
    std::unique_ptr<vectorized::NewJdbcScanner> scanner = vectorized::NewJdbcScanner::create_unique(
            state(), this, p._limit_per_scanner, p._tuple_id, p._query_string, p._table_type,
            _scanner_profile.get());
    RETURN_IF_ERROR(scanner->prepare(state(), _conjuncts));
    scanners->push_back(std::move(scanner));
    return Status::OK();
}

JDBCScanOperatorX::JDBCScanOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                                     const DescriptorTbl& descs)
        : ScanOperatorX<JDBCScanLocalState>(pool, tnode, operator_id, descs),
          _table_name(tnode.jdbc_scan_node.table_name),
          _tuple_id(tnode.jdbc_scan_node.tuple_id),
          _query_string(tnode.jdbc_scan_node.query_string),
          _table_type(tnode.jdbc_scan_node.table_type) {
    _output_tuple_id = tnode.jdbc_scan_node.tuple_id;
}

} // namespace doris::pipeline
