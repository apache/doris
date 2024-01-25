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

#include "mysql_scan_operator.h"

#include "vec/exec/vmysql_scan_node.h"

namespace doris::pipeline {

OPERATOR_CODE_GENERATOR(MysqlScanOperator, SourceOperator)

Status MysqlScanOperator::open(RuntimeState* state) {
    RETURN_IF_ERROR(SourceOperator::open(state));
    return _node->open(state);
}

Status MysqlScanOperator::close(RuntimeState* state) {
    RETURN_IF_ERROR(SourceOperator::close(state));
    static_cast<void>(_node->close(state));
    return Status::OK();
}

} // namespace doris::pipeline
