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

#include "exec/scan_node.h"

#include "vec/utils/util.hpp"

namespace doris {

const std::string ScanNode::_s_bytes_read_counter = "BytesRead";
const std::string ScanNode::_s_rows_read_counter = "RowsRead";
const std::string ScanNode::_s_total_throughput_counter = "TotalReadThroughput";
const std::string ScanNode::_s_num_disks_accessed_counter = "NumDiskAccess";

Status ScanNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::prepare(state));

    _bytes_read_counter = ADD_COUNTER(runtime_profile(), _s_bytes_read_counter, TUnit::BYTES);
    //TODO: The _rows_read_counter == RowsReturned counter in exec node, there is no need to keep both of them
    _rows_read_counter = ADD_COUNTER(runtime_profile(), _s_rows_read_counter, TUnit::UNIT);
#ifndef BE_TEST
    _total_throughput_counter =
            runtime_profile()->add_rate_counter(_s_total_throughput_counter, _bytes_read_counter);
#endif
    _num_disks_accessed_counter =
            ADD_COUNTER(runtime_profile(), _s_num_disks_accessed_counter, TUnit::UNIT);

    return Status::OK();
}

// This function is used to remove pushed expr in expr tree.
// It relies on the logic of function convertConjunctsToAndCompoundPredicate() of FE splicing expr.
// It requires FE to satisfy each splicing with 'and' expr, and spliced from left to right, in order.
// Expr tree specific forms do not require requirements.
std::string ScanNode::_peel_pushed_vconjunct(RuntimeState* state,
                                             const std::function<bool(int)>& checker) {
    if (_vconjunct_ctx_ptr == nullptr) {
        return "null";
    }

    int leaf_index = 0;
    vectorized::VExpr* conjunct_expr_root = (*_vconjunct_ctx_ptr)->root();

    if (conjunct_expr_root != nullptr) {
        vectorized::VExpr* new_conjunct_expr_root = vectorized::VectorizedUtils::dfs_peel_conjunct(
                state, *_vconjunct_ctx_ptr, conjunct_expr_root, leaf_index, checker);
        if (new_conjunct_expr_root == nullptr) {
            _vconjunct_ctx_ptr.reset(nullptr);
        } else {
            (*_vconjunct_ctx_ptr)->set_root(new_conjunct_expr_root);
            return new_conjunct_expr_root->debug_string();
        }
    }

    return "null";
}

} // namespace doris
