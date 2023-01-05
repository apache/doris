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

#include "vmeta_scanner.h"

namespace doris::vectorized {

VMetaScanner::VMetaScanner(RuntimeState* state, VMetaScanNode* parent, const TupleId& tuple_id, int64_t limit)
    : VScanner(state, static_cast<VScanNode*>(parent), limit) {
}

Status VMetaScanner::open(RuntimeState* state) {
    RETURN_IF_ERROR(VScanner::open(state));
    return Status::OK();
}

Status VMetaScanner::prepare(RuntimeState* state, VExprContext** vconjunct_ctx_ptr) {
    return Status::OK();
}

Status VMetaScanner::_get_block_impl(RuntimeState* state, Block* block, bool* eof) {
    return Status::OK();
}

Status VMetaScanner::close(RuntimeState* state) {
    RETURN_IF_ERROR(VScanner::close(state));
    return Status::OK();
}
} // namespace doris::vectorized
