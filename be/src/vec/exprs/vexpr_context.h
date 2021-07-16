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

#include "common/status.h"
#include "runtime/runtime_state.h"
#include "vec/core/block.h"

namespace doris::vectorized {
class VExpr;

class VExprContext {
public:
    VExprContext(VExpr* expr);
    doris::Status prepare(doris::RuntimeState* state, const doris::RowDescriptor& row_desc,
                          const std::shared_ptr<doris::MemTracker>& tracker);
    doris::Status open(doris::RuntimeState* state);
    void close(doris::RuntimeState* state);
    doris::Status clone(doris::RuntimeState* state, VExprContext** new_ctx);
    doris::Status execute(doris::vectorized::Block* block, int* result_column_id);

    VExpr* root() { return _root; }

private:
    VExpr* _root;
    bool _prepared;
    bool _opened;
    bool _closed;
};
} // namespace doris::vectorized
