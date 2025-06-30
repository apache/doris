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

#include "vec/exprs/vexpr_context.h"
#include "vec/exprs/virtual_slot_ref.h"

namespace doris::vectorized {

class ScoreRuntime {
    ENABLE_FACTORY_CREATOR(ScoreRuntime);

public:
    ScoreRuntime(VExprContextSPtr projection) : _projection(std::move(projection)) {};

    Status prepare(RuntimeState* state) {
        auto vir_slot_ref = std::dynamic_pointer_cast<VirtualSlotRef>(_projection->root());
        DCHECK(vir_slot_ref != nullptr);
        if (vir_slot_ref == nullptr) {
            return Status::InternalError("root of projection must be a VirtualSlotRef, got\n{}",
                                         _projection->root()->debug_string());
        }
        DCHECK(vir_slot_ref->column_id() >= 0);
        _dest_column_idx = vir_slot_ref->column_id();
        return Status::OK();
    }

    size_t get_dest_column_idx() const { return _dest_column_idx; }

private:
    VExprContextSPtr _projection;

    std::string _name = "score_runtime";
    size_t _dest_column_idx = -1;
};
using ScoreRuntimeSPtr = std::shared_ptr<ScoreRuntime>;

} // namespace doris::vectorized