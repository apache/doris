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

#include "vec/exprs/vslot_ref.h"

#include <fmt/format.h>

#include "runtime/descriptors.h"

namespace doris::vectorized {
using doris::Status;
using doris::SlotDescriptor;
VSlotRef::VSlotRef(const doris::TExprNode& node)
        : VExpr(node),
          _slot_id(node.slot_ref.slot_id),
          _column_id(-1),
          _column_name(nullptr) {
            if (node.__isset.is_nullable) {
              _is_nullable = node.is_nullable;
            } else {
              _is_nullable = true;
            }
        }

VSlotRef::VSlotRef(const SlotDescriptor* desc)
        : VExpr(desc->type(), true, desc->is_nullable()),
          _slot_id(desc->id()),
          _column_id(-1),
          _is_nullable(desc->is_nullable()),
          _column_name(nullptr) {}

Status VSlotRef::prepare(doris::RuntimeState* state, const doris::RowDescriptor& desc,
                         VExprContext* context) {
    DCHECK_EQ(_children.size(), 0);
    if (_slot_id == -1) {
        return Status::OK();
    }
    const SlotDescriptor* slot_desc = state->desc_tbl().get_slot_descriptor(_slot_id);
    if (slot_desc == NULL) {
        return Status::InternalError(fmt::format("couldn't resolve slot descriptor {}", _slot_id));
    }
    _column_id = desc.get_column_id(_slot_id);
    _column_name = &slot_desc->col_name();
    return Status::OK();
}

Status VSlotRef::execute(Block* block, int* result_column_id) {
    DCHECK_GE(_column_id, 0);
    *result_column_id = _column_id;
    return Status::OK();
}

const std::string& VSlotRef::expr_name() const {
    return *_column_name;
}
std::string VSlotRef::debug_string() const {
    std::stringstream out;
    out << "SlotRef(slot_id=" << _slot_id << VExpr::debug_string() << ")";
    return out.str();
}
} // namespace doris::vectorized
