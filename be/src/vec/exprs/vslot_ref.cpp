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

#include <gen_cpp/Exprs_types.h>
#include <glog/logging.h>

#include <ostream>
#include <vector>

#include "common/status.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "vec/core/block.h"
#include "vec/exprs/vexpr_context.h"

namespace doris {
namespace vectorized {
class VExprContext;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

VSlotRef::VSlotRef(const doris::TExprNode& node)
        : VExpr(node),
          _slot_id(node.slot_ref.slot_id),
          _column_id(-1),
          _column_name(nullptr),
          _column_label(node.label) {}

VSlotRef::VSlotRef(const SlotDescriptor* desc)
        : VExpr(desc->type(), true, desc->is_nullable()),
          _slot_id(desc->id()),
          _column_id(-1),
          _column_name(nullptr) {}

Status VSlotRef::prepare(doris::RuntimeState* state, const doris::RowDescriptor& desc,
                         VExprContext* context) {
    RETURN_IF_ERROR_OR_PREPARED(VExpr::prepare(state, desc, context));
    DCHECK_EQ(_children.size(), 0);
    if (_slot_id == -1) {
        _prepare_finished = true;
        return Status::OK();
    }
    const SlotDescriptor* slot_desc = state->desc_tbl().get_slot_descriptor(_slot_id);
    if (slot_desc == nullptr) {
        return Status::Error<ErrorCode::INTERNAL_ERROR>(
                "couldn't resolve slot descriptor {}, desc: {}", _slot_id,
                state->desc_tbl().debug_string());
    }
    _column_name = &slot_desc->col_name();
    if (!context->force_materialize_slot() && !slot_desc->need_materialize()) {
        // slot should be ignored manually
        _column_id = -1;
        _prepare_finished = true;
        return Status::OK();
    }
    _column_id = desc.get_column_id(_slot_id, context->force_materialize_slot());
    if (_column_id < 0) {
        return Status::Error<ErrorCode::INTERNAL_ERROR>(
                "VSlotRef {} have invalid slot id: {}, desc: {}, slot_desc: {}, desc_tbl: {}",
                *_column_name, _slot_id, desc.debug_string(), slot_desc->debug_string(),
                state->desc_tbl().debug_string());
    }
    _prepare_finished = true;
    return Status::OK();
}

Status VSlotRef::open(RuntimeState* state, VExprContext* context,
                      FunctionContext::FunctionStateScope scope) {
    DCHECK(_prepare_finished);
    RETURN_IF_ERROR(VExpr::open(state, context, scope));
    _open_finished = true;
    return Status::OK();
}

Status VSlotRef::execute(VExprContext* context, Block* block, int* result_column_id) {
    if (_column_id >= 0 && _column_id >= block->columns()) {
        return Status::Error<ErrorCode::INTERNAL_ERROR>(
                "input block not contain slot column {}, column_id={}, block={}", *_column_name,
                _column_id, block->dump_structure());
    }
    *result_column_id = _column_id;
    return Status::OK();
}

const std::string& VSlotRef::expr_name() const {
    return *_column_name;
}
std::string VSlotRef::expr_label() {
    return _column_label;
}

std::string VSlotRef::debug_string() const {
    std::stringstream out;
    out << "SlotRef(slot_id=" << _slot_id << VExpr::debug_string() << ")";
    return out.str();
}
} // namespace doris::vectorized
