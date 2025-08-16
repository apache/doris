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

#include "vec/exprs/virtual_slot_ref.h"

#include <gen_cpp/Exprs_types.h>
#include <glog/logging.h>
#include <thrift/protocol/TDebugProtocol.h>

#include <ostream>
#include <vector>

#include "common/exception.h"
#include "common/logging.h"
#include "common/status.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nothing.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/exprs/vectorized_fn_call.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/exprs/vexpr_fwd.h"
namespace doris::vectorized {
#include "common/compile_check_begin.h"
VirtualSlotRef::VirtualSlotRef(const doris::TExprNode& node)
        : VExpr(node),
          _column_id(-1),
          _slot_id(node.slot_ref.slot_id),
          _column_name(nullptr),
          _column_label(node.label) {}

VirtualSlotRef::VirtualSlotRef(const SlotDescriptor* desc)
        : VExpr(desc->type(), false), _column_id(-1), _slot_id(desc->id()), _column_name(nullptr) {}

Status VirtualSlotRef::prepare(doris::RuntimeState* state, const doris::RowDescriptor& desc,
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

    if (slot_desc->get_virtual_column_expr() == nullptr) {
        return Status::InternalError(
                "VirtualSlotRef {} has no virtual column expr, slot_id: {}, desc: {}, "
                "slot_desc: {}, desc_tbl: {}",
                *_column_name, _slot_id, desc.debug_string(), slot_desc->debug_string(),
                state->desc_tbl().debug_string());
    }

    _column_name = &slot_desc->col_name();
    _column_data_type = slot_desc->get_data_type_ptr();
    DCHECK(_column_data_type != nullptr);
    if (!context->force_materialize_slot() && !slot_desc->is_materialized()) {
        // slot should be ignored manually
        _column_id = -1;
        _prepare_finished = true;
        return Status::OK();
    }

    _column_id = desc.get_column_id(_slot_id, context->force_materialize_slot());
    if (_column_id < 0) {
        return Status::Error<ErrorCode::INTERNAL_ERROR>(
                "VirtualSlotRef {} has invalid slot id: "
                "{}.\nslot_desc:\n{},\ndesc:\n{},\ndesc_tbl:\n{}",
                *_column_name, _slot_id, slot_desc->debug_string(), desc.debug_string(),
                state->desc_tbl().debug_string());
    }
    const TExpr& expr = *slot_desc->get_virtual_column_expr();
    // Create a temp_ctx only for create_expr_tree.
    VExprContextSPtr temp_ctx;
    RETURN_IF_ERROR(VExpr::create_expr_tree(expr, temp_ctx));
    _virtual_column_expr = temp_ctx->root();
    // Virtual column expr should do prepare with original context.
    RETURN_IF_ERROR(_virtual_column_expr->prepare(state, desc, context));
    _prepare_finished = true;
    return Status::OK();
}

Status VirtualSlotRef::open(RuntimeState* state, VExprContext* context,
                            FunctionContext::FunctionStateScope scope) {
    DCHECK(_prepare_finished);
    RETURN_IF_ERROR(_virtual_column_expr->open(state, context, scope));
    RETURN_IF_ERROR(VExpr::open(state, context, scope));
    _open_finished = true;
    return Status::OK();
}

Status VirtualSlotRef::execute(VExprContext* context, Block* block, int* result_column_id) {
    if (_column_id >= 0 && _column_id >= block->columns()) {
        return Status::Error<ErrorCode::INTERNAL_ERROR>(
                "input block not contain slot column {}, column_id={}, block={}", *_column_name,
                _column_id, block->dump_structure());
    }

    ColumnWithTypeAndName col_type_name = block->get_by_position(_column_id);

    if (!col_type_name.column) {
        // Maybe we need to create a column in this situation.
        return Status::InternalError(
                "VirtualSlotRef column is null, column_id: {}, column_name: {}", _column_id,
                *_column_name);
    }

    const vectorized::ColumnNothing* col_nothing =
            check_and_get_column<ColumnNothing>(col_type_name.column.get());

    if (this->_virtual_column_expr != nullptr) {
        if (col_nothing != nullptr) {
            // Virtual column is not materialized, so we need to materialize it.
            // Note: After executing 'execute', we cannot use the column from line 120 in subsequent code,
            // because the vector might be resized during execution, causing previous references to become invalid.
            int tmp_column_id = -1;
            RETURN_IF_ERROR(_virtual_column_expr->execute(context, block, &tmp_column_id));

            // Maybe do clone.
            block->replace_by_position(_column_id,
                                       std::move(block->get_by_position(tmp_column_id).column));

            VLOG_DEBUG << fmt::format(
                    "Materialization of virtual column, slot_id {}, column_id {}, "
                    "tmp_column_id {}, column_name {}, column size {}",
                    _slot_id, _column_id, tmp_column_id, *_column_name,
                    block->get_by_position(_column_id).column->size());
        }

#ifndef NDEBUG
        // get_by_position again since vector in block may be resized
        col_type_name = block->get_by_position(_column_id);
        DCHECK(col_type_name.type != nullptr);
        if (!_column_data_type->equals(*col_type_name.type)) {
            throw doris::Exception(doris::ErrorCode::FATAL_ERROR,
                                   "Virtual column type not match, column_id: {}, "
                                   "column_name: {}, column_type: {}, virtual_column_type: {}",
                                   _column_id, *_column_name, col_type_name.type->get_name(),
                                   _column_data_type->get_name());
        }
#endif
    } else {
        // This is a virtual slot ref that not pushed to segment_iterator
        if (col_nothing == nullptr) {
            return Status::InternalError("Logical error, virtual column can not be materialized");
        } else {
            return Status::OK();
        }
    }

    *result_column_id = _column_id;
    VLOG_DEBUG << fmt::format("VirtualSlotRef execute, slot_id {}, column_id {}, column_name {}",
                              _slot_id, _column_id, *_column_name);
    return Status::OK();
}

const std::string& VirtualSlotRef::expr_name() const {
    return *_column_name;
}
std::string VirtualSlotRef::expr_label() {
    return _column_label;
}

std::string VirtualSlotRef::debug_string() const {
    std::stringstream out;
    out << "VirtualSlotRef(slot_id=" << _slot_id << VExpr::debug_string() << ")";
    return out.str();
}

bool VirtualSlotRef::equals(const VExpr& other) {
    const auto* other_ptr = dynamic_cast<const VirtualSlotRef*>(&other);
    if (!other_ptr) {
        return false;
    }

    // Compare slot_id and column_id
    if (this->_slot_id != other_ptr->_slot_id || this->_column_id != other_ptr->_column_id) {
        return false;
    }

    // Compare column_name pointers properly
    if (this->_column_name == nullptr && other_ptr->_column_name == nullptr) {
        // Both are null, they are equal
    } else if (this->_column_name == nullptr || other_ptr->_column_name == nullptr) {
        // One is null, the other is not, they are not equal
        return false;
    } else if (*this->_column_name != *other_ptr->_column_name) {
        // Both are not null, compare the string contents
        return false;
    }

    // Compare column_label
    if (this->_column_label != other_ptr->_column_label) {
        return false;
    }

    return true;
}

/**
 * @brief Implements ANN range search evaluation for virtual slot references.
 * 
 * This method handles the case where a virtual slot reference wraps a distance
 * function call that can be optimized using ANN index range search. Instead of
 * computing distances for all rows, it delegates to the underlying virtual
 * expression to perform the optimized search.
 * 
 * @param range_search_runtime Runtime parameters for the range search
 * @param cid_to_index_iterators Index iterators for each column
 * @param idx_to_cid Column ID mapping
 * @param column_iterators Data column iterators
 * @param row_bitmap Result bitmap to be updated with matching rows
 * @param ann_index_stats Performance statistics collector
 * @return Status::OK() if successful, error status otherwise
 */
Status VirtualSlotRef::evaluate_ann_range_search(
        const segment_v2::AnnRangeSearchRuntime& range_search_runtime,
        const std::vector<std::unique_ptr<segment_v2::IndexIterator>>& cid_to_index_iterators,
        const std::vector<ColumnId>& idx_to_cid,
        const std::vector<std::unique_ptr<segment_v2::ColumnIterator>>& column_iterators,
        roaring::Roaring& row_bitmap, segment_v2::AnnIndexStats& ann_index_stats) {
    if (_virtual_column_expr != nullptr) {
        return _virtual_column_expr->evaluate_ann_range_search(
                range_search_runtime, cid_to_index_iterators, idx_to_cid, column_iterators,
                row_bitmap, ann_index_stats);
    }
    return Status::OK();
}
#include "common/compile_check_end.h"
} // namespace doris::vectorized
