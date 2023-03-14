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

#include "vec/exprs/vschema_change_expr.h"

#include "vec/columns/column_object.h"
#include "vec/common/schema_util.h"
#include "vec/core/columns_with_type_and_name.h"

namespace doris::vectorized {

Status VSchemaChangeExpr::prepare(doris::RuntimeState* state, const doris::RowDescriptor& desc,
                                  VExprContext* context) {
    RETURN_IF_ERROR_OR_PREPARED(VExpr::prepare(state, desc, context));
    // DCHECK_EQ(_children.size(), 1);
    _table_id = _tnode.schema_change_expr.table_id;
    _expr_name = fmt::format("(SCHEMA_CHANGE {})", "VARIANT");
    _slot_id = _tnode.slot_ref.slot_id;
    const SlotDescriptor* slot_desc = state->desc_tbl().get_slot_descriptor(_slot_id);
    if (slot_desc == nullptr) {
        return Status::InternalError("couldn't resolve slot descriptor {}", _slot_id);
    }
    _column_id = desc.get_column_id(_slot_id);
    if (_column_id < 0) {
        return Status::InternalError(
                "VSlotRef have invalid slot id: {}, desc: {}, slot_desc: {}, desc_tbl: {}",
                *"VARIANT", _slot_id, desc.debug_string(), slot_desc->debug_string(),
                state->desc_tbl().debug_string());
    }
    return Status::OK();
}

const std::string& VSchemaChangeExpr::expr_name() const {
    return _expr_name;
}

Status VSchemaChangeExpr::open(doris::RuntimeState* state, VExprContext* context,
                               FunctionContext::FunctionStateScope scope) {
    RETURN_IF_ERROR(VExpr::open(state, context, scope));
    return Status::OK();
}

void VSchemaChangeExpr::close(doris::RuntimeState* state, VExprContext* context,
                              FunctionContext::FunctionStateScope scope) {
    VExpr::close(state, context, scope);
}

Status VSchemaChangeExpr::execute(VExprContext* context, doris::vectorized::Block* block,
                                  int* result_column_id) {
    // Send schema change RPC
    ColumnObject& object_column = *assert_cast<ColumnObject*>(
            block->get_by_position(_column_id).column->assume_mutable().get());
    CHECK(object_column.is_finalized());
    std::unique_ptr<vectorized::schema_util::FullBaseSchemaView> full_base_schema_view;
    full_base_schema_view.reset(new vectorized::schema_util::FullBaseSchemaView);
    full_base_schema_view->table_id = _table_id;
    vectorized::ColumnsWithTypeAndName cols_with_type_name;
    cols_with_type_name.reserve(object_column.get_subcolumns().size());
    for (auto& entry : object_column.get_subcolumns()) {
        cols_with_type_name.push_back(ColumnWithTypeAndName {entry->data.get_finalized_column_ptr(),
                                                             entry->data.get_least_common_type(),
                                                             entry->path.get_path()});
    }
    if (!cols_with_type_name.empty()) {
        // duplicated columns in full_base_schema_view will be idempotent
        RETURN_IF_ERROR(vectorized::schema_util::send_add_columns_rpc(cols_with_type_name,
                                                                      full_base_schema_view.get()));
    }

    // TODO: make sure the dynamic generated columns's types matched with schema in full_base_schema_view
    // handle dynamic generated columns
    // if (_full_base_schema_view && !_full_base_schema_view->empty()) {
    //     CHECK(_is_dynamic_schema);
    //     for (size_t i = block->columns(); i < _src_block.columns(); ++i) {
    //         auto& column_type_name = _src_block.get_by_position(i);
    //         // Column from schema change response
    //         const TColumn& tcolumn =
    //                 _full_base_schema_view->column_name_to_column[column_type_name.name];
    //         auto original_type = vectorized::DataTypeFactory::instance().create_data_type(tcolumn);
    //         // Detect type conflict, there may exist another load procedure, whitch has already added some columns
    //         // but, this load detects different type, we go type conflict free path, always cast to original type
    //         // TODO need to add type conflict abort feature
    //         if (!column_type_name.type->equals(*original_type)) {
    //             vectorized::ColumnPtr column_ptr;
    //             RETURN_IF_ERROR(vectorized::schema_util::cast_column(column_type_name,
    //                                                                  original_type, &column_ptr));
    //             column_type_name.column = column_ptr;
    //             column_type_name.type = original_type;
    //         }
    //         DCHECK(column_type_name.column != nullptr);
    //         block->insert(vectorized::ColumnWithTypeAndName(std::move(column_type_name.column),
    //                                                         std::move(column_type_name.type),
    //                                                         column_type_name.name));
    //     }
    // }

    *result_column_id = _column_id;
    return Status::OK();
}

std::string VSchemaChangeExpr::debug_string() const {
    return _expr_name;
}

} // namespace doris::vectorized
