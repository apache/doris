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

#include "format/reader/expr/delete_predicate.h"

#include <fmt/format.h>
#include <gen_cpp/Exprs_types.h>
#include <glog/logging.h>

#include <algorithm>
#include <cstddef>
#include <ostream>

#include "common/status.h"
#include "core/block/block.h"
#include "core/block/column_numbers.h"
#include "core/block/column_with_type_and_name.h"
#include "core/block/columns_with_type_and_name.h"

namespace doris {

DeletePredicate::DeletePredicate(const std::vector<int64_t>& deleted_rows)
        : VExpr(), _deleted_rows(deleted_rows) {
    _node_type = TExprNodeType::PREDICATE;
    _opcode = TExprOpcode::DELETE;
    _data_type = std::make_shared<DataTypeBool>();
}

Status DeletePredicate::prepare(RuntimeState* state, const RowDescriptor& desc,
                                VExprContext* context) {
    RETURN_IF_ERROR_OR_PREPARED(VExpr::prepare(state, desc, context));
    _expr_name = "DeletePredicate";
    _prepare_finished = true;
    return Status::OK();
}

Status DeletePredicate::open(RuntimeState* state, VExprContext* context,
                             FunctionContext::FunctionStateScope scope) {
    DCHECK(_prepare_finished);
    RETURN_IF_ERROR_OR_PREPARED(VExpr::open(state, context, scope));
    _open_finished = true;
    return Status::OK();
}

void DeletePredicate::close(VExprContext* context, FunctionContext::FunctionStateScope scope) {
    VExpr::close(context, scope);
}

Status DeletePredicate::execute_column_impl(VExprContext* context, const Block* block,
                                            const Selector* selector, size_t count,
                                            ColumnPtr& result_column) const {
    DCHECK(_open_finished || block == nullptr);

    static_cast<void>(_deleted_rows.size());
    // TODO: implement delete predicate logic here, currently we just return a column with all 0 (false)
    return Status::OK();
}

std::string DeletePredicate::debug_string() const {
    return _expr_name;
}

} // namespace doris
