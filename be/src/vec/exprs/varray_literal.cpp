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

#include "vec/exprs/varray_literal.h"

#include <gen_cpp/Exprs_types.h>
#include <glog/logging.h>

#include <memory>
#include <ostream>
#include <vector>

#include "runtime/types.h"
#include "vec/columns/column.h"
#include "vec/core/field.h"
#include "vec/data_types/data_type.h"
#include "vec/exprs/vexpr.h"

namespace doris {
class RowDescriptor;
class RuntimeState;
namespace vectorized {
class VExprContext;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

Status VArrayLiteral::prepare(RuntimeState* state, const RowDescriptor& row_desc,
                              VExprContext* context) {
    RETURN_IF_ERROR_OR_PREPARED(VExpr::prepare(state, row_desc, context));
    bool is_null = (_node_type == TExprNodeType::NULL_LITERAL);
    Field array = is_null ? Field() : Field::create_field<TYPE_ARRAY>(Array());
    for (auto& child : _children) {
        Field item;
        auto child_literal = std::dynamic_pointer_cast<const VLiteral>(child);
        child_literal->get_column_ptr()->get(0, item);
        array.get<Array>().push_back(item);
    }
    _column_ptr = _data_type->create_column_const(1, array);
    return Status::OK();
}

} // namespace doris::vectorized
