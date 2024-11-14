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

#include "vec/exprs/vliteral.h"

#include <fmt/format.h>
#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/Types_types.h>
#include <glog/logging.h>
#include <sys/types.h>

#include <algorithm>
#include <ostream>

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/core/block.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_decimal.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

class VExprContext;

void VLiteral::init(const TExprNode& node) {
    Field field;
    field = _data_type->get_field(node);
    _column_ptr = _data_type->create_column_const(1, field);
}

Status VLiteral::prepare(RuntimeState* state, const RowDescriptor& desc, VExprContext* context) {
    RETURN_IF_ERROR_OR_PREPARED(VExpr::prepare(state, desc, context));
    return Status::OK();
}

Status VLiteral::execute(VExprContext* context, vectorized::Block* block, int* result_column_id) {
    // Literal expr should return least one row.
    // sometimes we just use a VLiteral without open or prepare. so can't check it at this moment
    size_t row_size = std::max(block->rows(), _column_ptr->size());
    *result_column_id = VExpr::insert_param(block, {_column_ptr, _data_type, _expr_name}, row_size);
    return Status::OK();
}

std::string VLiteral::value() const {
    DCHECK(_column_ptr->size() == 1);
    return _data_type->to_string(*_column_ptr, 0);
}

std::string VLiteral::debug_string() const {
    std::stringstream out;
    out << "VLiteral (name = " << _expr_name;
    out << ", type = " << _data_type->get_name();
    out << ", value = (" << value();
    out << "))";
    return out.str();
}

bool VLiteral::equals(const VExpr& other) {
    const auto* other_ptr = dynamic_cast<const VLiteral*>(&other);
    if (!other_ptr) {
        return false;
    }
    if (this->_expr_name != other_ptr->_expr_name) {
        return false;
    }
    if (this->_column_ptr->structure_equals(*other_ptr->_column_ptr)) {
        if (this->_column_ptr->size() != other_ptr->_column_ptr->size()) {
            return false;
        }
        for (size_t i = 0; i < this->_column_ptr->size(); i++) {
            if (this->_column_ptr->compare_at(i, i, *other_ptr->_column_ptr, -1) != 0) {
                return false;
            }
        }
    }
    return true;
}

#include "common/compile_check_end.h"
} // namespace doris::vectorized
