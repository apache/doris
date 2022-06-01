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

#include "vec/exprs/vinfo_func.h"

#include <fmt/format.h>

#include "util/string_parser.hpp"
#include "vec/core/field.h"
#include "vec/data_types/data_type_nullable.h"

namespace doris::vectorized {

VInfoFunc::VInfoFunc(const TExprNode& node) : VExpr(node) {
    Field field;
    switch (_type.type) {
        case TYPE_BIGINT: {
            field = Int64(node.info_func.int_value);
            break;
        }
        case TYPE_STRING:
        case TYPE_CHAR:
        case TYPE_VARCHAR: {
            field = node.info_func.str_value;
            break;
        }
        default: {
            DCHECK(false) << "Invalid type: " << _type.type;
            break;
        }
    }
    this->_column_ptr = _data_type->create_column_const(1, field);
}

Status VInfoFunc::execute(VExprContext* context, vectorized::Block* block, int* result_column_id) {
    // Info function should return least one row, e.g. select current_user().
    size_t row_size = std::max(block->rows(), size_t(1));
    *result_column_id = VExpr::insert_param(block, {_column_ptr, _data_type, _expr_name}, row_size);
    return Status::OK();
}

} // namespace doris
