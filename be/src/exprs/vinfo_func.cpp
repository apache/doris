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

#include "exprs/vinfo_func.h"

#include <gen_cpp/Exprs_types.h>
#include <glog/logging.h>

#include <algorithm>

#include "core/block/block.h"
#include "core/data_type/data_type.h"
#include "core/data_type/define_primitive_type.h"
#include "core/field.h"
#include "core/types.h"

namespace doris {

class VExprContext;

VInfoFunc::VInfoFunc(const TExprNode& node) : VExpr(node) {
    Field field;
    switch (_data_type->get_primitive_type()) {
    case TYPE_BIGINT: {
        field = Field::create_field<TYPE_BIGINT>(Int64(node.info_func.int_value));
        break;
    }
    case TYPE_STRING:
    case TYPE_CHAR:
    case TYPE_VARCHAR: {
        field = Field::create_field<TYPE_STRING>(node.info_func.str_value);
        break;
    }
    default: {
        DCHECK(false) << "Invalid type: " << _data_type->get_name();
        break;
    }
    }
    this->_column_ptr = _data_type->create_column_const(1, field);
}

Status VInfoFunc::execute_column_impl(VExprContext* context, const Block* block,
                                      const Selector* selector, size_t count,
                                      ColumnPtr& result_column) const {
    result_column = _column_ptr->clone_resized(count);
    DCHECK_EQ(result_column->size(), count);
    return Status::OK();
}

} // namespace doris
