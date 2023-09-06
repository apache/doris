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
#include <math.h>
#include <stdint.h>
#include <sys/types.h>

#include <algorithm>
#include <ostream>
#include <vector>

#include "common/exception.h"
#include "olap/olap_common.h"
#include "runtime/decimalv2_value.h"
#include "runtime/define_primitive_type.h"
#include "runtime/jsonb_value.h"
#include "runtime/large_int_value.h"
#include "runtime/types.h"
#include "util/string_parser.hpp"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/common/string_ref.h"
#include "vec/common/typeid_cast.h"
#include "vec/core/block.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris {

namespace vectorized {
class VExprContext;

void VLiteral::init(const TExprNode& node) {
    Field field;
    field = _data_type->get_field(node);
    _column_ptr = _data_type->create_column_const(1, field);
}

Status VLiteral::execute(VExprContext* context, vectorized::Block* block, int* result_column_id) {
    // Literal expr should return least one row.
    size_t row_size = std::max(block->rows(), _column_ptr->size());
    *result_column_id = VExpr::insert_param(block, {_column_ptr, _data_type, _expr_name}, row_size);
    return Status::OK();
}

std::string VLiteral::value() const {
    //TODO: dcheck the equality of size with 1. then use string with size to replace the ss.
    std::stringstream out;
    for (size_t i = 0; i < _column_ptr->size(); i++) {
        if (i != 0) {
            out << ", ";
        }
        out << _data_type->to_string(*_column_ptr, i);
    }
    return out.str();
}

std::string VLiteral::debug_string() const {
    std::stringstream out;
    out << "VLiteral (name = " << _expr_name;
    out << ", type = " << _data_type->get_name();
    out << ", value = (" << value();
    out << "))";
    return out.str();
}

} // namespace vectorized
} // namespace doris
