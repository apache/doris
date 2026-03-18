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

Status VLiteral::execute_column(VExprContext* context, const Block* block, Selector* selector,
                                size_t count, ColumnPtr& result_column) const {
    DCHECK(selector == nullptr || selector->size() == count);
    result_column = _column_ptr->clone_resized(count);
    DCHECK_EQ(result_column->size(), count);
    return Status::OK();
}

std::string VLiteral::value(const DataTypeSerDe::FormatOptions& options) const {
    DCHECK(_column_ptr->size() == 1);
    return _data_type->to_string(*_column_ptr, 0, options);
}

#ifdef BE_TEST
std::string VLiteral::value() const {
    auto format_options = vectorized::DataTypeSerDe::get_default_format_options();
    auto timezone = cctz::utc_time_zone();
    format_options.timezone = &timezone;
    return value(format_options);
}
#endif

std::string VLiteral::debug_string() const {
    auto format_options = vectorized::DataTypeSerDe::get_default_format_options();
    auto timezone = cctz::utc_time_zone();
    format_options.timezone = &timezone;

    std::stringstream out;
    out << "VLiteral (name = " << _expr_name;
    out << ", type = " << _data_type->get_name();
    out << ", value = (" << value(format_options);
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

uint64_t VLiteral::get_digest(uint64_t seed) const {
    _column_ptr->update_xxHash_with_value(0, 1, seed, nullptr);
    return seed;
}

#include "common/compile_check_end.h"
} // namespace doris::vectorized
