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

#include "vec/exprs/vmap_literal.h"

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

//insert into table_map values ({'name':'zhangsan', 'gender':'male'}), ({'name':'lisi', 'gender':'female'});
namespace doris::vectorized {

Status VMapLiteral::prepare(RuntimeState* state, const RowDescriptor& row_desc,
                            VExprContext* context) {
    RETURN_IF_ERROR_OR_PREPARED(VExpr::prepare(state, row_desc, context));
    // map-field should contain two vector field for keys and values
    Field map = Field::create_field<TYPE_MAP>(Map());
    Field keys = Field::create_field<TYPE_ARRAY>(Array());
    Field values = Field::create_field<TYPE_ARRAY>(Array());
    // each child is slot with key1, value1, key2, value2...
    for (int idx = 0; idx < _children.size() && idx + 1 < _children.size(); idx += 2) {
        Field kf, vf;
        auto key_literal = std::dynamic_pointer_cast<const VLiteral>(_children[idx]);
        key_literal->get_column_ptr()->get(0, kf);
        auto val_literal = std::dynamic_pointer_cast<const VLiteral>(
                VExpr::expr_without_cast(_children[idx + 1]));
        val_literal->get_column_ptr()->get(0, vf);

        keys.get<Array>().push_back(kf);
        values.get<Array>().push_back(vf);
    }
    map.get<Map>().push_back(keys);
    map.get<Map>().push_back(values);

    _column_ptr = _data_type->create_column_const(1, map);
    return Status::OK();
}

} // namespace doris::vectorized
