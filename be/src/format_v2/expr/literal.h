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

#pragma once

#include "core/field.h"
#include "core/data_type/data_type.h"
#include "exprs/vliteral.h"

namespace doris {

class TableLiteral : public VLiteral {
    ENABLE_FACTORY_CREATOR(TableLiteral);

public:
    TableLiteral(const DataTypePtr& type, const Field& field) : VLiteral(type) {
        _data_type = type;
        _column_ptr = _data_type->create_column_const(1, field);
        _node_type = TExprNodeType::LITERAL;
    }

    Status clone_node(VExprSPtr* cloned_expr) const override {
        DORIS_CHECK(cloned_expr != nullptr);
        Field field;
        _column_ptr->get(0, field);
        *cloned_expr = TableLiteral::create_shared(_data_type, field);
        return Status::OK();
    }
};

} // namespace doris
