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

#include "exprs/vfile_literal.h"

#include "core/data_type/data_type_file.h"
#include "core/data_type/data_type_nullable.h"
#include "core/value/file_value.h"

namespace doris {

Status VFileLiteral::prepare(RuntimeState* state, const RowDescriptor& row_desc,
                             VExprContext* context) {
    RETURN_IF_ERROR_OR_PREPARED(VExpr::prepare(state, row_desc, context));
    const auto* file_type =
            check_and_get_data_type<DataTypeFile>(remove_nullable(_data_type).get());
    if (!file_type || _children.size() != DataTypeFile::FIELD_COUNT) {
        return Status::InvalidArgument("FILE literal requires FILE type and six children");
    }
    File value(DataTypeFile::FIELD_COUNT);
    for (size_t i = 0; i < value.size(); ++i) {
        const auto* literal = dynamic_cast<const VLiteral*>(_children[i].get());
        if (!literal) {
            return Status::InvalidArgument("Invalid FILE literal child {}", i);
        }
        literal->get_column_ptr()->get(0, value[i]);
        if (!value[i].is_null() && !remove_nullable(literal->get_data_type())
                                            ->equals(*remove_nullable(file_type->get_element(i)))) {
            return Status::InvalidArgument("Invalid FILE literal child type {}", i);
        }
    }
    RETURN_IF_ERROR(validate_file(value));
    _column_ptr =
            _data_type->create_column_const(1, Field::create_field<TYPE_FILE>(std::move(value)));
    return Status::OK();
}

Status VFileLiteral::clone_node(VExprSPtr* cloned_expr) const {
    DORIS_CHECK(cloned_expr != nullptr);
    // A FILE carries a nested descriptor; clone_texpr_node builds scalar descriptors.
    *cloned_expr = VFileLiteral::create_shared(*this);
    return Status::OK();
}

bool VFileLiteral::equals(const VExpr& other) {
    const auto* literal = dynamic_cast<const VFileLiteral*>(&other);
    if (!literal || !_data_type->equals(*literal->_data_type) ||
        _children.size() != literal->_children.size()) {
        return false;
    }
    // Expression identity is structural; SQL FILE comparison remains unsupported.
    for (size_t i = 0; i < _children.size(); ++i) {
        if (!_children[i]->equals(*literal->_children[i])) return false;
    }
    return true;
}

uint64_t VFileLiteral::get_digest(uint64_t seed) const {
    // Hash the literal's scalar children for expression identity, not the FILE column.
    return VExpr::get_digest(seed);
}

} // namespace doris
