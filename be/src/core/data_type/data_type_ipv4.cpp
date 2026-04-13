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

#include "core/data_type/data_type_ipv4.h"

#include "core/assert_cast.h"
#include "core/binary_cast.hpp"
#include "core/column/column.h"
#include "core/column/column_const.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/string_buffer.hpp"
#include "exprs/function/cast/cast_to_string.h"
#include "util/io_helper.h"

namespace doris {

bool DataTypeIPv4::equals(const IDataType& rhs) const {
    return typeid(rhs) == typeid(*this);
}

MutableColumnPtr DataTypeIPv4::create_column() const {
    return ColumnIPv4::create();
}

Field DataTypeIPv4::get_field(const TExprNode& node) const {
    return Field::create_field<TYPE_IPV4>(cast_set<const unsigned int>(node.ipv4_literal.value));
}

} // namespace doris
