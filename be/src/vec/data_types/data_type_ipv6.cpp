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

#include "vec/data_types/data_type_ipv6.h"

#include "util/binary_cast.hpp"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_buffer.hpp"
#include "vec/data_types/data_type.h"
#include "vec/functions/cast/cast_to_string.h"
#include "vec/io/io_helper.h"
#include "vec/runtime/ipv6_value.h"

namespace doris::vectorized {
bool DataTypeIPv6::equals(const IDataType& rhs) const {
    return typeid(rhs) == typeid(*this);
}

MutableColumnPtr DataTypeIPv6::create_column() const {
    return ColumnIPv6::create();
}

} // namespace doris::vectorized
