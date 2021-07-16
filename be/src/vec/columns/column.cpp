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

#include "vec/columns/column.h"

#include <sstream>

#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/core/field.h"

namespace doris::vectorized {

std::string IColumn::dump_structure() const {
    std::stringstream res;
    res << get_family_name() << "(size = " << size();

    ColumnCallback callback = [&](ColumnPtr& subcolumn) {
        res << ", " << subcolumn->dump_structure();
    };

    const_cast<IColumn*>(this)->for_each_subcolumn(callback);

    res << ")";
    return res.str();
}

void IColumn::insert_from(const IColumn& src, size_t n) {
    insert(src[n]);
}

bool is_column_nullable(const IColumn& column) {
    return check_column<ColumnNullable>(column);
}

bool is_column_const(const IColumn& column) {
    return check_column<ColumnConst>(column);
}

} // namespace doris::vectorized
