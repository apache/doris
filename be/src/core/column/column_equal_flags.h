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

#include "core/column/column.h"
#include "core/data_type/define_primitive_type.h"

namespace doris {

// flags[row] &= (row `row` of `column` equals row `row` - 1) for every row from 1 on: `flags` has
// one flag per row of `column`, and flags[0] stays as it is. Two rows are equal when
// IColumn::compare_at() finds them equal. Like StarRocks' build_tie_for_column(), a column of a
// type a key can have (integer, decimal, date and time, IP, string; nullable or not) is compared
// by one loop over it for its type; any other column by compare_at() row by row. `type` is the
// type of the values, without nullable.
// Example: the column 1 1 2 2 2 3 turns the flags 1 1 1 1 1 1 into 1 1 0 1 1 0; called for every
// key column of a Block, it leaves 1 on the rows whose key equals the key of the row before.
void build_equal_flags_for_column(const IColumn& column, PrimitiveType type, EqualFlags* flags);

} // namespace doris
