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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Core/SortDescription.h
// and modified by Doris

#pragma once

#include "cstddef"
#include "memory"
#include "string"
#include "vec/core/field.h"
#include "vector"

namespace doris::vectorized {

/// Description of the sorting rule by one column.
struct SortColumnDescription {
    std::string column_name; /// The name of the column.
    int column_number;       /// Column number (used if no name is given).
    int direction;           /// 1 - ascending, -1 - descending.
    int nulls_direction;     /// 1 - NULLs and NaNs are greater, -1 - less.

    SortColumnDescription(int column_number_, int direction_, int nulls_direction_)
            : column_number(column_number_),
              direction(direction_),
              nulls_direction(nulls_direction_) {}

    SortColumnDescription() = default;
};

/// Description of the sorting rule for several columns.
using SortDescription = std::vector<SortColumnDescription>;

} // namespace doris::vectorized
