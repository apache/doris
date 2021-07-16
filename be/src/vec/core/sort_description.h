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

#include "vector"
#include "memory"
#include "cstddef"
#include "string"
#include "vec/core/field.h"

class Collator;

namespace doris::vectorized
{

struct FillColumnDescription
{
    /// All missed values in range [FROM, TO) will be filled
    /// Range [FROM, TO) respects sorting direction
    Field fill_from;        /// Fill value >= FILL_FROM
    Field fill_to;          /// Fill value + STEP < FILL_TO
    Field fill_step;        /// Default = 1 or -1 according to direction
};

/// Description of the sorting rule by one column.
struct SortColumnDescription
{
    std::string column_name; /// The name of the column.
    int column_number;    /// Column number (used if no name is given).
    int direction;           /// 1 - ascending, -1 - descending.
    int nulls_direction;     /// 1 - NULLs and NaNs are greater, -1 - less.
                             /// To achieve NULLS LAST, set it equal to direction, to achieve NULLS FIRST, set it opposite.
    std::shared_ptr<Collator> collator = nullptr; /// Collator for locale-specific comparison of strings
    bool with_fill = false;
    FillColumnDescription fill_description = {};


    SortColumnDescription(
            int column_number_, int direction_, int nulls_direction_,
            const std::shared_ptr<Collator> & collator_ = nullptr, bool with_fill_ = false,
            const FillColumnDescription & fill_description_ = {})
            : column_number(column_number_), direction(direction_), nulls_direction(nulls_direction_), collator(collator_)
            , with_fill(with_fill_), fill_description(fill_description_) {}

    SortColumnDescription() {}

    bool operator == (const SortColumnDescription & other) const
    {
        return column_name == other.column_name && column_number == other.column_number
            && direction == other.direction && nulls_direction == other.nulls_direction;
    }

    bool operator != (const SortColumnDescription & other) const
    {
        return !(*this == other);
    }
};

/// Description of the sorting rule for several columns.
using SortDescription = std::vector<SortColumnDescription>;

}
