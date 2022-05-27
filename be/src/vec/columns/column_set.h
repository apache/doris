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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Columns/ColumnSet.h
// and modified by Doris

#pragma once

#include "exprs/hybrid_set.h"
#include "vec/columns/column_dummy.h"

namespace doris::vectorized {

using ConstSetPtr = std::shared_ptr<HybridSetBase>;

/** A column containing multiple values in the `IN` section.
  * Behaves like a constant-column (because the set is one, not its own for each line).
  * This column has a nonstandard value, so it can not be obtained via a normal interface.
  */
class ColumnSet final : public COWHelper<IColumnDummy, ColumnSet> {
public:
    friend class COWHelper<IColumnDummy, ColumnSet>;

    ColumnSet(size_t s_, const ConstSetPtr& data_) : data(data_) { s = s_; }
    ColumnSet(const ColumnSet&) = default;

    const char* get_family_name() const override { return "Set"; }
    MutableColumnPtr clone_dummy(size_t s_) const override { return ColumnSet::create(s_, data); }

    ConstSetPtr get_data() const { return data; }

private:
    ConstSetPtr data;
};

} // namespace doris::vectorized