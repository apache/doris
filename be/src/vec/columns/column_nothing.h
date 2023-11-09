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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/AggregateFunctions/ColumnNothing.h
// and modified by Doris

#pragma once

#include "vec/columns/column_dummy.h"

namespace doris::vectorized {

class ColumnNothing final : public COWHelper<IColumnDummy, ColumnNothing> {
private:
    friend class COWHelper<IColumnDummy, ColumnNothing>;

    ColumnNothing(size_t s_) { s = s_; }

    ColumnNothing(const ColumnNothing&) = default;

public:
    const char* get_family_name() const override { return "Nothing"; }
    MutableColumnPtr clone_dummy(size_t s_) const override { return ColumnNothing::create(s_); }

    bool structure_equals(const IColumn& rhs) const override {
        return typeid(rhs) == typeid(ColumnNothing);
    }
};

} // namespace doris::vectorized
