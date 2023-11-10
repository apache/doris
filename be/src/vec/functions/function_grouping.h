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

#include <glog/logging.h>
#include <stddef.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <memory>

#include "common/status.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/columns_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function.h"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris::vectorized {

class FunctionGroupingBase : public IFunction {
public:
    size_t get_number_of_arguments() const override { return 1; }

    bool use_default_implementation_for_nulls() const override { return false; }

    DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) const override {
        return std::make_shared<DataTypeInt64>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        const ColumnWithTypeAndName& src_column = block.get_by_position(arguments[0]);
        DCHECK(src_column.column->size() == input_rows_count);
        // result of functions grouping and grouping_id is always not nullable,
        // but outer join will convert the column to nullable when necessary,
        // so need to remove nullable here when functions grouping and grouping_id are executed
        block.get_by_position(result).column = remove_nullable(src_column.column);
        return Status::OK();
    }
};

class FunctionGrouping : public FunctionGroupingBase {
public:
    static constexpr auto name = "grouping";

    static FunctionPtr create() { return std::make_shared<FunctionGrouping>(); }

    String get_name() const override { return name; }
};

class FunctionGroupingId : public FunctionGroupingBase {
public:
    static constexpr auto name = "grouping_id";

    static FunctionPtr create() { return std::make_shared<FunctionGroupingId>(); }

    String get_name() const override { return name; }
};
} // namespace doris::vectorized
