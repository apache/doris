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

#ifndef DORIS_FUNCTION_GROUPING_H
#define DORIS_FUNCTION_GROUPING_H

#include "vec/functions/simple_function_factory.h"
#include "vec/columns/column_nullable.h"
#include "vec/functions/function_helpers.h"
#include "vec/utils/util.hpp"
#include "vec/data_types/get_least_supertype.h"

namespace doris::vectorized {

class FunctionGroupingBase : public IFunction {
public:
    size_t get_number_of_arguments() const override { return 1; }

    bool use_default_implementation_for_constants() const override { return false; }

    bool use_default_implementation_for_nulls() const override { return false; }

    DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) const override {
        return std::make_shared<DataTypeInt64>();
    }
};

class FunctionGrouping : public FunctionGroupingBase {
public:
    static constexpr auto name = "grouping";

    static FunctionPtr create() { return std::make_shared<FunctionGrouping>(); }

    String get_name() const override { return name; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        const ColumnWithTypeAndName& src_column = block.get_by_position(arguments[0]);
        const ColumnWithTypeAndName& rel_column = block.get_by_position(result);
        if (!src_column.column)
            return Status::InternalError("Illegal column " + src_column.column->get_name() + " of first argument of function " + name);

        DCHECK(src_column.type->is_nullable() == true);
        MutableColumnPtr res_column = rel_column.type->create_column();
        auto* src_nullable_column = reinterpret_cast<ColumnNullable *>(const_cast<IColumn *>(src_column.column.get()));
        res_column->insert_range_from(*src_nullable_column->get_nested_column_ptr().get(), 0, src_column.column->size());
        block.get_by_position(result).column = std::move(res_column);
        return Status::OK();
    }
};

class FunctionGroupingId : public FunctionGroupingBase {
public:
    static constexpr auto name = "grouping_id";

    static FunctionPtr create() { return std::make_shared<FunctionGroupingId>(); }

    String get_name() const override { return name; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        const ColumnWithTypeAndName& src_column = block.get_by_position(arguments[0]);
        const ColumnWithTypeAndName& rel_column = block.get_by_position(result);
        if (!src_column.column)
            return Status::InternalError("Illegal column " + src_column.column->get_name() + " of first argument of function " + name);

        DCHECK(src_column.type->is_nullable() == true);
        MutableColumnPtr res_column = rel_column.type->create_column();
        auto* src_nullable_column = reinterpret_cast<ColumnNullable *>(const_cast<IColumn *>(src_column.column.get()));
        res_column->insert_range_from(*src_nullable_column->get_nested_column_ptr().get(), 0, src_column.column->size());
        block.get_by_position(result).column = std::move(res_column);
        return Status::OK();
    }
};
}
#endif //DORIS_FUNCTION_GROUPING_H
