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

#include <vec/columns/column_array.h>
#include <vec/columns/column_nullable.h>
#include <vec/columns/columns_number.h>
#include <vec/data_types/data_type_array.h>
#include <vec/data_types/data_type_number.h>
#include <vec/functions/function.h>
#include <vec/functions/function_helpers.h>
#include <vec/functions/simple_function_factory.h>
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"

#include "vec/aggregate_functions/aggregate_function.h"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris::vectorized {

// array_reduce("sum", [1,2,3,4])
class FunctionArrayReduce : public IFunction {
public:
    static constexpr auto name = "array_reduce";

    static FunctionPtr create() { return std::make_shared<FunctionArrayReduce>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 2; }
    ColumnNumbers get_arguments_that_are_always_constant() const override { return {1, 2}; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        std::cout << "----test1" << std::endl;
        // DCHECK(is_array(arguments[1]))
        //         << "first argument for function: " << name << " should be DataTypeArray"
        //         << " and arguments[0] is " << arguments[1]->get_name();
        return arguments[1];
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        std::cout << "----test2" << std::endl;

        ColumnPtr src_column =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        std::cout << "----test3" << block.get_by_position(arguments[0]).type->get_name() << std::endl;
        // 获取函数名
        // const auto* aggregate_function_name_column = check_and_get_column<ColumnString>(src_column.get());
        // String aggregate_function_name_with_params = aggregate_function_name_column->get_value<String>();
        // std::cout << "----test4" << aggregate_function_name_with_params << std::endl;
        auto num = assert_cast<const ColumnNullable*>(src_column.get())->get_nested_column_ptr();
        // std::cout << "----test4" << aggregate_function_name_with_params << std::endl;

        // 获取聚合函数的名称的字符串
        String aggregate_function_name = block.get_by_position(arguments[0]).name;
        
        std::cout << block.get_by_position(arguments[0]).type->get_name() << std::endl;

        std::cout << "----test5" << aggregate_function_name << std::endl;
        // vectorized::DataTypePtr type = block.get_by_position(arguments[0]).type;

        

        std::cout << "----test5" << std::endl;
		
        DataTypePtr type = block.get_by_position(arguments[1]).type;
        std::cout << "----test6" << std::endl;

        DataTypes argument_types;
        argument_types.push_back(type);
        std::cout << "----test7" << std::endl;
        // getAggregateFunctionNameAndParametersArray (aggregate_function_name_with_params,aggregate_function_name, params_row, "function " + getName(), getContext());

        // vectorized::AggregateFunctionPtr function = get_aggregate_function(aggregate_function_name_with_params, vectorized::AGG_LOAD_SUFFIX)
        
        // AggregateFunctionPtr aggregate_function = vectorized::AggregateFunctionSimpleFactory::instance().get(
        //     aggregate_function_name, argument_types, argument_types.back()->is_nullable());

        // AggregateFunctionSimpleFactory factory = AggregateFunctionSimpleFactory::instance();
        // auto agg_function = factory.get(aggregate_function_name, argument_types, argument_types.back()->is_nullable());

        // // factory->get_return_type();
        // std::cout << "----test8" << std::endl;
        // std::cout << "----" << std::endl;
        // std::cout << aggregate_function_name << std::endl;
        // std::cout << "----" << std::endl;

        // const IAggregateFunction & agg_func = *aggregate_function;

        // // 数组个数
        // const size_t num_arguments_columns = arguments.size() - 1;
        // // 存放常量化数组
        // std::vector<const IColumn *> aggregate_arguments_vec(num_arguments_columns);
        // const ColumnArray::Offsets64 *offsets = nullptr;
        
        // // for (size_t i = 0; i < num_arguments_columns; ++i) {
        // const IColumn * col = block.get_by_position(arguments[0]).column;
        // const ColumnArray::Offsets64 * offsets_i = nullptr;

        // if (const ColumnArray * arr = check_and_get_column<ColumnArray>(col)) {
        //     aggregate_arguments_vec[i] = &arr->get_data();
        //     std::cout << "----" << std::endl;
        //     const auto& name = arr->get_name();
        //     std::cout << &name << std::endl;
        //     std::cout << "----" << std::endl;
        //     offsets_i = &arr->get_offsets();
        // }
        // else {
        //     std::cout << "---------ERROR----------" << std::endl;
        // }

        // if (i == 0)
        //     offsets = offsets_i;
        // else if (*offsets_i != *offsets)
        //     std::cout << "Lengths of all arrays passed to " + get_name() + " must be equal." << std::endl;
        // }
        // 获取array数组中的数据
    //     std::cout << "----test9" << std::endl;
    //     const IColumn ** aggregate_arguments = aggregate_arguments_vec.data();

    //     std::cout << agg_func.size_of_data() << std::endl;

    //     std::cout << "----test1" << std::endl;
    //     std::unique_ptr<Arena> arena = std::make_unique<Arena>();
    //     std::cout << "----test2" << std::endl;
    //     std::vector<AggregateDataPtr> places(input_rows_count);
        
    //     for (size_t i = 0; i < input_rows_count; ++i)
    //     {
    //         places[i] = arena->aligned_alloc(agg_func.size_of_data(), agg_func.align_of_data());
    //         try
    //         {
    //             agg_func.create(places[i]);
    //         }
    //         catch (...)
    //         {
    //             for (size_t j = 0; j < i; ++j)
    //                 agg_func.destroy(places[j]);
    //             throw;
    //         }
    //     }
    //     std::cout << "----test3" << std::endl;
    //     auto guard = AggregateFunctionGuard(aggregate_function.get());
    //     ColumnPtr result_ptr;

    //     aggregate_function->add_batch_range(0, input_rows_count, *places.data(), aggregate_arguments, arena.get(), false);
    //     aggregate_function->insert_result_into(*places.data(), result_ptr->assume_mutable_ref());
    //     std::cout << "----test4" << std::endl;
    //     block.replace_by_position(result, std::move(result_ptr));
    //     std::cout << "----test5" << std::endl;
        return Status::OK();
    }

    AggregateFunctionPtr _function;
};

void register_function_array_reduce(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionArrayReduce>();
}

} // namespace doris::vectorized
