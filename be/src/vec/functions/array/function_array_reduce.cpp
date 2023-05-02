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
#include "vec/common/arena.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "olap/tablet_schema.h"


namespace doris {
class FunctionContext;
} // namespace doris

namespace doris::vectorized {

class Arena;

// array_reduce("sum", [1,2,3,4])
class FunctionArrayReduce : public IFunction {
public:
    static constexpr auto name = "array_reduce";

    static FunctionPtr create() { return std::make_shared<FunctionArrayReduce>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 2; }
    ColumnNumbers get_arguments_that_are_always_constant() const override { return {1, 2}; }

    DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName & arguments) const override {
        std::cout << "----test1" << std::endl;
        // DCHECK(is_array(arguments[1]))
        //         << "first argument for function: " << name << " should be DataTypeArray"
        //         << " and arguments[0] is " << arguments[1]->get_name();
        // vectorized::AggregateFunctionPtr function = TabletColumn::get_aggregate_function(arguments, "_reader");

        // std::string agg_name = TabletColumn::get_string_by_aggregation_type(_aggregation) + s"_reader";
        // std::transform(agg_name.begin(), agg_name.end(), agg_name.begin(),
        //             [](unsigned char c) { return std::tolower(c); });
        // vectorized::AggregateFunctionPtr function = vectorized::AggregateFunctionSimpleFactory::instance().get(
        //         "sum", arguments, arguments.back()->is_nullable());

        // if (function->is_state()) {
        //     std::cout << "有问题没" << std::endl;
        // }
        // std::cout << "----test1" << std::endl;
        // string a = arguments[0].name;
        // std::cout << a << std::endl;
        // a = arguments[1].name;
        // std::cout << a << std::endl;

        // auto x = arguments[0].column.get();
        // const ColumnConst& aggregate_function_name_column = static_cast<const ColumnConst&>(*x);

        // const ColumnConst * aggregate_function_name_column = check_and_get_column_const<ColumnString>(arguments[0].column.get());




        std::cout << "----test2" << std::endl;

        DataTypes argument_types(arguments.size() - 1);
        std::cout << "----test3" << std::endl;
        const DataTypeArray * arg = check_and_get_data_type<DataTypeArray>(arguments[1].type.get());
        std::cout << "----test4" << std::endl;
        argument_types[0] = arg->get_nested_type();
        std::cout << "----test5" << std::endl;

        test = arguments[1];
        

        if (!aggregate_function)
        {
            // std::cout << "----" <
            aggregate_function = vectorized::AggregateFunctionSimpleFactory::instance().get(
                    "sum", argument_types);
            std::cout << "----test8" << std::endl;
        }

        std::cout << "YEAH" << std::endl;
        
        return arguments[1].type;
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {

        const ColumnConst& rhs_value_column =
                static_cast<const ColumnConst&>(*block.get_by_position(arguments[0]).column.get());

        String aggregate_function_name_with_params = rhs_value_column.get_value<String>();
        std::cout << aggregate_function_name_with_params << std::endl;

        ColumnPtr haystack_ptr = block.get_by_position(arguments[0]).column;

        const ColumnConst* col_haystack_const =
                check_and_get_column_const<ColumnString>(&*haystack_ptr);

        String aggregate_function_name_with_param = col_haystack_const->get_value<String>();
        
        std::cout << aggregate_function_name_with_param << std::endl;

        AggregateFunctionSimpleFactory factory = AggregateFunctionSimpleFactory::instance();
        auto agg_function = factory.get(aggregate_function_name_with_params, {block.get_data_type(1)});

        std::cout << "----" << std::endl;
        // const IAggregateFunction & agg_func = *agg_function;
        
        const size_t num_arguments_columns = arguments.size() - 1;

        std::vector<const IColumn *> aggregate_arguments_vec(num_arguments_columns);

        std::cout << "----" << std::endl;
        // for (size_t i = 0; i < num_arguments_columns; ++i) {
        auto col = block.get_by_position(arguments[1]).column->convert_to_full_column_if_const();
        // const ColumnArray::Offsets64 * offsets_i = nullptr;

        if (const ColumnArray * arr = check_and_get_column<ColumnArray>(*col)) {
            aggregate_arguments_vec[0] = &arr->get_data();
            std::cout << "YEAH" << std::endl;
            const auto& name = arr->get_name();
            std::cout << &name << std::endl;
            std::cout << "----" << std::endl;
            // offsets_i = &arr->get_offsets();
        }
        else {
            std::cout << "---------ERROR----------" << std::endl;
        }

        // if (i == 0)
        //     offsets = offsets_i;
        // else if (*offsets_i != *offsets)
        //     std::cout << "Lengths of all arrays passed to " + get_name() + " must be equal." << std::endl;
        // }

        // aggregate_function->create(places);
        // for (size_t i = 0; i < input_rows_count; ++i)
        // {
        //     std::cout << aggregate_function->size_of_data() << std::endl;
        //     std::cout << "----" << std::endl;
        //     std::cout << aggregate_function->align_of_data() << std::endl;
        //     std::cout << "----" << std::endl;
        //     arena->aligned_alloc(aggregate_function->size_of_data(), aggregate_function->align_of_data());
        //     places[i] = arena->aligned_alloc(aggregate_function->size_of_data(), aggregate_function->align_of_data());
        //     try
        //     {
        //         agg_function->create(places[i]);
        //         std::cout << "--1--" << std::endl;
        //     }
        //     catch (...)
        //     {
        //         for (size_t j = 0; j < i; ++j)
        //             agg_function->destroy(places[j]);
        //         throw;
        //     }
        // }

        // auto guard = AggregateFunctionGuard(aggregate_function.get());

        const IColumn ** aggregate_arguments = aggregate_arguments_vec.data();

        std::unique_ptr<vectorized::Arena> arena = std::make_unique<vectorized::Arena>();

        std::unique_ptr<char[]> memory(new char[aggregate_function->size_of_data()]);
        AggregateDataPtr places;
        places = memory.get();

        const DataTypeArray * arg = check_and_get_data_type<DataTypeArray>(test.type.get());
        std::cout << "----test4" << std::endl;
        DataTypes argument_types(arguments.size() - 1);
        argument_types[0] = arg->get_nested_type();
        auto datatype = block.get_by_position(arguments[1]).type;

        auto column_result = datatype->create_column();

        agg_function->add_batch_range(0, input_rows_count, places, aggregate_arguments, arena.get(), false);

        // for (int i = 0; i < input_rows_count; i++) {
        //     aggregate_function->add(places, aggregate_arguments, i, nullptr);
        // }
        
        aggregate_function->insert_result_into(places, *column_result);

        ColumnPtr res_column = (*column_result).get_ptr();

        // doris::vectorized::ColumnPtr = doris::vectorized::IColumn::Ptr
        block.replace_by_position(result, std::move(res_column));


        return Status::OK();
    }

    mutable ColumnWithTypeAndName test;
    const DataTypeArray * arg;
    mutable AggregateFunctionPtr aggregate_function;
};

void register_function_array_reduce(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionArrayReduce>();
}

} // namespace doris::vectorized
