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

#include <stddef.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <memory>
#include <utility>
#include <vector>

#include "common/status.h"
#include "olap/hll.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_complex.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris::vectorized {

struct HLLCardinality {
    static constexpr auto name = "hll_cardinality";

    using ReturnType = DataTypeNumber<Int64>;

    static void vector(const std::vector<HyperLogLog>& data, MutableColumnPtr& col_res) {
        typename ColumnVector<Int64>::Container& res =
                reinterpret_cast<ColumnVector<Int64>*>(col_res.get())->get_data();

        auto size = res.size();
        for (int i = 0; i < size; ++i) {
            res[i] = data[i].estimate_cardinality();
        }
    }

    static void vector_nullable(const std::vector<HyperLogLog>& data, const NullMap& nullmap,
                                MutableColumnPtr& col_res) {
        typename ColumnVector<Int64>::Container& res =
                reinterpret_cast<ColumnVector<Int64>*>(col_res.get())->get_data();

        auto size = res.size();
        for (int i = 0; i < size; ++i) {
            if (nullmap[i]) {
                res[i] = 0;
            } else {
                res[i] = data[i].estimate_cardinality();
            }
        }
    }
};

template <typename Function>
class FunctionHLL : public IFunction {
public:
    static constexpr auto name = Function::name;

    static FunctionPtr create() { return std::make_shared<FunctionHLL>(); }

    String get_name() const override { return Function::name; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<typename Function::ReturnType>();
    }

    bool use_default_implementation_for_nulls() const override { return false; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        auto column = block.get_by_position(arguments[0]).column;

        MutableColumnPtr column_result = get_return_type_impl({})->create_column();
        column_result->resize(input_rows_count);
        if (const ColumnNullable* col_nullable =
                    check_and_get_column<ColumnNullable>(column.get())) {
            const ColumnHLL* col =
                    check_and_get_column<ColumnHLL>(col_nullable->get_nested_column_ptr().get());
            const ColumnUInt8* col_nullmap = check_and_get_column<ColumnUInt8>(
                    col_nullable->get_null_map_column_ptr().get());

            if (col != nullptr && col_nullmap != nullptr) {
                Function::vector_nullable(col->get_data(), col_nullmap->get_data(), column_result);
                block.replace_by_position(result, std::move(column_result));
                return Status::OK();
            }
        } else if (const ColumnHLL* col = check_and_get_column<ColumnHLL>(column.get())) {
            Function::vector(col->get_data(), column_result);
            block.replace_by_position(result, std::move(column_result));
            return Status::OK();
        } else {
            return Status::RuntimeError("Illegal column {} of argument of function {}",
                                        block.get_by_position(arguments[0]).column->get_name(),
                                        get_name());
        }

        block.replace_by_position(result, std::move(column_result));
        return Status::OK();
    }
};

using FunctionHLLCardinality = FunctionHLL<HLLCardinality>;

void register_function_hll_cardinality(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionHLLCardinality>();
}

} // namespace doris::vectorized
