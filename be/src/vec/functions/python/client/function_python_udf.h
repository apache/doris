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

#include <memory>
#include <vector>

#include "common/status.h"
#include "udf/udf.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/columns_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/functions/function.h"

namespace doris::vectorized {

/**
 * The {@link FunctionPythonUdf} class represents a Python UDF (User Defined Function).
 * It extends the {@link IFunctionBase} interface to provide functionality for executing Python UDFs
 * within the vectorized sql engine. This class handles the setup, execution, and teardown of Python UDFs,
 * ensuring that the UDFs are executed in a separate subprocess to avoid GIL (Global Interpreter Lock) issues.
 *
 * The class is responsible for:
 * - Creating and managing the Python UDF environment.
 * - Preparing and executing the UDF on input columns.
 * - Handling the lifecycle of the UDF execution, including setup and cleanup.
 */
class FunctionPythonUdf final : public IFunctionBase {
public:
    static FunctionBasePtr create(const TFunction& fn, const ColumnsWithTypeAndName& argument_types,
                                  const DataTypePtr& return_type) {
        DataTypes data_types(argument_types.size());
        for (size_t i = 0; i < argument_types.size(); ++i) {
            data_types[i] = argument_types[i].type;
        }
        return std::make_shared<FunctionPythonUdf>(fn, data_types, return_type);
    }

    FunctionPythonUdf(const TFunction& fn, const DataTypes& argument_types,
                      DataTypePtr return_type);

    String get_name() const override {
        return fmt::format("{}: [{}/{}]", _function.name.function_name, _function.hdfs_location,
                           _function.scalar_fn.symbol);
    }
    const DataTypes& get_argument_types() const override { return _argument_types; }
    const DataTypePtr& get_return_type() const override { return _return_type; }

    PreparedFunctionPtr prepare(FunctionContext* context, const Block& sample_block,
                                const ColumnNumbers& arguments, size_t result) const override {
        return nullptr;
    }

    Status open(FunctionContext* context, FunctionContext::FunctionStateScope scope) override;

    Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                   size_t result, size_t input_rows_count, bool dry_run = false) const override;

    bool is_use_default_implementation_for_constants() const override { return true; }

    Status close(FunctionContext* context, FunctionContext::FunctionStateScope scope) override;

private:
    // original function input/output types.
    DataTypes _argument_types;
    DataTypePtr _return_type;
    // udf input/output types.
    DataTypes _udf_input_types;
    DataTypePtr _udf_output_type;
    bool _input_contains_nullable;
    bool _output_is_nullable;
    std::string _entry_python_filename;
    TFunction _function;
};
} // namespace doris::vectorized
