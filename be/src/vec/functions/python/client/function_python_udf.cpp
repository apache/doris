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

#include "function_python_udf.h"

#include <memory>
#include <string>
#include <utility>

#include "engine/python_env.hpp"
#include "engine/python_udf_executor.h"
#include "gutil/strings/substitute.h"
#include "runtime/exec_env.h"
#include "runtime/user_function_cache.h"
#include "vec/columns/column.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/serde/data_type_serde.h"
#include "vec/functions/function.h"

namespace doris::vectorized {

FunctionPythonUdf::FunctionPythonUdf(const TFunction& fn, const DataTypes& argument_types,
                                     DataTypePtr return_type)
        : _argument_types(argument_types), _return_type(std::move(return_type)), _function(fn) {
    _input_contains_nullable = false;
    for (const auto& type : argument_types) {
        auto argument_type = type;
        if (type->is_nullable()) {
            argument_type = remove_nullable(type);
            _input_contains_nullable = true;
        }
        _udf_input_types.push_back(argument_type);
    }
    _udf_output_type = _return_type;
    if (_return_type->is_nullable()) {
        _output_is_nullable = true;
        _udf_output_type = remove_nullable(_return_type);
    }
    this->_entry_python_filename = _function.scalar_fn.symbol;
}

Status FunctionPythonUdf::open(FunctionContext* context,
                               FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::THREAD_LOCAL) {
        auto* function_cache = UserFunctionCache::instance();
        std::string script_location;
        RETURN_IF_ERROR(function_cache->get_pypath(_function.id, _function.hdfs_location,
                                                   _function.checksum, &script_location));
        const auto executor = std::make_shared<pyudf::PythonUdfExecutor>();
        RETURN_IF_ERROR(executor->setup(script_location, this->_entry_python_filename,
                                        this->_function.python_udf_null_on_failure));
        context->set_function_state(FunctionContext::THREAD_LOCAL, executor);
        RETURN_IF_ERROR(pyudf::PythonEnv::init_env());
    }
    return Status::OK();
}

Status FunctionPythonUdf::execute(FunctionContext* context, Block& block,
                                  const ColumnNumbers& arguments, size_t result,
                                  size_t input_rows_count, bool dry_run) const {
    // 1. Get the python udf executor from the function state.
    const auto* python_udf_executor = static_cast<pyudf::PythonUdfExecutor*>(
            context->get_function_state(FunctionContext::THREAD_LOCAL));
    // 2. Check and prepare the udf input columns.
    const int arg_size = arguments.size();
    ColumnPtr udf_input_columns[arg_size];
    for (size_t arg_idx = 0; arg_idx < arg_size; ++arg_idx) {
        const ColumnWithTypeAndName& column = block.get_by_position(arguments[arg_idx]);
        DataTypePtr data_type = column.type;
        if (data_type->is_nullable() && !_input_contains_nullable) {
            return Status::InternalError(
                    fmt::format("Defined datatype is not nullable, but the actual column is "
                                "nullable, column type is {}",
                                data_type->get_name()));
        }
        if (data_type->is_nullable()) {
            data_type = remove_nullable(data_type);
        }
        DCHECK(_udf_input_types[arg_idx]->equals(*data_type))
                << " input column's type is " + data_type->get_name()
                << " does not equal to required type " << _udf_input_types[arg_idx]->get_name();
        const auto single_column = column.column->convert_to_full_column_if_const();
        udf_input_columns[arg_idx] = single_column;
    }
    // 3. Prepare the result output column.
    ColumnUInt8::MutablePtr null_map = ColumnUInt8::create();
    ColumnPtr udf_output_column;
    // 4. Process the data by python udf executor.
    const int batch_size = _function.batch_size;
    if (batch_size > 1) {
        RETURN_IF_ERROR(python_udf_executor->batch_execute(udf_input_columns,
                                                           arg_size, _udf_input_types, batch_size,
                                                           udf_output_column, _udf_output_type,
                                                           null_map));
    } else {
        RETURN_IF_ERROR(python_udf_executor->execute(udf_input_columns,
                                                     arg_size, _udf_input_types,
                                                     udf_output_column, _udf_output_type,
                                                     null_map));
    }
    // 5. Generate the output.
    block.replace_by_position(result,
                              ColumnNullable::create(udf_output_column, std::move(null_map)));
    return Status::OK();
}

Status FunctionPythonUdf::close(FunctionContext* context,
                                FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::THREAD_LOCAL) {
        const auto* python_udf_processer = static_cast<pyudf::PythonUdfExecutor*>(
                context->get_function_state(FunctionContext::THREAD_LOCAL));
        RETURN_IF_ERROR(python_udf_processer->close());
    }
    return Status::OK();
}

} // namespace doris::vectorized