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

#include "vec/functions/function_wasm.h"

#include <brpc/controller.h>
#include <fmt/format.h>

#include <algorithm>
#include <memory>
#include <string>
#include <utility>

#include "gutil/strings/substitute.h"
#include "runtime/exec_env.h"
#include "runtime/user_function_cache.h"
#include "vec/columns/column.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/serde/data_type_serde.h"
#include "vec/functions/function.h"

namespace doris::vectorized {

FunctionWasm::FunctionWasm(const TFunction& fn, const DataTypes& argument_types,
                           const DataTypePtr& return_type)
        : _argument_types(argument_types), _return_type(return_type), _tfn(fn) {
    _is_nullable = false;
    for (const auto& type : argument_types) {
        auto argument_type = type;
        if (type->is_nullable()) {
            argument_type = remove_nullable(type);
            _is_nullable = true;
        }
        _not_nullable_argument_types.push_back(argument_type);
    }
}

Status FunctionWasm::open(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        string local_location;
        auto* function_cache = UserFunctionCache::instance();
        RETURN_IF_ERROR(function_cache->get_watpath(_tfn.id, _tfn.hdfs_location, _tfn.checksum,
                                                    &local_location));
        std::shared_ptr<WasmFunctionManager> manager = std::make_shared<WasmFunctionManager>();
        context->set_function_state(FunctionContext::THREAD_LOCAL, manager);
        std::ifstream wat_file;
        wat_file.open(local_location.c_str());
        std::stringstream str_stream;
        str_stream << wat_file.rdbuf();
        const std::string wasm_body = str_stream.str();
        manager->RegisterFunction(_tfn.name.function_name, _tfn.scalar_fn.symbol, wasm_body);
    }
    return Status::OK();
}

Status FunctionWasm::execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                             size_t result, size_t input_rows_count, bool dry_run) {
    /**
     * i32, a 32-bit integer (equivalent to C++’s signed long int)
     * i64, a 64-bit integer (equivalent to C++’s signed long long int)
     * f32, 32-bit float (equivalent to C++’s float)
     * f64, 64-bit float (equivalent to C++’s double)
     */
    int arg_size = arguments.size();
    ColumnPtr data_cols[arg_size];
    auto* manager = reinterpret_cast<WasmFunctionManager*>(
            context->get_function_state(FunctionContext::THREAD_LOCAL));
    auto return_type = _return_type;
    auto result_nullable = return_type->is_nullable();
    ColumnUInt8::MutablePtr null_map = nullptr;

    if (result_nullable) {
        return_type = remove_nullable(_return_type);
        null_map = ColumnUInt8::create(input_rows_count, 0);
        memset(null_map->get_data().data(), 0, input_rows_count);
    }

    auto result_col = return_type->create_column();
    result_col->resize(input_rows_count);

    // check type : defined datatype same with param datatype
    for (size_t arg_idx = 0; arg_idx < arg_size; ++arg_idx) {
        ColumnWithTypeAndName& column = block.get_by_position(arguments[arg_idx]);
        DataTypePtr data_type = column.type;
        if (data_type->is_nullable() && !_is_nullable) {
            return Status::InternalError(fmt::format(
                    "Defined datatype is not nullable, but param datatype is nullable"));
        }

        if (data_type->is_nullable()) {
            data_type = remove_nullable(data_type);
        }

        DCHECK(_not_nullable_argument_types[arg_idx]->equals(*data_type))
                << " input column's type is " + data_type->get_name()
                << " does not equal to required type "
                << _not_nullable_argument_types[arg_idx]->get_name();

        auto data_col = column.column->convert_to_full_column_if_const();

        data_cols[arg_idx] = data_col;
    }

    // step1. process column value to wasm param
    // step2. call wasm function
    // step3. return wasm result to column value
    // TODO: vec the code to call wasm fun
    int row_size = data_cols[0]->size();
    for (size_t i = 0; i < row_size; ++i) {
        std::vector<wasmtime::Val> params;
        for (size_t arg_idx = 0; arg_idx < arg_size; ++arg_idx) {
            WhichDataType which_type(_not_nullable_argument_types[arg_idx]);
            if (data_cols[arg_idx]->is_null_at(i)) {
                null_map->get_data()[i] = 1;
                continue;
            }
            if (which_type.is_int32()) {
                auto data_col = data_cols[arg_idx];
                if (data_col->is_nullable()) {
                    data_col = remove_nullable(data_col);
                }
                const auto* param_column = check_and_get_column<ColumnInt32>(data_col);
                params.emplace_back(param_column->get_data()[i]);
            } else if (which_type.is_int64()) {
                auto data_col = data_cols[arg_idx];
                if (data_col->is_nullable()) {
                    data_col = remove_nullable(data_col);
                }
                const auto* param_column = check_and_get_column<ColumnInt64>(data_col);
                params.emplace_back(param_column->get_data()[i]);
            } else if (which_type.is_float32()) {
                auto data_col = data_cols[arg_idx];
                if (data_col->is_nullable()) {
                    data_col = remove_nullable(data_col);
                }
                const auto* param_column = check_and_get_column<ColumnFloat32>(data_col);
                params.emplace_back(param_column->get_data()[i]);
            } else if (which_type.is_float64()) {
                auto data_col = data_cols[arg_idx];
                if (data_col->is_nullable()) {
                    data_col = remove_nullable(data_col);
                }
                const auto* param_column = check_and_get_column<ColumnFloat64>(data_col);
                params.emplace_back(param_column->get_data()[i]);
            }
        }
        if (null_map->get_data()[i] == 1) {
            continue;
        }
        auto rets = manager->runElemFunc(_tfn.name.function_name, params);

        auto ret = rets.at(0);

        if (ret.kind() == wasmtime::ValKind::I32) {
            reinterpret_cast<ColumnInt32&>(*result_col).get_data()[i] = ret.i32();
        } else if (ret.kind() == wasmtime::ValKind::I64) {
            reinterpret_cast<ColumnInt64&>(*result_col).get_data()[i] = ret.i64();
        } else if (ret.kind() == wasmtime::ValKind::F32) {
            reinterpret_cast<ColumnFloat32&>(*result_col).get_data()[i] = ret.f32();
        } else if (ret.kind() == wasmtime::ValKind::F64) {
            reinterpret_cast<ColumnFloat64&>(*result_col).get_data()[i] = ret.f64();
        }
    }

    if (result_nullable) {
        block.replace_by_position(
                result, ColumnNullable::create(std::move(result_col), std::move(null_map)));
    } else {
        block.replace_by_position(result, std::move(result_col));
    }

    return Status::OK();
}

} // namespace doris::vectorized