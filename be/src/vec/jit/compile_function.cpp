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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Interpreters/JIT/compileFunction.cpp
// and modified by Doris

#ifdef DORIS_ENABLE_JIT
#include "compile_function.h"

#include <llvm/IR/BasicBlock.h>
#include <llvm/IR/Function.h>
#include <llvm/IR/IRBuilder.h>


#include "vec/data_types/native.h"
#include "vec/columns/column.h"
#include "jit/jit.h"

namespace {
    struct ColumnDataPlaceholder {
        llvm::Value* data_init = nullptr; /// first row
        llvm::Value* null_init = nullptr;
        llvm::PHINode * data = nullptr; /// current row
        llvm::PHINode * null = nullptr;
    };
}


namespace doris::vectorized {

namespace ErrorCodes {
    extern const int LOGICAL_ERROR;
}

Status get_column_data(const IColumn * column, ColumnData& result) {
    const bool is_const = is_column_const(*column);

    if (is_const)
        return Status::RuntimeError("Input columns should not be constant");

    if (const auto* nullable = typeid_cast<const ColumnNullable *>(column)) {
        result.null_data = nullable->get_null_map_column().get_raw_data().data;
        column = & nullable->get_nested_column();
    }

    result.data = column->get_raw_data().data;

    return Status::OK();
}

static Status compile_function(llvm::Module& module, const IFunctionBase& function) {
    /** Algorithm is to create a loop that iterate over ColumnDataRowsSize size_t argument and
     * over ColumnData data and null_data. On each step compiled expression from function
     * will be executed over column data and null_data row.
     *
     * Example of preudocode of generated instructions of function with 1 input column.
     * In case of multiple columns more column_i_data, column_i_null_data is created.
     *
     * void compiled_function(size_t rows_count, ColumnData * columns)
     * {
     *     /// Initialize column values
     *
     *     Column0Type * column_0_data = static_cast<Column0Type *>(columns[0].data);
     *     UInt8 * column_0_null_data = static_cast<UInt8>(columns[0].null_data);
     *
     *     /// Initialize other input columns data with indexes < input_columns_count
     *
     *     ResultType * result_column_data = static_cast<ResultType *>(columns[input_columns_count].data);
     *     UInt8 * result_column_null_data = static_cast<UInt8 *>(columns[input_columns_count].data);
     *
     *     if (rows_count == 0)
     *         goto end;
     *
     *     /// Loop
     *
     *     size_t counter = 0;
     *
     *     loop:
     *
     *     /// Create column values tuple in case of non nullable type it is just column value
     *     /// In case of nullable type it is tuple of column value and is column row nullable
     *
     *     Column0Tuple column_0_value;
     *     if (Column0Type is nullable)
     *     {
     *         value[0] = column_0_data;
     *         value[1] = static_cast<bool>(column_1_null_data);
     *     }
     *     else
     *     {
     *         value[0] = column_0_data
     *     }
     *
     *     /// Initialize other input column values tuple with indexes < input_columns_count
     *     /// execute_compiled_expressions function takes input columns values and must return single result value
     *
     *     if (ResultType is nullable)
     *     {
     *         (ResultType, bool) result_column_value = execute_compiled_expressions(column_0_value, ...);
     *         *result_column_data = result_column_value[0];
     *         *result_column_null_data = static_cast<UInt8>(result_column_value[1]);
     *     }
     *     else
     *     {
     *         ResultType result_column_value = execute_compiled_expressions(column_0_value, ...);
     *         *result_column_data = result_column_value;
     *     }
     *
     *     /// Increment input and result column current row pointer
     *
     *     ++column_0_data;
     *     if (Column 0 type is nullable)
     *     {
     *         ++column_0_null_data;
     *     }
     *
     *     ++result_column_data;
     *     if  (ResultType  is nullable)
     *     {
     *         ++result_column_null_data;
     *     }
     *
     *     /// Increment loop counter and check if we should exit.
     *
     *     ++counter;
     *     if (counter == rows_count)
     *         goto end;
     *     else
     *         goto loop;
     *
     *   /// End
     *   end:
     *       return;
     * }
     */

    const auto& arg_types = function.get_argument_types();

    llvm::IRBuilder<> b(module.getContext());
    auto* size_type = b.getIntNTy(sizeof(size_t) * 8);
    auto* data_type = llvm::StructType::get(b.getInt8PtrTy(), b.getInt8PtrTy());
    auto* func_type = llvm::FunctionType::get(b.getVoidTy(), { size_type, data_type->getPointerTo() }, /*isVarArg=*/false);

    /// Create function in module

    auto* func = llvm::Function::Create(func_type, llvm::Function::ExternalLinkage, function.get_name(), module);
    auto* args = func->args().begin();
    llvm::Value* rows_count_arg = args++;
    llvm::Value* columns_arg = args++;

    /// Initialize ColumnDataPlaceholder llvm representation of ColumnData

    auto* entry = llvm::BasicBlock::Create(b.getContext(), "entry", func);
    b.SetInsertPoint(entry);

    std::vector<ColumnDataPlaceholder> columns(arg_types.size() + 1);
    for (size_t i = 0; i <= arg_types.size(); ++i) {
        const auto& type = i == arg_types.size() ? function.get_return_type() : arg_types[i];
        auto* data = b.CreateLoad(data_type, b.CreateConstInBoundsGEP1_64(data_type, columns_arg, i), "data_load");
        columns[i].data_init = b.CreatePointerCast(b.CreateExtractValue(data, {0}), to_native_type(b, remove_nullable(type))->getPointerTo(), "data_init");
        columns[i].null_init = type->is_nullable() ? b.CreateExtractValue(data, {1}) : nullptr;
    }

    /// Initialize loop

    auto* end = llvm::BasicBlock::Create(b.getContext(), "end", func);
    auto* loop = llvm::BasicBlock::Create(b.getContext(), "loop", func);
    b.CreateCondBr(b.CreateICmpEQ(rows_count_arg, llvm::ConstantInt::get(size_type, 0)), end, loop);

    b.SetInsertPoint(loop);

    auto* counter_phi = b.CreatePHI(rows_count_arg->getType(), 2, "counter_phi");
    counter_phi->addIncoming(llvm::ConstantInt::get(size_type, 0), entry);

    for (auto& col : columns) {
        col.data = b.CreatePHI(col.data_init->getType(), 2, "col.data_phi");
        col.data->addIncoming(col.data_init, entry);
        if (col.null_init) {
            col.null = b.CreatePHI(col.null_init->getType(), 2);
            col.null->addIncoming(col.null_init, entry);
        }
    }

    /// Initialize column row values

    Values arguments;
    arguments.reserve(arg_types.size());

    for (size_t i = 0; i < arg_types.size(); ++i) {
        auto& column = columns[i];
        auto type = arg_types[i];

        DCHECK(to_native_type(b, remove_nullable(type)));
        auto* value = b.CreateLoad(to_native_type(b, remove_nullable(type)), column.data);
        if (!type->is_nullable()) {
            arguments.emplace_back(value);
            continue;
        }

        auto* is_null = b.CreateICmpNE(b.CreateLoad(b.getInt8Ty(), column.null), b.getInt8(0));

        DCHECK(to_native_type(b, type));
        auto* nullable_unitilized = llvm::Constant::getNullValue(to_native_type(b, type));
        auto* nullable_value = b.CreateInsertValue(b.CreateInsertValue(nullable_unitilized, value, {0}), is_null, {1});
        arguments.emplace_back(nullable_value);
    }

    /// Compile values for column rows and store compiled value in result column
    llvm::Value *result;
    auto status = function.compile(b, std::move(arguments), &result);
    if (!status.ok())
        return status;

    if (columns.back().null) {
        b.CreateStore(b.CreateExtractValue(result, {0}), columns.back().data);
        b.CreateStore(b.CreateSelect(b.CreateExtractValue(result, {1}), b.getInt8(1), b.getInt8(0)), columns.back().null);
    }
    else {
        b.CreateStore(result, columns.back().data);
    }

    /// End of loop

    auto* cur_block = b.GetInsertBlock();
    for (auto& col : columns) {
        col.data->addIncoming(b.CreateConstInBoundsGEP1_64(col.data->getType()->getPointerElementType(), col.data, 1), cur_block);
        if (col.null)
            col.null->addIncoming(b.CreateConstInBoundsGEP1_64(col.null->getType()->getPointerElementType(), col.null, 1), cur_block);
    }

    auto* value = b.CreateAdd(counter_phi, llvm::ConstantInt::get(size_type, 1));
    counter_phi->addIncoming(value, cur_block);

    b.CreateCondBr(b.CreateICmpEQ(value, rows_count_arg), end, loop);

    b.SetInsertPoint(end);
    b.CreateRetVoid();

    return Status::OK();
}


Status compile_function(JIT& jit, const IFunctionBase& function, CompiledFunction& result) {
    JIT::CompiledModule compiled_module;
    auto status = jit.compile_module([&](llvm::Module& module) {
        return compile_function(module, function);
    }, compiled_module);

    if (!status.ok())
        return status;

    auto compiled_function_ptr = reinterpret_cast<JITCompiledFunction>(compiled_module.function_name_to_symbol[function.get_name()]);

    result.compiled_function = compiled_function_ptr;
    result.compiled_module = compiled_module;

    return Status::OK();
}

}
#endif
