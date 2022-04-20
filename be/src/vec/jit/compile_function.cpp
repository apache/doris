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

Status compile_functions(JIT& jit, std::vector<FunctionBasePtr>& functions, std::vector<CompiledFunction>& results) {
    JIT::CompiledModule compiled_module;

    std::vector<Status> compile_status(functions.size());

    auto status = jit.compile_module([&](llvm::Module& module) {
        size_t i = 0;
        for (auto& function : functions) {
            auto st = compile_function(module, *function);
            compile_status[i++] = st;
        }
        return Status::OK();
    }, compiled_module);

    DCHECK(functions.size() == compile_status.size());

    for (size_t i = 0; i < functions.size(); i++) {
        if (compile_status[i].ok()) {
            auto& function = functions[i];
            auto compiled_function_ptr = reinterpret_cast<JITCompiledFunction>(compiled_module.function_name_to_symbol[function->get_name()]);
            results.emplace_back(CompiledFunction{compiled_function_ptr});
        } else
            results.emplace_back(CompiledFunction{nullptr});
    }

    return status;
}

static void compile_create_aggregate_states_functions(llvm::Module & module, const std::vector<AggregateFunctionWithOffset> & functions, const std::string & name) {
    auto & context = module.getContext();
    llvm::IRBuilder<> b(context);

    auto * aggregate_data_places_type = b.getInt8Ty()->getPointerTo();
    auto * create_aggregate_states_function_type = llvm::FunctionType::get(b.getVoidTy(), { aggregate_data_places_type }, false);
    auto * create_aggregate_states_function = llvm::Function::Create(create_aggregate_states_function_type, llvm::Function::ExternalLinkage, name, module);

    auto * arguments = create_aggregate_states_function->args().begin();
    llvm::Value * aggregate_data_place_arg = arguments++;

    auto * entry = llvm::BasicBlock::Create(b.getContext(), "entry", create_aggregate_states_function);
    b.SetInsertPoint(entry);

    std::vector<ColumnDataPlaceholder> columns(functions.size());
    for (const auto & function_to_compile : functions)
    {
        size_t aggregate_function_offset = function_to_compile.aggregate_data_offset;
        const auto * aggregate_function = function_to_compile.function;
        auto * aggregation_place_with_offset = b.CreateConstInBoundsGEP1_64(aggregate_data_place_arg->getType()->getPointerElementType(), aggregate_data_place_arg, aggregate_function_offset);
        aggregate_function->compile_create(b, aggregation_place_with_offset);
    }

    b.CreateRetVoid();
}

static void compile_add_into_aggregate_states_functions(llvm::Module & module, const std::vector<AggregateFunctionWithOffset> & functions, const std::string & name) {
    auto & context = module.getContext();
    llvm::IRBuilder<> b(context);

    auto * size_type = b.getIntNTy(sizeof(size_t) * 8);
    auto * places_type = b.getInt8Ty()->getPointerTo()->getPointerTo();
    auto * column_data_type = llvm::StructType::get(b.getInt8PtrTy(), b.getInt8PtrTy());

    auto * aggregate_loop_func_declaration = llvm::FunctionType::get(b.getVoidTy(), { size_type, column_data_type->getPointerTo(), places_type }, false);
    auto * aggregate_loop_func_definition = llvm::Function::Create(aggregate_loop_func_declaration, llvm::Function::ExternalLinkage, name, module);

    auto * arguments = aggregate_loop_func_definition->args().begin();
    llvm::Value * rows_count_arg = arguments++;
    llvm::Value * columns_arg = arguments++;
    llvm::Value * places_arg = arguments++;

    /// Initialize ColumnDataPlaceholder llvm representation of ColumnData

    auto * entry = llvm::BasicBlock::Create(b.getContext(), "entry", aggregate_loop_func_definition);
    b.SetInsertPoint(entry);

    std::vector<ColumnDataPlaceholder> columns;
    size_t previous_columns_size = 0;

    for (const auto & function : functions)
    {
        auto argument_types = function.function->get_argument_types();

        ColumnDataPlaceholder data_placeholder;

        size_t function_arguments_size = argument_types.size();

        for (size_t column_argument_index = 0; column_argument_index < function_arguments_size; ++column_argument_index)
        {
            const auto & argument_type = argument_types[column_argument_index];
            auto * data = b.CreateLoad(column_data_type, b.CreateConstInBoundsGEP1_64(column_data_type, columns_arg, previous_columns_size + column_argument_index));
            data_placeholder.data_init = b.CreatePointerCast(b.CreateExtractValue(data, {0}), to_native_type(b, remove_nullable(argument_type))->getPointerTo());
            data_placeholder.null_init = argument_type->is_nullable() ? b.CreateExtractValue(data, {1}) : nullptr;
            columns.emplace_back(data_placeholder);
        }

        previous_columns_size += function_arguments_size;
    }

    /// Initialize loop

    auto * end = llvm::BasicBlock::Create(b.getContext(), "end", aggregate_loop_func_definition);
    auto * loop = llvm::BasicBlock::Create(b.getContext(), "loop", aggregate_loop_func_definition);

    b.CreateCondBr(b.CreateICmpEQ(rows_count_arg, llvm::ConstantInt::get(size_type, 0)), end, loop);

    b.SetInsertPoint(loop);

    auto * counter_phi = b.CreatePHI(rows_count_arg->getType(), 2);
    counter_phi->addIncoming(llvm::ConstantInt::get(size_type, 0), entry);

    auto * places_phi = b.CreatePHI(places_arg->getType(), 2);
    places_phi->addIncoming(places_arg, entry);

    for (auto & col : columns)
    {
        col.data = b.CreatePHI(col.data_init->getType(), 2);
        col.data->addIncoming(col.data_init, entry);

        if (col.null_init)
        {
            col.null = b.CreatePHI(col.null_init->getType(), 2);
            col.null->addIncoming(col.null_init, entry);
        }
    }

    auto * aggregation_place = b.CreateLoad(b.getInt8Ty()->getPointerTo(), places_phi);

    previous_columns_size = 0;
    for (const auto & function : functions)
    {
        size_t aggregate_function_offset = function.aggregate_data_offset;
        const auto * aggregate_function_ptr = function.function;

        auto arguments_types = function.function->get_argument_types();
        std::vector<llvm::Value *> arguments_values;

        size_t function_arguments_size = arguments_types.size();
        arguments_values.resize(function_arguments_size);

        for (size_t column_argument_index = 0; column_argument_index < function_arguments_size; ++column_argument_index)
        {
            auto * column_argument_data = columns[previous_columns_size + column_argument_index].data;
            auto * column_argument_null_data = columns[previous_columns_size + column_argument_index].null;

            auto & argument_type = arguments_types[column_argument_index];

            auto * value = b.CreateLoad(to_native_type(b, remove_nullable(argument_type)), column_argument_data);
            if (!argument_type->is_nullable())
            {
                arguments_values[column_argument_index] = value;
                continue;
            }

            auto * is_null = b.CreateICmpNE(b.CreateLoad(b.getInt8Ty(), column_argument_null_data), b.getInt8(0));
            auto * nullable_unitilized = llvm::Constant::getNullValue(to_native_type(b, argument_type));
            auto * nullable_value = b.CreateInsertValue(b.CreateInsertValue(nullable_unitilized, value, {0}), is_null, {1});
            arguments_values[column_argument_index] = nullable_value;
        }

        auto * aggregation_place_with_offset = b.CreateConstInBoundsGEP1_64(aggregation_place->getType()->getPointerElementType(), aggregation_place, aggregate_function_offset);
        aggregate_function_ptr->compile_add(b, aggregation_place_with_offset, arguments_types, arguments_values);

        previous_columns_size += function_arguments_size;
    }

    /// End of loop

    auto * cur_block = b.GetInsertBlock();
    for (auto & col : columns)
    {
        col.data->addIncoming(b.CreateConstInBoundsGEP1_64(col.data->getType()->getPointerElementType(), col.data, 1), cur_block);

        if (col.null)
            col.null->addIncoming(b.CreateConstInBoundsGEP1_64(col.null->getType()->getPointerElementType(), col.null, 1), cur_block);
    }

    places_phi->addIncoming(b.CreateConstInBoundsGEP1_64(places_phi->getType()->getPointerElementType(), places_phi, 1), cur_block);

    auto * value = b.CreateAdd(counter_phi, llvm::ConstantInt::get(size_type, 1));
    counter_phi->addIncoming(value, cur_block);

    b.CreateCondBr(b.CreateICmpEQ(value, rows_count_arg), end, loop);

    b.SetInsertPoint(end);
    b.CreateRetVoid();
}

static void compile_add_into_aggregate_states_functions_single_place(llvm::Module & module, const std::vector<AggregateFunctionWithOffset> & functions, const std::string & name)
{
    auto & context = module.getContext();
    llvm::IRBuilder<> b(context);

    auto * size_type = b.getIntNTy(sizeof(size_t) * 8);
    auto * places_type = b.getInt8Ty()->getPointerTo();
    auto * column_data_type = llvm::StructType::get(b.getInt8PtrTy(), b.getInt8PtrTy());

    auto * aggregate_loop_func_declaration = llvm::FunctionType::get(b.getVoidTy(), { size_type, column_data_type->getPointerTo(), places_type }, false);
    auto * aggregate_loop_func_definition = llvm::Function::Create(aggregate_loop_func_declaration, llvm::Function::ExternalLinkage, name, module);

    auto * arguments = aggregate_loop_func_definition->args().begin();
    llvm::Value * rows_count_arg = arguments++;
    llvm::Value * columns_arg = arguments++;
    llvm::Value * place_arg = arguments++;

    /// Initialize ColumnDataPlaceholder llvm representation of ColumnData

    auto * entry = llvm::BasicBlock::Create(b.getContext(), "entry", aggregate_loop_func_definition);
    b.SetInsertPoint(entry);

    std::vector<ColumnDataPlaceholder> columns;
    size_t previous_columns_size = 0;

    for (const auto & function : functions)
    {
        auto argument_types = function.function->get_argument_types();

        ColumnDataPlaceholder data_placeholder;

        size_t function_arguments_size = argument_types.size();

        for (size_t column_argument_index = 0; column_argument_index < function_arguments_size; ++column_argument_index)
        {
            const auto & argument_type = argument_types[column_argument_index];
            auto * data = b.CreateLoad(column_data_type, b.CreateConstInBoundsGEP1_64(column_data_type, columns_arg, previous_columns_size + column_argument_index));
            data_placeholder.data_init = b.CreatePointerCast(b.CreateExtractValue(data, {0}), to_native_type(b, remove_nullable(argument_type))->getPointerTo());
            data_placeholder.null_init = argument_type->is_nullable() ? b.CreateExtractValue(data, {1}) : nullptr;
            columns.emplace_back(data_placeholder);
        }

        previous_columns_size += function_arguments_size;
    }

    /// Initialize loop

    auto * end = llvm::BasicBlock::Create(b.getContext(), "end", aggregate_loop_func_definition);
    auto * loop = llvm::BasicBlock::Create(b.getContext(), "loop", aggregate_loop_func_definition);

    b.CreateCondBr(b.CreateICmpEQ(rows_count_arg, llvm::ConstantInt::get(size_type, 0)), end, loop);

    b.SetInsertPoint(loop);

    auto * counter_phi = b.CreatePHI(rows_count_arg->getType(), 2);
    counter_phi->addIncoming(llvm::ConstantInt::get(size_type, 0), entry);

    for (auto & col : columns)
    {
        col.data = b.CreatePHI(col.data_init->getType(), 2);
        col.data->addIncoming(col.data_init, entry);

        if (col.null_init)
        {
            col.null = b.CreatePHI(col.null_init->getType(), 2);
            col.null->addIncoming(col.null_init, entry);
        }
    }

    previous_columns_size = 0;
    for (const auto & function : functions)
    {
        size_t aggregate_function_offset = function.aggregate_data_offset;
        const auto * aggregate_function_ptr = function.function;

        auto arguments_types = function.function->get_argument_types();
        std::vector<llvm::Value *> arguments_values;

        size_t function_arguments_size = arguments_types.size();
        arguments_values.resize(function_arguments_size);

        for (size_t column_argument_index = 0; column_argument_index < function_arguments_size; ++column_argument_index)
        {
            auto * column_argument_data = columns[previous_columns_size + column_argument_index].data;
            auto * column_argument_null_data = columns[previous_columns_size + column_argument_index].null;

            auto & argument_type = arguments_types[column_argument_index];

            auto * value = b.CreateLoad(to_native_type(b, remove_nullable(argument_type)), column_argument_data);
            if (!argument_type->is_nullable())
            {
                arguments_values[column_argument_index] = value;
                continue;
            }

            auto * is_null = b.CreateICmpNE(b.CreateLoad(b.getInt8Ty(), column_argument_null_data), b.getInt8(0));
            auto * nullable_unitilized = llvm::Constant::getNullValue(to_native_type(b, argument_type));
            auto * nullable_value = b.CreateInsertValue(b.CreateInsertValue(nullable_unitilized, value, {0}), is_null, {1});
            arguments_values[column_argument_index] = nullable_value;
        }

        auto * aggregation_place_with_offset = b.CreateConstInBoundsGEP1_64(place_arg->getType()->getPointerElementType(), place_arg, aggregate_function_offset);
        aggregate_function_ptr->compile_add(b, aggregation_place_with_offset, arguments_types, arguments_values);

        previous_columns_size += function_arguments_size;
    }

    /// End of loop

    auto * cur_block = b.GetInsertBlock();
    for (auto & col : columns)
    {
        col.data->addIncoming(b.CreateConstInBoundsGEP1_64(col.data->getType()->getPointerElementType(), col.data, 1), cur_block);

        if (col.null)
            col.null->addIncoming(b.CreateConstInBoundsGEP1_64(col.null->getType()->getPointerElementType(), col.null, 1), cur_block);
    }

    auto * value = b.CreateAdd(counter_phi, llvm::ConstantInt::get(size_type, 1));
    counter_phi->addIncoming(value, cur_block);

    b.CreateCondBr(b.CreateICmpEQ(value, rows_count_arg), end, loop);

    b.SetInsertPoint(end);
    b.CreateRetVoid();
}

static void compile_merge_aggregates_states(llvm::Module & module, const std::vector<AggregateFunctionWithOffset> & functions, const std::string & name)
{
    auto & context = module.getContext();
    llvm::IRBuilder<> b(context);

    auto * aggregate_data_places_type = b.getInt8Ty()->getPointerTo();
    auto * aggregate_loop_func_declaration = llvm::FunctionType::get(b.getVoidTy(), { aggregate_data_places_type, aggregate_data_places_type }, false);
    auto * aggregate_loop_func = llvm::Function::Create(aggregate_loop_func_declaration, llvm::Function::ExternalLinkage, name, module);

    auto * arguments = aggregate_loop_func->args().begin();
    llvm::Value * aggregate_data_place_dst_arg = arguments++;
    llvm::Value * aggregate_data_place_src_arg = arguments++;

    auto * entry = llvm::BasicBlock::Create(b.getContext(), "entry", aggregate_loop_func);
    b.SetInsertPoint(entry);

    for (const auto & function_to_compile : functions)
    {
        size_t aggregate_function_offset = function_to_compile.aggregate_data_offset;
        const auto * aggregate_function_ptr = function_to_compile.function;

        auto * aggregate_data_place_merge_dst_with_offset = b.CreateConstInBoundsGEP1_64(aggregate_data_place_dst_arg->getType()->getPointerElementType(), aggregate_data_place_dst_arg, aggregate_function_offset);
        auto * aggregate_data_place_merge_src_with_offset = b.CreateConstInBoundsGEP1_64(aggregate_data_place_src_arg->getType()->getPointerElementType(), aggregate_data_place_src_arg, aggregate_function_offset);

        aggregate_function_ptr->compile_merge(b, aggregate_data_place_merge_dst_with_offset, aggregate_data_place_merge_src_with_offset);
    }

    b.CreateRetVoid();
}


static void compile_insert_aggregates_into_result_columns(llvm::Module & module, const std::vector<AggregateFunctionWithOffset> & functions, const std::string & name)
{
    auto & context = module.getContext();
    llvm::IRBuilder<> b(context);

    auto * size_type = b.getIntNTy(sizeof(size_t) * 8);

    auto * column_data_type = llvm::StructType::get(b.getInt8PtrTy(), b.getInt8PtrTy());
    auto * aggregate_data_places_type = b.getInt8Ty()->getPointerTo()->getPointerTo();
    auto * aggregate_loop_func_declaration = llvm::FunctionType::get(b.getVoidTy(), { size_type, column_data_type->getPointerTo(), aggregate_data_places_type }, false);
    auto * aggregate_loop_func = llvm::Function::Create(aggregate_loop_func_declaration, llvm::Function::ExternalLinkage, name, module);

    auto * arguments = aggregate_loop_func->args().begin();
    llvm::Value * rows_count_arg = &*arguments++;
    llvm::Value * columns_arg = &*arguments++;
    llvm::Value * aggregate_data_places_arg = &*arguments++;

    auto * entry = llvm::BasicBlock::Create(b.getContext(), "entry", aggregate_loop_func);
    b.SetInsertPoint(entry);

    std::vector<ColumnDataPlaceholder> columns(functions.size());
    for (size_t i = 0; i < functions.size(); ++i)
    {
        auto return_type = functions[i].function->get_return_type();
        auto * data = b.CreateLoad(column_data_type, b.CreateConstInBoundsGEP1_64(column_data_type, columns_arg, i));
        columns[i].data_init = b.CreatePointerCast(b.CreateExtractValue(data, {0}), to_native_type(b, remove_nullable(return_type))->getPointerTo());
        columns[i].null_init = return_type->is_nullable() ? b.CreateExtractValue(data, {1}) : nullptr;
    }

    auto * end = llvm::BasicBlock::Create(b.getContext(), "end", aggregate_loop_func);
    auto * loop = llvm::BasicBlock::Create(b.getContext(), "loop", aggregate_loop_func);

    b.CreateCondBr(b.CreateICmpEQ(rows_count_arg, llvm::ConstantInt::get(size_type, 0)), end, loop);

    b.SetInsertPoint(loop);

    auto * counter_phi = b.CreatePHI(rows_count_arg->getType(), 2);
    counter_phi->addIncoming(llvm::ConstantInt::get(size_type, 0), entry);

    auto * aggregate_data_place_phi = b.CreatePHI(aggregate_data_places_type, 2);
    aggregate_data_place_phi->addIncoming(aggregate_data_places_arg, entry);

    for (auto & col : columns)
    {
        col.data = b.CreatePHI(col.data_init->getType(), 2);
        col.data->addIncoming(col.data_init, entry);

        if (col.null_init)
        {
            col.null = b.CreatePHI(col.null_init->getType(), 2);
            col.null->addIncoming(col.null_init, entry);
        }
    }

    for (size_t i = 0; i < functions.size(); ++i)
    {
        size_t aggregate_function_offset = functions[i].aggregate_data_offset;
        const auto * aggregate_function_ptr = functions[i].function;

        auto * aggregate_data_place = b.CreateLoad(b.getInt8Ty()->getPointerTo(), aggregate_data_place_phi);
        auto * aggregation_place_with_offset = b.CreateConstInBoundsGEP1_64(aggregate_data_place->getType()->getPointerElementType(), aggregate_data_place, aggregate_function_offset);

        auto * final_value = aggregate_function_ptr->compile_get_result(b, aggregation_place_with_offset);

        if (columns[i].null_init)
        {
            b.CreateStore(b.CreateExtractValue(final_value, {0}), columns[i].data);
            b.CreateStore(b.CreateSelect(b.CreateExtractValue(final_value, {1}), b.getInt8(1), b.getInt8(0)), columns[i].null);
        }
        else
        {
            b.CreateStore(final_value, columns[i].data);
        }
    }

    /// End of loop

    auto * cur_block = b.GetInsertBlock();
    for (auto & col : columns)
    {
        col.data->addIncoming(b.CreateConstInBoundsGEP1_64(col.data->getType()->getPointerElementType(), col.data, 1), cur_block);

        if (col.null)
            col.null->addIncoming(b.CreateConstInBoundsGEP1_64(col.null->getType()->getPointerElementType(), col.null, 1), cur_block);
    }

    auto * value = b.CreateAdd(counter_phi, llvm::ConstantInt::get(size_type, 1), "", true, true);
    counter_phi->addIncoming(value, cur_block);

    aggregate_data_place_phi->addIncoming(b.CreateConstInBoundsGEP1_64(aggregate_data_place_phi->getType()->getPointerElementType(), aggregate_data_place_phi, 1), cur_block);

    b.CreateCondBr(b.CreateICmpEQ(value, rows_count_arg), end, loop);

    b.SetInsertPoint(end);
    b.CreateRetVoid();
}

Status compile_aggregate_functions(JIT & jit, const std::vector<AggregateFunctionWithOffset>& functions, const std::string& functions_dump_name, CompiledAggregateFunctions& compiled_functions) {
    std::string create_aggregate_states_functions_name = functions_dump_name + "_create";
    std::string add_aggregate_states_functions_name = functions_dump_name + "_add";
    std::string add_aggregate_states_functions_name_single_place = functions_dump_name + "_add_single_place";
    std::string merge_aggregate_states_functions_name = functions_dump_name + "_merge";
    std::string insert_aggregate_states_functions_name = functions_dump_name + "_insert";

    JIT::CompiledModule compiled_module;
    auto status = jit.compile_module([&](llvm::Module & module) {
        compile_create_aggregate_states_functions(module, functions, create_aggregate_states_functions_name);
        compile_add_into_aggregate_states_functions(module, functions, add_aggregate_states_functions_name);
        compile_add_into_aggregate_states_functions_single_place(module, functions, add_aggregate_states_functions_name_single_place);
        compile_merge_aggregates_states(module, functions, merge_aggregate_states_functions_name);
        compile_insert_aggregates_into_result_columns(module, functions, insert_aggregate_states_functions_name);
        return Status::OK();
    }, compiled_module);

    if (!status.ok())
        return status;
    
    auto create_aggregate_states_function = reinterpret_cast<JITCreateAggregateStatesFunction>(compiled_module.function_name_to_symbol[create_aggregate_states_functions_name]);
    auto add_into_aggregate_states_function = reinterpret_cast<JITAddIntoAggregateStatesFunction>(compiled_module.function_name_to_symbol[add_aggregate_states_functions_name]);
    auto add_into_aggregate_states_function_single_place = reinterpret_cast<JITAddIntoAggregateStatesFunctionSinglePlace>(compiled_module.function_name_to_symbol[add_aggregate_states_functions_name_single_place]);
    auto merge_aggregate_states_function = reinterpret_cast<JITMergeAggregateStatesFunction>(compiled_module.function_name_to_symbol[merge_aggregate_states_functions_name]);
    auto insert_aggregate_states_function = reinterpret_cast<JITInsertAggregateStatesIntoColumnsFunction>(compiled_module.function_name_to_symbol[insert_aggregate_states_functions_name]);

    assert(create_aggregate_states_function);
    assert(add_into_aggregate_states_function);
    assert(add_into_aggregate_states_function_single_place);
    assert(merge_aggregate_states_function);
    assert(insert_aggregate_states_function);

    CompiledAggregateFunctions compiled_aggregate_functions
    {
        .create_aggregate_states_function = create_aggregate_states_function,
        .add_into_aggregate_states_function = add_into_aggregate_states_function,
        .add_into_aggregate_states_function_single_place = add_into_aggregate_states_function_single_place,
        .merge_aggregate_states_function = merge_aggregate_states_function,
        .insert_aggregates_into_columns_function = insert_aggregate_states_function,

        .functions_count = functions.size(),
        .compiled_module = std::move(compiled_module)
    };

    compiled_functions = compiled_aggregate_functions;
    return Status::OK();
}

Status compile_functions(JIT& jit, std::vector<FunctionBasePtr>& functions,
                         std::vector<CompiledFunction>& results,
                         std::vector<std::shared_ptr<AggregateFunctionsSetToCompile>>& aggregate_functions) {
    JIT::CompiledModule compiled_module;

    std::vector<Status> compile_status(functions.size());

    auto status = jit.compile_module([&](llvm::Module& module) {
        size_t i = 0;
        for (auto& function : functions) {
            compile_status[i++] = compile_function(module, *function);
        }

        for (auto& function : aggregate_functions) {
            compile_create_aggregate_states_functions(module, function->functions, function->functions_description + "_create");
            compile_add_into_aggregate_states_functions(module, function->functions, function->functions_description + "_add");
            compile_add_into_aggregate_states_functions_single_place(module, function->functions, function->functions_description + "_add_single_place");
            compile_merge_aggregates_states(module, function->functions, function->functions_description + "_merge");
            compile_insert_aggregates_into_result_columns(module, function->functions, function->functions_description + "_insert");
        }
        return Status::OK();
    }, compiled_module);

    if (!status.ok())
        return status;

    for (size_t i = 0; i < functions.size(); i++) {
        if (compile_status[i].ok()) {
            auto& function = functions[i];
            auto compiled_function_ptr = reinterpret_cast<JITCompiledFunction>(compiled_module.function_name_to_symbol[function->get_name()]);
            results.emplace_back(CompiledFunction{compiled_function_ptr});
        } else
            results.emplace_back(CompiledFunction{nullptr});
    }

    for (size_t i = 0; i < aggregate_functions.size(); i++) {
        const auto& function = aggregate_functions[i];
        auto create_aggregate_states_function = reinterpret_cast<JITCreateAggregateStatesFunction>(compiled_module.function_name_to_symbol[function->functions_description + "_create"]);
        auto add_into_aggregate_states_function = reinterpret_cast<JITAddIntoAggregateStatesFunction>(compiled_module.function_name_to_symbol[function->functions_description + "_add"]);
        auto add_into_aggregate_states_function_single_place = reinterpret_cast<JITAddIntoAggregateStatesFunctionSinglePlace>(compiled_module.function_name_to_symbol[function->functions_description + "_add_single_place"]);
        auto merge_aggregate_states_function = reinterpret_cast<JITMergeAggregateStatesFunction>(compiled_module.function_name_to_symbol[function->functions_description + "_merge"]);
        auto insert_aggregate_states_function = reinterpret_cast<JITInsertAggregateStatesIntoColumnsFunction>(compiled_module.function_name_to_symbol[function->functions_description + "_insert"]);

        DCHECK(create_aggregate_states_function != nullptr);
        DCHECK(add_into_aggregate_states_function != nullptr);
        DCHECK(add_into_aggregate_states_function_single_place != nullptr);
        DCHECK(merge_aggregate_states_function != nullptr);
        DCHECK(insert_aggregate_states_function != nullptr);

        CompiledAggregateFunctions compiled_aggregate_functions {
            .create_aggregate_states_function = create_aggregate_states_function,
            .add_into_aggregate_states_function = add_into_aggregate_states_function,
            .add_into_aggregate_states_function_single_place = add_into_aggregate_states_function_single_place,
            .merge_aggregate_states_function = merge_aggregate_states_function,
            .insert_aggregates_into_columns_function = insert_aggregate_states_function,

            .functions_count = functions.size(),
            .compiled_module = std::move(compiled_module)
        };

        function->holder->compiled_aggregate_functions = std::move(compiled_aggregate_functions);
        function->holder->valid = true;
    }

    return Status::OK();
}



}
#endif
