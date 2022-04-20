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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Interpreters/JIT/compileFunction.h
// and modified by Doris

#ifdef DORIS_ENABLE_JIT
#pragma once

#include "vec/functions/function.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "jit/jit.h"

namespace doris::vectorized {

/** ColumnData structure to pass into compiled function.
  * data is raw column data.
  * null_data is null map column raw data.
  */
struct ColumnData {
    const char * data = nullptr;
    const char * null_data = nullptr;
};

/** Returns ColumnData for column.
  * If constant column is passed, LOGICAL_ERROR will be thrown.
  */
Status get_column_data(const IColumn * column, ColumnData& result);

using ColumnDataRowsSize = size_t;

using JITCompiledFunction = void (*)(ColumnDataRowsSize, ColumnData *);

struct CompiledFunction {
    JITCompiledFunction compiled_function;
};

/** Compile functions to native jit code using JIT instance.
  * It is client responsibility to match ColumnData arguments size with
  * function arguments size and additional ColumnData for result.
  */
Status compile_functions(JIT& jit, std::vector<FunctionBasePtr>& functions, std::vector<CompiledFunction>& results);

struct AggregateFunctionWithOffset {
    const IAggregateFunction * function;
    size_t aggregate_data_offset;
};

using JITCreateAggregateStatesFunction = void (*)(AggregateDataPtr);
using JITAddIntoAggregateStatesFunction = void (*)(ColumnDataRowsSize, ColumnData *, AggregateDataPtr *);
using JITAddIntoAggregateStatesFunctionSinglePlace = void (*)(ColumnDataRowsSize, ColumnData *, AggregateDataPtr);
using JITMergeAggregateStatesFunction = void (*)(AggregateDataPtr, AggregateDataPtr);
using JITInsertAggregateStatesIntoColumnsFunction = void (*)(ColumnDataRowsSize, ColumnData *, AggregateDataPtr *);

struct CompiledAggregateFunctions {
    JITCreateAggregateStatesFunction create_aggregate_states_function;
    JITAddIntoAggregateStatesFunction add_into_aggregate_states_function;
    JITAddIntoAggregateStatesFunctionSinglePlace add_into_aggregate_states_function_single_place;

    JITMergeAggregateStatesFunction merge_aggregate_states_function;
    JITInsertAggregateStatesIntoColumnsFunction insert_aggregates_into_columns_function;

    /// Count of functions that were compiled
    size_t functions_count;

    /// Compiled module. It is client responsibility to destroy it after functions are no longer required.
    JIT::CompiledModule compiled_module;
};

class CompiledAggregateFunctionsHolder {
public:
    CompiledAggregateFunctionsHolder() : valid(false) {}
    explicit CompiledAggregateFunctionsHolder(CompiledAggregateFunctions compiled_function_) : compiled_aggregate_functions(compiled_function_), valid(true) {}

    ~CompiledAggregateFunctionsHolder() {
    }

    CompiledAggregateFunctions compiled_aggregate_functions;

    bool valid;
};

struct AggregateFunctionsSetToCompile {
    std::vector<AggregateFunctionWithOffset> functions;
    std::string functions_description;
    std::shared_ptr<CompiledAggregateFunctionsHolder> holder;
};

/** Compile aggregate function to native jit code using CHJIT instance.
  *
  * JITCreateAggregateStatesFunction will initialize aggregate data ptr with initial aggregate states values.
  * JITAddIntoAggregateStatesFunction will update aggregate states for aggregate functions with specified ColumnData.
  * JITAddIntoAggregateStatesFunctionSinglePlace will update single aggregate state for aggregate functions with specified ColumnData.
  * JITMergeAggregateStatesFunction will merge aggregate states for aggregate functions.
  * JITInsertAggregateStatesIntoColumnsFunction will insert aggregate states for aggregate functions into result columns.
  */
Status compile_aggregate_functions(JIT & jit,
                                   const std::vector<AggregateFunctionWithOffset>& functions,
                                   const std::string& functions_dump_name,
                                   CompiledAggregateFunctions& compiled_functions);

Status compile_functions(JIT& jit,
                         std::vector<FunctionBasePtr>& functions,
                         std::vector<CompiledFunction>& results,
                         std::vector<std::shared_ptr<AggregateFunctionsSetToCompile>>& aggregate_functions);

}
#endif
