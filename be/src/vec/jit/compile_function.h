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

    JIT::CompiledModule compiled_module;
};

/** Compile function to native jit code using JIT instance.
  * It is client responsibility to match ColumnData arguments size with
  * function arguments size and additional ColumnData for result.
  */
Status compile_function(JIT& jit, const IFunctionBase& function, CompiledFunction& result);

}
#endif
