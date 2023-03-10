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
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/udf/udf-internal.h
// and modified by Doris

#pragma once

#include <string.h>

#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "runtime/types.h"
#include "udf/udf.h"

namespace doris {

class RuntimeState;
struct ColumnPtrWrapper;
struct TypeDescriptor;

// This class actually implements the interface of FunctionContext. This is split to
// hide the details from the external header.
// Note: The actual user code does not include this file.
class FunctionContextImpl {
public:
    /// Create a FunctionContext for a UDA. Identical to the UDF version except for the
    /// intermediate type. Caller is responsible for deleting it.
    static std::unique_ptr<doris::FunctionContext> create_context(
            RuntimeState* state, const doris::TypeDescriptor& return_type,
            const std::vector<doris::TypeDescriptor>& arg_types);

    ~FunctionContextImpl() {}

    FunctionContextImpl();

    /// Returns a new FunctionContext with the same constant args, fragment-local state, and
    /// debug flag as this FunctionContext. The caller is responsible for calling delete on
    /// it.
    std::unique_ptr<doris::FunctionContext> clone();

    void set_constant_cols(const std::vector<std::shared_ptr<doris::ColumnPtrWrapper>>& cols);

    RuntimeState* state() { return _state; }

    std::string& string_result() { return _string_result; }

    const doris::TypeDescriptor& get_return_type() const;

    bool check_overflow_for_decimal() const { return _check_overflow_for_decimal; }

    bool set_check_overflow_for_decimal(bool check_overflow_for_decimal) {
        return _check_overflow_for_decimal = check_overflow_for_decimal;
    }

private:
    friend class doris::FunctionContext;

    // We use the query's runtime state to report errors and warnings. nullptr for test
    // contexts.
    RuntimeState* _state;

    // Empty if there's no error
    std::string _error_msg;

    // The number of warnings reported.
    int64_t _num_warnings;

    /// The function state accessed via FunctionContext::Get/SetFunctionState()
    std::shared_ptr<void> _thread_local_fn_state;
    std::shared_ptr<void> _fragment_local_fn_state;

    // Type descriptor for the return type of the function.
    doris::TypeDescriptor _return_type;

    // Type descriptors for each argument of the function.
    std::vector<doris::TypeDescriptor> _arg_types;

    std::vector<std::shared_ptr<doris::ColumnPtrWrapper>> _constant_cols;

    bool _check_overflow_for_decimal = false;

    std::string _string_result;
};

} // namespace doris
