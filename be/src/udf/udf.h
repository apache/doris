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
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/udf/udf.h
// and modified by Doris

#pragma once

#include <string.h>

#include <cstdint>
#include <functional>
#include <iostream>
#include <memory>
#include <vector>
namespace doris {

class FunctionContextImpl;
struct ColumnPtrWrapper;
struct StringRef;
class BitmapValue;
class DecimalV2Value;
class DateTimeValue;
class CollectionValue;
struct TypeDescriptor;
// All input and output values will be one of the structs below. The struct is a simple
// object containing a boolean to store if the value is nullptr and the value itself. The
// value is unspecified if the nullptr boolean is set.
struct AnyVal;
struct StringRef;
struct DateTimeVal;

// The FunctionContext is passed to every UDF/UDA and is the interface for the UDF to the
// rest of the system. It contains APIs to examine the system state, report errors
// and manage memory.
class FunctionContext {
public:
    enum FunctionStateScope {
        /// Indicates that the function state for this FunctionContext's UDF is shared across
        /// the plan fragment (a query is divided into multiple plan fragments, each of which
        /// is responsible for a part of the query execution). Within the plan fragment, there
        /// may be multiple instances of the UDF executing concurrently with multiple
        /// FunctionContexts sharing this state, meaning that the state must be
        /// thread-safe. The Prepare() function for the UDF may be called with this scope
        /// concurrently on a single host if the UDF will be evaluated in multiple plan
        /// fragments on that host. In general, read-only state that doesn't need to be
        /// recomputed for every UDF call should be fragment-local.
        /// TODO: not yet implemented
        FRAGMENT_LOCAL,

        /// Indicates that the function state is local to the execution thread. This state
        /// does not need to be thread-safe. However, this state will be initialized (via the
        /// Prepare() function) once for every execution thread, so fragment-local state
        /// should be used when possible for better performance. In general, inexpensive
        /// shared state that is written to by the UDF (e.g. scratch space) should be
        /// thread-local.
        THREAD_LOCAL,
    };

    // Sets an error for this UDF. If this is called, this will trigger the
    // query to fail.
    // Note: when you set error for the UDFs used in Data Load, you should
    // ensure the function return value is null.
    void set_error(const char* error_msg);

    // Adds a warning that is returned to the user. This can include things like
    // overflow or other recoverable error conditions.
    // Warnings are capped at a maximum number. Returns true if the warning was
    // added and false if it was ignored due to the cap.
    bool add_warning(const char* warning_msg);

    // TODO: Do we need to add arbitrary key/value metadata. This would be plumbed
    // through the query. E.g. "select UDA(col, 'sample=true') from tbl".
    // const char* GetMetadata(const char*) const;

    // TODO: Add mechanism for UDAs to update stats similar to runtime profile counters

    // TODO: Add mechanism to query for table/column stats

    // Returns the underlying opaque implementation object. The UDF/UDA should not
    // use this. This is used internally.
    doris::FunctionContextImpl* impl() { return _impl.get(); }

    /// Methods for maintaining state across UDF/UDA function calls. SetFunctionState() can
    /// be used to store a pointer that can then be retrieved via GetFunctionState(). If
    /// GetFunctionState() is called when no pointer is set, it will return
    /// nullptr. SetFunctionState() does not take ownership of 'ptr'; it is up to the UDF/UDA
    /// to clean up any function state if necessary.
    void set_function_state(FunctionStateScope scope, std::shared_ptr<void> ptr);

    void* get_function_state(FunctionStateScope scope) const;

    // Returns the return type information of this function. For UDAs, this is the final
    // return type of the UDA (e.g., the type returned by the finalize function).
    const doris::TypeDescriptor& get_return_type() const;

    // Returns the number of arguments to this function (not including the FunctionContext*
    // argument).
    int get_num_args() const;

    // Returns the type information for the arg_idx-th argument (0-indexed, not including
    // the FunctionContext* argument). Returns nullptr if arg_idx is invalid.
    const doris::TypeDescriptor* get_arg_type(int arg_idx) const;

    // Returns true if the arg_idx-th input argument (0 indexed, not including the
    // FunctionContext* argument) is a constant (e.g. 5, "string", 1 + 1).
    bool is_col_constant(int arg_idx) const;

    // Returns a pointer to the value of the arg_idx-th input argument (0 indexed, not
    // including the FunctionContext* argument). Returns nullptr if the argument is not
    // constant. This function can be used to obtain user-specified constants in a UDF's
    // Init() or Close() functions.
    doris::ColumnPtrWrapper* get_constant_col(int arg_idx) const;

    // Creates a StringRef, which memory is available when this function context is used next time
    StringRef create_temp_string_val(int64_t len);

    ~FunctionContext() = default;

private:
    friend class doris::FunctionContextImpl;

    FunctionContext();

    // Disable copy ctor and assignment operator
    FunctionContext(const FunctionContext& other);

    FunctionContext& operator=(const FunctionContext& other);

    std::unique_ptr<doris::FunctionContextImpl> _impl; // Owned by this object.
};

//----------------------------------------------------------------------------
//-------------Implementation of the *Val structs ----------------------------
//----------------------------------------------------------------------------
struct AnyVal {
    bool is_null;

    AnyVal() : is_null(false) {}

    AnyVal(bool is_null) : is_null(is_null) {}
};

// This object has a compatible storage format with boost::ptime.
struct DateTimeVal : public AnyVal {
    // MySQL packet time
    int64_t packed_time;
    // Indicate which type of this value.
    int type;

    // NOTE: Type 3 is TIME_DATETIME in runtime/datetime_value.h
    DateTimeVal() : packed_time(0), type(3) {}

    static DateTimeVal null() {
        DateTimeVal result;
        result.is_null = true;
        return result;
    }

    bool operator==(const DateTimeVal& other) const {
        if (is_null && other.is_null) {
            return true;
        }

        if (is_null || other.is_null) {
            return false;
        }

        return packed_time == other.packed_time;
    }

    bool operator!=(const DateTimeVal& other) const { return !(*this == other); }
};

using doris::DateTimeVal;
using doris::FunctionContext;
} // namespace doris
