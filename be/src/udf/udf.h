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

// This is the only Doris header required to develop UDFs and UDAs. This header
// contains the types that need to be used and the FunctionContext object. The context
// object serves as the interface object between the UDF/UDA and the doris process.
namespace doris {
class FunctionContextImpl;
struct ColumnPtrWrapper;
struct StringRef;
class BitmapValue;
class DecimalV2Value;
class DateTimeValue;
class CollectionValue;
} // namespace doris

namespace doris_udf {

// All input and output values will be one of the structs below. The struct is a simple
// object containing a boolean to store if the value is nullptr and the value itself. The
// value is unspecified if the nullptr boolean is set.
struct AnyVal;
struct StringVal;
struct DateTimeVal;
struct DecimalV2Val;

// The FunctionContext is passed to every UDF/UDA and is the interface for the UDF to the
// rest of the system. It contains APIs to examine the system state, report errors
// and manage memory.
class FunctionContext {
public:
    enum DorisVersion {
        V2_0,
    };

    enum Type {
        INVALID_TYPE = 0,
        TYPE_NULL,
        TYPE_BOOLEAN,
        TYPE_TINYINT,
        TYPE_SMALLINT,
        TYPE_INT,
        TYPE_BIGINT,
        TYPE_LARGEINT,
        TYPE_FLOAT,
        TYPE_DOUBLE,
        TYPE_DECIMAL [[deprecated]],
        TYPE_DATE,
        TYPE_DATETIME,
        TYPE_CHAR,
        TYPE_VARCHAR,
        TYPE_HLL,
        TYPE_STRING,
        TYPE_FIXED_BUFFER,
        TYPE_DECIMALV2,
        TYPE_OBJECT,
        TYPE_ARRAY,
        TYPE_MAP,
        TYPE_STRUCT,
        TYPE_QUANTILE_STATE,
        TYPE_DATEV2,
        TYPE_DATETIMEV2,
        TYPE_TIMEV2,
        TYPE_DECIMAL32,
        TYPE_DECIMAL64,
        TYPE_DECIMAL128I,
        TYPE_JSONB,
        TYPE_VARIANT
    };

    struct TypeDesc {
        Type type;

        /// Only valid if type == TYPE_DECIMAL
        int precision;
        int scale;

        /// Only valid if type == TYPE_FIXED_BUFFER || type == TYPE_VARCHAR
        int len;

        // only valid if type == TYPE_ARRAY
        std::vector<TypeDesc> children;
    };

    struct UniqueId {
        int64_t hi;
        int64_t lo;
    };

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

    // Returns the version of Doris that's currently running.
    DorisVersion version() const;

    // Returns the query_id for the current query.
    UniqueId query_id() const;

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
    const TypeDesc& get_return_type() const;

    // Returns the number of arguments to this function (not including the FunctionContext*
    // argument).
    int get_num_args() const;

    // Returns the type information for the arg_idx-th argument (0-indexed, not including
    // the FunctionContext* argument). Returns nullptr if arg_idx is invalid.
    const TypeDesc* get_arg_type(int arg_idx) const;

    // Returns true if the arg_idx-th input argument (0 indexed, not including the
    // FunctionContext* argument) is a constant (e.g. 5, "string", 1 + 1).
    bool is_col_constant(int arg_idx) const;

    // Returns a pointer to the value of the arg_idx-th input argument (0 indexed, not
    // including the FunctionContext* argument). Returns nullptr if the argument is not
    // constant. This function can be used to obtain user-specified constants in a UDF's
    // Init() or Close() functions.
    doris::ColumnPtrWrapper* get_constant_col(int arg_idx) const;

    // Creates a StringVal, which memory is available when this function context is used next time
    StringVal create_temp_string_val(int64_t len);

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

struct BigIntVal : public AnyVal {
    int64_t val;

    BigIntVal() : val(0) {}

    BigIntVal(int64_t val) : val(val) {}

    static BigIntVal null() {
        BigIntVal result;
        result.is_null = true;
        return result;
    }

    bool operator==(const BigIntVal& other) const {
        if (is_null && other.is_null) {
            return true;
        }

        if (is_null || other.is_null) {
            return false;
        }

        return val == other.val;
    }

    bool operator!=(const BigIntVal& other) const { return !(*this == other); }
};

struct DoubleVal : public AnyVal {
    double val;

    DoubleVal() : val(0.0) {}

    DoubleVal(double val) : val(val) {}

    static DoubleVal null() {
        DoubleVal result;
        result.is_null = true;
        return result;
    }

    bool operator==(const DoubleVal& other) const {
        if (is_null && other.is_null) {
            return true;
        }

        if (is_null || other.is_null) {
            return false;
        }

        return val == other.val;
    }

    bool operator!=(const DoubleVal& other) const { return !(*this == other); }
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

struct DateTimeV2Val : public AnyVal {
    uint64_t datetimev2_value;

    DateTimeV2Val() : datetimev2_value(0) {}

    DateTimeV2Val(uint64_t val) : datetimev2_value(val) {}

    static DateTimeV2Val null() {
        DateTimeV2Val result;
        result.is_null = true;
        return result;
    }

    bool operator==(const DateTimeV2Val& other) const {
        if (is_null && other.is_null) {
            return true;
        }

        if (is_null || other.is_null) {
            return false;
        }

        return datetimev2_value == other.datetimev2_value;
    }

    bool operator!=(const DateTimeV2Val& other) const { return !(*this == other); }
};

// TODO: need to set explicit align?
// Note: there is a difference between a nullptr string (is_null == true) and an
// empty string (len == 0).
struct StringVal : public AnyVal {
    static const int MAX_LENGTH = (1 << 30);

    int64_t len;
    uint8_t* ptr;

    // Construct a StringVal from ptr/len. Note: this does not make a copy of ptr
    // so the buffer must exist as long as this StringVal does.
    StringVal() : len(0), ptr(nullptr) {}

    // Construct a StringVal from ptr/len. Note: this does not make a copy of ptr
    // so the buffer must exist as long as this StringVal does.
    StringVal(uint8_t* ptr, int64_t len) : len(len), ptr(ptr) {}

    // Construct a StringVal from nullptr-terminated c-string. Note: this does not make a
    // copy of ptr so the underlying string must exist as long as this StringVal does.
    StringVal(const char* ptr) : len(strlen(ptr)), ptr((uint8_t*)ptr) {}

    StringVal(const char* ptr, int64_t len) : len(len), ptr((uint8_t*)ptr) {}

    static StringVal null() {
        StringVal sv;
        sv.is_null = true;
        return sv;
    }

    bool operator==(const StringVal& other) const {
        if (is_null != other.is_null) {
            return false;
        }

        if (is_null) {
            return true;
        }

        if (len != other.len) {
            return false;
        }

        return len == 0 || ptr == other.ptr || memcmp(ptr, other.ptr, len) == 0;
    }

    bool operator!=(const StringVal& other) const { return !(*this == other); }

    std::string to_string() const { return std::string((char*)ptr, len); }
};

std::ostream& operator<<(std::ostream& os, const StringVal& string_val);

struct DecimalV2Val : public AnyVal {
    __int128 val;

    // Default value is zero
    DecimalV2Val() : val(0) {}

    const __int128& value() const { return val; }

    DecimalV2Val(__int128 value) : val(value) {}

    static DecimalV2Val null() {
        DecimalV2Val result;
        result.is_null = true;
        return result;
    }

    void set_to_zero() { val = 0; }

    void set_to_abs_value() {
        if (val < 0) val = -val;
    }

    bool operator==(const DecimalV2Val& other) const {
        if (is_null && other.is_null) {
            return true;
        }

        if (is_null || other.is_null) {
            return false;
        }

        return val == other.val;
    }

    bool operator!=(const DecimalV2Val& other) const { return !(*this == other); }
};

using doris_udf::BigIntVal;
using doris_udf::DoubleVal;
using doris_udf::StringVal;
using doris_udf::DecimalV2Val;
using doris_udf::DateTimeVal;
using doris_udf::FunctionContext;
} // namespace doris_udf
