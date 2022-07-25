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
#include <iostream>
#include <vector>

// This is the only Doris header required to develop UDFs and UDAs. This header
// contains the types that need to be used and the FunctionContext object. The context
// object serves as the interface object between the UDF/UDA and the doris process.
namespace doris {
class FunctionContextImpl;
struct ColumnPtrWrapper;
struct StringValue;
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
struct BooleanVal;
struct TinyIntVal;
struct SmallIntVal;
struct IntVal;
struct BigIntVal;
struct StringVal;
struct DateTimeVal;
struct DateV2Val;
struct DecimalV2Val;
struct HllVal;
struct CollectionVal;

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
        TYPE_QUANTILE_STATE,
        TYPE_DATEV2,
        TYPE_DATETIMEV2,
        TYPE_TIMEV2,
        TYPE_DECIMAL32,
        TYPE_DECIMAL64,
        TYPE_DECIMAL128
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

    // Returns the user that is running the query. Returns nullptr if it is not
    // available.
    const char* user() const;

    // Returns the query_id for the current query.
    UniqueId query_id() const;

    // Sets an error for this UDF. If this is called, this will trigger the
    // query to fail.
    // Note: when you set error for the UDFs used in Data Load, you should
    // ensure the function return value is null.
    void set_error(const char* error_msg);

    // when you reused this FunctionContext, you maybe need clear the error status and message.
    void clear_error_msg();

    // Adds a warning that is returned to the user. This can include things like
    // overflow or other recoverable error conditions.
    // Warnings are capped at a maximum number. Returns true if the warning was
    // added and false if it was ignored due to the cap.
    bool add_warning(const char* warning_msg);

    // Returns true if there's been an error set.
    bool has_error() const;

    // Returns the current error message. Returns nullptr if there is no error.
    const char* error_msg() const;

    // Allocates memory for UDAs. All UDA allocations should use this if possible instead of
    // malloc/new. The UDA is responsible for calling Free() on all buffers returned
    // by Allocate().
    // If this Allocate causes the memory limit to be exceeded, the error will be set
    // in this object causing the query to fail.
    uint8_t* allocate(int byte_size);

    // Allocate and align memory for UDAs. All UDA allocations should use this if possible instead of
    // malloc/new. The UDA is responsible for calling Free() on all buffers returned
    // by Allocate().
    // If this Allocate causes the memory limit to be exceeded, the error will be set
    // in this object causing the query to fail.
    uint8_t* aligned_allocate(int alignment, int byte_size);

    // Reallocates 'ptr' to the new byte_size. If the currently underlying allocation
    // is big enough, the original ptr will be returned. If the allocation needs to
    // grow, a new allocation is made that is at least 'byte_size' and the contents
    // of 'ptr' will be copied into it.
    // This should be used for buffers that constantly get appended to.
    uint8_t* reallocate(uint8_t* ptr, int byte_size);

    // Frees a buffer returned from Allocate() or Reallocate()
    void free(uint8_t* buffer);

    // For allocations that cannot use the Allocate() API provided by this
    // object, TrackAllocation()/Free() can be used to just keep count of the
    // byte sizes. For each call to TrackAllocation(), the UDF/UDA must call
    // the corresponding Free().
    void track_allocation(int64_t byte_size);
    void free(int64_t byte_size);

    // TODO: Do we need to add arbitrary key/value metadata. This would be plumbed
    // through the query. E.g. "select UDA(col, 'sample=true') from tbl".
    // const char* GetMetadata(const char*) const;

    // TODO: Add mechanism for UDAs to update stats similar to runtime profile counters

    // TODO: Add mechanism to query for table/column stats

    // Returns the underlying opaque implementation object. The UDF/UDA should not
    // use this. This is used internally.
    doris::FunctionContextImpl* impl() { return _impl; }

    /// Methods for maintaining state across UDF/UDA function calls. SetFunctionState() can
    /// be used to store a pointer that can then be retrieved via GetFunctionState(). If
    /// GetFunctionState() is called when no pointer is set, it will return
    /// nullptr. SetFunctionState() does not take ownership of 'ptr'; it is up to the UDF/UDA
    /// to clean up any function state if necessary.
    void set_function_state(FunctionStateScope scope, void* ptr);
    void* get_function_state(FunctionStateScope scope) const;

    // Returns the return type information of this function. For UDAs, this is the final
    // return type of the UDA (e.g., the type returned by the finalize function).
    const TypeDesc& get_return_type() const;

    // Returns the intermediate type for UDAs, i.e., the one returned by
    // update and merge functions. Returns INVALID_TYPE for UDFs.
    const TypeDesc& get_intermediate_type() const;

    // Returns the number of arguments to this function (not including the FunctionContext*
    // argument).
    int get_num_args() const;

    // Returns _constant_args size
    int get_num_constant_args() const;

    // Returns the type information for the arg_idx-th argument (0-indexed, not including
    // the FunctionContext* argument). Returns nullptr if arg_idx is invalid.
    const TypeDesc* get_arg_type(int arg_idx) const;

    // Returns true if the arg_idx-th input argument (0 indexed, not including the
    // FunctionContext* argument) is a constant (e.g. 5, "string", 1 + 1).
    bool is_arg_constant(int arg_idx) const;

    bool is_col_constant(int arg_idx) const;

    // Returns a pointer to the value of the arg_idx-th input argument (0 indexed, not
    // including the FunctionContext* argument). Returns nullptr if the argument is not
    // constant. This function can be used to obtain user-specified constants in a UDF's
    // Init() or Close() functions.
    AnyVal* get_constant_arg(int arg_idx) const;

    doris::ColumnPtrWrapper* get_constant_col(int arg_idx) const;

    // Create a test FunctionContext object. The caller is responsible for calling delete
    // on it. This context has additional debugging validation enabled.
    static FunctionContext* create_test_context();

    ~FunctionContext();

private:
    friend class doris::FunctionContextImpl;
    FunctionContext();

    // Disable copy ctor and assignment operator
    FunctionContext(const FunctionContext& other);
    FunctionContext& operator=(const FunctionContext& other);

    doris::FunctionContextImpl* _impl; // Owned by this object.
};

//----------------------------------------------------------------------------
//------------------------------- UDFs ---------------------------------------
//----------------------------------------------------------------------------
// The UDF function must implement this function prototype. This is not
// a typedef as the actual UDF's signature varies from UDF to UDF.
//    typedef <*Val> Evaluate(FunctionContext* context, <const Val& arg>);
//
// The UDF must return one of the *Val structs. The UDF must accept a pointer
// to a FunctionContext object and then a const reference for each of the input arguments.
// nullptr input arguments will have nullptr passed in.
// Examples of valid Udf signatures are:
//  1) DoubleVal Example1(FunctionContext* context);
//  2) IntVal Example2(FunctionContext* context, const IntVal& a1, const DoubleVal& a2);
//
// UDFs can be variadic. The variable arguments must all come at the end and must be
// the same type. A example signature is:
//  StringVal Concat(FunctionContext* context, const StringVal& separator,
//    int num_var_args, const StringVal* args);
// In this case args[0] is the first variable argument and args[num_var_args - 1] is
// the last.
//
// The UDF should not maintain any state across calls since there is no guarantee
// on how the execution is multithreaded or distributed. Conceptually, the UDF
// should only read the input arguments and return the result, using only the
// FunctionContext as an external object.
//
// Memory Management: the UDF can assume that memory from input arguments will have
// the same lifetime as results for the UDF. In other words, the UDF can return
// memory from input arguments without making copies. For example, a function like
// substring will not need to allocate and copy the smaller string. For cases where
// the UDF needs a buffer, it should use the StringValue(FunctionContext, len) c'tor.
//
// The UDF can optionally specify a Prepare function. The prepare function is called
// once before any calls to the Udf to evaluate values. This is the appropriate time for
// the Udf to validate versions and things like that.
// If there is an error, this function should call FunctionContext::set_error()/
// FunctionContext::add_warning().
typedef void (*UdfPrepareFn)(FunctionContext* context);

/// --- Prepare / Close Functions ---
/// ---------------------------------
/// The UDF can optionally include a prepare function, specified in the "CREATE FUNCTION"
/// statement using "prepare_fn=<prepare function symbol>". The prepare function is called
/// before any calls to the UDF to evaluate values. This is the appropriate time for the
/// UDF to initialize any shared data structures, validate versions, etc. If there is an
/// error, this function should call FunctionContext::SetError()/
/// FunctionContext::AddWarning().
//
/// The prepare function is called multiple times with different FunctionStateScopes. It
/// will be called once per fragment with 'scope' set to FRAGMENT_LOCAL, and once per
/// execution thread with 'scope' set to THREAD_LOCAL.
typedef void (*UdfPrepare)(FunctionContext* context, FunctionContext::FunctionStateScope scope);

/// The UDF can also optionally include a close function, specified in the "CREATE
/// FUNCTION" statement using "close_fn=<close function symbol>". The close function is
/// called after all calls to the UDF have completed. This is the appropriate time for the
/// UDF to deallocate any shared data structures that are not needed to maintain the
/// results. If there is an error, this function should call FunctionContext::SetError()/
/// FunctionContext::AddWarning().
//
/// The close function is called multiple times with different FunctionStateScopes. It
/// will be called once per fragment with 'scope' set to FRAGMENT_LOCAL, and once per
/// execution thread with 'scope' set to THREAD_LOCAL.
typedef void (*UdfClose)(FunctionContext* context, FunctionContext::FunctionStateScope scope);

//----------------------------------------------------------------------------
//------------------------------- UDAs ---------------------------------------
//----------------------------------------------------------------------------
// The UDA execution is broken up into a few steps. The general calling pattern
// is one of these:
//  1) Init(), Evaluate() (repeatedly), Serialize()
//  2) Init(), Merge() (repeatedly), Serialize()
//  3) Init(), Finalize()
// The UDA is registered with three types: the result type, the input type and
// the intermediate type.
//
// If the UDA needs a fixed byte width intermediate buffer, the type should be
// TYPE_FIXED_BUFFER and Doris will allocate the buffer. If the UDA needs an unknown
// sized buffer, it should use TYPE_STRING and allocate it from the FunctionContext
// manually.
// For UDAs that need a complex data structure as the intermediate state, the
// intermediate type should be string and the UDA can cast the ptr to the structure
// it is using.
//
// Memory Management: For allocations that are not returned to Doris, the UDA
// should use the FunctionContext::Allocate()/Free() methods. For StringVal allocations
// returned to Doris (e.g. UdaSerialize()), the UDA should allocate the result
// via StringVal(FunctionContext*, int) ctor and Doris will automatically handle
// freeing it.
//
// For clarity in documenting the UDA interface, the various types will be typedefed
// here. The actual execution resolves all the types at runtime and none of these types
// should actually be used.
typedef AnyVal InputType;
typedef AnyVal InputType2;
typedef AnyVal ResultType;
typedef AnyVal IntermediateType;

// UdaInit is called once for each aggregate group before calls to any of the
// other functions below.
typedef void (*UdaInit)(FunctionContext* context, IntermediateType* result);

// This is called for each input value. The UDA should update result based on the
// input value. The update function can take any number of input arguments. Here
// are some examples:
typedef void (*UdaUpdate)(FunctionContext* context, const InputType& input,
                          IntermediateType* result);
typedef void (*UdaUpdate2)(FunctionContext* context, const InputType& input,
                           const InputType2& input2, IntermediateType* result);

// Merge an intermediate result 'src' into 'dst'.
typedef void (*UdaMerge)(FunctionContext* context, const IntermediateType& src,
                         IntermediateType* dst);

// Serialize the intermediate type. The serialized data is then sent across the
// wire. This is not called unless the intermediate type is String.
// No additional functions will be called with this FunctionContext object and the
// UDA should do final clean (e.g. Free()) here.
typedef const IntermediateType (*UdaSerialize)(FunctionContext* context,
                                               const IntermediateType& type);

// Called once at the end to return the final value for this UDA.
// No additional functions will be called with this FunctionContext object and the
// UDA should do final clean (e.g. Free()) here.
typedef ResultType (*UdaFinalize)(FunctionContext* context, const IntermediateType& v);

//----------------------------------------------------------------------------
//-------------Implementation of the *Val structs ----------------------------
//----------------------------------------------------------------------------
struct AnyVal {
    bool is_null;
    AnyVal() : is_null(false) {}
    AnyVal(bool is_null) : is_null(is_null) {}
};

struct BooleanVal : public AnyVal {
    bool val;

    BooleanVal() : val(false) {}
    BooleanVal(bool val) : val(val) {}

    static BooleanVal null() {
        BooleanVal result;
        result.is_null = true;
        return result;
    }

    bool operator==(const BooleanVal& other) const {
        if (is_null && other.is_null) {
            return true;
        }

        if (is_null || other.is_null) {
            return false;
        }

        return val == other.val;
    }
    bool operator!=(const BooleanVal& other) const { return !(*this == other); }
};

struct TinyIntVal : public AnyVal {
    int8_t val;

    TinyIntVal() : val(0) {}
    TinyIntVal(int8_t val) : val(val) {}

    static TinyIntVal null() {
        TinyIntVal result;
        result.is_null = true;
        return result;
    }

    bool operator==(const TinyIntVal& other) const {
        if (is_null && other.is_null) {
            return true;
        }

        if (is_null || other.is_null) {
            return false;
        }

        return val == other.val;
    }
    bool operator!=(const TinyIntVal& other) const { return !(*this == other); }
};

struct SmallIntVal : public AnyVal {
    int16_t val;

    SmallIntVal() : val(0) {}
    SmallIntVal(int16_t val) : val(val) {}

    static SmallIntVal null() {
        SmallIntVal result;
        result.is_null = true;
        return result;
    }

    bool operator==(const SmallIntVal& other) const {
        if (is_null && other.is_null) {
            return true;
        }

        if (is_null || other.is_null) {
            return false;
        }

        return val == other.val;
    }
    bool operator!=(const SmallIntVal& other) const { return !(*this == other); }
};

struct IntVal : public AnyVal {
    int32_t val;

    IntVal() : val(0) {}
    IntVal(int32_t val) : val(val) {}

    static IntVal null() {
        IntVal result;
        result.is_null = true;
        return result;
    }

    bool operator==(const IntVal& other) const {
        if (is_null && other.is_null) {
            return true;
        }

        if (is_null || other.is_null) {
            return false;
        }

        return val == other.val;
    }
    bool operator!=(const IntVal& other) const { return !(*this == other); }
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

struct Decimal32Val : public AnyVal {
    int32_t val;

    Decimal32Val() : val(0) {}
    Decimal32Val(int32_t val) : val(val) {}

    static Decimal32Val null() {
        Decimal32Val result;
        result.is_null = true;
        return result;
    }

    bool operator==(const Decimal32Val& other) const {
        if (is_null && other.is_null) {
            return true;
        }

        if (is_null || other.is_null) {
            return false;
        }

        return val == other.val;
    }
    bool operator!=(const Decimal32Val& other) const { return !(*this == other); }
};

struct Decimal64Val : public AnyVal {
    int64_t val;

    Decimal64Val() : val(0) {}
    Decimal64Val(int64_t val) : val(val) {}

    static Decimal64Val null() {
        Decimal64Val result;
        result.is_null = true;
        return result;
    }

    bool operator==(const Decimal64Val& other) const {
        if (is_null && other.is_null) {
            return true;
        }

        if (is_null || other.is_null) {
            return false;
        }

        return val == other.val;
    }
    bool operator!=(const Decimal64Val& other) const { return !(*this == other); }
};

struct Decimal128Val : public AnyVal {
    __int128 val;

    Decimal128Val() : val(0) {}

    Decimal128Val(__int128 large_value) : val(large_value) {}

    static Decimal128Val null() {
        Decimal128Val result;
        result.is_null = true;
        return result;
    }

    bool operator==(const Decimal128Val& other) const {
        if (is_null && other.is_null) {
            return true;
        }

        if (is_null || other.is_null) {
            return false;
        }

        return val == other.val;
    }
    bool operator!=(const Decimal128Val& other) const { return !(*this == other); }
};

struct FloatVal : public AnyVal {
    float val;

    FloatVal() : val(0.0) {}
    FloatVal(float val) : val(val) {}

    static FloatVal null() {
        FloatVal result;
        result.is_null = true;
        return result;
    }

    bool operator==(const FloatVal& other) const {
        return is_null == other.is_null && val == other.val;
    }
    bool operator!=(const FloatVal& other) const { return !(*this == other); }
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

struct DateV2Val : public AnyVal {
    uint32_t datev2_value;

    DateV2Val() {}
    DateV2Val(uint32_t val) : datev2_value(val) {}

    static DateV2Val null() {
        DateV2Val result;
        result.is_null = true;
        return result;
    }

    bool operator==(const DateV2Val& other) const {
        if (is_null && other.is_null) {
            return true;
        }

        if (is_null || other.is_null) {
            return false;
        }

        return datev2_value == other.datev2_value;
    }
    bool operator!=(const DateV2Val& other) const { return !(*this == other); }
};

struct DateTimeV2Val : public AnyVal {
    uint64_t datetimev2_value;

    DateTimeV2Val() {}
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

    static StringVal null() {
        StringVal sv;
        sv.is_null = true;
        return sv;
    }

    // Creates a StringVal, allocating a new buffer with 'len'. This should
    // be used to return StringVal objects in UDF/UDAs that need to allocate new
    // string memory.
    StringVal(FunctionContext* context, int64_t len);

    // Creates a StringVal, which memory is available when this function context is used next time
    static StringVal create_temp_string_val(FunctionContext* ctx, int64_t len);

    bool resize(FunctionContext* context, int64_t len);

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

    /// Will create a new StringVal with the given dimension and copy the data from the
    /// parameters. In case of an error will return a nullptr string and set an error on the
    /// function context.
    static StringVal copy_from(FunctionContext* ctx, const uint8_t* buf, int64_t len);

    /// Append the passed buffer to this StringVal. Reallocate memory to fit the buffer. If
    /// the memory allocation becomes too large, will set an error on FunctionContext and
    /// return a nullptr string.
    void append(FunctionContext* ctx, const uint8_t* buf, int64_t len);
    void append(FunctionContext* ctx, const uint8_t* buf, int64_t len, const uint8_t* buf2,
                int64_t buf2_len);
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

struct LargeIntVal : public AnyVal {
    __int128 val;

    LargeIntVal() : val(0) {}

    LargeIntVal(__int128 large_value) : val(large_value) {}

    static LargeIntVal null() {
        LargeIntVal result;
        result.is_null = true;
        return result;
    }

    bool operator==(const LargeIntVal& other) const {
        if (is_null && other.is_null) {
            return true;
        }

        if (is_null || other.is_null) {
            return false;
        }

        return val == other.val;
    }
    bool operator!=(const LargeIntVal& other) const { return !(*this == other); }
};

// todo(kks): keep HllVal struct only for backward compatibility, we should remove it
//            when doris 0.12 release
struct HllVal : public StringVal {
    HllVal() : StringVal() {}

    void init(FunctionContext* ctx);

    void agg_parse_and_cal(FunctionContext* ctx, const HllVal& other);

    void agg_merge(const HllVal& other);
};

struct CollectionVal : public AnyVal {
    void* data;
    uint64_t length;
    // item has no null value if has_null is false.
    // item ```may``` has null value if has_null is true.
    bool has_null;
    // null bitmap
    bool* null_signs;

    CollectionVal() = default;

    CollectionVal(void* data, uint64_t length, bool has_null, bool* null_signs)
            : data(data), length(length), has_null(has_null), null_signs(null_signs) {};

    static CollectionVal null() {
        CollectionVal val;
        val.is_null = true;
        return val;
    }
};
typedef uint8_t* BufferVal;
} // namespace doris_udf

using doris_udf::BooleanVal;
using doris_udf::TinyIntVal;
using doris_udf::SmallIntVal;
using doris_udf::IntVal;
using doris_udf::BigIntVal;
using doris_udf::LargeIntVal;
using doris_udf::FloatVal;
using doris_udf::DoubleVal;
using doris_udf::StringVal;
using doris_udf::DecimalV2Val;
using doris_udf::DateTimeVal;
using doris_udf::HllVal;
using doris_udf::FunctionContext;
using doris_udf::CollectionVal;
using doris_udf::Decimal32Val;
using doris_udf::Decimal64Val;
using doris_udf::Decimal128Val;
