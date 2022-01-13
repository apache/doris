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

#ifndef DORIS_BE_UDF_UDA_TEST_HARNESS_H
#define DORIS_BE_UDF_UDA_TEST_HARNESS_H

#include <sstream>
#include <string>
#include <vector>

#include "udf/udf.h"
#include "udf/udf_debug.h"

namespace doris_udf {

enum UdaExecutionMode {
    ALL = 0,
    SINGLE_NODE = 1,
    ONE_LEVEL = 2,
    TWO_LEVEL = 3,
};

template <typename RESULT, typename INTERMEDIATE>
class UdaTestHarnessBase {
public:
    typedef void (*InitFn)(FunctionContext* context, INTERMEDIATE* result);

    typedef void (*MergeFn)(FunctionContext* context, const INTERMEDIATE& src, INTERMEDIATE* dst);

    typedef const INTERMEDIATE (*SerializeFn)(FunctionContext* context, const INTERMEDIATE& type);

    typedef RESULT (*FinalizeFn)(FunctionContext* context, const INTERMEDIATE& value);

    // UDA test harness allows for custom comparator to validate results. UDAs
    // can specify a custom comparator to, for example, tolerate numerical imprecision.
    // Returns true if x and y should be treated as equal.
    typedef bool (*ResultComparator)(const RESULT& x, const RESULT& y);

    void set_result_comparator(ResultComparator fn) { _result_comparator_fn = fn; }

    // This must be called if the INTERMEDIATE is TYPE_FIXED_BUFFER
    void set_intermediate_size(int byte_size) { _fixed_buffer_byte_size = byte_size; }

    // Returns the failure string if any.
    const std::string& get_error_msg() const { return _error_msg; }

protected:
    UdaTestHarnessBase(InitFn init_fn, MergeFn merge_fn, SerializeFn serialize_fn,
                       FinalizeFn finalize_fn)
            : _init_fn(init_fn),
              _merge_fn(merge_fn),
              _serialize_fn(serialize_fn),
              _finalize_fn(finalize_fn),
              _result_comparator_fn(nullptr),
              _num_input_values(0) {}

    // Runs the UDA in all the modes, validating the result is 'expected' each time.
    bool execute(const RESULT& expected, UdaExecutionMode mode);

    // Returns false if there is an error set in the context.
    bool check_context(FunctionContext* context);

    // Verifies x == y, using the custom comparator if set.
    bool check_result(const RESULT& x, const RESULT& y);

    // Runs the UDA on a single node. The entire execution happens in 1 context.
    // The UDA does a update on all the input values and then a finalize.
    RESULT execute_single_node();

    // Runs the UDA, simulating a single level aggregation. The values are processed
    // on num_nodes + 1 contexts. There are num_nodes that do update and serialize.
    // There is a final context that does merge and finalize.
    RESULT execute_one_level(int num_nodes);

    // Runs the UDA, simulating a two level aggregation with num1 in the first level and
    // num2 in the second. The values are processed in num1 + num2 contexts.
    RESULT execute_two_level(int num1, int num2);

    virtual void update(int idx, FunctionContext* context, INTERMEDIATE* dst) = 0;

private:
    // UDA functions
    InitFn _init_fn;
    MergeFn _merge_fn;
    SerializeFn _serialize_fn;
    FinalizeFn _finalize_fn;

    // Customer comparator, nullptr if default == should be used.
    ResultComparator _result_comparator_fn;

    // Set during execute() by subclass
    int _num_input_values;

    // Buffer len for intermediate results if the type is TYPE_FIXED_BUFFER
    int _fixed_buffer_byte_size;

    // Error message if anything went wrong during the execution.
    std::string _error_msg;
};

template <typename RESULT, typename INTERMEDIATE, typename INPUT>
class UdaTestHarness : public UdaTestHarnessBase<RESULT, INTERMEDIATE> {
public:
    typedef void (*UpdateFn)(FunctionContext* context, const INPUT& input, INTERMEDIATE* result);

    typedef UdaTestHarnessBase<RESULT, INTERMEDIATE> BaseClass;

    UdaTestHarness(typename BaseClass::InitFn init_fn, UpdateFn update_fn,
                   typename BaseClass::MergeFn merge_fn,
                   typename BaseClass::SerializeFn serialize_fn,
                   typename BaseClass::FinalizeFn finalize_fn)
            : BaseClass(init_fn, merge_fn, serialize_fn, finalize_fn), _update_fn(update_fn) {}

    bool execute(const std::vector<INPUT>& values, const RESULT& expected) {
        return execute(values, expected, ALL);
    }
    // Runs the UDA in all the modes, validating the result is 'expected' each time.
    bool execute(const std::vector<INPUT>& values, const RESULT& expected, UdaExecutionMode mode);

    // Runs the UDA in all the modes, validating the result is 'expected' each time.
    // T needs to be compatible (i.e. castable to) with INPUT
    template <typename T>
    bool execute(const std::vector<T>& values, const RESULT& expected) {
        return execute(values, expected, ALL);
    }
    template <typename T>
    bool execute(const std::vector<T>& values, const RESULT& expected, UdaExecutionMode mode) {
        _input.resize(values.size());
        BaseClass::_num_input_values = _input.size();

        for (int i = 0; i < values.size(); ++i) {
            _input[i] = &values[i];
        }

        return BaseClass::execute(expected, mode);
    }

protected:
    virtual void update(int idx, FunctionContext* context, INTERMEDIATE* dst);

private:
    UpdateFn _update_fn;
    // Set during execute()
    std::vector<const INPUT*> _input;
};

template <typename RESULT, typename INTERMEDIATE, typename INPUT1, typename INPUT2>
class UdaTestHarness2 : public UdaTestHarnessBase<RESULT, INTERMEDIATE> {
public:
    typedef void (*UpdateFn)(FunctionContext* context, const INPUT1& input1, const INPUT2& input2,
                             INTERMEDIATE* result);

    typedef UdaTestHarnessBase<RESULT, INTERMEDIATE> BaseClass;

    ~UdaTestHarness2() {}
    UdaTestHarness2(typename BaseClass::InitFn init_fn, UpdateFn update_fn,
                    typename BaseClass::MergeFn merge_fn,
                    typename BaseClass::SerializeFn serialize_fn,
                    typename BaseClass::FinalizeFn finalize_fn)
            : BaseClass(init_fn, merge_fn, serialize_fn, finalize_fn), _update_fn(update_fn) {}

    // Runs the UDA in all the modes, validating the result is 'expected' each time.
    bool execute(const std::vector<INPUT1>& values1, const std::vector<INPUT2>& values2,
                 const RESULT& expected, UdaExecutionMode mode);

    bool execute(const std::vector<INPUT1>& values1, const std::vector<INPUT2>& values2,
                 const RESULT& expected) {
        return execute(values1, values2, expected, ALL);
    }

protected:
    virtual void update(int idx, FunctionContext* context, INTERMEDIATE* dst);

private:
    UpdateFn _update_fn;
    const std::vector<INPUT1>* _input1;
    const std::vector<INPUT2>* _input2;
};

template <typename RESULT, typename INTERMEDIATE, typename INPUT1, typename INPUT2, typename INPUT3>
class UdaTestHarness3 : public UdaTestHarnessBase<RESULT, INTERMEDIATE> {
public:
    typedef void (*UpdateFn)(FunctionContext* context, const INPUT1& input1, const INPUT2& input2,
                             const INPUT3& input3, INTERMEDIATE* result);

    typedef UdaTestHarnessBase<RESULT, INTERMEDIATE> BaseClass;

    ~UdaTestHarness3() {}
    UdaTestHarness3(typename BaseClass::InitFn init_fn, UpdateFn update_fn,
                    typename BaseClass::MergeFn merge_fn,
                    typename BaseClass::SerializeFn serialize_fn,
                    typename BaseClass::FinalizeFn finalize_fn)
            : BaseClass(init_fn, merge_fn, serialize_fn, finalize_fn), _update_fn(update_fn) {}

    // Runs the UDA in all the modes, validating the result is 'expected' each time.
    bool execute(const std::vector<INPUT1>& values1, const std::vector<INPUT2>& values2,
                 const std::vector<INPUT3>& values3, const RESULT& expected) {
        return execute(values1, values2, values3, expected, ALL);
    }
    // Runs the UDA in all the modes, validating the result is 'expected' each time.
    bool execute(const std::vector<INPUT1>& values1, const std::vector<INPUT2>& values2,
                 const std::vector<INPUT3>& values3, const RESULT& expected, UdaExecutionMode mode);

protected:
    virtual void update(int idx, FunctionContext* context, INTERMEDIATE* dst);

private:
    UpdateFn _update_fn;
    const std::vector<INPUT1>* _input1;
    const std::vector<INPUT2>* _input2;
    const std::vector<INPUT3>* _input3;
};

template <typename RESULT, typename INTERMEDIATE, typename INPUT1, typename INPUT2, typename INPUT3,
          typename INPUT4>
class UdaTestHarness4 : public UdaTestHarnessBase<RESULT, INTERMEDIATE> {
public:
    typedef void (*UpdateFn)(FunctionContext* context, const INPUT1& input1, const INPUT2& input2,
                             const INPUT3& input3, const INPUT4& input4, INTERMEDIATE* result);

    typedef UdaTestHarnessBase<RESULT, INTERMEDIATE> BaseClass;

    ~UdaTestHarness4() {}
    UdaTestHarness4(typename BaseClass::InitFn init_fn, UpdateFn update_fn,
                    typename BaseClass::MergeFn merge_fn,
                    typename BaseClass::SerializeFn serialize_fn,
                    typename BaseClass::FinalizeFn finalize_fn)
            : BaseClass(init_fn, merge_fn, serialize_fn, finalize_fn), _update_fn(update_fn) {}

    // Runs the UDA in all the modes, validating the result is 'expected' each time.
    bool execute(const std::vector<INPUT1>& values1, const std::vector<INPUT2>& values2,
                 const std::vector<INPUT3>& values3, const std::vector<INPUT4>& values4,
                 const RESULT& expected) {
        return execute(values1, values2, values3, values4, expected, ALL);
    }
    // Runs the UDA in all the modes, validating the result is 'expected' each time.
    bool execute(const std::vector<INPUT1>& values1, const std::vector<INPUT2>& values2,
                 const std::vector<INPUT3>& values3, const std::vector<INPUT4>& values4,
                 const RESULT& expected, UdaExecutionMode mode);

protected:
    virtual void update(int idx, FunctionContext* context, INTERMEDIATE* dst);

private:
    UpdateFn _update_fn;
    const std::vector<INPUT1>* _input1;
    const std::vector<INPUT2>* _input2;
    const std::vector<INPUT3>* _input3;
    const std::vector<INPUT4>* _input4;
};

} // namespace doris_udf

#include "udf/uda_test_harness_impl.hpp"

#endif
