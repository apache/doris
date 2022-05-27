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

#include <gtest/gtest.h>

#include <iostream>

#include "common/logging.h"
#include "udf/uda_test_harness.h"
#include "util/logging.h"

namespace doris_udf {

//-------------------------------- Count ------------------------------------
// Example of implementing Count(int_col).
// The input type is: int
// The intermediate type is bigint
// the return type is bigint
void CountInit(FunctionContext* context, BigIntVal* val) {
    val->is_null = false;
    val->val = 0;
}

void CountUpdate(FunctionContext* context, const IntVal& input, BigIntVal* val) {
    // BigIntVal is the same ptr as what was passed to CountInit
    if (input.is_null) {
        return;
    }

    ++val->val;
}

void CountMerge(FunctionContext* context, const BigIntVal& src, BigIntVal* dst) {
    dst->val += src.val;
}

BigIntVal CountFinalize(FunctionContext* context, const BigIntVal& val) {
    return val;
}

//-------------------------------- Count(...) ------------------------------------
// Example of implementing Count(...)
// The input type is: multiple ints
// The intermediate type is bigint
// the return type is bigint
void Count2Update(FunctionContext* context, const IntVal& input1, const IntVal& input2,
                  BigIntVal* val) {
    val->val += (!input1.is_null + !input2.is_null);
}
void Count3Update(FunctionContext* context, const IntVal& input1, const IntVal& input2,
                  const IntVal& input3, BigIntVal* val) {
    val->val += (!input1.is_null + !input2.is_null + !input3.is_null);
}
void Count4Update(FunctionContext* context, const IntVal& input1, const IntVal& input2,
                  const IntVal& input3, const IntVal& input4, BigIntVal* val) {
    val->val += (!input1.is_null + !input2.is_null + !input3.is_null + !input4.is_null);
}

//-------------------------------- Min(String) ------------------------------------
// Example of implementing MIN for strings.
// The input type is: STRING
// The intermediate type is BufferVal
// the return type is STRING
// This is a little more sophisticated since the result buffers are reused (it grows
// to the longest result string).
struct MinState {
    uint8_t* value;
    int len;
    int buffer_len;

    void set(FunctionContext* context, const StringVal& val) {
        if (buffer_len < val.len) {
            context->free(value);
            value = context->allocate(val.len);
            buffer_len = val.len;
        }

        memcpy(value, val.ptr, val.len);
        len = val.len;
    }
};

// Initialize the MinState scratch space
void MinInit(FunctionContext* context, BufferVal* val) {
    MinState* state = reinterpret_cast<MinState*>(*val);
    state->value = nullptr;
    state->buffer_len = 0;
}

// Update the min value, comparing with the current value in MinState
void MinUpdate(FunctionContext* context, const StringVal& input, BufferVal* val) {
    if (input.is_null) {
        return;
    }

    MinState* state = reinterpret_cast<MinState*>(*val);

    if (state->value == nullptr) {
        state->set(context, input);
        return;
    }

    int cmp = memcmp(input.ptr, state->value, std::min(input.len, state->len));

    if (cmp < 0 || (cmp == 0 && input.len < state->len)) {
        state->set(context, input);
    }
}

// Serialize the state into the min string
const BufferVal MinSerialize(FunctionContext* context, const BufferVal& intermediate) {
    return intermediate;
}

// Merge is the same as Update since the serialized format is the raw input format
void MinMerge(FunctionContext* context, const BufferVal& src, BufferVal* dst) {
    const MinState* src_state = reinterpret_cast<const MinState*>(src);

    if (src_state->value == nullptr) {
        return;
    }

    MinUpdate(context, StringVal(src_state->value, src_state->len), dst);
}

// Finalize also just returns the string so is the same as MinSerialize.
StringVal MinFinalize(FunctionContext* context, const BufferVal& val) {
    const MinState* state = reinterpret_cast<const MinState*>(val);

    if (state->value == nullptr) {
        return StringVal::null();
    }

    StringVal result = StringVal(context, state->len);
    memcpy(result.ptr, state->value, state->len);
    return result;
}

//----------------------------- Bits after Xor ------------------------------------
// Example of a UDA that xors all the input bits and then returns the number of
// resulting bits that are set. This illustrates where the result and intermediate
// are the same type, but a transformation is still needed in Finalize()
// The input type is: double
// The intermediate type is bigint
// the return type is bigint
void XorInit(FunctionContext* context, BigIntVal* val) {
    val->is_null = false;
    val->val = 0;
}

void XorUpdate(FunctionContext* context, const double* input, BigIntVal* val) {
    // BigIntVal is the same ptr as what was passed to CountInit
    if (input == nullptr) {
        return;
    }

    val->val |= *reinterpret_cast<const int64_t*>(input);
}

void XorMerge(FunctionContext* context, const BigIntVal& src, BigIntVal* dst) {
    dst->val |= src.val;
}

BigIntVal XorFinalize(FunctionContext* context, const BigIntVal& val) {
    int64_t set_bits = 0;
    // Do popcnt on val
    // set_bits = popcnt(val.val);
    return BigIntVal(set_bits);
}

//--------------------------- HLL(Distinct Estimate) ---------------------------------
// Example of implementing distinct estimate. As an example, we will compress the
// intermediate buffer.
// Note: this is not the actual algorithm but a sketch of how it would be implemented
// with the UDA interface.
// The input type is: bigint
// The intermediate type is string (fixed at 256 bytes)
// the return type is bigint
void DistinctEstimateInit(FunctionContext* context, StringVal* val) {
    // Since this is known, this will be allocated to 256 bytes.
    EXPECT_EQ(val->len, 256);
    memset(val->ptr, 0, 256);
}

void DistinctEstimatUpdate(FunctionContext* context, const int64_t* input, StringVal* val) {
    if (input == nullptr) {
        return;
    }

    for (int i = 0; i < 256; ++i) {
        int hash = 0;
        // Hash(input) with the ith hash function
        // hash = Hash(*input, i);
        val->ptr[i] = hash;
    }
}

StringVal DistinctEstimatSerialize(FunctionContext* context, const StringVal& intermediate) {
    int compressed_size = 0;
    uint8_t* result = nullptr; // SnappyCompress(intermediate.ptr, intermediate.len);
    return StringVal(result, compressed_size);
}

void DistinctEstimateMerge(FunctionContext* context, const StringVal& src, StringVal* dst) {
    uint8_t* src_uncompressed = nullptr; // SnappyUncompress(src.ptr, src.len);

    for (int i = 0; i < 256; ++i) {
        dst->ptr[i] ^= src_uncompressed[i];
    }
}

BigIntVal DistinctEstimateFinalize(FunctionContext* context, const StringVal& val) {
    int64_t set_bits = 0;
    // Do popcnt on val
    // set_bits = popcnt(val.val);
    return BigIntVal(set_bits);
}

TEST(CountTest, Basic) {
    UdaTestHarness<BigIntVal, BigIntVal, IntVal> test(CountInit, CountUpdate, CountMerge, nullptr,
                                                      CountFinalize);
    std::vector<IntVal> no_nulls;
    no_nulls.resize(1000);

    EXPECT_TRUE(test.execute(no_nulls, BigIntVal(no_nulls.size()))) << test.get_error_msg();
    EXPECT_FALSE(test.execute(no_nulls, BigIntVal(100))) << test.get_error_msg();
}

TEST(CountMultiArgTest, Basic) {
    int num = 1000;
    std::vector<IntVal> no_nulls;
    no_nulls.resize(num);

    UdaTestHarness2<BigIntVal, BigIntVal, IntVal, IntVal> test2(CountInit, Count2Update, CountMerge,
                                                                nullptr, CountFinalize);
    EXPECT_TRUE(test2.execute(no_nulls, no_nulls, BigIntVal(2 * num)));
    EXPECT_FALSE(test2.execute(no_nulls, no_nulls, BigIntVal(100)));

    UdaTestHarness3<BigIntVal, BigIntVal, IntVal, IntVal, IntVal> test3(
            CountInit, Count3Update, CountMerge, nullptr, CountFinalize);
    EXPECT_TRUE(test3.execute(no_nulls, no_nulls, no_nulls, BigIntVal(3 * num)));

    UdaTestHarness4<BigIntVal, BigIntVal, IntVal, IntVal, IntVal, IntVal> test4(
            CountInit, Count4Update, CountMerge, nullptr, CountFinalize);
    EXPECT_TRUE(test4.execute(no_nulls, no_nulls, no_nulls, no_nulls, BigIntVal(4 * num)));
}

bool FuzzyCompare(const BigIntVal& r1, const BigIntVal& r2) {
    if (r1.is_null && r2.is_null) {
        return true;
    }

    if (r1.is_null || r2.is_null) {
        return false;
    }

    return abs(r1.val - r2.val) <= 1;
}

TEST(CountTest, FuzzyEquals) {
    UdaTestHarness<BigIntVal, BigIntVal, IntVal> test(CountInit, CountUpdate, CountMerge, nullptr,
                                                      CountFinalize);
    std::vector<IntVal> no_nulls;
    no_nulls.resize(1000);

    EXPECT_TRUE(test.execute(no_nulls, BigIntVal(1000))) << test.get_error_msg();
    EXPECT_FALSE(test.execute(no_nulls, BigIntVal(999))) << test.get_error_msg();

    test.set_result_comparator(FuzzyCompare);
    EXPECT_TRUE(test.execute(no_nulls, BigIntVal(1000))) << test.get_error_msg();
    EXPECT_TRUE(test.execute(no_nulls, BigIntVal(999))) << test.get_error_msg();
    EXPECT_FALSE(test.execute(no_nulls, BigIntVal(998))) << test.get_error_msg();
}

TEST(MinTest, Basic) {
    UdaTestHarness<StringVal, BufferVal, StringVal> test(MinInit, MinUpdate, MinMerge, MinSerialize,
                                                         MinFinalize);
    test.set_intermediate_size(sizeof(MinState));

    std::vector<StringVal> values;
    values.push_back(StringVal("BBB"));
    EXPECT_TRUE(test.execute(values, StringVal("BBB"))) << test.get_error_msg();

    values.push_back(StringVal("AA"));
    EXPECT_TRUE(test.execute(values, StringVal("AA"))) << test.get_error_msg();

    values.push_back(StringVal("CCC"));
    EXPECT_TRUE(test.execute(values, StringVal("AA"))) << test.get_error_msg();

    values.push_back(StringVal("ABCDEF"));
    values.push_back(StringVal("AABCDEF"));
    values.push_back(StringVal("A"));
    EXPECT_TRUE(test.execute(values, StringVal("A"))) << test.get_error_msg();

    values.clear();
    values.push_back(StringVal::null());
    EXPECT_TRUE(test.execute(values, StringVal::null())) << test.get_error_msg();

    values.push_back(StringVal("ZZZ"));
    EXPECT_TRUE(test.execute(values, StringVal("ZZZ"))) << test.get_error_msg();
}
} // namespace doris_udf
